# hadoop-config-checker

A configuration observability and consistency tool for the Hadoop ecosystem.

A Hadoop stack is many independent services (HDFS, YARN, MapReduce, Hive, Spark,
ZooKeeper, Kafka), each loading config from its own files. When a key drifts
between nodes or diverges across a service boundary, the resulting bugs are
hard to trace. This tool makes those failures visible, traceable, and catchable
before they reach production.

The repo bundles the tool itself with a full Docker Compose Hadoop cluster that
serves as its test fixture. The cluster is not the product — the `checker/`
package is. See [plan.md](plan.md) for the full design rationale.

## Status

| Phase | Scope | Status |
|-------|-------|--------|
| 1 | Data model, XML + env collectors | Complete |
| 2 | Kafka agent (stateless, watchdog-based) | Complete |
| 3 | Consumer, drift detector | Complete |
| 4 | YAML rule engine / validator | Complete |
| 5 | Causality graph / root-cause tracing | Complete |
| 6 | Synthetic-worker simulator, scale test | Not started |

**Tests:** 199 tests across 11 files in `tests/checker/`. Full suite passes
against the shipped config fixtures with no cluster required.

## What it does

The tool is a pure-Python observer. It reads local config files, publishes
`ConfigSnapshot` messages to Kafka, and a consumer evaluates them:

```
  agent (per node)                         consumer
  ┌──────────────┐   ConfigSnapshot   ┌──────────────────────────┐
  │ xml_collector│  ───────────────►  │ SnapshotStore            │
  │ env_collector│    Kafka topic     │  ├─ temporal drift       │
  │ watchdog     │                    │  ├─ cross-source drift   │
  └──────────────┘                    │  ├─ YAML rule validator  │
                                      │  └─ causality graph      │
                                      │      → DriftResult       │
                                      │      → RootCause         │
                                      └──────────────────────────┘
                                              │
                                              ▼
                                      stdout JSON + optional
                                      `hadoop-config-alerts` topic
```

Key property: **the tool never calls any Hadoop HTTP endpoint.** No polling,
no load on services, no dependency on `/conf` or `/jmx`. It scales with Kafka,
not with the number of NameNodes it's watching. See
[plan.md §Architecture rationale](plan.md#architecture-rationale--why-push-over-kafka-not-http-pull)
for why.

## Install

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e '.[runtime,test]'
```

This installs the `hadoopconf` CLI and the runtime deps (`kafka-python`,
`watchdog`, `click`, `pyyaml`). The core `checker` package itself uses only
the standard library — the extras are for the CLI, Kafka, and file watching.

## Quick start (no cluster required)

The collectors work against any directory of Hadoop XMLs + optional env file.
Try it against the config files shipped in this repo:

```bash
# List what the collectors see, and check for cross-source drift
# between the XMLs and hadoop.env
hadoopconf collect conf/ --service namenode --env-file hadoop.env --detect-drift

# Validate the same configs against the reference rule set
hadoopconf validate conf/ rules/hadoop-3.3.x.yaml \
  --service namenode --env-file hadoop.env
```

The `validate` command exits 0 when all rules pass and 1 when any fail —
drop it into a CI/CD step as a pre-deploy gate.

## Full end-to-end (with the Hadoop cluster)

The repo ships a 15-service Docker Compose cluster plus an agent + checker
sidecar pair. Bringing it up exercises the full push-based pipeline:

```bash
# 1. Build the custom Hive image (~1 min first time)
docker compose build

# 2. Start the cluster + the agent + the checker.
#    docker-compose.override.yml is auto-loaded and adds the two sidecars.
docker compose up -d

# 3. Wait for everything to go (healthy) — usually 2–4 min on a cold start
docker compose ps

# 4. Watch the checker output
docker compose logs -f checker

# 5. Trigger drift: edit a value in ./conf/yarn-site.xml.
#    The agent's watchdog sees the change, re-collects, and re-publishes
#    within ~1s. The checker emits a JSON drift report to stdout.
```

When you're done:

```bash
docker compose down            # keep HDFS/Postgres volumes
docker compose down -v         # also nuke volumes (fresh cluster next time)
```

## CLI reference

`hadoopconf <subcommand>`:

| Command | Purpose | Exit code |
|---------|---------|-----------|
| `consume` | Start the long-running Kafka consumer loop (also the default `CMD` in `Dockerfile.checker`). | Runs until signal. |
| `collect CONF_DIR` | Collect snapshots from local files, print them as text or JSON. With `--detect-drift`, also runs cross-source drift. | `0` always. |
| `validate CONF_DIR RULES_FILE` | Collect local snapshots, evaluate against a YAML rule set, print pass/fail. | `0` if all rules pass, `1` otherwise. |
| `oneshot SNAPSHOT_FILE` | Load a JSON dump of snapshots and run the full drift/validator/graph pipeline. | `0` if no drift, `1` otherwise. |

Common options on `collect` and `validate`:

- `--service NAME` — logical service tag for the emitted snapshots.
- `--env-file PATH` — also parse a `KEY=VALUE` env file (e.g. `hadoop.env`).
- `--jvm-flags STRING` — parse `-Dkey=value` tokens from a JVM flags string
  (used for Hive, which doesn't bind-mount XMLs).
- `--format json|text` — output format.

## Environment variables

Both the agent and the consumer are configured entirely via env vars. Defaults
are set in `Dockerfile.agent` and `Dockerfile.checker`.

**Agent (`checker.agent`):**

| Var | Default | Purpose |
|-----|---------|---------|
| `HADOOP_CONF_DIR` | `/opt/hadoop/etc/hadoop` | Directory to scan for `*-site.xml`. |
| `CHECKER_ENV_FILE` | *(unset)* | Optional `KEY=VALUE` file (`hadoop.env`). |
| `CHECKER_JVM_FLAGS` | *(unset)* | Optional `-Dkey=value` string (e.g. `SERVICE_OPTS`). |
| `CHECKER_JVM_FLAGS_NAME` | `jvm_flags` | Human-readable source tag for JVM flags. |
| `CHECKER_KAFKA_BOOTSTRAP` | `kafka:9092` | Bootstrap server. |
| `CHECKER_TOPIC` | `hadoop-config-snapshots` | Topic to publish to (auto-created). |
| `CHECKER_SERVICE_NAME` | `unknown` | Logical service name on every snapshot. |
| `CHECKER_HEARTBEAT` | `60` | Heartbeat seconds — re-publishes even if nothing changed. |
| `CHECKER_TOPIC_PARTITIONS` | `12` | Used on auto-create only. |
| `CHECKER_TOPIC_REPLICATION` | `1` | Used on auto-create only. |
| `CHECKER_LOG_LEVEL` | `INFO` | Standard Python levels. |

**Consumer (`checker.consumer`):**

| Var | Default | Purpose |
|-----|---------|---------|
| `CHECKER_KAFKA_BOOTSTRAP` | `kafka:9092` | Bootstrap server. |
| `CHECKER_TOPIC` | `hadoop-config-snapshots` | Topic to consume. |
| `CHECKER_ALERTS_TOPIC` | `hadoop-config-alerts` | Topic for drift alerts (if enabled). |
| `CHECKER_CONSUMER_GROUP` | `hadoop-config-checker` | Kafka consumer group. |
| `CHECKER_EMIT_ALERTS` | `false` | If `true`, also publish `DriftResult`s to the alerts topic. |
| `CHECKER_RULES_FILE` | *(unset)* | Path to a YAML rule file — enables validator rules in the pipeline. |
| `CHECKER_LOG_LEVEL` | `INFO` | |

## Rule set

`rules/hadoop-3.3.x.yaml` ships with five reference rules covering the
rule-engine feature set:

- `hdfs-replication-max` — constraint (`dfs.replication` ≤ `dfs.replication.max`).
- `yarn-scheduler-ceiling` — constraint across services (scheduler max ≤ NM total).
- `fs-defaultfs-propagation` — single-key agreement across a list of services.
- `hive-warehouse-namenode` — "must contain value of" cross-key check.
- `dual-source-consistency` — XML vs env/JVM-flag agreement per service.

The four rule forms — `constraint` with a `relation`, `propagation` with
`services`, `propagation` with `must_contain_value_of`, and
`dual-source-consistency` — are all the validator supports today. New rule
forms require a new handler in `checker/analysis/validator.py`.

## Adopting this on your own cluster

The tool is designed to drop into an existing Hadoop deployment. To run it
against a cluster you don't own:

1. **Pick your Kafka.** If you already have one, set `CHECKER_KAFKA_BOOTSTRAP`
   and `CHECKER_TOPIC`. If you don't, you'll need one — the tool has no
   direct-mode fallback by design.
2. **Deploy the agent per node.** Build `Dockerfile.agent` into your registry
   and run one sidecar per Hadoop-family container you want monitored. Mount
   that service's `HADOOP_CONF_DIR` read-only into the sidecar. Set
   `CHECKER_SERVICE_NAME` to a stable logical name (`namenode-prod-1`,
   `rm-staging`, whatever matches how you want rules to address it). See the
   `agent-hive-server2` commented block in `docker-compose.override.yml` for a
   JVM-flags example (for services that ship config via `-D` flags rather than
   XML).
3. **Write your rules.** Copy `rules/hadoop-3.3.x.yaml` as a starting point and
   edit it to match your version constraints and cross-service invariants.
   Point the consumer at it via `CHECKER_RULES_FILE`.
4. **Extend the causality graph (optional).** Edges are seeded from
   `plan.md` and can be added programmatically via
   `CausalityGraph.add_edge()`. YAML-loadable edges are on the roadmap.
5. **Wire the alerts topic into your pager.** Set `CHECKER_EMIT_ALERTS=true`
   and consume `hadoop-config-alerts` from whatever you already use for
   incident routing. The payload is `DriftResult` JSON — severity, rule_id,
   both values, both source labels.

The tool has no auth-aware mode — see [plan.md](plan.md#goals) for non-goals
(Kerberos, HA NameNode, federated HDFS). If you need any of those, the
collectors are the extension point.

## Testing

The repo has two test suites that test different things:

- `pytest tests/checker/` — 199 unit + integration tests for the checker
  package. Runs without any cluster, uses fixtures in
  `tests/checker/fixtures/`. This is the primary regression suite.
- `tests/run-all.sh` — 6 bash smoke tests that drive the live cluster (HDFS
  round-trip, YARN MapReduce pi, Hive beeline, Kafka pub/sub, ZooKeeper
  znodes, Spark-on-YARN). Requires the stack to be up and healthy. These
  exist to prove the fixture cluster actually works; they don't exercise the
  checker.

```bash
pytest tests/checker/                  # all 199
pytest tests/checker/ -k validator     # one module
cd tests && ./run-all.sh               # cluster smoke tests
```

## Repo layout

```
hadoop-config-project/
├── checker/                          The tool (Python package)
│   ├── models.py                     ConfigSnapshot, DriftResult, RootCause, EdgeType
│   ├── collectors/
│   │   ├── xml_collector.py          *-site.xml parser (handles <n> and <name>)
│   │   └── env_collector.py          hadoop.env + JVM -Dkey=value parsers
│   ├── analysis/
│   │   ├── drift_detector.py         Pairwise + cross-source diff
│   │   ├── validator.py              YAML rule engine
│   │   └── causality_graph.py        Edge set + BFS root-cause tracing
│   ├── agent.py                      Kafka producer sidecar, watchdog-based
│   ├── consumer.py                   Kafka consumer + full pipeline
│   └── cli.py                        `hadoopconf` entry point
├── rules/
│   └── hadoop-3.3.x.yaml             Reference rule set (5 rules)
├── tests/
│   ├── checker/                      199 pytest tests for the tool
│   └── 0*-*.sh                       6 bash smoke tests for the cluster
├── conf/                             Cluster XMLs — also the test fixture
├── hadoop.env                        Dual-source env file (intentionally redundant)
├── Dockerfile.agent                  Agent sidecar image
├── Dockerfile.checker                Consumer image
├── docker-compose.yml                15-service Hadoop stack
├── docker-compose.override.yml       Adds agent + checker sidecars
├── pyproject.toml                    Package definition (core = stdlib only)
└── plan.md                           Full design doc — read this for rationale
```

## The Hadoop fixture cluster

The cluster exists to run the tool against something real. It's a complete
Hadoop stack — useful on its own if you want to poke at HDFS/YARN/Hive/Spark
locally. Troubleshooting and tuning notes live in
[Troubleshooting.md](Troubleshooting.md).

**Services:**

| Service | Image | Ports (host) | Purpose |
|---------|-------|--------------|---------|
| zookeeper | `zookeeper:3.9.2` | 2181 | Coordination |
| kafka | `apache/kafka:3.8.0` | 9092 | Snapshot bus (and fixture for the 04 test) |
| postgres-metastore | `postgres:16` | — | Hive metastore backing DB |
| namenode | `apache/hadoop:3.3.6` | 9870, 8020 | HDFS NN |
| datanode | `apache/hadoop:3.3.6` | 9864 | HDFS DN |
| resourcemanager | `apache/hadoop:3.3.6` | 8088, 8032 | YARN RM |
| nodemanager | `apache/hadoop:3.3.6` | 8042 | YARN NM |
| hive-metastore | `hadoop-stack-hive:local` | 9083 | Hive Thrift metastore |
| hive-server2 | `hadoop-stack-hive:local` | 10000, 10002 | Hive JDBC/Thrift server |
| spark-master | `apache/spark:3.5.3` | 8080, 7077 | Spark standalone master |
| spark-worker | `apache/spark:3.5.3` | 8081 | Spark standalone worker |
| spark-client | `apache/spark:3.5.3` | — | Idle container for `spark-submit --master yarn` |
| hdfs-init / spark-archive-init / hive-schema-init | — | — | One-shot init containers |
| agent / checker | local builds | — | The tool itself (from the override file) |

**Web UIs:** NameNode 9870 · ResourceManager 8088 · DataNode 9864 · NodeManager
8042 · Hive HS2 10002 · Spark Master 8080 · Spark Worker 8081.

**Why Hive is special.** Hive services don't read `conf/*.xml` — they get
config via `-Dkey=value` flags in `SERVICE_OPTS`. That's why `env_collector.py`
has a `parse_jvm_flags()` path and why the agent accepts `CHECKER_JVM_FLAGS`.
It's a real-world case the tool has to handle to be useful outside this repo.

**Why `hadoop.env` duplicates the XMLs.** Deliberately. The dual-source
pattern is exactly the kind of propagation hazard the tool exists to detect,
and `dual-source-consistency` (in the rule set) validates that the two agree.

**Debug endpoints the tool does *not* use.** Each Hadoop service exposes
its effective runtime config over HTTP (`/conf`, `/jmx`). These are useful
for manual debugging but are deliberately never polled by the checker — the
push-over-Kafka architecture exists precisely to avoid that load. Handy URLs
for when you're debugging by hand:

| Endpoint | What it gives |
|----------|---------------|
| `http://namenode:9870/conf` | NN's parsed core+hdfs-site |
| `http://namenode:9870/jmx` | NN JMX, includes effective HDFS config |
| `http://resourcemanager:8088/conf` | RM's parsed yarn-site |
| `http://nodemanager:8042/conf` | NM's parsed yarn-site |
| `http://hive-server2:10002/conf` | HS2's parsed hive-site |
| `thrift://hive-metastore:9083` | Metastore Thrift |
| `jdbc:postgresql://postgres-metastore:5432/metastore` | Raw metastore schema |

## Reference

- [plan.md](plan.md) — full design doc, architecture rationale, phase-by-phase plan.
- [Troubleshooting.md](Troubleshooting.md) — common issues, memory tuning, startup timing.
- [rules/hadoop-3.3.x.yaml](rules/hadoop-3.3.x.yaml) — the reference rule set.
