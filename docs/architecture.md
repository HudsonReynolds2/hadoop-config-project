# Architecture

`hadoop-config-checker` is a producer / Kafka / consumer system. Agents run
as sidecars next to each Hadoop service, watch its config sources, and publish
`ConfigSnapshot` records to a Kafka topic. A long-running consumer ingests
that topic, maintains an in-memory store of the latest snapshot per agent,
and runs three analyses on every incoming record: temporal drift, cross-source
drift, and rule validation. A causality graph then traces every drift to its
downstream effects.

The same pipeline backs the on-demand `hadoopconf status` CLI, which spins up
a fresh consumer, replays the topic from the current end-offset back, and
returns a JSON drift report.

```
┌─────────────────────────────────────────────────────────────────┐
│  Hadoop services           Agents (sidecars)                     │
│  ───────────────────       ─────────────────────                 │
│  namenode                  config-agent (namenode)               │
│  datanode                  config-agent-datanode                 │
│  resourcemanager           config-agent-resourcemanager          │
│  nodemanager               config-agent-nodemanager              │
│  hive-server2              config-agent-hive-server2             │
│  hive-metastore            config-agent-hive-metastore           │
│  spark-client              config-agent-spark-client             │
│  kafka                     config-agent-kafka                    │
│  zookeeper                 config-agent-zookeeper                │
│                                                                  │
│             │ inotify on bind-mounted conf dir                   │
│             │ + heartbeat (60s default)                          │
│             ▼                                                    │
│      ┌──────────────────────────────────────┐                    │
│      │  topic: hadoop-config-snapshots      │                    │
│      │  Kafka — single-broker compose       │                    │
│      └──────────────────────────────────────┘                    │
│             │                                                    │
│             ▼                                                    │
│   ┌────────────────────────────────────────────┐                 │
│   │  config-checker (consumer)                 │                 │
│   │   SnapshotStore (latest per agent)         │                 │
│   │   AgentLivenessTracker (silent-agent)      │                 │
│   │   drift_detector (temporal, cross-source)  │                 │
│   │   validator (rule engine)                  │                 │
│   │   causality_graph (BFS trace)              │                 │
│   │   ─►  JSON drift_report                    │                 │
│   └────────────────────────────────────────────┘                 │
└─────────────────────────────────────────────────────────────────┘
                  ▲
                  │  hadoopconf status   (on-demand replay)
                  │  hadoopconf validate (stateless, local files)
                  │  hadoopconf oneshot  (single snapshot file)
```

## Why Kafka

Three reasons, in order:

1. **Decoupling.** Agents do not know who consumes their data. The cluster can
   add a Prometheus exporter, a web UI, or a second analytics consumer without
   touching agents. Agents and consumer are restarted independently.
2. **Replayability.** `hadoopconf status` replays current snapshots without
   rebuilding state from disk. Each agent re-publishes everything on its
   heartbeat (default 60s), so even a freshly started consumer reaches a full
   view of the cluster within one heartbeat window.
3. **Backpressure isolation.** A slow or crashed consumer cannot stall the
   agents. Snapshots accumulate on the topic and are processed when the
   consumer recovers.

The single-broker compose deployment is fine for development and single-node
clusters. A production deployment would point `CHECKER_KAFKA_BOOTSTRAP` at an
existing Kafka cluster — see [operations.md](operations.md).

## Agents

Each agent is one container running [`checker/agent.py`](../checker/agent.py).
The same image is used for all 9 sidecars; behaviour is configured via env
vars (set in [`docker-compose.override.yml`](../docker-compose.override.yml)).

### What an agent watches

Three optional sources, configurable per-container:

| Source | Trigger | Env var |
| --- | --- | --- |
| `*-site.xml` files in a directory | inotify on the directory | `HADOOP_CONF_DIR` |
| A `KEY=VALUE` env file (typically `hadoop.env`) | inotify on the file | `CHECKER_ENV_FILE` |
| A JVM `-D` flags string | startup + heartbeat only (no file to watch) | `CHECKER_JVM_FLAGS` |
| A `spark-defaults.conf` file | inotify on the file (parent dir if outside `HADOOP_CONF_DIR`) | `CHECKER_SPARK_DEFAULTS_FILE` |

`CHECKER_SPARK_DEFAULTS_FILE` is opt-in — there is no auto-discovery. The
running `agent-spark-client` in `docker-compose.override.yml` does not
currently set it, so spark-defaults snapshots reach the consumer only via
the CLI (`hadoopconf validate --spark-defaults …`). The
`fs-defaultfs-propagation` rule still catches Spark-side drift today
because spark-client mounts the same `core-site.xml` as the rest of the
cluster — the XML snapshot is what gets compared. This is documented as a
gap; see [limitations.md](limitations.md).

An agent emits one `ConfigSnapshot` per source per cycle. The namenode agent,
for example, emits four: `core-site.xml`, `hdfs-site.xml`, `yarn-site.xml`,
`mapred-site.xml` (XML), plus one for `hadoop.env` (env). The Hive agents emit
one snapshot for their JVM flags string only — Hive's config in this image is
injected via `SERVICE_OPTS`, so there is no XML on disk to watch.

### Inotify on directories, not files

The override file mounts `./conf/` as a directory rather than each XML file
individually. Single-file bind mounts on macOS and WSL2 do not always
propagate inotify events, but directory mounts do. The heartbeat is a
defence-in-depth: even if a watcher misses a write, every agent re-publishes
its full set of snapshots every `CHECKER_HEARTBEAT` seconds (default 60).

### Auto-creating the topic

The agent calls `KafkaAdminClient` on startup to create
`hadoop-config-snapshots` if it does not exist. Failure is non-fatal —
the producer retries on each publish, and the warning
`could not connect to Kafka admin for topic check` is expected briefly while
Kafka is still booting.

## The consumer

[`checker/consumer.py`](../checker/consumer.py) is the long-running drift
checker. Two state objects:

- **`SnapshotStore`** — dict keyed by `agent_id`, holding the most recent
  `ConfigSnapshot` from each agent. Last-write-wins per agent.
- **`AgentLivenessTracker`** — monotonic-time map of `agent_id → last_seen`.
  Any agent that has not published within `2 × heartbeat` seconds is reported
  exactly once as a `silent-agent` drift, then suppressed until the agent
  recovers and goes silent again. The default heartbeat is 60s in
  production; the evaluator harness lowers it to 10s via `CHECKER_HEARTBEAT`
  to bound scenario detection latency to ~5–10s.

The pipeline runs on every incoming snapshot
([`process_snapshot`](../checker/consumer.py)):

1. Update `SnapshotStore` and `AgentLivenessTracker`.
2. **Temporal drift** — diff the new snapshot against the previous snapshot
   from the same `agent_id`. Any value change emits a
   `rule_id="temporal-drift"` `DriftResult`.
3. **Cross-source drift** — for every other snapshot from the same service,
   diff overlapping keys. Disagreements emit
   `rule_id="dual-source-consistency"`.
4. **Rule validation** — run the full rule set from
   `rules/hadoop-3.3.x.yaml` against the current `SnapshotStore`. Each
   violated rule emits one `DriftResult` tagged with its rule id.
5. **Silent-agent check** — `AgentLivenessTracker.find_silent()` adds one
   `DriftResult` per newly silent agent.
6. **Causality trace** — `CausalityGraph.trace(drifts)` walks outbound from
   each drifting key and produces one `RootCause` per drift, sorted by impact.

The result is serialised as a `drift_report` JSON object. Optionally (when
`CHECKER_EMIT_ALERTS=true`) it is published to a `hadoop-config-alerts` topic.

## On-demand mode: `hadoopconf status`

The same code runs in a stateless mode:

```bash
hadoopconf status --rules rules/hadoop-3.3.x.yaml --format json
```

It spins up a temporary consumer, polls the topic until either the timeout
elapses or no new messages arrive for one poll cycle, runs the same pipeline,
and exits. Exit code is 1 if any rule failed or any drift is present, 0
otherwise — making it suitable as a CI gate after a config push.

## Stateless mode: `hadoopconf validate`

For pre-deployment CI checks where no cluster is running:

```bash
hadoopconf validate conf/ rules/hadoop-3.3.x.yaml --service namenode
```

The CLI parses every supplied source (XML dir, env file, JVM flags, spark
defaults), builds a synthetic `SnapshotStore`, and runs the validator. No
Kafka, no agents, no cluster. Exit 1 on any rule violation. This is what the
evaluator harness uses to produce per-scenario `checker-report.json` files.

## Component map

```
checker/
  __main__.py                 # python -m checker → cli.main
  cli.py                      # click entry point: validate | status | consume | oneshot | collect
  agent.py                    # producer side: collect_all + KafkaProducer + watchdog
  consumer.py                 # SnapshotStore, AgentLivenessTracker, process_snapshot, run_consumer
  models.py                   # ConfigSnapshot, DriftResult, RootCause, EdgeType, source constants
  collectors/
    xml_collector.py          # collect_xml() — *-site.xml → ConfigSnapshot
    env_collector.py          # parse_env_file(), parse_jvm_flags()
    spark_collector.py        # parse_spark_conf() — whitespace-separated KEY VALUE
  analysis/
    drift_detector.py         # detect(), detect_cross_source(), detect_temporal()
    validator.py              # YAML rule engine: constraint, propagation, must-reference, dual-source
    causality_graph.py        # CausalityGraph, Edge, _DEFAULT_EDGES, trace()
```

For the data flow through these modules see
[detection-pipeline.md](detection-pipeline.md).

## Limitations of this architecture

- **Single broker.** The compose deployment runs one Kafka broker. No
  replication, no quorum.
- **No authentication.** Kafka traffic is plaintext on `hadoop-net`. Agents
  trust the broker, the broker trusts agents. Fine for a dev cluster, not for
  production.
- **No schema registry.** `ConfigSnapshot` is JSON; backwards-compatibility
  with future fields rests on `dataclass.from_dict(**d)` accepting unknown
  keys (which, today, it does not — adding a field will break older
  consumers).
- **Bounded replay window.** `hadoopconf status` replays whatever is currently
  on the topic. Kafka's retention determines how far back you can see; default
  in this compose is whatever the bitnami image ships with.
- **One agent per service.** No HA. If an agent crashes, the silent-agent
  detector flags it after `2 × heartbeat` seconds, but the snapshot itself
  goes stale until the agent recovers.

For component-level limitations and proposed upgrades see
[limitations.md](limitations.md).
