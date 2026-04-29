# Operations

Deploying the checker against a real cluster, configuring it, and
troubleshooting common issues.

This page replaces the older `Troubleshooting.md`, which was written
against an earlier version of the override compose file. Anything that
contradicts what's here should be considered out of date.

## Configuration via env vars

Every behavior knob is an environment variable. The override compose file
sets sensible defaults; in a real deployment, set them in your orchestrator
of choice.

### Agent variables

| Variable | Default | What it does |
| --- | --- | --- |
| `CHECKER_KAFKA_BOOTSTRAP` | `kafka:9092` | Kafka broker to publish to |
| `CHECKER_TOPIC` | `hadoop-config-snapshots` | Topic to publish snapshots to |
| `CHECKER_SERVICE_NAME` | (required) | Logical service tag for this agent's snapshots |
| `CHECKER_HEARTBEAT` | `60` | Re-publish interval (seconds). Detection latency is bounded by 1× this; silent-agent threshold is 2×. |
| `CHECKER_LOG_LEVEL` | `INFO` | Python logger level |
| `HADOOP_CONF_DIR` | `/opt/hadoop/etc/hadoop` | Directory to inotify-watch for `*-site.xml` |
| `CHECKER_ENV_FILE` | (none) | Path to a `KEY=VALUE` env file (e.g. `hadoop.env`) |
| `CHECKER_JVM_FLAGS` | (none) | A `-Dkey=value -Dkey=value ...` string |
| `CHECKER_JVM_FLAGS_NAME` | `jvm_flags` | Human-readable name for the flag source (e.g. `SERVICE_OPTS`) |
| `CHECKER_SPARK_DEFAULTS_FILE` | (none) | Path to a `spark-defaults.conf` file |

`CHECKER_SERVICE_NAME` is the only **required** agent variable. The rest
have defaults or are opt-in.

A single agent typically sets one or more of `HADOOP_CONF_DIR` /
`CHECKER_ENV_FILE` / `CHECKER_JVM_FLAGS` / `CHECKER_SPARK_DEFAULTS_FILE`,
depending on what configuration sources the service it sidecars actually
has on disk. Hive, Kafka, and ZooKeeper agents only set
`CHECKER_JVM_FLAGS` because those services configure themselves from env
vars at startup, not from files.

### Consumer variables

| Variable | Default | What it does |
| --- | --- | --- |
| `CHECKER_KAFKA_BOOTSTRAP` | `kafka:9092` | Where to read from |
| `CHECKER_TOPIC` | `hadoop-config-snapshots` | Snapshot topic |
| `CHECKER_ALERTS_TOPIC` | `hadoop-config-alerts` | Where to publish drift_report messages (only used if `CHECKER_EMIT_ALERTS=true`) |
| `CHECKER_EMIT_ALERTS` | `false` | Whether to publish drift reports back to Kafka |
| `CHECKER_CONSUMER_GROUP` | `hadoop-config-checker` | Kafka consumer group |
| `CHECKER_RULES_FILE` | (none) | Path to a YAML rule file |
| `CHECKER_GRAPH_FILE` | (none) | Path to a YAML causality graph file. Falls back to compiled `_DEFAULT_EDGES`. |
| `CHECKER_LOG_LEVEL` | `INFO` | Python logger level |

Both `CHECKER_RULES_FILE` and `CHECKER_GRAPH_FILE` should be paths inside
the consumer container. The override file mounts `./rules` at
`/etc/checker/rules:ro` and points both at it.

## Deploying to a real cluster

The compose stack runs everything in one Docker network for development.
For a real cluster, you'll typically want:

1. **Existing Kafka.** Point `CHECKER_KAFKA_BOOTSTRAP` at it. The agents
   create the snapshot topic on startup if missing; if your Kafka is
   locked down, pre-create the topic.
2. **One agent per Hadoop service host.** Deploy as a sidecar container on
   the same host as the service it watches. The agent needs read access to
   the same config directory the service reads.
3. **One consumer.** A single instance of the checker is enough; it's not
   CPU-bound and the rule engine runs in well under a second per
   snapshot. Run it anywhere with network access to Kafka.

### Minimal real-cluster agent (Kubernetes example)

```yaml
# agent sidecar in the same Pod as a NameNode
- name: config-agent
  image: hadoop-config-agent:0.1.0
  env:
    - name: CHECKER_KAFKA_BOOTSTRAP
      value: "kafka.platform.svc.cluster.local:9092"
    - name: CHECKER_SERVICE_NAME
      value: "namenode"
    - name: CHECKER_HEARTBEAT
      value: "60"
    - name: HADOOP_CONF_DIR
      value: "/etc/hadoop/conf"
    - name: CHECKER_ENV_FILE
      value: "/etc/hadoop/hadoop.env"
  volumeMounts:
    - name: hadoop-conf
      mountPath: /etc/hadoop/conf
      readOnly: true
    - name: hadoop-env
      mountPath: /etc/hadoop/hadoop.env
      readOnly: true
      subPath: hadoop.env
```

### Profiles in compose

The override file uses Docker Compose profiles to make agents
individually toggleable. `.env`:

```
COMPOSE_PROFILES=namenode,datanode,resourcemanager,nodemanager,hive-server2,hive-metastore,spark-client,kafka,zookeeper
```

Removing a profile name disables that agent. Rules that depend on the
disabled agent's snapshots **skip** rather than fail (see [rules.md
§"Result status"](rules.md)).

## Troubleshooting

### Cluster startup

#### NameNode stays unhealthy on cold boot

The first-boot HDFS format takes up to 60s on a cold volume. Give the
healthcheck its `start_period`. If it genuinely fails:

```bash
docker compose logs namenode
```

Watch for permission errors on the named volume — older Docker Desktop
on Windows in particular creates volumes with restrictive ownership.

```bash
docker compose down -v && docker compose up -d
```

#### `hive-schema-init` fails with "database not ready"

The Postgres healthcheck sometimes reports ready before `init.sql`
finishes. The init service then races and exits with an error.

Re-run just the init:

```bash
docker compose up -d hive-schema-init
```

#### `beeline` returns nothing on a cold cluster

HiveServer2 reports healthy ~60s before it actually serves sessions. If
test 03 fails on a cold cluster, wait 60s and rerun.

### Workload failures

#### SparkPi fails with "Application application_xxx failed 2 times"

Almost always a memory issue. Check the NodeManager logs:

```bash
docker compose logs nodemanager
```

Common cause: executor memory exceeds
`yarn.scheduler.maximum-allocation-mb` (2048 MB in the shipped config).
Either lower the request, or raise both of these together (never one
without the other):

- `yarn.nodemanager.resource.memory-mb`
- `yarn.scheduler.maximum-allocation-mb`

This is exactly what the `yarn-scheduler-ceiling` rule catches.

#### YARN containers killed mid-job

Docker cgroup vmem accounting is unreliable.
`yarn.nodemanager.vmem-check-enabled=false` in `conf/yarn-site.xml`
exists for this reason. Re-enabling it leads to Spark containers being
killed with `Container killed on request. Exit code is 143`.

This is a documented footgun, not a checker bug. Don't add a rule that
forbids `vmem-check-enabled=false` unless it can detect "running inside
Docker".

### Agent issues

#### Agent logs `could not connect to Kafka admin for topic check`

The agent calls `KafkaAdminClient` on startup to auto-create the topic.
If Kafka isn't reachable yet, this warning appears — it's not fatal.
The producer retries on publish. If it persists after Kafka is healthy:

```bash
docker compose logs <agent-name> | grep -i kafka
docker compose exec <agent-name> python -c "import socket; print(socket.gethostbyname('kafka'))"
```

#### Consumer reports nothing on file change

Sanity-check the path the agent is watching:

```bash
docker compose logs <agent-name> | grep 'watching'
```

The override file mounts `./conf/` as a directory, not individual files,
because single-file bind mounts don't always propagate inotify on every
host filesystem. Confirm inside the container:

```bash
docker compose exec <agent-name> ls -la /opt/hadoop/etc/hadoop/
```

The heartbeat (`CHECKER_HEARTBEAT=60` by default) re-publishes everything
on its interval anyway, so even a missed inotify event shows up within a
minute.

### CLI issues

#### `hadoopconf` command not found

The package is installed but the entry point isn't on `PATH`.

```bash
source venv/bin/activate
# or
python -m checker.cli <subcommand>
```

#### `hadoopconf status` exits with "topic has no partitions reachable"

The `status` command bounds both metadata discovery and message iteration
with `--timeout` (default 30s). This message means Kafka was reached but
the topic name doesn't exist or no broker has the topic's metadata
available. Check:

```bash
docker compose exec kafka kafka-topics.sh \
    --bootstrap-server kafka:9092 --list
```

If the topic is missing, an agent hasn't started yet — check
`docker compose logs <some-agent>`. Agents auto-create the topic on
startup.

#### `pytest tests/checker/` fails before any test runs

Missing test deps:

```bash
pip install -e '.[runtime,test]'
```

`[runtime]` is also needed — some tests exercise the Kafka-using code
paths with `kafka-python` mocked, but the imports still have to resolve.

#### Validator says "rule passed" for a rule you thought was broken

The validator's key lookup is flexible: it first tries an exact-service
match, then falls back to *any* service that has the key. This is
intentional — bind-mounted XMLs are read by multiple services, and
strict per-service matching produces false negatives.

See [`checker/analysis/validator.py`](../checker/analysis/validator.py)
("Key lookup strategy") and [rules.md §"Multi-service propagation"](rules.md)
for the strict-then-fallback strategy used by `fs-defaultfs-propagation`.

If you want a strict per-service rule, use the multi-service propagation
form, which has the strict-then-fallback logic; or write a constraint rule
naming the service explicitly.

### Complete tear-down

When something is deeply wrong:

```bash
docker compose down -v              # stops + removes volumes
docker system prune -f              # optional: reclaim space
docker compose build --no-cache     # rebuild custom images
docker compose up -d
```

Expect 3-5 minutes on the first boot after a full nuke.

## Logging

All checker components log to stdout via Python's `logging` module. The
log level is controlled by `CHECKER_LOG_LEVEL` and applies to both agent
and consumer.

- `INFO` (default): one line per snapshot collected, one line per drift
  report. Suitable for production.
- `DEBUG`: verbose; one line per inotify event, per Kafka publish, per
  rule evaluated. Very loud — use temporarily.

The logger names follow `checker.<module>`, so per-module filtering is
possible if you wire in a Python logging config:

```
checker.agent
checker.consumer
checker.collectors.xml_collector
checker.collectors.env_collector
checker.collectors.spark_collector
checker.analysis.validator
checker.analysis.drift_detector
checker.analysis.causality_graph
```

## Capacity

Single-broker compose Kafka, single consumer, on a typical workstation:

- Snapshot rate: each agent publishes one message per source per
  heartbeat. With 9 agents and the default 60s heartbeat, that's roughly
  one message every 6.7 seconds across the whole cluster. A burst of
  inotify-driven publishes adds maybe 9 more in a second when somebody
  changes a config file.
- Consumer overhead: rule evaluation runs in well under a second per
  cycle on the shipped 7-rule set. The dominant cost is Kafka poll
  latency.
- Memory: the `SnapshotStore` holds at most one entry per `agent_id`.
  ~30 agent-ids, each carrying a few hundred properties as strings — well
  under 5 MB resident.

For a 100-service cluster, expect proportionally higher snapshot rates
but no memory or CPU concerns; the bottleneck would be human-readable
output, not engine throughput.
