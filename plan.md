# hadoop-config-checker — project plan

## What this project is

A configuration observability and consistency tool for the Hadoop ecosystem.
The Hadoop stack is composed of many independent services (HDFS, YARN, MapReduce,
Hive, Spark, ZooKeeper, Kafka) that each load configuration from their own files.
When configs drift between nodes, or when a key that must agree across service
boundaries diverges, bugs appear that are extremely difficult to trace. This tool
makes those failures visible, traceable, and catchable before they reach production.

The tool is intended as a genuine contribution to the Hadoop ecosystem — not a
one-off script, but a reusable, extensible Python package that other teams can
adopt against their own clusters.

---

## Goals

**Primary goal:** given a running Hadoop cluster, detect configuration mismatches,
identify which config keys failed to propagate correctly, and trace observed
failures back to their root cause in a specific key or key relationship.

**Secondary goals:**

- Validate configs *before* deployment by comparing a proposed config set against
  a reference schema with known constraints and cross-component relationships.
- Scale to large clusters (hundreds to thousands of workers) without imposing
  load on Hadoop services.
- Provide a CLI for on-demand auditing and a stream consumer for continuous
  monitoring.
- Be pluggable into CI/CD pipelines as a pre-deploy gate.

**Non-goals for this implementation:**

- Auto-remediation (the tool detects and reports; it does not change configs).
- Kerberos / auth-aware endpoint access.
- HA NameNode or federated HDFS support.
- HTTP polling of Hadoop web UIs (see architecture rationale below).

---

## Architecture rationale — why push over Kafka, not HTTP pull

The obvious approach is to poll each service's `/conf` HTTP endpoint on a
schedule. This is explicitly rejected for the following reasons:

**Load on services.** The Hadoop web UI endpoints (`/conf`, `/jmx`) are debug
interfaces, not monitoring APIs. Polling them from a central checker at any
meaningful frequency adds external load to every NameNode, ResourceManager,
and NodeManager in the cluster. At hundreds of workers this becomes a thundering
herd problem on services that are already doing real work.

**Polling latency.** A drift that happens between poll cycles is invisible until
the next cycle. Push-on-change has zero detection latency.

**Irrelevance in containers.** In a Docker Compose cluster with bind-mounted
configs, the runtime-loaded vs on-disk divergence that HTTP polling is meant to
catch does not exist in the same way as on bare-metal. The container sees file
changes immediately.

**The tool instead uses a push model over Kafka**, which is already present in
this cluster. Each node runs a lightweight agent that reads its own local config
files and publishes a `ConfigSnapshot` to a Kafka topic. The checker is a pure
Kafka consumer. This means:

- Zero load on Hadoop services at any scale.
- Drift is detected the moment an agent detects a file change and publishes.
- The number of workers is bounded only by Kafka throughput, not the checker.
- Kafka provides a natural time-series of snapshots for drift history at no
  extra cost.

The Hadoop `/conf` HTTP endpoints are useful for manual debugging and are
documented as such in the repo README, but are not called by this tool.

---

## The test environment

The repo at `~/EC528/hadoop-config-project` is a Docker Compose cluster that
serves as both the development environment and the demonstration target.
It runs 15 services on a single `hadoop-net` bridge network.

### Relevant services

| Service            | Container name      | Role in this tool                              |
|--------------------|---------------------|------------------------------------------------|
| Kafka              | `kafka`             | Config snapshot bus (KRaft, port 9092)         |
| ZooKeeper          | `zookeeper`         | Available if needed for coordination           |
| HDFS NameNode      | `namenode`          | Hadoop-family service, agent target            |
| YARN ResourceMgr   | `resourcemanager`   | Hadoop-family service, agent target            |
| YARN NodeManager   | `nodemanager`       | Hadoop-family service, agent target            |
| Hive Server2       | `hive-server2`      | Hive service — config via JVM flags, not XMLs  |
| Spark client       | `spark-client`      | Idle container, useful for running CLI tools   |

### Config files on disk

All Hadoop-family services bind-mount these files from `./conf/`:

```
conf/core-site.xml       fs.defaultFS = hdfs://namenode:8020
conf/hdfs-site.xml       dfs.replication=1, NN/DN paths
conf/yarn-site.xml       NM memory=4096MB, scheduler.max=2048MB, vmem-check=false
conf/mapred-site.xml     framework=yarn, MR memory settings
conf/hive-site.xml       Postgres metastore JDBC, HDFS warehouse path
conf/spark-defaults.conf master=yarn, spark.yarn.archive path
```

`hadoop.env` sets the same key-value pairs as the XMLs as environment variables
by design — this dual-source pattern is a known propagation-conflict scenario
the tool must detect.

**Important:** `hdfs-site.xml` uses `<n>` tags instead of the standard `<name>`
tag. The XML collector must handle both variants.

**Important:** Hive services (`hive-metastore`, `hive-server2`) do not use
bind-mounted XMLs. They receive config exclusively via `SERVICE_OPTS` JVM system
properties set in `docker-compose.yml` (e.g. `-Dfs.defaultFS=hdfs://namenode:8020`).
The env collector must parse this format.

### Known intentional config choices (suppress these in output)

- `yarn.nodemanager.vmem-check-enabled=false` — deliberate Docker cgroup workaround.
- `hadoop.env` duplicating XML values — intentional dual-source test scenario.

### Kafka topic

Single topic: `hadoop-config-snapshots`

Bootstrap server: `kafka:9092` (inside `hadoop-net`) or `localhost:9092` (host).
Create with 12 partitions, replication factor 1 (single-broker cluster).

---

## Data model (`checker/models.py`)

```python
from dataclasses import dataclass, field
from enum import Enum

class EdgeType(Enum):
    CONSTRAINT   = "constraint"    # hard rule — violation = definite bug
    INFLUENCE    = "influence"     # soft — violation = probable symptom
    PROPAGATION  = "propagation"   # must-match across service boundaries

@dataclass
class ConfigSnapshot:
    agent_id: str       # unique per agent, e.g. "namenode-hdfs-site"
    service: str        # "namenode", "resourcemanager", "nodemanager", etc.
    source: str         # "xml_file" | "env_file" | "jvm_flags"
    source_path: str    # file path or description
    host: str           # container hostname
    timestamp: str      # ISO 8601
    properties: dict    # key -> value, all strings

@dataclass
class DriftResult:
    key: str
    service: str
    source_a: str        # e.g. "xml_file:conf/yarn-site.xml"
    value_a: str | None
    source_b: str        # e.g. "env_file:hadoop.env"
    value_b: str | None
    severity: str        # "critical" | "warning" | "info"
    rule_id: str | None

@dataclass
class RootCause:
    key: str
    service: str
    drift: DriftResult
    downstream_effects: list[str]
    severity: str
```

---

## Repository layout

```
hadoop-config-project/
├── checker/                          Python package
│   ├── __init__.py
│   ├── models.py
│   ├── collectors/
│   │   ├── xml_collector.py          Parses *-site.xml files
│   │   └── env_collector.py          Parses hadoop.env and JVM -D flags
│   ├── analysis/
│   │   ├── drift_detector.py         Diffs snapshots, applies rules
│   │   ├── validator.py              Evaluates YAML rule sets
│   │   └── causality_graph.py        Graph traversal for root cause tracing
│   ├── agent.py                      Reads local config, publishes to Kafka
│   ├── consumer.py                   Kafka consumer + drift pipeline
│   └── cli.py                        Click-based CLI entry point
├── rules/
│   └── hadoop-3.3.x.yaml             Reference rule set
├── simulate/
│   ├── worker.py                     Simulated worker (no real Hadoop needed)
│   ├── inject_drift.py               Publishes a deliberate violation
│   └── README.md
├── tests/
│   └── checker/
│       ├── test_xml_collector.py
│       ├── test_env_collector.py
│       ├── test_drift_detector.py
│       └── test_causality_graph.py
├── Dockerfile.agent                  Agent sidecar image
├── Dockerfile.checker                Consumer image
├── docker-compose.override.yml       Adds agent + checker to the stack
└── plan.md                           This file
```

---

## Component design

### Agent (`checker/agent.py`)

Runs on every node. Reads only local files. Never calls any Hadoop HTTP endpoint.

**On startup:** instantiate collectors for all sources in `HADOOP_CONF_DIR`,
publish one `ConfigSnapshot` per source file to `hadoop-config-snapshots`.

**On file change:** use `watchdog` to monitor `HADOOP_CONF_DIR`. Re-publish the
affected snapshot immediately when any `*-site.xml` is modified.

**Heartbeat:** re-publish all snapshots on `CHECKER_HEARTBEAT` interval (default
60s) so the consumer can detect silent agents.

```
Environment variables:
  HADOOP_CONF_DIR          directory to watch (default: /opt/hadoop/etc/hadoop)
  CHECKER_KAFKA_BOOTSTRAP  bootstrap server (default: kafka:9092)
  CHECKER_TOPIC            topic name (default: hadoop-config-snapshots)
  CHECKER_SERVICE_NAME     logical service name for snapshot tagging
  CHECKER_HEARTBEAT        heartbeat interval seconds (default: 60)
```

The agent has no state. It reads files, writes to Kafka, nothing else.

### Consumer (`checker/consumer.py`)

A Kafka consumer group member.

**SnapshotStore:** in-memory dict of `agent_id -> ConfigSnapshot`. Updated on
every incoming message.

**On each new snapshot:** run `DriftDetector` against the reference snapshot for
that service. Run `Validator` against loaded rules. On any violation, run
`CausalityGraph.trace()`. Emit a structured JSON report to stdout.

**Output topic (optional):** write `DriftResult` and `RootCause` objects to a
`hadoop-config-alerts` topic for downstream consumption.

### Simulator (`simulate/worker.py`)

Generates N synthetic agents that publish realistic `ConfigSnapshot` objects
based on the real config files, with controllable drift injected into a
configurable fraction. Does not require running Hadoop containers.

```bash
python simulate/worker.py \
  --workers 200 \
  --drift-rate 0.05 \
  --kafka localhost:9092
```

`inject_drift.py` publishes a single deliberate violation for a named worker
and key — used for end-to-end detection testing.

---

## Rule set (`rules/hadoop-3.3.x.yaml`)

```yaml
rules:
  - id: hdfs-replication-max
    description: dfs.replication must not exceed dfs.replication.max
    type: constraint
    key: dfs.replication
    service: namenode
    relation: lte
    target_key: dfs.replication.max
    target_service: namenode
    severity: critical

  - id: yarn-scheduler-ceiling
    description: scheduler max allocation must not exceed NM total memory
    type: constraint
    key: yarn.scheduler.maximum-allocation-mb
    service: resourcemanager
    relation: lte
    target_key: yarn.nodemanager.resource.memory-mb
    target_service: nodemanager
    severity: critical

  - id: fs-defaultfs-propagation
    description: fs.defaultFS must be identical across all services
    type: propagation
    key: fs.defaultFS
    services: [namenode, resourcemanager, nodemanager, hive-server2]
    severity: critical

  - id: hive-warehouse-namenode
    description: hive.metastore.warehouse.dir must reference the configured namenode
    type: propagation
    key: hive.metastore.warehouse.dir
    service: hive-server2
    must_contain_value_of:
      key: fs.defaultFS
      service: namenode
    severity: warning

  - id: dual-source-consistency
    description: keys present in both XML files and hadoop.env must agree
    type: propagation
    sources: [xml_file, env_file]
    severity: warning
```

---

## Causality graph — initial edge set

| Source key (service) | Edge type | Target | Notes |
|---|---|---|---|
| `dfs.replication` (namenode) | constraint | `dfs.replication.max` (namenode) | must be ≤ |
| `dfs.replication` (namenode) | influence | HDFS write pipeline | higher = more ack round trips |
| `fs.defaultFS` (core-site) | propagation | `fs.defaultFS` (hive-server2) | injected via SERVICE_OPTS |
| `fs.defaultFS` (core-site) | propagation | `spark.hadoop.fs.defaultFS` (spark-client) | via spark-defaults |
| `yarn.nodemanager.resource.memory-mb` (nodemanager) | constraint | `yarn.scheduler.maximum-allocation-mb` (resourcemanager) | NM total ≥ scheduler max |
| `yarn.scheduler.maximum-allocation-mb` (resourcemanager) | influence | container OOM kill risk | containers requesting more get killed |
| `hive.metastore.uris` (hive-server2) | propagation | `hive-metastore:9083` reachability | must resolve |
| `hadoop.env` keys | propagation | corresponding XML keys | dual-source conflict |

---

## Implementation plan

### Phase 1 — data model and collectors (start here)

**Goal:** collect a `ConfigSnapshot` from every config source in the cluster
and serialize it to JSON. No containers required for this phase.

**Tasks:**

1. Create `checker/models.py` with the full data model above.

2. Create `checker/collectors/xml_collector.py`. Parse `*-site.xml` using
   `xml.etree.ElementTree`. Handle both `<name>` and `<n>` tag variants — the
   cluster's XMLs use `<n>`. Return a `ConfigSnapshot` with `source="xml_file"`.

3. Create `checker/collectors/env_collector.py` with two parsers:
   - `parse_env_file(path)` — reads `KEY=VALUE` format (hadoop.env).
   - `parse_jvm_flags(flags_str)` — reads `-Dkey=value` format (SERVICE_OPTS).
   Both return `ConfigSnapshot` with appropriate `source` tags.

4. Write unit tests in `tests/checker/` that run both collectors against the
   real files in `conf/` and `hadoop.env`. No mocking needed — use the actual
   files.

**Acceptance:** `pytest tests/checker/test_xml_collector.py` and
`test_env_collector.py` pass with no cluster running. Both collectors produce
correct `ConfigSnapshot` JSON with all keys from the real config files.

### Phase 2 — Kafka agent

**Goal:** publish snapshots from a running container to the Kafka topic.

**Tasks:**

1. Create `checker/agent.py`. On startup: collect all sources in
   `HADOOP_CONF_DIR`, publish each snapshot to `hadoop-config-snapshots`.
   Create the topic if it does not exist (12 partitions, RF=1).

2. Add file watching with `watchdog`. On any `*-site.xml` modification,
   re-collect and re-publish that file's snapshot.

3. Add heartbeat loop at `CHECKER_HEARTBEAT` interval.

4. Write `Dockerfile.agent`:
   ```
   FROM python:3.12-slim
   RUN pip install kafka-python watchdog click pyyaml
   COPY checker/ /app/checker/
   WORKDIR /app
   ENTRYPOINT ["python", "-m", "checker.agent"]
   ```

5. Add an `agent` service to `docker-compose.override.yml` mounting `./conf`
   read-only, with `CHECKER_SERVICE_NAME=test-agent`.

**Acceptance:** after `docker compose -f docker-compose.yml -f docker-compose.override.yml up agent`,
messages appear in the topic:
```bash
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic hadoop-config-snapshots \
  --from-beginning
```

### Phase 3 — consumer and drift detector

**Goal:** consume the snapshot stream and report drift in real time.

**Tasks:**

1. Create `checker/analysis/drift_detector.py`. Core function:
   `detect(snapshot_a, snapshot_b) -> list[DriftResult]`. Compare key by key.
   Keys present in one but absent in the other are reported as drift.

2. Create `checker/consumer.py` with a `SnapshotStore` and the consumer loop.
   On each new snapshot: update store, run `detect` against the reference for
   that service, print violations as JSON.

3. Add `hadoopconf consume` CLI subcommand.

4. Write `Dockerfile.checker` and add `checker` service to
   `docker-compose.override.yml`.

**Acceptance:** modify a value in `conf/yarn-site.xml` while the stack is
running. The agent detects the change, re-publishes, and the checker reports
the drift within seconds. No Hadoop service is restarted or queried.

### Phase 4 — validator and rule engine

**Goal:** evaluate the snapshot store against the YAML rule set.

**Tasks:**

1. Write `rules/hadoop-3.3.x.yaml` (full content above).

2. Create `checker/analysis/validator.py`. Load rules, evaluate each against
   the current `SnapshotStore`, return `ValidationResult` objects.

3. Integrate validator into consumer loop.

4. Add `hadoopconf validate` CLI subcommand (one-shot, reads snapshots from a
   saved JSON file).

**Acceptance:** validator reports the `yarn-scheduler-ceiling` rule as passing
with the current cluster values (scheduler.max=2048 ≤ NM memory=4096) and
correctly flags it as a violation when those values are swapped in the XML.

### Phase 5 — causality graph

**Goal:** trace drift to root causes with downstream effects.

**Tasks:**

1. Create `checker/analysis/causality_graph.py`. Graph as
   `dict[(service, key), list[Edge]]`. Seed with the initial edge set above.

2. Implement `trace(drifts) -> list[RootCause]`. Walk outbound edges, collect
   downstream effects, rank by impact (most downstream nodes first).

3. Integrate into consumer — emit `RootCause` objects alongside drift results.

**Acceptance:** introducing a mismatch in `fs.defaultFS` produces a root cause
report listing Hive and Spark as downstream effects.

### Phase 6 — simulation and scale testing

**Goal:** verify the tool scales without touching Hadoop services.

**Tasks:**

1. Write `simulate/worker.py`. Generates N synthetic `ConfigSnapshot` objects
   based on the real config files and publishes them to Kafka at a configurable
   rate. Each simulated worker has a unique `agent_id`.

2. Write `simulate/inject_drift.py`. Publishes a single deliberate violation
   for a named worker ID and key.

3. Run load test: 200 workers at 60s heartbeat (≈3.3 msg/s aggregate).
   Verify consumer lag stays near zero and no HTTP requests are made to any
   Hadoop service.

**Acceptance:** `simulate/worker.py --workers 200 --drift-rate 0.05` completes
with all injected violations detected and zero impact on the running cluster.

---

## Dependencies

```
kafka-python     # Kafka producer and consumer
watchdog         # inotify-based file watching
click            # CLI
pyyaml           # rule set parsing
```

Notably absent: `requests`, any Hadoop or Spark library, any JVM dependency.
The tool is a pure Python observer — it reads files and speaks Kafka. It never
calls any Hadoop HTTP endpoint.

---
