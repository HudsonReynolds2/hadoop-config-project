# Usage

How the tool is actually used: cold start the cluster, run each `hadoopconf`
subcommand against it, read the output, run the evaluator harness end-to-end,
and integrate into CI. Everything below is grounded in the shipped CLI flags
and real evaluator output — not aspirational.

## Prerequisites

- Docker + Docker Compose
- Python 3.10+
- `~6 GB` free disk for the Hadoop service images
- `~4 GB` free RAM for the running cluster

For development / running the test suite locally:

```bash
python3 -m venv venv
source venv/bin/activate
pip install -e '.[runtime,test]'
```

`runtime` adds Kafka, watchdog, click, and pyyaml. `test` adds pytest. The
core package is stdlib-only.

## Workflow A — Continuous monitoring (the headline mode)

This is what runs on a real cluster: agents publish constantly, the consumer
sits on the topic, drift gets caught as it happens.

### Cold-start the cluster

```bash
docker compose up -d
```

This brings up:

- 15 Hadoop / Hive / Kafka / Postgres / ZooKeeper services (from
  `docker-compose.yml`)
- 9 agent sidecars + 1 checker consumer (from
  `docker-compose.override.yml`)

Cold boot takes 60-90 seconds. Watch readiness:

```bash
docker compose ps
```

The healthchecks lag behind real readiness — `hive-server2` reports healthy
~60s before it actually serves sessions. When in doubt, wait.

### Watch the consumer

```bash
docker compose logs -f config-checker
```

You'll see lines like:

```
INFO checker.consumer: ingested namenode-hdfs-site (15 keys)
INFO checker.consumer: 0 drifts, 0 rule failures
```

When something drifts:

```
WARN checker.consumer: 1 drift, 1 rule failure
WARN checker.consumer: rule yarn-scheduler-ceiling failed: 8192 > 4096
```

### Make a config change

In another terminal:

```bash
# Bump scheduler max above NM memory — this should fire yarn-scheduler-ceiling
sed -i 's|<value>2048</value>|<value>8192</value>|' \
    conf/yarn-site.xml
```

Within `2 × CHECKER_HEARTBEAT` seconds (default 60s, override default 60s),
the `agent-resourcemanager` sidecar will detect the inotify event, re-collect
its snapshots, and publish to Kafka. The consumer will report the drift.

Restore:

```bash
sed -i 's|<value>8192</value>|<value>2048</value>|' \
    conf/yarn-site.xml
```

## Workflow B — Live status snapshot

You don't need to tail logs. `hadoopconf status` polls the topic, runs the
full pipeline against a fresh in-memory store, and exits.

```
Usage: hadoopconf status [OPTIONS]

  Query live cluster state by replaying current snapshots from Kafka.
  Reads every snapshot currently on the topic into a fresh in-memory store,
  runs the full validator + causality pipeline against it, and prints the
  result. Exits 1 if any rule fails or any drift is present; exits 0 if the
  cluster is clean.

Options:
  --bootstrap TEXT      Kafka bootstrap (overrides CHECKER_KAFKA_BOOTSTRAP).
  --topic TEXT          Snapshot topic (overrides CHECKER_TOPIC).
  --rules PATH          YAML rule file. Falls back to CHECKER_RULES_FILE.
  --graph-file PATH     YAML causality-graph file. Falls back to CHECKER_GRAPH_FILE.
  --timeout INTEGER     Seconds to wait for a complete snapshot poll.
  --format [json|text]
```

```bash
hadoopconf status \
    --rules rules/hadoop-3.3.x.yaml \
    --format text
```

The text formatter (from `cli.py`) produces:

```
Snapshots received: 14
Agents: 14 (config-agent-datanode, config-agent-hive-metastore, ...)
Services: 9 (datanode, hive-metastore, hive-server2, kafka, ...)

Rules: 5 passed, 1 failed, 1 skipped

  [CRITICAL] yarn-scheduler-ceiling: 8192 > 4096

Cross-source drift: 1
  [WARNING] yarn.scheduler.maximum-allocation-mb: xml_file:...='8192' vs env_file:...='2048'

Root causes: 1
  [CRITICAL] yarn.scheduler.maximum-allocation-mb (resourcemanager)
    → resourcemanager:container.oom.kill.risk (containers requesting more than scheduler max get killed)
```

A clean cluster prints the snapshot/agent/services lines, `Rules: N passed,
0 failed, M skipped`, and nothing else.

`--timeout` (default 30s) bounds both the metadata-discovery phase and the
message-iteration phase. If Kafka is unreachable or the topic doesn't
exist, `status` exits non-zero with a clear error rather than hanging.

For machine-readable output:

```bash
hadoopconf status --rules rules/hadoop-3.3.x.yaml --format json
```

The shape is the same as the `checker-report.json` files written by
`evaluate.sh` — see "Reading a drift report" below.

Exit code is 0 if clean, 1 if anything is dirty. Suitable for cron, alert
scripts, or a sanity check before running a workload.

## Workflow C — CI gate (stateless, no cluster)

`hadoopconf validate` is the pre-deployment check. No Kafka, no agents, no
running cluster — it parses local files and runs the rule engine against
the result.

```
Usage: hadoopconf validate [OPTIONS] CONF_DIR RULES_FILE

  Validate local config files against a YAML rule set.
  Exits 1 if any rule fails, 0 if all pass.

Options:
  --service TEXT
  --env-file TEXT
  --jvm-flags TEXT
  --jvm-flags-name TEXT
  --spark-defaults PATH
  --format [json|text]
```

Minimal use:

```bash
hadoopconf validate conf/ rules/hadoop-3.3.x.yaml --service namenode
```

Full pre-deployment check including env file, JVM flags, and Spark defaults:

```bash
hadoopconf validate conf/ rules/hadoop-3.3.x.yaml \
    --service namenode \
    --env-file conf/hadoop.env \
    --jvm-flags "$SERVICE_OPTS" \
    --jvm-flags-name SERVICE_OPTS \
    --spark-defaults conf/spark-defaults.conf \
    --format json
```

Exit codes:

- `0` — every rule passed (or skipped — skips are not failures)
- `1` — at least one rule failed

GitHub Actions example:

```yaml
- name: Validate Hadoop config
  run: |
    pip install -e '.[runtime]'
    hadoopconf validate conf/ rules/hadoop-3.3.x.yaml \
        --service namenode \
        --env-file conf/hadoop.env
```

The job fails the build if any rule fails. Skipped rules don't fail the
build — useful for the CI environment where some agents/files don't exist.

## Workflow D — One-shot replay

`hadoopconf oneshot` runs the pipeline against a single
`ConfigSnapshot` JSON file. Useful for debugging a captured snapshot
without spinning up the full stack.

```bash
hadoopconf oneshot --snapshot dump.json --rules rules/hadoop-3.3.x.yaml
```

## Workflow E — Just dump snapshots

`hadoopconf collect` parses local files and prints the resulting
`ConfigSnapshot` objects as JSON. No detection, no rules — just the
collector output. Useful for inspecting what the agent would publish.

```bash
hadoopconf collect conf/ --service namenode --env-file conf/hadoop.env
```

## Reading a drift report

Every JSON report has the same top-level shape:

```json
{
  "type": "drift_report",
  "timestamp": "2026-04-29T01:55:08Z",
  "agent_id": "resourcemanager-yarn-site",
  "drift_count": 4,
  "drifts":      [ ...DriftResult... ],
  "root_causes": [ ...RootCause... ]
}
```

`drifts` is the flat list of every problem found. `root_causes` is the same
list re-organized by drifting key, each carrying the downstream effects
from the causality graph.

A single `DriftResult`:

```json
{
  "key":      "yarn.scheduler.maximum-allocation-mb",
  "service":  "resourcemanager",
  "rule_id":  "yarn-scheduler-ceiling",
  "severity": "critical",
  "source_a": "rule:yarn-scheduler-ceiling",
  "source_b": "target:yarn.nodemanager.resource.memory-mb",
  "value_a":  "8192",
  "value_b":  "4096"
}
```

Read it as: rule `yarn-scheduler-ceiling` fired on key
`yarn.scheduler.maximum-allocation-mb` (current value `8192`) because
the target `yarn.nodemanager.resource.memory-mb` is `4096`. Severity
`critical`.

A single `RootCause`:

```json
{
  "key":      "yarn.scheduler.maximum-allocation-mb",
  "service":  "resourcemanager",
  "severity": "critical",
  "drift":    { ...DriftResult... },
  "downstream_effects": [
    "resourcemanager:container.oom.kill.risk (containers requesting more than scheduler max get killed)"
  ]
}
```

`downstream_effects` is human-readable. Each entry is `<service>:<key>` plus
the edge description in parens. The list is from
`CausalityGraph.trace()` (see [causality-graph.md](causality-graph.md)).

## Workflow F — End-to-end evaluator harness

`tests/evaluate.sh` is what produced the numbers on slide 10. It runs all
five drift scenarios sequentially: inject a known bug, wait for the
checker to detect it, restore clean config, confirm recovery, move on.

```bash
cd tests
./evaluate.sh
```

The harness:

1. Verifies the clean baseline produces zero rule failures.
2. For each scenario in `tests/configs/buggy/<name>/`:
   - Copies the buggy config files into `conf/`.
   - Polls `hadoopconf status` until the expected rule fires (timing
     captured).
   - Greps `config-checker` logs to confirm the consumer also saw it
     (log timing captured).
   - Restores `tests/configs/clean/` into `conf/`.
   - Polls `hadoopconf status` until clean again (recovery timing).
3. Writes per-scenario artefacts under
   `tests/results/<timestamp>/<scenario>/`:
   - `before.json`, `detected.json`, `post-restore.json` — `status` output
     at each phase
   - `checker-report.json` — the dirty-state report itself
   - `inject.diff` — the unified diff between clean and buggy config
   - `scenario.txt`, `timing.json`
4. Updates `tests/results/latest` symlink.

Real output from `2026-04-29T03-20-04Z`:

```
scenario                         status severity  status_ms    log_ms
----------------------------------------------------------------------
dual-source-disagreement         PASS   warning   34826        34923
hive-warehouse-wrong-namenode    PASS   warning   5377         5474
replication-exceeds-max          PASS   critical  5420         5528
scheduler-ceiling-violation      PASS   critical  5424         5522
spark-fs-mismatch                PASS   critical  5458         5560
----------------------------------------------------------------------
passed: 5   failed: 0   total: 5
heartbeat: 10s
```

A few notes on the latencies:

- `heartbeat: 10s` — `evaluate.sh` overrides `CHECKER_HEARTBEAT` to 10s for
  the run. Production default is 60s. Detection latency is approximately
  one heartbeat past the inotify event (~5s).
- The first scenario is consistently slower than the rest. Above, it's
  ~35s; in earlier runs (`2026-04-29T01-53-59Z`) it was ~10s. The variance
  is consumer cold-start: until the consumer first polls a complete cycle
  through the topic, the harness's `status` calls don't see the new
  snapshots. After the first scenario, the topic is "warm" and detection
  hits 5s steadily. This is documented and expected, not a bug.
- `severity: warning` for `dual-source-disagreement` is correct — the
  `dual-source-consistency` rule is `severity: warning`. The other
  scenarios trigger `critical` rules.

## Cluster smoke tests

The 10-test suite in `tests/run-all.sh` confirms the cluster itself is
functional, then exercises the checker end-to-end:

```bash
bash tests/run-all.sh
```

Each test is a standalone shell script. See [testing.md](testing.md) for
what each one verifies and how to add one.

## Tearing down

```bash
docker compose down       # stop services, keep volumes
docker compose down -v    # nuke everything including HDFS data
```

Add `-v` if you want a truly clean reboot. Without it, HDFS remembers its
namenode metadata across reboots, which is normally what you want.

## Common operational tasks

### Disable an agent

Edit `.env` and remove its profile from `COMPOSE_PROFILES`:

```bash
# Before
COMPOSE_PROFILES=namenode,datanode,resourcemanager,nodemanager,hive-server2,hive-metastore,spark-client,kafka,zookeeper

# After (kafka and zookeeper agents disabled)
COMPOSE_PROFILES=namenode,datanode,resourcemanager,nodemanager,hive-server2,hive-metastore,spark-client
```

Run `docker compose up -d` again. The disabled agents won't start; rules
that depend on their snapshots will skip rather than fail.

### Point at a real Kafka cluster

Edit `.env`:

```
CHECKER_KAFKA_BOOTSTRAP=kafka1.prod:9092,kafka2.prod:9092
CHECKER_TOPIC=hadoop-config-snapshots
```

Restart agents and consumer. No code changes. See
[operations.md](operations.md) for the full real-cluster deployment guide.

### Run a single smoke test

```bash
bash tests/07-checker-drift.sh
```

Each test is self-contained and exit-code-driven.

### Reload rules without restarting the consumer

Edit `rules/hadoop-3.3.x.yaml`. The `config-checker` container mounts
`./rules:/etc/checker/rules:ro`, so the file is visible to the consumer
immediately. **However**, the consumer loads rules at startup — to pick up
edits:

```bash
docker compose restart config-checker
```

Live rule reload is on the upgrade list ([limitations.md](limitations.md)).

For now the workaround is `hadoopconf status --rules <newfile>`, which
loads rules per-invocation.
