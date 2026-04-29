# Buggy scenarios

Each subdirectory is a self-contained "inject this misconfiguration and
make sure the checker catches it" test. The eval harness
(`tests/evaluate.sh`) iterates them in alphabetical order and produces a
results bundle under `tests/results/<UTC-timestamp>/`.

## Scenario layout

```
tests/configs/buggy/<scenario-name>/
  metadata.env       required, sourced by the harness
  README.md          human-readable explanation
  <conf-file>        one or more files matching ./conf/* — these
                     replace the matching baseline files for the run.
                     Files not listed remain at the baseline.
```

### `metadata.env`

Standard KEY=VALUE shell-sourceable file. Required keys:

```sh
SCENARIO_DESCRIPTION="One sentence description of the bug."
EXPECTED_RULE=<rule_id_from_rules/hadoop-3.3.x.yaml>
EXPECTED_SEVERITY=<warning|critical>
EXPECTED_DOWNSTREAM=<comma-separated list of services, may be empty>
```

`EXPECTED_DOWNSTREAM` is checked against the causality graph trace
(`root_causes[].downstream_effects`). Set empty to skip that check.

## Current scenarios

1. **scheduler-ceiling-violation** — `yarn.scheduler.maximum-allocation-mb >
   yarn.nodemanager.resource.memory-mb`. Constraint rule.
2. **replication-exceeds-max** — `dfs.replication > dfs.replication.max`.
   Constraint rule.
3. **hive-warehouse-wrong-namenode** — `hive.metastore.warehouse.dir`
   references a different host than `fs.defaultFS`. Must-contain rule.
4. **dual-source-disagreement** — `hadoop.env` and `core-site.xml`
   disagree on `fs.defaultFS`. Cross-source rule.
5. **spark-fs-mismatch** — `spark.hadoop.fs.defaultFS` in
   `spark-defaults.conf` disagrees with `fs.defaultFS` on namenode.
   Cross-key propagation rule (Stage 3 spark-conf integration).

## Scenarios that are deliberately omitted

Two scenarios from the original Stage 3 plan are not implemented because
they cannot be reproduced honestly with the current
shared-configuration topology:

- **fs-defaultfs-mismatch** (multi-service propagation). All XML agents
  bind-mount the same `./conf/`, so a single mutation is seen by every
  agent identically; they all agree on the wrong value, so the
  multi-service propagation rule passes. The same mutation does
  surface as a `dual-source-consistency` failure (covered by scenario
  4). To genuinely test multi-service propagation, the test harness
  would need to mutate just one agent's view — e.g. a per-agent conf
  overlay — which is a topology change beyond Stage 3's scope.

- **kafka-zk-wrong** (must_reference rule). The kafka and zookeeper
  agents publish their config via JVM flags read from environment
  variables at agent startup. There is no on-disk file to mutate that
  would propagate through inotify. Reproducing this scenario would
  require restarting the agent with a different env var, which the
  harness intentionally avoids (it would conflate restart latency
  with detection latency). The `must_reference` rule and its
  evaluator are unit-tested directly in
  `tests/checker/test_must_reference.py`.

## Authoring a new scenario

1. `mkdir tests/configs/buggy/<name>`
2. Copy the relevant baseline file(s) from `./conf/` (or
   `tests/configs/clean/` for reference) into the new directory and
   apply the minimum mutation needed to trigger your target rule. Don't
   include files you don't change — the harness restores the entire
   `./conf/` after each run regardless, but keeping each scenario
   minimal makes the diff easier to read.
3. Write `metadata.env`.
4. Write `README.md` explaining the bug and which rule should fire.
5. Run `tests/evaluate.sh` and confirm your scenario passes.

## `tests/configs/clean/`

A reference snapshot of the baseline `./conf/` tree at the time the
scenarios were authored. The harness does NOT use this directory for
restoration — it snapshots whatever is in `./conf/` at startup and
restores from that snapshot, so any local changes to your `./conf/`
roundtrip cleanly. The `clean/` reference is just a sanity check
("what does a known-good config look like?") and a place to diff
against when authoring new scenarios.
