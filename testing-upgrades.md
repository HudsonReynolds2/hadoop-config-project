# Testing Upgrades

Gap analysis and work plan for hardening the `hadoop-config-checker` test
suite so that it proves the tool *actually* works — not just that the Python
API agrees with itself. Companion document to [plan.md](plan.md).

## Premise

199 tests pass today. They cover every collector, rule evaluator, and graph
edge at the function level, and [tests/checker/test_integration.py](tests/checker/test_integration.py)
exercises the pipeline end-to-end against mutated fixture files. That
proves the Python API detects mutations it's asked to detect. It does not
prove the productized claims in [README.md](README.md):

- "edit `conf/yarn-site.xml` — the agent's watchdog sees the change and
  re-publishes within ~1s. The checker emits a JSON drift report to stdout."
- "drop `hadoopconf validate` into a CI/CD step as a pre-deploy gate."
- "no Hadoop service is restarted or queried."

If the agent or checker container broke tomorrow, every `pytest` would
still pass. That gap is the target of this document.

---

## Gaps in the current suite

### 1. The live stack is never exercised against the checker
[tests/run-all.sh](tests/run-all.sh) smokes HDFS/YARN/Hive/Kafka but never
touches the `config-agent` / `config-checker` containers. The README's
headline demo has zero automated coverage.

### 2. The watchdog path is not tested
All integration tests call `collect_all()` directly. The agent's
`watchdog`-based re-publish on file change (Phase 2 acceptance in plan.md)
is unverified end-to-end.

### 3. Kafka is mocked
`test_agent.py` / `test_consumer.py` fake the producer/consumer. Wire
format, partition keys, consumer group rebalance are untested against a
real broker.

### 4. Rule matrix is hand-picked, not exhaustive
Each rule in [rules/hadoop-3.3.x.yaml](rules/hadoop-3.3.x.yaml) is tested
via a single mutation. This leaves holes:
- No parametrized "break one rule, assert **only** that rule fails" test —
  could mask a rule that fires spuriously on every mutation.
- No false-positive tests. The plan calls out `vmem-check-enabled=false`
  and dual-source `hadoop.env` as intentionally redundant; nothing asserts
  those don't produce noise.
- [`hive-warehouse-namenode`](checker/analysis/validator.py#L381) uses a
  naive `ref_val in val` substring match; no test for trailing-slash,
  scheme-difference, or "contained as substring of an unrelated path"
  cases.
- Constraint rules silently skip when a key is missing ([validator.py:194-209](checker/analysis/validator.py#L194-L209))
  and report `passed=True`. Users can't distinguish "skipped" from "passed."

### 5. JVM-flags (Hive) path has no integration coverage
Phase 1 explicitly lists `parse_jvm_flags` because Hive ships config via
`SERVICE_OPTS`. There are unit tests for the parser, but no integration
test where a JVM-flags snapshot participates in `fs-defaultfs-propagation`
alongside XML snapshots — which is the original motivation.

### 6. Causality-graph edge set has no contract test
Edges are seeded in Python ([causality_graph.py](checker/analysis/causality_graph.py)).
Nothing regresses if an edge is removed, as long as one remaining edge
still mentions "hive" or "spark".

### 7. CLI exit code is only tested in-process
`validate`'s exit 1 on failure is the CI-gate promise. There's no
subprocess-level test — only Click's `CliRunner`, which can hide
packaging / entry-point breakage.

### 8. No silent-agent / stale-snapshot detection
Heartbeat is advertised in the README but no rule/test detects "agent X
hasn't published in 3 × heartbeat". This is both a missing test and a
missing feature.

---

## Proposed work

### Tier A — prove it works on the real stack

**A1. `tests/07-checker-drift.sh`** added to `run-all.sh`.
- Assert `config-agent` and `config-checker` are up.
- Capture baseline `docker compose logs checker` offset.
- Mutate `conf/yarn-site.xml` (flip `yarn.scheduler.maximum-allocation-mb`
  to `9999`).
- Poll `docker compose logs --since` for up to 10s; assert a drift JSON
  mentioning `yarn-scheduler-ceiling` or `temporal-drift` appears.
- Restore the file; assert a new snapshot flows through.
- Assert no drift appears in an untouched 90s window (no heartbeat noise).

**A2. `tests/checker/test_watchdog_live.py`** — subprocess + real Kafka.
- Spawn `checker.agent` against a tmp `HADOOP_CONF_DIR`, pointing at a
  testcontainers Kafka (or skip if unavailable).
- Modify a file; consume the topic; assert the new snapshot arrives < 2s.
- Verify heartbeat re-publish without a file change.

### Tier B — close rule-matrix and false-positive gaps (in-process, cheap)

**B3. `tests/checker/test_rule_matrix.py`** — one parametrized test per
rule. For each rule, define a mutation that should break *only* that rule,
run the full pipeline, assert `passed=False` on that rule and
`passed=True` on every other rule. Catches rules that fire spuriously.

**B4. `tests/checker/test_false_positives.py`**:
- Baseline config → zero validator failures (regression anchor).
- Re-load the baseline 10× through `process_snapshot` — heartbeats
  produce zero temporal drift.
- Comment-only XML edit → zero drift (parser stable against formatting).
- `hive-warehouse-namenode` with a warehouse path that incidentally
  contains a namenode-like substring (e.g. `hdfs://notnamenode:8020`
  vs `hdfs://namenode:8020`) — probe for substring-match false positives.
  **Expected outcome:** current substring match is too loose; tighten to
  URL-authority comparison.

**B5. `tests/checker/test_missing_keys.py`** — for every rule type, run
with the referenced key deleted from the fixture. Assert the result is
distinguishable from a pass.
- **Tool gap exposed:** `ValidationResult` has no `status: pass|fail|skip`.
  Recommend adding it so skipped rules aren't reported as passes.

### Tier C — contract tests for things that quietly rot

**C6. `tests/checker/test_graph_contract.py`** — data-driven. Loads the
edge table from a fixture (derived from plan.md) and asserts
`CausalityGraph()` seeds exactly that set. Forces plan and code to stay
in sync.

**C7. `tests/checker/test_cli_subprocess.py`** — calls
`subprocess.run(["hadoopconf", "validate", ...])` against clean and
mutated configs. Asserts exit codes 0 and 1. Catches entry-point /
packaging breakage.

**C8. `tests/checker/test_jvm_flags_propagation.py`** — builds a store
with one XML snapshot for `namenode` and one `jvm_flags` snapshot for
`hive-server2`, runs `fs-defaultfs-propagation`; asserts the rule agrees
when they match and fails when they don't. This is the Hive case — the
whole reason the env collector has two parsers.

### Tier D — tool gaps that writing tests surfaced

- Tighten `hive-warehouse-namenode` match semantics (URL authority, not
  substring).
- Add `status: pass|fail|skip` to `ValidationResult`.
- Add a silent-agent rule that flags any `agent_id` whose last snapshot
  is older than `2 × CHECKER_HEARTBEAT`.

---

## Execution order

1. Tier B (B3, B4, B5) — fast, in-process, exposes real weaknesses today.
2. Tier C (C6, C7, C8) — low cost, catches regressions that would
   otherwise be silent.
3. Tier A1 — the live-stack smoke is the bash version of "prove it works
   on a buggy Hadoop env," so this is the bridge to the user's next task.
4. Tier A2 — only if testcontainers is acceptable as a dev dep.
5. Tier D — address as follow-ups, once the tests surface the behaviour
   to change.
