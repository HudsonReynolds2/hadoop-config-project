# Rules

Rules are declarative YAML. Operators add, change, and remove rules without
touching Python. Each rule is one block in the top-level `rules:` list of a
file like [`rules/hadoop-3.3.x.yaml`](../rules/hadoop-3.3.x.yaml). The
validator
([`checker/analysis/validator.py`](../checker/analysis/validator.py)) parses
them, runs each one against the current `SnapshotStore`, and produces one
`ValidationResult` per rule with status `pass`, `fail`, or `skip`.

This page covers the schema, the four rule types, every shipped rule, and
how to add a new one.

## Result status

```python
STATUS_PASS = "pass"   # rule evaluated and held
STATUS_FAIL = "fail"   # rule evaluated and was violated
STATUS_SKIP = "skip"   # a referenced key was absent; rule could not evaluate
```

`skip` is **not** failure. Skips happen for legitimate reasons:

- An agent isn't running (e.g. the `kafka` profile is disabled in
  `COMPOSE_PROFILES`, so no `kafka` service has any keys in the store).
- A bind-mounted file doesn't define the key.
- A rule's `target_service` has no agent yet.

Strict failure on missing keys would generate noise during partial
deployments and CI runs against minimal fixtures. The convention: if the
rule cannot be answered, it skips.

`hadoopconf validate` and `hadoopconf status` exit non-zero only when
`status == "fail"` somewhere. Skips are non-fatal.

## Rule schema

```yaml
- id: <rule-id>                # required, must be unique within the file
  description: <prose>          # required, shown in reports
  type: constraint | propagation
  severity: warning | critical  # default warning
  # ... type-specific fields ...
```

`id` and `description` are mandatory. `severity` defaults to `warning`. Any
unrecognized field is ignored, which makes incremental schema additions
safe for old YAMLs.

## The four rule kinds

The validator distinguishes them by which fields are present, not by an
explicit subtype field. This was a deliberate choice — a rule's shape
matches its purpose.

| Kind | Discriminator field | What it asserts |
| --- | --- | --- |
| **Constraint** | `type: constraint` + `relation` + `target_key` | Two keys on (possibly) different services satisfy a numeric relation |
| **Multi-service propagation** | `type: propagation` + `services: [...]` | One key has the same value across N services |
| **Cross-key propagation (must-contain)** | `type: propagation` + `must_contain_value_of` | One key's value contains another key's value as a substring |
| **Dual-source consistency** | `type: propagation` + `sources: [...]` | The same key in two different source types (XML vs env) agree |

### 1. Constraint

```yaml
- id: yarn-scheduler-ceiling
  description: scheduler max allocation must not exceed NM total memory
  type: constraint
  key: yarn.scheduler.maximum-allocation-mb
  service: resourcemanager
  relation: lte
  target_key: yarn.nodemanager.resource.memory-mb
  target_service: nodemanager
  severity: critical
```

Reads as: `<key>@<service> <relation> <target_key>@<target_service>`.

Five relations supported, mapping directly to Python `operator`:

| `relation` | Symbol | `operator` |
| --- | --- | --- |
| `lte` | `<=` | `operator.le` |
| `lt` | `<` | `operator.lt` |
| `gte` | `>=` | `operator.ge` |
| `gt` | `>` | `operator.gt` |
| `eq` | `==` | `operator.eq` |

For numeric relations the validator coerces both sides through
`int()`. Non-numeric values cause the rule to fail with a
`details=` message. For `eq`, string equality is used directly.

The `service` and `target_service` fields are looked up flexibly: the
validator first tries an exact-service match in the store, then falls back
to *any* service that has the key. This is the documented "key lookup
strategy" — bind-mounted XMLs are read by multiple services, and a strict
per-service match would generate false negatives.

On failure, the resulting `DriftResult` has:

- `key` = the rule's `key`
- `service` = the rule's `service`
- `value_a` = the actual value of `key`
- `value_b` = the actual value of `target_key`
- `source_a` = `"rule:<rule-id>"`
- `source_b` = `"target:<target_key>"`
- `severity` = the rule's `severity` (or `"warning"` if not set)
- `rule_id` = the rule's `id`

### 2. Multi-service propagation

```yaml
- id: fs-defaultfs-propagation
  description: fs.defaultFS must be identical across all services
  type: propagation
  key: fs.defaultFS
  services: [namenode, resourcemanager, nodemanager, hive-server2,
             spark-client, datanode, hive-metastore]
  severity: critical
```

Asserts that `key` has the same value across every service in `services`.

The validator uses a **strict-then-fallback** strategy:

1. For each service in `services`, do a strict per-service lookup of `key`.
2. If at least 2 services contributed a strict value, compare those.
3. For services that didn't, do a fallback lookup (any service has this key
   from a bind mount). These are sanity-checked against the strict
   agreement but don't override it.

If fewer than 2 services contributed any value (strict or fallback), the
rule **skips** rather than failing — there is nothing to compare.

The reason for this two-tier scheme: bind-mount sharing means
`spark-client` may read `core-site.xml` from a shared mount and report
`fs.defaultFS` even though Spark itself doesn't natively own that key.
Strict mode is correct for services that genuinely publish their own
config; fallback covers services that exist in the rule's `services` list
but only see the file via a mount. The combined behaviour is exactly what
the test suite (`test_rule_matrix.py`) verifies.

### 3. Must-contain (cross-key propagation)

```yaml
- id: hive-warehouse-namenode
  description: hive.metastore.warehouse.dir must reference the configured namenode
  type: propagation
  key: hive.metastore.warehouse.dir
  service: hive-server2
  must_contain_value_of:
    key: fs.defaultFS
    service: namenode
  severity: warning
```

Asserts that `key`'s value (on `service`) **contains** the referenced
key's value as a substring.

Use case: warehouse paths like `hdfs://namenode:8020/user/hive/warehouse`
must include the namenode authority `hdfs://namenode:8020`. If somebody
edits `fs.defaultFS` to `hdfs://wrongnode:9000` but leaves the warehouse
path unchanged, this rule fires.

Skip behavior: if either key is absent, skip.

### 4. Dual-source consistency

```yaml
- id: dual-source-consistency
  description: keys present in both XML files and hadoop.env must agree
  type: propagation
  sources: [xml_file, env_file]
  severity: warning
```

The simplest and broadest rule: across every `(service, key)` that appears
in **both** of the listed source types, values must agree. Implemented by
delegating to `drift_detector.detect_cross_source` (see
[detection-pipeline.md](detection-pipeline.md) §"Mode 2 — Cross-source").

The `sources:` list is currently always `[xml_file, env_file]`, but the
mechanism is general — adding `spark_conf` would also work once the
spark-client agent is wired to publish its `spark-defaults.conf`.

## All shipped rules

[`rules/hadoop-3.3.x.yaml`](../rules/hadoop-3.3.x.yaml) ships 7 rules.

| ID | Type | What it catches | Severity |
| --- | --- | --- | --- |
| `hdfs-replication-max` | constraint (lte) | `dfs.replication > dfs.replication.max` | critical |
| `yarn-scheduler-ceiling` | constraint (lte) | `yarn.scheduler.maximum-allocation-mb > yarn.nodemanager.resource.memory-mb` | critical |
| `fs-defaultfs-propagation` | propagation (multi-service) | `fs.defaultFS` disagrees across the 7 listed services | critical |
| `hive-warehouse-namenode` | propagation (must-contain) | warehouse path doesn't contain `fs.defaultFS` | warning |
| `dual-source-consistency` | propagation (sources) | XML and `hadoop.env` disagree on a shared key | warning |
| `datanode-replication-match` | propagation (multi-service) | datanode sees different `dfs.replication` than namenode | warning |
| `hive-metastore-uri-consistency` | propagation (multi-service) | `hive.metastore.uris` disagrees between hive-server2 and hive-metastore | critical |

### Rules deferred from the original plan

Two rules from the Stage 1.3 plan were **deferred** with explicit reasons,
documented in the YAML itself:

- **`kafka-zookeeper-connect`** — ZooKeeper's `zoo.cfg` has no natural
  counterpart to Kafka's `zookeeper.connect` string. A meaningful check
  would need a port-extracting comparator or a synthetic collector-side
  key, both out of scope for Stage 1. The corresponding causality-graph
  edge (`kafka:zookeeper.connect → zookeeper:clientPort`) still exists for
  reachability traces.
- **`spark-fs-defaultfs`** — `spark-defaults.conf` uses
  whitespace-separated `KEY VALUE` syntax, not `KEY=VALUE`. Adding a
  Spark-specific collector path through the agent was out of scope; today
  spark-client is covered indirectly via `fs-defaultfs-propagation`,
  which compares the bind-mounted `core-site.xml` that spark-client
  reads. See [limitations.md](limitations.md) for the gap.

## Adding a new rule

1. Pick the rule kind. Numeric relation between two keys → constraint.
   Same key across services → multi-service propagation. Substring
   reference between two keys → must-contain. Same key across source
   types → dual-source.
2. Append a YAML block to `rules/hadoop-3.3.x.yaml` (or your own rule
   file). Give it a unique `id` and a one-sentence `description`.
3. **Add the matching causality edge** if the rule represents a real
   dependency — see [causality-graph.md](causality-graph.md). Constraint
   rules pair with `constraint` edges; propagation rules pair with
   `propagation` edges. The graph contract test will fail until you also
   update the fixture and the Python defaults.
4. **Add a test entry to the rule matrix** in
   `tests/checker/test_rule_matrix.py` — see [testing.md](testing.md). The
   rule matrix asserts that each rule fires on exactly its own mutation
   and is silent on every other mutation. New rules must pass this matrix.
5. **Add a clean-config test entry** in
   `tests/checker/test_false_positives.py`. The fixture clean config must
   pass the new rule.
6. (Optional) Add an evaluation scenario under
   `tests/configs/buggy/<scenario-name>/` with a `metadata.env` declaring
   the expected rule and severity, plus the modified config files. The
   `evaluate.sh` harness picks up scenarios automatically.

The contract is: every shipped rule has a unit-test mutation that fires it,
and the clean baseline does not. If both hold, the rule is on the rails.

## Why YAML rather than Python

Three reasons:

1. **Operator extensibility.** Site-local rules don't require code changes,
   reviews, or a redeploy of the checker image — just a YAML edit and a
   reload.
2. **Auditability.** A rule is one block; its preconditions, target, and
   severity are visible without reading code.
3. **Versioning.** `hadoop-3.3.x.yaml`, `hadoop-4.0.x.yaml`, etc. could
   coexist. The validator already supports loading a different file via
   `CHECKER_RULES_FILE` or `--rules`.

## Limitations

- **No schema validation on the YAML itself.** A typo like
  `relations: lte` (plural) won't be flagged; the rule will silently
  fail to match. We mitigate this with the rule-matrix test, which fails
  if a rule stops firing on its mutation.
- **Numeric coercion is `int()`-only.** Memory values like `2g` or `2048m`
  are not supported. All values must be in matching units.
- **No rule conflict detection.** Two rules can target the same key with
  inconsistent assertions; both will fire. There is no precedence.
- **No rule grouping or precondition syntax.** A rule cannot say "only
  evaluate me if some other rule passed" or "only on production
  clusters."
- **Service tags are flat strings.** No wildcards, no inheritance.
  `services: [hive-*]` is not supported.

For proposed upgrades see [limitations.md](limitations.md).
