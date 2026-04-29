# Limitations and future upgrades

What the tool doesn't do, organized by component, with concrete proposals
for what the next team could pick up. Each item is something a careful
operator should know about before relying on the tool in a real
deployment.

## Known bugs (fix-now items)

These are real defects shipped in 0.1.0, not architectural limitations.

### `agent-spark-client` does not publish `spark-defaults.conf`

The collector exists (`spark_collector.parse_spark_conf`), the agent reads
`CHECKER_SPARK_DEFAULTS_FILE` if set, and the CLI accepts
`--spark-defaults`. But `docker-compose.override.yml` does not set the env
var on the running spark-client agent, so spark-defaults snapshots reach
the consumer only via CLI (`hadoopconf validate --spark-defaults`).

**Fix.** Add to the spark-client agent's environment in the override file:

```yaml
environment:
  CHECKER_SPARK_DEFAULTS_FILE: "/opt/spark/conf/spark-defaults.conf"
volumes:
  - ./conf/spark-defaults.conf:/opt/spark/conf/spark-defaults.conf:ro
```

Then enable the deferred `spark-fs-defaultfs` rule in
`hadoop-3.3.x.yaml`.

## Per-component limitations

### Agents

| Limitation | Impact | Proposed upgrade |
| --- | --- | --- |
| inotify only on directories, not files | Single-file bind mounts can miss events on macOS / WSL2 | Already mitigated: directory mounts in override + heartbeat fallback |
| No content-hash dedup | Same content rewritten to disk republishes a snapshot | Hash properties dict, skip publish if unchanged |
| In-flight events lost on restart | An inotify event during agent crash is dropped | Acceptable: heartbeat re-publishes within `CHECKER_HEARTBEAT` |
| No auth to Kafka | Plaintext on `hadoop-net`, agents and broker trust each other | Wire SASL/SSL via env vars; `kafka-python` supports both |
| One agent per service | If an agent crashes, snapshots go stale | Run two agents; `SnapshotStore` is last-write-wins so duplicates are harmless |
| JVM-flag agents have stale config in YAML | Hive/Kafka/ZK agents replicate `SERVICE_OPTS` as a literal `CHECKER_JVM_FLAGS` string in the override file. If the service's own env changes and the agent's doesn't, drift is invisible. | Read the actual env from `docker compose config` at startup and dedup |

### Consumer

| Limitation | Impact | Proposed upgrade |
| --- | --- | --- |
| Single instance, no HA | Consumer crash halts drift detection | `CHECKER_CONSUMER_GROUP` is set; run two replicas, Kafka handles failover |
| No persistence | `SnapshotStore` is in-memory only; restart re-reads from Kafka | Acceptable given Kafka retention covers the gap; or persist to disk for cold start |
| Rule reload requires restart | `CHECKER_RULES_FILE` is read at startup | Watch the rules file with watchdog; reload on change |
| Alerts emit to a Kafka topic only | `CHECKER_EMIT_ALERTS=true` publishes drift_reports back to Kafka. No HTTP webhook, no PagerDuty, no Slack | Add a pluggable sink interface; one implementation per integration |
| No Prometheus metrics | Operators have to grep logs for cluster health | Expose `/metrics` from the consumer with rules-passed/failed/skipped counters |

### Rule engine

| Limitation | Impact | Proposed upgrade |
| --- | --- | --- |
| No YAML schema validation | Typo like `relations:` (plural) silently fails to match | JSON Schema for rule files; validate at load |
| Numeric coercion is `int()` only | `2g`, `2048m` etc. don't work | Add a memory-string parser shared with the constraint evaluator |
| No rule conflict detection | Two rules can target the same key with contradictory assertions | Static check at load time |
| No rule grouping or preconditions | Can't say "only evaluate on prod" or "only if rule X passed" | Add a `when:` clause that's a key=value match against the cluster |
| Service tags are flat | No wildcards (`hive-*`), no inheritance | `services: ["hive-*"]` glob support |
| Rules are version-blind | A rule that's wrong on Hadoop 4 still runs against a Hadoop 4 cluster | Add `applies_to: ["3.3", "3.4"]` field |

### Causality graph

| Limitation | Impact | Proposed upgrade |
| --- | --- | --- |
| Hand-coded edges | Drifts from reality on Hadoop version upgrades | Edge inference from operational traces (read real Hadoop docs metadata, validate against cluster behavior) |
| Influence vs constraint indistinguishable in report | Operator reading downstream effect can't tell hard rule from heuristic | Preserve `edge_type` in `downstream_effects` JSON, color in UI |
| Synthetic nodes opaque | Nodes like `container.oom.kill.risk` exist only in graph; never receive drifts | Mark them with a `synthetic: true` flag; UI can render differently |
| Fallback service-match can confuse | Drift tagged `resourcemanager` traces from `namenode:dfs.replication` | Add the resolved service to the report, e.g. `resolved_via: "namenode"` |
| BFS is unweighted | No edge-distance attenuation; long downstream lists may overstate impact | Add an optional `confidence` field per edge; weight transitively |
| No cycle reporting | Cycles terminate but aren't surfaced | Detect SCCs at load and warn |

### CLI

| Limitation | Impact | Proposed upgrade |
| --- | --- | --- |
| JSON output only, no UI | Operators reading reports need to parse JSON | Web UI showing live cluster state, drift history, rule violations |
| No diff view between snapshots | Can't see "what changed since 9am" easily | `hadoopconf diff <agent_id> --since 9am` reading from Kafka history |
| No per-rule disable flag | Have to comment out rules in YAML | `--disable-rule rule_id` flag |
| `oneshot` reads JSON only | Can't replay from a Kafka offset directly | Add `--from-offset` |

### Tests

| Limitation | Impact | Proposed upgrade |
| --- | --- | --- |
| Silent-agent only unit-tested | Not exercised end-to-end against a real cluster | Add scenario 06: kill an agent, assert silent-agent fires within heartbeat window |
| No chaos / network-partition tests | Tool's behavior under broken Kafka, dropped messages, etc. is unspecified | Toxiproxy in front of Kafka in compose; scenarios that drop / delay packets |
| No Hadoop-version matrix | Rules are pinned to 3.3.x; nothing tests against 3.4 / 4.x | CI matrix with multiple Hadoop versions; rule files per version |
| No load tests | Behavior at 100+ agents not measured | Synthetic agent load generator |

### Compose / deployment

| Limitation | Impact | Proposed upgrade |
| --- | --- | --- |
| Single broker | No replication, no quorum | Documented "for production, point at existing Kafka cluster" |
| Plaintext Kafka | No auth, no TLS | Add SASL/SSL env vars, default off, document |
| No schema registry | Adding fields to `ConfigSnapshot` breaks old consumers | Either schema-registry integration or a `version:` field with backward-compatible parsing |
| Bind-mounted rules | Live edit visible inside container, but consumer doesn't reload | See "Rule reload requires restart" above |
| `tests/results/` grows unbounded | Each `evaluate.sh` run adds a directory; no cleanup | `tests/results/` should rotate, or evaluator should `--keep N` |

## Out-of-scope decisions

These are deliberate non-goals, not deficiencies:

- **Not a runtime checker.** This tool inspects configuration. It does not
  watch process state, network reachability, JVM heap, or HDFS quotas.
  Those problems already have tools (Cloudera Manager, Ambari, Prometheus
  exporters).
- **Not an alerting system.** Drift reports get emitted; routing them to
  PagerDuty / Slack / email is left to the operator. The Kafka alerts
  topic exists as the integration surface.
- **Not a compliance tool.** Rules describe "what's consistent" not
  "what's compliant". A rule like `kerberos-must-be-enabled` would be
  trivial to add but isn't shipped.

## Roadmap by priority

If the next team picks this up, recommended order:

1. **Wire spark-defaults to the agent.** Removes a documented gap and
   enables the deferred `spark-fs-defaultfs` rule.
2. **YAML schema validation for rules.** Cheap, prevents silent typos.
3. **Prometheus exporter on the consumer.** Operational hygiene; connects
   to existing observability.
4. **Web UI / dashboard.** Highest user-visible impact; the JSON-only
   output is the most common complaint.
5. **Rule reload on file change.** Quality-of-life; reduces "edit rules
   then restart consumer" cycle.
6. **Hadoop 4 rule pack + version-aware rules.** The headline expansion.
7. **Edge inference from operational traces.** The research-grade
   upgrade — turn the causality graph from hand-coded to learned.

The first three are low-risk and add immediate value. Items 4-6 are
larger projects. Item 7 is a research contribution worth its own paper.
