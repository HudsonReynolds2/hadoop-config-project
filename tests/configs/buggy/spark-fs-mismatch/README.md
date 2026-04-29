# spark-fs-mismatch

## Bug

`spark.hadoop.fs.defaultFS` in `spark-defaults.conf` is set to
`hdfs://wrongnamenode:8020`, while the cluster's `fs.defaultFS` (in
`core-site.xml` on namenode) is `hdfs://namenode:8020`. A Spark job
launched with these defaults would attempt to read/write to the wrong
NameNode — either failing immediately or, worse, silently misrouting if
a host with that name resolves elsewhere.

## Why this triggers

Rule `spark-fs-defaultfs` is a `propagation` cross-key rule (Stage 3
addition):

```yaml
key: spark.hadoop.fs.defaultFS
service: spark-client
target:
  key: fs.defaultFS
  service: namenode
```

The validator's `_eval_propagation_cross_key` looks up
`spark.hadoop.fs.defaultFS` from the spark-client agent's spark-conf
snapshot and compares it (string equality) against `fs.defaultFS` from
the namenode's core-site.xml. They differ → critical violation.

## Files mutated

- `spark-defaults.conf` — only the `spark.hadoop.fs.defaultFS` line.

## Expected behaviour

- The spark-client agent (and only that agent) sees the change via
  inotify on `./conf/spark-defaults.conf`. It re-collects and publishes
  the new spark-conf snapshot.
- `hadoopconf status` reports `spark-fs-defaultfs` failed, severity
  critical.
- Streaming consumer also emits a drift report referencing the rule.

## Note

This scenario is the headline test for Stage 3's spark-conf integration:
it requires (a) the `spark_collector` to parse the file, (b) a separate
source tag (`spark_conf`) so the validator's source-preference doesn't
prefer the wrong source, and (c) the `agent-spark-client` service to
have `CHECKER_SPARK_DEFAULTS_FILE` set in its environment.
