# hive-warehouse-wrong-namenode

## Bug

`hive.metastore.warehouse.dir` is set to `hdfs://wronghost:8020/user/hive/warehouse`
while `fs.defaultFS` on namenode is `hdfs://namenode:8020`. Hive would
attempt to write tables to a NameNode that doesn't exist in this cluster,
either failing immediately (DNS resolution) or silently misrouting if a
host with that name exists elsewhere.

## Why this triggers

Rule `hive-warehouse-namenode` is a `propagation` rule with
`must_contain_value_of`. The validator's `_eval_must_contain` does
URL-authority comparison (host:port match), so `hdfs://wronghost:8020`
and `hdfs://namenode:8020` are correctly flagged as different even though
they share scheme and port.

## Files mutated

- `hive-site.xml` — only the warehouse dir line changed.

## Expected behaviour

- `hadoopconf status` reports `hive-warehouse-namenode` failed, severity
  warning.
