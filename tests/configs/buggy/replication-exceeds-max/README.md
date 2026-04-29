# replication-exceeds-max

## Bug

`dfs.replication` is raised to 5, exceeding `dfs.replication.max` of 3.
The HDFS NameNode would silently clamp to the max — files end up with
replication=3, not the requested 5 — which causes confusion when
operators check actual block placement against the configured policy.

## Why this triggers

Rule `hdfs-replication-max` is a `constraint` rule:

```yaml
relation: lte
key: dfs.replication
target_key: dfs.replication.max
```

`5 <= 3` is false → critical violation.

## Files mutated

- `hdfs-site.xml` — only `dfs.replication` changed.

## Expected behaviour

- `hadoopconf status` reports `hdfs-replication-max` failed, severity
  critical.
- Restored cleanly within one heartbeat.

## Note on baseline

The clean baseline now includes `dfs.replication.max=3`. Without it,
the constraint rule has nothing to compare against and would skip.
