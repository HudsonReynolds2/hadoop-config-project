# dual-source-disagreement

## Bug

The cluster runs with two sources of truth: `./conf/*-site.xml` files
that services read, and `./conf/hadoop.env` that some Docker entrypoints
read to populate environment variables. We mutate `hadoop.env` so its
`CORE-SITE.XML_fs.defaultFS` disagrees with the XML.

This is the classic dual-source bug. Some services read XML, some honor
the env var. The cluster comes up in a half-broken state where `hdfs`
client commands and Spark may resolve to different NameNodes.

## Why this triggers

Rule `dual-source-consistency`:

```yaml
type: propagation
sources: [xml_file, env_file]
```

The drift detector's `detect_cross_source` finds `fs.defaultFS` published
by the XML collector with one value and the env collector with another,
and emits a warning.

## Files mutated

- `hadoop.env` — only the `CORE-SITE.XML_fs.defaultFS` line changed.

## Expected behaviour

- `hadoopconf status` reports `dual-source-consistency` failed, severity
  warning.
