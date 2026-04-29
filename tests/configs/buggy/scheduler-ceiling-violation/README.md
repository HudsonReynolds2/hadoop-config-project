# scheduler-ceiling-violation

## Bug

`yarn.scheduler.maximum-allocation-mb` is raised to 8192, exceeding the
NodeManager's `yarn.nodemanager.resource.memory-mb` of 4096. YARN will
reject any container request larger than the NM's actual memory, so
this is a "looks fine in config, breaks at submit time" bug.

## Why this triggers

Rule `yarn-scheduler-ceiling` is a `constraint` rule:

```yaml
relation: lte
key: yarn.scheduler.maximum-allocation-mb
target_key: yarn.nodemanager.resource.memory-mb
```

`8192 <= 4096` is false → critical violation.

## Files mutated

- `yarn-site.xml` — only the one key changed, everything else
  identical to baseline.

## Expected behaviour

- `hadoopconf status` reports `yarn-scheduler-ceiling` as failed,
  severity critical.
- The streaming consumer also emits a drift report referencing the
  same rule.
- After restoring the clean baseline, status returns to clean within
  one heartbeat (file-watch is on yarn-site.xml so detection is sub-second).
