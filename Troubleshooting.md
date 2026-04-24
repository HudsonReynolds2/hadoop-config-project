# Troubleshooting

Common issues with the fixture Hadoop cluster and the checker tool itself.
If you hit something not listed here, `docker compose logs <service>` is
almost always the right first step.

## Cluster startup

### NameNode stays unhealthy

The first-boot HDFS format can take up to 60s on a cold volume. Give the
healthcheck its `start_period`. If it genuinely fails to come up:

```bash
docker compose logs namenode
```

Check for `ENSURE_NAMENODE_DIR` permission errors on the named volume — on
some Docker setups (older Docker Desktop on Windows in particular) the volume
is created with restrictive ownership. `docker compose down -v` and retry
usually fixes it.

### `hive-schema-init` fails with "database not ready"

The Postgres healthcheck sometimes reports ready before `init.sql` has
finished creating the `hive` user and `metastore` database. The init service
exits with an error when this races.

Fix: wait for Postgres to stabilise, then re-run just the init:

```bash
docker compose up -d hive-schema-init
```

### `beeline` / test 03 returns nothing on a cold cluster

HiveServer2 reports `(healthy)` well before it's actually serving sessions —
it takes 60–90 seconds past the healthcheck to finish loading session state.
If `tests/03-hive.sh` fails on a cold cluster, wait 60s and rerun it.

## Runtime / workload failures

### SparkPi fails with "Application application_xxx failed 2 times"

Almost always a memory issue. Check the NodeManager logs:

```bash
docker compose logs nodemanager
```

The common culprit is executor memory exceeding
`yarn.scheduler.maximum-allocation-mb` (2048 MB in the shipped config).
Either lower the request, or raise both of these together (never one without
the other):

- `yarn.nodemanager.resource.memory-mb`
- `yarn.scheduler.maximum-allocation-mb`

This is exactly what the `yarn-scheduler-ceiling` rule in
`rules/hadoop-3.3.x.yaml` catches.

### YARN containers get killed mid-job

Docker cgroup vmem accounting is unreliable. `yarn.nodemanager.vmem-check-enabled`
is set to `false` in `conf/yarn-site.xml` for exactly this reason. If you
re-enable it, Spark containers will start getting killed with
`Container killed on request. Exit code is 143`.

**This is a documented footgun, not a config-checker bug.** If you add a rule
that forbids `vmem-check-enabled=false`, make sure it only fires outside Docker.

### NM refuses larger jobs

Default cap is 4 GB total, 2 GB per container. Bump
`yarn.nodemanager.resource.memory-mb` and `yarn.scheduler.maximum-allocation-mb`
together in `conf/yarn-site.xml` and recreate the YARN services:

```bash
docker compose up -d --force-recreate resourcemanager nodemanager
```

## Checker tool

### Agent logs `could not connect to Kafka admin for topic check`

The agent tries to auto-create its topic on startup. If Kafka isn't reachable
yet, you'll see this warning — it's not fatal. The producer will retry on
publish. If it persists after Kafka is healthy, check `CHECKER_KAFKA_BOOTSTRAP`
and the `hadoop-net` bridge:

```bash
docker compose logs agent | grep -i kafka
docker compose exec agent python -c "import socket; print(socket.gethostbyname('kafka'))"
```

### Consumer reports nothing on file change

Sanity-check the path the agent is watching:

```bash
docker compose logs agent | grep 'watching'
```

The override file mounts `./conf/*.xml` individually (not the whole `conf/`
directory) because Docker bind-mounts of individual files don't always
propagate inotify events on every host filesystem. If your edits aren't
being seen, confirm inside the container:

```bash
docker compose exec agent ls -la /opt/hadoop/etc/hadoop/
```

The heartbeat (`CHECKER_HEARTBEAT=60` by default) will still re-publish
everything on its interval even if the watcher misses an event.

### `hadoopconf` command not found

You installed the package but the entry point isn't on `PATH`. Either
re-activate the venv or invoke as a module:

```bash
source .venv/bin/activate
# or
python -m checker.cli <subcommand>
```

### Validator says "rule passed" for a rule you thought was broken

The validator looks up keys across the entire snapshot store, not just for
the service named in the rule. This is intentional — bind-mounted XMLs are
read by multiple services, and a strict per-service match would produce false
negatives.

See the module docstring in `checker/analysis/validator.py` ("Key lookup
strategy") for the full logic.

### `pytest tests/checker/` fails before any test runs

Missing test deps. Install the `test` extra:

```bash
pip install -e '.[runtime,test]'
```

`[runtime]` is also needed — a couple of integration tests exercise the
Kafka-using code paths with `kafka-python` mocked, but the imports still
have to resolve.

## Complete tear-down

When something is deeply wrong and you want a clean slate:

```bash
docker compose down -v              # stops + removes volumes
docker system prune -f              # optional: reclaim space
docker compose build --no-cache     # rebuild custom images from scratch
docker compose up -d
```

Expect 3–5 minutes on the first boot after a full nuke.
