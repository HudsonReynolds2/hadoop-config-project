# hadoop-stack

Full Hadoop/Spark/Hive/Kafka/ZooKeeper docker-compose cluster for the
config-propagation checker project. Baseline-before-tool.

## What's in here

| Service             | Image                       | Ports (host)       | Purpose                                      |
|---------------------|-----------------------------|--------------------|----------------------------------------------|
| zookeeper           | zookeeper:3.9.2             | 2181               | Real ZK for config-drift tests               |
| kafka               | apache/kafka:3.8.0          | 9092               | KRaft combined mode (controller+broker)      |
| postgres-metastore  | postgres:16                 | —                  | Hive metastore backing DB                    |
| namenode            | apache/hadoop:3.3.6         | 9870, 8020         | HDFS NN                                      |
| datanode            | apache/hadoop:3.3.6         | 9864               | HDFS DN                                      |
| resourcemanager     | apache/hadoop:3.3.6         | 8088, 8032         | YARN RM                                      |
| nodemanager         | apache/hadoop:3.3.6         | 8042               | YARN NM                                      |
| hive-metastore      | hadoop-stack-hive:local     | 9083               | Hive Thrift metastore                        |
| hive-server2        | hadoop-stack-hive:local     | 10000, 10002       | Hive JDBC/Thrift server                      |
| spark-master        | apache/spark:3.5.3          | 8080, 7077         | Spark standalone master                      |
| spark-worker        | apache/spark:3.5.3          | 8081               | Spark standalone worker                      |
| spark-client        | apache/spark:3.5.3          | —                  | Idle container for spark-submit --master yarn|
| hdfs-init           | apache/hadoop:3.3.6         | —                  | One-shot: create HDFS dirs                   |
| spark-archive-init  | apache/spark:3.5.3          | —                  | One-shot: upload spark jars to HDFS          |
| hive-schema-init    | hadoop-stack-hive:local     | —                  | One-shot: schematool -initSchema             |

## Files

```
├── docker-compose.yml           15 services, named volumes, healthchecks
├── hadoop.env                   env vars for apache/hadoop (redundant with XMLs by design)
├── Dockerfile.hive              apache/hive:4.0.0 + Postgres JDBC 42.7.3
├── conf/
│   ├── core-site.xml            fs.defaultFS + proxyuser
│   ├── hdfs-site.xml            NN/DN paths, webhdfs on, permissions off
│   ├── yarn-site.xml            RM addrs, NM 4GB/2 cores, vmem-check off
│   ├── mapred-site.xml          yarn framework, classpath, MR memory
│   ├── hive-site.xml            Postgres metastore, HDFS warehouse
│   └── spark-defaults.conf      master=yarn, yarn.archive pointer
├── postgres/init.sql            creates hive user + metastore db
└── tests/
    ├── 01-hdfs.sh               1MB round-trip byte match
    ├── 02-yarn-pi.sh            MapReduce pi via `hadoop jar`
    ├── 03-hive.sh               create/insert/select/drop via beeline
    ├── 04-kafka.sh              produce 10, consume 10
    ├── 05-zookeeper.sh          znode create/get/delete via zkCli
    ├── 06-spark-yarn.sh         SparkPi --master yarn
    └── run-all.sh               run everything, summarize
```

## Bring it up

```bash
# 1. Ensure Docker Desktop WSL integration is on, or docker engine is running in WSL.
#    Verify:
docker version
docker compose version

# 2. Build the custom Hive image (~1 min first time, Postgres JDBC download).
docker compose build

# 3. Start everything. Init services run once and exit; long-running ones stay up.
docker compose up -d

# 4. Watch startup — should take 2–4 minutes on a cold cluster.
docker compose ps
docker compose logs -f namenode resourcemanager hive-metastore hive-server2
```

Healthchecks gate startup. When `docker compose ps` shows every long-running
service as `(healthy)` and the init services as `Exited (0)`, you're ready.

## Run the tests

```bash
cd tests && ./run-all.sh
```

Expected output: 6 PASS lines. Total runtime ~2–3 min (SparkPi dominates).

## Web UIs

| UI                   | URL                          |
|----------------------|------------------------------|
| HDFS NameNode        | http://localhost:9870        |
| YARN ResourceManager | http://localhost:8088        |
| DataNode             | http://localhost:9864        |
| NodeManager          | http://localhost:8042        |
| Hive HS2             | http://localhost:10002       |
| Spark Master         | http://localhost:8080        |
| Spark Worker         | http://localhost:8081        |

## Tear down

```bash
docker compose down            # keep volumes, restart picks up state
docker compose down -v         # also nuke volumes (fresh cluster next time)
```

## Design notes (read before modifying)

**Configs are bind-mounted from `./conf/`.** Every Hadoop-family service mounts
the four XMLs read-only over `/opt/hadoop/etc/hadoop/`. `hadoop.env` is kept
for redundancy and deliberately sets the same values a second time — the
future config-checker can treat this dual-source pattern as a known
propagation-conflict scenario to detect.

**Spark-on-YARN uses a pre-staged archive.** `spark-archive-init` zips
`/opt/spark/jars` and uploads to `hdfs:///user/spark/spark-jars.zip` once.
Then `conf/spark-defaults.conf` sets `spark.yarn.archive` to that path so
every submit skips the 30s re-upload.

**Hive uses Postgres, not embedded Derby.** Metastore and HS2 are separate
services reading the same backing DB. This gives the checker three distinct
points to probe: HS2 config, HMS config, Postgres schema version.

**YARN vmem check is disabled.** Docker cgroup vmem accounting is unreliable;
without this, Spark containers get killed mid-job. This is a documented
footgun, not a config-checker bug to flag.

**Memory sizing.** NodeManager caps at 4 GB total, 2 GB per container. YARN
will refuse larger jobs. If you need more, bump `yarn.nodemanager.resource.memory-mb`
in `yarn-site.xml` and `yarn.scheduler.maximum-allocation-mb` together (both
or neither).

## Common issues

- **`namenode` stays unhealthy**: first-boot format can take 60s. Give it
  `start_period` time. If it genuinely fails, `docker compose logs namenode`
  and check `ENSURE_NAMENODE_DIR` permissions on the volume.
- **`hive-schema-init` fails with "database not ready"**: the Postgres
  healthcheck sometimes passes before `init.sql` has finished. Re-run
  `docker compose up -d hive-schema-init` once Postgres is definitely ready.
- **SparkPi fails with "Application application_xxx failed 2 times"**:
  usually a memory issue. Check NM logs: `docker compose logs nodemanager`.
  The common culprit is executor memory > `yarn.scheduler.maximum-allocation-mb`.
- **`beeline` returns nothing**: HS2 takes 60–90s past "healthy" to finish
  loading session state. If 03 fails on a cold cluster, wait 60s and rerun.

## Port map for the config-checker tool

The future tool will want to reach these programmatically:

| Endpoint                                             | What it gives                         |
|------------------------------------------------------|---------------------------------------|
| `http://namenode:9870/jmx`                           | NN JMX, includes effective HDFS config|
| `http://namenode:9870/conf`                          | NN's parsed core+hdfs-site            |
| `http://resourcemanager:8088/conf`                   | RM's parsed yarn-site                 |
| `http://resourcemanager:8088/ws/v1/cluster/info`     | RM state + version                    |
| `http://nodemanager:8042/conf`                       | NM's parsed yarn-site                 |
| `http://hive-server2:10002/conf`                     | HS2's parsed hive-site                |
| `jdbc:hive2://hive-server2:10000`                    | SQL access to metastore               |
| `thrift://hive-metastore:9083`                       | Metastore Thrift (programmatic)       |
| `jdbc:postgresql://postgres-metastore:5432/metastore`| Raw metastore schema                  |
| `localhost:2181` (zkCli or kazoo)                    | ZK znodes                             |
| `localhost:9092` (kafka clients)                     | Kafka metadata                        |

Every `/conf` endpoint returns the *runtime-effective* config the service
actually loaded — which is what you diff against the XMLs in `./conf/` to
detect propagation failures.
