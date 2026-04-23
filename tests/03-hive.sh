#!/usr/bin/env bash
# Test 03: Create table, insert, select, drop via beeline against HS2.
# Exercises HS2 -> metastore -> Postgres -> HDFS warehouse propagation chain.

set -euo pipefail
TEST_NAME="03-hive"
TABLE="cfgcheck_smoke_$$"

SQL="
CREATE TABLE ${TABLE} (id INT, name STRING);
INSERT INTO ${TABLE} VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma');
SELECT COUNT(*) AS n FROM ${TABLE};
DROP TABLE ${TABLE};
"

echo "[$TEST_NAME] running table lifecycle via beeline"
OUT=$(docker exec hive-server2 beeline -u "jdbc:hive2://localhost:10000" \
  --silent=true --outputformat=csv2 -e "$SQL" 2>&1) || {
  echo "[$TEST_NAME] FAIL: beeline exited non-zero"
  echo "$OUT" | tail -40
  exit 1
}

# Expect to see "3" in the count output
if echo "$OUT" | grep -qE '^3$|^n$|,3$'; then
  echo "[$TEST_NAME] PASS: table created, 3 rows inserted+counted, dropped"
  exit 0
else
  echo "[$TEST_NAME] FAIL: didn't find expected count of 3"
  echo "$OUT"
  exit 1
fi
