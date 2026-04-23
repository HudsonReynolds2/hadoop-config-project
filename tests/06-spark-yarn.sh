#!/usr/bin/env bash
# Test 06: Spark on YARN — submit SparkPi from spark-client, check output.
# This is the big integration test: Spark reads Hadoop XMLs -> talks to RM ->
# RM launches AM on NM -> AM runs driver -> executors launched -> HDFS archive
# pulled -> Pi computed.

set -euo pipefail
TEST_NAME="06-spark-yarn"

# SparkPi is in the examples jar shipped with the spark image
JAR=$(docker exec spark-client bash -c 'ls /opt/spark/examples/jars/spark-examples_*.jar | head -n1')

if [ -z "$JAR" ]; then
  echo "[$TEST_NAME] FAIL: spark-examples jar not found"
  exit 1
fi

echo "[$TEST_NAME] submitting SparkPi via --master yarn (this takes ~60s)"
OUT=$(docker exec spark-client /opt/spark/bin/spark-submit \
  --master yarn \
  --deploy-mode client \
  --class org.apache.spark.examples.SparkPi \
  "$JAR" 50 2>&1) || {
  echo "[$TEST_NAME] FAIL: spark-submit exited non-zero"
  echo "$OUT" | tail -60
  exit 1
}

if echo "$OUT" | grep -qE 'Pi is roughly [0-9]'; then
  echo "[$TEST_NAME] PASS: $(echo "$OUT" | grep 'Pi is roughly')"
  exit 0
else
  echo "[$TEST_NAME] FAIL: no Pi estimate in output"
  echo "$OUT" | tail -60
  exit 1
fi
