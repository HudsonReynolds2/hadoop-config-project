#!/usr/bin/env bash
# Test 02: Submit the built-in MapReduce pi example to YARN.
# Success criterion: job exits 0 and output contains an estimate of Pi.

set -euo pipefail
TEST_NAME="02-yarn-pi"

# Find the examples jar inside the nodemanager (resourcemanager doesn't need it
# but nodemanager definitely has it; running from resourcemanager is also fine)
JAR=$(docker exec resourcemanager bash -c 'ls /opt/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar | head -n1')

if [ -z "$JAR" ]; then
  echo "[$TEST_NAME] FAIL: examples jar not found in resourcemanager"
  exit 1
fi

echo "[$TEST_NAME] submitting pi job (2 maps, 100 samples)"
OUT=$(docker exec resourcemanager hadoop jar "$JAR" pi 2 100 2>&1) || {
  echo "[$TEST_NAME] FAIL: job exited non-zero"
  echo "$OUT" | tail -40
  exit 1
}

if echo "$OUT" | grep -qE 'Estimated value of Pi is [0-9]'; then
  echo "[$TEST_NAME] PASS: $(echo "$OUT" | grep 'Estimated value of Pi')"
  exit 0
else
  echo "[$TEST_NAME] FAIL: no Pi estimate in output"
  echo "$OUT" | tail -40
  exit 1
fi
