#!/usr/bin/env bash
# Run all functionality tests in order. Exit 0 only if every test passes.

set -uo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
cd "$HERE"

TESTS=(
  01-hdfs.sh
  02-yarn-pi.sh
  03-hive.sh
  04-kafka.sh
  05-zookeeper.sh
  06-spark-yarn.sh
  07-checker-drift.sh
  08-multi-agent-propagation.sh
  09-cross-service-constraint.sh
  10-causality-trace.sh
)

PASS=()
FAIL=()

echo "================================================================"
echo " hadoop-stack functionality tests"
echo "================================================================"

echo "Waiting for namenode to exit safe mode..."
until docker exec namenode hdfs dfsadmin -safemode get 2>/dev/null | grep -q "Safe mode is OFF"; do
  sleep 3
done
echo "Namenode ready."
echo

for t in "${TESTS[@]}"; do
  echo
  echo "---- running $t ----"
  if bash "$HERE/$t"; then
    PASS+=("$t")
  else
    FAIL+=("$t")
  fi
done

echo
echo "================================================================"
echo " summary"
echo "================================================================"
echo "passed: ${#PASS[@]}"
for t in "${PASS[@]}"; do echo "  + $t"; done
echo "failed: ${#FAIL[@]}"
for t in "${FAIL[@]}"; do echo "  - $t"; done

[ ${#FAIL[@]} -eq 0 ]
