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
)

PASS=()
FAIL=()

echo "================================================================"
echo " hadoop-stack functionality tests"
echo "================================================================"

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
