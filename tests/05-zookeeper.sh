#!/usr/bin/env bash
# Test 05: ZooKeeper create/get/delete via zkCli.sh.

set -euo pipefail
TEST_NAME="05-zookeeper"
ZNODE="/cfgcheck_smoke_$$"
EXPECTED="hello-zk"

# Run all three commands in one zkCli invocation to avoid session setup overhead
OUT=$(docker exec zookeeper bash -c "
  echo 'create $ZNODE $EXPECTED
  get $ZNODE
  delete $ZNODE
  quit' | /apache-zookeeper-*/bin/zkCli.sh -server localhost:2181 2>&1
") || true

if echo "$OUT" | grep -q "$EXPECTED"; then
  echo "[$TEST_NAME] PASS: znode created, read back '$EXPECTED', deleted"
  exit 0
else
  echo "[$TEST_NAME] FAIL: didn't read back expected value"
  echo "$OUT" | tail -40
  exit 1
fi
