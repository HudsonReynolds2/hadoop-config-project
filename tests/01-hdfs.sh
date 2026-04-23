#!/usr/bin/env bash
# Test 01: HDFS write, read, verify byte-for-byte match.
# Success criterion: file uploaded and read back equals original.

set -euo pipefail

TEST_NAME="01-hdfs"
TMP=$(mktemp)
TMP_OUT=$(mktemp)
HDFS_PATH="/tmp/hdfs-test-$$"

# Generate 1 MB of random bytes
dd if=/dev/urandom of="$TMP" bs=1024 count=1024 status=none

# Copy into container, put to HDFS, get back, copy out, diff
docker cp "$TMP" namenode:/tmp/in.bin
docker exec namenode hdfs dfs -put -f /tmp/in.bin "$HDFS_PATH"
docker exec namenode hdfs dfs -get "$HDFS_PATH" /tmp/out.bin
docker cp namenode:/tmp/out.bin "$TMP_OUT"

if cmp -s "$TMP" "$TMP_OUT"; then
  echo "[$TEST_NAME] PASS: 1MB round-trip byte-identical"
  docker exec namenode hdfs dfs -rm -f "$HDFS_PATH" >/dev/null
  rm -f "$TMP" "$TMP_OUT"
  exit 0
else
  echo "[$TEST_NAME] FAIL: bytes differ"
  rm -f "$TMP" "$TMP_OUT"
  exit 1
fi
