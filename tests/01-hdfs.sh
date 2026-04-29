#!/usr/bin/env bash
# Test 01: HDFS write, read, verify byte-for-byte match.
# Success criterion: file uploaded and read back equals original.

set -euo pipefail

TEST_NAME="01-hdfs"
TMP=$(mktemp)
TMP_OUT=$(mktemp)
STAMP=$$
HDFS_PATH="/tmp/hdfs-test-$STAMP"
IN_LOCAL="/tmp/in-$STAMP.bin"
OUT_LOCAL="/tmp/out-$STAMP.bin"

# Generate 1 MB of random bytes
dd if=/dev/urandom of="$TMP" bs=1024 count=1024 status=none

# Copy into container, put to HDFS, get back, copy out, diff.
# Container-side filenames are stamped so parallel/repeated runs don't collide.
docker cp "$TMP" "namenode:$IN_LOCAL"
docker exec namenode hdfs dfs -put -f "$IN_LOCAL" "$HDFS_PATH"
docker exec namenode hdfs dfs -get "$HDFS_PATH" "$OUT_LOCAL"
docker cp "namenode:$OUT_LOCAL" "$TMP_OUT"

# Cleanup container-side files regardless of outcome
docker exec namenode rm -f "$IN_LOCAL" "$OUT_LOCAL" >/dev/null 2>&1 || true
docker exec namenode hdfs dfs -rm -f "$HDFS_PATH" >/dev/null 2>&1 || true

if cmp -s "$TMP" "$TMP_OUT"; then
  echo "[$TEST_NAME] PASS: 1MB round-trip byte-identical"
  rm -f "$TMP" "$TMP_OUT"
  exit 0
else
  echo "[$TEST_NAME] FAIL: bytes differ"
  rm -f "$TMP" "$TMP_OUT"
  exit 1
fi
