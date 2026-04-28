#!/usr/bin/env bash
# Test 10: causality trace.
#
# Trigger an fs.defaultFS mismatch and confirm the checker JSON output
# contains a non-empty root_causes array listing downstream services.
# fs.defaultFS in the seeded causality graph has edges to hive-server2,
# hive-metastore, and spark-client.
#
# This test reads the checker's structured JSON output (printed to stdout
# by format_summary in checker/consumer.py) — not just log greps. We pull
# the lines, locate the report containing fs.defaultFS, and inspect the
# root_causes field.
#
# In-place edits only.
#
# Exit codes: 0 on pass, 1 on any failure.

set -uo pipefail

TEST_NAME="10-causality-trace"
REPO_ROOT=$(cd "$(dirname "$0")/.." && pwd)
CORE_XML="$REPO_ROOT/conf/core-site.xml"
ORIGINAL_CONTENT=""
DRIFT_WAIT_SEC=120

fail() {
  echo "[$TEST_NAME] FAIL: $*"
  exit 1
}

restore_core_xml() {
  if [ -n "$ORIGINAL_CONTENT" ] && [ -f "$CORE_XML" ]; then
    echo "[$TEST_NAME] restoring $CORE_XML in place"
    printf '%s' "$ORIGINAL_CONTENT" > "$CORE_XML"
  fi
}
trap restore_core_xml EXIT

# ---------------------------------------------------------------------------
# 1. Preflight. We need the checker plus at least namenode and one
#    downstream agent (hive-server2 or spark-client) for the trace to
#    list a real downstream effect.
# ---------------------------------------------------------------------------

for c in config-agent config-checker kafka; do
  state=$(docker inspect -f '{{.State.Status}}' "$c" 2>/dev/null || echo "missing")
  if [ "$state" != "running" ]; then
    fail "$c is not running (state=$state)"
  fi
done

DOWNSTREAM_PRESENT=""
for c in config-agent-hive-server2 config-agent-hive-metastore config-agent-spark-client; do
  state=$(docker inspect -f '{{.State.Status}}' "$c" 2>/dev/null || echo "missing")
  if [ "$state" = "running" ]; then
    DOWNSTREAM_PRESENT=1
    break
  fi
done
[ -n "$DOWNSTREAM_PRESENT" ] || fail "no downstream fs.defaultFS agent (hive-server2/hive-metastore/spark-client) is running — causality trace would be empty"

[ -f "$CORE_XML" ] || fail "core-site.xml not found at $CORE_XML"

ORIGINAL_CONTENT=$(cat "$CORE_XML")

# ---------------------------------------------------------------------------
# 2. Mutate fs.defaultFS in place.
# ---------------------------------------------------------------------------

BASELINE=$(date -u +%Y-%m-%dT%H:%M:%SZ)
sleep 1

NEW_VAL="hdfs://causality-test-namenode:8020"
new_content=$(printf '%s' "$ORIGINAL_CONTENT" | awk -v new="$NEW_VAL" '
  /<name>fs.defaultFS<\/name>/ {
    sub(/<value>[^<]+<\/value>/, "<value>" new "</value>")
  }
  { print }
')
printf '%s' "$new_content" > "$CORE_XML"

cont_val=$(docker exec config-agent cat /opt/hadoop/etc/hadoop/core-site.xml \
  | grep -oE 'fs.defaultFS</(name|n)><value>[^<]+' \
  | sed -E 's/.*<value>//')
[ "$cont_val" = "$NEW_VAL" ] || fail "namenode agent did not see mutation (got $cont_val)"

# ---------------------------------------------------------------------------
# 3. Wait for a checker report containing both fs.defaultFS and root_causes.
# ---------------------------------------------------------------------------

DEADLINE=$(( $(date +%s) + DRIFT_WAIT_SEC ))
REPORT=""
while [ "$(date +%s)" -lt "$DEADLINE" ]; do
  checker_logs=$(docker logs --since "$BASELINE" config-checker 2>&1 || true)
  # Pull every JSON line that mentions both fs.defaultFS and root_causes.
  candidate=$(echo "$checker_logs" \
    | grep -E '^\{.*"fs.defaultFS".*"root_causes"' \
    | tail -1 || true)
  if [ -n "$candidate" ]; then
    REPORT="$candidate"
    break
  fi
  sleep 3
done

if [ -z "$REPORT" ]; then
  echo "---- checker logs since $BASELINE ----"
  docker logs --since "$BASELINE" config-checker 2>&1 | tail -60
  echo "--------------------------------------"
  fail "no checker JSON report containing fs.defaultFS + root_causes within ${DRIFT_WAIT_SEC}s"
fi

# ---------------------------------------------------------------------------
# 4. Inspect the JSON. Use python rather than jq; jq is not guaranteed.
# ---------------------------------------------------------------------------

EFFECT_COUNT=$(python3 -c '
import json, sys
data = json.loads(sys.argv[1])
rcs = data.get("root_causes", [])
total = 0
for rc in rcs:
    if rc.get("key") == "fs.defaultFS":
        total += len(rc.get("downstream_effects", []))
print(total)
' "$REPORT" 2>&1) || fail "could not parse checker JSON report: $EFFECT_COUNT"

if [ "$EFFECT_COUNT" -lt 1 ]; then
  echo "---- report ----"
  echo "$REPORT"
  echo "----------------"
  fail "root_causes array for fs.defaultFS is empty (expected ≥1 downstream effect)"
fi
echo "[$TEST_NAME] root_causes lists $EFFECT_COUNT downstream effect(s) for fs.defaultFS"

# Confirm at least one expected downstream service appears in the effects.
SAW_DOWNSTREAM=$(python3 -c '
import json, sys
data = json.loads(sys.argv[1])
expected = {"hive-server2", "hive-metastore", "spark-client"}
hits = set()
for rc in data.get("root_causes", []):
    if rc.get("key") != "fs.defaultFS":
        continue
    for eff in rc.get("downstream_effects", []):
        for svc in expected:
            if eff.startswith(svc + ":") or ":" + svc + ":" in eff or svc in eff.split(":", 1)[0]:
                hits.add(svc)
print(",".join(sorted(hits)))
' "$REPORT" 2>&1) || fail "could not extract downstream services: $SAW_DOWNSTREAM"

if [ -z "$SAW_DOWNSTREAM" ]; then
  echo "---- report ----"
  echo "$REPORT"
  echo "----------------"
  fail "no expected downstream service (hive-server2/hive-metastore/spark-client) in causality trace"
fi
echo "[$TEST_NAME] downstream services in trace: $SAW_DOWNSTREAM"

# ---------------------------------------------------------------------------
# 5. Restore.
# ---------------------------------------------------------------------------

restore_core_xml
trap - EXIT

echo "[$TEST_NAME] PASS"
exit 0
