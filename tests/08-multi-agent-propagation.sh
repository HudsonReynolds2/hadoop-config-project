#!/usr/bin/env bash
# Test 08: multi-agent propagation drift.
#
# Mutate fs.defaultFS in core-site.xml. Confirm the checker reports drift
# from multiple agents (namenode, resourcemanager, nodemanager, datanode,
# spark-client) — they all mount the same ./conf/ tree, so all of them
# should re-publish a snapshot containing the new value.
#
# This test exists to prove that single-file inotify limitations have been
# overcome by the directory-mount switch in docker-compose.override.yml,
# AND that the multi-agent topology now actually propagates a single config
# change to every consumer of the shared conf/.
#
# In-place edits only — see 07-checker-drift.sh for why `mv`/`cp` over
# bind-mounted files breaks Docker Desktop / WSL2 mounts.
#
# Exit codes: 0 on pass, 1 on any failure.

set -uo pipefail

TEST_NAME="08-multi-agent-propagation"
REPO_ROOT=$(cd "$(dirname "$0")/.." && pwd)
CORE_XML="$REPO_ROOT/conf/core-site.xml"
ORIGINAL_CONTENT=""
DRIFT_WAIT_SEC=90

# Agents we expect to react. The list intentionally omits hive-server2 and
# hive-metastore (their fs.defaultFS comes from JVM flags, not the XML
# file we are editing) and omits kafka/zookeeper (which don't carry
# fs.defaultFS at all). We also keep the canonical "config-agent"
# (= namenode, kept under that name for backwards compat with test 07).
EXPECTED_AGENTS=(
  config-agent
  config-agent-datanode
  config-agent-resourcemanager
  config-agent-nodemanager
  config-agent-spark-client
)

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
# 1. Preflight — every expected agent and the checker must be running.
# ---------------------------------------------------------------------------

for c in "${EXPECTED_AGENTS[@]}" config-checker kafka; do
  state=$(docker inspect -f '{{.State.Status}}' "$c" 2>/dev/null || echo "missing")
  if [ "$state" != "running" ]; then
    fail "$c is not running (state=$state). Run 'docker compose up -d' with all agent profiles enabled."
  fi
done

[ -f "$CORE_XML" ] || fail "core-site.xml not found at $CORE_XML"
echo "[$TEST_NAME] all expected agents are up: ${EXPECTED_AGENTS[*]}"

ORIGINAL_CONTENT=$(cat "$CORE_XML")

# Sanity: every agent's container view of core-site.xml should match the
# host. If any disagree, a previous run broke a mount — recommend recreate.
host_val=$(grep -oE 'fs.defaultFS</(name|n)><value>[^<]+' "$CORE_XML" | sed -E 's/.*<value>//')
for c in "${EXPECTED_AGENTS[@]}"; do
  cont_val=$(docker exec "$c" cat /opt/hadoop/etc/hadoop/core-site.xml 2>/dev/null \
    | grep -oE 'fs.defaultFS</(name|n)><value>[^<]+' \
    | sed -E 's/.*<value>//' || true)
  if [ "$host_val" != "$cont_val" ]; then
    fail "host ($host_val) and $c ($cont_val) disagree on fs.defaultFS — bind-mount stale, run 'docker compose up -d --force-recreate'"
  fi
done
echo "[$TEST_NAME] host and all agents agree on baseline: $host_val"

# ---------------------------------------------------------------------------
# 2. Baseline timestamp.
# ---------------------------------------------------------------------------

BASELINE=$(date -u +%Y-%m-%dT%H:%M:%SZ)
sleep 1
echo "[$TEST_NAME] baseline timestamp: $BASELINE"

# ---------------------------------------------------------------------------
# 3. Mutate fs.defaultFS in place.
# ---------------------------------------------------------------------------

NEW_VAL="hdfs://drifted-namenode:8020"
new_content=$(printf '%s' "$ORIGINAL_CONTENT" | awk -v new="$NEW_VAL" '
  /<name>fs.defaultFS<\/name>/ {
    sub(/<value>[^<]+<\/value>/, "<value>" new "</value>")
  }
  { print }
')
printf '%s' "$new_content" > "$CORE_XML"

# Verify every agent sees the mutation before we wait on Kafka/checker.
for c in "${EXPECTED_AGENTS[@]}"; do
  cont_val=$(docker exec "$c" cat /opt/hadoop/etc/hadoop/core-site.xml \
    | grep -oE 'fs.defaultFS</(name|n)><value>[^<]+' \
    | sed -E 's/.*<value>//')
  if [ "$cont_val" != "$NEW_VAL" ]; then
    fail "$c did not see the host mutation (got '$cont_val'). Bind-mount cache may be stale."
  fi
done
echo "[$TEST_NAME] mutation visible in all agents: $NEW_VAL"

# ---------------------------------------------------------------------------
# 4. Wait for the checker to report drift mentioning the new value.
# ---------------------------------------------------------------------------

DEADLINE=$(( $(date +%s) + DRIFT_WAIT_SEC ))
SAW_DRIFT=""
while [ "$(date +%s)" -lt "$DEADLINE" ]; do
  checker_logs=$(docker logs --since "$BASELINE" config-checker 2>&1 || true)
  if echo "$checker_logs" | grep -q "drifted-namenode"; then
    SAW_DRIFT=1
    break
  fi
  sleep 2
done

if [ -z "$SAW_DRIFT" ]; then
  fail "checker did not report drift containing 'drifted-namenode' within ${DRIFT_WAIT_SEC}s"
fi
echo "[$TEST_NAME] checker reported drift containing the new fs.defaultFS"

# Re-fetch to pick up any lines that arrived after the loop's last capture.
checker_logs=$(docker logs --since "$BASELINE" config-checker 2>&1 || true)

# Confirm at least 2 distinct expected agents appear in drift output. The
# fs-defaultfs-propagation rule reports the disagreement as a single
# DriftResult, but cross-source / temporal lines should mention multiple
# services as each agent re-publishes.
SEEN_SERVICES=0
for svc in namenode datanode resourcemanager nodemanager spark-client; do
  if echo "$checker_logs" | grep -q "\"service\": \"$svc\""; then
    SEEN_SERVICES=$(( SEEN_SERVICES + 1 ))
  fi
done
if [ "$SEEN_SERVICES" -lt 2 ]; then
  fail "expected ≥2 distinct services in drift output, saw $SEEN_SERVICES"
fi
echo "[$TEST_NAME] drift output names $SEEN_SERVICES services"

# ---------------------------------------------------------------------------
# 5. Restore in place; trap will fire too, but disarm after explicit restore.
# ---------------------------------------------------------------------------

RESTORE_BASELINE=$(date -u +%Y-%m-%dT%H:%M:%SZ)
sleep 1
restore_core_xml
trap - EXIT

cont_val=$(docker exec config-agent cat /opt/hadoop/etc/hadoop/core-site.xml \
  | grep -oE 'fs.defaultFS</(name|n)><value>[^<]+' \
  | sed -E 's/.*<value>//')
if [ "$cont_val" = "$NEW_VAL" ]; then
  fail "container still sees mutated value after restore — in-place write failed"
fi
echo "[$TEST_NAME] restored value visible: $cont_val"

# Wait for any agent to republish post-restore. The checker may stay silent
# if the restore matches the prior store value, so accept either an agent
# republish line or a checker line referencing the restored value.
DEADLINE=$(( $(date +%s) + DRIFT_WAIT_SEC ))
SAW_RESTORE=""
while [ "$(date +%s)" -lt "$DEADLINE" ]; do
  for c in "${EXPECTED_AGENTS[@]}"; do
    agent_logs=$(docker logs --since "$RESTORE_BASELINE" "$c" 2>&1 || true)
    if echo "$agent_logs" | grep -Eq "published|detected change"; then
      SAW_RESTORE=1
      break 2
    fi
  done
  sleep 2
done

if [ -z "$SAW_RESTORE" ]; then
  fail "no agent republished after restore within ${DRIFT_WAIT_SEC}s"
fi
echo "[$TEST_NAME] restore observed by pipeline"

echo "[$TEST_NAME] PASS"
exit 0
