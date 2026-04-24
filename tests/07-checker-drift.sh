#!/usr/bin/env bash
# Test 07: live-stack drift detection.
#
# This is the README's headline demo written as an automated test:
#
#   1. Assert the `config-agent` and `config-checker` sidecars are up and
#      healthy against the running cluster.
#   2. Capture a baseline offset in `docker compose logs checker`.
#   3. Mutate conf/yarn-site.xml (bump the scheduler ceiling past NM).
#   4. Poll the checker's logs for up to 15s; PASS when a drift report
#      naming `yarn.scheduler.maximum-allocation-mb` appears.
#   5. Restore the file; PASS when a new snapshot flows through.
#   6. Sanity: a 15s idle window must NOT produce spurious drift reports.
#
# Requires the full `docker compose up -d` stack including the
# agent + checker from docker-compose.override.yml. No pytest / python.
#
# Exit codes: 0 on pass, 1 on any failure.

set -uo pipefail

TEST_NAME="07-checker-drift"
REPO_ROOT=$(cd "$(dirname "$0")/.." && pwd)
YARN_XML="$REPO_ROOT/conf/yarn-site.xml"
BACKUP="$YARN_XML.bak-$$"

cleanup() {
  if [ -f "$BACKUP" ]; then
    echo "[$TEST_NAME] restoring $YARN_XML from backup"
    mv "$BACKUP" "$YARN_XML"
    # Give the agent a moment to re-publish.
    sleep 2
  fi
}
trap cleanup EXIT

fail() {
  echo "[$TEST_NAME] FAIL: $*"
  exit 1
}

# ---------------------------------------------------------------------------
# 1. Preflight — sidecars must be running.
# ---------------------------------------------------------------------------

for c in config-agent config-checker kafka; do
  state=$(docker inspect -f '{{.State.Status}}' "$c" 2>/dev/null || echo "missing")
  if [ "$state" != "running" ]; then
    fail "$c is not running (state=$state). Run 'docker compose up -d' first."
  fi
done

echo "[$TEST_NAME] sidecars are up"

# ---------------------------------------------------------------------------
# 2. Baseline — record the current log offset so we only inspect new lines.
# ---------------------------------------------------------------------------

BASELINE=$(date -u +%Y-%m-%dT%H:%M:%S)
sleep 1  # ensure a strict > comparison in `docker logs --since`
echo "[$TEST_NAME] baseline timestamp: $BASELINE"

# ---------------------------------------------------------------------------
# 3. Mutation — bump scheduler ceiling above NM total.
# ---------------------------------------------------------------------------

[ -f "$YARN_XML" ] || fail "yarn-site.xml not found at $YARN_XML"
cp -p "$YARN_XML" "$BACKUP"

# Use awk so we don't rely on sed's in-place quirks across platforms.
# Replace the scheduler max value from 2048 to 9999.
awk '
  /<name>yarn.scheduler.maximum-allocation-mb<\/name>/ {
    sub(/<value>[0-9]+<\/value>/, "<value>9999</value>")
  }
  { print }
' "$BACKUP" > "$YARN_XML"

if ! grep -q '9999' "$YARN_XML"; then
  fail "mutation did not take effect in $YARN_XML"
fi
echo "[$TEST_NAME] mutated yarn.scheduler.maximum-allocation-mb -> 9999"

# ---------------------------------------------------------------------------
# 4. Poll the checker logs until the drift is reported (≤ 15s).
# ---------------------------------------------------------------------------

DEADLINE=$(( $(date +%s) + 15 ))
HIT=""
while [ "$(date +%s)" -lt "$DEADLINE" ]; do
  LOGS=$(docker logs --since "$BASELINE" config-checker 2>&1 || true)
  if echo "$LOGS" | grep -q 'yarn.scheduler.maximum-allocation-mb'; then
    HIT="$LOGS"
    break
  fi
  sleep 1
done

if [ -z "$HIT" ]; then
  echo "---- recent checker logs ----"
  docker logs --since "$BASELINE" config-checker 2>&1 | tail -40
  echo "-----------------------------"
  fail "checker did not report drift on yarn.scheduler.maximum-allocation-mb within 15s"
fi
echo "[$TEST_NAME] drift detected in checker logs"

# Must also carry the new value so operators can see what changed.
if ! echo "$HIT" | grep -q '9999'; then
  fail "drift report did not include the new value 9999"
fi
echo "[$TEST_NAME] drift report includes new value 9999"

# ---------------------------------------------------------------------------
# 5. Restore — cleanup trap puts the file back. Confirm the agent re-publishes.
# ---------------------------------------------------------------------------

RESTORE_BASELINE=$(date -u +%Y-%m-%dT%H:%M:%S)
sleep 1
cleanup
trap - EXIT  # disarm so we don't try to restore twice

DEADLINE=$(( $(date +%s) + 15 ))
SAW_RESTORED=""
while [ "$(date +%s)" -lt "$DEADLINE" ]; do
  LOGS=$(docker logs --since "$RESTORE_BASELINE" config-agent 2>&1 || true)
  # The agent logs which file it re-collected on a change.
  if echo "$LOGS" | grep -q 'yarn-site.xml'; then
    SAW_RESTORED=1
    break
  fi
  sleep 1
done

if [ -z "$SAW_RESTORED" ]; then
  echo "---- recent agent logs ----"
  docker logs --since "$RESTORE_BASELINE" config-agent 2>&1 | tail -40
  echo "---------------------------"
  fail "agent did not re-publish after restore within 15s"
fi
echo "[$TEST_NAME] agent re-published after restore"

# ---------------------------------------------------------------------------
# 6. Idle-window sanity — no spurious drift reports in 15s of no changes.
# Heartbeats keep flowing (every 60s default), and those should be silent.
# We only guard against temporal/propagation drift lines, not the periodic
# 'processed snapshot' info logs.
# ---------------------------------------------------------------------------

IDLE_BASELINE=$(date -u +%Y-%m-%dT%H:%M:%S)
sleep 15
LOGS=$(docker logs --since "$IDLE_BASELINE" config-checker 2>&1 || true)
# A drift report in the checker emits JSON containing "severity" and
# "rule_id". Plain processing logs don't.
SPURIOUS=$(echo "$LOGS" | grep -E '"rule_id"' || true)
if [ -n "$SPURIOUS" ]; then
  echo "---- spurious drift ----"
  echo "$SPURIOUS"
  echo "------------------------"
  fail "checker emitted drift during a 15s idle window — false positive"
fi
echo "[$TEST_NAME] idle window produced no spurious drift"

echo "[$TEST_NAME] PASS"
exit 0
