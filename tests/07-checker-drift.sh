#!/usr/bin/env bash
# Test 07: live-stack drift detection.
#
# This is the README's headline demo written as an automated test.
#
#   1. Preflight: `config-agent`, `config-checker`, and `kafka` must be
#      running.
#   2. Baseline: capture an agent-log offset so we only inspect new lines.
#   3. Mutate conf/yarn-site.xml IN PLACE (bash `>` preserves the inode,
#      which matters because docker-compose.override.yml bind-mounts the
#      single file — a host-side `mv` would break the bind-mount and the
#      container would never see the change).
#   4. Wait up to HEARTBEAT + 15s for the agent to re-publish. We don't
#      require watchdog (inotify over docker single-file bind mounts is
#      unreliable on WSL2 / Docker Desktop; heartbeat will still catch it).
#   5. Confirm the checker reports drift on the changed key.
#   6. Restore in place; confirm the agent re-publishes again.
#   7. Idle-window sanity: no spurious drift lines in 15s of quiet.
#
# Requires the full stack with the agent+checker sidecars from
# docker-compose.override.yml. No pytest / python.
#
# Exit codes: 0 on pass, 1 on any failure.
#
# NB: this test exposes the limits of the watchdog path on WSL2 — see
# testing-upgrades.md §"Findings from Tier A#1 live-stack smoke".

set -uo pipefail

TEST_NAME="07-checker-drift"
REPO_ROOT=$(cd "$(dirname "$0")/.." && pwd)
YARN_XML="$REPO_ROOT/conf/yarn-site.xml"
ORIGINAL_CONTENT=""  # snapshot of the original file for in-place restore

# Wait long enough to cover one heartbeat (60s default) plus a buffer.
# If CHECKER_HEARTBEAT is overridden in the override file, bump this.
DRIFT_WAIT_SEC=75

fail() {
  echo "[$TEST_NAME] FAIL: $*"
  exit 1
}

restore_yarn_xml() {
  if [ -n "$ORIGINAL_CONTENT" ] && [ -f "$YARN_XML" ]; then
    echo "[$TEST_NAME] restoring $YARN_XML in place"
    # In-place write (preserves inode, and with it the docker bind-mount).
    printf '%s' "$ORIGINAL_CONTENT" > "$YARN_XML"
  fi
}
trap restore_yarn_xml EXIT

# ---------------------------------------------------------------------------
# 1. Preflight
# ---------------------------------------------------------------------------

for c in config-agent config-checker kafka; do
  state=$(docker inspect -f '{{.State.Status}}' "$c" 2>/dev/null || echo "missing")
  if [ "$state" != "running" ]; then
    fail "$c is not running (state=$state). Run 'docker compose up -d' first."
  fi
done

[ -f "$YARN_XML" ] || fail "yarn-site.xml not found at $YARN_XML"
echo "[$TEST_NAME] sidecars are up"

# Sanity: host and container views agree before we start. If they don't,
# a previous run of this test broke the bind-mount — recommend recreate.
host_val=$(grep -oE 'yarn.scheduler.maximum-allocation-mb</(name|n)><value>[0-9]+' "$YARN_XML" | grep -oE '[0-9]+$')
cont_val=$(docker exec config-agent cat /opt/hadoop/etc/hadoop/yarn-site.xml \
  | grep -oE 'yarn.scheduler.maximum-allocation-mb</(name|n)><value>[0-9]+' \
  | grep -oE '[0-9]+$')
if [ "$host_val" != "$cont_val" ]; then
  fail "host ($host_val) and container ($cont_val) disagree on the scheduler value. The docker bind-mount is stale — run 'docker compose up -d --force-recreate agent checker' and retry."
fi
echo "[$TEST_NAME] host and container agree on baseline: $host_val"

ORIGINAL_CONTENT=$(cat "$YARN_XML")

# ---------------------------------------------------------------------------
# 2. Baseline offset
# ---------------------------------------------------------------------------

BASELINE=$(date -u +%Y-%m-%dT%H:%M:%SZ)
sleep 1
echo "[$TEST_NAME] baseline timestamp: $BASELINE"

# ---------------------------------------------------------------------------
# 3. Mutation — IN PLACE write (preserves inode, preserves bind-mount).
# ---------------------------------------------------------------------------

new_content=$(printf '%s' "$ORIGINAL_CONTENT" | awk '
  /<name>yarn.scheduler.maximum-allocation-mb<\/name>/ {
    sub(/<value>[0-9]+<\/value>/, "<value>9999</value>")
  }
  { print }
')
printf '%s' "$new_content" > "$YARN_XML"

# Confirm the container sees the new value before we wait on the agent.
cont_val=$(docker exec config-agent cat /opt/hadoop/etc/hadoop/yarn-site.xml \
  | grep -oE 'yarn.scheduler.maximum-allocation-mb</(name|n)><value>[0-9]+' \
  | grep -oE '[0-9]+$')
if [ "$cont_val" != "9999" ]; then
  fail "container does not see the host mutation (got $cont_val). Bind-mount cache may be stale."
fi
echo "[$TEST_NAME] mutation visible in container: 9999"

# ---------------------------------------------------------------------------
# 4. Wait for the agent to re-publish.
# ---------------------------------------------------------------------------

DEADLINE=$(( $(date +%s) + DRIFT_WAIT_SEC ))
SAW_REPUBLISH=""
WATCHDOG_FIRED=""
while [ "$(date +%s)" -lt "$DEADLINE" ]; do
  agent_logs=$(docker logs --since "$BASELINE" config-agent 2>&1 || true)
  if echo "$agent_logs" | grep -q "detected change"; then
    WATCHDOG_FIRED=1
  fi
  # Either code path ends in "published N snapshot(s)" — watchdog logs
  # "published N snapshot(s) after file change", heartbeat logs only at
  # DEBUG, so for heartbeat we detect republish indirectly via the
  # checker picking up the change.
  checker_logs=$(docker logs --since "$BASELINE" config-checker 2>&1 || true)
  if echo "$checker_logs" | grep -q 'yarn.scheduler.maximum-allocation-mb'; then
    SAW_REPUBLISH=1
    break
  fi
  sleep 2
done

if [ -z "$SAW_REPUBLISH" ]; then
  echo "---- agent logs since $BASELINE ----"
  docker logs --since "$BASELINE" config-agent 2>&1 | tail -30
  echo "---- checker logs since $BASELINE ----"
  docker logs --since "$BASELINE" config-checker 2>&1 | tail -30
  echo "---------------------------------------"
  fail "checker did not see the drift within ${DRIFT_WAIT_SEC}s (heartbeat: ~60s, watchdog unreliable on docker single-file bind mounts)"
fi

if [ -n "$WATCHDOG_FIRED" ]; then
  echo "[$TEST_NAME] watchdog fired — drift detected quickly"
else
  echo "[$TEST_NAME] watchdog did NOT fire; heartbeat caught the drift (expected on WSL2 bind-mounts)"
fi

# Re-fetch to pick up any lines that arrived after the loop's last capture.
checker_logs=$(docker logs --since "$BASELINE" config-checker 2>&1 || true)

# Drift output must carry the new value so operators see what changed.
if ! echo "$checker_logs" | grep -q '9999'; then
  fail "checker drift log did not include the new value 9999"
fi
echo "[$TEST_NAME] checker reported drift including new value 9999"

# ---------------------------------------------------------------------------
# 5. Restore — in place, same inode.
# ---------------------------------------------------------------------------

RESTORE_BASELINE=$(date -u +%Y-%m-%dT%H:%M:%SZ)
sleep 1
restore_yarn_xml
trap - EXIT  # disarm; we've already restored

cont_val=$(docker exec config-agent cat /opt/hadoop/etc/hadoop/yarn-site.xml \
  | grep -oE 'yarn.scheduler.maximum-allocation-mb</(name|n)><value>[0-9]+' \
  | grep -oE '[0-9]+$')
if [ "$cont_val" = "9999" ]; then
  fail "container still sees 9999 after restore — in-place write failed"
fi
echo "[$TEST_NAME] restored value visible in container: $cont_val"

# Wait for the agent/checker to notice the restore.
DEADLINE=$(( $(date +%s) + DRIFT_WAIT_SEC ))
SAW_RESTORE=""
while [ "$(date +%s)" -lt "$DEADLINE" ]; do
  checker_logs=$(docker logs --since "$RESTORE_BASELINE" config-checker 2>&1 || true)
  if echo "$checker_logs" | grep -q "$cont_val"; then
    SAW_RESTORE=1
    break
  fi
  sleep 2
done

if [ -z "$SAW_RESTORE" ]; then
  # The checker only emits drift when a key *changes*. If the restore
  # matches the last-seen temporal baseline, the checker will be silent.
  # That's fine — we accept either "log line mentioning the restored value"
  # OR "agent published something new in this window".
  agent_logs=$(docker logs --since "$RESTORE_BASELINE" config-agent 2>&1 || true)
  if echo "$agent_logs" | grep -Eq "published|detected change"; then
    SAW_RESTORE=1
    echo "[$TEST_NAME] agent republished; checker may be silent (value matches last-seen)"
  fi
fi

if [ -z "$SAW_RESTORE" ]; then
  echo "---- agent logs since $RESTORE_BASELINE ----"
  docker logs --since "$RESTORE_BASELINE" config-agent 2>&1 | tail -30
  echo "---- checker logs since $RESTORE_BASELINE ----"
  docker logs --since "$RESTORE_BASELINE" config-checker 2>&1 | tail -30
  echo "--------------------------------------------"
  fail "no agent republish visible after restore within ${DRIFT_WAIT_SEC}s"
fi
echo "[$TEST_NAME] restore observed by pipeline"

# ---------------------------------------------------------------------------
# 6. Idle-window sanity — no spurious drift in a quiet window.
# ---------------------------------------------------------------------------

sleep 10
IDLE_BASELINE=$(date -u +%Y-%m-%dT%H:%M:%SZ)
sleep 15
LOGS=$(docker logs --since "$IDLE_BASELINE" config-checker 2>&1 || true)
# Drift JSON from format_drift_report includes '"type": "drift"' or
# 'DriftResult' — processing-only logs don't.
SPURIOUS=$(echo "$LOGS" | grep -E '"rule_id"|DriftResult' || true)
if [ -n "$SPURIOUS" ]; then
  echo "---- spurious drift ----"
  echo "$SPURIOUS"
  echo "------------------------"
  fail "checker emitted drift during a 15s idle window — false positive"
fi
echo "[$TEST_NAME] idle window produced no spurious drift"

echo "[$TEST_NAME] PASS"
exit 0
