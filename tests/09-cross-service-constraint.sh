#!/usr/bin/env bash
# Test 09: cross-service constraint rule (yarn-scheduler-ceiling).
#
# Mutate yarn.scheduler.maximum-allocation-mb to a value larger than
# yarn.nodemanager.resource.memory-mb. The yarn-scheduler-ceiling rule
# (constraint: scheduler.max <= NM total memory) must fire because both
# the resourcemanager and nodemanager agents are now publishing — without
# both, the rule was silently dead.
#
# In-place edits only; trap restores on exit.
#
# Exit codes: 0 on pass, 1 on any failure.

set -uo pipefail

TEST_NAME="09-cross-service-constraint"
REPO_ROOT=$(cd "$(dirname "$0")/.." && pwd)
YARN_XML="$REPO_ROOT/conf/yarn-site.xml"
ORIGINAL_CONTENT=""
DRIFT_WAIT_SEC=90

# Both these agents must be running for yarn-scheduler-ceiling to evaluate.
REQUIRED_AGENTS=(config-agent-resourcemanager config-agent-nodemanager)

fail() {
  echo "[$TEST_NAME] FAIL: $*"
  exit 1
}

restore_yarn_xml() {
  if [ -n "$ORIGINAL_CONTENT" ] && [ -f "$YARN_XML" ]; then
    echo "[$TEST_NAME] restoring $YARN_XML in place"
    printf '%s' "$ORIGINAL_CONTENT" > "$YARN_XML"
  fi
}
trap restore_yarn_xml EXIT

# ---------------------------------------------------------------------------
# 1. Preflight.
# ---------------------------------------------------------------------------

for c in "${REQUIRED_AGENTS[@]}" config-checker kafka; do
  state=$(docker inspect -f '{{.State.Status}}' "$c" 2>/dev/null || echo "missing")
  if [ "$state" != "running" ]; then
    fail "$c is not running (state=$state). yarn-scheduler-ceiling requires both RM and NM agents."
  fi
done

[ -f "$YARN_XML" ] || fail "yarn-site.xml not found at $YARN_XML"

# Sanity: read NM memory ceiling so the mutation we apply is genuinely larger.
NM_MEM=$(grep -oE 'yarn.nodemanager.resource.memory-mb</(name|n)><value>[0-9]+' "$YARN_XML" \
  | grep -oE '[0-9]+$' || echo "")
[ -n "$NM_MEM" ] || fail "could not read yarn.nodemanager.resource.memory-mb from $YARN_XML"
echo "[$TEST_NAME] NM memory ceiling: $NM_MEM MB"

NEW_SCHED=$(( NM_MEM * 4 + 1 ))   # comfortably above the ceiling
echo "[$TEST_NAME] will set scheduler max to $NEW_SCHED MB (must violate ceiling)"

ORIGINAL_CONTENT=$(cat "$YARN_XML")

# ---------------------------------------------------------------------------
# 2. Baseline timestamp.
# ---------------------------------------------------------------------------

BASELINE=$(date -u +%Y-%m-%dT%H:%M:%SZ)
sleep 1

# ---------------------------------------------------------------------------
# 3. Mutate scheduler max in place.
# ---------------------------------------------------------------------------

new_content=$(printf '%s' "$ORIGINAL_CONTENT" | awk -v v="$NEW_SCHED" '
  /<name>yarn.scheduler.maximum-allocation-mb<\/name>/ {
    sub(/<value>[0-9]+<\/value>/, "<value>" v "</value>")
  }
  { print }
')
printf '%s' "$new_content" > "$YARN_XML"

cont_val=$(docker exec config-agent-resourcemanager cat /opt/hadoop/etc/hadoop/yarn-site.xml \
  | grep -oE 'yarn.scheduler.maximum-allocation-mb</(name|n)><value>[0-9]+' \
  | grep -oE '[0-9]+$')
if [ "$cont_val" != "$NEW_SCHED" ]; then
  fail "RM agent does not see mutation (got $cont_val); bind-mount stale"
fi
echo "[$TEST_NAME] mutation visible in RM agent: $cont_val"

# ---------------------------------------------------------------------------
# 4. Wait for the checker to report yarn-scheduler-ceiling failure.
# ---------------------------------------------------------------------------

DEADLINE=$(( $(date +%s) + DRIFT_WAIT_SEC ))
SAW_RULE=""
while [ "$(date +%s)" -lt "$DEADLINE" ]; do
  checker_logs=$(docker logs --since "$BASELINE" config-checker 2>&1 || true)
  if echo "$checker_logs" | grep -q "yarn-scheduler-ceiling"; then
    SAW_RULE=1
    break
  fi
  sleep 2
done

if [ -z "$SAW_RULE" ]; then
  fail "checker did not report yarn-scheduler-ceiling within ${DRIFT_WAIT_SEC}s"
fi

# Re-fetch to pick up any lines that arrived after the loop's last capture.
checker_logs=$(docker logs --since "$BASELINE" config-checker 2>&1 || true)

# Confirm the new value is referenced and severity is critical.
if ! echo "$checker_logs" | grep -q "$NEW_SCHED"; then
  fail "yarn-scheduler-ceiling fired but output does not reference the new value $NEW_SCHED"
fi
if ! echo "$checker_logs" | grep -qiE 'critical'; then
  fail "yarn-scheduler-ceiling fired but severity 'critical' not in output"
fi

echo "[$TEST_NAME] yarn-scheduler-ceiling fired with new value $NEW_SCHED at critical severity"

# ---------------------------------------------------------------------------
# 5. Restore.
# ---------------------------------------------------------------------------

RESTORE_BASELINE=$(date -u +%Y-%m-%dT%H:%M:%SZ)
sleep 1
restore_yarn_xml
trap - EXIT

cont_val=$(docker exec config-agent-resourcemanager cat /opt/hadoop/etc/hadoop/yarn-site.xml \
  | grep -oE 'yarn.scheduler.maximum-allocation-mb</(name|n)><value>[0-9]+' \
  | grep -oE '[0-9]+$')
if [ "$cont_val" = "$NEW_SCHED" ]; then
  fail "container still sees mutated value after restore"
fi
echo "[$TEST_NAME] restored value visible: $cont_val"

# Wait for an agent republish after restore.
DEADLINE=$(( $(date +%s) + DRIFT_WAIT_SEC ))
SAW_RESTORE=""
while [ "$(date +%s)" -lt "$DEADLINE" ]; do
  for c in "${REQUIRED_AGENTS[@]}"; do
    agent_logs=$(docker logs --since "$RESTORE_BASELINE" "$c" 2>&1 || true)
    if echo "$agent_logs" | grep -Eq "published|detected change"; then
      SAW_RESTORE=1
      break 2
    fi
  done
  sleep 2
done
[ -n "$SAW_RESTORE" ] || fail "no RM/NM agent republished after restore within ${DRIFT_WAIT_SEC}s"
echo "[$TEST_NAME] restore observed by pipeline"

echo "[$TEST_NAME] PASS"
exit 0
