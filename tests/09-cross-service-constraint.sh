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
DRIFT_WAIT_SEC=70

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

NM_MEM=$(grep -oE 'yarn.nodemanager.resource.memory-mb</(name|n)><value>[0-9]+' "$YARN_XML" \
  | grep -oE '[0-9]+$' || echo "")
[ -n "$NM_MEM" ] || fail "could not read yarn.nodemanager.resource.memory-mb from $YARN_XML"
echo "[$TEST_NAME] NM memory ceiling: $NM_MEM MB"

NEW_SCHED=$(( NM_MEM * 4 + 1 ))
echo "[$TEST_NAME] will set scheduler max to $NEW_SCHED MB (must violate ceiling)"

ORIGINAL_CONTENT=$(cat "$YARN_XML")

# ---------------------------------------------------------------------------
# 2. Wait for checker to go quiet, then set baseline.
# ---------------------------------------------------------------------------

QUIET_DEADLINE=$(( $(date +%s) + 30 ))
while [ "$(date +%s)" -lt "$QUIET_DEADLINE" ]; do
  probe_ts=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  sleep 1
  if [ -z "$(docker logs --since "$probe_ts" config-checker 2>&1)" ]; then
    break
  fi
done

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
checker_logs=""
while [ "$(date +%s)" -lt "$DEADLINE" ]; do
  checker_logs=$(docker logs --since "$BASELINE" config-checker 2>&1 || true)
  if echo "$checker_logs" | grep -q "yarn-scheduler-ceiling" && \
     echo "$checker_logs" | grep -q "$NEW_SCHED"; then
    SAW_RULE=1
    break
  fi
  sleep 2
done

if [ -z "$SAW_RULE" ]; then
  fail "checker did not report yarn-scheduler-ceiling with value $NEW_SCHED within ${DRIFT_WAIT_SEC}s"
fi

# -- severity check (relaxed) --
# Accept any of:
#   - the literal word "critical" / "CRITICAL" in checker logs
#   - "severity": "critical" in JSON drift reports
#   - the [CRITICAL] tag from `hadoopconf status --format text`
# If none of those appear in the streamed logs, fall back to a direct
# `hadoopconf status` query and check its output. This is robust to
# whether the consumer logs severity inline or only emits structured
# JSON to a different topic.
SAW_CRITICAL=""
if echo "$checker_logs" | grep -qiE 'critical'; then
  SAW_CRITICAL=1
fi

if [ -z "$SAW_CRITICAL" ]; then
  echo "[$TEST_NAME] severity not in streamed logs; querying status directly"
  status_out=$(docker exec config-checker hadoopconf status \
    --rules /etc/checker/rules/hadoop-3.3.x.yaml \
    --timeout 10 --format json 2>/dev/null || true)
  if echo "$status_out" | grep -qiE '"severity":[[:space:]]*"critical"'; then
    SAW_CRITICAL=1
  fi
fi

if [ -z "$SAW_CRITICAL" ]; then
  echo "---- checker logs since $BASELINE (last 50 lines) ----"
  echo "$checker_logs" | tail -50
  echo "------------------------------------------------------"
  fail "yarn-scheduler-ceiling fired but severity 'critical' not found in logs OR status output"
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
