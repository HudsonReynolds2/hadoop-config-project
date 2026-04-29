#!/usr/bin/env bash
# tests/evaluate.sh — Stage 3 evaluation harness.
#
# For each scenario in tests/configs/buggy/<scenario>/, this script:
#   1. Verifies the clean baseline produces zero rule failures
#      (`hadoopconf status` against the live cluster via Kafka).
#   2. Copies the scenario's mutated files into ./conf/ in place.
#      This fires inotify in every running agent's bind-mounted conf dir.
#      Records t_inject = epoch milliseconds.
#   3. Polls `hadoopconf status` (in the checker container) until the
#      expected rule_id appears as failed with the expected severity.
#      Records t_status_detect.
#   4. Polls `docker logs --since $BASELINE config-checker` until a JSON
#      drift report mentions the expected rule_id. Records t_log_detect
#      — this is agent → Kafka → consumer streaming latency for the
#      always-running consumer.
#   5. Captures the status JSON output and the checker JSON report.
#   6. Restores the clean baseline. Polls status until clean.
#   7. Writes per-scenario artifacts and a top-level summary.
#
# Why `status` and not `validate` for correctness?
#   `validate` checks a single conf dir on disk locally. It cannot see
#   what the kafka, zookeeper, or hive agents are publishing via JVM
#   flags — those rules would always skip. `status` reads from Kafka
#   and sees every agent's view, which is what real cluster operators
#   want.
#
# Output: tests/results/<UTC-timestamp>/
#   summary.json         machine-readable
#   summary.txt          human-readable table
#   <scenario>/
#     inject.diff        unified diff of clean vs mutated configs
#     before.json        status JSON of clean baseline
#     detected.json      status JSON when bug was caught
#     post-restore.json  status JSON after restore
#     checker-report.json the JSON line from config-checker logs that named the rule
#     timing.json        all per-scenario timing fields
#     scenario.txt       human-readable per-scenario summary
#
# A symlink tests/results/latest -> <UTC-timestamp> is created on success.
#
# Exit 0 iff every scenario passes every assertion AND the baseline is
# clean. Exit non-zero with a clear message otherwise.

set -uo pipefail

# ---------------------------------------------------------------------------
# Locations
# ---------------------------------------------------------------------------

REPO_ROOT=$(cd "$(dirname "$0")/.." && pwd)
CONF_DIR="$REPO_ROOT/conf"
RULES_FILE="$REPO_ROOT/rules/hadoop-3.3.x.yaml"
SCENARIOS_DIR="$REPO_ROOT/tests/configs/buggy"
RESULTS_ROOT="$REPO_ROOT/tests/results"

CHECKER_CONTAINER="config-checker"

# Heartbeat from .env (default 60). Used to bound how long we wait.
HEARTBEAT=$(grep -E "^CHECKER_HEARTBEAT=" "$REPO_ROOT/.env" 2>/dev/null \
  | cut -d= -f2 || echo "")
HEARTBEAT=${HEARTBEAT:-60}

# Per-scenario timeouts. Generous because some scenarios depend on the
# heartbeat republish path (no inotify event), and we want to give the
# pipeline plenty of headroom.
DETECT_TIMEOUT_SEC=$(( HEARTBEAT * 3 + 30 ))
RESTORE_TIMEOUT_SEC=$(( HEARTBEAT * 3 + 30 ))
STATUS_POLL_TIMEOUT=5

# Polling cadence (seconds).
POLL_INTERVAL=2

# ---------------------------------------------------------------------------
# UTC run id and per-run results dir
# ---------------------------------------------------------------------------

RUN_ID=$(date -u +%Y-%m-%dT%H-%M-%SZ)
RUN_DIR="$RESULTS_ROOT/$RUN_ID"
mkdir -p "$RUN_DIR"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

now_ms() { date -u +%s%3N; }
now_iso() { date -u +%Y-%m-%dT%H:%M:%SZ; }

log() { printf "[evaluate] %s\n" "$*"; }
fail() { printf "[evaluate] FAIL: %s\n" "$*" >&2; }

# Run hadoopconf status inside the checker container.
status_in_checker() {
  docker exec "$CHECKER_CONTAINER" hadoopconf status \
    --rules /etc/checker/rules/hadoop-3.3.x.yaml \
    --timeout "$STATUS_POLL_TIMEOUT" \
    --format json 2>/dev/null
}

# Python helpers: accept JSON as first argument (not stdin).
# Pipe + heredoc cannot share stdin in bash, so callers pass JSON as $1/$2.

rule_failed_in_status() {
  local rule_id="$1"
  local json="$2"
  python3 -c "
import json, sys
rule_id = sys.argv[1]
try:
    data = json.loads(sys.argv[2])
except Exception:
    sys.exit(0)
for r in data.get('rules', {}).get('failed', []):
    if r.get('rule_id') == rule_id:
        print('1')
        break
" "$rule_id" "$json"
}

rule_severity_in_status() {
  local rule_id="$1"
  local json="$2"
  python3 -c "
import json, sys
rule_id = sys.argv[1]
try:
    data = json.loads(sys.argv[2])
except Exception:
    sys.exit(0)
for r in data.get('rules', {}).get('failed', []):
    if r.get('rule_id') == rule_id:
        print(r.get('severity', ''))
        break
" "$rule_id" "$json"
}

is_status_clean() {
  local json="$1"
  python3 -c "
import json, sys
try:
    data = json.loads(sys.argv[1])
except Exception:
    sys.exit(0)
print('1' if data.get('clean') else '')
" "$json"
}

preflight() {
  for path in "$CONF_DIR" "$RULES_FILE" "$SCENARIOS_DIR"; do
    if [ ! -e "$path" ]; then
      fail "missing required path: $path"
      exit 2
    fi
  done

  if ! docker inspect -f '{{.State.Status}}' "$CHECKER_CONTAINER" >/dev/null 2>&1; then
    fail "container $CHECKER_CONTAINER not found. Run: docker compose up -d"
    exit 2
  fi
  state=$(docker inspect -f '{{.State.Status}}' "$CHECKER_CONTAINER")
  if [ "$state" != "running" ]; then
    fail "$CHECKER_CONTAINER not running (state=$state)"
    exit 2
  fi
}

SNAPSHOT_BACKUP=""
snapshot_conf() {
  SNAPSHOT_BACKUP=$(mktemp -d -t evaluate-conf-backup-XXXXXX)
  cp -a "$CONF_DIR/." "$SNAPSHOT_BACKUP/"
}

restore_conf() {
  if [ -n "$SNAPSHOT_BACKUP" ] && [ -d "$SNAPSHOT_BACKUP" ]; then
    for f in "$SNAPSHOT_BACKUP"/*; do
      [ -f "$f" ] || continue
      cp -a "$f" "$CONF_DIR/$(basename "$f")"
    done
  fi
}

cleanup_on_exit() {
  log "cleanup: restoring clean conf/ from snapshot"
  restore_conf
  if [ -n "$SNAPSHOT_BACKUP" ] && [ -d "$SNAPSHOT_BACKUP" ]; then
    rm -rf "$SNAPSHOT_BACKUP"
  fi
}
trap cleanup_on_exit EXIT

poll_for_rule_failure() {
  local rule_id="$1"
  local timeout_sec="$2"
  local deadline=$(( $(date +%s) + timeout_sec ))
  local last_json=""
  while [ "$(date +%s)" -lt "$deadline" ]; do
    last_json=$(status_in_checker || true)
    if [ -n "$last_json" ]; then
      hit=$(rule_failed_in_status "$rule_id" "$last_json")
      if [ "$hit" = "1" ]; then
        echo "$last_json"
        return 0
      fi
    fi
    sleep "$POLL_INTERVAL"
  done
  echo "$last_json"
  return 1
}

poll_for_clean() {
  local timeout_sec="$1"
  local deadline=$(( $(date +%s) + timeout_sec ))
  local last_json=""
  while [ "$(date +%s)" -lt "$deadline" ]; do
    last_json=$(status_in_checker || true)
    if [ -n "$last_json" ]; then
      clean=$(is_status_clean "$last_json")
      if [ "$clean" = "1" ]; then
        echo "$last_json"
        return 0
      fi
    fi
    sleep "$POLL_INTERVAL"
  done
  echo "$last_json"
  return 1
}

poll_for_log_report() {
  local rule_id="$1"
  local since="$2"
  local timeout_sec="$3"
  local deadline=$(( $(date +%s) + timeout_sec ))
  while [ "$(date +%s)" -lt "$deadline" ]; do
    local logs
    logs=$(docker logs --since "$since" "$CHECKER_CONTAINER" 2>&1 || true)
    local hit
    hit=$(echo "$logs" \
      | grep -E '^\{.*"drifts".*'"$rule_id" \
      | head -1 || true)
    if [ -n "$hit" ]; then
      echo "$hit"
      return 0
    fi
    sleep "$POLL_INTERVAL"
  done
  return 1
}

# ---------------------------------------------------------------------------
# Scenario execution
# ---------------------------------------------------------------------------

SUMMARY_ROWS=()

run_scenario() {
  local scenario="$1"
  local s_dir="$SCENARIOS_DIR/$scenario"
  local out_dir="$RUN_DIR/$scenario"
  mkdir -p "$out_dir"

  log ""
  log "=== scenario: $scenario ==="

  if [ ! -f "$s_dir/metadata.env" ]; then
    fail "$scenario: missing metadata.env"
    SUMMARY_ROWS+=("$scenario|MISSING_METADATA||||")
    return 1
  fi
  set -a
  SCENARIO_DESCRIPTION=""
  EXPECTED_RULE=""
  EXPECTED_SEVERITY=""
  EXPECTED_DOWNSTREAM=""
  # shellcheck disable=SC1090
  source "$s_dir/metadata.env"
  set +a

  if [ -z "$EXPECTED_RULE" ]; then
    fail "$scenario: metadata.env missing EXPECTED_RULE"
    SUMMARY_ROWS+=("$scenario|BAD_METADATA||||")
    return 1
  fi

  log "expected: rule=$EXPECTED_RULE severity=$EXPECTED_SEVERITY downstream=$EXPECTED_DOWNSTREAM"

  local before_json
  before_json=$(status_in_checker || true)
  printf '%s\n' "$before_json" > "$out_dir/before.json"

  : > "$out_dir/inject.diff"
  for f in "$s_dir"/*; do
    [ -f "$f" ] || continue
    local base
    base=$(basename "$f")
    case "$base" in
      metadata.env|README.md) continue ;;
    esac
    diff -u "$CONF_DIR/$base" "$f" >> "$out_dir/inject.diff" || true
  done

  local baseline_iso
  baseline_iso=$(now_iso)
  sleep 1
  local t_inject_ms
  t_inject_ms=$(now_ms)

  for f in "$s_dir"/*; do
    [ -f "$f" ] || continue
    local base
    base=$(basename "$f")
    case "$base" in
      metadata.env|README.md) continue ;;
    esac
    cp -a "$f" "$CONF_DIR/$base"
  done

  local detected_json=""
  local status_detect_ms=""
  local status_check="FAIL"
  if detected_json=$(poll_for_rule_failure "$EXPECTED_RULE" "$DETECT_TIMEOUT_SEC"); then
    status_detect_ms=$(now_ms)
    status_check="PASS"
    log "status caught $EXPECTED_RULE in $((status_detect_ms - t_inject_ms)) ms"
  else
    log "status did NOT catch $EXPECTED_RULE within ${DETECT_TIMEOUT_SEC}s"
  fi
  printf '%s\n' "$detected_json" > "$out_dir/detected.json"

  local severity_check="SKIP"
  if [ "$status_check" = "PASS" ] && [ -n "$EXPECTED_SEVERITY" ]; then
    actual_sev=$(rule_severity_in_status "$EXPECTED_RULE" "$detected_json")
    if [ "$actual_sev" = "$EXPECTED_SEVERITY" ]; then
      severity_check="PASS"
    else
      severity_check="FAIL($actual_sev)"
    fi
  fi

  local log_report=""
  local log_detect_ms=""
  local log_check="FAIL"
  if log_report=$(poll_for_log_report "$EXPECTED_RULE" "$baseline_iso" "$DETECT_TIMEOUT_SEC"); then
    log_detect_ms=$(now_ms)
    log_check="PASS"
    log "checker logs reported $EXPECTED_RULE in $((log_detect_ms - t_inject_ms)) ms"
  else
    log "checker logs did NOT report $EXPECTED_RULE within ${DETECT_TIMEOUT_SEC}s"
  fi
  printf '%s\n' "$log_report" > "$out_dir/checker-report.json"

  local downstream_check="SKIP"
  if [ -n "$EXPECTED_DOWNSTREAM" ]; then
    local source_for_downstream=""
    if [ -n "$detected_json" ] && [ "$status_check" = "PASS" ]; then
      source_for_downstream="$detected_json"
    elif [ -n "$log_report" ]; then
      source_for_downstream="$log_report"
    fi

    if [ -n "$source_for_downstream" ]; then
      downstream_check=$(python3 -c "
import json, sys
expected = [s.strip() for s in sys.argv[1].split(',') if s.strip()]
data = json.loads(sys.argv[2])
seen = set()
for rc in data.get('root_causes', []):
    for eff in rc.get('downstream_effects', []):
        head = eff.split(':', 1)[0] if ':' in eff else eff
        for svc in expected:
            if head == svc or eff.startswith(svc + ':'):
                seen.add(svc)
missing = [s for s in expected if s not in seen]
print('PASS' if not missing else 'FAIL:' + ','.join(missing))
" "$EXPECTED_DOWNSTREAM" "$source_for_downstream")
    else
      downstream_check="FAIL(no-source)"
    fi
  fi

  log "restoring clean baseline"
  restore_conf
  local restore_baseline_ms
  restore_baseline_ms=$(now_ms)

  local restore_check="FAIL"
  local restore_detect_ms=""
  if poll_for_clean "$RESTORE_TIMEOUT_SEC" > "$out_dir/post-restore.json"; then
    restore_detect_ms=$(now_ms)
    restore_check="PASS"
    log "status clean again in $((restore_detect_ms - restore_baseline_ms)) ms"
  else
    log "status did NOT return to clean within ${RESTORE_TIMEOUT_SEC}s"
  fi

  local status_ms_field="null"
  local log_ms_field="null"
  local restore_ms_field="null"
  [ -n "$status_detect_ms" ] && status_ms_field=$((status_detect_ms - t_inject_ms))
  [ -n "$log_detect_ms" ] && log_ms_field=$((log_detect_ms - t_inject_ms))
  [ -n "$restore_detect_ms" ] && restore_ms_field=$((restore_detect_ms - restore_baseline_ms))

  cat > "$out_dir/timing.json" <<EOF
{
  "scenario": "$scenario",
  "expected_rule": "$EXPECTED_RULE",
  "expected_severity": "$EXPECTED_SEVERITY",
  "expected_downstream": "$EXPECTED_DOWNSTREAM",
  "heartbeat_seconds": $HEARTBEAT,
  "t_inject_ms": $t_inject_ms,
  "t_status_detect_latency_ms": $status_ms_field,
  "t_log_detect_latency_ms": $log_ms_field,
  "t_restore_detect_latency_ms": $restore_ms_field,
  "status_check": "$status_check",
  "log_check": "$log_check",
  "severity_check": "$severity_check",
  "downstream_check": "$downstream_check",
  "restore_check": "$restore_check"
}
EOF

  cat > "$out_dir/scenario.txt" <<EOF
scenario:        $scenario
description:     $SCENARIO_DESCRIPTION
expected_rule:   $EXPECTED_RULE
expected_sev:    $EXPECTED_SEVERITY
expected_downst: $EXPECTED_DOWNSTREAM
status check:    $status_check (latency: ${status_ms_field} ms)
log streaming:   $log_check     (latency: ${log_ms_field} ms)
severity:        $severity_check
downstream:      $downstream_check
restore:         $restore_check (latency: ${restore_ms_field} ms)
heartbeat:       ${HEARTBEAT}s
EOF

  local row_status="PASS"
  if [ "$status_check" != "PASS" ] \
     || [ "$severity_check" != "PASS" ] \
     || [ "$restore_check" != "PASS" ]; then
    row_status="FAIL"
  fi
  if [ "$downstream_check" != "PASS" ] && [ "$downstream_check" != "SKIP" ]; then
    row_status="FAIL"
  fi
  SUMMARY_ROWS+=("$scenario|$row_status|$EXPECTED_RULE|$EXPECTED_SEVERITY|$status_ms_field|$log_ms_field")
}

# ---------------------------------------------------------------------------
# Main flow
# ---------------------------------------------------------------------------

preflight
snapshot_conf

log "verifying clean baseline produces zero rule failures..."
baseline_json=$(status_in_checker || true)
printf '%s\n' "$baseline_json" > "$RUN_DIR/baseline.json"
baseline_clean=$(is_status_clean "$baseline_json")

if [ "$baseline_clean" != "1" ]; then
  fail "baseline cluster is NOT clean — fix before evaluating"
  echo "$baseline_json" | python3 -m json.tool 2>/dev/null | head -60
  exit 3
fi
log "baseline clean"

for s_dir in "$SCENARIOS_DIR"/*/; do
  [ -d "$s_dir" ] || continue
  scenario=$(basename "$s_dir")
  run_scenario "$scenario"
done

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------

passed=0
failed=0
{
  printf '['
  first=1
  for row in "${SUMMARY_ROWS[@]}"; do
    IFS='|' read -r scen status rule sev status_ms log_ms <<<"$row"
    [ "$status" = "PASS" ] && passed=$((passed + 1)) || failed=$((failed + 1))
    [ $first -eq 0 ] && printf ','
    first=0
    printf '\n  {"scenario":"%s","status":"%s","rule":"%s","severity":"%s","status_detect_latency_ms":%s,"log_detect_latency_ms":%s}' \
      "$scen" "$status" "$rule" "$sev" "${status_ms:-null}" "${log_ms:-null}"
  done
  printf '\n]\n'
} > "$RUN_DIR/summary.json"

{
  printf 'evaluation summary — %s\n' "$RUN_ID"
  printf '%s\n' "----------------------------------------------------------------------"
  printf '%-32s %-6s %-9s %-12s %-12s\n' "scenario" "status" "severity" "status_ms" "log_ms"
  printf '%s\n' "----------------------------------------------------------------------"
  for row in "${SUMMARY_ROWS[@]}"; do
    IFS='|' read -r scen status rule sev status_ms log_ms <<<"$row"
    printf '%-32s %-6s %-9s %-12s %-12s\n' \
      "$scen" "$status" "$sev" "${status_ms:-?}" "${log_ms:-?}"
  done
  printf '%s\n' "----------------------------------------------------------------------"
  printf 'passed: %d   failed: %d   total: %d\n' "$passed" "$failed" $(( passed + failed ))
  printf 'heartbeat: %ds\n' "$HEARTBEAT"
} > "$RUN_DIR/summary.txt"

cat "$RUN_DIR/summary.txt"

ln -sfn "$RUN_ID" "$RESULTS_ROOT/latest"
log "results: $RUN_DIR (and tests/results/latest -> $RUN_ID)"

[ "$failed" -eq 0 ] && exit 0 || exit 1