#!/usr/bin/env bash
# demo.sh — fast, hang-proof walkthrough for hadoop-config-checker.
# Targets config-checker, polls for state changes, never sleeps blindly.

set -uo pipefail

REPO_ROOT="$HOME/EC528/hadoop-config-project"
cd "$REPO_ROOT"
unset CHECKER_KAFKA_BOOTSTRAP 2>/dev/null || true

CHECKER="config-checker"
RULES="/etc/checker/rules/hadoop-3.3.x.yaml"
STATUS_TIMEOUT=5
DOCKER_EXEC_TIMEOUT=30
PROPAGATE_TIMEOUT=45
PREFLIGHT_TIMEOUT=90
POLL_INTERVAL=2

# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

bounded() { local s="$1"; shift; timeout --kill-after=5 "$s" "$@"; }

inplace_sed() {
  local expr="$1" file="$2"
  local tmp; tmp=$(mktemp)
  sed "$expr" "$file" > "$tmp"
  cat "$tmp" > "$file"
  rm -f "$tmp"
}

# Run hadoopconf status. Stderr is dropped so kafka-python INFO logs
# don't pollute the recording.
status_run() {
  local fmt="$1"
  bounded "$DOCKER_EXEC_TIMEOUT" docker exec "$CHECKER" \
    hadoopconf status --rules "$RULES" \
    --timeout "$STATUS_TIMEOUT" --format "$fmt" 2>/dev/null
}

status_text()    { status_run text; }
status_json()    { status_run json; }
status_capture() { status_run text; }

wait_for_drift() {
  local label="$1"
  local deadline=$(( $(date +%s) + PROPAGATE_TIMEOUT ))
  echo "  waiting for drift to propagate (max ${PROPAGATE_TIMEOUT}s)..."
  while [ "$(date +%s)" -lt "$deadline" ]; do
    if status_capture | grep -q "DRIFT DETECTED"; then
      echo "  drift observed."
      return 0
    fi
    sleep "$POLL_INTERVAL"
  done
  echo "  WARN: ${label} drift not observed within ${PROPAGATE_TIMEOUT}s."
  return 1
}

wait_for_clean() {
  local deadline=$(( $(date +%s) + PROPAGATE_TIMEOUT ))
  echo "  waiting for cluster to return to clean (max ${PROPAGATE_TIMEOUT}s)..."
  while [ "$(date +%s)" -lt "$deadline" ]; do
    if status_capture | grep -q "^CLEAN\\."; then
      echo "  clean observed."
      return 0
    fi
    sleep "$POLL_INTERVAL"
  done
  echo "  WARN: clean state not observed within ${PROPAGATE_TIMEOUT}s."
  return 1
}

section() { echo ""; echo "=== $* ==="; }

# ---------------------------------------------------------------------------
# preflight: wait until the checker is actually receiving snapshots
# ---------------------------------------------------------------------------

section "Preflight: waiting for checker to receive snapshots"
deadline=$(( $(date +%s) + PREFLIGHT_TIMEOUT ))
ready=0
last_out=""
while [ "$(date +%s)" -lt "$deadline" ]; do
  last_out=$(status_capture || true)
  if echo "$last_out" | grep -qE "^Snapshots received: [1-9]"; then
    ready=1
    break
  fi
  echo "  waiting for snapshots..."
  sleep "$POLL_INTERVAL"
done

if [ "$ready" -ne 1 ]; then
  echo "ERROR: no snapshots arrived within ${PREFLIGHT_TIMEOUT}s."
  echo "Last status output:"
  echo "$last_out"
  echo ""
  echo "Recent checker logs:"
  docker logs --tail=20 "$CHECKER" 2>&1 | tail -20 || true
  exit 1
fi
echo "  checker is receiving snapshots."

# ---------------------------------------------------------------------------
# 1. running stack
# ---------------------------------------------------------------------------

section "1. Running containers"
docker compose ps --format 'table {{.Name}}\t{{.Service}}\t{{.Status}}' | head -25

# ---------------------------------------------------------------------------
# 2. baseline
# ---------------------------------------------------------------------------

section "2. Current cluster status (text)"
status_text || true

section "2b. Same status as JSON"
status_json || true

# ---------------------------------------------------------------------------
# 3. inject fs.defaultFS bug
# ---------------------------------------------------------------------------

section "3. Injecting bug: fs.defaultFS -> hdfs://wronghost:8020"
inplace_sed 's|hdfs://namenode:8020|hdfs://wronghost:8020|' conf/core-site.xml
grep fs.defaultFS conf/core-site.xml || true
wait_for_drift "fs.defaultFS" || true

section "3b. Drift detected (text)"
status_text || true

section "3c. Drift detected (JSON, root_causes shows graph trace)"
status_json || true

# ---------------------------------------------------------------------------
# 4. restore
# ---------------------------------------------------------------------------

section "4. Restoring conf/core-site.xml"
inplace_sed 's|hdfs://wronghost:8020|hdfs://namenode:8020|' conf/core-site.xml
wait_for_clean || true

section "4b. Confirm clean after restore"
status_text || true

# ---------------------------------------------------------------------------
# 5. cross-service bug (Hive warehouse -> wrong namenode)
# ---------------------------------------------------------------------------

if [ -f tests/configs/buggy/hive-warehouse-wrong-namenode/hive-site.xml ]; then
  section "5. Cross-service bug: Hive warehouse points at wrong namenode"
  cp conf/hive-site.xml /tmp/hive-site.xml.bak
  cp tests/configs/buggy/hive-warehouse-wrong-namenode/hive-site.xml conf/hive-site.xml
  grep -A1 hive.metastore.warehouse.dir conf/hive-site.xml || true
  wait_for_drift "hive warehouse" || true

  section "5b. Cross-service drift detected (text)"
  status_text || true

  section "5c. Cross-service drift (JSON)"
  status_json || true

  section "5d. Restoring conf/hive-site.xml"
  cp /tmp/hive-site.xml.bak conf/hive-site.xml
  rm -f /tmp/hive-site.xml.bak
  wait_for_clean || true

  section "5e. Confirm clean"
  status_text || true
fi

# ---------------------------------------------------------------------------
# 6. local-only validate
# ---------------------------------------------------------------------------

section "6. Local validate against clean conf"
hadoopconf validate tests/configs/clean rules/hadoop-3.3.x.yaml --service demo || true

if [ -d tests/configs/buggy/replication-exceeds-max ]; then
  section "6b. Local validate against buggy conf (should fail)"
  hadoopconf validate tests/configs/buggy/replication-exceeds-max \
    rules/hadoop-3.3.x.yaml --service demo || echo "(exit code: $?)"
fi

# ---------------------------------------------------------------------------
# 7. host-side status via external Kafka listener
# ---------------------------------------------------------------------------

section "7. Host-side status via localhost:9094"
bounded "$DOCKER_EXEC_TIMEOUT" hadoopconf status \
  --bootstrap localhost:9094 \
  --rules rules/hadoop-3.3.x.yaml \
  --timeout "$STATUS_TIMEOUT" --format text 2>/dev/null || \
  echo "(host-side status unavailable — continuing)"

section "Done"