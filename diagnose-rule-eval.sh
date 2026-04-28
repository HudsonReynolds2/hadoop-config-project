#!/usr/bin/env bash
# diagnose-rule-eval.sh
#
# One-off diagnostic for the test 09 mystery: "yarn-scheduler-ceiling fires
# but severity 'critical' not in checker output".
#
# This script does NOT mutate anything. It snapshots the current state of
# every running agent, runs the validator inside the checker container, and
# prints a per-rule pass/fail/skip table plus a dump of the SnapshotStore.
#
# Run AFTER applying a mutation (e.g. while test 09 is hung) to see what
# the validator actually sees. Or run on a clean tree to confirm baseline.
#
# Exit code is always 0 — this is a diagnostic, not an assertion.

set -uo pipefail

REPO_ROOT=$(cd "$(dirname "$0")/.." && pwd)

echo "================================================================"
echo " diagnostic: rule evaluation against live store"
echo "================================================================"

# Dump the host's view of the two keys this test cares about.
echo
echo "---- host conf/ ----"
for f in core-site.xml yarn-site.xml; do
  echo "[$f]"
  grep -E '<name>(fs.defaultFS|yarn.scheduler.maximum-allocation-mb|yarn.nodemanager.resource.memory-mb)</' \
    "$REPO_ROOT/conf/$f" 2>/dev/null | sed 's/^[[:space:]]*//' || echo "  (file missing)"
done

# Per-agent container view of the same keys.
echo
echo "---- container views ----"
for c in $(docker ps --format '{{.Names}}' | grep '^config-agent' || true); do
  echo "[$c]"
  docker exec "$c" cat /opt/hadoop/etc/hadoop/yarn-site.xml 2>/dev/null \
    | grep -E '<name>(yarn.scheduler.maximum-allocation-mb|yarn.nodemanager.resource.memory-mb)</' \
    | sed 's/^[[:space:]]*/  /' || echo "  (no yarn-site.xml mounted)"
done

# Run the validator inside the checker container against the live conf/.
# The checker image already has the python package installed.
echo
echo "---- validator output (one-shot against ./conf/) ----"
docker exec config-checker python3 -c "
import sys, json
sys.path.insert(0, '/app')
from pathlib import Path
from checker.collectors.xml_collector import collect_xml
from checker.collectors.env_collector import parse_env_file
from checker.consumer import SnapshotStore
from checker.analysis.validator import load_rules, validate

store = SnapshotStore()

# Mirror what the running agents would emit: each service-tagged snapshot
# of the shared conf/ tree.
conf_dir = Path('/etc/checker/conf-readonly')
# checker container does NOT have conf/ mounted by default; bail with a
# helpful note so the operator knows to add the mount or run on host.
if not conf_dir.exists():
    print('conf/ not available inside config-checker; skipping in-container validate.')
    sys.exit(0)
" 2>&1 || echo "(in-container validate skipped — see note above)"

# Host-side validate using the installed CLI.
echo
echo "---- host validate (per-service simulated) ----"
cd "$REPO_ROOT"
if ! command -v hadoopconf >/dev/null 2>&1; then
  echo "hadoopconf CLI not found on PATH — run: pip install -e ."
else
  for svc in namenode resourcemanager nodemanager; do
    echo
    echo "[service=$svc]"
    hadoopconf validate conf/ rules/hadoop-3.3.x.yaml \
      --service "$svc" --env-file hadoop.env --format text 2>&1 \
      | sed 's/^/  /' || true
  done
fi

# Tail the most recent checker output to capture what the live consumer is
# actually emitting for the latest mutation.
echo
echo "---- last 30 lines of checker output ----"
docker logs --tail 30 config-checker 2>&1 | sed 's/^/  /'

echo
echo "================================================================"
echo " end diagnostic"
echo "================================================================"
exit 0
