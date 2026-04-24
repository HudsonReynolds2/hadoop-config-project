"""Missing-key behaviour — observable distinction between pass and skip.

The validator's current contract
([validator.py](../../checker/analysis/validator.py) §_eval_constraint etc.)
is that when a required key is missing the rule returns ``passed=True``
with ``details`` containing ``"rule skipped"``. That conflates two very
different outcomes:

    pass  — rule was evaluated, constraint held
    skip  — rule could not be evaluated, we know nothing about the config

A dashboard that only looks at ``passed`` marks the cluster healthy in
both cases, which is wrong for the skip case. These tests pin the current
(silent-pass) behaviour and document via ``details`` substring matches
the only handle the caller has today.

**Tool gap** (see [testing-upgrades.md](../../testing-upgrades.md) Tier D):
add ``status: "pass" | "fail" | "skip"`` to ``ValidationResult`` so skips
are explicit.
"""

from __future__ import annotations

from pathlib import Path

from checker.agent import collect_all
from checker.analysis.validator import load_rules, validate
from checker.consumer import SnapshotStore
from checker.models import ConfigSnapshot, SOURCE_XML_FILE


def _rules():
    return load_rules(Path(__file__).parent / "fixtures" / "hadoop-3.3.x.yaml")


def _store_with(properties: dict, service: str = "namenode") -> SnapshotStore:
    store = SnapshotStore()
    store.put(ConfigSnapshot(
        agent_id=f"{service}-synth", service=service,
        source=SOURCE_XML_FILE, source_path="synthetic.xml",
        host="test", properties=properties,
    ))
    return store


# ---------------------------------------------------------------------------
# Constraint rules with a missing key — currently pass with "rule skipped".
# ---------------------------------------------------------------------------


def test_constraint_rule_with_missing_primary_key_is_skipped() -> None:
    """``hdfs-replication-max`` references ``dfs.replication``. With that
    key absent from the store, the rule reports passed=True and the
    reason should be discoverable via ``details``."""
    store = _store_with({"dfs.replication.max": "3"})  # primary missing
    results = validate(_rules(), store)
    rule = next(r for r in results if r.rule_id == "hdfs-replication-max")
    assert rule.passed is True  # current (silent-skip) behaviour
    assert "rule skipped" in rule.details
    assert "dfs.replication" in rule.details


def test_constraint_rule_with_missing_target_key_is_skipped() -> None:
    """Mirror of the above — the target key is the one missing."""
    store = _store_with({"dfs.replication": "1"})  # target missing
    results = validate(_rules(), store)
    rule = next(r for r in results if r.rule_id == "hdfs-replication-max")
    assert rule.passed is True
    assert "rule skipped" in rule.details
    assert "dfs.replication.max" in rule.details


def test_constraint_rule_with_both_keys_present_actually_evaluates() -> None:
    """Sanity check: the skip branch is real, not a catch-all."""
    store = _store_with({"dfs.replication": "2", "dfs.replication.max": "3"})
    results = validate(_rules(), store)
    rule = next(r for r in results if r.rule_id == "hdfs-replication-max")
    assert rule.passed is True
    assert "rule skipped" not in rule.details
    # The OK path formats as "key=val <= target_key=target: OK"
    assert ": OK" in rule.details


# ---------------------------------------------------------------------------
# Propagation (must_contain_value_of) with missing keys.
# ---------------------------------------------------------------------------


def test_must_contain_with_missing_key_is_skipped() -> None:
    store = _store_with({"fs.defaultFS": "hdfs://nn:8020"})  # warehouse missing
    results = validate(_rules(), store)
    rule = next(r for r in results if r.rule_id == "hive-warehouse-namenode")
    assert rule.passed is True
    assert "rule skipped" in rule.details
    assert "hive.metastore.warehouse.dir" in rule.details


def test_must_contain_with_missing_reference_key_is_skipped() -> None:
    store = _store_with(
        {"hive.metastore.warehouse.dir": "hdfs://nn:8020/warehouse"},
    )  # fs.defaultFS missing
    results = validate(_rules(), store)
    rule = next(r for r in results if r.rule_id == "hive-warehouse-namenode")
    assert rule.passed is True
    assert "rule skipped" in rule.details
    assert "fs.defaultFS" in rule.details


# ---------------------------------------------------------------------------
# Multi-service propagation with < 2 services reporting the key.
# ---------------------------------------------------------------------------


def test_multi_service_propagation_with_no_matches_passes_trivially() -> None:
    """``fs-defaultfs-propagation`` lists 4 services; if no snapshot in the
    store contains the key at all, the validator records that zero of the
    listed services reported the key and passes with a 'nothing to compare'
    details message."""
    store = _store_with({"unrelated.key": "x"})
    results = validate(_rules(), store)
    rule = next(r for r in results if r.rule_id == "fs-defaultfs-propagation")
    assert rule.passed is True
    assert "nothing to compare" in rule.details


def test_multi_service_propagation_falls_back_to_global_lookup(
    conf_dir: Path, hadoop_env_path: Path
) -> None:
    """Architectural quirk worth pinning: ``_find_key_by_service`` falls
    back to searching *all* snapshots when the preferred service has no
    match. With a single-service store, the fallback makes every listed
    service report the same value — the rule passes trivially.

    This is why ``fs-defaultfs-propagation`` can only fire when different
    services see *different* snapshots for the key (e.g. per-service conf
    divergence), not when some services simply don't publish at all.
    Documented here so that a future change to the fallback behaviour
    triggers a review of this rule."""
    store = SnapshotStore()
    for s in collect_all(str(conf_dir), "namenode", str(hadoop_env_path)):
        store.put(s)
    # Only namenode-tagged snapshots — other 3 services never publish.

    results = validate(_rules(), store)
    rule = next(r for r in results if r.rule_id == "fs-defaultfs-propagation")
    assert rule.passed is True
    # The fallback resolves every service to the namenode snapshot, so the
    # details line says "agrees across" all 4 services.
    assert "agrees across" in rule.details
    for svc in ["namenode", "resourcemanager", "nodemanager", "hive-server2"]:
        assert svc in rule.details
