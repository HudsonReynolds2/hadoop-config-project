"""Missing-key behaviour — observable distinction between pass and skip.

Stage 2.2 closed the tool gap noted in this file's previous incarnation:
``ValidationResult`` now carries a ``status`` field with explicit values
``"pass" | "fail" | "skip"``. A dashboard that only looks at ``passed``
no longer mistakes a skip for a real pass.

These tests pin the new contract:

    pass  — status="pass", passed=True   (rule evaluated, constraint held)
    fail  — status="fail", passed=False  (rule evaluated, constraint violated)
    skip  — status="skip", passed=True   (rule could not be evaluated)

The ``passed`` field is kept around for backwards compatibility (callers
that built dashboards on the old contract still work — a skip still
counts as "not failing"). New callers should branch on ``status``.
"""

from __future__ import annotations

from pathlib import Path

from checker.agent import collect_all
from checker.analysis.validator import (
    STATUS_FAIL,
    STATUS_PASS,
    STATUS_SKIP,
    load_rules,
    validate,
)
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
# Constraint rules with a missing key — status must be "skip".
# ---------------------------------------------------------------------------


def test_constraint_rule_with_missing_primary_key_is_skipped() -> None:
    """``hdfs-replication-max`` references ``dfs.replication``. With that
    key absent, status must be 'skip'. ``passed`` stays True for
    backwards compatibility (skip is not a failure)."""
    store = _store_with({"dfs.replication.max": "3"})  # primary missing
    results = validate(_rules(), store)
    rule = next(r for r in results if r.rule_id == "hdfs-replication-max")
    assert rule.status == STATUS_SKIP
    assert rule.passed is True  # backwards-compat
    assert "rule skipped" in rule.details
    assert "dfs.replication" in rule.details


def test_constraint_rule_with_missing_target_key_is_skipped() -> None:
    """Mirror of the above — the target key is the one missing."""
    store = _store_with({"dfs.replication": "1"})  # target missing
    results = validate(_rules(), store)
    rule = next(r for r in results if r.rule_id == "hdfs-replication-max")
    assert rule.status == STATUS_SKIP
    assert rule.passed is True
    assert "rule skipped" in rule.details
    assert "dfs.replication.max" in rule.details


def test_constraint_rule_with_both_keys_present_actually_evaluates() -> None:
    """Sanity check: the skip branch is real, not a catch-all. With both
    keys present and the constraint holding, status='pass'."""
    store = _store_with({"dfs.replication": "2", "dfs.replication.max": "3"})
    results = validate(_rules(), store)
    rule = next(r for r in results if r.rule_id == "hdfs-replication-max")
    assert rule.status == STATUS_PASS
    assert rule.passed is True
    assert "rule skipped" not in rule.details
    # The OK path formats as "key=val <= target_key=target: OK"
    assert ": OK" in rule.details


def test_constraint_rule_violated_is_fail() -> None:
    """Symmetry check: when the constraint is violated, status='fail'."""
    store = _store_with({"dfs.replication": "10", "dfs.replication.max": "3"})
    results = validate(_rules(), store)
    rule = next(r for r in results if r.rule_id == "hdfs-replication-max")
    assert rule.status == STATUS_FAIL
    assert rule.passed is False


# ---------------------------------------------------------------------------
# Propagation (must_contain_value_of) with missing keys.
# ---------------------------------------------------------------------------


def test_must_contain_with_missing_key_is_skipped() -> None:
    store = _store_with({"fs.defaultFS": "hdfs://nn:8020"})  # warehouse missing
    results = validate(_rules(), store)
    rule = next(r for r in results if r.rule_id == "hive-warehouse-namenode")
    assert rule.status == STATUS_SKIP
    assert rule.passed is True
    assert "rule skipped" in rule.details
    assert "hive.metastore.warehouse.dir" in rule.details


def test_must_contain_with_missing_reference_key_is_skipped() -> None:
    store = _store_with(
        {"hive.metastore.warehouse.dir": "hdfs://nn:8020/warehouse"},
    )  # fs.defaultFS missing
    results = validate(_rules(), store)
    rule = next(r for r in results if r.rule_id == "hive-warehouse-namenode")
    assert rule.status == STATUS_SKIP
    assert rule.passed is True
    assert "rule skipped" in rule.details
    assert "fs.defaultFS" in rule.details


# ---------------------------------------------------------------------------
# Multi-service propagation with < 2 services reporting the key.
# ---------------------------------------------------------------------------


def test_multi_service_propagation_with_no_matches_is_skipped() -> None:
    """``fs-defaultfs-propagation`` lists multiple services; if no
    snapshot in the store contains the key at all, the validator records
    that zero of the listed services reported the key and marks the rule
    as skipped (cannot evaluate — nothing to compare)."""
    store = _store_with({"unrelated.key": "x"})
    results = validate(_rules(), store)
    rule = next(r for r in results if r.rule_id == "fs-defaultfs-propagation")
    assert rule.status == STATUS_SKIP
    assert rule.passed is True
    assert "nothing to compare" in rule.details


def test_multi_service_propagation_falls_back_to_global_lookup(
    conf_dir: Path, hadoop_env_path: Path
) -> None:
    """Architectural quirk worth pinning: when most services aren't
    publishing strict snapshots, a single namenode-tagged snapshot
    satisfies every service via the fallback path. Every service
    resolves to the same value — the rule passes trivially (status='pass').

    Stage 2 fix note: this used to be brittle because ``_find_key_by_service``
    fell back silently and could mix sources. The new
    ``_find_key_strict_service`` distinguishes services that ARE
    publishing (compared strictly) from services that aren't (resolved
    via fallback). With only one service in the store, only one strict
    value exists, and the rule lands in the legacy single-strict path
    that passes trivially. Documented here so a future change to the
    fallback behaviour triggers a review of this rule."""
    store = SnapshotStore()
    for s in collect_all(str(conf_dir), "namenode", str(hadoop_env_path)):
        store.put(s)
    # Only namenode-tagged snapshots — other services never publish.

    results = validate(_rules(), store)
    rule = next(r for r in results if r.rule_id == "fs-defaultfs-propagation")
    assert rule.status == STATUS_PASS
    assert rule.passed is True
    # The fallback resolves every service to the namenode snapshot, so
    # the details line says "agrees across" all services.
    assert "agrees across" in rule.details
    for svc in ["namenode", "resourcemanager", "nodemanager", "hive-server2"]:
        assert svc in rule.details
