"""Tests for ``checker.analysis.validator``.

Covers constraint rules, propagation rules, must-contain rules,
dual-source-consistency delegation, and the acceptance test from plan.md:
yarn-scheduler-ceiling passes with real values and fails when swapped.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from checker.analysis.validator import (
    ValidationResult,
    load_rules,
    validate,
    validate_from_file,
    _find_key,
)
from checker.consumer import SnapshotStore
from checker.models import ConfigSnapshot, SOURCE_ENV_FILE, SOURCE_XML_FILE


def _snap(
    agent_id: str = "test-snap",
    service: str = "namenode",
    source: str = SOURCE_XML_FILE,
    source_path: str = "conf/test.xml",
    properties: dict | None = None,
) -> ConfigSnapshot:
    return ConfigSnapshot(
        agent_id=agent_id,
        service=service,
        source=source,
        source_path=source_path,
        host="test-host",
        timestamp="2026-04-23T12:00:00Z",
        properties=properties or {},
    )


def _store_with(*snapshots: ConfigSnapshot) -> SnapshotStore:
    store = SnapshotStore()
    for snap in snapshots:
        store.put(snap)
    return store


# ---------------------------------------------------------------------------
# Rule loading
# ---------------------------------------------------------------------------


class TestLoadRules:
    def test_load_real_rules_file(self) -> None:
        # The real rule file lives at project root / rules/hadoop-3.3.x.yaml
        # but in tests we use the fixture copy.
        rules_path = Path(__file__).parent / "fixtures" / "hadoop-3.3.x.yaml"
        if not rules_path.exists():
            pytest.skip("fixture rule file not present")
        rules = load_rules(rules_path)
        assert len(rules) == 5
        ids = {r["id"] for r in rules}
        assert "yarn-scheduler-ceiling" in ids
        assert "dual-source-consistency" in ids

    def test_missing_file_raises(self) -> None:
        with pytest.raises(FileNotFoundError):
            load_rules("/nonexistent/rules.yaml")

    def test_bad_format_raises(self, tmp_path: Path) -> None:
        p = tmp_path / "bad.yaml"
        p.write_text("not_rules: true")
        with pytest.raises(ValueError, match="expected top-level 'rules'"):
            load_rules(p)


# ---------------------------------------------------------------------------
# _find_key — key lookup logic
# ---------------------------------------------------------------------------


class TestFindKey:
    def test_finds_key_in_preferred_service(self) -> None:
        store = _store_with(
            _snap(agent_id="a", service="namenode", properties={"k": "nn-val"}),
            _snap(agent_id="b", service="datanode", properties={"k": "dn-val"}),
        )
        assert _find_key(store, "k", preferred_service="namenode") == "nn-val"
        assert _find_key(store, "k", preferred_service="datanode") == "dn-val"

    def test_falls_back_to_any_service(self) -> None:
        store = _store_with(
            _snap(agent_id="a", service="namenode", properties={"k": "val"}),
        )
        # Ask for datanode (doesn't have it) — falls back to namenode.
        assert _find_key(store, "k", preferred_service="datanode") == "val"

    def test_returns_none_if_missing(self) -> None:
        store = _store_with(
            _snap(agent_id="a", properties={"other": "v"}),
        )
        assert _find_key(store, "missing") is None


# ---------------------------------------------------------------------------
# Constraint rules
# ---------------------------------------------------------------------------


class TestConstraintRules:
    def test_lte_passes(self) -> None:
        store = _store_with(_snap(properties={
            "yarn.scheduler.maximum-allocation-mb": "2048",
            "yarn.nodemanager.resource.memory-mb": "4096",
        }))
        rule = {
            "id": "yarn-scheduler-ceiling",
            "description": "scheduler max <= NM memory",
            "type": "constraint",
            "key": "yarn.scheduler.maximum-allocation-mb",
            "service": "resourcemanager",
            "relation": "lte",
            "target_key": "yarn.nodemanager.resource.memory-mb",
            "target_service": "nodemanager",
            "severity": "critical",
        }
        results = validate([rule], store)
        assert len(results) == 1
        assert results[0].passed is True
        assert "OK" in results[0].details

    def test_lte_fails_when_swapped(self) -> None:
        """The plan's acceptance test: swap the values, rule must fail."""
        store = _store_with(_snap(properties={
            "yarn.scheduler.maximum-allocation-mb": "4096",
            "yarn.nodemanager.resource.memory-mb": "2048",
        }))
        rule = {
            "id": "yarn-scheduler-ceiling",
            "description": "scheduler max <= NM memory",
            "type": "constraint",
            "key": "yarn.scheduler.maximum-allocation-mb",
            "service": "resourcemanager",
            "relation": "lte",
            "target_key": "yarn.nodemanager.resource.memory-mb",
            "target_service": "nodemanager",
            "severity": "critical",
        }
        results = validate([rule], store)
        assert len(results) == 1
        assert results[0].passed is False
        assert results[0].severity == "critical"
        assert results[0].drift is not None
        assert results[0].drift.rule_id == "yarn-scheduler-ceiling"

    def test_lte_equal_passes(self) -> None:
        store = _store_with(_snap(properties={
            "yarn.scheduler.maximum-allocation-mb": "4096",
            "yarn.nodemanager.resource.memory-mb": "4096",
        }))
        rule = {
            "id": "test", "type": "constraint",
            "key": "yarn.scheduler.maximum-allocation-mb",
            "relation": "lte",
            "target_key": "yarn.nodemanager.resource.memory-mb",
            "severity": "warning",
        }
        results = validate([rule], store)
        assert results[0].passed is True

    def test_missing_key_skips(self) -> None:
        store = _store_with(_snap(properties={"only": "one"}))
        rule = {
            "id": "test", "type": "constraint",
            "key": "missing.key", "relation": "lte",
            "target_key": "also.missing", "severity": "warning",
        }
        results = validate([rule], store)
        assert results[0].passed is True
        assert "not found" in results[0].details

    def test_non_numeric_fails(self) -> None:
        store = _store_with(_snap(properties={
            "a": "not-a-number", "b": "4096",
        }))
        rule = {
            "id": "test", "type": "constraint",
            "key": "a", "relation": "lte", "target_key": "b",
            "severity": "warning",
        }
        results = validate([rule], store)
        assert results[0].passed is False
        assert "non-numeric" in results[0].details

    def test_gt_relation(self) -> None:
        store = _store_with(_snap(properties={"a": "10", "b": "5"}))
        rule = {
            "id": "test", "type": "constraint",
            "key": "a", "relation": "gt", "target_key": "b",
            "severity": "warning",
        }
        results = validate([rule], store)
        assert results[0].passed is True

    def test_eq_relation_fails(self) -> None:
        store = _store_with(_snap(properties={"a": "10", "b": "20"}))
        rule = {
            "id": "test", "type": "constraint",
            "key": "a", "relation": "eq", "target_key": "b",
            "severity": "warning",
        }
        results = validate([rule], store)
        assert results[0].passed is False


# ---------------------------------------------------------------------------
# Propagation rules — multi-service
# ---------------------------------------------------------------------------


class TestPropagationMultiService:
    def test_all_agree_passes(self) -> None:
        store = _store_with(
            _snap(agent_id="a", service="namenode",
                  properties={"fs.defaultFS": "hdfs://nn:8020"}),
            _snap(agent_id="b", service="resourcemanager",
                  properties={"fs.defaultFS": "hdfs://nn:8020"}),
        )
        rule = {
            "id": "fs-defaultfs-propagation",
            "type": "propagation",
            "key": "fs.defaultFS",
            "services": ["namenode", "resourcemanager"],
            "severity": "critical",
        }
        results = validate([rule], store)
        assert results[0].passed is True

    def test_disagreement_fails(self) -> None:
        store = _store_with(
            _snap(agent_id="a", service="namenode",
                  properties={"fs.defaultFS": "hdfs://nn:8020"}),
            _snap(agent_id="b", service="hive-server2",
                  properties={"fs.defaultFS": "hdfs://wrong:8020"}),
        )
        rule = {
            "id": "fs-defaultfs-propagation",
            "type": "propagation",
            "key": "fs.defaultFS",
            "services": ["namenode", "hive-server2"],
            "severity": "critical",
        }
        results = validate([rule], store)
        assert results[0].passed is False
        assert results[0].drift is not None

    def test_single_service_present_still_passes_via_fallback(self) -> None:
        """When only one service is in the store, the fallback lookup finds
        the key for both services (since they share the same config files
        in a real cluster).  This should pass — same value for both."""
        store = _store_with(
            _snap(agent_id="a", service="namenode",
                  properties={"fs.defaultFS": "hdfs://nn:8020"}),
        )
        rule = {
            "id": "test", "type": "propagation",
            "key": "fs.defaultFS",
            "services": ["namenode", "datanode"],
            "severity": "critical",
        }
        results = validate([rule], store)
        assert results[0].passed is True

    def test_key_absent_everywhere_skips(self) -> None:
        """When the key doesn't exist in any snapshot, nothing to compare."""
        store = _store_with(
            _snap(agent_id="a", service="namenode",
                  properties={"other.key": "val"}),
        )
        rule = {
            "id": "test", "type": "propagation",
            "key": "fs.defaultFS",
            "services": ["namenode", "datanode"],
            "severity": "critical",
        }
        results = validate([rule], store)
        assert results[0].passed is True
        assert "nothing to compare" in results[0].details


# ---------------------------------------------------------------------------
# Propagation — must_contain_value_of
# ---------------------------------------------------------------------------


class TestMustContain:
    def test_contains_passes(self) -> None:
        store = _store_with(
            _snap(agent_id="a", service="hive-server2", properties={
                "hive.metastore.warehouse.dir": "hdfs://nn:8020/user/hive/warehouse",
            }),
            _snap(agent_id="b", service="namenode", properties={
                "fs.defaultFS": "hdfs://nn:8020",
            }),
        )
        rule = {
            "id": "hive-warehouse-namenode",
            "type": "propagation",
            "key": "hive.metastore.warehouse.dir",
            "service": "hive-server2",
            "must_contain_value_of": {
                "key": "fs.defaultFS",
                "service": "namenode",
            },
            "severity": "warning",
        }
        results = validate([rule], store)
        assert results[0].passed is True

    def test_not_contains_fails(self) -> None:
        store = _store_with(
            _snap(agent_id="a", service="hive-server2", properties={
                "hive.metastore.warehouse.dir": "hdfs://wrong:9999/user/hive/warehouse",
            }),
            _snap(agent_id="b", service="namenode", properties={
                "fs.defaultFS": "hdfs://nn:8020",
            }),
        )
        rule = {
            "id": "hive-warehouse-namenode",
            "type": "propagation",
            "key": "hive.metastore.warehouse.dir",
            "service": "hive-server2",
            "must_contain_value_of": {
                "key": "fs.defaultFS",
                "service": "namenode",
            },
            "severity": "warning",
        }
        results = validate([rule], store)
        assert results[0].passed is False
        assert results[0].drift is not None


# ---------------------------------------------------------------------------
# Dual-source consistency
# ---------------------------------------------------------------------------


class TestDualSourceRule:
    def test_agreement_passes(self) -> None:
        store = _store_with(
            _snap(agent_id="a", source=SOURCE_XML_FILE,
                  source_path="core-site.xml",
                  properties={"fs.defaultFS": "hdfs://nn:8020"}),
            _snap(agent_id="b", source=SOURCE_ENV_FILE,
                  source_path="hadoop.env",
                  properties={"fs.defaultFS": "hdfs://nn:8020"}),
        )
        rule = {
            "id": "dual-source-consistency",
            "type": "propagation",
            "sources": ["xml_file", "env_file"],
            "severity": "warning",
        }
        results = validate([rule], store)
        assert results[0].passed is True

    def test_disagreement_fails(self) -> None:
        store = _store_with(
            _snap(agent_id="a", source=SOURCE_XML_FILE,
                  source_path="core-site.xml",
                  properties={"fs.defaultFS": "hdfs://nn:8020"}),
            _snap(agent_id="b", source=SOURCE_ENV_FILE,
                  source_path="hadoop.env",
                  properties={"fs.defaultFS": "hdfs://wrong:8020"}),
        )
        rule = {
            "id": "dual-source-consistency",
            "type": "propagation",
            "sources": ["xml_file", "env_file"],
            "severity": "warning",
        }
        results = validate([rule], store)
        assert results[0].passed is False


# ---------------------------------------------------------------------------
# Acceptance test from plan.md — real config files
# ---------------------------------------------------------------------------


class TestAcceptance:
    """The plan says:

    'validator reports the yarn-scheduler-ceiling rule as passing with
    the current cluster values (scheduler.max=2048 ≤ NM memory=4096)
    and correctly flags it as a violation when those values are swapped.'

    These tests use the real config fixtures.
    """

    def test_yarn_scheduler_ceiling_passes_with_real_config(
        self, conf_dir: Path
    ) -> None:
        """Real yarn-site.xml has scheduler.max=2048, NM.memory=4096."""
        from checker.collectors.xml_collector import collect_xml

        snap = collect_xml(conf_dir / "yarn-site.xml", service="namenode")
        store = _store_with(snap)

        rule = {
            "id": "yarn-scheduler-ceiling",
            "description": "scheduler max <= NM memory",
            "type": "constraint",
            "key": "yarn.scheduler.maximum-allocation-mb",
            "service": "resourcemanager",
            "relation": "lte",
            "target_key": "yarn.nodemanager.resource.memory-mb",
            "target_service": "nodemanager",
            "severity": "critical",
        }
        results = validate([rule], store)
        assert results[0].passed is True
        assert "2048" in results[0].details
        assert "4096" in results[0].details

    def test_yarn_scheduler_ceiling_fails_when_swapped(
        self, tmp_path: Path
    ) -> None:
        """Swap the values: scheduler.max=4096, NM.memory=2048 → FAIL."""
        from checker.collectors.xml_collector import collect_xml

        # Write a yarn-site.xml with swapped values.
        swapped = tmp_path / "yarn-site.xml"
        swapped.write_text(
            "<configuration>"
            "<property><name>yarn.scheduler.maximum-allocation-mb</name>"
            "<value>4096</value></property>"
            "<property><name>yarn.nodemanager.resource.memory-mb</name>"
            "<value>2048</value></property>"
            "</configuration>"
        )
        snap = collect_xml(swapped, service="namenode")
        store = _store_with(snap)

        rule = {
            "id": "yarn-scheduler-ceiling",
            "description": "scheduler max <= NM memory",
            "type": "constraint",
            "key": "yarn.scheduler.maximum-allocation-mb",
            "service": "resourcemanager",
            "relation": "lte",
            "target_key": "yarn.nodemanager.resource.memory-mb",
            "target_service": "nodemanager",
            "severity": "critical",
        }
        results = validate([rule], store)
        assert results[0].passed is False
        assert results[0].severity == "critical"
        assert results[0].drift is not None

    def test_full_ruleset_against_real_config(
        self, conf_dir: Path, hadoop_env_path: Path
    ) -> None:
        """Run the entire rule set against the real fixtures.

        With the current config, all rules should pass (the cluster is
        intentionally consistent).
        """
        from checker.collectors.xml_collector import collect_xml
        from checker.collectors.env_collector import parse_env_file

        store = SnapshotStore()
        for xml_file in sorted(conf_dir.glob("*-site.xml")):
            snap = collect_xml(xml_file, service="namenode")
            store.put(snap)
        env_snap = parse_env_file(hadoop_env_path, service="namenode")
        store.put(env_snap)

        rules_path = Path(__file__).parent / "fixtures" / "hadoop-3.3.x.yaml"
        if not rules_path.exists():
            pytest.skip("fixture rule file not present")

        results = validate_from_file(rules_path, store)
        failed = [r for r in results if not r.passed]
        assert failed == [], (
            f"rules failed against real config: "
            f"{[(r.rule_id, r.details) for r in failed]}"
        )


# ---------------------------------------------------------------------------
# Unknown rule type
# ---------------------------------------------------------------------------


class TestUnknownRuleType:
    def test_unknown_type_skipped(self) -> None:
        store = _store_with(_snap())
        rule = {"id": "test", "type": "banana", "severity": "info"}
        results = validate([rule], store)
        assert results[0].passed is True
        assert "unknown rule type" in results[0].details
        