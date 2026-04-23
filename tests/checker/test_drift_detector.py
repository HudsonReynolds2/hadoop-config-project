"""Tests for ``checker.analysis.drift_detector``.

Covers pairwise diff, cross-source consistency, and temporal drift
detection.  Uses synthetic snapshots — no files or Kafka needed.
"""

from __future__ import annotations

import pytest

from checker.analysis.drift_detector import (
    detect,
    detect_cross_source,
    detect_temporal,
)
from checker.models import ConfigSnapshot, SOURCE_ENV_FILE, SOURCE_XML_FILE, SOURCE_JVM_FLAGS


def _snap(
    agent_id: str = "test-core-site",
    service: str = "namenode",
    source: str = SOURCE_XML_FILE,
    source_path: str = "conf/core-site.xml",
    properties: dict | None = None,
) -> ConfigSnapshot:
    """Helper to build a snapshot with sensible defaults."""
    return ConfigSnapshot(
        agent_id=agent_id,
        service=service,
        source=source,
        source_path=source_path,
        host="test-host",
        timestamp="2026-04-23T12:00:00Z",
        properties=properties or {},
    )


# ---------------------------------------------------------------------------
# detect() — pairwise diff
# ---------------------------------------------------------------------------


class TestPairwiseDetect:
    def test_identical_snapshots_produce_no_drift(self) -> None:
        a = _snap(properties={"fs.defaultFS": "hdfs://nn:8020", "dfs.replication": "1"})
        b = _snap(properties={"fs.defaultFS": "hdfs://nn:8020", "dfs.replication": "1"})
        assert detect(a, b) == []

    def test_empty_snapshots_produce_no_drift(self) -> None:
        assert detect(_snap(), _snap()) == []

    def test_value_mismatch_detected(self) -> None:
        a = _snap(properties={"fs.defaultFS": "hdfs://nn:8020"})
        b = _snap(properties={"fs.defaultFS": "hdfs://other:8020"})
        results = detect(a, b)
        assert len(results) == 1
        assert results[0].key == "fs.defaultFS"
        assert results[0].value_a == "hdfs://nn:8020"
        assert results[0].value_b == "hdfs://other:8020"

    def test_key_present_in_a_only(self) -> None:
        a = _snap(properties={"fs.defaultFS": "hdfs://nn:8020", "extra": "val"})
        b = _snap(properties={"fs.defaultFS": "hdfs://nn:8020"})
        results = detect(a, b)
        assert len(results) == 1
        assert results[0].key == "extra"
        assert results[0].value_a == "val"
        assert results[0].value_b is None

    def test_key_present_in_b_only(self) -> None:
        a = _snap(properties={"fs.defaultFS": "hdfs://nn:8020"})
        b = _snap(properties={"fs.defaultFS": "hdfs://nn:8020", "extra": "val"})
        results = detect(a, b)
        assert len(results) == 1
        assert results[0].key == "extra"
        assert results[0].value_a is None
        assert results[0].value_b == "val"

    def test_multiple_drifts_sorted_by_key(self) -> None:
        a = _snap(properties={"z.key": "1", "a.key": "2", "m.key": "3"})
        b = _snap(properties={"z.key": "X", "a.key": "Y", "m.key": "3"})
        results = detect(a, b)
        assert len(results) == 2
        assert results[0].key == "a.key"
        assert results[1].key == "z.key"

    def test_severity_passed_through(self) -> None:
        a = _snap(properties={"k": "1"})
        b = _snap(properties={"k": "2"})
        results = detect(a, b, severity="critical")
        assert results[0].severity == "critical"

    def test_rule_id_passed_through(self) -> None:
        a = _snap(properties={"k": "1"})
        b = _snap(properties={"k": "2"})
        results = detect(a, b, rule_id="my-rule")
        assert results[0].rule_id == "my-rule"

    def test_default_rule_id_is_none(self) -> None:
        a = _snap(properties={"k": "1"})
        b = _snap(properties={"k": "2"})
        results = detect(a, b)
        assert results[0].rule_id is None

    def test_source_labels_formatted_correctly(self) -> None:
        a = _snap(source=SOURCE_XML_FILE, source_path="conf/core-site.xml",
                  properties={"k": "1"})
        b = _snap(source=SOURCE_ENV_FILE, source_path="hadoop.env",
                  properties={"k": "2"})
        results = detect(a, b)
        assert results[0].source_a == "xml_file:conf/core-site.xml"
        assert results[0].source_b == "env_file:hadoop.env"

    def test_empty_string_vs_value_is_drift(self) -> None:
        a = _snap(properties={"k": ""})
        b = _snap(properties={"k": "val"})
        results = detect(a, b)
        assert len(results) == 1

    def test_empty_string_vs_empty_string_is_not_drift(self) -> None:
        a = _snap(properties={"k": ""})
        b = _snap(properties={"k": ""})
        assert detect(a, b) == []


# ---------------------------------------------------------------------------
# detect_cross_source()
# ---------------------------------------------------------------------------


class TestCrossSourceDetect:
    def test_no_overlap_no_drift(self) -> None:
        """Keys in different sources but no overlapping key names."""
        a = _snap(source=SOURCE_XML_FILE, source_path="core-site.xml",
                  properties={"xml.only": "1"})
        b = _snap(source=SOURCE_ENV_FILE, source_path="hadoop.env",
                  properties={"env.only": "2"})
        assert detect_cross_source([a, b]) == []

    def test_overlapping_keys_agree_no_drift(self) -> None:
        a = _snap(source=SOURCE_XML_FILE, source_path="core-site.xml",
                  properties={"fs.defaultFS": "hdfs://nn:8020"})
        b = _snap(source=SOURCE_ENV_FILE, source_path="hadoop.env",
                  properties={"fs.defaultFS": "hdfs://nn:8020"})
        assert detect_cross_source([a, b]) == []

    def test_overlapping_keys_disagree_detected(self) -> None:
        a = _snap(source=SOURCE_XML_FILE, source_path="core-site.xml",
                  properties={"fs.defaultFS": "hdfs://nn:8020"})
        b = _snap(source=SOURCE_ENV_FILE, source_path="hadoop.env",
                  properties={"fs.defaultFS": "hdfs://other:8020"})
        results = detect_cross_source([a, b])
        assert len(results) == 1
        assert results[0].key == "fs.defaultFS"
        assert results[0].rule_id == "dual-source-consistency"

    def test_same_source_type_not_compared(self) -> None:
        """Two XML snapshots from the same service are NOT cross-source."""
        a = _snap(source=SOURCE_XML_FILE, source_path="core-site.xml",
                  properties={"fs.defaultFS": "hdfs://nn:8020"})
        b = _snap(source=SOURCE_XML_FILE, source_path="hdfs-site.xml",
                  properties={"fs.defaultFS": "hdfs://other:8020"})
        assert detect_cross_source([a, b]) == []

    def test_different_services_not_compared(self) -> None:
        """Cross-source only compares within the same service."""
        a = _snap(service="namenode", source=SOURCE_XML_FILE,
                  source_path="core-site.xml",
                  properties={"fs.defaultFS": "hdfs://nn:8020"})
        b = _snap(service="datanode", source=SOURCE_ENV_FILE,
                  source_path="hadoop.env",
                  properties={"fs.defaultFS": "hdfs://other:8020"})
        assert detect_cross_source([a, b]) == []

    def test_three_sources_all_pairs_checked(self) -> None:
        """XML, env, and JVM flags for the same service — all pairs compared."""
        xml = _snap(source=SOURCE_XML_FILE, source_path="core-site.xml",
                    properties={"fs.defaultFS": "hdfs://nn:8020"})
        env = _snap(source=SOURCE_ENV_FILE, source_path="hadoop.env",
                    properties={"fs.defaultFS": "hdfs://nn:8020"})
        jvm = _snap(source=SOURCE_JVM_FLAGS, source_path="SERVICE_OPTS",
                    properties={"fs.defaultFS": "hdfs://wrong:8020"})
        results = detect_cross_source([xml, env, jvm])
        # JVM disagrees with both XML and env = 2 drifts.
        assert len(results) == 2
        keys = {(r.source_a, r.source_b) for r in results}
        assert any("xml_file" in a and "jvm_flags" in b for a, b in keys)
        assert any("env_file" in a and "jvm_flags" in b for a, b in keys)

    def test_single_snapshot_no_drift(self) -> None:
        assert detect_cross_source([_snap()]) == []

    def test_empty_list_no_drift(self) -> None:
        assert detect_cross_source([]) == []

    def test_real_dual_source_scenario(
        self, conf_dir, hadoop_env_path
    ) -> None:
        """Run cross-source against real fixtures — should find no drift
        because the env and XML values currently agree by design."""
        from checker.collectors.xml_collector import collect_xml
        from checker.collectors.env_collector import parse_env_file

        snapshots = [
            collect_xml(conf_dir / "core-site.xml", service="namenode"),
            collect_xml(conf_dir / "hdfs-site.xml", service="namenode"),
            collect_xml(conf_dir / "yarn-site.xml", service="namenode"),
            collect_xml(conf_dir / "mapred-site.xml", service="namenode"),
            parse_env_file(hadoop_env_path, service="namenode"),
        ]
        results = detect_cross_source(snapshots)
        assert results == [], (
            f"unexpected cross-source drift in real fixtures: "
            f"{[r.to_dict() for r in results]}"
        )


# ---------------------------------------------------------------------------
# detect_temporal()
# ---------------------------------------------------------------------------


class TestTemporalDetect:
    def test_no_change_no_drift(self) -> None:
        a = _snap(properties={"k": "v"})
        b = _snap(properties={"k": "v"})
        assert detect_temporal(a, b) == []

    def test_value_change_detected(self) -> None:
        a = _snap(properties={"k": "old"})
        b = _snap(properties={"k": "new"})
        results = detect_temporal(a, b)
        assert len(results) == 1
        assert results[0].rule_id == "temporal-drift"
        assert results[0].value_a == "old"
        assert results[0].value_b == "new"

    def test_key_added(self) -> None:
        a = _snap(properties={})
        b = _snap(properties={"k": "v"})
        results = detect_temporal(a, b)
        assert len(results) == 1
        assert results[0].value_a is None

    def test_key_removed(self) -> None:
        a = _snap(properties={"k": "v"})
        b = _snap(properties={})
        results = detect_temporal(a, b)
        assert len(results) == 1
        assert results[0].value_b is None

    def test_different_agent_id_raises(self) -> None:
        a = _snap(agent_id="agent-1")
        b = _snap(agent_id="agent-2")
        with pytest.raises(ValueError, match="same agent_id"):
            detect_temporal(a, b)
