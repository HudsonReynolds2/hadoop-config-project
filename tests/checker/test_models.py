"""Tests for ``checker.models``.

Minimal coverage: the dataclasses are almost trivial, but the JSON
round-trip matters because the Kafka agent (Phase 2) and consumer (Phase 3)
will serialize these over the wire.
"""

from __future__ import annotations

import json

from checker.models import (
    ConfigSnapshot,
    DriftResult,
    EdgeType,
    RootCause,
    SOURCE_ENV_FILE,
    SOURCE_JVM_FLAGS,
    SOURCE_XML_FILE,
)


class TestEdgeType:
    def test_expected_members(self) -> None:
        assert EdgeType.CONSTRAINT.value == "constraint"
        assert EdgeType.INFLUENCE.value == "influence"
        assert EdgeType.PROPAGATION.value == "propagation"


class TestSourceConstants:
    def test_values(self) -> None:
        assert SOURCE_XML_FILE == "xml_file"
        assert SOURCE_ENV_FILE == "env_file"
        assert SOURCE_JVM_FLAGS == "jvm_flags"


class TestConfigSnapshot:
    def _make(self) -> ConfigSnapshot:
        return ConfigSnapshot(
            agent_id="namenode-core-site",
            service="namenode",
            source=SOURCE_XML_FILE,
            source_path="conf/core-site.xml",
            host="nn-01",
            timestamp="2026-04-23T12:00:00Z",
            properties={"fs.defaultFS": "hdfs://namenode:8020"},
        )

    def test_to_dict_round_trip(self) -> None:
        snap = self._make()
        d = snap.to_dict()
        snap2 = ConfigSnapshot.from_dict(d)
        assert snap2 == snap

    def test_to_json_is_valid_json(self) -> None:
        snap = self._make()
        parsed = json.loads(snap.to_json())
        assert parsed["agent_id"] == "namenode-core-site"
        assert parsed["properties"]["fs.defaultFS"] == "hdfs://namenode:8020"

    def test_default_timestamp_is_populated(self) -> None:
        snap = ConfigSnapshot(
            agent_id="a",
            service="s",
            source=SOURCE_XML_FILE,
            source_path="p",
            host="h",
        )
        # Non-empty ISO-ish string.
        assert snap.timestamp
        assert "T" in snap.timestamp
        assert snap.properties == {}


class TestDriftResult:
    def test_json_serializable_with_none_values(self) -> None:
        drift = DriftResult(
            key="fs.defaultFS",
            service="hive-server2",
            source_a="xml_file:conf/core-site.xml",
            value_a="hdfs://namenode:8020",
            source_b="jvm_flags:SERVICE_OPTS",
            value_b=None,
            severity="critical",
        )
        parsed = json.loads(drift.to_json())
        assert parsed["value_b"] is None
        assert parsed["rule_id"] is None


class TestRootCause:
    def test_nested_drift_serializes(self) -> None:
        drift = DriftResult(
            key="fs.defaultFS",
            service="namenode",
            source_a="xml_file:conf/core-site.xml",
            value_a="hdfs://namenode:8020",
            source_b="env_file:hadoop.env",
            value_b="hdfs://other:8020",
            severity="critical",
            rule_id="dual-source-consistency",
        )
        rc = RootCause(
            key="fs.defaultFS",
            service="namenode",
            drift=drift,
            downstream_effects=["hive-server2", "spark-client"],
            severity="critical",
        )
        parsed = json.loads(rc.to_json())
        assert parsed["downstream_effects"] == ["hive-server2", "spark-client"]
        assert parsed["drift"]["rule_id"] == "dual-source-consistency"
