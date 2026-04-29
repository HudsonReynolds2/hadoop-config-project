"""Tests for the ``must_reference`` validator rule type.

Stage 3.

The plan: a connection-string key on service A must reference service B
by literal hostname and a port read from a key on service B. Used for
the kafka-zookeeper-connect rule.

These tests are direct unit tests of ``checker.analysis.validator``,
mirroring the style of ``test_validator.py``.
"""

from __future__ import annotations

from checker.analysis.validator import (
    STATUS_FAIL,
    STATUS_PASS,
    STATUS_SKIP,
    _split_authority,
    validate,
)
from checker.consumer import SnapshotStore
from checker.models import (
    SOURCE_JVM_FLAGS,
    SOURCE_XML_FILE,
    ConfigSnapshot,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _store_with(*snaps: ConfigSnapshot) -> SnapshotStore:
    s = SnapshotStore()
    for sn in snaps:
        s.put(sn)
    return s


def _snap(
    agent_id: str,
    service: str,
    properties: dict,
    source: str = SOURCE_JVM_FLAGS,
) -> ConfigSnapshot:
    return ConfigSnapshot(
        agent_id=agent_id,
        service=service,
        source=source,
        source_path=f"jvm:{agent_id}",
        host="testhost",
        properties=properties,
    )


_KAFKA_RULE = {
    "id": "kafka-zookeeper-connect",
    "description": "kafka.zookeeper.connect must reference ZK host:port",
    "type": "must_reference",
    "key": "zookeeper.connect",
    "service": "kafka",
    "target_host": "zookeeper",
    "target_port_key": "clientPort",
    "target_port_service": "zookeeper",
    "severity": "critical",
}


# ---------------------------------------------------------------------------
# _split_authority — direct unit tests
# ---------------------------------------------------------------------------


class TestSplitAuthority:
    def test_host_port(self) -> None:
        assert _split_authority("zookeeper:2181") == ("zookeeper", "2181")

    def test_host_only(self) -> None:
        assert _split_authority("zookeeper") == ("zookeeper", None)

    def test_strips_whitespace(self) -> None:
        assert _split_authority("  zookeeper:2181  ") == ("zookeeper", "2181")

    def test_empty(self) -> None:
        assert _split_authority("") == ("", None)

    def test_ipv6_with_port(self) -> None:
        assert _split_authority("[::1]:2181") == ("::1", "2181")

    def test_ipv6_no_port(self) -> None:
        assert _split_authority("[::1]") == ("::1", None)

    def test_host_with_path_in_string_keeps_only_host_port(self) -> None:
        # zookeeper.connect can include a chroot suffix: "zk:2181/kafka".
        # _split_authority is called per comma-entry; chroot would be
        # included in the entry. We accept that the port match here will
        # be "2181/kafka" which won't equal a plain "2181" — so the rule
        # would fail, which is fine: it draws attention to the chroot
        # form and a developer can add a separate rule if they want.
        # Test just confirms split behaviour: rpartition on ':' picks the
        # last colon.
        assert _split_authority("zk:2181/kafka") == ("zk", "2181/kafka")


# ---------------------------------------------------------------------------
# Pass / fail / skip behaviour
# ---------------------------------------------------------------------------


class TestMustReferenceBasic:
    def test_passes_when_host_and_port_match(self) -> None:
        store = _store_with(
            _snap("kafka-broker", "kafka", {"zookeeper.connect": "zookeeper:2181"}),
            _snap("zookeeper-cfg", "zookeeper", {"clientPort": "2181"}),
        )
        results = validate([_KAFKA_RULE], store)
        assert len(results) == 1
        r = results[0]
        assert r.passed is True
        assert r.status == STATUS_PASS

    def test_fails_when_host_wrong(self) -> None:
        store = _store_with(
            _snap("kafka-broker", "kafka", {"zookeeper.connect": "wrongzk:2181"}),
            _snap("zookeeper-cfg", "zookeeper", {"clientPort": "2181"}),
        )
        results = validate([_KAFKA_RULE], store)
        r = results[0]
        assert r.passed is False
        assert r.status == STATUS_FAIL
        assert r.severity == "critical"
        assert r.drift is not None
        assert "wrongzk" in r.details

    def test_fails_when_port_wrong(self) -> None:
        store = _store_with(
            _snap("kafka-broker", "kafka", {"zookeeper.connect": "zookeeper:9999"}),
            _snap("zookeeper-cfg", "zookeeper", {"clientPort": "2181"}),
        )
        results = validate([_KAFKA_RULE], store)
        r = results[0]
        assert r.passed is False
        assert r.status == STATUS_FAIL


class TestMustReferenceList:
    def test_passes_when_any_entry_in_list_matches(self) -> None:
        # zookeeper.connect of zk1:2181,zookeeper:2181,zk3:2181 — middle
        # entry matches the configured target.
        store = _store_with(
            _snap("kafka-broker", "kafka", {
                "zookeeper.connect": "zk1:2181,zookeeper:2181,zk3:2181",
            }),
            _snap("zookeeper-cfg", "zookeeper", {"clientPort": "2181"}),
        )
        results = validate([_KAFKA_RULE], store)
        assert results[0].passed is True

    def test_fails_when_no_entry_matches(self) -> None:
        store = _store_with(
            _snap("kafka-broker", "kafka", {
                "zookeeper.connect": "zk1:2181,zk2:2181,zk3:2181",
            }),
            _snap("zookeeper-cfg", "zookeeper", {"clientPort": "2181"}),
        )
        results = validate([_KAFKA_RULE], store)
        assert results[0].passed is False


class TestMustReferenceSkip:
    def test_skip_when_source_key_missing(self) -> None:
        store = _store_with(
            _snap("kafka-broker", "kafka", {}),  # no zookeeper.connect
            _snap("zookeeper-cfg", "zookeeper", {"clientPort": "2181"}),
        )
        results = validate([_KAFKA_RULE], store)
        r = results[0]
        assert r.passed is True
        assert r.status == STATUS_SKIP

    def test_skip_when_target_port_key_missing(self) -> None:
        store = _store_with(
            _snap("kafka-broker", "kafka", {"zookeeper.connect": "zookeeper:2181"}),
            _snap("zookeeper-cfg", "zookeeper", {}),  # no clientPort
        )
        results = validate([_KAFKA_RULE], store)
        r = results[0]
        assert r.passed is True
        assert r.status == STATUS_SKIP

    def test_skip_when_both_keys_missing(self) -> None:
        store = SnapshotStore()
        results = validate([_KAFKA_RULE], store)
        r = results[0]
        assert r.passed is True
        assert r.status == STATUS_SKIP

    def test_skip_when_rule_missing_target_host(self) -> None:
        bad = dict(_KAFKA_RULE)
        del bad["target_host"]
        store = _store_with(
            _snap("kafka-broker", "kafka", {"zookeeper.connect": "zookeeper:2181"}),
            _snap("zookeeper-cfg", "zookeeper", {"clientPort": "2181"}),
        )
        results = validate([bad], store)
        r = results[0]
        assert r.status == STATUS_SKIP


class TestMustReferenceAcceptance:
    def test_real_kafka_zk_scenario_passes(self) -> None:
        """End-to-end acceptance: kafka pointed at zookeeper:2181 and
        zookeeper publishes clientPort=2181 — the rule passes."""
        store = _store_with(
            _snap(
                "kafka-broker.config", "kafka",
                {"zookeeper.connect": "zookeeper:2181"},
                source=SOURCE_JVM_FLAGS,
            ),
            _snap(
                "zookeeper-zoo.cfg", "zookeeper",
                {"clientPort": "2181"},
                source=SOURCE_JVM_FLAGS,
            ),
        )
        results = validate([_KAFKA_RULE], store)
        assert results[0].passed is True
        assert results[0].status == STATUS_PASS
