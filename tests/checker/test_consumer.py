"""Tests for ``checker.consumer`` — SnapshotStore and drift pipeline.

No Kafka required.  These tests exercise the pure-logic parts of the
consumer: the in-memory store and the ``process_snapshot`` function that
ties collection to drift detection.
"""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

import pytest

from checker.consumer import SnapshotStore, process_snapshot, run_oneshot
from checker.models import ConfigSnapshot, SOURCE_ENV_FILE, SOURCE_XML_FILE


def _snap(
    agent_id: str = "nn-core-site",
    service: str = "namenode",
    source: str = SOURCE_XML_FILE,
    source_path: str = "conf/core-site.xml",
    properties: dict | None = None,
    timestamp: str = "2026-04-23T12:00:00Z",
) -> ConfigSnapshot:
    return ConfigSnapshot(
        agent_id=agent_id,
        service=service,
        source=source,
        source_path=source_path,
        host="test-host",
        timestamp=timestamp,
        properties=properties or {},
    )


# ---------------------------------------------------------------------------
# SnapshotStore
# ---------------------------------------------------------------------------


class TestSnapshotStore:
    def test_put_and_get(self) -> None:
        store = SnapshotStore()
        snap = _snap()
        store.put(snap)
        assert store.get("nn-core-site") == snap

    def test_get_missing_returns_none(self) -> None:
        store = SnapshotStore()
        assert store.get("nonexistent") is None

    def test_put_returns_previous(self) -> None:
        store = SnapshotStore()
        snap1 = _snap(properties={"k": "v1"})
        snap2 = _snap(properties={"k": "v2"})
        assert store.put(snap1) is None
        previous = store.put(snap2)
        assert previous is not None
        assert previous.properties["k"] == "v1"

    def test_len(self) -> None:
        store = SnapshotStore()
        assert len(store) == 0
        store.put(_snap(agent_id="a"))
        store.put(_snap(agent_id="b"))
        assert len(store) == 2

    def test_snapshots_for_service(self) -> None:
        store = SnapshotStore()
        store.put(_snap(agent_id="nn-core", service="namenode"))
        store.put(_snap(agent_id="nn-hdfs", service="namenode"))
        store.put(_snap(agent_id="rm-core", service="resourcemanager"))
        assert len(store.snapshots_for_service("namenode")) == 2
        assert len(store.snapshots_for_service("resourcemanager")) == 1
        assert len(store.snapshots_for_service("unknown")) == 0

    def test_services(self) -> None:
        store = SnapshotStore()
        store.put(_snap(agent_id="a", service="namenode"))
        store.put(_snap(agent_id="b", service="datanode"))
        assert store.services() == {"namenode", "datanode"}

    def test_all_snapshots(self) -> None:
        store = SnapshotStore()
        store.put(_snap(agent_id="a"))
        store.put(_snap(agent_id="b"))
        assert len(store.all_snapshots()) == 2

    def test_clear(self) -> None:
        store = SnapshotStore()
        store.put(_snap())
        store.clear()
        assert len(store) == 0

    def test_to_dict_round_trip(self) -> None:
        store = SnapshotStore()
        snap = _snap(properties={"fs.defaultFS": "hdfs://nn:8020"})
        store.put(snap)
        data = store.to_dict()
        restored = SnapshotStore.from_dict(data)
        assert len(restored) == 1
        assert restored.get("nn-core-site").properties["fs.defaultFS"] == "hdfs://nn:8020"


# ---------------------------------------------------------------------------
# process_snapshot — the pipeline
# ---------------------------------------------------------------------------


class TestProcessSnapshot:
    def test_first_snapshot_no_drift(self) -> None:
        """The very first snapshot has nothing to compare against."""
        store = SnapshotStore()
        snap = _snap(properties={"k": "v"})
        results = process_snapshot(snap, store)
        assert results == []

    def test_temporal_drift_on_value_change(self) -> None:
        """Same agent, different value → temporal drift."""
        store = SnapshotStore()
        snap1 = _snap(properties={"k": "old"}, timestamp="T1")
        snap2 = _snap(properties={"k": "new"}, timestamp="T2")

        process_snapshot(snap1, store)
        results = process_snapshot(snap2, store)

        temporal = [r for r in results if r.rule_id == "temporal-drift"]
        assert len(temporal) == 1
        assert temporal[0].value_a == "old"
        assert temporal[0].value_b == "new"

    def test_cross_source_drift_on_mismatch(self) -> None:
        """Two sources for the same service with differing values."""
        store = SnapshotStore()
        xml_snap = _snap(
            agent_id="nn-core-site",
            source=SOURCE_XML_FILE,
            source_path="core-site.xml",
            properties={"fs.defaultFS": "hdfs://nn:8020"},
        )
        env_snap = _snap(
            agent_id="nn-hadoop-env",
            source=SOURCE_ENV_FILE,
            source_path="hadoop.env",
            properties={"fs.defaultFS": "hdfs://wrong:8020"},
        )

        process_snapshot(xml_snap, store)
        results = process_snapshot(env_snap, store)

        cross = [r for r in results if r.rule_id == "dual-source-consistency"]
        assert len(cross) == 1
        assert cross[0].key == "fs.defaultFS"

    def test_no_cross_source_when_values_agree(self) -> None:
        store = SnapshotStore()
        xml_snap = _snap(
            agent_id="nn-core-site",
            source=SOURCE_XML_FILE,
            source_path="core-site.xml",
            properties={"fs.defaultFS": "hdfs://nn:8020"},
        )
        env_snap = _snap(
            agent_id="nn-hadoop-env",
            source=SOURCE_ENV_FILE,
            source_path="hadoop.env",
            properties={"fs.defaultFS": "hdfs://nn:8020"},
        )

        process_snapshot(xml_snap, store)
        results = process_snapshot(env_snap, store)
        assert results == []

    def test_store_updated_after_processing(self) -> None:
        store = SnapshotStore()
        snap = _snap(properties={"k": "v"})
        process_snapshot(snap, store)
        assert store.get("nn-core-site") == snap

    def test_pipeline_with_real_fixtures(
        self, conf_dir, hadoop_env_path
    ) -> None:
        """Process real XML + env snapshots through the pipeline.

        Values agree in the fixtures, so the only drift should be from
        cross-source checks finding no issues (empty results).
        """
        from checker.collectors.xml_collector import collect_xml
        from checker.collectors.env_collector import parse_env_file

        store = SnapshotStore()
        all_results = []

        for xml_file in sorted(conf_dir.glob("*-site.xml")):
            snap = collect_xml(xml_file, service="namenode")
            results = process_snapshot(snap, store)
            all_results.extend(results)

        env_snap = parse_env_file(hadoop_env_path, service="namenode")
        results = process_snapshot(env_snap, store)
        all_results.extend(results)

        # All values agree in fixtures, so no cross-source drift.
        cross_source = [r for r in all_results if r.rule_id == "dual-source-consistency"]
        assert cross_source == [], (
            f"unexpected drift in real fixtures: {[r.to_dict() for r in cross_source]}"
        )


# ---------------------------------------------------------------------------
# run_oneshot — file-based drift detection
# ---------------------------------------------------------------------------


class TestRunOneshot:
    def test_oneshot_dict_format(self, tmp_path: Path) -> None:
        """Snapshot file as {agent_id: snapshot_dict}."""
        data = {
            "nn-core-site": _snap(
                agent_id="nn-core-site",
                source=SOURCE_XML_FILE,
                source_path="core-site.xml",
                properties={"fs.defaultFS": "hdfs://nn:8020"},
            ).to_dict(),
            "nn-env": _snap(
                agent_id="nn-env",
                source=SOURCE_ENV_FILE,
                source_path="hadoop.env",
                properties={"fs.defaultFS": "hdfs://wrong:8020"},
            ).to_dict(),
        }
        f = tmp_path / "snapshots.json"
        f.write_text(json.dumps(data))
        results = run_oneshot(str(f))
        assert any(r.key == "fs.defaultFS" for r in results)

    def test_oneshot_list_format(self, tmp_path: Path) -> None:
        """Snapshot file as [snapshot_dict, ...]."""
        data = [
            _snap(
                agent_id="nn-core-site",
                source=SOURCE_XML_FILE,
                source_path="core-site.xml",
                properties={"k": "v1"},
            ).to_dict(),
            _snap(
                agent_id="nn-env",
                source=SOURCE_ENV_FILE,
                source_path="hadoop.env",
                properties={"k": "v2"},
            ).to_dict(),
        ]
        f = tmp_path / "snapshots.json"
        f.write_text(json.dumps(data))
        results = run_oneshot(str(f))
        assert any(r.key == "k" for r in results)

    def test_oneshot_no_drift(self, tmp_path: Path) -> None:
        """All values agree → no drift."""
        data = {
            "a": _snap(
                agent_id="a",
                source=SOURCE_XML_FILE,
                source_path="core-site.xml",
                properties={"k": "v"},
            ).to_dict(),
            "b": _snap(
                agent_id="b",
                source=SOURCE_ENV_FILE,
                source_path="hadoop.env",
                properties={"k": "v"},
            ).to_dict(),
        }
        f = tmp_path / "snapshots.json"
        f.write_text(json.dumps(data))
        results = run_oneshot(str(f))
        assert results == []

    def test_oneshot_missing_file_raises(self) -> None:
        with pytest.raises(FileNotFoundError):
            run_oneshot("/tmp/nonexistent-snapshot-file.json")
