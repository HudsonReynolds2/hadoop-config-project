"""Tests for ``checker.consumer`` — SnapshotStore and drift pipeline."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from checker.consumer import SnapshotStore, process_snapshot, run_oneshot
from checker.models import ConfigSnapshot, SOURCE_ENV_FILE, SOURCE_XML_FILE


def _snap(agent_id="nn-core-site", service="namenode", source=SOURCE_XML_FILE,
          source_path="conf/core-site.xml", properties=None, timestamp="2026-04-23T12:00:00Z"):
    return ConfigSnapshot(agent_id=agent_id, service=service, source=source,
                          source_path=source_path, host="test-host",
                          timestamp=timestamp, properties=properties or {})


class TestSnapshotStore:
    def test_put_and_get(self):
        store = SnapshotStore(); snap = _snap(); store.put(snap)
        assert store.get("nn-core-site") == snap

    def test_get_missing_returns_none(self):
        assert SnapshotStore().get("x") is None

    def test_put_returns_previous(self):
        store = SnapshotStore()
        assert store.put(_snap(properties={"k": "v1"})) is None
        prev = store.put(_snap(properties={"k": "v2"}))
        assert prev.properties["k"] == "v1"

    def test_len(self):
        store = SnapshotStore()
        store.put(_snap(agent_id="a")); store.put(_snap(agent_id="b"))
        assert len(store) == 2

    def test_snapshots_for_service(self):
        store = SnapshotStore()
        store.put(_snap(agent_id="a", service="nn"))
        store.put(_snap(agent_id="b", service="nn"))
        store.put(_snap(agent_id="c", service="rm"))
        assert len(store.snapshots_for_service("nn")) == 2
        assert len(store.snapshots_for_service("rm")) == 1

    def test_services(self):
        store = SnapshotStore()
        store.put(_snap(agent_id="a", service="nn"))
        store.put(_snap(agent_id="b", service="dn"))
        assert store.services() == {"nn", "dn"}

    def test_all_snapshots(self):
        store = SnapshotStore()
        store.put(_snap(agent_id="a")); store.put(_snap(agent_id="b"))
        assert len(store.all_snapshots()) == 2

    def test_clear(self):
        store = SnapshotStore(); store.put(_snap()); store.clear()
        assert len(store) == 0

    def test_to_dict_round_trip(self):
        store = SnapshotStore()
        store.put(_snap(properties={"fs.defaultFS": "hdfs://nn:8020"}))
        restored = SnapshotStore.from_dict(store.to_dict())
        assert restored.get("nn-core-site").properties["fs.defaultFS"] == "hdfs://nn:8020"


class TestProcessSnapshot:
    def test_first_snapshot_no_drift(self):
        results, rcs = process_snapshot(_snap(properties={"k": "v"}), SnapshotStore())
        assert results == [] and rcs == []

    def test_temporal_drift_on_value_change(self):
        store = SnapshotStore()
        process_snapshot(_snap(properties={"k": "old"}, timestamp="T1"), store)
        results, _ = process_snapshot(_snap(properties={"k": "new"}, timestamp="T2"), store)
        temporal = [r for r in results if r.rule_id == "temporal-drift"]
        assert len(temporal) == 1 and temporal[0].value_a == "old"

    def test_cross_source_drift_on_mismatch(self):
        store = SnapshotStore()
        process_snapshot(_snap(agent_id="a", source=SOURCE_XML_FILE, source_path="x.xml",
                               properties={"fs.defaultFS": "hdfs://nn:8020"}), store)
        results, _ = process_snapshot(
            _snap(agent_id="b", source=SOURCE_ENV_FILE, source_path="h.env",
                  properties={"fs.defaultFS": "hdfs://wrong:8020"}), store)
        cross = [r for r in results if r.rule_id == "dual-source-consistency"]
        assert len(cross) == 1 and cross[0].key == "fs.defaultFS"

    def test_no_cross_source_when_values_agree(self):
        store = SnapshotStore()
        process_snapshot(_snap(agent_id="a", source=SOURCE_XML_FILE, source_path="x.xml",
                               properties={"fs.defaultFS": "hdfs://nn:8020"}), store)
        results, _ = process_snapshot(
            _snap(agent_id="b", source=SOURCE_ENV_FILE, source_path="h.env",
                  properties={"fs.defaultFS": "hdfs://nn:8020"}), store)
        assert results == []

    def test_store_updated_after_processing(self):
        store = SnapshotStore(); snap = _snap(properties={"k": "v"})
        process_snapshot(snap, store)
        assert store.get("nn-core-site") == snap

    def test_pipeline_with_real_fixtures(self, conf_dir, hadoop_env_path):
        from checker.collectors.xml_collector import collect_xml
        from checker.collectors.env_collector import parse_env_file
        store = SnapshotStore(); all_results = []
        for xml_file in sorted(conf_dir.glob("*-site.xml")):
            r, _ = process_snapshot(collect_xml(xml_file, service="namenode"), store)
            all_results.extend(r)
        r, _ = process_snapshot(parse_env_file(hadoop_env_path, service="namenode"), store)
        all_results.extend(r)
        cross = [x for x in all_results if x.rule_id == "dual-source-consistency"]
        assert cross == []


class TestRunOneshot:
    def test_oneshot_dict_format(self, tmp_path):
        data = {
            "a": _snap(agent_id="a", source=SOURCE_XML_FILE, source_path="x",
                        properties={"fs.defaultFS": "hdfs://nn:8020"}).to_dict(),
            "b": _snap(agent_id="b", source=SOURCE_ENV_FILE, source_path="e",
                        properties={"fs.defaultFS": "hdfs://wrong:8020"}).to_dict(),
        }
        f = tmp_path / "s.json"; f.write_text(json.dumps(data))
        results, _ = run_oneshot(str(f))
        assert any(r.key == "fs.defaultFS" for r in results)

    def test_oneshot_list_format(self, tmp_path):
        data = [
            _snap(agent_id="a", source=SOURCE_XML_FILE, source_path="x",
                  properties={"k": "v1"}).to_dict(),
            _snap(agent_id="b", source=SOURCE_ENV_FILE, source_path="e",
                  properties={"k": "v2"}).to_dict(),
        ]
        f = tmp_path / "s.json"; f.write_text(json.dumps(data))
        results, _ = run_oneshot(str(f))
        assert any(r.key == "k" for r in results)

    def test_oneshot_no_drift(self, tmp_path):
        data = {
            "a": _snap(agent_id="a", source=SOURCE_XML_FILE, source_path="x",
                        properties={"k": "v"}).to_dict(),
            "b": _snap(agent_id="b", source=SOURCE_ENV_FILE, source_path="e",
                        properties={"k": "v"}).to_dict(),
        }
        f = tmp_path / "s.json"; f.write_text(json.dumps(data))
        results, _ = run_oneshot(str(f))
        assert results == []

    def test_oneshot_missing_file_raises(self):
        with pytest.raises(FileNotFoundError):
            run_oneshot("/tmp/nonexistent-snapshot-file.json")
