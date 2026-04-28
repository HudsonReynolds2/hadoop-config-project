"""End-to-end integration tests for hadoop-config-checker.

These tests simulate the real workflow: modify a config file on disk,
re-collect, and verify the entire pipeline (collector → drift detector →
validator → causality graph) catches and reports the change correctly.

No Kafka or Docker required — everything runs against real fixture files
with modifications written to tmp_path.
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest

from checker.agent import collect_all
from checker.analysis.causality_graph import CausalityGraph
from checker.analysis.drift_detector import detect, detect_cross_source
from checker.analysis.validator import load_rules, validate, validate_from_file
from checker.collectors.env_collector import parse_env_file
from checker.collectors.xml_collector import collect_xml
from checker.consumer import SnapshotStore, process_snapshot, run_oneshot


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _copy_fixtures_to(dst: Path, conf_dir: Path, hadoop_env_path: Path) -> dict:
    """Copy all fixture files to a writable directory.  Returns paths."""
    conf_dst = dst / "conf"
    conf_dst.mkdir()
    for xml in conf_dir.glob("*-site.xml"):
        shutil.copy(xml, conf_dst / xml.name)
    env_dst = dst / "hadoop.env"
    shutil.copy(hadoop_env_path, env_dst)
    return {"conf": conf_dst, "env": env_dst}


def _modify_xml_value(xml_path: Path, key: str, new_value: str) -> None:
    """Replace a value in a Hadoop XML file.  Brute-force string replace."""
    text = xml_path.read_text()
    # Find <value>OLD</value> for the given key
    import re
    # Handle both <name> and <n> tags
    pattern = rf"(<(?:name|n)>{re.escape(key)}</(?:name|n)>\s*<value>)(.*?)(</value>)"
    new_text, count = re.subn(pattern, rf"\g<1>{new_value}\g<3>", text)
    assert count == 1, f"expected 1 match for {key} in {xml_path}, got {count}"
    xml_path.write_text(new_text)


def _modify_env_value(env_path: Path, prefix_key: str, new_value: str) -> None:
    """Replace a value in a hadoop.env file."""
    lines = env_path.read_text().splitlines()
    new_lines = []
    found = False
    for line in lines:
        if line.startswith(prefix_key + "="):
            new_lines.append(f"{prefix_key}={new_value}")
            found = True
        else:
            new_lines.append(line)
    assert found, f"{prefix_key} not found in {env_path}"
    env_path.write_text("\n".join(new_lines) + "\n")


# ---------------------------------------------------------------------------
# Scenario 1: Modify yarn-site.xml — scheduler ceiling violated
# ---------------------------------------------------------------------------


class TestScenarioSchedulerCeilingViolation:
    """Swap yarn.scheduler.maximum-allocation-mb and NM memory so the
    constraint rule fails. Verify the entire pipeline catches it."""

    def test_collect_detect_validate_trace(
        self, tmp_path: Path, conf_dir: Path, hadoop_env_path: Path
    ) -> None:
        paths = _copy_fixtures_to(tmp_path, conf_dir, hadoop_env_path)

        # Collect baseline — everything should be clean.
        baseline = collect_all(str(paths["conf"]), "namenode", str(paths["env"]))
        store_base = SnapshotStore()
        for s in baseline:
            store_base.put(s)

        rules_path = Path(__file__).parent / "fixtures" / "hadoop-3.3.x.yaml"
        rules = load_rules(rules_path)
        base_results = validate(rules, store_base)
        base_failures = [r for r in base_results if not r.passed]
        assert base_failures == [], "baseline should have no failures"

        # Now swap the values: scheduler=8192, NM=2048
        _modify_xml_value(
            paths["conf"] / "yarn-site.xml",
            "yarn.scheduler.maximum-allocation-mb", "8192"
        )
        _modify_xml_value(
            paths["conf"] / "yarn-site.xml",
            "yarn.nodemanager.resource.memory-mb", "2048"
        )

        # Re-collect
        modified = collect_all(str(paths["conf"]), "namenode", str(paths["env"]))
        store_mod = SnapshotStore()
        for s in modified:
            store_mod.put(s)

        # Validate — scheduler ceiling must fail
        mod_results = validate(rules, store_mod)
        ceiling = [r for r in mod_results if r.rule_id == "yarn-scheduler-ceiling"]
        assert len(ceiling) == 1
        assert ceiling[0].passed is False
        assert ceiling[0].severity == "critical"

        # Temporal drift — re-processing should catch the value change
        store_temporal = SnapshotStore()
        all_drifts = []
        for s in baseline:
            process_snapshot(s, store_temporal)
        for s in modified:
            drifts, _ = process_snapshot(s, store_temporal, rules=rules)
            all_drifts.extend(drifts)

        # Should detect temporal drift on the changed keys
        temporal = [d for d in all_drifts if d.rule_id == "temporal-drift"]
        changed_keys = {d.key for d in temporal}
        assert "yarn.scheduler.maximum-allocation-mb" in changed_keys
        assert "yarn.nodemanager.resource.memory-mb" in changed_keys

        # Causality graph: NM memory drift → downstream constraint edge
        graph = CausalityGraph()
        nm_drift = [d for d in temporal
                    if d.key == "yarn.nodemanager.resource.memory-mb"]
        if nm_drift:
            rcs = graph.trace(nm_drift)
            assert len(rcs) >= 1
            # Should find the scheduler max as a downstream constraint
            all_effects = " ".join(rcs[0].downstream_effects)
            assert "yarn.scheduler.maximum-allocation-mb" in all_effects


# ---------------------------------------------------------------------------
# Scenario 2: fs.defaultFS mismatch between XML and env
# ---------------------------------------------------------------------------


class TestScenarioFsDefaultfsDualSourceDrift:
    """Change fs.defaultFS in hadoop.env but NOT in core-site.xml.
    The dual-source-consistency check must catch it."""

    def test_env_xml_disagree_detected(
        self, tmp_path: Path, conf_dir: Path, hadoop_env_path: Path
    ) -> None:
        paths = _copy_fixtures_to(tmp_path, conf_dir, hadoop_env_path)

        # Modify env only
        _modify_env_value(
            paths["env"],
            "CORE-SITE.XML_fs.defaultFS",
            "hdfs://wrong-namenode:8020"
        )

        snapshots = collect_all(str(paths["conf"]), "namenode", str(paths["env"]))

        # Cross-source drift should find fs.defaultFS disagreement
        drifts = detect_cross_source(snapshots)
        fs_drifts = [d for d in drifts if d.key == "fs.defaultFS"]
        assert len(fs_drifts) >= 1
        assert fs_drifts[0].rule_id == "dual-source-consistency"

        # The validator dual-source rule should also fail
        rules_path = Path(__file__).parent / "fixtures" / "hadoop-3.3.x.yaml"
        store = SnapshotStore()
        for s in snapshots:
            store.put(s)
        results = validate(load_rules(rules_path), store)
        dual = [r for r in results if r.rule_id == "dual-source-consistency"]
        assert len(dual) == 1
        assert dual[0].passed is False

    def test_causality_traces_hive_and_spark(
        self, tmp_path: Path, conf_dir: Path, hadoop_env_path: Path
    ) -> None:
        """Acceptance test from plan.md: fs.defaultFS mismatch lists
        Hive and Spark as downstream effects."""
        paths = _copy_fixtures_to(tmp_path, conf_dir, hadoop_env_path)

        _modify_env_value(
            paths["env"],
            "CORE-SITE.XML_fs.defaultFS",
            "hdfs://wrong:8020"
        )

        snapshots = collect_all(str(paths["conf"]), "namenode", str(paths["env"]))
        drifts = detect_cross_source(snapshots)
        fs_drifts = [d for d in drifts if d.key == "fs.defaultFS"]
        assert fs_drifts

        graph = CausalityGraph()
        root_causes = graph.trace(fs_drifts)
        assert len(root_causes) >= 1

        all_effects = " ".join(
            eff for rc in root_causes for eff in rc.downstream_effects
        )
        assert "hive-server2" in all_effects
        assert "spark-client" in all_effects


# ---------------------------------------------------------------------------
# Scenario 3: Hive warehouse dir doesn't contain fs.defaultFS
# ---------------------------------------------------------------------------


class TestScenarioHiveWarehouseMismatch:
    """Change hive.metastore.warehouse.dir to point at a different namenode.
    The must-contain rule must fail."""

    def test_warehouse_dir_wrong_namenode(
        self, tmp_path: Path, conf_dir: Path, hadoop_env_path: Path
    ) -> None:
        paths = _copy_fixtures_to(tmp_path, conf_dir, hadoop_env_path)

        _modify_xml_value(
            paths["conf"] / "hive-site.xml",
            "hive.metastore.warehouse.dir",
            "hdfs://other-namenode:8020/user/hive/warehouse"
        )

        snapshots = collect_all(str(paths["conf"]), "namenode", str(paths["env"]))
        store = SnapshotStore()
        for s in snapshots:
            store.put(s)

        rules_path = Path(__file__).parent / "fixtures" / "hadoop-3.3.x.yaml"
        results = validate(load_rules(rules_path), store)

        warehouse = [r for r in results if r.rule_id == "hive-warehouse-namenode"]
        assert len(warehouse) == 1
        assert warehouse[0].passed is False
        # Stage 2.1: rule now uses URL-authority comparison; details
        # phrasing changed from "does NOT contain" to "does NOT match".
        assert "does NOT match" in warehouse[0].details


# ---------------------------------------------------------------------------
# Scenario 4: Full pipeline through process_snapshot with graph
# ---------------------------------------------------------------------------


class TestScenarioFullPipeline:
    """Feed snapshots through process_snapshot with rules + graph.
    Verify drifts and root causes come out together."""

    def test_temporal_plus_rules_plus_graph(
        self, tmp_path: Path, conf_dir: Path, hadoop_env_path: Path
    ) -> None:
        paths = _copy_fixtures_to(tmp_path, conf_dir, hadoop_env_path)
        rules_path = Path(__file__).parent / "fixtures" / "hadoop-3.3.x.yaml"
        rules = load_rules(rules_path)
        graph = CausalityGraph()

        # Load baseline
        store = SnapshotStore()
        baseline = collect_all(str(paths["conf"]), "namenode", str(paths["env"]))
        for s in baseline:
            process_snapshot(s, store)

        # Modify fs.defaultFS in core-site.xml
        _modify_xml_value(
            paths["conf"] / "core-site.xml",
            "fs.defaultFS", "hdfs://moved-namenode:8020"
        )

        # Re-collect and process
        modified = collect_all(str(paths["conf"]), "namenode", str(paths["env"]))
        all_drifts = []
        all_rcs = []
        for s in modified:
            drifts, rcs = process_snapshot(s, store, rules=rules, graph=graph)
            all_drifts.extend(drifts)
            all_rcs.extend(rcs)

        # Should have temporal drift on fs.defaultFS
        temporal_fs = [d for d in all_drifts
                       if d.key == "fs.defaultFS" and d.rule_id == "temporal-drift"]
        assert temporal_fs

        # Should have cross-source drift (env still has old value)
        cross = [d for d in all_drifts
                 if d.rule_id == "dual-source-consistency"]
        assert any(d.key == "fs.defaultFS" for d in cross)

        # Root causes should include downstream effects
        if all_rcs:
            all_effects = " ".join(
                eff for rc in all_rcs for eff in rc.downstream_effects
            )
            # fs.defaultFS drift should trace to hive/spark
            if any(rc.key == "fs.defaultFS" for rc in all_rcs):
                assert "hive" in all_effects.lower() or "spark" in all_effects.lower()


# ---------------------------------------------------------------------------
# Scenario 5: oneshot mode — JSON file as CI gate
# ---------------------------------------------------------------------------


class TestScenarioOneshotCIGate:
    """Verify run_oneshot works as a CI/CD gate: clean config returns
    no drift, broken config returns drift."""

    def test_clean_config_no_drift(
        self, tmp_path: Path, conf_dir: Path, hadoop_env_path: Path
    ) -> None:
        snapshots = collect_all(str(conf_dir), "namenode", str(hadoop_env_path))
        data = {s.agent_id: s.to_dict() for s in snapshots}
        f = tmp_path / "snapshots.json"
        f.write_text(json.dumps(data))

        results, rcs = run_oneshot(str(f))
        assert results == []

    def test_broken_config_returns_drift(
        self, tmp_path: Path, conf_dir: Path, hadoop_env_path: Path
    ) -> None:
        paths = _copy_fixtures_to(tmp_path, conf_dir, hadoop_env_path)

        # Break fs.defaultFS in env
        _modify_env_value(
            paths["env"],
            "CORE-SITE.XML_fs.defaultFS",
            "hdfs://broken:9999"
        )

        snapshots = collect_all(str(paths["conf"]), "namenode", str(paths["env"]))
        data = {s.agent_id: s.to_dict() for s in snapshots}
        f = tmp_path / "snapshots.json"
        f.write_text(json.dumps(data))

        results, rcs = run_oneshot(str(f))
        assert len(results) > 0
        assert any(d.key == "fs.defaultFS" for d in results)

    def test_oneshot_with_rules_and_graph(
        self, tmp_path: Path, conf_dir: Path, hadoop_env_path: Path
    ) -> None:
        paths = _copy_fixtures_to(tmp_path, conf_dir, hadoop_env_path)

        # Swap scheduler values
        _modify_xml_value(
            paths["conf"] / "yarn-site.xml",
            "yarn.scheduler.maximum-allocation-mb", "9999"
        )

        snapshots = collect_all(str(paths["conf"]), "namenode", str(paths["env"]))
        data = {s.agent_id: s.to_dict() for s in snapshots}
        f = tmp_path / "snapshots.json"
        f.write_text(json.dumps(data))

        rules = load_rules(Path(__file__).parent / "fixtures" / "hadoop-3.3.x.yaml")
        graph = CausalityGraph()
        results, rcs = run_oneshot(str(f), rules=rules, graph=graph)

        # Should find the scheduler ceiling violation
        ceiling = [d for d in results if d.rule_id == "yarn-scheduler-ceiling"]
        assert ceiling


# ---------------------------------------------------------------------------
# Scenario 6: Multiple simultaneous changes
# ---------------------------------------------------------------------------


class TestScenarioMultipleChanges:
    """Change multiple values at once and verify all are detected."""

    def test_three_changes_all_detected(
        self, tmp_path: Path, conf_dir: Path, hadoop_env_path: Path
    ) -> None:
        paths = _copy_fixtures_to(tmp_path, conf_dir, hadoop_env_path)

        # Change 1: dfs.replication
        _modify_xml_value(
            paths["conf"] / "hdfs-site.xml", "dfs.replication", "5"
        )
        # Change 2: scheduler max
        _modify_xml_value(
            paths["conf"] / "yarn-site.xml",
            "yarn.scheduler.maximum-allocation-mb", "8192"
        )
        # Change 3: env fs.defaultFS
        _modify_env_value(
            paths["env"],
            "CORE-SITE.XML_fs.defaultFS",
            "hdfs://other:8020"
        )

        # Load baseline then modified
        store = SnapshotStore()
        baseline = collect_all(str(conf_dir), "namenode", str(hadoop_env_path))
        for s in baseline:
            process_snapshot(s, store)

        modified = collect_all(str(paths["conf"]), "namenode", str(paths["env"]))
        all_drifts = []
        for s in modified:
            drifts, _ = process_snapshot(s, store)
            all_drifts.extend(drifts)

        temporal = [d for d in all_drifts if d.rule_id == "temporal-drift"]
        changed_keys = {d.key for d in temporal}
        assert "dfs.replication" in changed_keys
        assert "yarn.scheduler.maximum-allocation-mb" in changed_keys
        # fs.defaultFS in env changes the normalised key
        assert "fs.defaultFS" in changed_keys


# ---------------------------------------------------------------------------
# Edge case: empty config file
# ---------------------------------------------------------------------------


class TestEdgeCases:
    def test_empty_xml_no_crash(self, tmp_path: Path) -> None:
        """An XML with no properties should produce an empty snapshot."""
        p = tmp_path / "empty-site.xml"
        p.write_text("<configuration></configuration>")
        snap = collect_xml(p, service="test")
        assert snap.properties == {}

    def test_snapshot_store_survives_millions_of_keys(self) -> None:
        """The store should handle large property dicts."""
        from checker.models import ConfigSnapshot, SOURCE_XML_FILE
        big_props = {f"key.{i}": f"value.{i}" for i in range(10_000)}
        snap = ConfigSnapshot(
            agent_id="big", service="test", source=SOURCE_XML_FILE,
            source_path="test.xml", host="h",
            properties=big_props,
        )
        store = SnapshotStore()
        store.put(snap)
        assert len(store.get("big").properties) == 10_000

    def test_detect_on_large_snapshots(self) -> None:
        """Diff two snapshots with many keys — only changed ones reported."""
        from checker.models import ConfigSnapshot, SOURCE_XML_FILE
        props_a = {f"key.{i}": f"val.{i}" for i in range(1000)}
        props_b = dict(props_a)
        props_b["key.500"] = "CHANGED"
        props_b["key.999"] = "ALSO_CHANGED"

        a = ConfigSnapshot(agent_id="a", service="s", source=SOURCE_XML_FILE,
                           source_path="x", host="h", properties=props_a)
        b = ConfigSnapshot(agent_id="a", service="s", source=SOURCE_XML_FILE,
                           source_path="x", host="h", properties=props_b)
        drifts = detect(a, b)
        assert len(drifts) == 2
        assert {d.key for d in drifts} == {"key.500", "key.999"}

    def test_validate_cli_exit_code(
        self, tmp_path: Path, conf_dir: Path, hadoop_env_path: Path
    ) -> None:
        """validate returns 0 when all rules pass (tested programmatically)."""
        rules_path = Path(__file__).parent / "fixtures" / "hadoop-3.3.x.yaml"
        snapshots = collect_all(str(conf_dir), "namenode", str(hadoop_env_path))
        store = SnapshotStore()
        for s in snapshots:
            store.put(s)
        results = validate_from_file(rules_path, store)
        assert all(r.passed for r in results)
