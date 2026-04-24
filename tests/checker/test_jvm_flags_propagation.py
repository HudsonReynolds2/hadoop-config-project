"""Integration test: JVM-flags snapshots participate in propagation rules.

Hive services don't bind-mount ``*-site.xml``. They receive config via
``-Dkey=value`` tokens in ``SERVICE_OPTS``. plan.md §"Why Hive is special"
calls out that the env collector has ``parse_jvm_flags`` specifically for
this case.

The unit tests for ``parse_jvm_flags`` prove the parser works. These tests
prove that the *resulting* snapshot integrates correctly with the rest of
the pipeline — in particular, that ``fs-defaultfs-propagation`` sees a
hive-server2 snapshot sourced from JVM flags and compares it against a
namenode XML snapshot the same way it would two XML snapshots.

This is the Hive scenario described in the
[override file](../../docker-compose.override.yml)'s commented
``agent-hive-server2`` block, tested in-process.
"""

from __future__ import annotations

from pathlib import Path

from checker.analysis.validator import load_rules, validate
from checker.collectors.env_collector import parse_jvm_flags
from checker.collectors.xml_collector import collect_xml
from checker.consumer import SnapshotStore


HIVE_FLAGS_MATCHING = (
    "-Dhive.metastore.uris=thrift://hive-metastore:9083 "
    "-Dhive.server2.thrift.port=10000 "
    "-Dhive.server2.thrift.bind.host=0.0.0.0 "
    "-Dhive.server2.authentication=NONE "
    "-Dhive.server2.enable.doAs=false "
    "-Dfs.defaultFS=hdfs://namenode:8020"
)

HIVE_FLAGS_DIVERGED = HIVE_FLAGS_MATCHING.replace(
    "hdfs://namenode:8020", "hdfs://wrong-namenode:8020"
)


def _rules():
    return load_rules(Path(__file__).parent / "fixtures" / "hadoop-3.3.x.yaml")


def _build_store(conf_dir: Path, hive_flags: str) -> SnapshotStore:
    """Reproduce the production shape: namenode/RM/NM read XMLs, Hive
    reads JVM flags. Every Hadoop-family service shares the same
    ``conf/`` bind mount; Hive doesn't see those files at all."""
    store = SnapshotStore()

    # XML-reading services
    for xml in sorted(conf_dir.glob("*-site.xml")):
        for service in ["namenode", "resourcemanager", "nodemanager"]:
            store.put(collect_xml(xml, service=service))

    # Hive — JVM flags only, no XMLs.
    store.put(parse_jvm_flags(
        hive_flags, service="hive-server2", source_path="SERVICE_OPTS",
    ))
    return store


def test_jvm_flags_agree_with_xmls_propagation_passes(conf_dir: Path) -> None:
    """fs.defaultFS is set identically in core-site.xml and in
    ``SERVICE_OPTS``. The propagation rule must pass across all four
    services."""
    store = _build_store(conf_dir, HIVE_FLAGS_MATCHING)
    results = validate(_rules(), store)
    rule = next(r for r in results if r.rule_id == "fs-defaultfs-propagation")
    assert rule.passed is True, rule.details
    # All four services should appear in the agreement message.
    for svc in ["namenode", "resourcemanager", "nodemanager", "hive-server2"]:
        assert svc in rule.details


def test_jvm_flags_diverge_from_xmls_propagation_fails(conf_dir: Path) -> None:
    """Same setup, but the Hive SERVICE_OPTS has a different fs.defaultFS.
    The propagation rule must fail and the drift must name both values."""
    store = _build_store(conf_dir, HIVE_FLAGS_DIVERGED)
    results = validate(_rules(), store)
    rule = next(r for r in results if r.rule_id == "fs-defaultfs-propagation")
    assert rule.passed is False, (
        "Hive JVM-flags divergence should fail fs-defaultfs-propagation; "
        f"details={rule.details!r}"
    )
    # The details line should list both values so an operator can see what
    # diverged without opening a dashboard.
    assert "hdfs://namenode:8020" in rule.details
    assert "hdfs://wrong-namenode:8020" in rule.details


def test_jvm_flags_snapshot_has_correct_source_tag(conf_dir: Path) -> None:
    """Guard the snapshot metadata the consumer uses to label alerts —
    if this regresses, drift reports will stop saying ``jvm_flags:`` and
    start saying ``xml_file:`` for Hive."""
    store = _build_store(conf_dir, HIVE_FLAGS_DIVERGED)
    hive_snaps = store.snapshots_for_service("hive-server2")
    assert len(hive_snaps) == 1
    snap = hive_snaps[0]
    assert snap.source == "jvm_flags"
    assert snap.source_path == "SERVICE_OPTS"
    assert snap.properties.get("fs.defaultFS") == "hdfs://wrong-namenode:8020"
