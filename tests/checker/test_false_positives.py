"""False-positive guards for the drift pipeline.

The inverse of the rule-matrix: these tests assert the tool stays *quiet*
when nothing has actually changed, or when changes are semantically
irrelevant. Silent false positives are worse than missed detections
because every spurious alert trains operators to ignore the tool.

Covered:

    * baseline conf/env → zero validator failures
    * heartbeat storm → zero temporal drift
    * comment-only / whitespace XML edit → zero drift
    * ``hive-warehouse-namenode``'s substring match on URLs with a
      prefix relationship — surfaces a known weakness in the current
      rule (marked xfail so we can see it flip green if the rule is
      tightened to URL-authority comparison; see testing-upgrades.md
      Tier D).
"""

from __future__ import annotations

import re
import shutil
from pathlib import Path

import pytest

from checker.agent import collect_all
from checker.analysis.drift_detector import detect, detect_cross_source
from checker.analysis.validator import load_rules, validate
from checker.consumer import SnapshotStore, process_snapshot
from checker.models import ConfigSnapshot, SOURCE_XML_FILE


# ---------------------------------------------------------------------------
# Helpers (scoped locally; see test_rule_matrix.py for the rationale).
# ---------------------------------------------------------------------------


def _copy_fixtures(dst: Path, conf_dir: Path, hadoop_env_path: Path) -> dict:
    conf_dst = dst / "conf"
    conf_dst.mkdir()
    for xml in conf_dir.glob("*-site.xml"):
        shutil.copy(xml, conf_dst / xml.name)
    env_dst = dst / "hadoop.env"
    shutil.copy(hadoop_env_path, env_dst)
    return {"conf": conf_dst, "env": env_dst}


def _collect_multi_service(conf_dir: Path, env_path: Path) -> SnapshotStore:
    store = SnapshotStore()
    for service in ["namenode", "resourcemanager", "nodemanager", "hive-server2"]:
        for snap in collect_all(str(conf_dir), service, str(env_path)):
            store.put(snap)
    return store


# ---------------------------------------------------------------------------
# Baseline — regression anchor.
# ---------------------------------------------------------------------------


def test_shipped_fixtures_produce_zero_validator_failures(
    conf_dir: Path, hadoop_env_path: Path
) -> None:
    rules_path = Path(__file__).parent / "fixtures" / "hadoop-3.3.x.yaml"
    store = _collect_multi_service(conf_dir, hadoop_env_path)
    results = validate(load_rules(rules_path), store)
    failures = [r for r in results if not r.passed]
    assert failures == [], (
        "shipped fixtures must be a clean baseline; got: "
        + ", ".join(r.rule_id for r in failures)
    )


def test_shipped_fixtures_produce_zero_cross_source_drift(
    conf_dir: Path, hadoop_env_path: Path
) -> None:
    """hadoop.env is deliberately redundant with conf/*.xml; that
    redundancy should produce zero cross-source disagreements."""
    snapshots = collect_all(str(conf_dir), "namenode", str(hadoop_env_path))
    drifts = detect_cross_source(snapshots)
    assert drifts == [], (
        "cross-source drift found on shipped fixtures: "
        + ", ".join(f"{d.key}={d.value_a!r}/{d.value_b!r}" for d in drifts)
    )


# ---------------------------------------------------------------------------
# Heartbeat stability — re-publishing identical snapshots must be silent.
# ---------------------------------------------------------------------------


def test_heartbeat_storm_produces_no_temporal_drift(
    conf_dir: Path, hadoop_env_path: Path
) -> None:
    """Simulate 10 heartbeat cycles. The agent re-publishes fresh
    ``ConfigSnapshot`` objects with new timestamps on every heartbeat;
    properties are unchanged, so ``detect_temporal`` must return empty
    each cycle."""
    store = SnapshotStore()
    first = collect_all(str(conf_dir), "namenode", str(hadoop_env_path))
    for s in first:
        process_snapshot(s, store)

    spurious: list = []
    for _ in range(10):
        # Re-collect: this creates new ConfigSnapshot instances with a
        # fresh UTC timestamp but identical properties.
        batch = collect_all(str(conf_dir), "namenode", str(hadoop_env_path))
        for s in batch:
            drifts, _ = process_snapshot(s, store)
            temporal = [d for d in drifts if d.rule_id == "temporal-drift"]
            spurious.extend(temporal)

    assert spurious == [], (
        f"{len(spurious)} spurious temporal drifts from identical heartbeats"
    )


# ---------------------------------------------------------------------------
# Parser stability — cosmetic edits must not look like drift.
# ---------------------------------------------------------------------------


def test_comment_only_xml_edit_is_invisible(
    tmp_path: Path, conf_dir: Path, hadoop_env_path: Path
) -> None:
    """Adding an XML comment must not register as a config change."""
    paths = _copy_fixtures(tmp_path, conf_dir, hadoop_env_path)

    baseline = collect_all(str(paths["conf"]), "namenode", str(paths["env"]))
    store = SnapshotStore()
    for s in baseline:
        process_snapshot(s, store)

    yarn = paths["conf"] / "yarn-site.xml"
    yarn.write_text(
        "<!-- harmless comment added by an operator -->\n" + yarn.read_text()
    )

    modified = collect_all(str(paths["conf"]), "namenode", str(paths["env"]))
    all_drifts: list = []
    for s in modified:
        drifts, _ = process_snapshot(s, store)
        all_drifts.extend(drifts)

    assert all_drifts == [], (
        f"comment-only edit produced drift: "
        + ", ".join(f"{d.key}({d.rule_id})" for d in all_drifts)
    )


def test_whitespace_only_xml_edit_is_invisible(
    tmp_path: Path, conf_dir: Path, hadoop_env_path: Path
) -> None:
    """Re-indenting an XML must not register as a config change."""
    paths = _copy_fixtures(tmp_path, conf_dir, hadoop_env_path)

    baseline = collect_all(str(paths["conf"]), "namenode", str(paths["env"]))
    store = SnapshotStore()
    for s in baseline:
        process_snapshot(s, store)

    yarn = paths["conf"] / "yarn-site.xml"
    # Replace the single-line <property> blocks with multi-line ones.
    reformatted = re.sub(
        r"<property><name>([^<]+)</name><value>([^<]*)</value></property>",
        r"<property>\n  <name>\1</name>\n  <value>\2</value>\n</property>",
        yarn.read_text(),
    )
    yarn.write_text(reformatted)

    modified = collect_all(str(paths["conf"]), "namenode", str(paths["env"]))
    all_drifts: list = []
    for s in modified:
        drifts, _ = process_snapshot(s, store)
        all_drifts.extend(drifts)

    assert all_drifts == [], (
        f"whitespace-only edit produced drift: "
        + ", ".join(f"{d.key}({d.rule_id})" for d in all_drifts)
    )


# ---------------------------------------------------------------------------
# Pairwise detector — identical snapshots produce zero drift, even with
# different agent_ids / timestamps.
# ---------------------------------------------------------------------------


def test_detect_on_identical_properties_is_silent() -> None:
    props = {"fs.defaultFS": "hdfs://namenode:8020", "dfs.replication": "1"}
    a = ConfigSnapshot(
        agent_id="a", service="namenode", source=SOURCE_XML_FILE,
        source_path="/a.xml", host="h-a", properties=dict(props),
    )
    b = ConfigSnapshot(
        agent_id="b", service="namenode", source=SOURCE_XML_FILE,
        source_path="/b.xml", host="h-b", properties=dict(props),
    )
    assert detect(a, b) == []


# ---------------------------------------------------------------------------
# Known-weakness probe: hive-warehouse-namenode uses naive substring match.
#
# URL authorities that share a prefix (e.g. port 8020 vs 80201) pass the
# current rule, which is arguably wrong. Marked xfail(strict=True) so the
# suite stays green on current behaviour but flips to failure if the rule
# is ever tightened — that way we see the change landing.
# ---------------------------------------------------------------------------


@pytest.mark.xfail(
    strict=True,
    reason=(
        "hive-warehouse-namenode uses `ref_val in val` substring match. "
        "A warehouse dir whose host:port is a prefix of a longer host:port "
        "passes the rule incorrectly. Tighten to URL-authority comparison "
        "to fix (see testing-upgrades.md Tier D)."
    ),
)
def test_hive_warehouse_substring_prefix_should_be_rejected(
    tmp_path: Path, conf_dir: Path, hadoop_env_path: Path
) -> None:
    paths = _copy_fixtures(tmp_path, conf_dir, hadoop_env_path)

    # Change the warehouse dir so the port is "80201" instead of "8020/".
    # The current substring match says "hdfs://namenode:8020" is in
    # "hdfs://namenode:80201/user/hive/warehouse", so the rule passes —
    # but the warehouse is on a different host:port than the namenode.
    yarn_xml = paths["conf"] / "hive-site.xml"
    text = yarn_xml.read_text()
    text = re.sub(
        r"(<(?:name|n)>hive\.metastore\.warehouse\.dir</(?:name|n)>\s*<value>)[^<]+(</value>)",
        r"\1hdfs://namenode:80201/user/hive/warehouse\2",
        text,
    )
    yarn_xml.write_text(text)

    store = _collect_multi_service(paths["conf"], paths["env"])
    rules_path = Path(__file__).parent / "fixtures" / "hadoop-3.3.x.yaml"
    results = validate(load_rules(rules_path), store)
    warehouse = next(r for r in results if r.rule_id == "hive-warehouse-namenode")
    assert warehouse.passed is False, (
        "a warehouse dir whose port prefix-matches but does not equal the "
        "namenode's port should not pass the rule"
    )
