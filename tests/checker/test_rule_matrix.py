"""Rule matrix — one targeted mutation per rule, assert only that rule fails.

[test_integration.py](test_integration.py) already proves the pipeline can
detect *some* drift for *some* scenarios. This suite is stricter: for every
rule shipped in [rules/hadoop-3.3.x.yaml](../../rules/hadoop-3.3.x.yaml) we
define the minimum mutation that should break it, run the full validator,
and assert

    * the targeted rule fails,
    * every other rule passes (or is in a documented co-failure set).

If a rule fires spuriously on an unrelated mutation, this suite catches it.

The mutation function returns the populated ``SnapshotStore`` directly so
that scenarios requiring per-service config divergence (e.g. bind-mount
drift for ``fs-defaultfs-propagation``) can build the store on their own
terms.
"""

from __future__ import annotations

import re
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

import pytest

from checker.agent import collect_all
from checker.analysis.validator import load_rules, validate
from checker.consumer import SnapshotStore


# ---------------------------------------------------------------------------
# File-mutation helpers (local copies — keeping the suites decoupled).
# ---------------------------------------------------------------------------


def _copy_fixtures(dst: Path, conf_dir: Path, hadoop_env_path: Path) -> dict:
    conf_dst = dst / "conf"
    conf_dst.mkdir()
    for xml in conf_dir.glob("*-site.xml"):
        shutil.copy(xml, conf_dst / xml.name)
    env_dst = dst / "hadoop.env"
    shutil.copy(hadoop_env_path, env_dst)
    return {"conf": conf_dst, "env": env_dst}


def _clone_conf(src: Path, dst: Path) -> Path:
    """Clone a conf directory so per-service mutations can diverge."""
    dst.mkdir()
    for xml in src.glob("*-site.xml"):
        shutil.copy(xml, dst / xml.name)
    return dst


def _modify_xml_value(xml_path: Path, key: str, new_value: str) -> None:
    text = xml_path.read_text()
    pattern = rf"(<(?:name|n)>{re.escape(key)}</(?:name|n)>\s*<value>)(.*?)(</value>)"
    new_text, count = re.subn(pattern, rf"\g<1>{new_value}\g<3>", text)
    assert count == 1, f"expected 1 match for {key} in {xml_path}, got {count}"
    xml_path.write_text(new_text)


def _add_xml_property(xml_path: Path, key: str, value: str) -> None:
    """Append a new <property> block before </configuration>."""
    text = xml_path.read_text()
    prop = f"<property><name>{key}</name><value>{value}</value></property>\n"
    new_text, count = re.subn(r"</configuration>", prop + "</configuration>", text)
    assert count == 1, f"could not find </configuration> in {xml_path}"
    xml_path.write_text(new_text)


def _modify_env_value(env_path: Path, prefix_key: str, new_value: str) -> None:
    lines = env_path.read_text().splitlines()
    new_lines: list[str] = []
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
# Collection helper — mirrors the production sidecar pattern.
# ---------------------------------------------------------------------------


PROPAGATION_SERVICES = ["namenode", "resourcemanager", "nodemanager", "hive-server2"]


def _collect_store(
    conf_dir: Path,
    env_path: Path,
    *,
    per_service_conf: dict[str, Path] | None = None,
) -> SnapshotStore:
    """Collect a multi-service snapshot store.

    By default every service sees the same ``conf_dir``. ``per_service_conf``
    overrides the conf directory for specific services, which is how real
    cluster bind-mount drift is simulated in the tests.
    """
    store = SnapshotStore()
    per_service_conf = per_service_conf or {}
    for service in PROPAGATION_SERVICES:
        svc_conf = per_service_conf.get(service, conf_dir)
        for snap in collect_all(str(svc_conf), service, str(env_path)):
            store.put(snap)
    return store


# ---------------------------------------------------------------------------
# Mutation catalogue — one Callable per rule in rules/hadoop-3.3.x.yaml.
# Each returns a populated SnapshotStore representing the post-mutation cluster.
# ---------------------------------------------------------------------------


StoreBuilder = Callable[[Path, Path, Path], SnapshotStore]


@dataclass
class Mutation:
    rule_id: str
    description: str
    build: StoreBuilder


def _break_hdfs_replication_max(
    tmp_path: Path, conf_dir: Path, hadoop_env_path: Path
) -> SnapshotStore:
    paths = _copy_fixtures(tmp_path, conf_dir, hadoop_env_path)
    # Raise replication above a newly-introduced max=2.
    _modify_xml_value(paths["conf"] / "hdfs-site.xml", "dfs.replication", "5")
    _add_xml_property(paths["conf"] / "hdfs-site.xml", "dfs.replication.max", "2")
    # Keep hadoop.env aligned so dual-source-consistency stays clean.
    _modify_env_value(paths["env"], "HDFS-SITE.XML_dfs.replication", "5")
    return _collect_store(paths["conf"], paths["env"])


def _break_yarn_scheduler_ceiling(
    tmp_path: Path, conf_dir: Path, hadoop_env_path: Path
) -> SnapshotStore:
    paths = _copy_fixtures(tmp_path, conf_dir, hadoop_env_path)
    # Scheduler max > NM total (9999 > 4096).
    _modify_xml_value(
        paths["conf"] / "yarn-site.xml",
        "yarn.scheduler.maximum-allocation-mb",
        "9999",
    )
    # Keep env aligned so dual-source-consistency doesn't cascade.
    _modify_env_value(
        paths["env"],
        "YARN-SITE.XML_yarn.scheduler.maximum-allocation-mb",
        "9999",
    )
    return _collect_store(paths["conf"], paths["env"])


def _break_fs_defaultfs_propagation(
    tmp_path: Path, conf_dir: Path, hadoop_env_path: Path
) -> SnapshotStore:
    """Simulate bind-mount drift: hive-server2 reads a core-site.xml with a
    different ``fs.defaultFS`` than the HDFS/YARN services. This is the
    real-world failure mode — a per-service conf divergence — and is the
    only shape that the current validator can catch with the shipped rule.

    Also aligns hadoop.env to the majority value so that
    ``dual-source-consistency`` doesn't cascade for services that see the
    majority conf.
    """
    paths = _copy_fixtures(tmp_path, conf_dir, hadoop_env_path)
    hive_conf = _clone_conf(paths["conf"], tmp_path / "hive-conf")
    _modify_xml_value(
        hive_conf / "core-site.xml",
        "fs.defaultFS",
        "hdfs://wrong-namenode:8020",
    )
    _modify_xml_value(
        hive_conf / "hive-site.xml",
        "fs.defaultFS",
        "hdfs://wrong-namenode:8020",
    )
    # Warehouse dir also has to point at the same wrong NN so the
    # hive-warehouse-namenode rule (substring contains) stays happy for
    # the hive service.
    _modify_xml_value(
        hive_conf / "hive-site.xml",
        "hive.metastore.warehouse.dir",
        "hdfs://wrong-namenode:8020/user/hive/warehouse",
    )
    return _collect_store(
        paths["conf"], paths["env"], per_service_conf={"hive-server2": hive_conf},
    )


def _break_hive_warehouse_namenode(
    tmp_path: Path, conf_dir: Path, hadoop_env_path: Path
) -> SnapshotStore:
    paths = _copy_fixtures(tmp_path, conf_dir, hadoop_env_path)
    _modify_xml_value(
        paths["conf"] / "hive-site.xml",
        "hive.metastore.warehouse.dir",
        "hdfs://other-namenode:8020/user/hive/warehouse",
    )
    return _collect_store(paths["conf"], paths["env"])


def _break_dual_source_consistency(
    tmp_path: Path, conf_dir: Path, hadoop_env_path: Path
) -> SnapshotStore:
    """Env disagrees with XML on a key that is present in both but is NOT
    fs.defaultFS (to avoid cascading into fs-defaultfs-propagation)."""
    paths = _copy_fixtures(tmp_path, conf_dir, hadoop_env_path)
    # Pick a YARN key that hadoop.env duplicates but that no propagation
    # or constraint rule cares about. The shuffle class is inert.
    _modify_env_value(
        paths["env"],
        "YARN-SITE.XML_yarn.nodemanager.aux-services.mapreduce.shuffle.class",
        "org.wrong.ShuffleHandler",
    )
    return _collect_store(paths["conf"], paths["env"])


MUTATIONS: list[Mutation] = [
    Mutation("hdfs-replication-max",
             "raise dfs.replication above a newly-introduced max",
             _break_hdfs_replication_max),
    Mutation("yarn-scheduler-ceiling",
             "scheduler max > NM total",
             _break_yarn_scheduler_ceiling),
    Mutation("fs-defaultfs-propagation",
             "hive-server2 sees a different core-site.xml (bind-mount drift)",
             _break_fs_defaultfs_propagation),
    Mutation("hive-warehouse-namenode",
             "warehouse dir points at a different namenode",
             _break_hive_warehouse_namenode),
    Mutation("dual-source-consistency",
             "env disagrees with xml on an inert YARN key",
             _break_dual_source_consistency),
]


@pytest.fixture
def rules_path() -> Path:
    return Path(__file__).parent / "fixtures" / "hadoop-3.3.x.yaml"


# ---------------------------------------------------------------------------
# Regression anchor — the fixture set must produce zero failures.
# Without this, every rule-matrix assertion becomes meaningless.
# ---------------------------------------------------------------------------


def test_baseline_has_no_failures(
    tmp_path: Path, conf_dir: Path, hadoop_env_path: Path, rules_path: Path
) -> None:
    paths = _copy_fixtures(tmp_path, conf_dir, hadoop_env_path)
    store = _collect_store(paths["conf"], paths["env"])
    results = validate(load_rules(rules_path), store)
    failures = [r for r in results if not r.passed]
    assert failures == [], (
        "baseline fixtures should produce zero failures; got: "
        + ", ".join(f"{r.rule_id}({r.details})" for r in failures)
    )


# ---------------------------------------------------------------------------
# Rule matrix.
# ---------------------------------------------------------------------------

# Some mutations legitimately cascade because two rules share observable
# state. These are *expected* co-failures, not bugs.
EXPECTED_COFAILURES: dict[str, set[str]] = {
    # Per-service core-site drift for fs.defaultFS also shows up as:
    #   - dual-source-consistency (hive-server2's XML says wrong, its env
    #     still says right);
    #   - hive-warehouse-namenode (hive's warehouse dir now diverges from
    #     the authoritative namenode fs.defaultFS). Both are semantically
    #     correct cascades, not spurious firings.
    "fs-defaultfs-propagation": {"dual-source-consistency", "hive-warehouse-namenode"},
}


@pytest.mark.parametrize("mutation", MUTATIONS, ids=lambda m: m.rule_id)
def test_mutation_breaks_only_its_rule(
    mutation: Mutation,
    tmp_path: Path,
    conf_dir: Path,
    hadoop_env_path: Path,
    rules_path: Path,
) -> None:
    store = mutation.build(tmp_path, conf_dir, hadoop_env_path)
    results = validate(load_rules(rules_path), store)
    by_id = {r.rule_id: r for r in results}

    target = by_id.get(mutation.rule_id)
    assert target is not None, f"rule {mutation.rule_id} missing from results"
    assert target.passed is False, (
        f"expected {mutation.rule_id} to FAIL after: {mutation.description}. "
        f"details={target.details!r}"
    )

    allowed = {mutation.rule_id} | EXPECTED_COFAILURES.get(mutation.rule_id, set())
    spurious = [
        r for r in results if not r.passed and r.rule_id not in allowed
    ]
    assert spurious == [], (
        f"{mutation.rule_id}: unexpected extra failures — "
        + ", ".join(f"{r.rule_id}({r.details})" for r in spurious)
    )
