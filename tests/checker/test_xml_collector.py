"""Tests for ``checker.collectors.xml_collector``.

Per the project plan, no mocking: these tests run ``collect_xml`` against
the real ``conf/*-site.xml`` files from the cluster. The fixture directory
carries copies so the tests are self-contained and don't require a
particular repo layout.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from checker.collectors.xml_collector import (
    XmlCollectorError,
    _extract_properties,
    collect_xml,
)
from checker.models import SOURCE_XML_FILE, ConfigSnapshot


# ---------------------------------------------------------------------------
# Smoke tests against the real conf files
# ---------------------------------------------------------------------------


class TestRealConfFiles:
    """Exercise collect_xml against every real XML in ``conf/``."""

    def test_core_site_parses(self, conf_dir: Path) -> None:
        snap = collect_xml(conf_dir / "core-site.xml", service="namenode")
        assert isinstance(snap, ConfigSnapshot)
        assert snap.service == "namenode"
        assert snap.source == SOURCE_XML_FILE
        assert snap.properties == {"fs.defaultFS": "hdfs://namenode:8020"}

    def test_hdfs_site_parses_all_four_properties(self, conf_dir: Path) -> None:
        snap = collect_xml(conf_dir / "hdfs-site.xml", service="namenode")
        assert snap.properties == {
            "dfs.namenode.rpc-address": "namenode:8020",
            "dfs.datanode.data.dir": "/hadoop/dfs/data",
            "dfs.namenode.name.dir": "/hadoop/dfs/name",
            "dfs.replication": "1",
        }

    def test_yarn_site_parses_memory_and_cap_values(self, conf_dir: Path) -> None:
        snap = collect_xml(conf_dir / "yarn-site.xml", service="nodemanager")
        # The two values the plan calls out as the scheduler-ceiling rule inputs.
        assert snap.properties["yarn.nodemanager.resource.memory-mb"] == "4096"
        assert snap.properties["yarn.scheduler.maximum-allocation-mb"] == "2048"
        # The deliberate cgroup workaround the plan says to suppress.
        assert snap.properties["yarn.nodemanager.vmem-check-enabled"] == "false"

    def test_mapred_site_parses(self, conf_dir: Path) -> None:
        snap = collect_xml(conf_dir / "mapred-site.xml", service="resourcemanager")
        assert snap.properties["mapreduce.framework.name"] == "yarn"
        # Classpath is a colon-separated string — make sure we didn't split it.
        cp = snap.properties["mapreduce.application.classpath"]
        assert ":" in cp
        assert cp.startswith("/opt/hadoop")

    def test_hive_site_parses_short_n_tag_variant(self, conf_dir: Path) -> None:
        """hive-site.xml uses ``<n>`` in place of ``<n>`` — the collector
        has to handle both. If this test fails, the _NAME_TAGS fallback is
        broken.
        """
        snap = collect_xml(conf_dir / "hive-site.xml", service="hive-server2")
        # 15 properties in the real file.
        assert len(snap.properties) == 15
        # Spot-check values from across the file.
        assert (
            snap.properties["javax.jdo.option.ConnectionURL"]
            == "jdbc:postgresql://postgres-metastore:5432/metastore"
        )
        assert snap.properties["hive.metastore.uris"] == "thrift://hive-metastore:9083"
        assert (
            snap.properties["hive.metastore.warehouse.dir"]
            == "hdfs://namenode:8020/user/hive/warehouse"
        )
        # fs.defaultFS duplicated from core-site — needed by the propagation rule.
        assert snap.properties["fs.defaultFS"] == "hdfs://namenode:8020"


# ---------------------------------------------------------------------------
# Snapshot metadata
# ---------------------------------------------------------------------------


class TestSnapshotMetadata:
    def test_agent_id_uses_service_and_file_stem(self, conf_dir: Path) -> None:
        snap = collect_xml(conf_dir / "hdfs-site.xml", service="namenode")
        assert snap.agent_id == "namenode-hdfs-site"

    def test_source_path_is_the_path_given(self, conf_dir: Path) -> None:
        path = conf_dir / "core-site.xml"
        snap = collect_xml(path, service="namenode")
        assert snap.source_path == str(path)

    def test_host_override_propagates(self, conf_dir: Path) -> None:
        snap = collect_xml(conf_dir / "core-site.xml", service="namenode", host="nn-01")
        assert snap.host == "nn-01"

    def test_service_override_propagates(self, conf_dir: Path) -> None:
        snap = collect_xml(conf_dir / "core-site.xml", service="custom")
        assert snap.service == "custom"

    def test_service_falls_back_to_env_var(
        self, conf_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("CHECKER_SERVICE_NAME", "from-env")
        snap = collect_xml(conf_dir / "core-site.xml")
        assert snap.service == "from-env"

    def test_service_falls_back_to_unknown(
        self, conf_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv("CHECKER_SERVICE_NAME", raising=False)
        snap = collect_xml(conf_dir / "core-site.xml")
        assert snap.service == "unknown"

    def test_timestamp_is_iso_8601_z(self, conf_dir: Path) -> None:
        snap = collect_xml(conf_dir / "core-site.xml", service="namenode")
        assert snap.timestamp.endswith("Z")
        # Round-trip through ConfigSnapshot.from_dict/to_dict shouldn't lose it.
        assert ConfigSnapshot.from_dict(snap.to_dict()).timestamp == snap.timestamp


# ---------------------------------------------------------------------------
# Tag variants and edge cases — tested via small XML blobs written to tmp
# ---------------------------------------------------------------------------


class TestTagVariants:
    def test_both_name_and_n_tags_in_same_file(self, tmp_path: Path) -> None:
        """A single file with mixed ``<n>`` / ``<n>`` tags: both parse."""
        p = tmp_path / "mixed.xml"
        p.write_text(
            "<configuration>"
            "<property><name>key.long</name><value>A</value></property>"
            "<property><n>key.short</n><value>B</value></property>"
            "</configuration>"
        )
        snap = collect_xml(p, service="test")
        assert snap.properties == {"key.long": "A", "key.short": "B"}

    def test_empty_value_preserved_as_empty_string(self, tmp_path: Path) -> None:
        """An explicit empty ``<value/>`` is a meaningful "unset" — keep it."""
        p = tmp_path / "empty.xml"
        p.write_text(
            "<configuration>"
            "<property><name>key</name><value></value></property>"
            "</configuration>"
        )
        snap = collect_xml(p, service="test")
        assert snap.properties == {"key": ""}

    def test_property_without_value_element_maps_to_empty(self, tmp_path: Path) -> None:
        p = tmp_path / "novalue.xml"
        p.write_text(
            "<configuration>"
            "<property><name>key</name></property>"
            "</configuration>"
        )
        snap = collect_xml(p, service="test")
        assert snap.properties == {"key": ""}

    def test_property_without_name_element_is_skipped(self, tmp_path: Path) -> None:
        p = tmp_path / "noname.xml"
        p.write_text(
            "<configuration>"
            "<property><value>orphaned</value></property>"
            "<property><name>valid</name><value>ok</value></property>"
            "</configuration>"
        )
        snap = collect_xml(p, service="test")
        assert snap.properties == {"valid": "ok"}

    def test_duplicate_key_last_wins(self, tmp_path: Path) -> None:
        p = tmp_path / "dup.xml"
        p.write_text(
            "<configuration>"
            "<property><name>k</name><value>first</value></property>"
            "<property><name>k</name><value>second</value></property>"
            "</configuration>"
        )
        snap = collect_xml(p, service="test")
        assert snap.properties == {"k": "second"}

    def test_description_and_final_elements_are_ignored(self, tmp_path: Path) -> None:
        """Hadoop XMLs may include ``<description>`` and ``<final>``. We
        only look at ``<name>``/``<n>`` and ``<value>``; the rest is noise
        for Phase 1.
        """
        p = tmp_path / "extra.xml"
        p.write_text(
            "<configuration>"
            "<property>"
            "<name>k</name><value>v</value>"
            "<description>ignored</description>"
            "<final>true</final>"
            "</property>"
            "</configuration>"
        )
        snap = collect_xml(p, service="test")
        assert snap.properties == {"k": "v"}

    def test_whitespace_around_values_is_stripped(self, tmp_path: Path) -> None:
        p = tmp_path / "ws.xml"
        p.write_text(
            "<configuration>"
            "<property><name>key</name><value>\n  spaced  \n</value></property>"
            "</configuration>"
        )
        snap = collect_xml(p, service="test")
        assert snap.properties == {"key": "spaced"}


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------


class TestErrorCases:
    def test_missing_file_raises(self, tmp_path: Path) -> None:
        with pytest.raises(XmlCollectorError, match="not a file"):
            collect_xml(tmp_path / "does-not-exist.xml", service="test")

    def test_malformed_xml_raises(self, tmp_path: Path) -> None:
        p = tmp_path / "broken.xml"
        p.write_text("<configuration><property><name>k</name>")  # truncated
        with pytest.raises(XmlCollectorError, match="malformed XML"):
            collect_xml(p, service="test")

    def test_wrong_root_element_raises(self, tmp_path: Path) -> None:
        p = tmp_path / "wrong-root.xml"
        p.write_text("<settings><property><name>k</name><value>v</value></property></settings>")
        with pytest.raises(XmlCollectorError, match="expected <configuration>"):
            collect_xml(p, service="test")

    def test_directory_path_raises(self, tmp_path: Path) -> None:
        with pytest.raises(XmlCollectorError, match="not a file"):
            collect_xml(tmp_path, service="test")
