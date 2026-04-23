"""Tests for ``checker.agent`` — collection and configuration logic.

These tests exercise the agent's collection pipeline without requiring
Kafka or watchdog.  They use the same real fixture files from Phase 1.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from checker.agent import collect_all, _env, _env_int


class TestCollectAll:
    """Integration tests for the collect_all entry point."""

    def test_collects_all_xml_files(self, conf_dir: Path) -> None:
        """collect_all should find every *-site.xml in conf_dir."""
        snapshots = collect_all(str(conf_dir), service="namenode")
        xml_names = {s.source_path for s in snapshots}
        # The fixture conf/ has 5 XML files.
        assert len(snapshots) == 5
        assert any("core-site.xml" in p for p in xml_names)
        assert any("hdfs-site.xml" in p for p in xml_names)
        assert any("yarn-site.xml" in p for p in xml_names)
        assert any("mapred-site.xml" in p for p in xml_names)
        assert any("hive-site.xml" in p for p in xml_names)

    def test_collects_xml_plus_env(
        self, conf_dir: Path, hadoop_env_path: Path
    ) -> None:
        """When env_path is given, we get XML snapshots + 1 env snapshot."""
        snapshots = collect_all(
            str(conf_dir), service="namenode", env_path=str(hadoop_env_path)
        )
        sources = {s.source for s in snapshots}
        assert "xml_file" in sources
        assert "env_file" in sources
        # 5 XML + 1 env = 6
        assert len(snapshots) == 6

    def test_collects_xml_plus_jvm_flags(self, conf_dir: Path) -> None:
        """When jvm_flags is given, we get XML snapshots + 1 JVM snapshot."""
        flags = "-Dfs.defaultFS=hdfs://namenode:8020 -Dhive.metastore.uris=thrift://hm:9083"
        snapshots = collect_all(
            str(conf_dir),
            service="hive-server2",
            jvm_flags=flags,
            jvm_flags_name="SERVICE_OPTS",
        )
        sources = {s.source for s in snapshots}
        assert "jvm_flags" in sources
        jvm_snap = [s for s in snapshots if s.source == "jvm_flags"][0]
        assert jvm_snap.source_path == "SERVICE_OPTS"
        assert jvm_snap.properties["fs.defaultFS"] == "hdfs://namenode:8020"

    def test_collects_all_three_sources(
        self, conf_dir: Path, hadoop_env_path: Path
    ) -> None:
        """XML + env + JVM flags all together."""
        snapshots = collect_all(
            str(conf_dir),
            service="namenode",
            env_path=str(hadoop_env_path),
            jvm_flags="-Dfoo=bar",
        )
        sources = {s.source for s in snapshots}
        assert sources == {"xml_file", "env_file", "jvm_flags"}

    def test_nonexistent_conf_dir_yields_empty(self, tmp_path: Path) -> None:
        """A missing conf dir should produce zero snapshots, not crash."""
        snapshots = collect_all(str(tmp_path / "nope"), service="test")
        assert snapshots == []

    def test_empty_jvm_flags_skipped(self, conf_dir: Path) -> None:
        """An empty JVM flags string should not add a snapshot."""
        snapshots = collect_all(str(conf_dir), service="test", jvm_flags="")
        assert all(s.source != "jvm_flags" for s in snapshots)

    def test_all_snapshots_have_correct_service(self, conf_dir: Path) -> None:
        snapshots = collect_all(str(conf_dir), service="my-service")
        assert all(s.service == "my-service" for s in snapshots)


class TestEnvHelpers:
    """Test the _env / _env_int config helpers."""

    def test_env_returns_default(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("TEST_KEY_XYZ", raising=False)
        assert _env("TEST_KEY_XYZ", "fallback") == "fallback"

    def test_env_returns_set_value(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("TEST_KEY_XYZ", "custom")
        assert _env("TEST_KEY_XYZ", "fallback") == "custom"

    def test_env_int_returns_default(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("TEST_INT_XYZ", raising=False)
        assert _env_int("TEST_INT_XYZ", 42) == 42

    def test_env_int_parses_value(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("TEST_INT_XYZ", "100")
        assert _env_int("TEST_INT_XYZ", 42) == 100

    def test_env_int_invalid_falls_back(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("TEST_INT_XYZ", "not-a-number")
        assert _env_int("TEST_INT_XYZ", 42) == 42
