"""Tests for ``checker.collectors.spark_collector``.

Stage 3.

The plan: parse ``spark-defaults.conf`` whitespace-separated KEY VALUE
pairs, ignore comments and blank lines, last-wins on duplicate keys.
Snapshots have source ``spark_conf``.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from checker.collectors.spark_collector import (
    SparkCollectorError,
    parse_spark_conf,
)
from checker.models import SOURCE_SPARK_CONF


# ---------------------------------------------------------------------------
# Real fixture: tests/checker/fixtures/conf/spark-defaults.conf
# ---------------------------------------------------------------------------


class TestRealSparkDefaults:
    def test_parses_real_fixture(self, conf_dir: Path) -> None:
        snap = parse_spark_conf(conf_dir / "spark-defaults.conf", service="spark-client")
        # We seed the fixture with these well-known keys; assert they're all there.
        assert snap.properties["spark.master"] == "yarn"
        assert snap.properties["spark.submit.deployMode"] == "client"
        assert snap.properties["spark.driver.memory"] == "1g"
        assert snap.properties["spark.hadoop.fs.defaultFS"] == "hdfs://namenode:8020"

    def test_real_fixture_has_no_comment_keys(self, conf_dir: Path) -> None:
        snap = parse_spark_conf(conf_dir / "spark-defaults.conf")
        # No key should start with '#' — we strip comment lines entirely.
        assert all(not k.startswith("#") for k in snap.properties)


# ---------------------------------------------------------------------------
# Parser unit tests — using inline strings via tmp_path
# ---------------------------------------------------------------------------


class TestSparkConfParsing:
    def _write(self, tmp_path: Path, body: str) -> Path:
        p = tmp_path / "spark-defaults.conf"
        p.write_text(body, encoding="utf-8")
        return p

    def test_basic_key_value(self, tmp_path: Path) -> None:
        p = self._write(tmp_path, "spark.master  yarn\n")
        snap = parse_spark_conf(p)
        assert snap.properties == {"spark.master": "yarn"}

    def test_comments_and_blank_lines_skipped(self, tmp_path: Path) -> None:
        p = self._write(tmp_path, """\
# this is a comment
\t# indented comment

spark.master   yarn

# trailing comment
""")
        snap = parse_spark_conf(p)
        assert snap.properties == {"spark.master": "yarn"}

    def test_value_can_contain_spaces(self, tmp_path: Path) -> None:
        # Spark splits on the first whitespace run; everything after is value.
        p = self._write(tmp_path, "spark.executor.extraJavaOptions   -Xmx2g -XX:+UseG1GC\n")
        snap = parse_spark_conf(p)
        assert snap.properties == {
            "spark.executor.extraJavaOptions": "-Xmx2g -XX:+UseG1GC",
        }

    def test_value_can_contain_equals(self, tmp_path: Path) -> None:
        # key=value tokens inside the value (e.g. JVM args) survive intact.
        p = self._write(
            tmp_path,
            "spark.driver.extraJavaOptions    -Dlog.dir=/var/log -Xmx1g\n",
        )
        snap = parse_spark_conf(p)
        assert snap.properties["spark.driver.extraJavaOptions"] == \
            "-Dlog.dir=/var/log -Xmx1g"

    def test_duplicate_keys_last_wins(self, tmp_path: Path) -> None:
        p = self._write(tmp_path, """\
spark.master   yarn
spark.master   local[2]
""")
        snap = parse_spark_conf(p)
        assert snap.properties == {"spark.master": "local[2]"}

    def test_tab_separated_works(self, tmp_path: Path) -> None:
        p = self._write(tmp_path, "spark.master\tyarn\n")
        snap = parse_spark_conf(p)
        assert snap.properties == {"spark.master": "yarn"}

    def test_bare_key_no_value_is_skipped(self, tmp_path: Path) -> None:
        # A key with no whitespace-separated value isn't meaningful.
        p = self._write(tmp_path, "spark.master\n")
        snap = parse_spark_conf(p)
        assert snap.properties == {}

    def test_empty_file_is_empty_snapshot(self, tmp_path: Path) -> None:
        p = self._write(tmp_path, "")
        snap = parse_spark_conf(p)
        assert snap.properties == {}

    def test_missing_file_raises(self, tmp_path: Path) -> None:
        with pytest.raises(SparkCollectorError):
            parse_spark_conf(tmp_path / "does-not-exist.conf")

    def test_directory_raises(self, tmp_path: Path) -> None:
        with pytest.raises(SparkCollectorError):
            parse_spark_conf(tmp_path)


# ---------------------------------------------------------------------------
# Snapshot metadata
# ---------------------------------------------------------------------------


class TestSnapshotMetadata:
    def test_source_tag_is_spark_conf(self, conf_dir: Path) -> None:
        snap = parse_spark_conf(conf_dir / "spark-defaults.conf", service="spark-client")
        assert snap.source == SOURCE_SPARK_CONF

    def test_source_path_carried_through(self, conf_dir: Path) -> None:
        path = conf_dir / "spark-defaults.conf"
        snap = parse_spark_conf(path, service="spark-client")
        assert snap.source_path == str(path)

    def test_agent_id_uses_service_and_stem(self, conf_dir: Path) -> None:
        snap = parse_spark_conf(conf_dir / "spark-defaults.conf", service="spark-client")
        assert snap.agent_id == "spark-client-spark-defaults"

    def test_service_falls_back_to_env_var(self, conf_dir: Path, monkeypatch) -> None:
        monkeypatch.setenv("CHECKER_SERVICE_NAME", "envspark")
        snap = parse_spark_conf(conf_dir / "spark-defaults.conf")
        assert snap.service == "envspark"

    def test_service_falls_back_to_unknown(self, conf_dir: Path, monkeypatch) -> None:
        monkeypatch.delenv("CHECKER_SERVICE_NAME", raising=False)
        snap = parse_spark_conf(conf_dir / "spark-defaults.conf")
        assert snap.service == "unknown"
