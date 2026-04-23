"""Tests for ``checker.collectors.env_collector``.

Covers both ``parse_env_file`` (KEY=VALUE, as used by ``hadoop.env``) and
``parse_jvm_flags`` (``-Dkey=value`` tokens, as used by Hive SERVICE_OPTS).
Runs ``parse_env_file`` against the real ``hadoop.env`` from the cluster.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from checker.collectors.env_collector import (
    EnvCollectorError,
    parse_env_file,
    parse_jvm_flags,
)
from checker.models import SOURCE_ENV_FILE, SOURCE_JVM_FLAGS


# ---------------------------------------------------------------------------
# parse_env_file — real hadoop.env
# ---------------------------------------------------------------------------


class TestRealHadoopEnv:
    """Run against the actual hadoop.env copied into fixtures/."""

    def test_parses_all_13_pairs(self, hadoop_env_path: Path) -> None:
        snap = parse_env_file(hadoop_env_path, service="namenode")
        assert snap.source == SOURCE_ENV_FILE
        # The real file has 13 KEY=VALUE lines (the rest are comments/blank).
        assert len(snap.properties) == 13

    def test_normalisation_strips_hadoop_xml_prefix(
        self, hadoop_env_path: Path
    ) -> None:
        """CORE-SITE.XML_fs.defaultFS should become fs.defaultFS.

        This is the normalisation the drift detector relies on — if it
        regresses, the dual-source consistency rule cannot match env values
        against XML values.
        """
        snap = parse_env_file(hadoop_env_path, service="namenode")
        # The prefixed form must be gone and the bare key must be present.
        assert "CORE-SITE.XML_fs.defaultFS" not in snap.properties
        assert snap.properties["fs.defaultFS"] == "hdfs://namenode:8020"
        # Spot-check keys sourced from each of the four prefixes in the file.
        assert snap.properties["dfs.replication"] == "1"
        assert snap.properties["mapreduce.framework.name"] == "yarn"
        assert snap.properties["yarn.nodemanager.resource.memory-mb"] == "4096"

    def test_normalisation_can_be_disabled(self, hadoop_env_path: Path) -> None:
        """With normalize_hadoop_env=False, prefixed keys remain verbatim."""
        snap = parse_env_file(
            hadoop_env_path, service="namenode", normalize_hadoop_env=False
        )
        assert (
            snap.properties["CORE-SITE.XML_fs.defaultFS"] == "hdfs://namenode:8020"
        )
        assert "fs.defaultFS" not in snap.properties

    def test_values_agree_with_matching_xml_file(
        self, hadoop_env_path: Path, conf_dir: Path
    ) -> None:
        """The whole point of the dual-source scenario: for keys present in
        both, the values should currently agree. If they ever stop agreeing
        it's real drift — which is exactly what the checker is meant to
        detect. Phase 1 just documents the baseline.
        """
        from checker.collectors.xml_collector import collect_xml

        env = parse_env_file(hadoop_env_path, service="namenode")
        xml_snaps = [
            collect_xml(conf_dir / "core-site.xml", service="namenode"),
            collect_xml(conf_dir / "hdfs-site.xml", service="namenode"),
            collect_xml(conf_dir / "yarn-site.xml", service="namenode"),
            collect_xml(conf_dir / "mapred-site.xml", service="namenode"),
        ]
        merged_xml: dict[str, str] = {}
        for s in xml_snaps:
            merged_xml.update(s.properties)

        overlap = set(env.properties) & set(merged_xml)
        assert overlap, "expected some keys to appear in both env and XML"
        for key in overlap:
            assert env.properties[key] == merged_xml[key], (
                f"dual-source drift on {key!r}: env={env.properties[key]!r} "
                f"xml={merged_xml[key]!r}"
            )


# ---------------------------------------------------------------------------
# parse_env_file — synthetic edge cases
# ---------------------------------------------------------------------------


class TestEnvFileParsing:
    def test_basic_key_value(self, tmp_path: Path) -> None:
        p = tmp_path / "x.env"
        p.write_text("FOO=bar\nBAZ=qux\n")
        snap = parse_env_file(p, service="t", normalize_hadoop_env=False)
        assert snap.properties == {"FOO": "bar", "BAZ": "qux"}

    def test_comments_and_blank_lines_skipped(self, tmp_path: Path) -> None:
        p = tmp_path / "x.env"
        p.write_text("# a comment\n\n   \nFOO=bar\n  # indented comment\n")
        snap = parse_env_file(p, service="t", normalize_hadoop_env=False)
        assert snap.properties == {"FOO": "bar"}

    def test_export_prefix_is_accepted(self, tmp_path: Path) -> None:
        p = tmp_path / "x.env"
        p.write_text("export FOO=bar\nexport\tBAZ=qux\n")
        snap = parse_env_file(p, service="t", normalize_hadoop_env=False)
        assert snap.properties == {"FOO": "bar", "BAZ": "qux"}

    def test_quoted_values_are_unquoted(self, tmp_path: Path) -> None:
        p = tmp_path / "x.env"
        p.write_text('FOO="hello world"\nBAR=\'single quoted\'\n')
        snap = parse_env_file(p, service="t", normalize_hadoop_env=False)
        assert snap.properties == {"FOO": "hello world", "BAR": "single quoted"}

    def test_value_containing_equals_preserved(self, tmp_path: Path) -> None:
        """Only the first ``=`` separates key from value."""
        p = tmp_path / "x.env"
        p.write_text("JDBC=jdbc:postgresql://host:5432/db?user=x\n")
        snap = parse_env_file(p, service="t", normalize_hadoop_env=False)
        assert snap.properties == {"JDBC": "jdbc:postgresql://host:5432/db?user=x"}

    def test_line_without_equals_skipped(self, tmp_path: Path) -> None:
        p = tmp_path / "x.env"
        p.write_text("FOO=bar\nnot-a-kv-line\nBAZ=qux\n")
        snap = parse_env_file(p, service="t", normalize_hadoop_env=False)
        assert snap.properties == {"FOO": "bar", "BAZ": "qux"}

    def test_duplicate_keys_last_wins(self, tmp_path: Path) -> None:
        p = tmp_path / "x.env"
        p.write_text("FOO=first\nFOO=second\n")
        snap = parse_env_file(p, service="t", normalize_hadoop_env=False)
        assert snap.properties == {"FOO": "second"}

    def test_missing_file_raises(self, tmp_path: Path) -> None:
        with pytest.raises(EnvCollectorError, match="not a file"):
            parse_env_file(tmp_path / "nope.env", service="t")


# ---------------------------------------------------------------------------
# parse_jvm_flags
# ---------------------------------------------------------------------------


class TestJvmFlagsParsing:
    def test_single_flag(self) -> None:
        snap = parse_jvm_flags("-Dfoo=bar", service="t")
        assert snap.source == SOURCE_JVM_FLAGS
        assert snap.properties == {"foo": "bar"}

    def test_real_hive_server2_opts(self) -> None:
        """The exact SERVICE_OPTS string from the hive-server2 service."""
        opts = (
            "-Dhive.metastore.uris=thrift://hive-metastore:9083 "
            "-Dhive.server2.thrift.port=10000 "
            "-Dhive.server2.thrift.bind.host=0.0.0.0 "
            "-Dhive.server2.authentication=NONE "
            "-Dhive.server2.enable.doAs=false "
            "-Dfs.defaultFS=hdfs://namenode:8020"
        )
        snap = parse_jvm_flags(opts, service="hive-server2", source_path="SERVICE_OPTS")
        assert snap.properties == {
            "hive.metastore.uris": "thrift://hive-metastore:9083",
            "hive.server2.thrift.port": "10000",
            "hive.server2.thrift.bind.host": "0.0.0.0",
            "hive.server2.authentication": "NONE",
            "hive.server2.enable.doAs": "false",
            "fs.defaultFS": "hdfs://namenode:8020",
        }
        # The propagation target: fs.defaultFS from core-site must equal what
        # SERVICE_OPTS injected into hive-server2.
        assert snap.properties["fs.defaultFS"] == "hdfs://namenode:8020"

    def test_real_hive_metastore_opts(self) -> None:
        """hive-metastore's SERVICE_OPTS includes the JDBC connection string
        with an embedded ``/`` and ``:`` — make sure the value survives
        whole.
        """
        opts = (
            "-Djavax.jdo.option.ConnectionDriverName=org.postgresql.Driver "
            "-Djavax.jdo.option.ConnectionURL="
            "jdbc:postgresql://postgres-metastore:5432/metastore "
            "-Djavax.jdo.option.ConnectionUserName=hive "
            "-Djavax.jdo.option.ConnectionPassword=hive "
            "-Dhive.metastore.warehouse.dir=hdfs://namenode:8020/user/hive/warehouse "
            "-Dfs.defaultFS=hdfs://namenode:8020"
        )
        snap = parse_jvm_flags(opts, service="hive-metastore")
        assert (
            snap.properties["javax.jdo.option.ConnectionURL"]
            == "jdbc:postgresql://postgres-metastore:5432/metastore"
        )
        assert (
            snap.properties["hive.metastore.warehouse.dir"]
            == "hdfs://namenode:8020/user/hive/warehouse"
        )

    def test_ignores_non_dash_d_tokens(self) -> None:
        """``-Xmx2g`` and similar tokens shouldn't show up as properties."""
        snap = parse_jvm_flags("-Xmx2g -Dfoo=bar -verbose:gc", service="t")
        assert snap.properties == {"foo": "bar"}

    def test_bare_dash_d_key_maps_to_empty(self) -> None:
        """``-Dkey`` without ``=`` is valid JVM syntax for an empty value."""
        snap = parse_jvm_flags("-Dfoo -Dbar=1", service="t")
        assert snap.properties == {"foo": "", "bar": "1"}

    def test_value_containing_equals_preserved(self) -> None:
        snap = parse_jvm_flags("-Dquery=a=b&c=d", service="t")
        assert snap.properties == {"query": "a=b&c=d"}

    def test_quoted_value_with_spaces(self) -> None:
        snap = parse_jvm_flags('-Dmsg="hello world" -Dn=1', service="t")
        assert snap.properties == {"msg": "hello world", "n": "1"}

    def test_duplicate_key_last_wins(self) -> None:
        snap = parse_jvm_flags("-Dfoo=1 -Dfoo=2", service="t")
        assert snap.properties == {"foo": "2"}

    def test_empty_input_produces_empty_properties(self) -> None:
        snap = parse_jvm_flags("", service="t")
        assert snap.properties == {}
        snap = parse_jvm_flags("   \t  ", service="t")
        assert snap.properties == {}

    def test_unmatched_quote_falls_back_to_whitespace_split(self) -> None:
        """Malformed input still yields as many valid -D tokens as possible."""
        snap = parse_jvm_flags('-Dgood=1 -Dbroken="unclosed -Dalso=ok', service="t")
        # After the fallback tokenisation, at least the well-formed tokens
        # before the break should be extracted.
        assert snap.properties.get("good") == "1"


# ---------------------------------------------------------------------------
# Snapshot metadata
# ---------------------------------------------------------------------------


class TestSnapshotMetadata:
    def test_env_file_source_and_path(self, hadoop_env_path: Path) -> None:
        snap = parse_env_file(hadoop_env_path, service="namenode")
        assert snap.source == SOURCE_ENV_FILE
        assert snap.source_path == str(hadoop_env_path)
        assert snap.service == "namenode"

    def test_jvm_flags_source_path_carried_through(self) -> None:
        snap = parse_jvm_flags(
            "-Dfoo=bar", service="hive-server2", source_path="SERVICE_OPTS"
        )
        assert snap.source_path == "SERVICE_OPTS"
        assert snap.source == SOURCE_JVM_FLAGS

    def test_service_fallback_to_env_var(
        self, hadoop_env_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("CHECKER_SERVICE_NAME", "from-env")
        snap = parse_env_file(hadoop_env_path)
        assert snap.service == "from-env"
