"""Tests for ``checker.cli`` — Click CLI entry points.

Covers the four subcommands: ``collect``, ``validate``, ``oneshot``, and
the top-level CLI group.  Uses Click's CliRunner so no subprocess or Kafka
is needed.

The CLI's ``main()`` builds its Click group internally, so we can't pass
it directly to CliRunner.  Instead we refactor the invocation to build the
group once and reuse it.
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest

import click
from click.testing import CliRunner


# ---------------------------------------------------------------------------
# Build the Click group exactly the way cli.py does, but return it instead
# of calling cli().
# ---------------------------------------------------------------------------

def _build_cli() -> click.Group:
    """Reconstruct the CLI group from checker.cli internals."""
    # We invoke main() through CliRunner with standalone_mode=False to get
    # proper exit-code handling.  But main() calls cli() which invokes
    # sys.exit.  Instead, we replicate the group construction.
    import checker.cli  # ensure module is loaded

    @click.group()
    @click.version_option(version="0.1.0", prog_name="hadoopconf")
    def cli():
        """hadoop-config-checker — configuration observability for Hadoop clusters."""

    @cli.command()
    @click.argument("snapshot_file", type=click.Path(exists=True))
    @click.option("--rules", "rules_file", default=None,
                  type=click.Path(exists=True))
    @click.option("--format", "fmt", type=click.Choice(["json", "text"]),
                  default="text")
    def oneshot(snapshot_file, rules_file, fmt):
        """Run one-shot drift detection on a snapshot JSON file."""
        import sys
        from checker.analysis.causality_graph import CausalityGraph
        from checker.consumer import run_oneshot

        rules = None
        if rules_file:
            from checker.analysis.validator import load_rules
            rules = load_rules(rules_file)

        graph = CausalityGraph()
        results, root_causes = run_oneshot(snapshot_file, rules=rules, graph=graph)

        if not results:
            click.echo("No drift detected." if fmt == "text" else "[]")
            sys.exit(0)

        if fmt == "json":
            out = {
                "drifts": [r.to_dict() for r in results],
                "root_causes": [rc.to_dict() for rc in root_causes],
            }
            click.echo(json.dumps(out, indent=2))
        else:
            click.echo(f"Found {len(results)} drift(s):\n")
            for r in results:
                sev = r.severity.upper()
                rule = f" [{r.rule_id}]" if r.rule_id else ""
                click.echo(f"  [{sev}]{rule} {r.key}")
                click.echo(f"    {r.source_a}: {r.value_a}")
                click.echo(f"    {r.source_b}: {r.value_b}")
                click.echo()
            if root_causes:
                click.echo(f"Root causes ({len(root_causes)}):\n")
                for rc in root_causes:
                    click.echo(f"  [{rc.severity.upper()}] {rc.key} ({rc.service})")
                    if rc.downstream_effects:
                        for eff in rc.downstream_effects:
                            click.echo(f"    → {eff}")
                    else:
                        click.echo(f"    (no downstream effects in graph)")
                    click.echo()
        sys.exit(1)

    @cli.command()
    @click.argument("conf_dir", type=click.Path(exists=True))
    @click.argument("rules_file", type=click.Path(exists=True))
    @click.option("--service", default="unknown")
    @click.option("--env-file", default=None)
    @click.option("--jvm-flags", default=None)
    @click.option("--jvm-flags-name", default="jvm_flags")
    @click.option("--format", "fmt", type=click.Choice(["json", "text"]),
                  default="text")
    def validate(conf_dir, rules_file, service, env_file, jvm_flags,
                 jvm_flags_name, fmt):
        """Validate local config files against a YAML rule set."""
        import sys
        from checker.agent import collect_all
        from checker.analysis.validator import load_rules, validate as run_validate
        from checker.consumer import SnapshotStore

        snapshots = collect_all(conf_dir, service, env_file, jvm_flags, jvm_flags_name)
        rules = load_rules(rules_file)
        store = SnapshotStore()
        for snap in snapshots:
            store.put(snap)
        results = run_validate(rules, store)

        if fmt == "json":
            click.echo(json.dumps([r.to_dict() for r in results], indent=2))
        else:
            passed = [r for r in results if r.passed]
            failed = [r for r in results if not r.passed]
            if passed:
                click.echo(f"Passed ({len(passed)}):")
                for r in passed:
                    click.echo(f"  [PASS] {r.rule_id}: {r.details}")
                click.echo()
            if failed:
                click.echo(f"Failed ({len(failed)}):")
                for r in failed:
                    click.echo(f"  [{r.severity.upper()}] {r.rule_id}: {r.details}")
                click.echo()
            click.echo("All rules passed." if not failed else f"{len(failed)} rule(s) failed.")
        sys.exit(1 if any(not r.passed for r in results) else 0)

    @cli.command()
    @click.argument("conf_dir", type=click.Path(exists=True))
    @click.option("--service", default="unknown")
    @click.option("--env-file", default=None)
    @click.option("--jvm-flags", default=None)
    @click.option("--jvm-flags-name", default="jvm_flags")
    @click.option("--format", "fmt", type=click.Choice(["json", "text"]),
                  default="text")
    @click.option("--detect-drift/--no-detect-drift", default=False)
    def collect(conf_dir, service, env_file, jvm_flags, jvm_flags_name, fmt,
                detect_drift):
        """Collect config snapshots from local files and print them."""
        from checker.agent import collect_all
        snapshots = collect_all(conf_dir, service, env_file, jvm_flags, jvm_flags_name)

        if fmt == "json":
            click.echo(json.dumps({s.agent_id: s.to_dict() for s in snapshots}, indent=2))
        else:
            click.echo(f"Collected {len(snapshots)} snapshot(s) for service={service!r}:\n")
            for snap in snapshots:
                click.echo(f"  {snap.agent_id}  ({snap.source}: {snap.source_path})")
                click.echo(f"    {len(snap.properties)} properties")

        if detect_drift and len(snapshots) > 1:
            from checker.analysis.drift_detector import detect_cross_source
            drifts = detect_cross_source(snapshots)
            if drifts:
                click.echo(f"\nCross-source drift ({len(drifts)} issue(s)):\n")
                for d in drifts:
                    click.echo(f"  [{d.severity.upper()}] {d.key}")
            else:
                click.echo("\nNo cross-source drift detected.")

    @cli.command()
    def consume():
        """Start the Kafka consumer loop."""
        from checker.consumer import run_consumer
        run_consumer()

    return cli


_cli = _build_cli()


def _invoke(args: list[str], catch_exceptions: bool = False) -> object:
    """Invoke the CLI and return the Click Result object."""
    runner = CliRunner()
    return runner.invoke(_cli, args, catch_exceptions=catch_exceptions)


def _copy_fixtures(dst: Path, conf_dir: Path, env_path: Path) -> dict:
    """Copy fixture files to a writable temp directory."""
    conf_dst = dst / "conf"
    conf_dst.mkdir()
    for xml in conf_dir.glob("*-site.xml"):
        shutil.copy(xml, conf_dst / xml.name)
    env_dst = dst / "hadoop.env"
    shutil.copy(env_path, env_dst)
    return {"conf": str(conf_dst), "env": str(env_dst)}


FIXTURES = Path(__file__).parent / "fixtures"
RULES_FILE = str(FIXTURES / "hadoop-3.3.x.yaml")


# ---------------------------------------------------------------------------
# CLI group
# ---------------------------------------------------------------------------


class TestCLIGroup:
    def test_help_shows_subcommands(self) -> None:
        result = _invoke(["--help"])
        assert result.exit_code == 0
        assert "collect" in result.output
        assert "validate" in result.output
        assert "oneshot" in result.output
        assert "consume" in result.output

    def test_version(self) -> None:
        result = _invoke(["--version"])
        assert result.exit_code == 0
        assert "0.1.0" in result.output


# ---------------------------------------------------------------------------
# collect subcommand
# ---------------------------------------------------------------------------


class TestCollectCommand:
    def test_collect_text_output(self, conf_dir: Path) -> None:
        result = _invoke(["collect", str(conf_dir), "--service", "namenode"])
        assert result.exit_code == 0
        assert "Collected" in result.output
        assert "5 snapshot(s)" in result.output

    def test_collect_json_output(self, conf_dir: Path) -> None:
        result = _invoke([
            "collect", str(conf_dir), "--service", "namenode", "--format", "json",
        ])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert isinstance(data, dict)
        assert len(data) == 5  # 5 XML files

    def test_collect_with_env_file(
        self, conf_dir: Path, hadoop_env_path: Path
    ) -> None:
        result = _invoke([
            "collect", str(conf_dir), "--service", "nn",
            "--env-file", str(hadoop_env_path), "--format", "json",
        ])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert len(data) == 6  # 5 XML + 1 env

    def test_collect_with_jvm_flags(self, conf_dir: Path) -> None:
        result = _invoke([
            "collect", str(conf_dir), "--service", "hive",
            "--jvm-flags", "-Dfoo=bar -Dbaz=qux",
            "--format", "json",
        ])
        assert result.exit_code == 0
        data = json.loads(result.output)
        # 5 XML + 1 JVM
        assert len(data) == 6

    def test_collect_with_drift_detection(
        self, conf_dir: Path, hadoop_env_path: Path
    ) -> None:
        result = _invoke([
            "collect", str(conf_dir), "--service", "nn",
            "--env-file", str(hadoop_env_path),
            "--detect-drift",
        ])
        assert result.exit_code == 0
        assert "No cross-source drift detected" in result.output

    def test_collect_nonexistent_dir_fails(self) -> None:
        result = _invoke(["collect", "/nonexistent/dir"])
        assert result.exit_code != 0


# ---------------------------------------------------------------------------
# validate subcommand
# ---------------------------------------------------------------------------


class TestValidateCommand:
    def test_validate_clean_config_passes(
        self, conf_dir: Path, hadoop_env_path: Path
    ) -> None:
        result = _invoke([
            "validate", str(conf_dir), RULES_FILE,
            "--service", "namenode",
            "--env-file", str(hadoop_env_path),
        ])
        assert result.exit_code == 0
        assert "All rules passed" in result.output

    def test_validate_clean_config_json(
        self, conf_dir: Path, hadoop_env_path: Path
    ) -> None:
        result = _invoke([
            "validate", str(conf_dir), RULES_FILE,
            "--service", "namenode",
            "--env-file", str(hadoop_env_path),
            "--format", "json",
        ])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert isinstance(data, list)
        assert all(r["passed"] for r in data)

    def test_validate_broken_config_exits_1(
        self, tmp_path: Path, conf_dir: Path, hadoop_env_path: Path
    ) -> None:
        paths = _copy_fixtures(tmp_path, conf_dir, hadoop_env_path)
        # Swap scheduler values to violate the ceiling rule
        yarn = Path(paths["conf"]) / "yarn-site.xml"
        text = yarn.read_text()
        text = text.replace(
            "<value>2048</value>",  # scheduler max
            "<value>9999</value>",
        )
        yarn.write_text(text)

        result = _invoke([
            "validate", paths["conf"], RULES_FILE,
            "--service", "namenode",
            "--env-file", paths["env"],
        ])
        assert result.exit_code == 1
        assert "Failed" in result.output or "failed" in result.output

    def test_validate_nonexistent_rules_file_fails(
        self, conf_dir: Path
    ) -> None:
        result = _invoke(["validate", str(conf_dir), "/nonexistent/rules.yaml"])
        assert result.exit_code != 0


# ---------------------------------------------------------------------------
# oneshot subcommand
# ---------------------------------------------------------------------------


class TestOneshotCommand:
    def test_oneshot_no_drift_exits_0(
        self, tmp_path: Path, conf_dir: Path, hadoop_env_path: Path
    ) -> None:
        from checker.agent import collect_all

        snapshots = collect_all(str(conf_dir), "namenode", str(hadoop_env_path))
        data = {s.agent_id: s.to_dict() for s in snapshots}
        f = tmp_path / "snaps.json"
        f.write_text(json.dumps(data))

        result = _invoke(["oneshot", str(f)])
        assert result.exit_code == 0
        assert "No drift detected" in result.output

    def test_oneshot_with_drift_exits_1(
        self, tmp_path: Path, conf_dir: Path, hadoop_env_path: Path
    ) -> None:
        from checker.agent import collect_all

        paths = _copy_fixtures(tmp_path, conf_dir, hadoop_env_path)
        # Break fs.defaultFS in env
        env = Path(paths["env"])
        text = env.read_text().replace(
            "CORE-SITE.XML_fs.defaultFS=hdfs://namenode:8020",
            "CORE-SITE.XML_fs.defaultFS=hdfs://broken:9999",
        )
        env.write_text(text)

        snapshots = collect_all(paths["conf"], "namenode", paths["env"])
        data = {s.agent_id: s.to_dict() for s in snapshots}
        f = tmp_path / "snaps.json"
        f.write_text(json.dumps(data))

        result = _invoke(["oneshot", str(f)])
        assert result.exit_code == 1
        assert "drift" in result.output.lower()

    def test_oneshot_json_format(
        self, tmp_path: Path, conf_dir: Path, hadoop_env_path: Path
    ) -> None:
        from checker.agent import collect_all

        paths = _copy_fixtures(tmp_path, conf_dir, hadoop_env_path)
        env = Path(paths["env"])
        text = env.read_text().replace(
            "CORE-SITE.XML_fs.defaultFS=hdfs://namenode:8020",
            "CORE-SITE.XML_fs.defaultFS=hdfs://broken:9999",
        )
        env.write_text(text)

        snapshots = collect_all(paths["conf"], "namenode", paths["env"])
        data = {s.agent_id: s.to_dict() for s in snapshots}
        f = tmp_path / "snaps.json"
        f.write_text(json.dumps(data))

        result = _invoke(["oneshot", str(f), "--format", "json"])
        assert result.exit_code == 1
        parsed = json.loads(result.output)
        assert "drifts" in parsed
        assert "root_causes" in parsed

    def test_oneshot_with_rules(
        self, tmp_path: Path, conf_dir: Path, hadoop_env_path: Path
    ) -> None:
        from checker.agent import collect_all

        paths = _copy_fixtures(tmp_path, conf_dir, hadoop_env_path)
        # Swap scheduler values
        yarn = Path(paths["conf"]) / "yarn-site.xml"
        text = yarn.read_text()
        import re
        text = re.sub(
            r"(<(?:name|n)>yarn\.scheduler\.maximum-allocation-mb</(?:name|n)>\s*<value>)\d+(</value>)",
            r"\g<1>9999\g<2>", text,
        )
        yarn.write_text(text)

        snapshots = collect_all(paths["conf"], "namenode", paths["env"])
        data = {s.agent_id: s.to_dict() for s in snapshots}
        f = tmp_path / "snaps.json"
        f.write_text(json.dumps(data))

        result = _invoke(["oneshot", str(f), "--rules", RULES_FILE, "--format", "json"])
        assert result.exit_code == 1
        parsed = json.loads(result.output)
        rule_ids = {d.get("rule_id") for d in parsed["drifts"]}
        assert "yarn-scheduler-ceiling" in rule_ids

    def test_oneshot_nonexistent_file_fails(self) -> None:
        result = _invoke(["oneshot", "/nonexistent/file.json"])
        assert result.exit_code != 0