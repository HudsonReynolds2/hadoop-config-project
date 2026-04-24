"""End-to-end CLI tests via ``subprocess``.

The existing [test_cli.py](test_cli.py) uses Click's ``CliRunner``, which
invokes the command in-process. That hides two classes of failure:

    * entry-point / packaging bugs (``hadoopconf`` not on PATH, wrong
      ``[project.scripts]`` target, missing ``__main__`` wiring);
    * exit codes that differ between in-process ``ctx.exit(n)`` and the
      real process exit returned to the shell.

The README's CI-gate claim ("``validate`` exits 0 when all rules pass and
1 when any fail") is only meaningful via a real subprocess. These tests
assert that contract.

Skipped if ``hadoopconf`` is not on PATH in the test environment.
"""

from __future__ import annotations

import json
import re
import shutil
import subprocess
from pathlib import Path

import pytest

HADOOPCONF = shutil.which("hadoopconf")

pytestmark = pytest.mark.skipif(
    HADOOPCONF is None,
    reason="hadoopconf CLI not installed; `pip install -e '.[runtime,test]'`",
)


RULES_FILE = Path(__file__).parent.parent.parent / "rules" / "hadoop-3.3.x.yaml"


def _run(*args: str, cwd: Path | None = None) -> subprocess.CompletedProcess:
    return subprocess.run(
        [HADOOPCONF, *args],
        capture_output=True, text=True, cwd=cwd,
        timeout=30,
    )


def _copy_conf(dst: Path, conf_dir: Path, hadoop_env_path: Path) -> dict:
    conf_dst = dst / "conf"
    conf_dst.mkdir()
    for xml in conf_dir.glob("*-site.xml"):
        shutil.copy(xml, conf_dst / xml.name)
    env_dst = dst / "hadoop.env"
    shutil.copy(hadoop_env_path, env_dst)
    return {"conf": conf_dst, "env": env_dst}


def _modify_xml_value(xml_path: Path, key: str, new_value: str) -> None:
    text = xml_path.read_text()
    pattern = rf"(<(?:name|n)>{re.escape(key)}</(?:name|n)>\s*<value>)(.*?)(</value>)"
    new_text, count = re.subn(pattern, rf"\g<1>{new_value}\g<3>", text)
    assert count == 1
    xml_path.write_text(new_text)


# ---------------------------------------------------------------------------
# The CI-gate contract: exit 0 on clean, exit 1 on broken.
# ---------------------------------------------------------------------------


def test_validate_returns_0_on_clean_fixtures(
    conf_dir: Path, hadoop_env_path: Path
) -> None:
    result = _run(
        "validate", str(conf_dir), str(RULES_FILE),
        "--service", "namenode",
        "--env-file", str(hadoop_env_path),
    )
    assert result.returncode == 0, (
        f"expected exit 0 on clean fixtures; got {result.returncode}\n"
        f"stdout: {result.stdout}\nstderr: {result.stderr}"
    )


def test_validate_returns_1_on_broken_config(
    tmp_path: Path, conf_dir: Path, hadoop_env_path: Path
) -> None:
    paths = _copy_conf(tmp_path, conf_dir, hadoop_env_path)
    # Break yarn-scheduler-ceiling: scheduler max > NM total.
    _modify_xml_value(
        paths["conf"] / "yarn-site.xml",
        "yarn.scheduler.maximum-allocation-mb",
        "9999",
    )

    result = _run(
        "validate", str(paths["conf"]), str(RULES_FILE),
        "--service", "namenode",
        "--env-file", str(paths["env"]),
    )
    assert result.returncode == 1, (
        f"expected exit 1 on broken config; got {result.returncode}\n"
        f"stdout: {result.stdout}\nstderr: {result.stderr}"
    )
    # Failure reason should be reported somewhere in the output.
    combined = result.stdout + result.stderr
    assert "yarn-scheduler-ceiling" in combined, (
        "failed rule id should appear in output; got:\n" + combined
    )


# ---------------------------------------------------------------------------
# collect subcommand — always exits 0 per README, even with drift.
# ---------------------------------------------------------------------------


def test_collect_emits_valid_json(
    conf_dir: Path, hadoop_env_path: Path
) -> None:
    result = _run(
        "collect", str(conf_dir),
        "--service", "namenode",
        "--env-file", str(hadoop_env_path),
        "--format", "json",
    )
    assert result.returncode == 0, result.stderr
    # Output must be parseable JSON (a dict keyed by agent_id).
    parsed = json.loads(result.stdout)
    assert isinstance(parsed, dict) and parsed
    # Every snapshot should have the expected shape.
    for agent_id, snap in parsed.items():
        assert {"agent_id", "service", "source", "properties"} <= set(snap)
        assert snap["agent_id"] == agent_id


def test_collect_with_detect_drift_still_exits_0(
    tmp_path: Path, conf_dir: Path, hadoop_env_path: Path
) -> None:
    """README guarantees ``collect`` always exits 0, even with drift.
    Contrast with ``validate`` which is the CI gate."""
    paths = _copy_conf(tmp_path, conf_dir, hadoop_env_path)
    _modify_xml_value(
        paths["conf"] / "core-site.xml",
        "fs.defaultFS",
        "hdfs://drifted:8020",
    )

    result = _run(
        "collect", str(paths["conf"]),
        "--service", "namenode",
        "--env-file", str(paths["env"]),
        "--detect-drift",
    )
    assert result.returncode == 0, (
        f"`collect --detect-drift` should always exit 0 per README; "
        f"got {result.returncode}\nstdout: {result.stdout}\nstderr: {result.stderr}"
    )


# ---------------------------------------------------------------------------
# oneshot — exit code contract mirrors validate.
# ---------------------------------------------------------------------------


def test_oneshot_exit_codes(
    tmp_path: Path, conf_dir: Path, hadoop_env_path: Path
) -> None:
    # Build a clean snapshot bundle.
    clean = _run(
        "collect", str(conf_dir),
        "--service", "namenode",
        "--env-file", str(hadoop_env_path),
        "--format", "json",
    )
    assert clean.returncode == 0
    clean_json = tmp_path / "clean.json"
    # `collect --format json` already emits {agent_id: snapshot_dict},
    # which is the shape `oneshot` reads.
    clean_json.write_text(clean.stdout)
    result = _run("oneshot", str(clean_json))
    assert result.returncode == 0, (
        f"oneshot on clean bundle: {result.returncode}\n"
        f"{result.stdout}\n{result.stderr}"
    )

    # Now a broken bundle: break dual-source by diverging env.
    paths = _copy_conf(tmp_path, conf_dir, hadoop_env_path)
    env = paths["env"]
    text = env.read_text().replace(
        "CORE-SITE.XML_fs.defaultFS=hdfs://namenode:8020",
        "CORE-SITE.XML_fs.defaultFS=hdfs://drifted:8020",
    )
    env.write_text(text)

    dirty = _run(
        "collect", str(paths["conf"]),
        "--service", "namenode",
        "--env-file", str(env),
        "--format", "json",
    )
    assert dirty.returncode == 0
    dirty_json = tmp_path / "dirty.json"
    dirty_json.write_text(dirty.stdout)
    result = _run("oneshot", str(dirty_json))
    assert result.returncode == 1, (
        f"oneshot on drifted bundle should exit 1; got {result.returncode}\n"
        f"{result.stdout}\n{result.stderr}"
    )
