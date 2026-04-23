"""CLI entry point for hadoop-config-checker.

Provides the ``hadoopconf`` command with subcommands for consuming the
snapshot stream, running one-shot drift detection, validating rules, and
collecting snapshots from local files.

Usage::

    hadoopconf consume                    # start the Kafka consumer loop
    hadoopconf oneshot FILE               # drift detection on a snapshot JSON
    hadoopconf validate CONF_DIR RULES    # validate local configs against rules
    hadoopconf collect DIR                # collect and print snapshots

All Kafka-related configuration is via environment variables (see
``checker.consumer`` and ``checker.agent`` docstrings).
"""

from __future__ import annotations

import json
import sys


def _lazy_click():
    """Import click on demand so the module can be loaded without it."""
    try:
        import click

        return click
    except ImportError:
        print(
            "click is required for the CLI.  Install it with:  pip install click",
            file=sys.stderr,
        )
        sys.exit(1)


def main():
    click = _lazy_click()

    @click.group()
    @click.version_option(version="0.1.0", prog_name="hadoopconf")
    def cli():
        """hadoop-config-checker — configuration observability for Hadoop clusters."""

    @cli.command()
    def consume():
        """Start the Kafka consumer loop.

        Connects to the snapshot topic, detects drift in real time, and
        prints structured JSON reports to stdout.  Configure via env vars:

        \b
        CHECKER_KAFKA_BOOTSTRAP   (default: kafka:9092)
        CHECKER_TOPIC             (default: hadoop-config-snapshots)
        CHECKER_CONSUMER_GROUP    (default: hadoop-config-checker)
        CHECKER_EMIT_ALERTS       (default: false)
        CHECKER_RULES_FILE        (optional path to YAML rule file)
        CHECKER_LOG_LEVEL         (default: INFO)
        """
        from checker.consumer import run_consumer

        run_consumer()

    @cli.command()
    @click.argument("snapshot_file", type=click.Path(exists=True))
    @click.option("--rules", "rules_file", default=None,
                  type=click.Path(exists=True),
                  help="YAML rule file to validate against.")
    @click.option(
        "--format",
        "fmt",
        type=click.Choice(["json", "text"]),
        default="text",
        help="Output format for drift results.",
    )
    def oneshot(snapshot_file: str, rules_file: str | None, fmt: str):
        """Run one-shot drift detection on a snapshot JSON file.

        Exits with code 1 if any drift is found, 0 otherwise (useful as
        a CI/CD gate).
        """
        from checker.consumer import run_oneshot

        rules = None
        if rules_file:
            from checker.analysis.validator import load_rules
            rules = load_rules(rules_file)

        results = run_oneshot(snapshot_file, rules=rules)
        if not results:
            if fmt == "text":
                click.echo("No drift detected.")
            else:
                click.echo("[]")
            sys.exit(0)

        if fmt == "json":
            click.echo(json.dumps([r.to_dict() for r in results], indent=2))
        else:
            click.echo(f"Found {len(results)} drift(s):\n")
            for r in results:
                sev = r.severity.upper()
                rule = f" [{r.rule_id}]" if r.rule_id else ""
                click.echo(f"  [{sev}]{rule} {r.key}")
                click.echo(f"    {r.source_a}: {r.value_a}")
                click.echo(f"    {r.source_b}: {r.value_b}")
                click.echo()

        sys.exit(1)

    @cli.command()
    @click.argument("conf_dir", type=click.Path(exists=True))
    @click.argument("rules_file", type=click.Path(exists=True))
    @click.option("--service", default="unknown", help="Service name tag.")
    @click.option("--env-file", default=None, help="Path to hadoop.env file.")
    @click.option("--jvm-flags", default=None, help="JVM flags string.")
    @click.option("--jvm-flags-name", default="jvm_flags", help="JVM flags source name.")
    @click.option(
        "--format",
        "fmt",
        type=click.Choice(["json", "text"]),
        default="text",
        help="Output format.",
    )
    def validate(
        conf_dir: str,
        rules_file: str,
        service: str,
        env_file: str | None,
        jvm_flags: str | None,
        jvm_flags_name: str,
        fmt: str,
    ):
        """Validate local config files against a YAML rule set.

        Collects snapshots from CONF_DIR, loads rules from RULES_FILE,
        and reports all rule violations.  Exits with code 1 if any rule
        fails, 0 if all pass.

        \b
        Example:
          hadoopconf validate conf/ rules/hadoop-3.3.x.yaml --service namenode
          hadoopconf validate conf/ rules/hadoop-3.3.x.yaml --env-file hadoop.env
        """
        from checker.agent import collect_all
        from checker.analysis.validator import load_rules, validate as run_validate
        from checker.consumer import SnapshotStore

        snapshots = collect_all(
            conf_dir, service, env_file, jvm_flags, jvm_flags_name
        )
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

            if not failed:
                click.echo("All rules passed.")
            else:
                click.echo(f"{len(failed)} rule(s) failed.")

        sys.exit(1 if any(not r.passed for r in results) else 0)

    @cli.command()
    @click.argument("conf_dir", type=click.Path(exists=True))
    @click.option("--service", default="unknown", help="Service name tag.")
    @click.option("--env-file", default=None, help="Path to hadoop.env file.")
    @click.option("--jvm-flags", default=None, help="JVM flags string.")
    @click.option("--jvm-flags-name", default="jvm_flags", help="JVM flags source name.")
    @click.option(
        "--format",
        "fmt",
        type=click.Choice(["json", "text"]),
        default="text",
        help="Output format.",
    )
    @click.option("--detect-drift/--no-detect-drift", default=False,
                  help="Also run cross-source drift detection on collected snapshots.")
    def collect(
        conf_dir: str,
        service: str,
        env_file: str | None,
        jvm_flags: str | None,
        jvm_flags_name: str,
        fmt: str,
        detect_drift: bool,
    ):
        """Collect config snapshots from local files and print them.

        Useful for debugging what the agent would publish, or for generating
        a snapshot file to feed into ``hadoopconf oneshot``.
        """
        from checker.agent import collect_all

        snapshots = collect_all(
            conf_dir, service, env_file, jvm_flags, jvm_flags_name
        )

        if fmt == "json":
            data = {s.agent_id: s.to_dict() for s in snapshots}
            click.echo(json.dumps(data, indent=2))
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
                    click.echo(f"    {d.source_a}: {d.value_a}")
                    click.echo(f"    {d.source_b}: {d.value_b}")
                    click.echo()
            else:
                click.echo("\nNo cross-source drift detected.")

    cli()


if __name__ == "__main__":
    main()
