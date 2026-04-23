"""CLI entry point for hadoop-config-checker.

Provides the ``hadoopconf`` command with subcommands for consuming the
snapshot stream and running one-shot drift detection.

Usage::

    hadoopconf consume          # start the Kafka consumer loop
    hadoopconf oneshot FILE     # run drift detection on a snapshot JSON file
    hadoopconf collect DIR      # collect and print snapshots from local files

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
        CHECKER_LOG_LEVEL         (default: INFO)
        """
        from checker.consumer import run_consumer

        run_consumer()

    @cli.command()
    @click.argument("snapshot_file", type=click.Path(exists=True))
    @click.option(
        "--format",
        "fmt",
        type=click.Choice(["json", "text"]),
        default="text",
        help="Output format for drift results.",
    )
    def oneshot(snapshot_file: str, fmt: str):
        """Run one-shot drift detection on a snapshot JSON file.

        The file should be a JSON object mapping agent_id to snapshot dicts,
        or a JSON array of snapshot dicts.  Exits with code 1 if any drift
        is found, 0 otherwise (useful as a CI/CD gate).
        """
        from checker.consumer import run_oneshot

        results = run_oneshot(snapshot_file)
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
