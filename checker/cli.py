"""CLI entry point for hadoop-config-checker.

Usage::

    hadoopconf consume                    # Kafka consumer loop
    hadoopconf oneshot FILE               # drift detection on snapshot JSON
    hadoopconf validate CONF_DIR RULES    # validate local configs against rules
    hadoopconf collect DIR                # collect and print snapshots
    hadoopconf status                     # query live cluster state via Kafka
"""

from __future__ import annotations

import json
import sys


def _lazy_click():
    try:
        import click
        return click
    except ImportError:
        print("click is required.  pip install click", file=sys.stderr)
        sys.exit(1)


def main():
    click = _lazy_click()

    @click.group()
    @click.version_option(version="0.1.0", prog_name="hadoopconf")
    def cli():
        """hadoop-config-checker — configuration observability for Hadoop clusters."""

    @cli.command()
    def consume():
        """Start the Kafka consumer loop."""
        from checker.consumer import run_consumer
        run_consumer()

    @cli.command()
    @click.argument("snapshot_file", type=click.Path(exists=True))
    @click.option("--rules", "rules_file", default=None,
                  type=click.Path(exists=True),
                  help="YAML rule file to validate against.")
    @click.option("--format", "fmt", type=click.Choice(["json", "text"]),
                  default="text", help="Output format.")
    def oneshot(snapshot_file: str, rules_file: str | None, fmt: str):
        """Run one-shot drift detection on a snapshot JSON file.

        Exits 1 if drift found, 0 otherwise (CI/CD gate).
        """
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
    @click.option("--service", default="unknown", help="Service name tag.")
    @click.option("--env-file", default=None, help="Path to hadoop.env file.")
    @click.option("--jvm-flags", default=None, help="JVM flags string.")
    @click.option("--jvm-flags-name", default="jvm_flags", help="JVM flags source name.")
    @click.option("--spark-defaults", "spark_defaults", default=None,
                  type=click.Path(exists=True),
                  help="Path to spark-defaults.conf file.")
    @click.option("--format", "fmt", type=click.Choice(["json", "text"]),
                  default="text", help="Output format.")
    def validate(conf_dir, rules_file, service, env_file, jvm_flags,
                 jvm_flags_name, spark_defaults, fmt):
        """Validate local config files against a YAML rule set.

        Exits 1 if any rule fails, 0 if all pass.
        """
        from checker.agent import collect_all
        from checker.analysis.validator import load_rules, validate as run_validate
        from checker.consumer import SnapshotStore

        snapshots = collect_all(
            conf_dir, service, env_file, jvm_flags, jvm_flags_name,
            spark_defaults,
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
            click.echo("All rules passed." if not failed else f"{len(failed)} rule(s) failed.")

        sys.exit(1 if any(not r.passed for r in results) else 0)

    @cli.command()
    @click.argument("conf_dir", type=click.Path(exists=True))
    @click.option("--service", default="unknown", help="Service name tag.")
    @click.option("--env-file", default=None, help="Path to hadoop.env file.")
    @click.option("--jvm-flags", default=None, help="JVM flags string.")
    @click.option("--jvm-flags-name", default="jvm_flags", help="JVM flags source name.")
    @click.option("--spark-defaults", "spark_defaults", default=None,
                  type=click.Path(exists=True),
                  help="Path to spark-defaults.conf file.")
    @click.option("--format", "fmt", type=click.Choice(["json", "text"]),
                  default="text", help="Output format.")
    @click.option("--detect-drift/--no-detect-drift", default=False,
                  help="Run cross-source drift detection.")
    def collect(conf_dir, service, env_file, jvm_flags, jvm_flags_name,
                spark_defaults, fmt, detect_drift):
        """Collect config snapshots from local files and print them."""
        from checker.agent import collect_all

        snapshots = collect_all(
            conf_dir, service, env_file, jvm_flags, jvm_flags_name,
            spark_defaults,
        )

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
                    click.echo(f"    {d.source_a}: {d.value_a}")
                    click.echo(f"    {d.source_b}: {d.value_b}")
                    click.echo()
            else:
                click.echo("\nNo cross-source drift detected.")

    @cli.command()
    @click.option("--bootstrap", default=None,
                  help="Kafka bootstrap (overrides CHECKER_KAFKA_BOOTSTRAP).")
    @click.option("--topic", default=None,
                  help="Snapshot topic (overrides CHECKER_TOPIC).")
    @click.option("--rules", "rules_file", default=None,
                  type=click.Path(exists=True),
                  help="YAML rule file to validate against. Falls back to "
                       "CHECKER_RULES_FILE if unset.")
    @click.option("--graph-file", "graph_file", default=None,
                  type=click.Path(exists=True),
                  help="YAML causality-graph file. Falls back to "
                       "CHECKER_GRAPH_FILE if unset.")
    @click.option("--timeout", "timeout", default=10, type=int,
                  help="Seconds to wait for a complete snapshot poll.")
    @click.option("--format", "fmt", type=click.Choice(["json", "text"]),
                  default="text", help="Output format.")
    def status(bootstrap, topic, rules_file, graph_file, timeout, fmt):
        """Query live cluster state by replaying current snapshots from Kafka.

        Reads every snapshot currently on the topic into a fresh in-memory
        store, runs the full validator + causality pipeline against it,
        and prints the result. Exits 1 if any rule fails or any drift is
        present; exits 0 if the cluster is clean.

        Unlike ``validate``, which checks one local conf dir, ``status``
        sees what every running agent is actually publishing — so it
        catches multi-service propagation issues that a single conf
        directory cannot reveal.
        """
        import os
        import time

        bootstrap = bootstrap or os.environ.get("CHECKER_KAFKA_BOOTSTRAP", "kafka:9092")
        topic = topic or os.environ.get("CHECKER_TOPIC", "hadoop-config-snapshots")
        rules_file = rules_file or os.environ.get("CHECKER_RULES_FILE")

        from checker.consumer import SnapshotStore
        from checker.analysis.validator import validate as run_validate
        from checker.analysis.causality_graph import CausalityGraph
        from checker.analysis.drift_detector import detect_cross_source
        from checker.models import ConfigSnapshot

        try:
            from kafka import KafkaConsumer, TopicPartition
        except ImportError:
            click.echo("kafka-python is required. pip install kafka-python", err=True)
            sys.exit(2)

        # Each `status` invocation is a fresh, anonymous group.
        # Rather than replaying the entire topic from the beginning (which
        # accumulates thousands of messages over a long-running cluster),
        # we seek to messages published within the last 2x timeout window.
        # Every agent republishes its full snapshot on every heartbeat, so
        # one heartbeat window is enough to see the current state of every
        # agent. This keeps each status poll fast regardless of topic size.
        group_id = f"hadoopconf-status-{int(time.time() * 1000)}"
        lookback_ms = int(timeout * 1000) * 2
        since_ms = int(time.time() * 1000) - lookback_ms

        consumer = KafkaConsumer(
            bootstrap_servers=bootstrap,
            group_id=group_id,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            enable_auto_commit=False,
            consumer_timeout_ms=int(timeout * 1000),
        )

        # Assign partitions manually so we can seek before consuming.
        partitions = consumer.partitions_for_topic(topic) or {0}
        tp_list = [TopicPartition(topic, p) for p in partitions]
        consumer.assign(tp_list)

        # Seek each partition to the first offset at or after since_ms.
        # If no messages exist that recent, seek to end (empty result).
        offsets = consumer.offsets_for_times({tp: since_ms for tp in tp_list})
        for tp, offset_and_ts in offsets.items():
            if offset_and_ts is not None:
                consumer.seek(tp, offset_and_ts.offset)
            else:
                consumer.seek_to_end(tp)

        store = SnapshotStore()
        snap_count = 0
        try:
            for message in consumer:
                try:
                    snap = ConfigSnapshot.from_dict(message.value)
                except (TypeError, KeyError):
                    continue
                store.put(snap)
                snap_count += 1
        finally:
            consumer.close()

        rules = []
        if rules_file:
            from checker.analysis.validator import load_rules
            rules = load_rules(rules_file)

        # Causality graph: env var or --graph-file overrides the default.
        if graph_file:
            os.environ["CHECKER_GRAPH_FILE"] = graph_file
        graph = CausalityGraph.load_default()

        validation_results = run_validate(rules, store) if rules else []
        cross_source_drifts = detect_cross_source(store.all_snapshots())

        # Aggregate everything that points at a real drift, then trace.
        all_drifts = list(cross_source_drifts)
        for vr in validation_results:
            if not vr.passed and vr.drift is not None:
                all_drifts.append(vr.drift)
        root_causes = graph.trace(all_drifts) if all_drifts else []

        # Summary numbers.
        services = sorted(store.services())
        agents = sorted(s.agent_id for s in store.all_snapshots())
        passed_rules = [r for r in validation_results if r.passed]
        failed_rules = [r for r in validation_results if not r.passed]
        skipped_rules = [
            r for r in validation_results if getattr(r, "status", "pass") == "skip"
        ]

        is_clean = not failed_rules and not cross_source_drifts and snap_count > 0

        if fmt == "json":
            out = {
                "clean": is_clean,
                "snapshots_received": snap_count,
                "agents": agents,
                "services": services,
                "rules": {
                    "passed": [r.rule_id for r in passed_rules],
                    "failed": [r.to_dict() for r in failed_rules],
                    "skipped": [r.rule_id for r in skipped_rules],
                },
                "drifts": [d.to_dict() for d in all_drifts],
                "root_causes": [rc.to_dict() for rc in root_causes],
            }
            click.echo(json.dumps(out, indent=2, sort_keys=True))
        else:
            click.echo(f"Snapshots received: {snap_count}")
            click.echo(f"Agents: {len(agents)} ({', '.join(agents) or '-'})")
            click.echo(f"Services: {len(services)} ({', '.join(services) or '-'})")
            click.echo()
            click.echo(
                f"Rules: {len(passed_rules)} passed, {len(failed_rules)} failed, "
                f"{len(skipped_rules)} skipped"
            )
            if failed_rules:
                click.echo()
                for r in failed_rules:
                    click.echo(f"  [{r.severity.upper()}] {r.rule_id}: {r.details}")
            if cross_source_drifts:
                click.echo()
                click.echo(f"Cross-source drift: {len(cross_source_drifts)}")
                for d in cross_source_drifts:
                    click.echo(f"  [{d.severity.upper()}] {d.key}: "
                               f"{d.source_a}={d.value_a!r} vs "
                               f"{d.source_b}={d.value_b!r}")
            if root_causes:
                click.echo()
                click.echo(f"Root causes: {len(root_causes)}")
                for rc in root_causes:
                    click.echo(f"  [{rc.severity.upper()}] {rc.key} ({rc.service})")
                    for eff in rc.downstream_effects:
                        click.echo(f"    → {eff}")
            click.echo()
            click.echo("CLEAN." if is_clean else "DRIFT DETECTED.")

        if snap_count == 0:
            # No snapshots received at all — neither clean nor drifted,
            # but a developer needs to know. Distinct exit code.
            click.echo("(no snapshots received within timeout)", err=True)
            sys.exit(2)
        sys.exit(0 if is_clean else 1)

    cli()


if __name__ == "__main__":
    main()
    