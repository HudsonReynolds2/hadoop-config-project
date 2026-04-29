"""CLI entry point for hadoop-config-checker."""

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
                  type=click.Path(exists=True))
    @click.option("--format", "fmt", type=click.Choice(["json", "text"]),
                  default="text")
    def oneshot(snapshot_file, rules_file, fmt):
        """Run one-shot drift detection on a snapshot JSON file."""
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
                rule = f" [{r.rule_id}]" if r.rule_id else ""
                click.echo(f"  [{r.severity.upper()}]{rule} {r.key}")
                click.echo(f"    {r.source_a}: {r.value_a}")
                click.echo(f"    {r.source_b}: {r.value_b}\n")
        sys.exit(1)

    @cli.command()
    @click.argument("conf_dir", type=click.Path(exists=True))
    @click.argument("rules_file", type=click.Path(exists=True))
    @click.option("--service", default="unknown")
    @click.option("--env-file", default=None)
    @click.option("--jvm-flags", default=None)
    @click.option("--jvm-flags-name", default="jvm_flags")
    @click.option("--spark-defaults", "spark_defaults", default=None,
                  type=click.Path(exists=True))
    @click.option("--format", "fmt", type=click.Choice(["json", "text"]),
                  default="text")
    def validate(conf_dir, rules_file, service, env_file, jvm_flags,
                 jvm_flags_name, spark_defaults, fmt):
        """Validate local config files against a YAML rule set."""
        from checker.agent import collect_all
        from checker.analysis.validator import load_rules, validate as run_validate
        from checker.consumer import SnapshotStore

        snapshots = collect_all(conf_dir, service, env_file, jvm_flags,
                                jvm_flags_name, spark_defaults)
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
    @click.option("--spark-defaults", "spark_defaults", default=None,
                  type=click.Path(exists=True))
    @click.option("--format", "fmt", type=click.Choice(["json", "text"]),
                  default="text")
    @click.option("--detect-drift/--no-detect-drift", default=False)
    def collect(conf_dir, service, env_file, jvm_flags, jvm_flags_name,
                spark_defaults, fmt, detect_drift):
        """Collect config snapshots from local files and print them."""
        from checker.agent import collect_all
        snapshots = collect_all(conf_dir, service, env_file, jvm_flags,
                                jvm_flags_name, spark_defaults)
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
                    click.echo(f"    {d.source_b}: {d.value_b}\n")
            else:
                click.echo("\nNo cross-source drift detected.")

    # -----------------------------------------------------------------
    # status — the command that was hanging.
    # New strategy: read ONLY the latest message per partition by
    # seeking each partition to (end-offset - N) where N is just enough
    # to cover one heartbeat window's worth of messages. Bound the read
    # by a wall-clock deadline that ALWAYS fires.
    # -----------------------------------------------------------------
    @cli.command()
    @click.option("--bootstrap", default=None)
    @click.option("--topic", default=None)
    @click.option("--rules", "rules_file", default=None,
                  type=click.Path(exists=True))
    @click.option("--graph-file", "graph_file", default=None,
                  type=click.Path(exists=True))
    @click.option("--timeout", "timeout", default=10, type=int)
    @click.option("--format", "fmt", type=click.Choice(["json", "text"]),
                  default="text")
    def status(bootstrap, topic, rules_file, graph_file, timeout, fmt):
        """Query live cluster state by replaying recent snapshots from Kafka."""
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
            click.echo("kafka-python is required.", err=True)
            sys.exit(2)

        # Hard wall-clock deadline. Nothing past this point waits for Kafka
        # longer than `timeout` seconds total.
        deadline = time.time() + timeout

        # Read at most this many messages per partition. With 12 partitions
        # this caps memory + time; comfortably more than the number of
        # active agents (~10).
        MAX_MSGS_PER_PARTITION = 50

        try:
            consumer = KafkaConsumer(
                bootstrap_servers=bootstrap,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                enable_auto_commit=False,
                consumer_timeout_ms=2000,
                request_timeout_ms=5000,
                api_version_auto_timeout_ms=5000,
            )
        except Exception as e:
            click.echo(f"(kafka connect failed: {e})", err=True)
            sys.exit(2)

        # --- partition discovery (bounded) ---
        partitions = None
        while time.time() < deadline:
            try:
                partitions = consumer.partitions_for_topic(topic)
                if partitions:
                    break
            except Exception:
                pass
            time.sleep(0.3)

        if not partitions:
            try: consumer.close()
            except Exception: pass
            click.echo(f"(no metadata for topic {topic!r})", err=True)
            sys.exit(2)

        tp_list = [TopicPartition(topic, p) for p in partitions]
        consumer.assign(tp_list)

        # --- seek each partition to (end - MAX_MSGS_PER_PARTITION) ---
        # This is the key change: we read only the tail of each partition,
        # not from since_ms (which on a long-running cluster covers
        # thousands of messages and was the source of the hang).
        try:
            end_offsets = consumer.end_offsets(tp_list)
            beg_offsets = consumer.beginning_offsets(tp_list)
            for tp in tp_list:
                end = end_offsets.get(tp, 0)
                beg = beg_offsets.get(tp, 0)
                target = max(beg, end - MAX_MSGS_PER_PARTITION)
                consumer.seek(tp, target)
        except Exception as e:
            click.echo(f"(seek failed: {e})", err=True)
            try: consumer.close()
            except Exception: pass
            sys.exit(2)

        # Total messages we expect to read across all partitions.
        total_expected = 0
        try:
            for tp in tp_list:
                end = end_offsets.get(tp, 0)
                beg = beg_offsets.get(tp, 0)
                target = max(beg, end - MAX_MSGS_PER_PARTITION)
                total_expected += max(0, end - target)
        except Exception:
            total_expected = MAX_MSGS_PER_PARTITION * len(tp_list)

        store = SnapshotStore()
        snap_count = 0
        try:
            for message in consumer:
                if time.time() > deadline:
                    break
                try:
                    snap = ConfigSnapshot.from_dict(message.value)
                except (TypeError, KeyError):
                    continue
                store.put(snap)
                snap_count += 1
                if snap_count >= total_expected:
                    break
        except Exception:
            pass
        finally:
            try: consumer.close(autocommit=False)
            except Exception: pass

        rules = []
        if rules_file:
            from checker.analysis.validator import load_rules
            rules = load_rules(rules_file)

        if graph_file:
            os.environ["CHECKER_GRAPH_FILE"] = graph_file
        graph = CausalityGraph.load_default()

        validation_results = run_validate(rules, store) if rules else []
        cross_source_drifts = detect_cross_source(store.all_snapshots())

        all_drifts = list(cross_source_drifts)
        for vr in validation_results:
            if not vr.passed and vr.drift is not None:
                all_drifts.append(vr.drift)
        root_causes = graph.trace(all_drifts) if all_drifts else []

        services = sorted(store.services())
        agents = sorted(s.agent_id for s in store.all_snapshots())
        passed_rules = [r for r in validation_results if r.passed and getattr(r, "status", "pass") != "skip"]
        failed_rules = [r for r in validation_results if not r.passed]
        skipped_rules = [r for r in validation_results if getattr(r, "status", "pass") == "skip"]

        is_clean = not failed_rules and not cross_source_drifts and len(store) > 0

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
            click.echo(f"Rules: {len(passed_rules)} passed, {len(failed_rules)} failed, {len(skipped_rules)} skipped")
            if failed_rules:
                click.echo()
                for r in failed_rules:
                    click.echo(f"  [{r.severity.upper()}] {r.rule_id}: {r.details}")
            if skipped_rules:
                click.echo()
                for r in skipped_rules:
                    click.echo(f"  [SKIP] {r.rule_id}: {r.details}")
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

        if len(store) == 0:
            click.echo("(no snapshots received within timeout)", err=True)
            sys.exit(2)
        sys.exit(0 if is_clean else 1)

    cli()


if __name__ == "__main__":
    main()
