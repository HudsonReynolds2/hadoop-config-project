"""Kafka consumer — ingests snapshots, detects drift, traces root causes.

Pipeline on each incoming snapshot:

1. Temporal drift detection (same agent, config changed over time).
2. Update the SnapshotStore.
3. Cross-source drift (XML vs env vs JVM disagree for same service).
4. Validator rules (constraint, propagation, dual-source checks).
5. Silent-agent detection (any known agent gone quiet?).
6. Causality graph tracing (map drifts to root causes with downstream effects).
7. Emit structured JSON report to stdout.
8. Optionally publish to the alerts Kafka topic.

Environment variables
---------------------
CHECKER_KAFKA_BOOTSTRAP    Default: kafka:9092
CHECKER_TOPIC              Default: hadoop-config-snapshots
CHECKER_ALERTS_TOPIC       Default: hadoop-config-alerts
CHECKER_CONSUMER_GROUP     Default: hadoop-config-checker
CHECKER_EMIT_ALERTS        Default: false
CHECKER_RULES_FILE         Optional YAML rule file path
CHECKER_GRAPH_FILE         Optional YAML causality-graph file path (Stage 2.4)
CHECKER_HEARTBEAT          Default: 60 (seconds; used by silent-agent check)
CHECKER_LOG_LEVEL          Default: INFO
"""

from __future__ import annotations

import json
import logging
import os
import signal
import sys
import threading
import time
from datetime import datetime, timezone

from checker.analysis.drift_detector import detect_cross_source, detect_temporal
from checker.models import ConfigSnapshot, DriftResult, RootCause

logger = logging.getLogger("checker.consumer")


# ---------------------------------------------------------------------------
# SnapshotStore
# ---------------------------------------------------------------------------


class SnapshotStore:
    """In-memory store of the latest ``ConfigSnapshot`` per ``agent_id``."""

    def __init__(self) -> None:
        self._store: dict[str, ConfigSnapshot] = {}
        self._lock = threading.Lock()

    def get(self, agent_id: str) -> ConfigSnapshot | None:
        with self._lock:
            return self._store.get(agent_id)

    def put(self, snapshot: ConfigSnapshot) -> ConfigSnapshot | None:
        with self._lock:
            previous = self._store.get(snapshot.agent_id)
            self._store[snapshot.agent_id] = snapshot
            return previous

    def snapshots_for_service(self, service: str) -> list[ConfigSnapshot]:
        with self._lock:
            return [s for s in self._store.values() if s.service == service]

    def all_snapshots(self) -> list[ConfigSnapshot]:
        with self._lock:
            return list(self._store.values())

    def services(self) -> set[str]:
        with self._lock:
            return {s.service for s in self._store.values()}

    def __len__(self) -> int:
        with self._lock:
            return len(self._store)

    def clear(self) -> None:
        with self._lock:
            self._store.clear()

    def to_dict(self) -> dict[str, dict]:
        with self._lock:
            return {aid: s.to_dict() for aid, s in self._store.items()}

    @classmethod
    def from_dict(cls, data: dict[str, dict]) -> "SnapshotStore":
        store = cls()
        for aid, snap_dict in data.items():
            store.put(ConfigSnapshot.from_dict(snap_dict))
        return store


# ---------------------------------------------------------------------------
# AgentLivenessTracker — tracks last-seen monotonic time per agent_id.
# Stage 2.3.
#
# Strategy (option a — learn dynamically): every agent that has ever
# published in this consumer's lifetime is "known". An agent is "silent"
# when it hasn't published for 2 × CHECKER_HEARTBEAT seconds.
#
# Cold-start caveat: a freshly-started consumer doesn't know an agent
# exists until that agent publishes its first snapshot. An agent that is
# down at consumer-start time will not be flagged as silent — only one
# that goes silent AFTER being seen at least once. Documented in
# OPERATIONS.md.
# ---------------------------------------------------------------------------


class AgentLivenessTracker:
    """Track last-seen time per agent_id and flag silent agents.

    Times are stored using ``time.monotonic()`` so they're insensitive to
    wall-clock changes (NTP adjustments, host suspend/resume).
    """

    SILENT_RULE_ID = "silent-agent"
    SILENT_KEY = "agent.heartbeat"
    SILENT_SEVERITY = "critical"

    def __init__(self, heartbeat_seconds: int) -> None:
        self._heartbeat = heartbeat_seconds
        # Threshold = 2 × heartbeat per the plan. One missed heartbeat is
        # noise (clock skew, GC pause); two is a real outage.
        self._silent_threshold = 2 * heartbeat_seconds
        self._last_seen: dict[str, float] = {}
        # Track which agents we've already alerted about, to avoid
        # spamming the alerts topic every time a new snapshot arrives.
        # Cleared when the agent is seen again.
        self._alerted: set[str] = set()
        # Service tag last associated with each agent_id, for nicer
        # DriftResult.service field on silent-agent reports.
        self._last_service: dict[str, str] = {}
        self._lock = threading.Lock()

    def record(self, agent_id: str, service: str) -> None:
        """Record a heartbeat for ``agent_id``."""
        now = time.monotonic()
        with self._lock:
            self._last_seen[agent_id] = now
            self._last_service[agent_id] = service
            # Re-arm: if the agent comes back, we'll alert again on next
            # silence. Idempotent.
            self._alerted.discard(agent_id)

    def find_silent(self, now: float | None = None) -> list[DriftResult]:
        """Return DriftResult entries for any newly-silent agents.

        Each agent is reported at most once per silent period — the
        agent's name is added to ``_alerted`` so subsequent calls don't
        re-emit. ``record()`` clears the set, so an agent that comes back
        and goes silent again is reported again.
        """
        if now is None:
            now = time.monotonic()
        out: list[DriftResult] = []
        with self._lock:
            for agent_id, last_seen in self._last_seen.items():
                age = now - last_seen
                if age <= self._silent_threshold:
                    continue
                if agent_id in self._alerted:
                    continue
                self._alerted.add(agent_id)
                service = self._last_service.get(agent_id, "unknown")
                out.append(DriftResult(
                    key=self.SILENT_KEY,
                    service=service,
                    source_a=f"agent:{agent_id}",
                    value_a=f"last_seen={last_seen:.1f} (monotonic)",
                    source_b="expected:heartbeat",
                    value_b=f"≤{self._silent_threshold:.0f}s ago",
                    severity=self.SILENT_SEVERITY,
                    rule_id=self.SILENT_RULE_ID,
                ))
        return out

    def known_agents(self) -> set[str]:
        with self._lock:
            return set(self._last_seen.keys())


# ---------------------------------------------------------------------------
# Drift pipeline
# ---------------------------------------------------------------------------


def process_snapshot(
    snapshot: ConfigSnapshot,
    store: SnapshotStore,
    rules: list[dict] | None = None,
    graph=None,
    liveness: AgentLivenessTracker | None = None,
) -> tuple[list[DriftResult], list[RootCause]]:
    """Run the full pipeline for one incoming snapshot.

    Returns ``(drift_results, root_causes)``.
    """
    results: list[DriftResult] = []

    # 1. Temporal drift
    previous = store.get(snapshot.agent_id)
    if previous is not None:
        try:
            temporal = detect_temporal(previous, snapshot)
            results.extend(temporal)
        except ValueError:
            logger.warning(
                "agent_id mismatch in temporal diff: %s vs %s",
                previous.agent_id, snapshot.agent_id,
            )

    # 2. Update store + record heartbeat for liveness.
    store.put(snapshot)
    if liveness is not None:
        liveness.record(snapshot.agent_id, snapshot.service)

    # 3. Cross-source drift
    service_snaps = store.snapshots_for_service(snapshot.service)
    if len(service_snaps) > 1:
        cross = detect_cross_source(service_snaps)
        results.extend(cross)

    # 4. Validator rules
    if rules:
        from checker.analysis.validator import validate

        vresults = validate(rules, store)
        for vr in vresults:
            if not vr.passed and vr.drift is not None:
                results.append(vr.drift)

    # 5. Silent-agent detection — runs on every snapshot, cheap (just a
    # dict scan). Reports each newly-silent agent at most once.
    if liveness is not None:
        silent = liveness.find_silent()
        if silent:
            results.extend(silent)
            for d in silent:
                logger.warning(
                    "silent-agent detected: %s (rule_id=%s)",
                    d.source_a, d.rule_id,
                )

    # 6. Causality graph tracing
    root_causes: list[RootCause] = []
    if graph is not None and results:
        root_causes = graph.trace(results)

    return results, root_causes


# ---------------------------------------------------------------------------
# Report formatting
# ---------------------------------------------------------------------------


def format_drift_report(drift: DriftResult) -> dict:
    return {
        "type": "drift",
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        **drift.to_dict(),
    }


def format_summary(
    results: list[DriftResult],
    agent_id: str,
    root_causes: list[RootCause] | None = None,
) -> dict:
    report = {
        "type": "drift_report",
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "agent_id": agent_id,
        "drift_count": len(results),
        "drifts": [r.to_dict() for r in results],
    }
    if root_causes:
        report["root_causes"] = [rc.to_dict() for rc in root_causes]
    return report


# ---------------------------------------------------------------------------
# Kafka consumer loop
# ---------------------------------------------------------------------------

_kafka_consumer_cls = None
_kafka_producer_cls = None


def _ensure_kafka():
    global _kafka_consumer_cls, _kafka_producer_cls
    if _kafka_consumer_cls is not None:
        return
    try:
        from kafka import KafkaConsumer, KafkaProducer
        _kafka_consumer_cls = KafkaConsumer
        _kafka_producer_cls = KafkaProducer
    except ImportError as exc:
        raise RuntimeError(
            "kafka-python is required.  pip install kafka-python"
        ) from exc


def _env(key: str, default: str) -> str:
    return os.environ.get(key, default)


def run_consumer() -> None:
    """Entry point: Kafka consumer loop with full pipeline."""
    bootstrap = _env("CHECKER_KAFKA_BOOTSTRAP", "kafka:9092")
    topic = _env("CHECKER_TOPIC", "hadoop-config-snapshots")
    alerts_topic = _env("CHECKER_ALERTS_TOPIC", "hadoop-config-alerts")
    group_id = _env("CHECKER_CONSUMER_GROUP", "hadoop-config-checker")
    emit_alerts = _env("CHECKER_EMIT_ALERTS", "false").lower() == "true"
    log_level = _env("CHECKER_LOG_LEVEL", "INFO")
    rules_file = os.environ.get("CHECKER_RULES_FILE")
    heartbeat = int(_env("CHECKER_HEARTBEAT", "60"))

    logging.basicConfig(
        level=getattr(logging, log_level.upper(), logging.INFO),
        format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
        stream=sys.stderr,
    )

    logger.info(
        "consumer starting: kafka=%s topic=%s group=%s alerts=%s heartbeat=%ds",
        bootstrap, topic, group_id, emit_alerts, heartbeat,
    )

    # Load rules
    rules = None
    if rules_file:
        from checker.analysis.validator import load_rules
        try:
            rules = load_rules(rules_file)
            logger.info("loaded %d rules from %s", len(rules), rules_file)
        except Exception as exc:
            logger.error("failed to load rules from %s: %s", rules_file, exc)

    # Initialize causality graph (Stage 2.4: honour CHECKER_GRAPH_FILE).
    from checker.analysis.causality_graph import CausalityGraph
    graph = CausalityGraph.load_default()
    logger.info("causality graph initialized with %d edges", len(graph.all_edges()))

    # Liveness tracker (Stage 2.3).
    liveness = AgentLivenessTracker(heartbeat_seconds=heartbeat)

    _ensure_kafka()

    consumer = _kafka_consumer_cls(
        topic,
        bootstrap_servers=bootstrap,
        group_id=group_id,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        consumer_timeout_ms=-1,
    )

    alerts_producer = None
    if emit_alerts:
        try:
            alerts_producer = _kafka_producer_cls(
                bootstrap_servers=bootstrap,
                value_serializer=lambda v: json.dumps(v, sort_keys=True).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
            )
            logger.info("alerts producer connected, publishing to %s", alerts_topic)
        except Exception as exc:
            logger.warning("could not create alerts producer: %s", exc)

    store = SnapshotStore()

    shutdown = threading.Event()

    def _on_signal(signum, frame):
        logger.info("received signal %d, shutting down", signum)
        shutdown.set()

    signal.signal(signal.SIGTERM, _on_signal)
    signal.signal(signal.SIGINT, _on_signal)

    logger.info("consuming from %s ...", topic)
    try:
        for message in consumer:
            if shutdown.is_set():
                break

            try:
                snap = ConfigSnapshot.from_dict(message.value)
            except (TypeError, KeyError) as exc:
                logger.warning("malformed snapshot message: %s", exc)
                continue

            logger.debug(
                "received snapshot: agent=%s service=%s source=%s keys=%d",
                snap.agent_id, snap.service, snap.source, len(snap.properties),
            )

            results, root_causes = process_snapshot(
                snap, store, rules=rules, graph=graph, liveness=liveness,
            )

            if results:
                report = format_summary(results, snap.agent_id, root_causes)
                print(json.dumps(report, sort_keys=True), flush=True)

                if alerts_producer is not None:
                    for drift in results:
                        try:
                            alerts_producer.send(
                                alerts_topic,
                                key=drift.key,
                                value=format_drift_report(drift),
                            )
                        except Exception as exc:
                            logger.warning("failed to publish alert: %s", exc)
                    alerts_producer.flush()
            else:
                logger.debug("no drift for agent=%s", snap.agent_id)

    except KeyboardInterrupt:
        logger.info("interrupted")
    finally:
        consumer.close()
        if alerts_producer is not None:
            alerts_producer.close(timeout=5)
        logger.info(
            "consumer stopped. store contains %d agent(s) across %d service(s)",
            len(store), len(store.services()),
        )


# ---------------------------------------------------------------------------
# One-shot mode
# ---------------------------------------------------------------------------


def run_oneshot(
    snapshot_file: str,
    rules: list[dict] | None = None,
    graph=None,
) -> tuple[list[DriftResult], list[RootCause]]:
    """Load snapshots from a JSON file and run the full pipeline.

    Returns ``(all_drifts, all_root_causes)``.
    """
    import pathlib

    path = pathlib.Path(snapshot_file)
    if not path.is_file():
        raise FileNotFoundError(f"snapshot file not found: {path}")

    data = json.loads(path.read_text(encoding="utf-8"))

    if isinstance(data, dict):
        snapshots = [ConfigSnapshot.from_dict(v) for v in data.values()]
    elif isinstance(data, list):
        snapshots = [ConfigSnapshot.from_dict(d) for d in data]
    else:
        raise ValueError(f"expected dict or list in {path}, got {type(data).__name__}")

    store = SnapshotStore()
    all_results: list[DriftResult] = []
    all_root_causes: list[RootCause] = []
    for snap in snapshots:
        results, rcs = process_snapshot(snap, store, rules=rules, graph=graph)
        all_results.extend(results)
        all_root_causes.extend(rcs)

    return all_results, all_root_causes


if __name__ == "__main__":
    run_consumer()
