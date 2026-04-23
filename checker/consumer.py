"""Kafka consumer — ingests snapshots, detects drift, emits reports.

This module is the central nervous system of the checker.  It maintains an
in-memory ``SnapshotStore`` of the latest snapshot from every agent, runs
the drift detector on each incoming update, and writes structured JSON
reports to stdout (and optionally to an alerts Kafka topic).

Architecture
------------
The consumer is designed to be horizontally scalable: multiple instances
can form a Kafka consumer group, each processing a subset of partitions.
The ``SnapshotStore`` is per-instance (no shared state), so each instance
sees a consistent view of its own partition set.

The pipeline on each incoming snapshot:

1. Deserialise the ``ConfigSnapshot`` from the Kafka message value.
2. If a previous snapshot exists for this ``agent_id``, run temporal drift
   detection (same agent, config changed over time).
3. Update the store.
4. Run cross-source drift detection across all snapshots for this service
   (e.g. XML vs env file for the same service disagree).
5. Emit all ``DriftResult`` objects as JSON to stdout.
6. Optionally publish to the ``hadoop-config-alerts`` topic.

Environment variables
---------------------
CHECKER_KAFKA_BOOTSTRAP
    Kafka bootstrap server.  Default: ``kafka:9092``

CHECKER_TOPIC
    Snapshot input topic.  Default: ``hadoop-config-snapshots``

CHECKER_ALERTS_TOPIC
    Optional alerts output topic.  Default: ``hadoop-config-alerts``

CHECKER_CONSUMER_GROUP
    Kafka consumer group ID.  Default: ``hadoop-config-checker``

CHECKER_EMIT_ALERTS
    Set to ``"true"`` to publish drift results to the alerts topic.
    Default: ``"false"``

CHECKER_LOG_LEVEL
    Python log level.  Default: ``INFO``
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
from checker.models import ConfigSnapshot, DriftResult

logger = logging.getLogger("checker.consumer")


# ---------------------------------------------------------------------------
# SnapshotStore
# ---------------------------------------------------------------------------


class SnapshotStore:
    """In-memory store of the latest ``ConfigSnapshot`` per ``agent_id``.

    Thread-safe for the single-consumer-thread model used here (one writer,
    reads only happen on the same thread), but uses a lock anyway so the
    class can be safely reused in multi-threaded contexts like tests.
    """

    def __init__(self) -> None:
        self._store: dict[str, ConfigSnapshot] = {}
        self._lock = threading.Lock()

    def get(self, agent_id: str) -> ConfigSnapshot | None:
        """Return the latest snapshot for ``agent_id``, or None."""
        with self._lock:
            return self._store.get(agent_id)

    def put(self, snapshot: ConfigSnapshot) -> ConfigSnapshot | None:
        """Store a snapshot, returning the previous one (or None).

        The caller uses the returned previous snapshot to run temporal
        drift detection.
        """
        with self._lock:
            previous = self._store.get(snapshot.agent_id)
            self._store[snapshot.agent_id] = snapshot
            return previous

    def snapshots_for_service(self, service: str) -> list[ConfigSnapshot]:
        """Return all current snapshots tagged with ``service``."""
        with self._lock:
            return [s for s in self._store.values() if s.service == service]

    def all_snapshots(self) -> list[ConfigSnapshot]:
        """Return all current snapshots (copy of values)."""
        with self._lock:
            return list(self._store.values())

    def services(self) -> set[str]:
        """Return the set of distinct service names in the store."""
        with self._lock:
            return {s.service for s in self._store.values()}

    def __len__(self) -> int:
        with self._lock:
            return len(self._store)

    def clear(self) -> None:
        with self._lock:
            self._store.clear()

    def to_dict(self) -> dict[str, dict]:
        """Serialise the entire store as ``{agent_id: snapshot_dict}``."""
        with self._lock:
            return {aid: s.to_dict() for aid, s in self._store.items()}

    @classmethod
    def from_dict(cls, data: dict[str, dict]) -> "SnapshotStore":
        """Reconstruct a store from serialised data."""
        store = cls()
        for aid, snap_dict in data.items():
            store.put(ConfigSnapshot.from_dict(snap_dict))
        return store


# ---------------------------------------------------------------------------
# Drift pipeline (no Kafka dependency — pure logic)
# ---------------------------------------------------------------------------


def process_snapshot(
    snapshot: ConfigSnapshot,
    store: SnapshotStore,
) -> list[DriftResult]:
    """Run the full drift pipeline for one incoming snapshot.

    1. Temporal diff against the previous snapshot for this agent.
    2. Update the store.
    3. Cross-source diff across all snapshots for this service.

    Returns all drift results (may be empty if nothing changed).
    """
    results: list[DriftResult] = []

    # 1. Temporal drift — did this agent's config change since last time?
    previous = store.get(snapshot.agent_id)
    if previous is not None:
        try:
            temporal = detect_temporal(previous, snapshot)
            results.extend(temporal)
        except ValueError:
            # agent_id mismatch — shouldn't happen, but don't crash.
            logger.warning(
                "agent_id mismatch in temporal diff: %s vs %s",
                previous.agent_id, snapshot.agent_id,
            )

    # 2. Update store (must happen after temporal diff so we compare old vs new).
    store.put(snapshot)

    # 3. Cross-source drift — do XML/env/JVM sources for this service agree?
    service_snaps = store.snapshots_for_service(snapshot.service)
    if len(service_snaps) > 1:
        cross = detect_cross_source(service_snaps)
        results.extend(cross)

    return results


# ---------------------------------------------------------------------------
# Report formatting
# ---------------------------------------------------------------------------


def format_drift_report(drift: DriftResult) -> dict:
    """Format a single DriftResult as a structured report dict."""
    return {
        "type": "drift",
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        **drift.to_dict(),
    }


def format_summary(results: list[DriftResult], agent_id: str) -> dict:
    """Format a batch of results into a summary report."""
    return {
        "type": "drift_report",
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "agent_id": agent_id,
        "drift_count": len(results),
        "drifts": [r.to_dict() for r in results],
    }


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
            "kafka-python is required for the consumer. "
            "Install it with:  pip install kafka-python"
        ) from exc


def _env(key: str, default: str) -> str:
    return os.environ.get(key, default)


def run_consumer() -> None:
    """Entry point: connect to Kafka, consume snapshots, detect drift."""
    bootstrap = _env("CHECKER_KAFKA_BOOTSTRAP", "kafka:9092")
    topic = _env("CHECKER_TOPIC", "hadoop-config-snapshots")
    alerts_topic = _env("CHECKER_ALERTS_TOPIC", "hadoop-config-alerts")
    group_id = _env("CHECKER_CONSUMER_GROUP", "hadoop-config-checker")
    emit_alerts = _env("CHECKER_EMIT_ALERTS", "false").lower() == "true"
    log_level = _env("CHECKER_LOG_LEVEL", "INFO")

    logging.basicConfig(
        level=getattr(logging, log_level.upper(), logging.INFO),
        format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
        stream=sys.stderr,
    )

    logger.info(
        "consumer starting: kafka=%s topic=%s group=%s alerts=%s",
        bootstrap, topic, group_id, emit_alerts,
    )

    _ensure_kafka()

    consumer = _kafka_consumer_cls(
        topic,
        bootstrap_servers=bootstrap,
        group_id=group_id,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        consumer_timeout_ms=-1,  # block indefinitely
    )

    # Optional alerts producer
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

    # Graceful shutdown
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

            results = process_snapshot(snap, store)

            if results:
                report = format_summary(results, snap.agent_id)
                report_json = json.dumps(report, sort_keys=True)
                print(report_json, flush=True)

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
# One-shot mode — read snapshots from a JSON file and run drift detection.
# Useful for CI/CD pipelines and offline auditing.
# ---------------------------------------------------------------------------


def run_oneshot(snapshot_file: str) -> list[DriftResult]:
    """Load snapshots from a JSON file and run full drift detection.

    The file should contain a JSON object mapping ``agent_id`` to snapshot
    dicts (the format produced by ``SnapshotStore.to_dict()``), or a JSON
    array of snapshot dicts.

    Returns all drift results found.
    """
    import pathlib

    path = pathlib.Path(snapshot_file)
    if not path.is_file():
        raise FileNotFoundError(f"snapshot file not found: {path}")

    data = json.loads(path.read_text(encoding="utf-8"))

    # Accept both dict-of-dicts and list-of-dicts formats.
    if isinstance(data, dict):
        snapshots = [ConfigSnapshot.from_dict(v) for v in data.values()]
    elif isinstance(data, list):
        snapshots = [ConfigSnapshot.from_dict(d) for d in data]
    else:
        raise ValueError(f"expected dict or list in {path}, got {type(data).__name__}")

    # Load all into a store, processing each one through the pipeline.
    store = SnapshotStore()
    all_results: list[DriftResult] = []
    for snap in snapshots:
        results = process_snapshot(snap, store)
        all_results.extend(results)

    return all_results


if __name__ == "__main__":
    run_consumer()
