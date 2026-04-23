"""Kafka-based config agent — watches local files and publishes snapshots.

Runs as a sidecar (or embedded process) on every Hadoop node. Reads only
local config files, never calls any Hadoop HTTP endpoint. Publishes
``ConfigSnapshot`` messages to a Kafka topic so that a centralised consumer
can detect drift across the cluster.

The agent is **stateless**: it reads files, writes to Kafka, nothing else.

Environment variables
---------------------
HADOOP_CONF_DIR
    Directory containing ``*-site.xml`` files.
    Default: ``/opt/hadoop/etc/hadoop``

CHECKER_ENV_FILE
    Optional path to a ``hadoop.env``-style KEY=VALUE file. When set, the
    agent publishes an additional snapshot for the env source. Unset by
    default — not every node has one.

CHECKER_JVM_FLAGS
    Optional JVM flags string (e.g. the value of ``SERVICE_OPTS``). When
    set, the agent parses ``-Dkey=value`` tokens and publishes a snapshot
    with ``source="jvm_flags"``. Useful for Hive services where config is
    injected via system properties rather than XML files.

CHECKER_JVM_FLAGS_NAME
    Human-readable tag for the JVM flags source (e.g. ``"SERVICE_OPTS"``).
    Default: ``"jvm_flags"``

CHECKER_KAFKA_BOOTSTRAP
    Kafka bootstrap server.  Default: ``kafka:9092``

CHECKER_TOPIC
    Kafka topic name.  Default: ``hadoop-config-snapshots``

CHECKER_SERVICE_NAME
    Logical service name for snapshot tagging (e.g. ``"namenode"``).
    Default: ``"unknown"``

CHECKER_HEARTBEAT
    Heartbeat interval in seconds.  Default: ``60``

CHECKER_TOPIC_PARTITIONS
    Number of partitions when auto-creating the topic.  Default: ``12``

CHECKER_TOPIC_REPLICATION
    Replication factor when auto-creating the topic.  Default: ``1``

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
from pathlib import Path

logger = logging.getLogger("checker.agent")

# ---------------------------------------------------------------------------
# Lazy Kafka imports — fail fast with a clear message if kafka-python is
# missing, but don't force the dependency at import time so the module can
# still be unit-tested without it.
# ---------------------------------------------------------------------------

_kafka_producer_cls = None  # type: ignore[assignment]
_NewPartitions = None
_KafkaAdminClient = None
_NewTopic = None


def _ensure_kafka():
    """Import kafka-python classes on first use."""
    global _kafka_producer_cls, _NewPartitions, _KafkaAdminClient, _NewTopic
    if _kafka_producer_cls is not None:
        return
    try:
        from kafka import KafkaProducer
        from kafka.admin import KafkaAdminClient as _Admin
        from kafka.admin import NewPartitions, NewTopic

        _kafka_producer_cls = KafkaProducer
        _NewPartitions = NewPartitions
        _KafkaAdminClient = _Admin
        _NewTopic = NewTopic
    except ImportError as exc:
        raise RuntimeError(
            "kafka-python is required for the agent. "
            "Install it with:  pip install kafka-python"
        ) from exc


# ---------------------------------------------------------------------------
# Configuration — all from env vars with sensible defaults.
# ---------------------------------------------------------------------------


def _env(key: str, default: str) -> str:
    return os.environ.get(key, default)


def _env_int(key: str, default: int) -> int:
    raw = os.environ.get(key)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError:
        logger.warning("invalid integer for %s=%r, using default %d", key, raw, default)
        return default


# ---------------------------------------------------------------------------
# Topic management
# ---------------------------------------------------------------------------


def _ensure_topic(
    bootstrap: str,
    topic: str,
    partitions: int,
    replication: int,
    timeout_s: int = 30,
) -> None:
    """Create the snapshot topic if it does not already exist.

    This is idempotent — if the topic is already present (even with
    different settings) the call is a no-op. The agent doesn't try to
    alter existing topics; that's an operator decision.
    """
    _ensure_kafka()
    try:
        admin = _KafkaAdminClient(bootstrap_servers=bootstrap)
    except Exception:
        logger.warning("could not connect to Kafka admin for topic check — will retry on publish")
        return

    try:
        existing = admin.list_topics()
        if topic in existing:
            logger.info("topic %r already exists", topic)
            return
        logger.info("creating topic %r (%d partitions, RF %d)", topic, partitions, replication)
        admin.create_topics(
            [_NewTopic(name=topic, num_partitions=partitions, replication_factor=replication)],
            timeout_ms=timeout_s * 1000,
        )
        logger.info("topic %r created", topic)
    except Exception as exc:
        # TopicAlreadyExistsError, or network blip — either way we proceed.
        logger.warning("topic creation note: %s", exc)
    finally:
        admin.close()


# ---------------------------------------------------------------------------
# Snapshot collection
# ---------------------------------------------------------------------------


def _collect_xml_snapshots(
    conf_dir: str, service: str
) -> list:
    """Collect a ConfigSnapshot for every *-site.xml in ``conf_dir``."""
    from checker.collectors.xml_collector import collect_xml, XmlCollectorError

    snapshots = []
    conf_path = Path(conf_dir)
    if not conf_path.is_dir():
        logger.warning("HADOOP_CONF_DIR %s is not a directory", conf_dir)
        return snapshots

    for xml_file in sorted(conf_path.glob("*-site.xml")):
        try:
            snap = collect_xml(xml_file, service=service)
            snapshots.append(snap)
            logger.debug("collected %s (%d keys)", xml_file.name, len(snap.properties))
        except XmlCollectorError as exc:
            logger.error("failed to collect %s: %s", xml_file, exc)
    return snapshots


def _collect_env_snapshot(
    env_path: str, service: str
):
    """Collect a ConfigSnapshot from a hadoop.env-style file, or None."""
    from checker.collectors.env_collector import parse_env_file, EnvCollectorError

    try:
        snap = parse_env_file(env_path, service=service)
        logger.debug("collected env file %s (%d keys)", env_path, len(snap.properties))
        return snap
    except EnvCollectorError as exc:
        logger.error("failed to collect env file %s: %s", env_path, exc)
        return None


def _collect_jvm_snapshot(
    flags_str: str, service: str, source_name: str
):
    """Collect a ConfigSnapshot from a JVM flags string, or None."""
    from checker.collectors.env_collector import parse_jvm_flags

    snap = parse_jvm_flags(flags_str, service=service, source_path=source_name)
    if snap.properties:
        logger.debug("collected JVM flags %s (%d keys)", source_name, len(snap.properties))
        return snap
    logger.debug("JVM flags %s yielded no properties", source_name)
    return None


def collect_all(
    conf_dir: str,
    service: str,
    env_path: str | None = None,
    jvm_flags: str | None = None,
    jvm_flags_name: str = "jvm_flags",
) -> list:
    """Collect snapshots from all configured sources.

    This is the single entry point that the heartbeat loop and the
    file-change handler both call. Returns a list of ``ConfigSnapshot``
    objects ready to publish.
    """
    snapshots = _collect_xml_snapshots(conf_dir, service)
    if env_path:
        snap = _collect_env_snapshot(env_path, service)
        if snap:
            snapshots.append(snap)
    if jvm_flags:
        snap = _collect_jvm_snapshot(jvm_flags, service, jvm_flags_name)
        if snap:
            snapshots.append(snap)
    return snapshots


# ---------------------------------------------------------------------------
# Publishing
# ---------------------------------------------------------------------------


class SnapshotPublisher:
    """Thin wrapper around KafkaProducer that serialises ConfigSnapshots.

    Separating this from the collection logic makes it easy to swap out the
    transport (e.g. for testing, or to add a REST fallback in the future).
    """

    def __init__(self, bootstrap: str, topic: str):
        _ensure_kafka()
        self._topic = topic
        self._producer = _kafka_producer_cls(
            bootstrap_servers=bootstrap,
            value_serializer=lambda v: json.dumps(v, sort_keys=True).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            # Retry on transient errors — config snapshots are idempotent so
            # at-least-once is fine.
            retries=5,
            acks="all",
        )

    def publish(self, snapshots: list) -> int:
        """Publish a batch of snapshots. Returns the count actually sent."""
        sent = 0
        for snap in snapshots:
            try:
                self._producer.send(
                    self._topic,
                    key=snap.agent_id,
                    value=snap.to_dict(),
                )
                sent += 1
            except Exception as exc:
                logger.error("failed to publish %s: %s", snap.agent_id, exc)
        self._producer.flush()
        return sent

    def close(self):
        try:
            self._producer.close(timeout=5)
        except Exception:
            pass


# ---------------------------------------------------------------------------
# File watcher
# ---------------------------------------------------------------------------


def _start_watcher(
    conf_dir: str,
    service: str,
    publisher: SnapshotPublisher,
    env_path: str | None,
    jvm_flags: str | None,
    jvm_flags_name: str,
) -> object | None:
    """Start a watchdog observer on ``conf_dir``.

    Returns the observer instance (so the caller can stop it), or None if
    watchdog is not installed.
    """
    try:
        from watchdog.events import FileSystemEventHandler
        from watchdog.observers import Observer
    except ImportError:
        logger.warning("watchdog not installed — file watching disabled")
        return None

    class _Handler(FileSystemEventHandler):
        """Re-collect and publish on any *-site.xml change."""

        def __init__(self):
            super().__init__()
            # Simple debounce: ignore events within 1s of the last publish
            # for the same file. Editors often write multiple times.
            self._last: dict[str, float] = {}
            self._lock = threading.Lock()

        def on_modified(self, event):
            if event.is_directory:
                return
            path = Path(event.src_path)
            if not path.name.endswith("-site.xml"):
                return

            with self._lock:
                now = time.monotonic()
                last = self._last.get(path.name, 0.0)
                if now - last < 1.0:
                    return
                self._last[path.name] = now

            logger.info("detected change in %s — re-collecting", path.name)
            snapshots = collect_all(
                conf_dir, service, env_path, jvm_flags, jvm_flags_name
            )
            count = publisher.publish(snapshots)
            logger.info("published %d snapshot(s) after file change", count)

    observer = Observer()
    observer.schedule(_Handler(), conf_dir, recursive=False)
    observer.daemon = True
    observer.start()
    logger.info("watching %s for changes", conf_dir)
    return observer


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------


def run_agent() -> None:
    """Entry point: collect, publish, watch, heartbeat."""
    # Read config from environment
    conf_dir = _env("HADOOP_CONF_DIR", "/opt/hadoop/etc/hadoop")
    bootstrap = _env("CHECKER_KAFKA_BOOTSTRAP", "kafka:9092")
    topic = _env("CHECKER_TOPIC", "hadoop-config-snapshots")
    service = _env("CHECKER_SERVICE_NAME", "unknown")
    heartbeat = _env_int("CHECKER_HEARTBEAT", 60)
    partitions = _env_int("CHECKER_TOPIC_PARTITIONS", 12)
    replication = _env_int("CHECKER_TOPIC_REPLICATION", 1)
    log_level = _env("CHECKER_LOG_LEVEL", "INFO")

    env_path = os.environ.get("CHECKER_ENV_FILE")
    jvm_flags = os.environ.get("CHECKER_JVM_FLAGS")
    jvm_flags_name = _env("CHECKER_JVM_FLAGS_NAME", "jvm_flags")

    logging.basicConfig(
        level=getattr(logging, log_level.upper(), logging.INFO),
        format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
        stream=sys.stderr,
    )

    logger.info(
        "agent starting: service=%s conf_dir=%s kafka=%s topic=%s heartbeat=%ds",
        service, conf_dir, bootstrap, topic, heartbeat,
    )

    # Ensure topic exists
    _ensure_topic(bootstrap, topic, partitions, replication)

    # Create publisher
    publisher = SnapshotPublisher(bootstrap, topic)

    # Initial collection + publish
    snapshots = collect_all(conf_dir, service, env_path, jvm_flags, jvm_flags_name)
    count = publisher.publish(snapshots)
    logger.info("initial publish: %d snapshot(s), %d source(s)", count, len(snapshots))

    # Start file watcher
    observer = _start_watcher(
        conf_dir, service, publisher, env_path, jvm_flags, jvm_flags_name
    )

    # Graceful shutdown
    shutdown = threading.Event()

    def _on_signal(signum, frame):
        logger.info("received signal %d, shutting down", signum)
        shutdown.set()

    signal.signal(signal.SIGTERM, _on_signal)
    signal.signal(signal.SIGINT, _on_signal)

    # Heartbeat loop
    try:
        while not shutdown.is_set():
            shutdown.wait(timeout=heartbeat)
            if shutdown.is_set():
                break
            snapshots = collect_all(
                conf_dir, service, env_path, jvm_flags, jvm_flags_name
            )
            count = publisher.publish(snapshots)
            logger.debug("heartbeat: published %d snapshot(s)", count)
    finally:
        if observer is not None:
            observer.stop()
            observer.join(timeout=5)
        publisher.close()
        logger.info("agent stopped")


# ---------------------------------------------------------------------------
# Module entry point: python -m checker.agent
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    run_agent()
