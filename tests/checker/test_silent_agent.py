"""Tests for ``checker.consumer.AgentLivenessTracker`` and the
silent-agent detection path in ``process_snapshot``.

Stage 2.3.

The plan: if any known agent_id has not published in
``2 × CHECKER_HEARTBEAT`` seconds, emit a ``DriftResult`` with
``rule_id="silent-agent"``, ``severity="critical"``, and
``key="agent.heartbeat"``.

These tests inject stale monotonic timestamps directly into the tracker
rather than waiting on real time. They cover the contract:

  - never-seen agent → not flagged (we have to learn it first)
  - just-seen agent → not flagged
  - agent stale by > 2× heartbeat → flagged exactly once
  - agent comes back, then goes silent again → flagged again
"""

from __future__ import annotations

from checker.consumer import (
    AgentLivenessTracker,
    SnapshotStore,
    process_snapshot,
)
from checker.models import ConfigSnapshot, SOURCE_XML_FILE


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _snap(
    agent_id: str = "test-snap",
    service: str = "namenode",
    properties: dict | None = None,
    timestamp: str = "2026-04-28T12:00:00Z",
) -> ConfigSnapshot:
    return ConfigSnapshot(
        agent_id=agent_id,
        service=service,
        source=SOURCE_XML_FILE,
        source_path="conf/test.xml",
        host="test-host",
        timestamp=timestamp,
        properties=properties or {},
    )


# ---------------------------------------------------------------------------
# AgentLivenessTracker — direct unit tests
# ---------------------------------------------------------------------------


class TestAgentLivenessTracker:
    def test_never_seen_returns_no_silent(self) -> None:
        """Cold-start agent not yet recorded → can't be silent."""
        tracker = AgentLivenessTracker(heartbeat_seconds=60)
        assert tracker.find_silent() == []

    def test_just_recorded_not_silent(self) -> None:
        tracker = AgentLivenessTracker(heartbeat_seconds=60)
        tracker.record("agent-1", "namenode")
        assert tracker.find_silent() == []

    def test_agent_silent_after_two_heartbeats(self) -> None:
        """Inject a stale monotonic time directly; the tracker should
        flag the agent on the next find_silent() call."""
        tracker = AgentLivenessTracker(heartbeat_seconds=60)
        tracker.record("agent-1", "namenode")

        # Pretend 200 seconds have passed (> 2 × 60 = 120).
        tracker._last_seen["agent-1"] -= 200

        silent = tracker.find_silent()
        assert len(silent) == 1
        d = silent[0]
        assert d.rule_id == "silent-agent"
        assert d.severity == "critical"
        assert d.key == "agent.heartbeat"
        assert d.service == "namenode"
        assert "agent-1" in d.source_a

    def test_silent_agent_only_alerted_once(self) -> None:
        """Once flagged, subsequent calls don't re-emit until the agent
        comes back. Otherwise the alerts topic gets spammed every
        message we process while the agent is down."""
        tracker = AgentLivenessTracker(heartbeat_seconds=60)
        tracker.record("agent-1", "namenode")
        tracker._last_seen["agent-1"] -= 200

        first = tracker.find_silent()
        second = tracker.find_silent()
        third = tracker.find_silent()
        assert len(first) == 1
        assert second == []
        assert third == []

    def test_silent_then_recovered_then_silent_again(self) -> None:
        """If an agent comes back and then goes silent again, it should
        be flagged a second time."""
        tracker = AgentLivenessTracker(heartbeat_seconds=60)
        tracker.record("agent-1", "namenode")
        tracker._last_seen["agent-1"] -= 200
        first = tracker.find_silent()
        assert len(first) == 1

        # Recovery: agent publishes again. The alerted-set entry is cleared.
        tracker.record("agent-1", "namenode")
        assert tracker.find_silent() == []

        # Second silence.
        tracker._last_seen["agent-1"] -= 200
        second = tracker.find_silent()
        assert len(second) == 1

    def test_threshold_is_two_times_heartbeat(self) -> None:
        """Boundary check: just inside 2× heartbeat is NOT silent; well
        outside is. Avoids false alarms on a single skipped beat.

        Note: monotonic time advances between record() and find_silent(),
        so we test "well inside" and "well outside" rather than the
        knife-edge boundary which is impossible to land on reliably.
        """
        tracker = AgentLivenessTracker(heartbeat_seconds=30)
        tracker.record("agent-1", "namenode")

        # 50 seconds old (< 60s threshold) — not silent.
        tracker._last_seen["agent-1"] -= 50
        assert tracker.find_silent() == []

        # 70 seconds old (> 60s threshold) — silent.
        tracker._last_seen["agent-1"] -= 20
        silent = tracker.find_silent()
        assert len(silent) == 1

    def test_multiple_agents_independent(self) -> None:
        """Each agent's silence is tracked independently."""
        tracker = AgentLivenessTracker(heartbeat_seconds=60)
        tracker.record("agent-1", "namenode")
        tracker.record("agent-2", "datanode")
        tracker.record("agent-3", "resourcemanager")

        # Only agent-2 goes silent.
        tracker._last_seen["agent-2"] -= 200

        silent = tracker.find_silent()
        assert len(silent) == 1
        assert "agent-2" in silent[0].source_a

    def test_known_agents(self) -> None:
        tracker = AgentLivenessTracker(heartbeat_seconds=60)
        tracker.record("a", "namenode")
        tracker.record("b", "datanode")
        assert tracker.known_agents() == {"a", "b"}


# ---------------------------------------------------------------------------
# Integration with process_snapshot
# ---------------------------------------------------------------------------


class TestSilentAgentInPipeline:
    def test_pipeline_emits_silent_agent_drift(self) -> None:
        """Wire a tracker into process_snapshot, age out one agent, and
        confirm a silent-agent DriftResult appears in the results from
        the NEXT snapshot we process."""
        store = SnapshotStore()
        tracker = AgentLivenessTracker(heartbeat_seconds=60)

        # Two agents publish; both are now known and recent.
        process_snapshot(
            _snap(agent_id="a", service="namenode", properties={"k": "v"}),
            store, liveness=tracker,
        )
        process_snapshot(
            _snap(agent_id="b", service="datanode", properties={"k": "v"}),
            store, liveness=tracker,
        )

        # Age out agent "b". Now any subsequent snapshot from anyone
        # will trigger a silent-agent DriftResult for "b".
        tracker._last_seen["b"] -= 200

        results, _ = process_snapshot(
            _snap(
                agent_id="a", service="namenode",
                properties={"k": "v"},
                timestamp="2026-04-28T12:01:00Z",
            ),
            store, liveness=tracker,
        )

        silent = [r for r in results if r.rule_id == "silent-agent"]
        assert len(silent) == 1
        assert silent[0].severity == "critical"
        assert silent[0].key == "agent.heartbeat"
        assert "b" in silent[0].source_a

    def test_pipeline_without_liveness_tracker_works(self) -> None:
        """Backward-compat: callers that don't pass ``liveness=`` get the
        original pipeline behaviour, no silent-agent results."""
        store = SnapshotStore()
        results, _ = process_snapshot(
            _snap(properties={"k": "v"}), store,
        )
        assert all(r.rule_id != "silent-agent" for r in results)
