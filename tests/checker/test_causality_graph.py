"""Tests for ``checker.analysis.causality_graph``.

Covers graph construction, BFS traversal, trace(), and the acceptance
test from plan.md: a mismatch in fs.defaultFS produces a root cause
report listing Hive and Spark as downstream effects.
"""

from __future__ import annotations

import pytest

from checker.analysis.causality_graph import CausalityGraph, Edge, _DEFAULT_EDGES
from checker.models import DriftResult, EdgeType, RootCause


def _drift(
    key: str = "fs.defaultFS",
    service: str = "namenode",
    value_a: str = "hdfs://nn:8020",
    value_b: str = "hdfs://wrong:8020",
    severity: str = "critical",
    rule_id: str | None = None,
) -> DriftResult:
    return DriftResult(
        key=key,
        service=service,
        source_a="xml_file:core-site.xml",
        value_a=value_a,
        source_b="env_file:hadoop.env",
        value_b=value_b,
        severity=severity,
        rule_id=rule_id,
    )


# ---------------------------------------------------------------------------
# Graph construction
# ---------------------------------------------------------------------------


class TestGraphConstruction:
    def test_default_graph_has_edges(self) -> None:
        g = CausalityGraph()
        assert len(g.all_edges()) == len(_DEFAULT_EDGES)

    def test_custom_edges(self) -> None:
        e = Edge("svc-a", "k1", EdgeType.PROPAGATION, "svc-b", "k2", "test")
        g = CausalityGraph(edges=[e])
        assert len(g.all_edges()) == 1

    def test_add_edge(self) -> None:
        g = CausalityGraph(edges=[])
        assert len(g.all_edges()) == 0
        g.add_edge(Edge("a", "k", EdgeType.INFLUENCE, "b", "k", ""))
        assert len(g.all_edges()) == 1

    def test_edges_from(self) -> None:
        g = CausalityGraph()
        edges = g.edges_from("namenode", "fs.defaultFS")
        # fs.defaultFS has edges to hive-server2, spark-client, hive-metastore
        assert len(edges) >= 2
        targets = {(e.target_service, e.target_key) for e in edges}
        assert ("hive-server2", "fs.defaultFS") in targets
        assert ("spark-client", "spark.hadoop.fs.defaultFS") in targets

    def test_edges_from_nonexistent_returns_empty(self) -> None:
        g = CausalityGraph()
        assert g.edges_from("nonexistent", "nonexistent") == []

    def test_nodes_includes_sources_and_targets(self) -> None:
        g = CausalityGraph()
        nodes = g.nodes()
        assert ("namenode", "fs.defaultFS") in nodes
        assert ("hive-server2", "fs.defaultFS") in nodes
        assert ("spark-client", "spark.hadoop.fs.defaultFS") in nodes


# ---------------------------------------------------------------------------
# BFS downstream walk
# ---------------------------------------------------------------------------


class TestDownstream:
    def test_fs_defaultfs_downstream(self) -> None:
        g = CausalityGraph()
        downstream = g.downstream("namenode", "fs.defaultFS")
        target_keys = {node[1] for node, _ in downstream}
        assert "fs.defaultFS" in target_keys  # hive-server2, hive-metastore
        assert "spark.hadoop.fs.defaultFS" in target_keys

    def test_downstream_from_nonexistent_returns_empty(self) -> None:
        g = CausalityGraph()
        assert g.downstream("nope", "nope") == []

    def test_downstream_does_not_include_source(self) -> None:
        g = CausalityGraph()
        downstream = g.downstream("namenode", "fs.defaultFS")
        source_in_results = any(
            node == ("namenode", "fs.defaultFS") for node, _ in downstream
        )
        assert not source_in_results

    def test_transitive_downstream(self) -> None:
        """A → B → C: downstream of A includes both B and C."""
        g = CausalityGraph(edges=[
            Edge("a", "k1", EdgeType.PROPAGATION, "b", "k2", ""),
            Edge("b", "k2", EdgeType.PROPAGATION, "c", "k3", ""),
        ])
        downstream = g.downstream("a", "k1")
        nodes = {n for n, _ in downstream}
        assert ("b", "k2") in nodes
        assert ("c", "k3") in nodes

    def test_cycle_does_not_loop(self) -> None:
        """A → B → A: BFS must terminate."""
        g = CausalityGraph(edges=[
            Edge("a", "k1", EdgeType.PROPAGATION, "b", "k2", ""),
            Edge("b", "k2", EdgeType.PROPAGATION, "a", "k1", ""),
        ])
        downstream = g.downstream("a", "k1")
        assert len(downstream) == 1  # only B, not back to A


# ---------------------------------------------------------------------------
# trace() — drifts → root causes
# ---------------------------------------------------------------------------


class TestTrace:
    def test_fs_defaultfs_drift_lists_hive_and_spark(self) -> None:
        """The plan's acceptance test: fs.defaultFS mismatch produces a
        root cause listing Hive and Spark as downstream effects."""
        g = CausalityGraph()
        drift = _drift(key="fs.defaultFS", service="namenode")
        root_causes = g.trace([drift])

        assert len(root_causes) == 1
        rc = root_causes[0]
        assert rc.key == "fs.defaultFS"
        assert rc.service == "namenode"
        assert rc.drift == drift

        effects_text = " ".join(rc.downstream_effects)
        assert "hive-server2" in effects_text
        assert "spark-client" in effects_text

    def test_trace_no_downstream_still_returns_root_cause(self) -> None:
        """A drift on a key not in the graph still produces a RootCause,
        just with no downstream effects."""
        g = CausalityGraph()
        drift = _drift(key="some.unknown.key", service="namenode")
        root_causes = g.trace([drift])
        assert len(root_causes) == 1
        assert root_causes[0].downstream_effects == []

    def test_trace_multiple_drifts_sorted_by_impact(self) -> None:
        """Drifts are returned sorted by downstream effect count."""
        g = CausalityGraph()
        # fs.defaultFS has many downstream; dfs.replication has fewer.
        drift_big = _drift(key="fs.defaultFS", service="namenode")
        drift_small = _drift(key="dfs.replication", service="namenode",
                             value_a="1", value_b="3")
        root_causes = g.trace([drift_small, drift_big])
        assert len(root_causes) == 2
        assert root_causes[0].key == "fs.defaultFS"  # more effects → first

    def test_trace_escalates_severity_on_constraint(self) -> None:
        """If any downstream edge is a CONSTRAINT, severity is critical."""
        g = CausalityGraph()
        # yarn NM memory has a constraint edge to scheduler max
        drift = _drift(
            key="yarn.nodemanager.resource.memory-mb",
            service="nodemanager",
            value_a="4096", value_b="2048",
            severity="warning",
        )
        root_causes = g.trace([drift])
        assert len(root_causes) == 1
        assert root_causes[0].severity == "critical"

    def test_trace_with_service_fallback(self) -> None:
        """The drift service tag may not match the graph's source node.
        trace() should still find downstream effects via key matching."""
        g = CausalityGraph()
        # The graph has fs.defaultFS under "namenode", but the drift comes
        # from a snapshot tagged "unknown".
        drift = _drift(key="fs.defaultFS", service="unknown")
        root_causes = g.trace([drift])
        assert len(root_causes) == 1
        assert len(root_causes[0].downstream_effects) > 0

    def test_trace_serialization(self) -> None:
        """RootCause objects must be JSON-serializable."""
        g = CausalityGraph()
        drift = _drift(key="fs.defaultFS", service="namenode")
        root_causes = g.trace([drift])
        import json
        serialized = json.dumps([rc.to_dict() for rc in root_causes])
        parsed = json.loads(serialized)
        assert parsed[0]["key"] == "fs.defaultFS"
        assert "downstream_effects" in parsed[0]

    def test_empty_drifts_returns_empty(self) -> None:
        g = CausalityGraph()
        assert g.trace([]) == []
