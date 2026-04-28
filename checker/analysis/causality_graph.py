"""Causality graph — traces drift to root causes with downstream effects.

The graph encodes known relationships between config keys across the
Hadoop ecosystem. When a drift is detected, ``trace()`` walks the graph
outbound from the drifting key to find everything affected downstream.

Graph structure
---------------
Nodes are ``(service, key)`` tuples. Edges are ``Edge`` dataclasses with
a type (constraint, influence, propagation) and a human-readable
description. Some edges target a concrete ``(service, key)`` pair; others
target a descriptive string (e.g. ``"HDFS write pipeline"``).

The graph is seeded with the initial edge set (``_DEFAULT_EDGES``) which
is the source of truth for tests. ``rules/causality-graph.yaml`` mirrors
this list and is what operators edit at runtime — see
``CausalityGraph.from_yaml()`` and the ``CHECKER_GRAPH_FILE`` env var
honoured by ``CausalityGraph.load_default()``.

Trace algorithm
---------------
``trace(drifts)`` takes a list of ``DriftResult`` objects and returns a
list of ``RootCause`` objects, one per drifting key, sorted by the number
of downstream effects (most impactful first). The walk is a simple BFS
that collects all reachable nodes from each drifting key.
"""

from __future__ import annotations

import logging
import os
from collections import deque
from dataclasses import dataclass

from checker.models import DriftResult, EdgeType, RootCause

logger = logging.getLogger("checker.causality_graph")


# ---------------------------------------------------------------------------
# Edge definition
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Edge:
    """A directed edge in the causality graph."""

    source_service: str
    source_key: str
    edge_type: EdgeType
    target_service: str
    target_key: str
    description: str = ""


# Type alias for graph nodes
Node = tuple[str, str]  # (service, key)


# ---------------------------------------------------------------------------
# Default edge set from plan.md
#
# This list is the source of truth for the test suite (see
# tests/checker/test_graph_contract.py and
# tests/checker/fixtures/expected-graph-edges.yaml). The matching YAML
# file at rules/causality-graph.yaml mirrors this list and is what
# operators edit; the consumer loads the YAML when CHECKER_GRAPH_FILE is
# set, otherwise it uses this Python list.
# ---------------------------------------------------------------------------

_DEFAULT_EDGES: list[Edge] = [
    Edge(
        source_service="namenode",
        source_key="dfs.replication",
        edge_type=EdgeType.CONSTRAINT,
        target_service="namenode",
        target_key="dfs.replication.max",
        description="dfs.replication must be ≤ dfs.replication.max",
    ),
    Edge(
        source_service="namenode",
        source_key="dfs.replication",
        edge_type=EdgeType.INFLUENCE,
        target_service="namenode",
        target_key="hdfs.write.pipeline",
        description="higher replication = more ack round trips",
    ),
    Edge(
        source_service="namenode",
        source_key="fs.defaultFS",
        edge_type=EdgeType.PROPAGATION,
        target_service="hive-server2",
        target_key="fs.defaultFS",
        description="fs.defaultFS must match across core-site and hive SERVICE_OPTS",
    ),
    Edge(
        source_service="namenode",
        source_key="fs.defaultFS",
        edge_type=EdgeType.PROPAGATION,
        target_service="spark-client",
        target_key="spark.hadoop.fs.defaultFS",
        description="fs.defaultFS must propagate to spark-defaults",
    ),
    Edge(
        source_service="nodemanager",
        source_key="yarn.nodemanager.resource.memory-mb",
        edge_type=EdgeType.CONSTRAINT,
        target_service="resourcemanager",
        target_key="yarn.scheduler.maximum-allocation-mb",
        description="NM total memory must be ≥ scheduler max allocation",
    ),
    Edge(
        source_service="resourcemanager",
        source_key="yarn.scheduler.maximum-allocation-mb",
        edge_type=EdgeType.INFLUENCE,
        target_service="resourcemanager",
        target_key="container.oom.kill.risk",
        description="containers requesting more than scheduler max get killed",
    ),
    Edge(
        source_service="hive-server2",
        source_key="hive.metastore.uris",
        edge_type=EdgeType.PROPAGATION,
        target_service="hive-metastore",
        target_key="metastore.thrift.reachability",
        description="hive.metastore.uris must resolve to a running metastore",
    ),
    Edge(
        source_service="namenode",
        source_key="fs.defaultFS",
        edge_type=EdgeType.PROPAGATION,
        target_service="hive-metastore",
        target_key="fs.defaultFS",
        description="fs.defaultFS must match in hive-metastore SERVICE_OPTS",
    ),
    # ---------------------------------------------------------------
    # Stage 1.4 additions — edges that become meaningful once the new
    # agent sidecars (kafka, zookeeper, datanode, hive-metastore) are
    # actually publishing snapshots.
    # ---------------------------------------------------------------
    Edge(
        source_service="kafka",
        source_key="zookeeper.connect",
        edge_type=EdgeType.PROPAGATION,
        target_service="zookeeper",
        target_key="clientPort",
        description="kafka's zookeeper.connect must reference a reachable ZooKeeper",
    ),
    Edge(
        source_service="datanode",
        source_key="dfs.replication",
        edge_type=EdgeType.INFLUENCE,
        target_service="namenode",
        target_key="hdfs.write.pipeline",
        description="datanode-side replication setting affects the HDFS write pipeline",
    ),
    Edge(
        source_service="hive-metastore",
        source_key="hive.metastore.uris",
        edge_type=EdgeType.PROPAGATION,
        target_service="hive-server2",
        target_key="hive.metastore.uris",
        description="metastore URI must agree between hive-metastore and hive-server2",
    ),
]


# ---------------------------------------------------------------------------
# CausalityGraph
# ---------------------------------------------------------------------------


class CausalityGraph:
    """Directed graph of config-key relationships.

    Nodes are ``(service, key)`` tuples. The graph stores adjacency lists
    keyed by source node.
    """

    def __init__(self, edges: list[Edge] | None = None) -> None:
        self._adj: dict[Node, list[Edge]] = {}
        for edge in (edges if edges is not None else _DEFAULT_EDGES):
            self.add_edge(edge)

    def add_edge(self, edge: Edge) -> None:
        """Add a directed edge to the graph."""
        src: Node = (edge.source_service, edge.source_key)
        self._adj.setdefault(src, []).append(edge)

    def edges_from(self, service: str, key: str) -> list[Edge]:
        """Return all outbound edges from ``(service, key)``."""
        return list(self._adj.get((service, key), []))

    def all_edges(self) -> list[Edge]:
        """Return a flat list of every edge in the graph."""
        result: list[Edge] = []
        for edges in self._adj.values():
            result.extend(edges)
        return result

    def nodes(self) -> set[Node]:
        """Return all nodes that appear as either source or target."""
        ns: set[Node] = set()
        for src, edges in self._adj.items():
            ns.add(src)
            for e in edges:
                ns.add((e.target_service, e.target_key))
        return ns

    # -------------------------------------------------------------------
    # BFS downstream walk
    # -------------------------------------------------------------------

    def downstream(self, service: str, key: str) -> list[tuple[Node, Edge]]:
        """BFS walk from ``(service, key)``, returning all reachable
        ``(node, edge)`` pairs.

        The walk follows edges regardless of type. The source node itself
        is NOT included in the results — only its downstream dependents.
        """
        start: Node = (service, key)
        visited: set[Node] = {start}
        queue: deque[Node] = deque([start])
        results: list[tuple[Node, Edge]] = []

        while queue:
            current = queue.popleft()
            for edge in self._adj.get(current, []):
                target: Node = (edge.target_service, edge.target_key)
                if target not in visited:
                    visited.add(target)
                    results.append((target, edge))
                    queue.append(target)

        return results

    # -------------------------------------------------------------------
    # Trace: drifts → root causes
    # -------------------------------------------------------------------

    def trace(self, drifts: list[DriftResult]) -> list[RootCause]:
        """Map drifts to root causes with downstream effects.

        For each drift, walks outbound edges from ``(service, key)`` to
        collect all downstream effects. If ``(service, key)`` is not in
        the graph, also tries with a wildcard service match (because the
        real cluster may tag everything as "namenode" while the graph
        uses the logical service name).

        Results are sorted by number of downstream effects (most impactful
        first).
        """
        root_causes: list[RootCause] = []

        for drift in drifts:
            effects = self._downstream_effects(drift.service, drift.key)

            # Determine severity: if any downstream edge is a constraint,
            # escalate to critical.
            severity = drift.severity
            for _node, edge in effects:
                if edge.edge_type == EdgeType.CONSTRAINT:
                    severity = "critical"
                    break

            effect_descriptions = []
            for (svc, k), edge in effects:
                desc = f"{svc}:{k}"
                if edge.description:
                    desc += f" ({edge.description})"
                effect_descriptions.append(desc)

            root_causes.append(RootCause(
                key=drift.key,
                service=drift.service,
                drift=drift,
                downstream_effects=effect_descriptions,
                severity=severity,
            ))

        # Sort by impact: most downstream effects first.
        root_causes.sort(key=lambda rc: len(rc.downstream_effects), reverse=True)
        return root_causes

    def _downstream_effects(
        self, service: str, key: str
    ) -> list[tuple[Node, Edge]]:
        """Find downstream effects, trying both the exact service and
        all services that have edges from this key."""
        # Try exact match first.
        effects = self.downstream(service, key)
        if effects:
            return effects

        # Fallback: the drift's service tag might not match the graph's
        # node names (e.g. drift says "namenode" but graph uses
        # "nodemanager" for yarn keys). Search all source nodes for
        # matching keys.
        for (src_svc, src_key), _ in self._adj.items():
            if src_key == key and src_svc != service:
                effects = self.downstream(src_svc, key)
                if effects:
                    return effects

        return []

    # -------------------------------------------------------------------
    # YAML loading (for custom edge sets)
    # -------------------------------------------------------------------

    @classmethod
    def from_yaml(cls, path: str) -> "CausalityGraph":
        """Load a causality graph from a YAML file.

        Expected format::

            edges:
              - source_service: namenode
                source_key: fs.defaultFS
                edge_type: propagation
                target_service: hive-server2
                target_key: fs.defaultFS
                description: must match
        """
        import yaml
        from pathlib import Path as P

        p = P(path)
        data = yaml.safe_load(p.read_text(encoding="utf-8"))
        raw_edges = data.get("edges", [])
        edges = []
        for raw in raw_edges:
            edges.append(Edge(
                source_service=raw["source_service"],
                source_key=raw["source_key"],
                edge_type=EdgeType(raw["edge_type"]),
                target_service=raw["target_service"],
                target_key=raw["target_key"],
                description=raw.get("description", ""),
            ))
        return cls(edges=edges)

    @classmethod
    def load_default(cls) -> "CausalityGraph":
        """Load the graph honouring the ``CHECKER_GRAPH_FILE`` env var.

        Stage 2.4: if ``CHECKER_GRAPH_FILE`` points to an existing YAML
        file, load from it. Otherwise fall back to the compiled
        ``_DEFAULT_EDGES`` list. Used by the consumer entry point.
        """
        path = os.environ.get("CHECKER_GRAPH_FILE")
        if path:
            from pathlib import Path as P
            if P(path).is_file():
                try:
                    g = cls.from_yaml(path)
                    logger.info(
                        "loaded causality graph from %s (%d edges)",
                        path, len(g.all_edges()),
                    )
                    return g
                except Exception as exc:
                    logger.warning(
                        "failed to load causality graph from %s: %s — "
                        "falling back to compiled defaults",
                        path, exc,
                    )
            else:
                logger.warning(
                    "CHECKER_GRAPH_FILE=%s does not exist — "
                    "using compiled defaults", path,
                )
        return cls()
