"""Pin the default ``CausalityGraph()`` edge set.

The edge table is documented in three places:

    1. plan.md §"Causality graph — initial edge set"
    2. ``_DEFAULT_EDGES`` in
       [checker/analysis/causality_graph.py](../../checker/analysis/causality_graph.py)
    3. ``tests/checker/fixtures/expected-graph-edges.yaml``

Until a machine-readable source of truth exists, #3 plays that role. This
test asserts that #2 loads the same edges #3 does. Drift between the two
is a bug; update both, plus plan.md, in the same commit.

Consequential failure modes caught here:

    * An edge removed in code — e.g. the ``fs.defaultFS → hive-server2``
      propagation edge — would stop being reported as a downstream effect
      and no existing integration test would fail because other matching
      edges (``fs.defaultFS → hive-metastore``, ``→ spark-client``) still
      contain substrings like "hive" and "spark".
"""

from __future__ import annotations

from pathlib import Path

from checker.analysis.causality_graph import CausalityGraph
from checker.models import EdgeType


def _canonical(edge) -> tuple:
    """Hashable identity for an edge — description is prose and not part
    of the contract, so it's intentionally excluded."""
    return (
        edge.source_service,
        edge.source_key,
        edge.edge_type.value if isinstance(edge.edge_type, EdgeType) else edge.edge_type,
        edge.target_service,
        edge.target_key,
    )


def _load_expected(path: Path) -> set[tuple]:
    import yaml
    data = yaml.safe_load(path.read_text())
    expected = set()
    for e in data["edges"]:
        expected.add((
            e["source_service"],
            e["source_key"],
            e["edge_type"],
            e["target_service"],
            e["target_key"],
        ))
    return expected


def test_default_graph_matches_contract_fixture() -> None:
    fixture = Path(__file__).parent / "fixtures" / "expected-graph-edges.yaml"
    expected = _load_expected(fixture)

    graph = CausalityGraph()
    actual = {_canonical(e) for e in graph.all_edges()}

    missing = expected - actual
    extra = actual - expected
    assert not missing and not extra, (
        "default graph drift from contract:\n"
        f"  missing (in fixture, not in graph): {sorted(missing)}\n"
        f"  extra   (in graph, not in fixture): {sorted(extra)}\n"
        "Update causality_graph._DEFAULT_EDGES, the fixture, AND plan.md together."
    )


def test_default_graph_has_no_duplicate_edges() -> None:
    graph = CausalityGraph()
    edges = [_canonical(e) for e in graph.all_edges()]
    assert len(edges) == len(set(edges)), (
        "duplicate edges in default graph: "
        + ", ".join(str(e) for e in edges if edges.count(e) > 1)
    )


def test_every_edge_targets_a_known_shape() -> None:
    """Every edge's type must be a member of ``EdgeType``. A typo in a
    new edge definition would surface here before it silently changes the
    severity-escalation logic in ``trace()``."""
    graph = CausalityGraph()
    valid = {t.value for t in EdgeType}
    for edge in graph.all_edges():
        value = edge.edge_type.value if isinstance(edge.edge_type, EdgeType) else edge.edge_type
        assert value in valid, f"edge {edge} has unknown type {value!r}"
