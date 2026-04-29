# Causality graph

The causality graph is what turns a list of "this key drifted" results into
"and here is what else is affected." It is a directed graph of known
relationships between config keys across the Hadoop ecosystem; for every
detected drift, the consumer walks outbound from the drifting key to collect
all reachable nodes and reports them as `downstream_effects`.

This document covers the graph's model, the BFS walk, **how we know the graph
is consistent with itself** (test contract), how we know it's empirically
useful (smoke test 10), and the pitfalls of the approach.

Code: [`checker/analysis/causality_graph.py`](../checker/analysis/causality_graph.py).
Runtime-editable mirror: [`rules/causality-graph.yaml`](../rules/causality-graph.yaml).

## Model

### Nodes

A node is a `(service, key)` tuple, e.g.
`("namenode", "fs.defaultFS")`. Two same-named keys on different services are
different nodes — `("hive-server2", "fs.defaultFS")` is downstream of
`("namenode", "fs.defaultFS")`, not the same node.

Some nodes are **synthetic**: they don't represent a real config key but a
named consequence the operator should care about. Examples in the current
graph:

- `(namenode, hdfs.write.pipeline)` — "the HDFS write pipeline as a whole"
- `(resourcemanager, container.oom.kill.risk)` — "containers may be killed
  when requesting > scheduler max"
- `(zookeeper, clientPort)` — used as a target of the kafka→zookeeper edge

These never appear as drift sources (no agent publishes them); they only
appear in `downstream_effects` strings to make the report human-readable.
A more sophisticated implementation could track them as separate node
classes, but for now the synthetic/real distinction is implicit in the data.

### Edges

```python
@dataclass(frozen=True)
class Edge:
    source_service: str
    source_key: str
    edge_type: EdgeType
    target_service: str
    target_key: str
    description: str
```

Edges are directed. `description` is human prose for the report and is
deliberately **not** part of the contract — see "Correctness argument"
below.

### Edge types

```python
class EdgeType(Enum):
    CONSTRAINT  = "constraint"     # hard rule, violation is a definite bug
    INFLUENCE   = "influence"      # soft, violation is a probable symptom
    PROPAGATION = "propagation"    # value must agree across a service boundary
```

The type matters in two places:

1. **Severity escalation in `trace()`.** If any edge reachable downstream of
   a drifting key is a `CONSTRAINT`, severity on the resulting `RootCause`
   is upgraded to `critical` regardless of the original drift's severity.
   `INFLUENCE` and `PROPAGATION` do not escalate.
2. **Rule grounding.** Every `constraint` edge corresponds to a rule (e.g.
   `dfs.replication → dfs.replication.max` with the `hdfs-replication-max`
   rule). Every `propagation` edge corresponds to either a multi-service
   propagation rule (e.g. `fs.defaultFS`) or a propagation rule pair (e.g.
   `hive.metastore.uris` between hive-server2 and hive-metastore). Influence
   edges are documentary — they describe a known consequence but no rule
   asserts it.

The current edge set, taken from `_DEFAULT_EDGES`:

| # | Edge | Type | Pairs with rule |
| --- | --- | --- | --- |
| 1 | `namenode:dfs.replication → namenode:dfs.replication.max` | constraint | `hdfs-replication-max` |
| 2 | `namenode:dfs.replication → namenode:hdfs.write.pipeline` | influence | (none — documentary) |
| 3 | `datanode:dfs.replication → namenode:hdfs.write.pipeline` | influence | (none — documentary) |
| 4 | `namenode:fs.defaultFS → hive-server2:fs.defaultFS` | propagation | `fs-defaultfs-propagation` |
| 5 | `namenode:fs.defaultFS → spark-client:spark.hadoop.fs.defaultFS` | propagation | `fs-defaultfs-propagation` |
| 6 | `namenode:fs.defaultFS → hive-metastore:fs.defaultFS` | propagation | `fs-defaultfs-propagation` |
| 7 | `nodemanager:yarn.nodemanager.resource.memory-mb → resourcemanager:yarn.scheduler.maximum-allocation-mb` | constraint | `yarn-scheduler-ceiling` |
| 8 | `resourcemanager:yarn.scheduler.maximum-allocation-mb → resourcemanager:container.oom.kill.risk` | influence | (none — documentary) |
| 9 | `hive-server2:hive.metastore.uris → hive-metastore:metastore.thrift.reachability` | propagation | (consequence-only) |
| 10 | `hive-metastore:hive.metastore.uris → hive-server2:hive.metastore.uris` | propagation | `hive-metastore-uri-consistency` |
| 11 | `kafka:zookeeper.connect → zookeeper:clientPort` | propagation | (no shipped rule — `kafka-zookeeper-connect` was deferred) |

The canonical fixture is
[`tests/checker/fixtures/expected-graph-edges.yaml`](../tests/checker/fixtures/expected-graph-edges.yaml);
the runtime-editable copy is
[`rules/causality-graph.yaml`](../rules/causality-graph.yaml); the
compiled-in defaults are `_DEFAULT_EDGES` in
[`causality_graph.py`](../checker/analysis/causality_graph.py). All three
must stay in sync. The contract test enforces it.

## BFS walk

`CausalityGraph.downstream(service, key)` does a vanilla BFS from the
starting node, following all outbound edges regardless of type, returning
every reachable `(node, edge)` pair. Properties of the implementation:

- The starting node itself is **not** included in the result (the test
  `test_downstream_does_not_include_source` pins this).
- Cycles terminate. The visited set is keyed on the target node, so an edge
  back to an already-visited node is skipped (`test_cycle_does_not_loop`).
  Today the graph has no real cycles, but edges 9 and 10 form one in spirit
  (`hive-server2:hive.metastore.uris ↔ hive-metastore:metastore.thrift.reachability`
  and `hive-metastore:hive.metastore.uris → hive-server2:hive.metastore.uris`)
  and the BFS handles it.
- The walk is **unweighted** — there is no concept of "shortest path" or
  "strongest edge". Every reachable node is reported, and it's up to the
  reader to interpret which downstream effect is most consequential. Severity
  escalation is the one place edge type bleeds into the result.

### From drifts to root causes — `trace()`

```python
def trace(self, drifts: list[DriftResult]) -> list[RootCause]:
```

For each drift:

1. Try `downstream(drift.service, drift.key)` directly.
2. If that returns nothing, scan all source nodes for the same key under any
   service and try those (the "fallback" branch). This handles the case
   where a drift is tagged with one service but the graph node lives under a
   logically equivalent service — e.g. a `dfs.replication` drift tagged
   `resourcemanager` (because that's where the YAML rule fired) still finds
   the `namenode:dfs.replication` node.
3. If any reachable edge is a `CONSTRAINT`, escalate severity to `critical`.
4. Wrap in `RootCause(key, service, drift, downstream_effects, severity)`.

Results are sorted by `len(downstream_effects)` descending — most impactful
first.

## Correctness argument

We make four claims, each with a specific basis:

### Claim 1 — The graph is internally consistent

**Argument.** The canonical edge set in
`tests/checker/fixtures/expected-graph-edges.yaml` is the source of truth.
[`tests/checker/test_graph_contract.py`](../tests/checker/test_graph_contract.py)
asserts:

```python
def test_default_graph_matches_contract_fixture():
    expected = _load_expected(fixture)
    graph = CausalityGraph()
    actual = {_canonical(e) for e in graph.all_edges()}
    missing = expected - actual
    extra   = actual - expected
    assert not missing and not extra
```

`_canonical(edge)` deliberately omits `description` — descriptions are
prose, not part of the contract. Adding, removing, or changing the
endpoints or type of an edge in `_DEFAULT_EDGES` without updating the
fixture (and vice versa) fails this test. Two further tests guard against
typos:

- `test_default_graph_has_no_duplicate_edges` — same `(src, key, type, tgt,
  key)` cannot appear twice.
- `test_every_edge_targets_a_known_shape` — every edge's `edge_type` must
  be a member of `EdgeType`. A typo would silently change the
  severity-escalation logic since unknown types compare unequal to
  `EdgeType.CONSTRAINT`. This test is the safety net.

This **does not** prove the graph is right about Hadoop; it only proves
that the three places we declare the graph (Python, YAML, fixture) agree
with each other.

### Claim 2 — The BFS is correct

**Argument.** The BFS test cases in
[`test_causality_graph.py`](../tests/checker/test_causality_graph.py) cover
the cases that matter:

- `test_downstream_does_not_include_source` — the starting node is excluded.
- `test_transitive_downstream` — A → B → C reports both B and C.
- `test_cycle_does_not_loop` — A → B → A terminates.
- `test_downstream_from_nonexistent_returns_empty` — unknown source returns
  empty list rather than raising.

These do not require the production graph; they construct small custom
graphs and assert behaviour, which is the right level of test for the
algorithm. Any change to `downstream()` that breaks one of these breaks
the test.

### Claim 3 — Trace produces the expected report

**Argument.** This is the plan's named acceptance test
(`test_fs_defaultfs_drift_lists_hive_and_spark`). Given a drift on
`(namenode, fs.defaultFS)`, `trace()` must produce a `RootCause` whose
`downstream_effects` mention both Hive and Spark. The test is intentionally
tolerant — it asserts the *content* (the strings "hive" and "spark" appear)
rather than the exact list — so adding new propagation targets to
`fs.defaultFS` does not break it. The contract test in Claim 1 is what
catches edge changes; this one catches "the report no longer contains the
information operators expect."

`test_trace_multiple_drifts_sorted_by_impact` pins the descending sort
order, so a refactor that returned root causes in input order would fail
loudly.

### Claim 4 — The graph is empirically useful end-to-end

**Argument.** Cluster smoke test 10 (`tests/10-causality-trace.sh`) runs
the full pipeline: agents publish, the consumer ingests, and the resulting
drift report is asserted to contain the expected downstream services.
Slide 7 cites "9 downstream effects traced from test 10" — this is what
that number refers to. If the graph is structurally fine but operationally
broken (agent isn't publishing the right key, validator names a different
service tag, etc.), Claim 1 still passes but smoke test 10 fails.

The five-scenario harness (`tests/evaluate.sh`) is also indirectly a check
on causality: each scenario's `checker-report.json` carries a populated
`root_causes` array, visible in
[`tests/results/latest/<scenario>/checker-report.json`](../tests/results/latest/).
Empty `root_causes` on a known-buggy scenario would mean the graph missed
the path, and the scenario would (correctly) fail.

### What we are NOT claiming

- That the graph is **complete**. There are known Hadoop config
  relationships not modeled — anything involving `dfs.namenode.handler.count`,
  `mapreduce.job.reduces`, log4j config, etc. We modeled the relationships
  needed for the seven shipped rules and the five evaluation scenarios.
- That `INFLUENCE` edges are **provably correct**. They are documentary
  claims about what tends to happen ("higher replication = more ack round
  trips"). They are not tested against a real cluster and cannot be —
  they describe statistical tendencies, not assertions.
- That the absence of a downstream effect means a key is **isolated**.
  Many real keys have effects we haven't modeled. The graph is a positive
  set: edges we claim exist, not edges we deny.

## Pitfalls

These are real ways the graph misleads, in roughly descending severity.

### 1. Hand-coded edges drift from reality

Every edge was added by a human reading Hadoop documentation. There is no
mechanism that re-derives the edge set from a real cluster. If a future
Hadoop version renames a key, removes a relationship, or adds a new one,
the graph silently goes stale. **Symptom**: rule fires, but
`downstream_effects` is empty or names a stale service.

**Mitigation today**: contract test (Claim 1) keeps the *declared* edge
set internally consistent. A future upgrade is "edge inference from
operational traces" — see [limitations.md](limitations.md).

### 2. Influence edges look authoritative

The report just says "downstream effect: X". Constraint, propagation, and
influence all serialize the same way. An operator reading
`namenode:hdfs.write.pipeline` cannot tell from the report alone whether
that's a hard rule violation or a soft tendency. We mitigate this by:

- only escalating severity on `CONSTRAINT` edges (so the severity field is
  trustworthy);
- including the edge `description` in the rendered effect string (so the
  operator can read "higher replication = more ack round trips" and infer
  this is descriptive, not a violation).

But a future UI should color edge types differently. The JSON output
today does not preserve edge type in `downstream_effects` — it's lost in
the descriptor string.

### 3. Synthetic nodes have no agent

Nodes like `container.oom.kill.risk` and `hdfs.write.pipeline` exist only
in the graph. They never receive a drift; they only show up as effects.
Several reasonable refactorings (e.g. "compute the closure of all real
keys reachable from a drift") would silently exclude them. The current
BFS includes them because it doesn't distinguish — but a developer not
aware of this could break it.

### 4. The fallback service-match in `_downstream_effects` can be surprising

When a drift is tagged with service A but the matching graph node is
under service B (because the validator names the rule's target service,
not the source), the trace falls back to scanning all sources for a
matching key. This works in practice but means the **service in the
report does not always match the service in the graph node**. Operators
seeing "downstream of resourcemanager:dfs.replication" should not be
confused; the underlying graph node is `namenode:dfs.replication`.

**Mitigation**: the description string and the actual `downstream_effects`
include the correct (graph-node) service name. The drift's own
`service` field still reflects where the rule fired, which is also useful.

### 5. BFS reports everything reachable, not just direct effects

`fs.defaultFS` on the namenode reaches three direct propagation targets
(hive-server2, spark-client, hive-metastore). Each of those is itself a
`fs.defaultFS` node — but in the current graph none of them have outbound
edges, so the report is one hop deep. If a future edge addition put
hive-server2's `fs.defaultFS` upstream of something else, a single
`fs.defaultFS` drift would suddenly report transitive effects too. This is
correct BFS behavior but operators may interpret a long downstream list as
"this drift caused all of these" rather than "all of these are reachable
from this drift." There is no edge-distance attenuation.

### 6. No cycle reporting

The BFS terminates on cycles but does not surface them. If a cycle exists
between, say, `hive-server2:hive.metastore.uris` and
`hive-metastore:hive.metastore.uris` (which it does, via edge 10 plus a
hypothetical reverse), and an operator wants to know which one is
"upstream", the graph cannot answer.

## Editing the graph

To add an edge:

1. Append to `_DEFAULT_EDGES` in
   [`checker/analysis/causality_graph.py`](../checker/analysis/causality_graph.py).
2. Append to
   [`rules/causality-graph.yaml`](../rules/causality-graph.yaml) (the
   runtime-editable copy).
3. Append to
   [`tests/checker/fixtures/expected-graph-edges.yaml`](../tests/checker/fixtures/expected-graph-edges.yaml)
   (the contract).
4. Run `pytest tests/checker/test_graph_contract.py -v`.

The contract test failing means you missed one of the three locations.
This is the intended check — it forces the same change to land in all
three places in one commit.

To override the graph at runtime without rebuilding the image, set
`CHECKER_GRAPH_FILE=/etc/checker/rules/causality-graph.yaml` (the override
file already does this). `CausalityGraph.load_default()` honors this env
var; if the path is missing or unreadable it logs a warning and falls back
to `_DEFAULT_EDGES`.
