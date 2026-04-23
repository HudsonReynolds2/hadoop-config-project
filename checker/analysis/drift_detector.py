"""Drift detector — compares ``ConfigSnapshot`` pairs and emits ``DriftResult`` objects.

The detector is deliberately stateless: it takes two snapshots and returns
a list of disagreements.  The *consumer* is responsible for choosing which
pairs to compare (same agent over time, same service across sources, etc.).

Comparison modes
----------------

``detect(snapshot_a, snapshot_b)``
    Raw pairwise diff.  Every key that exists in either snapshot is compared.
    Keys present in one but absent in the other are reported with the missing
    side set to ``None``.  Keys present in both but with differing values are
    reported with both values.  Keys that agree are silently skipped.

``detect_cross_source(snapshots, severity="warning")``
    Compares all snapshots that share the same ``service`` but come from
    different sources (e.g. ``xml_file`` vs ``env_file``).  This implements
    the ``dual-source-consistency`` rule from the plan: for every key that
    appears in more than one source for the same service, values must agree.

Both return ``DriftResult`` objects ready for JSON serialization.

Design notes
------------
- The detector never mutates its inputs.
- Severity defaults are intentionally conservative (``"warning"``); the
  rule engine in Phase 4 can upgrade specific keys to ``"critical"``.
- ``rule_id`` is ``None`` for raw pairwise diffs and set to a descriptive
  string for cross-source checks, so downstream consumers can distinguish
  rule-triggered results from generic diffs.
"""

from __future__ import annotations

from checker.models import ConfigSnapshot, DriftResult


def _source_label(snap: ConfigSnapshot) -> str:
    """Build the ``source_a`` / ``source_b`` descriptor string.

    Format: ``"<source>:<source_path>"`` — e.g. ``"xml_file:conf/core-site.xml"``.
    """
    return f"{snap.source}:{snap.source_path}"


# ---------------------------------------------------------------------------
# Pairwise diff
# ---------------------------------------------------------------------------


def detect(
    snapshot_a: ConfigSnapshot,
    snapshot_b: ConfigSnapshot,
    severity: str = "warning",
    rule_id: str | None = None,
) -> list[DriftResult]:
    """Compare two snapshots key-by-key and return all disagreements.

    Parameters
    ----------
    snapshot_a, snapshot_b
        The two snapshots to compare.  Order determines which appears as
        ``source_a`` vs ``source_b`` in the result.
    severity
        Default severity for every drift found.  Can be overridden per-key
        by a higher-level rule engine.
    rule_id
        Optional rule identifier attached to every result (e.g.
        ``"dual-source-consistency"``).  ``None`` for raw diffs.

    Returns
    -------
    list[DriftResult]
        One entry per disagreeing key.  Empty if the snapshots agree on
        every key (including both having the same set of keys).
    """
    results: list[DriftResult] = []
    all_keys = set(snapshot_a.properties) | set(snapshot_b.properties)
    label_a = _source_label(snapshot_a)
    label_b = _source_label(snapshot_b)

    # Use the service from snapshot_a; when comparing cross-service the
    # caller should pick the appropriate service tag.
    service = snapshot_a.service

    for key in sorted(all_keys):
        val_a = snapshot_a.properties.get(key)
        val_b = snapshot_b.properties.get(key)
        if val_a == val_b:
            continue
        results.append(
            DriftResult(
                key=key,
                service=service,
                source_a=label_a,
                value_a=val_a,
                source_b=label_b,
                value_b=val_b,
                severity=severity,
                rule_id=rule_id,
            )
        )
    return results


# ---------------------------------------------------------------------------
# Cross-source consistency (dual-source-consistency rule)
# ---------------------------------------------------------------------------


def detect_cross_source(
    snapshots: list[ConfigSnapshot],
    severity: str = "warning",
) -> list[DriftResult]:
    """Compare overlapping keys across different sources for the same service.

    Groups snapshots by ``(service, source)`` and compares every pair of
    distinct sources within the same service.  Only keys that appear in
    *both* sources are checked — a key exclusive to one source is not drift,
    it's just a different config surface.

    This is the programmatic equivalent of the ``dual-source-consistency``
    rule in ``rules/hadoop-3.3.x.yaml``.
    """
    # Group by service
    by_service: dict[str, list[ConfigSnapshot]] = {}
    for snap in snapshots:
        by_service.setdefault(snap.service, []).append(snap)

    results: list[DriftResult] = []
    for service, group in sorted(by_service.items()):
        # Within a service, compare each unique pair of snapshots that have
        # different source types.
        for i, snap_a in enumerate(group):
            for snap_b in group[i + 1 :]:
                if snap_a.source == snap_b.source:
                    continue
                # Only report keys present in both — missing keys are not
                # drift for cross-source checks.
                overlap = set(snap_a.properties) & set(snap_b.properties)
                label_a = _source_label(snap_a)
                label_b = _source_label(snap_b)
                for key in sorted(overlap):
                    val_a = snap_a.properties[key]
                    val_b = snap_b.properties[key]
                    if val_a == val_b:
                        continue
                    results.append(
                        DriftResult(
                            key=key,
                            service=service,
                            source_a=label_a,
                            value_a=val_a,
                            source_b=label_b,
                            value_b=val_b,
                            severity=severity,
                            rule_id="dual-source-consistency",
                        )
                    )
    return results


# ---------------------------------------------------------------------------
# Temporal diff — same agent, two points in time
# ---------------------------------------------------------------------------


def detect_temporal(
    previous: ConfigSnapshot,
    current: ConfigSnapshot,
    severity: str = "warning",
) -> list[DriftResult]:
    """Compare two snapshots from the *same* agent at different timestamps.

    Semantically identical to ``detect()``, but sets ``rule_id`` to
    ``"temporal-drift"`` so consumers can distinguish "config changed over
    time" from "configs disagree across sources".

    Raises ``ValueError`` if the two snapshots have different ``agent_id``
    values — temporal comparison only makes sense for the same agent.
    """
    if previous.agent_id != current.agent_id:
        raise ValueError(
            f"temporal diff requires same agent_id, got "
            f"{previous.agent_id!r} vs {current.agent_id!r}"
        )
    return detect(previous, current, severity=severity, rule_id="temporal-drift")
