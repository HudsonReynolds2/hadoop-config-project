"""Rule-based validator — evaluates a YAML rule set against a SnapshotStore.

The validator is the bridge between the declarative rule definitions in
``rules/hadoop-3.3.x.yaml`` and the runtime snapshot data.  It handles
three rule types:

``constraint``
    Numeric comparison between two keys.  The ``relation`` field is one of
    ``lte``, ``lt``, ``gte``, ``gt``, ``eq``.  Both keys are looked up
    across the entire snapshot store — the ``service`` fields in the rule
    are hints for provenance, but the lookup searches all snapshots so that
    shared config files (like yarn-site.xml read by both resourcemanager
    and nodemanager) work correctly.

``propagation``
    Value-agreement rules.  Three sub-types:

    - **multi-service propagation** (``services`` list): a single key must
      have the same value across all listed services.
    - **cross-key propagation** (``target`` block): a key on one service
      must equal a different key on another service (e.g.
      ``spark.hadoop.fs.defaultFS`` on spark-client must equal
      ``fs.defaultFS`` on namenode).
    - **must-contain** (``must_contain_value_of``): one key's value must
      appear as a substring of another key's value (e.g. hive warehouse
      dir must contain fs.defaultFS).

``dual-source-consistency``
    Already handled by ``drift_detector.detect_cross_source()`` and
    re-invoked here for completeness when the rule set includes the
    ``dual-source-consistency`` rule.

Key lookup strategy
-------------------
Real Hadoop clusters share config files across services via bind mounts.
A single ``yarn-site.xml`` is read by both the ResourceManager and the
NodeManager.  The agent tags each snapshot with a single ``service`` name,
but the file may contain keys that logically belong to a different service.

The validator therefore does NOT filter by service when looking up a key.
It searches all snapshots in the store and returns the first value found.
This matches the real-world behaviour: if ``yarn-site.xml`` contains
``yarn.scheduler.maximum-allocation-mb``, the validator will find it
regardless of whether the snapshot was tagged ``resourcemanager``,
``nodemanager``, or ``namenode``.

When multiple snapshots contain the same key with different values, the
service-specific snapshot is preferred if one matches the rule's service
field.  Otherwise the first match wins.  This is consistent with
last-writer-wins in the store and with the fact that bind-mounted files
produce identical values across services.
"""

from __future__ import annotations

import dataclasses
import json
import logging
import operator
from dataclasses import dataclass, field
from pathlib import Path

from checker.models import DriftResult

logger = logging.getLogger("checker.validator")


# ---------------------------------------------------------------------------
# ValidationResult — one per rule evaluation
# ---------------------------------------------------------------------------


@dataclass
class ValidationResult:
    """The outcome of evaluating a single rule against the store."""

    rule_id: str
    description: str
    passed: bool
    severity: str
    details: str = ""
    drift: DriftResult | None = None

    def to_dict(self) -> dict:
        d = dataclasses.asdict(self)
        return d

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), sort_keys=True)


# ---------------------------------------------------------------------------
# Rule loading
# ---------------------------------------------------------------------------


def load_rules(path: str | Path) -> list[dict]:
    """Load rules from a YAML file.  Returns a list of rule dicts."""
    try:
        import yaml
    except ImportError as exc:
        raise RuntimeError(
            "pyyaml is required for the rule engine. "
            "Install it with:  pip install pyyaml"
        ) from exc

    p = Path(path)
    if not p.is_file():
        raise FileNotFoundError(f"rule file not found: {p}")

    data = yaml.safe_load(p.read_text(encoding="utf-8"))
    if not isinstance(data, dict) or "rules" not in data:
        raise ValueError(f"expected top-level 'rules' key in {p}")

    rules = data["rules"]
    if not isinstance(rules, list):
        raise ValueError(f"'rules' must be a list in {p}")

    return rules


# ---------------------------------------------------------------------------
# Key lookup in the store
# ---------------------------------------------------------------------------


def _find_key(store, key: str, preferred_service: str | None = None) -> str | None:
    """Find a config key's value across all snapshots in the store.

    If ``preferred_service`` is given and a snapshot tagged with that
    service contains the key, its value is returned.  Otherwise the first
    snapshot containing the key wins.

    Returns None if the key is not found in any snapshot.
    """
    # First pass: check preferred service
    if preferred_service:
        for snap in store.snapshots_for_service(preferred_service):
            if key in snap.properties:
                return snap.properties[key]

    # Second pass: any service
    for snap in store.all_snapshots():
        if key in snap.properties:
            return snap.properties[key]

    return None


def _find_key_by_service(store, key: str, service: str) -> str | None:
    """Find a key's value specifically from snapshots tagged with ``service``.

    Falls back to searching all snapshots if the service has no match.
    """
    return _find_key(store, key, preferred_service=service)


# ---------------------------------------------------------------------------
# Relation operators
# ---------------------------------------------------------------------------

_RELATIONS = {
    "lte": operator.le,
    "lt": operator.lt,
    "gte": operator.ge,
    "gt": operator.gt,
    "eq": operator.eq,
}

_RELATION_SYMBOLS = {
    "lte": "<=",
    "lt": "<",
    "gte": ">=",
    "gt": ">",
    "eq": "==",
}


# ---------------------------------------------------------------------------
# Rule evaluators
# ---------------------------------------------------------------------------


def _eval_constraint(rule: dict, store) -> ValidationResult:
    """Evaluate a ``constraint`` rule: numeric comparison between two keys."""
    rule_id = rule["id"]
    key = rule["key"]
    target_key = rule["target_key"]
    relation = rule["relation"]
    severity = rule.get("severity", "warning")
    service = rule.get("service")
    target_service = rule.get("target_service", service)

    val_str = _find_key(store, key, preferred_service=service)
    target_str = _find_key(store, target_key, preferred_service=target_service)

    # If either key is missing, we can't evaluate the rule.
    if val_str is None:
        return ValidationResult(
            rule_id=rule_id,
            description=rule.get("description", ""),
            passed=True,  # can't fail what we can't check
            severity=severity,
            details=f"key {key!r} not found in store, rule skipped",
        )
    if target_str is None:
        return ValidationResult(
            rule_id=rule_id,
            description=rule.get("description", ""),
            passed=True,
            severity=severity,
            details=f"target key {target_key!r} not found in store, rule skipped",
        )

    # Parse as numbers
    try:
        val = float(val_str)
        target_val = float(target_str)
    except ValueError:
        return ValidationResult(
            rule_id=rule_id,
            description=rule.get("description", ""),
            passed=False,
            severity=severity,
            details=(
                f"cannot compare non-numeric values: "
                f"{key}={val_str!r}, {target_key}={target_str!r}"
            ),
        )

    op_func = _RELATIONS.get(relation)
    if op_func is None:
        return ValidationResult(
            rule_id=rule_id,
            description=rule.get("description", ""),
            passed=False,
            severity=severity,
            details=f"unknown relation {relation!r}",
        )

    passed = op_func(val, target_val)
    symbol = _RELATION_SYMBOLS.get(relation, relation)

    if passed:
        details = f"{key}={val_str} {symbol} {target_key}={target_str}: OK"
    else:
        details = f"{key}={val_str} {symbol} {target_key}={target_str}: FAILED"

    drift = None
    if not passed:
        drift = DriftResult(
            key=key,
            service=service or "unknown",
            source_a=f"rule:{rule_id}",
            value_a=val_str,
            source_b=f"target:{target_key}",
            value_b=target_str,
            severity=severity,
            rule_id=rule_id,
        )

    return ValidationResult(
        rule_id=rule_id,
        description=rule.get("description", ""),
        passed=passed,
        severity=severity,
        details=details,
        drift=drift,
    )


def _eval_propagation(rule: dict, store) -> ValidationResult:
    """Evaluate a ``propagation`` rule: value agreement across services."""
    rule_id = rule["id"]
    severity = rule.get("severity", "warning")

    # Sub-type 1: multi-service propagation (services list)
    if "services" in rule:
        return _eval_propagation_multi(rule, store)

    # Sub-type 2: cross-key propagation (target block)
    if "target" in rule:
        return _eval_propagation_cross_key(rule, store)

    # Sub-type 3: must-contain
    if "must_contain_value_of" in rule:
        return _eval_must_contain(rule, store)

    # Sub-type 4: dual-source-consistency (handled by drift_detector)
    if "sources" in rule:
        return _eval_dual_source(rule, store)

    return ValidationResult(
        rule_id=rule_id,
        description=rule.get("description", ""),
        passed=True,
        severity=severity,
        details="propagation rule has no services/target/must_contain_value_of/sources — skipped",
    )


def _eval_propagation_multi(rule: dict, store) -> ValidationResult:
    """fs.defaultFS must be identical across all listed services."""
    rule_id = rule["id"]
    key = rule["key"]
    services = rule["services"]
    severity = rule.get("severity", "warning")

    values: dict[str, str] = {}
    for svc in services:
        val = _find_key_by_service(store, key, svc)
        if val is not None:
            values[svc] = val

    if len(values) <= 1:
        found = list(values.keys())
        return ValidationResult(
            rule_id=rule_id,
            description=rule.get("description", ""),
            passed=True,
            severity=severity,
            details=f"{key!r} found in {len(values)} of {len(services)} services ({found}), nothing to compare",
        )

    unique_vals = set(values.values())
    passed = len(unique_vals) == 1

    if passed:
        details = f"{key!r} agrees across {list(values.keys())}: {list(unique_vals)[0]!r}"
    else:
        pairs = ", ".join(f"{svc}={v!r}" for svc, v in values.items())
        details = f"{key!r} disagrees: {pairs}"

    drift = None
    if not passed:
        # Report the first disagreement
        svcs = list(values.keys())
        drift = DriftResult(
            key=key,
            service=svcs[0],
            source_a=f"service:{svcs[0]}",
            value_a=values[svcs[0]],
            source_b=f"service:{svcs[1]}",
            value_b=values[svcs[1]],
            severity=severity,
            rule_id=rule_id,
        )

    return ValidationResult(
        rule_id=rule_id,
        description=rule.get("description", ""),
        passed=passed,
        severity=severity,
        details=details,
        drift=drift,
    )


def _eval_propagation_cross_key(rule: dict, store) -> ValidationResult:
    """Cross-key propagation: ``key@service`` must equal ``target.key@target.service``.

    Used when two different keys on two different services must hold the
    same value (e.g. ``spark.hadoop.fs.defaultFS`` on spark-client must
    equal ``fs.defaultFS`` on namenode). Symmetric string equality. If
    either side is missing the rule is skipped (``passed=True`` with a
    skipped detail) — same convention as the other evaluators.
    """
    rule_id = rule["id"]
    key = rule["key"]
    service = rule.get("service")
    severity = rule.get("severity", "warning")
    target = rule["target"]
    target_key = target["key"]
    target_service = target.get("service")

    val = _find_key_by_service(store, key, service) if service else _find_key(store, key)
    target_val = (
        _find_key_by_service(store, target_key, target_service)
        if target_service
        else _find_key(store, target_key)
    )

    if val is None:
        return ValidationResult(
            rule_id=rule_id,
            description=rule.get("description", ""),
            passed=True,
            severity=severity,
            details=f"key {key!r} (service={service}) not found, rule skipped",
        )
    if target_val is None:
        return ValidationResult(
            rule_id=rule_id,
            description=rule.get("description", ""),
            passed=True,
            severity=severity,
            details=f"target key {target_key!r} (service={target_service}) not found, rule skipped",
        )

    passed = val == target_val

    if passed:
        details = (
            f"{service}:{key}={val!r} == {target_service}:{target_key}={target_val!r}: OK"
        )
    else:
        details = (
            f"{service}:{key}={val!r} != {target_service}:{target_key}={target_val!r}"
        )

    drift = None
    if not passed:
        drift = DriftResult(
            key=key,
            service=service or "unknown",
            source_a=f"service:{service}:{key}",
            value_a=val,
            source_b=f"service:{target_service}:{target_key}",
            value_b=target_val,
            severity=severity,
            rule_id=rule_id,
        )

    return ValidationResult(
        rule_id=rule_id,
        description=rule.get("description", ""),
        passed=passed,
        severity=severity,
        details=details,
        drift=drift,
    )


def _eval_must_contain(rule: dict, store) -> ValidationResult:
    """hive.metastore.warehouse.dir must contain the value of fs.defaultFS."""
    rule_id = rule["id"]
    key = rule["key"]
    service = rule.get("service")
    severity = rule.get("severity", "warning")
    ref = rule["must_contain_value_of"]
    ref_key = ref["key"]
    ref_service = ref.get("service")

    val = _find_key(store, key, preferred_service=service)
    ref_val = _find_key(store, ref_key, preferred_service=ref_service)

    if val is None:
        return ValidationResult(
            rule_id=rule_id,
            description=rule.get("description", ""),
            passed=True,
            severity=severity,
            details=f"key {key!r} not found, rule skipped",
        )
    if ref_val is None:
        return ValidationResult(
            rule_id=rule_id,
            description=rule.get("description", ""),
            passed=True,
            severity=severity,
            details=f"reference key {ref_key!r} not found, rule skipped",
        )

    passed = ref_val in val

    if passed:
        details = f"{key}={val!r} contains {ref_key}={ref_val!r}: OK"
    else:
        details = f"{key}={val!r} does NOT contain {ref_key}={ref_val!r}"

    drift = None
    if not passed:
        drift = DriftResult(
            key=key,
            service=service or "unknown",
            source_a=f"rule:{rule_id}",
            value_a=val,
            source_b=f"expected-contains:{ref_key}",
            value_b=ref_val,
            severity=severity,
            rule_id=rule_id,
        )

    return ValidationResult(
        rule_id=rule_id,
        description=rule.get("description", ""),
        passed=passed,
        severity=severity,
        details=details,
        drift=drift,
    )


def _eval_dual_source(rule: dict, store) -> ValidationResult:
    """Dual-source consistency — delegates to drift_detector."""
    from checker.analysis.drift_detector import detect_cross_source

    rule_id = rule["id"]
    severity = rule.get("severity", "warning")

    all_snaps = store.all_snapshots()
    drifts = detect_cross_source(all_snaps, severity=severity)

    if not drifts:
        return ValidationResult(
            rule_id=rule_id,
            description=rule.get("description", ""),
            passed=True,
            severity=severity,
            details="all cross-source keys agree",
        )

    keys = sorted({d.key for d in drifts})
    return ValidationResult(
        rule_id=rule_id,
        description=rule.get("description", ""),
        passed=False,
        severity=severity,
        details=f"{len(drifts)} cross-source disagreement(s) on: {', '.join(keys)}",
        drift=drifts[0],  # attach the first one for reporting
    )


# ---------------------------------------------------------------------------
# Top-level evaluator
# ---------------------------------------------------------------------------

_EVALUATORS = {
    "constraint": _eval_constraint,
    "propagation": _eval_propagation,
}


def validate(rules: list[dict], store) -> list[ValidationResult]:
    """Evaluate all rules against the snapshot store.

    Parameters
    ----------
    rules
        List of rule dicts (as returned by ``load_rules()``).
    store
        A ``SnapshotStore`` (or anything with ``all_snapshots()``,
        ``snapshots_for_service()``, and snapshot objects with
        ``.properties`` dicts).

    Returns
    -------
    list[ValidationResult]
        One result per rule, in rule-definition order.
    """
    results: list[ValidationResult] = []
    for rule in rules:
        rule_type = rule.get("type", "unknown")
        evaluator = _EVALUATORS.get(rule_type)
        if evaluator is None:
            results.append(ValidationResult(
                rule_id=rule.get("id", "?"),
                description=rule.get("description", ""),
                passed=True,
                severity=rule.get("severity", "warning"),
                details=f"unknown rule type {rule_type!r}, skipped",
            ))
            continue
        try:
            result = evaluator(rule, store)
            results.append(result)
        except Exception as exc:
            logger.error("rule %s raised %s: %s", rule.get("id"), type(exc).__name__, exc)
            results.append(ValidationResult(
                rule_id=rule.get("id", "?"),
                description=rule.get("description", ""),
                passed=False,
                severity=rule.get("severity", "warning"),
                details=f"evaluation error: {exc}",
            ))
    return results


def validate_from_file(rule_path: str | Path, store) -> list[ValidationResult]:
    """Convenience: load rules from a file and validate."""
    rules = load_rules(rule_path)
    return validate(rules, store)
