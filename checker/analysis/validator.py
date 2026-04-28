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

**Stage 2 source-preference layer (added 2026-04-28):**
Each agent now publishes both an XML snapshot and an env-file snapshot for
the same service.  When those two disagree (e.g. mid-mutation, or because
hadoop.env still holds the old value while core-site.xml has been
edited), an unscoped lookup would return whichever snapshot happened to be
iterated first — non-deterministic, and the source of the test 08/09
failures.  ``_find_key`` now accepts a ``prefer_source`` argument so
callers can prefer ``xml_file`` (the cluster's authoritative source for
*-site.xml keys) over ``env_file``/``jvm_flags``.  Multi-service
propagation rules use ``prefer_source="xml_file"`` so that they compare
XML to XML across services rather than mixing sources.

**Stage 2 status field (added 2026-04-28):**
``ValidationResult`` now carries a ``status`` field with values
``"pass" | "fail" | "skip"``.  ``passed=True`` is now ambiguous on its own
(could be pass-because-evaluated or pass-because-skipped); ``status``
disambiguates.  Existing assertions on ``passed`` are kept working for
backwards compatibility.
"""

from __future__ import annotations

import dataclasses
import json
import logging
import operator
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import urlparse

from checker.models import (
    DriftResult,
    SOURCE_ENV_FILE,
    SOURCE_JVM_FLAGS,
    SOURCE_XML_FILE,
)

logger = logging.getLogger("checker.validator")


# ---------------------------------------------------------------------------
# ValidationResult — one per rule evaluation
# ---------------------------------------------------------------------------


# Status values for ValidationResult.status.
# - PASS: rule was evaluated and the constraint held.
# - FAIL: rule was evaluated and the constraint was violated.
# - SKIP: rule could not be evaluated (e.g. a referenced key is missing).
STATUS_PASS = "pass"
STATUS_FAIL = "fail"
STATUS_SKIP = "skip"


@dataclass
class ValidationResult:
    """The outcome of evaluating a single rule against the store."""

    rule_id: str
    description: str
    passed: bool
    severity: str
    details: str = ""
    drift: DriftResult | None = None
    # Stage 2: explicit status disambiguates skip from pass.
    # Default ``pass`` so legacy code paths that don't set it are correct
    # when ``passed=True``; evaluators set this explicitly throughout.
    status: str = STATUS_PASS

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


# Default source preference order: XML wins over env-file wins over
# jvm-flags. XML is the authoritative *-site.xml on disk; env-file is
# typically a docker entrypoint convenience; jvm-flags are last because
# they may or may not be passed through to the running process depending
# on the service.
_DEFAULT_SOURCE_ORDER = (SOURCE_XML_FILE, SOURCE_ENV_FILE, SOURCE_JVM_FLAGS)


def _find_key(
    store,
    key: str,
    preferred_service: str | None = None,
    prefer_source: str | None = None,
) -> str | None:
    """Find a config key's value across all snapshots in the store.

    Lookup order:
      1. If ``preferred_service`` is set, search snapshots tagged with that
         service first. Within that service, snapshots whose ``source``
         matches ``prefer_source`` (or the default XML > env > jvm order
         when ``prefer_source`` is None) are preferred.
      2. Fall back to all snapshots in the store, with the same source
         preference applied.

    Returns None if the key is not found anywhere.

    Source preference exists because each agent publishes both an XML
    snapshot and an env-file snapshot for the same service; when those
    disagree, an unscoped first-match lookup is non-deterministic.
    """
    # Build the source priority tuple. If the caller specifies a preferred
    # source, it sits at the head of the priority list.
    source_order: tuple[str, ...]
    if prefer_source is not None:
        source_order = (prefer_source,) + tuple(
            s for s in _DEFAULT_SOURCE_ORDER if s != prefer_source
        )
    else:
        source_order = _DEFAULT_SOURCE_ORDER

    def _best_in(snaps: list) -> str | None:
        # Bucket by source and return the highest-priority bucket's value.
        buckets: dict[str, str] = {}
        for snap in snaps:
            if key in snap.properties and snap.source not in buckets:
                buckets[snap.source] = snap.properties[key]
        for src in source_order:
            if src in buckets:
                return buckets[src]
        # Fallback: any source not in our priority list (e.g. custom).
        if buckets:
            return next(iter(buckets.values()))
        return None

    # First pass: preferred service.
    if preferred_service:
        val = _best_in(store.snapshots_for_service(preferred_service))
        if val is not None:
            return val

    # Second pass: any service.
    return _best_in(store.all_snapshots())


def _find_key_by_service(
    store,
    key: str,
    service: str,
    prefer_source: str | None = None,
) -> str | None:
    """Find a key's value specifically from snapshots tagged with ``service``.

    Falls back to searching all snapshots if the service has no match.
    """
    return _find_key(
        store, key, preferred_service=service, prefer_source=prefer_source
    )


def _find_key_strict_service(
    store,
    key: str,
    service: str,
    prefer_source: str | None = None,
) -> str | None:
    """Find a key's value from snapshots tagged with ``service`` ONLY.

    No fallback to other services. Used by multi-service propagation rules
    where mixing services would defeat the rule's purpose (we want to
    detect when service A and service B disagree, so a fallback that
    silently returns service A's value for service B would mask the bug).
    """
    snaps = store.snapshots_for_service(service)
    if not snaps:
        return None
    # Reuse the source-preference logic by limiting lookup to one service.
    source_order: tuple[str, ...]
    if prefer_source is not None:
        source_order = (prefer_source,) + tuple(
            s for s in _DEFAULT_SOURCE_ORDER if s != prefer_source
        )
    else:
        source_order = _DEFAULT_SOURCE_ORDER

    buckets: dict[str, str] = {}
    for snap in snaps:
        if key in snap.properties and snap.source not in buckets:
            buckets[snap.source] = snap.properties[key]
    for src in source_order:
        if src in buckets:
            return buckets[src]
    if buckets:
        return next(iter(buckets.values()))
    return None


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

    # Prefer XML for both sides — bind-mounted *-site.xml is the
    # authoritative source for these constraint keys, and we want both
    # halves of the comparison to reflect the same on-disk reality.
    val_str = _find_key(
        store, key, preferred_service=service, prefer_source=SOURCE_XML_FILE
    )
    target_str = _find_key(
        store,
        target_key,
        preferred_service=target_service,
        prefer_source=SOURCE_XML_FILE,
    )

    # If either key is missing, we can't evaluate the rule.
    if val_str is None:
        return ValidationResult(
            rule_id=rule_id,
            description=rule.get("description", ""),
            passed=True,  # can't fail what we can't check
            severity=severity,
            details=f"key {key!r} not found in store, rule skipped",
            status=STATUS_SKIP,
        )
    if target_str is None:
        return ValidationResult(
            rule_id=rule_id,
            description=rule.get("description", ""),
            passed=True,
            severity=severity,
            details=f"target key {target_key!r} not found in store, rule skipped",
            status=STATUS_SKIP,
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
            status=STATUS_FAIL,
        )

    op_func = _RELATIONS.get(relation)
    if op_func is None:
        return ValidationResult(
            rule_id=rule_id,
            description=rule.get("description", ""),
            passed=False,
            severity=severity,
            details=f"unknown relation {relation!r}",
            status=STATUS_FAIL,
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
        status=STATUS_PASS if passed else STATUS_FAIL,
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
        status=STATUS_SKIP,
    )


def _eval_propagation_multi(rule: dict, store) -> ValidationResult:
    """fs.defaultFS must be identical across all listed services.

    Each service is looked up STRICTLY (no fallback to other services'
    snapshots) because a fallback would silently return service A's value
    when checking service B, masking the very disagreement the rule
    exists to find.

    However, if a service has no snapshots in the store at all (e.g. its
    agent isn't running), we DO fall back — that's a "rule cannot
    evaluate this service" condition, not a "disagreement" condition,
    and the existing test contract (test_single_service_present_still_passes
    _via_fallback in test_validator.py) requires the fallback path. So
    the rule here is: strict for services that ARE publishing, fallback
    only for services that aren't.
    """
    rule_id = rule["id"]
    key = rule["key"]
    services = rule["services"]
    severity = rule.get("severity", "warning")

    # values_per_service[svc] = (value, source_used, was_strict)
    # was_strict tracks whether we got the value from a snapshot tagged
    # with this exact service (True) or via fallback (False). Disagreement
    # detection only considers strict matches; fallback matches are used
    # only as a "best guess" for reporting and to preserve the legacy
    # "single service in store passes via fallback" behaviour.
    strict_values: dict[str, str] = {}
    fallback_values: dict[str, str] = {}

    for svc in services:
        strict = _find_key_strict_service(
            store, key, svc, prefer_source=SOURCE_XML_FILE
        )
        if strict is not None:
            strict_values[svc] = strict
        else:
            fb = _find_key(
                store, key, preferred_service=svc, prefer_source=SOURCE_XML_FILE
            )
            if fb is not None:
                fallback_values[svc] = fb

    # If fewer than 2 services contributed values total, nothing to compare.
    total_known = len(strict_values) + len(fallback_values)
    if total_known <= 1:
        found = list(strict_values.keys()) + list(fallback_values.keys())
        return ValidationResult(
            rule_id=rule_id,
            description=rule.get("description", ""),
            passed=True,
            severity=severity,
            details=(
                f"{key!r} found in {total_known} of {len(services)} "
                f"services ({found}), nothing to compare"
            ),
            status=STATUS_SKIP,
        )

    # Compare strict values amongst themselves first — these are the only
    # values we trust for detecting genuine disagreement.
    if len(strict_values) >= 2:
        unique_strict = set(strict_values.values())
        if len(unique_strict) == 1:
            # All strict-publishing services agree. Sanity-check fallbacks
            # match too, but don't fail on a fallback mismatch (it's noise
            # from a service that isn't publishing).
            agreed = next(iter(unique_strict))
            all_services = list(strict_values.keys()) + list(fallback_values.keys())
            return ValidationResult(
                rule_id=rule_id,
                description=rule.get("description", ""),
                passed=True,
                severity=severity,
                details=f"{key!r} agrees across {all_services}: {agreed!r}",
                status=STATUS_PASS,
            )

        # Disagreement — find the first pair that actually differs and
        # report that pair. Reporting svcs[0] vs svcs[1] blindly was the
        # bug behind test 08: when those two happened to agree but a third
        # service disagreed, the JSON report showed value_a == value_b.
        svc_list = list(strict_values.keys())
        first_a = svc_list[0]
        val_a = strict_values[first_a]
        first_b = None
        val_b = None
        for svc in svc_list[1:]:
            if strict_values[svc] != val_a:
                first_b = svc
                val_b = strict_values[svc]
                break
        if first_b is None:
            # Defensive: unique_strict had >=2 entries but we couldn't
            # find a disagreeing pair. Shouldn't happen, but fall back to
            # svcs[0]/svcs[1].
            first_b = svc_list[1]
            val_b = strict_values[first_b]

        pairs = ", ".join(f"{svc}={v!r}" for svc, v in strict_values.items())
        details = f"{key!r} disagrees: {pairs}"

        drift = DriftResult(
            key=key,
            service=first_a,
            source_a=f"service:{first_a}",
            value_a=val_a,
            source_b=f"service:{first_b}",
            value_b=val_b,
            severity=severity,
            rule_id=rule_id,
        )

        return ValidationResult(
            rule_id=rule_id,
            description=rule.get("description", ""),
            passed=False,
            severity=severity,
            details=details,
            drift=drift,
            status=STATUS_FAIL,
        )

    # Only one service is strictly publishing; the rest fell back. This
    # matches the legacy "single service in store via fallback" path:
    # every fallback resolves to the same single source, so trivially
    # agrees. Pass with a note that explains why.
    all_values = {**fallback_values, **strict_values}
    unique = set(all_values.values())
    if len(unique) == 1:
        return ValidationResult(
            rule_id=rule_id,
            description=rule.get("description", ""),
            passed=True,
            severity=severity,
            details=(
                f"{key!r} agrees across {list(all_values.keys())}: "
                f"{next(iter(unique))!r} (only {list(strict_values.keys())} "
                "strictly publishing, others via fallback)"
            ),
            status=STATUS_PASS,
        )

    # Strict service disagrees with a fallback — possible but unusual,
    # report it as a disagreement.
    svc_a = next(iter(strict_values))
    val_a = strict_values[svc_a]
    svc_b = next(s for s, v in fallback_values.items() if v != val_a)
    val_b = fallback_values[svc_b]
    pairs = ", ".join(f"{svc}={v!r}" for svc, v in all_values.items())
    drift = DriftResult(
        key=key,
        service=svc_a,
        source_a=f"service:{svc_a}",
        value_a=val_a,
        source_b=f"service:{svc_b}(fallback)",
        value_b=val_b,
        severity=severity,
        rule_id=rule_id,
    )
    return ValidationResult(
        rule_id=rule_id,
        description=rule.get("description", ""),
        passed=False,
        severity=severity,
        details=f"{key!r} disagrees: {pairs}",
        drift=drift,
        status=STATUS_FAIL,
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

    val = (
        _find_key_by_service(store, key, service, prefer_source=SOURCE_XML_FILE)
        if service
        else _find_key(store, key, prefer_source=SOURCE_XML_FILE)
    )
    target_val = (
        _find_key_by_service(
            store, target_key, target_service, prefer_source=SOURCE_XML_FILE
        )
        if target_service
        else _find_key(store, target_key, prefer_source=SOURCE_XML_FILE)
    )

    if val is None:
        return ValidationResult(
            rule_id=rule_id,
            description=rule.get("description", ""),
            passed=True,
            severity=severity,
            details=f"key {key!r} (service={service}) not found, rule skipped",
            status=STATUS_SKIP,
        )
    if target_val is None:
        return ValidationResult(
            rule_id=rule_id,
            description=rule.get("description", ""),
            passed=True,
            severity=severity,
            details=f"target key {target_key!r} (service={target_service}) not found, rule skipped",
            status=STATUS_SKIP,
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
        status=STATUS_PASS if passed else STATUS_FAIL,
    )


def _values_url_match(haystack: str, needle: str) -> bool:
    """Return True iff ``haystack`` references the same authority as ``needle``.

    Used by ``_eval_must_contain`` (Stage 2.1 fix). The previous behaviour
    was a naked ``needle in haystack`` substring match, which incorrectly
    accepted ``hdfs://notnamenode:8020`` as containing
    ``hdfs://namenode:8020`` (the longer string contains the shorter one
    as a tail substring).

    Rules:
      1. If both values parse as URLs with non-empty netlocs, compare the
         netloc (host:port) for equality.
      2. Otherwise fall back to substring containment (legacy behaviour),
         so non-URL must-contain rules — if any are added later — still
         work.
    """
    needle_url = urlparse(needle)
    haystack_url = urlparse(haystack)
    if needle_url.netloc and haystack_url.netloc:
        # URL-authority comparison: scheme need not match (some tools
        # mix file:// vs hdfs://) but host:port must.
        return needle_url.netloc == haystack_url.netloc
    return needle in haystack


def _eval_must_contain(rule: dict, store) -> ValidationResult:
    """hive.metastore.warehouse.dir must contain the value of fs.defaultFS.

    Stage 2.1: the comparison is now URL-authority-based when both sides
    parse as URLs, fixing the false-positive on ``hdfs://notnamenode:8020``
    vs ``hdfs://namenode:8020``. See ``_values_url_match``.
    """
    rule_id = rule["id"]
    key = rule["key"]
    service = rule.get("service")
    severity = rule.get("severity", "warning")
    ref = rule["must_contain_value_of"]
    ref_key = ref["key"]
    ref_service = ref.get("service")

    val = _find_key(
        store, key, preferred_service=service, prefer_source=SOURCE_XML_FILE
    )
    ref_val = _find_key(
        store, ref_key, preferred_service=ref_service, prefer_source=SOURCE_XML_FILE
    )

    if val is None:
        return ValidationResult(
            rule_id=rule_id,
            description=rule.get("description", ""),
            passed=True,
            severity=severity,
            details=f"key {key!r} not found, rule skipped",
            status=STATUS_SKIP,
        )
    if ref_val is None:
        return ValidationResult(
            rule_id=rule_id,
            description=rule.get("description", ""),
            passed=True,
            severity=severity,
            details=f"reference key {ref_key!r} not found, rule skipped",
            status=STATUS_SKIP,
        )

    passed = _values_url_match(val, ref_val)

    if passed:
        details = f"{key}={val!r} matches {ref_key}={ref_val!r}: OK"
    else:
        details = f"{key}={val!r} does NOT match {ref_key}={ref_val!r}"

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
        status=STATUS_PASS if passed else STATUS_FAIL,
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
            status=STATUS_PASS,
        )

    keys = sorted({d.key for d in drifts})
    return ValidationResult(
        rule_id=rule_id,
        description=rule.get("description", ""),
        passed=False,
        severity=severity,
        details=f"{len(drifts)} cross-source disagreement(s) on: {', '.join(keys)}",
        drift=drifts[0],  # attach the first one for reporting
        status=STATUS_FAIL,
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
                status=STATUS_SKIP,
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
                status=STATUS_FAIL,
            ))
    return results


def validate_from_file(rule_path: str | Path, store) -> list[ValidationResult]:
    """Convenience: load rules from a file and validate."""
    rules = load_rules(rule_path)
    return validate(rules, store)
