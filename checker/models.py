"""Data model for the hadoop-config-checker.

The model is intentionally flat and JSON-friendly: every field on every
dataclass is either a primitive, a string enum value, or a dict of primitives.
This keeps serialization trivial and avoids pulling in pydantic as a
dependency. See plan.md §"Data model".
"""

from __future__ import annotations

import dataclasses
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum


class EdgeType(Enum):
    """Edge categories for the causality graph (Phase 5).

    CONSTRAINT  — hard rule. Violation is a definite bug.
    INFLUENCE   — soft relationship. Violation is a probable symptom only.
    PROPAGATION — value must agree across a service boundary.
    """

    CONSTRAINT = "constraint"
    INFLUENCE = "influence"
    PROPAGATION = "propagation"


# Canonical values for ConfigSnapshot.source. Strings, not an enum, because
# the plan specifies the source field as a string and because treating it as
# an open set makes it trivial to add new collector types later without
# touching consumers that just forward the value.
SOURCE_XML_FILE = "xml_file"
SOURCE_ENV_FILE = "env_file"
SOURCE_JVM_FLAGS = "jvm_flags"


def _utc_now_iso() -> str:
    """Return the current UTC time as an ISO 8601 string with a 'Z' suffix."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


@dataclass
class ConfigSnapshot:
    """A single collector's view of one config source at one point in time.

    One snapshot corresponds to one source (one XML file, one env file, or one
    JVM flags string). A single service typically emits several snapshots —
    e.g. the namenode emits one each for core-site.xml, hdfs-site.xml, and
    hadoop.env.

    agent_id is unique per (service, source) pair and is what the consumer
    keys its SnapshotStore on.
    """

    agent_id: str
    service: str
    source: str
    source_path: str
    host: str
    timestamp: str = field(default_factory=_utc_now_iso)
    properties: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return dataclasses.asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), sort_keys=True)

    @classmethod
    def from_dict(cls, d: dict) -> "ConfigSnapshot":
        return cls(**d)


@dataclass
class DriftResult:
    """One observed disagreement between two sources for a single key.

    source_a and source_b are human-readable descriptors in the form
    "<source_type>:<source_path>" (e.g. "xml_file:conf/yarn-site.xml"). Either
    value may be None, meaning the key is present in one source but absent in
    the other. rule_id is set when the drift was detected by a named rule
    rather than by a raw pairwise diff.
    """

    key: str
    service: str
    source_a: str
    value_a: str | None
    source_b: str
    value_b: str | None
    severity: str
    rule_id: str | None = None

    def to_dict(self) -> dict:
        return dataclasses.asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), sort_keys=True)


@dataclass
class RootCause:
    """A drift result enriched with its downstream effects from the graph.

    downstream_effects is a list of human-readable descriptors of what else
    is affected by this key — populated by CausalityGraph.trace() in Phase 5.
    For Phase 1 this type is defined but not constructed anywhere.
    """

    key: str
    service: str
    drift: DriftResult
    downstream_effects: list[str] = field(default_factory=list)
    severity: str = "warning"

    def to_dict(self) -> dict:
        return dataclasses.asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), sort_keys=True)
