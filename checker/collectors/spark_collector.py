"""Spark collector — parses ``spark-defaults.conf`` style files.

``spark-defaults.conf`` is a whitespace-separated ``KEY VALUE`` file (with
``#`` comments and blank lines) used by ``spark-submit`` and the Spark
client. The parser splits on the *first* run of whitespace so values
containing spaces survive intact.

This sits alongside ``env_collector`` and ``xml_collector``: same return
type (``ConfigSnapshot``), same resolution rules for ``service`` and
``host``, same skip-on-missing-data convention.

The ``source`` tag on the resulting snapshot is ``SOURCE_SPARK_CONF``
(distinct from env-file and XML so the source-preference logic in the
validator can treat it as authoritative for Spark keys).
"""

from __future__ import annotations

import os
import re
import socket
from pathlib import Path

from checker.models import SOURCE_SPARK_CONF, ConfigSnapshot


class SparkCollectorError(Exception):
    """Raised when a spark-defaults file cannot be read or parsed."""


# Match the first run of whitespace separating key and value.
_WS_SPLIT_RE = re.compile(r"\s+", re.UNICODE)


def _parse_spark_line(line: str) -> tuple[str, str] | None:
    """Parse a single line of a spark-defaults.conf file.

    Returns ``(key, value)`` on success, ``None`` for comments, blank
    lines, or lines without a value. The first whitespace run separates
    key from value; the rest of the line (rstripped) is the value
    verbatim — Spark itself does no quoting or escaping in this file.
    """
    stripped = line.strip()
    if not stripped or stripped.startswith("#"):
        return None
    # Spark accepts inline comments? In practice no — the config reader
    # treats the entire post-whitespace remainder as the value. We match
    # that behaviour rather than guessing.
    parts = _WS_SPLIT_RE.split(stripped, maxsplit=1)
    if len(parts) != 2:
        # Bare key with no value — not meaningful in spark-defaults.
        return None
    key, value = parts[0], parts[1].rstrip()
    if not key:
        return None
    return key, value


def _parse_spark_properties(text: str) -> dict[str, str]:
    """Extract all KEY VALUE pairs from the body of a spark-defaults file.

    Last-wins on duplicate keys, matching Spark's own resolution.
    """
    props: dict[str, str] = {}
    for raw_line in text.splitlines():
        parsed = _parse_spark_line(raw_line)
        if parsed is None:
            continue
        key, value = parsed
        props[key] = value
    return props


def parse_spark_conf(
    path: str | os.PathLike,
    service: str | None = None,
    host: str | None = None,
) -> ConfigSnapshot:
    """Parse a spark-defaults.conf file into a ``ConfigSnapshot``.

    Resolution rules for ``service`` and ``host`` mirror the other
    collectors: argument > ``CHECKER_SERVICE_NAME`` env > ``"unknown"``
    for service; argument > ``socket.gethostname()`` for host.
    """
    p = Path(path)
    if not p.is_file():
        raise SparkCollectorError(f"not a file: {p}")

    try:
        text = p.read_text(encoding="utf-8")
    except OSError as e:
        raise SparkCollectorError(f"cannot read {p}: {e}") from e

    properties = _parse_spark_properties(text)
    service_name = service or os.environ.get("CHECKER_SERVICE_NAME") or "unknown"
    host_name = host if host is not None else socket.gethostname()

    return ConfigSnapshot(
        agent_id=f"{service_name}-{p.stem}",
        service=service_name,
        source=SOURCE_SPARK_CONF,
        source_path=str(p),
        host=host_name,
        properties=properties,
    )
