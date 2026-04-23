"""Environment collector — parses ``KEY=VALUE`` env files and JVM -D flag strings.

Two input formats, two parsers:

``parse_env_file(path)``
    Reads a shell-style environment file (``hadoop.env`` is the canonical
    target). Accepts ``KEY=VALUE`` lines, comments, blank lines, and the
    optional leading ``export`` keyword. Single- and double-quoted values are
    unquoted. Multiline values, command substitution, variable interpolation,
    and heredocs are deliberately **not** supported — this is a config
    inspector, not a shell.

    By default, keys that follow the apache/hadoop image's
    ``<FILENAME>.XML_<key>`` convention (e.g. ``CORE-SITE.XML_fs.defaultFS``)
    are normalised to the bare config key (``fs.defaultFS``). This makes env
    values directly comparable to XML values of the same key — which is the
    whole point of the dual-source drift check. The original file name is
    already captured in ``source_path``, so no provenance is lost. Pass
    ``normalize_hadoop_env=False`` to disable the stripping.

``parse_jvm_flags(flags_str)``
    Reads a whitespace-separated string of ``-Dkey=value`` tokens, as found
    in the ``SERVICE_OPTS`` / ``HADOOP_OPTS`` / ``HIVE_SERVER2_OPTS`` env
    vars. Non-``-D`` tokens are ignored (so a mixed string like
    ``-Xmx2g -Dfs.defaultFS=hdfs://nn:8020`` parses correctly). Values
    containing ``=`` are preserved — only the first ``=`` separates key from
    value.

Both return a ``ConfigSnapshot`` with the appropriate ``source`` tag.
"""

from __future__ import annotations

import os
import re
import shlex
import socket
from pathlib import Path

from checker.models import SOURCE_ENV_FILE, SOURCE_JVM_FLAGS, ConfigSnapshot


class EnvCollectorError(Exception):
    """Raised when an env file cannot be read or parsed."""


# The apache/hadoop:3.3.6 image encodes config keys in env vars using a
# ``<FILE>_<key>`` convention, e.g. ``CORE-SITE.XML_fs.defaultFS``. The image
# reads these at start-up and writes them into the corresponding XML file. For
# this tool to compare env values against XML values by the same key name we
# strip the prefix when normalising. Recognised file tags are the four Hadoop
# XMLs the stack bind-mounts; anything else we leave alone.
_HADOOP_ENV_PREFIX_RE = re.compile(
    r"^(?:CORE-SITE|HDFS-SITE|YARN-SITE|MAPRED-SITE|HTTPFS-SITE|"
    r"KMS-SITE|KMS-ACLS)\.XML_(.+)$"
)


# ---------------------------------------------------------------------------
# env-file format
# ---------------------------------------------------------------------------


def _strip_inline_quotes(value: str) -> str:
    """Remove a single matched pair of surrounding quotes, if present.

    ``"hdfs://nn:8020"`` -> ``hdfs://nn:8020``. A mismatched or unpaired quote
    is left alone — the goal is to be forgiving, not to validate shell syntax.
    """
    if len(value) >= 2 and value[0] == value[-1] and value[0] in ('"', "'"):
        return value[1:-1]
    return value


def _parse_env_line(line: str) -> tuple[str, str] | None:
    """Parse a single line of an env file.

    Returns ``(key, value)`` on success, ``None`` for comments, blank lines,
    or lines that do not contain ``=``. Lines beginning with ``export`` are
    supported by stripping the keyword before the split.
    """
    stripped = line.strip()
    if not stripped or stripped.startswith("#"):
        return None
    # Accept `export KEY=VALUE` as well as plain `KEY=VALUE`.
    if stripped.startswith("export ") or stripped.startswith("export\t"):
        stripped = stripped[len("export ") :].lstrip()
    if "=" not in stripped:
        return None
    key, _, value = stripped.partition("=")
    key = key.strip()
    if not key:
        return None
    # Values may be quoted. Do not evaluate escape sequences — configs are
    # not shell-interpolated in this tool's view of the world.
    value = _strip_inline_quotes(value.strip())
    return key, value


def _parse_env_properties(text: str, normalize_hadoop_env: bool = True) -> dict[str, str]:
    """Extract all KEY=VALUE pairs from the body of an env file.

    When ``normalize_hadoop_env`` is true (the default), keys matching the
    apache/hadoop image's ``<FILENAME>.XML_<key>`` pattern are rewritten to
    the bare config key — see module docstring. Normalisation happens after
    parsing so an unprefixed key of the same name present alongside a
    prefixed one still takes last-wins precedence in source order.
    """
    props: dict[str, str] = {}
    for raw_line in text.splitlines():
        parsed = _parse_env_line(raw_line)
        if parsed is None:
            continue
        key, value = parsed
        if normalize_hadoop_env:
            m = _HADOOP_ENV_PREFIX_RE.match(key)
            if m:
                key = m.group(1)
        props[key] = value
    return props


def parse_env_file(
    path: str | os.PathLike,
    service: str | None = None,
    host: str | None = None,
    normalize_hadoop_env: bool = True,
) -> ConfigSnapshot:
    """Parse a KEY=VALUE env file (e.g. ``hadoop.env``) into a ConfigSnapshot.

    The same resolution rules as the XML collector apply to ``service`` and
    ``host``: argument, then ``CHECKER_SERVICE_NAME``, then ``"unknown"`` /
    ``socket.gethostname()``.

    ``normalize_hadoop_env`` controls whether the apache/hadoop
    ``<FILENAME>.XML_<key>`` prefix is stripped from keys. Defaults to True,
    which is what you want for the real hadoop.env in this cluster. Pass
    False for generic env files where the keys are not meant to map to XML.
    """
    p = Path(path)
    if not p.is_file():
        raise EnvCollectorError(f"not a file: {p}")

    try:
        text = p.read_text(encoding="utf-8")
    except OSError as e:
        raise EnvCollectorError(f"cannot read {p}: {e}") from e

    properties = _parse_env_properties(text, normalize_hadoop_env=normalize_hadoop_env)
    service_name = service or os.environ.get("CHECKER_SERVICE_NAME") or "unknown"
    host_name = host if host is not None else socket.gethostname()

    return ConfigSnapshot(
        agent_id=f"{service_name}-{p.stem}",
        service=service_name,
        source=SOURCE_ENV_FILE,
        source_path=str(p),
        host=host_name,
        properties=properties,
    )


# ---------------------------------------------------------------------------
# JVM -Dkey=value flags
# ---------------------------------------------------------------------------


def _parse_jvm_properties(flags_str: str) -> dict[str, str]:
    """Extract all ``-Dkey=value`` pairs from a JVM flags string.

    Tokens without a ``-D`` prefix are ignored. A bare ``-Dkey`` (no ``=``)
    maps to the empty string, matching JVM semantics of ``-Dkey`` setting an
    empty property. When a key appears more than once the last value wins —
    the order the JVM itself would apply.

    Tokenization uses shlex in POSIX mode so quoted values with embedded
    spaces are handled correctly.
    """
    props: dict[str, str] = {}
    if not flags_str or not flags_str.strip():
        return props
    try:
        tokens = shlex.split(flags_str, posix=True)
    except ValueError:
        # Unmatched quote, etc. Fall back to a whitespace split so we still
        # extract whatever well-formed -D tokens we can.
        tokens = flags_str.split()

    for tok in tokens:
        if not tok.startswith("-D"):
            continue
        body = tok[2:]
        if not body:
            continue
        key, sep, value = body.partition("=")
        if not key:
            continue
        props[key] = value if sep else ""
    return props


def parse_jvm_flags(
    flags_str: str,
    service: str | None = None,
    source_path: str = "jvm_flags",
    host: str | None = None,
) -> ConfigSnapshot:
    """Parse a JVM flags string (e.g. ``HIVE_SERVER2_OPTS``) into a snapshot.

    ``source_path`` is a human-readable tag describing where the flag string
    came from — typically the env-var name like ``"SERVICE_OPTS"`` or
    ``"HIVE_SERVER2_OPTS"``. It is carried through verbatim to the snapshot
    so the consumer can cite it in drift reports.
    """
    properties = _parse_jvm_properties(flags_str)
    service_name = service or os.environ.get("CHECKER_SERVICE_NAME") or "unknown"
    host_name = host if host is not None else socket.gethostname()

    # Derive a stable, filesystem-safe id fragment from source_path.
    id_frag = source_path.replace("/", "_").replace(" ", "_")

    return ConfigSnapshot(
        agent_id=f"{service_name}-{id_frag}",
        service=service_name,
        source=SOURCE_JVM_FLAGS,
        source_path=source_path,
        host=host_name,
        properties=properties,
    )
