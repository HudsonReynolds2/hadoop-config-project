"""XML collector — reads Hadoop-style ``*-site.xml`` files.

Hadoop config XMLs have this canonical shape::

    <configuration>
      <property>
        <name>fs.defaultFS</name>
        <value>hdfs://namenode:8020</value>
      </property>
      ...
    </configuration>

The cluster at hand also contains files written with ``<n>`` in place of
``<name>`` — a non-standard but syntactically valid abbreviation that Hadoop
itself does not accept but which appears in the bind-mounted XMLs here. This
collector treats ``<name>`` and ``<n>`` as equivalent name tags so both file
styles parse identically.

No mocking, no I/O abstraction: this module opens a path and parses it. Tests
call it against the real files in ``conf/``.
"""

from __future__ import annotations

import os
import socket
import xml.etree.ElementTree as ET
from pathlib import Path

from checker.models import SOURCE_XML_FILE, ConfigSnapshot

# The two tag names we accept for the key element inside <property>. Order
# doesn't matter — we check for either. "name" is standard Hadoop; "n" is the
# variant that appears in this cluster's hdfs-site.xml.
_NAME_TAGS = ("name", "n")
_VALUE_TAG = "value"


class XmlCollectorError(Exception):
    """Raised when an XML file cannot be parsed into a snapshot."""


def _text_or_empty(elem: ET.Element | None) -> str:
    """Return stripped text of ``elem``, or '' if the element or its text is None.

    Hadoop XMLs routinely contain empty-value properties (``<value></value>``).
    These are meaningful — they represent an explicit "unset" — so we preserve
    them as the empty string rather than skipping the property.
    """
    if elem is None or elem.text is None:
        return ""
    return elem.text.strip()


def _extract_properties(root: ET.Element) -> dict[str, str]:
    """Walk <property> children of <configuration> and collect name→value.

    Properties without a recognizable name element are skipped silently;
    properties with a name but no <value> element map to the empty string.
    A property appearing more than once takes the last value seen, matching
    Hadoop's own last-wins resolution.
    """
    props: dict[str, str] = {}
    for prop in root.findall("property"):
        name_elem: ET.Element | None = None
        for tag in _NAME_TAGS:
            found = prop.find(tag)
            if found is not None:
                name_elem = found
                break
        if name_elem is None:
            continue
        key = _text_or_empty(name_elem)
        if not key:
            continue
        value = _text_or_empty(prop.find(_VALUE_TAG))
        props[key] = value
    return props


def _service_from_path(path: Path, service: str | None) -> str:
    """Best-effort service tag. Caller can always override via the argument.

    When no service is supplied the collector falls back to the environment
    variable ``CHECKER_SERVICE_NAME`` (set by the agent container) and, if
    that too is absent, to the string ``"unknown"``. The point is that a
    snapshot always has *some* service tag — downstream code need not defend
    against None here.
    """
    if service:
        return service
    env_service = os.environ.get("CHECKER_SERVICE_NAME")
    if env_service:
        return env_service
    return "unknown"


def _agent_id_for(service: str, path: Path) -> str:
    """Compose a stable agent_id from service name and file stem.

    Example: ``namenode`` + ``hdfs-site.xml`` -> ``namenode-hdfs-site``.
    The stem-only form (no extension) matches the example in plan.md.
    """
    return f"{service}-{path.stem}"


def collect_xml(
    path: str | os.PathLike,
    service: str | None = None,
    host: str | None = None,
) -> ConfigSnapshot:
    """Parse a single Hadoop ``*-site.xml`` and return a ConfigSnapshot.

    Parameters
    ----------
    path
        Filesystem path to the XML file.
    service
        Logical service name for the snapshot tag (e.g. ``"namenode"``). If
        omitted, ``CHECKER_SERVICE_NAME`` env var is consulted, then
        ``"unknown"``.
    host
        Host or container name. Defaults to ``socket.gethostname()``.
    """
    p = Path(path)
    if not p.is_file():
        raise XmlCollectorError(f"not a file: {p}")

    try:
        tree = ET.parse(p)
    except ET.ParseError as e:
        raise XmlCollectorError(f"malformed XML in {p}: {e}") from e

    root = tree.getroot()
    if root.tag != "configuration":
        raise XmlCollectorError(
            f"expected <configuration> root in {p}, got <{root.tag}>"
        )

    properties = _extract_properties(root)
    service_name = _service_from_path(p, service)
    host_name = host if host is not None else socket.gethostname()

    return ConfigSnapshot(
        agent_id=_agent_id_for(service_name, p),
        service=service_name,
        source=SOURCE_XML_FILE,
        source_path=str(p),
        host=host_name,
        properties=properties,
    )
