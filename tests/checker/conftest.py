"""Shared pytest fixtures for the checker test suite.

The test plan says: no mocking, parse real files. These fixtures expose the
real config files under ``tests/fixtures/`` so test modules don't have to
recompute the path from their own location.
"""

from __future__ import annotations

from pathlib import Path

import pytest


TESTS_DIR = Path(__file__).parent
FIXTURES_DIR = TESTS_DIR / "fixtures"
CONF_DIR = FIXTURES_DIR / "conf"


@pytest.fixture(scope="session")
def conf_dir() -> Path:
    """Absolute path to the bundled ``conf/`` fixture directory."""
    return CONF_DIR


@pytest.fixture(scope="session")
def fixtures_dir() -> Path:
    """Absolute path to the top-level ``tests/fixtures/`` directory."""
    return FIXTURES_DIR


@pytest.fixture(scope="session")
def hadoop_env_path() -> Path:
    """Absolute path to the bundled ``hadoop.env`` fixture."""
    return FIXTURES_DIR / "hadoop.env"
