"""Unit tests for configuration validation — invalid values.

Validates:
- Requirement 2.4: PORT must be an integer in range 1–65535
- Requirement 2.6: CACHE_TTL_SECONDS must be a positive integer
- Requirement 2.7: Non-integer numeric env vars cause startup failure with descriptive error
"""

from __future__ import annotations

import importlib
import sys

import pytest

# Ensure config module is loaded once with valid defaults during collection.
# Each test will then monkeypatch env vars and reload to trigger validation errors.
import config  # noqa: F401


# ---------------------------------------------------------------------------
# PORT validation tests (Requirements 2.4, 2.7)
# ---------------------------------------------------------------------------


def test_port_non_integer_raises_system_exit(monkeypatch):
    """Setting PORT to a non-integer string raises SystemExit mentioning PORT."""
    monkeypatch.setenv("PORT", "abc")

    with pytest.raises(SystemExit, match="PORT"):
        importlib.reload(config)


def test_port_non_integer_float_raises_system_exit(monkeypatch):
    """Setting PORT to a float string raises SystemExit mentioning PORT."""
    monkeypatch.setenv("PORT", "80.5")

    with pytest.raises(SystemExit, match="PORT"):
        importlib.reload(config)


def test_port_zero_raises_system_exit(monkeypatch):
    """Setting PORT to '0' (below range 1–65535) raises SystemExit."""
    monkeypatch.setenv("PORT", "0")

    with pytest.raises(SystemExit, match="PORT"):
        importlib.reload(config)


def test_port_above_max_raises_system_exit(monkeypatch):
    """Setting PORT to '70000' (above range 1–65535) raises SystemExit."""
    monkeypatch.setenv("PORT", "70000")

    with pytest.raises(SystemExit, match="PORT"):
        importlib.reload(config)


# ---------------------------------------------------------------------------
# CACHE_TTL_SECONDS validation tests (Requirements 2.6, 2.7)
# ---------------------------------------------------------------------------


def test_cache_ttl_non_integer_raises_system_exit(monkeypatch):
    """Setting CACHE_TTL_SECONDS to a non-integer string raises SystemExit mentioning CACHE_TTL_SECONDS."""
    monkeypatch.setenv("CACHE_TTL_SECONDS", "abc")

    with pytest.raises(SystemExit, match="CACHE_TTL_SECONDS"):
        importlib.reload(config)


def test_cache_ttl_zero_raises_system_exit(monkeypatch):
    """Setting CACHE_TTL_SECONDS to '0' (non-positive) raises SystemExit."""
    monkeypatch.setenv("CACHE_TTL_SECONDS", "0")

    with pytest.raises(SystemExit, match="CACHE_TTL_SECONDS"):
        importlib.reload(config)


def test_cache_ttl_negative_raises_system_exit(monkeypatch):
    """Setting CACHE_TTL_SECONDS to '-1' (non-positive) raises SystemExit."""
    monkeypatch.setenv("CACHE_TTL_SECONDS", "-1")

    with pytest.raises(SystemExit, match="CACHE_TTL_SECONDS"):
        importlib.reload(config)
