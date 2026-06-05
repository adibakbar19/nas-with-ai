"""Unit tests for health endpoint and configuration.

Validates:
- Requirement 1.1: GET /health returns {"status": "ok", "service": "autocomplete-service"} with HTTP 200
- Requirement 2.1: OPENSEARCH_URL default and env var override
- Requirement 2.2: REDIS_URL default and env var override
- Requirement 2.5: DOMAIN_CONFIG contains "address" entry with correct fields
"""

from __future__ import annotations

import pytest

from config import DOMAIN_CONFIG


# ---------------------------------------------------------------------------
# Health endpoint tests (Requirement 1.1)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_health_returns_200(async_client):
    """GET /health returns HTTP 200."""
    response = await async_client.get("/health")
    assert response.status_code == 200


@pytest.mark.asyncio
async def test_health_returns_expected_json(async_client):
    """GET /health returns {"status": "ok", "service": "autocomplete-service"}."""
    response = await async_client.get("/health")
    data = response.json()
    assert data == {"status": "ok", "service": "autocomplete-service"}


@pytest.mark.asyncio
async def test_health_content_type_is_json(async_client):
    """GET /health response has application/json content type."""
    response = await async_client.get("/health")
    assert "application/json" in response.headers["content-type"]


# ---------------------------------------------------------------------------
# Settings defaults tests (Requirements 2.1, 2.2)
# ---------------------------------------------------------------------------


def test_settings_default_opensearch_url():
    """Settings.OPENSEARCH_URL defaults to http://opensearch:9200."""
    from config import Settings

    s = Settings()
    assert s.OPENSEARCH_URL == "http://opensearch:9200"


def test_settings_default_redis_url():
    """Settings.REDIS_URL defaults to redis://valkey:6379/0."""
    from config import Settings

    s = Settings()
    assert s.REDIS_URL == "redis://valkey:6379/0"


def test_settings_default_host():
    """Settings.HOST defaults to 0.0.0.0."""
    from config import Settings

    s = Settings()
    assert s.HOST == "0.0.0.0"


def test_settings_default_port():
    """Settings.PORT defaults to 8002."""
    from config import Settings

    s = Settings()
    assert s.PORT == 8002


def test_settings_default_cache_ttl():
    """Settings.CACHE_TTL_SECONDS defaults to 300."""
    from config import Settings

    s = Settings()
    assert s.CACHE_TTL_SECONDS == 300


# ---------------------------------------------------------------------------
# Settings env var override tests (Requirements 2.1, 2.2)
# ---------------------------------------------------------------------------


def test_settings_override_opensearch_url(monkeypatch):
    """OPENSEARCH_URL env var overrides the default."""
    import importlib

    import config

    monkeypatch.setenv("OPENSEARCH_URL", "http://custom-os:9201")
    importlib.reload(config)
    s = config.Settings()
    assert s.OPENSEARCH_URL == "http://custom-os:9201"


def test_settings_override_redis_url(monkeypatch):
    """REDIS_URL env var overrides the default."""
    import importlib

    import config

    monkeypatch.setenv("REDIS_URL", "redis://custom-redis:6380/1")
    importlib.reload(config)
    s = config.Settings()
    assert s.REDIS_URL == "redis://custom-redis:6380/1"


def test_settings_override_host(monkeypatch):
    """HOST env var overrides the default."""
    import importlib

    import config

    monkeypatch.setenv("HOST", "127.0.0.1")
    importlib.reload(config)
    s = config.Settings()
    assert s.HOST == "127.0.0.1"


def test_settings_override_port(monkeypatch):
    """PORT env var overrides the default."""
    import importlib

    import config

    monkeypatch.setenv("PORT", "9999")
    importlib.reload(config)
    s = config.Settings()
    assert s.PORT == 9999


def test_settings_override_cache_ttl(monkeypatch):
    """CACHE_TTL_SECONDS env var overrides the default."""
    import importlib

    import config

    monkeypatch.setenv("CACHE_TTL_SECONDS", "600")
    importlib.reload(config)
    s = config.Settings()
    assert s.CACHE_TTL_SECONDS == 600


# ---------------------------------------------------------------------------
# DOMAIN_CONFIG tests (Requirement 2.5)
# ---------------------------------------------------------------------------


def test_domain_config_contains_address():
    """DOMAIN_CONFIG has an 'address' entry."""
    assert "address" in DOMAIN_CONFIG


def test_domain_config_address_index():
    """Address domain uses index 'nas_addresses'."""
    assert DOMAIN_CONFIG["address"]["index"] == "nas_addresses"


def test_domain_config_address_query_type():
    """Address domain uses query_type 'bool_prefix'."""
    assert DOMAIN_CONFIG["address"]["query_type"] == "bool_prefix"


def test_domain_config_address_query_fields():
    """Address domain queries autocomplete search_as_you_type sub-fields."""
    expected_fields = [
        "autocomplete",
        "autocomplete._2gram",
        "autocomplete._3gram",
    ]
    assert DOMAIN_CONFIG["address"]["query_fields"] == expected_fields


def test_domain_config_address_result_field():
    """Address domain extracts 'address_clean' from hits."""
    assert DOMAIN_CONFIG["address"]["result_field"] == "address_clean"


def test_domain_config_address_has_all_required_keys():
    """Address domain entry contains all required config keys."""
    required_keys = {"index", "query_type", "query_fields", "result_field"}
    assert required_keys.issubset(DOMAIN_CONFIG["address"].keys())
