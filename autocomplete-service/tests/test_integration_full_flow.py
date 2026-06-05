"""Integration tests — full request flow with mocked backends.

Validates: Requirements 3.2, 5.1, 5.2

Tests the complete lifecycle:
- Cache miss → OpenSearch query → cache write → subsequent cache hit
- Empty results are cached (prevent repeated misses)
- Different limit values produce different cache entries
"""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, patch

import pytest

from app import app


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def make_in_memory_redis() -> AsyncMock:
    """Create a mock Redis client backed by an in-memory dict.

    Simulates real Redis GET/SETEX behavior so that values written
    via setex can be retrieved via get.
    """
    cache_store: dict[str, str] = {}

    async def fake_get(key: str):
        return cache_store.get(key)

    async def fake_setex(key: str, ttl: int, value: str):
        cache_store[key] = value
        return True

    mock_redis = AsyncMock()
    mock_redis.get = AsyncMock(side_effect=fake_get)
    mock_redis.setex = AsyncMock(side_effect=fake_setex)
    mock_redis.aclose = AsyncMock()
    mock_redis._store = cache_store  # Expose for assertions
    return mock_redis


# ---------------------------------------------------------------------------
# Integration: cache miss → OpenSearch → cache write → cache hit
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
class TestFullRequestFlowCacheMissAndHit:
    """Verify the full lifecycle: miss → query → write → hit."""

    async def test_first_request_is_cache_miss_triggers_opensearch(self, async_client):
        """First request should miss cache, call OpenSearch, and write to cache."""
        mock_redis = make_in_memory_redis()
        app.state.redis = mock_redis

        mock_results = ["Jalan Ampang, KL", "Jalan Alor, KL"]

        with patch("app.query_opensearch", new_callable=AsyncMock) as mock_os:
            mock_os.return_value = mock_results

            response = await async_client.get(
                "/autocomplete",
                params={"q": "jalan", "domain": "address", "limit": 10},
            )

        assert response.status_code == 200
        assert response.json() == {"suggestions": mock_results}

        # OpenSearch was called (cache miss)
        mock_os.assert_called_once()

        # Cache write happened
        mock_redis.setex.assert_called_once()
        written_key = mock_redis.setex.call_args[0][0]
        assert written_key == "ac:address:jalan:10"

    async def test_second_request_hits_cache_skips_opensearch(self, async_client):
        """Second identical request should hit cache without calling OpenSearch."""
        mock_redis = make_in_memory_redis()
        app.state.redis = mock_redis

        mock_results = ["Jalan Ampang, KL", "Jalan Alor, KL"]

        with patch("app.query_opensearch", new_callable=AsyncMock) as mock_os:
            mock_os.return_value = mock_results

            # First request: cache miss → OpenSearch → cache write
            r1 = await async_client.get(
                "/autocomplete",
                params={"q": "jalan", "domain": "address", "limit": 10},
            )
            assert r1.status_code == 200
            assert r1.json() == {"suggestions": mock_results}
            assert mock_os.call_count == 1

            # Second request: same params → cache hit → no OpenSearch call
            r2 = await async_client.get(
                "/autocomplete",
                params={"q": "jalan", "domain": "address", "limit": 10},
            )
            assert r2.status_code == 200
            assert r2.json() == {"suggestions": mock_results}
            # OpenSearch was NOT called again
            assert mock_os.call_count == 1

    async def test_cache_hit_returns_same_data_as_original(self, async_client):
        """Data returned from cache hit matches the originally cached data."""
        mock_redis = make_in_memory_redis()
        app.state.redis = mock_redis

        mock_results = ["123 Main St", "456 Oak Ave", "789 Pine Rd"]

        with patch("app.query_opensearch", new_callable=AsyncMock) as mock_os:
            mock_os.return_value = mock_results

            # First request populates the cache
            r1 = await async_client.get(
                "/autocomplete",
                params={"q": "main", "domain": "address", "limit": 10},
            )

            # Second request reads from cache
            r2 = await async_client.get(
                "/autocomplete",
                params={"q": "main", "domain": "address", "limit": 10},
            )

        # Both responses are identical
        assert r1.json() == r2.json() == {"suggestions": mock_results}


# ---------------------------------------------------------------------------
# Integration: empty results are cached (Requirement 5.2)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
class TestEmptyResultsCaching:
    """Verify that empty results from OpenSearch are cached to prevent repeated misses."""

    async def test_empty_results_are_cached(self, async_client):
        """Empty list from OpenSearch should be written to cache."""
        mock_redis = make_in_memory_redis()
        app.state.redis = mock_redis

        with patch("app.query_opensearch", new_callable=AsyncMock) as mock_os:
            mock_os.return_value = []  # No results from OpenSearch

            response = await async_client.get(
                "/autocomplete",
                params={"q": "zzzznonexistent", "domain": "address", "limit": 10},
            )

        assert response.status_code == 200
        assert response.json() == {"suggestions": []}

        # Cache write was called even for empty results
        mock_redis.setex.assert_called_once()
        written_key = mock_redis.setex.call_args[0][0]
        written_value = mock_redis.setex.call_args[0][2]

        assert written_key == "ac:address:zzzznonexistent:10"
        assert json.loads(written_value) == []

    async def test_subsequent_request_for_empty_results_hits_cache(self, async_client):
        """After caching empty results, subsequent requests should not query OpenSearch."""
        mock_redis = make_in_memory_redis()
        app.state.redis = mock_redis

        with patch("app.query_opensearch", new_callable=AsyncMock) as mock_os:
            mock_os.return_value = []

            # First request: miss → OpenSearch returns [] → cache write
            r1 = await async_client.get(
                "/autocomplete",
                params={"q": "xyznothing", "domain": "address", "limit": 10},
            )
            assert r1.status_code == 200
            assert r1.json() == {"suggestions": []}
            assert mock_os.call_count == 1

            # Second request: cache hit → OpenSearch NOT called
            r2 = await async_client.get(
                "/autocomplete",
                params={"q": "xyznothing", "domain": "address", "limit": 10},
            )
            assert r2.status_code == 200
            assert r2.json() == {"suggestions": []}
            assert mock_os.call_count == 1  # Still 1 — cache hit


# ---------------------------------------------------------------------------
# Integration: different limit values produce different cache entries
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
class TestDifferentLimitsCacheSeparately:
    """Verify that different limit values produce separate cache entries."""

    async def test_different_limits_produce_different_cache_keys(self, async_client):
        """Requests with limit=5 and limit=10 should not share a cache entry."""
        mock_redis = make_in_memory_redis()
        app.state.redis = mock_redis

        results_5 = ["Result A", "Result B", "Result C", "Result D", "Result E"]
        results_10 = results_5 + ["Result F", "Result G", "Result H", "Result I", "Result J"]

        with patch("app.query_opensearch", new_callable=AsyncMock) as mock_os:
            # Return different results based on the limit parameter
            async def os_side_effect(*, opensearch_url, domain_config, query, limit):
                if limit == 5:
                    return results_5
                return results_10

            mock_os.side_effect = os_side_effect

            # Request with limit=5
            r1 = await async_client.get(
                "/autocomplete",
                params={"q": "jalan", "domain": "address", "limit": 5},
            )
            assert r1.status_code == 200
            assert r1.json() == {"suggestions": results_5}

            # Request with limit=10
            r2 = await async_client.get(
                "/autocomplete",
                params={"q": "jalan", "domain": "address", "limit": 10},
            )
            assert r2.status_code == 200
            assert r2.json() == {"suggestions": results_10}

            # Both required OpenSearch calls (different cache entries)
            assert mock_os.call_count == 2

        # Verify two distinct keys in the cache store
        store = mock_redis._store
        assert "ac:address:jalan:5" in store
        assert "ac:address:jalan:10" in store
        assert json.loads(store["ac:address:jalan:5"]) == results_5
        assert json.loads(store["ac:address:jalan:10"]) == results_10

    async def test_same_limit_hits_cache_different_limit_misses(self, async_client):
        """After caching limit=5, a request with limit=10 should miss cache."""
        mock_redis = make_in_memory_redis()
        app.state.redis = mock_redis

        results_5 = ["A", "B", "C", "D", "E"]
        results_10 = ["A", "B", "C", "D", "E", "F", "G", "H", "I", "J"]

        with patch("app.query_opensearch", new_callable=AsyncMock) as mock_os:
            async def os_side_effect(*, opensearch_url, domain_config, query, limit):
                if limit == 5:
                    return results_5
                return results_10

            mock_os.side_effect = os_side_effect

            # First: limit=5 → cache miss → OpenSearch
            await async_client.get(
                "/autocomplete",
                params={"q": "ampang", "domain": "address", "limit": 5},
            )
            assert mock_os.call_count == 1

            # Second: limit=5 again → cache hit
            r2 = await async_client.get(
                "/autocomplete",
                params={"q": "ampang", "domain": "address", "limit": 5},
            )
            assert r2.status_code == 200
            assert r2.json() == {"suggestions": results_5}
            assert mock_os.call_count == 1  # No new call

            # Third: limit=10 → different key → cache miss → OpenSearch
            r3 = await async_client.get(
                "/autocomplete",
                params={"q": "ampang", "domain": "address", "limit": 10},
            )
            assert r3.status_code == 200
            assert r3.json() == {"suggestions": results_10}
            assert mock_os.call_count == 2  # New OpenSearch call
