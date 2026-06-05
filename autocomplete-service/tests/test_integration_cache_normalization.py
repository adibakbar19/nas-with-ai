"""Integration tests — verify end-to-end flow with normalized cache keys.

Validates: Requirements 3.1, 3.2, 3.4, 5.1

Ensures that querying with different whitespace/case variants of the same
logical query ("  Jalan ", "jalan", "JALAN") all produce the same cache key,
and that the cache write uses the normalized key so subsequent requests hit
the cache without re-querying OpenSearch.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from app import build_cache_key


# ---------------------------------------------------------------------------
# Unit-level: build_cache_key produces identical keys for equivalent queries
# ---------------------------------------------------------------------------


class TestBuildCacheKeyNormalization:
    """Verify that semantically identical queries produce the same cache key."""

    def test_whitespace_variants_produce_same_key(self):
        """'  Jalan ', 'jalan', 'JALAN' all map to the same cache key."""
        key1 = build_cache_key("address", "  Jalan ", 10)
        key2 = build_cache_key("address", "jalan", 10)
        key3 = build_cache_key("address", "JALAN", 10)

        assert key1 == key2 == key3
        assert key1 == "ac:address:jalan:10"

    def test_mixed_case_and_spaces(self):
        """Tabs, newlines, and mixed case all normalize identically."""
        key1 = build_cache_key("address", "\t JaLaN AmPaNg \n", 5)
        key2 = build_cache_key("address", "jalan ampang", 5)

        assert key1 == key2
        assert key1 == "ac:address:jalan ampang:5"

    def test_different_limits_produce_different_keys(self):
        """Different limit values produce different cache entries."""
        key_10 = build_cache_key("address", "jalan", 10)
        key_20 = build_cache_key("address", "jalan", 20)

        assert key_10 != key_20
        assert key_10 == "ac:address:jalan:10"
        assert key_20 == "ac:address:jalan:20"


# ---------------------------------------------------------------------------
# Integration: full request flow verifies cache key normalization end-to-end
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
class TestEndToEndCacheNormalization:
    """Full request flow: different query forms hit the same cache entry."""

    async def test_first_request_triggers_opensearch_and_cache_write(
        self, async_client
    ):
        """Request with '  Jalan ' triggers OpenSearch and writes to cache."""
        mock_results = ["Jalan Ampang, KL", "Jalan Alor, KL"]

        with patch("app.query_opensearch", new_callable=AsyncMock) as mock_os:
            mock_os.return_value = mock_results

            response = await async_client.get(
                "/autocomplete", params={"q": "  Jalan ", "domain": "address", "limit": 10}
            )

        assert response.status_code == 200
        assert response.json() == {"suggestions": mock_results}

        # OpenSearch was called exactly once
        mock_os.assert_called_once()

    async def test_normalized_variants_share_cache_entry(self, async_client):
        """After caching with '  Jalan ', queries 'jalan' and 'JALAN' hit cache."""
        from app import app

        mock_results = ["Jalan Ampang, KL", "Jalan Alor, KL"]

        # Use an in-memory dict to simulate real Redis behavior
        cache_store: dict[str, str] = {}

        import json

        async def fake_get(key: str):
            return cache_store.get(key)

        async def fake_setex(key: str, ttl: int, value: str):
            cache_store[key] = value
            return True

        mock_redis = AsyncMock()
        mock_redis.get = AsyncMock(side_effect=fake_get)
        mock_redis.setex = AsyncMock(side_effect=fake_setex)
        mock_redis.aclose = AsyncMock()
        app.state.redis = mock_redis

        with patch("app.query_opensearch", new_callable=AsyncMock) as mock_os:
            mock_os.return_value = mock_results

            # 1st call: "  Jalan " — cache miss → OpenSearch → cache write
            r1 = await async_client.get(
                "/autocomplete", params={"q": "  Jalan ", "domain": "address", "limit": 10}
            )
            assert r1.status_code == 200
            assert r1.json() == {"suggestions": mock_results}
            assert mock_os.call_count == 1

            # 2nd call: "jalan" — should hit cache, no new OpenSearch call
            r2 = await async_client.get(
                "/autocomplete", params={"q": "jalan", "domain": "address", "limit": 10}
            )
            assert r2.status_code == 200
            assert r2.json() == {"suggestions": mock_results}
            assert mock_os.call_count == 1  # Still 1 — cache hit

            # 3rd call: "JALAN" — should also hit cache
            r3 = await async_client.get(
                "/autocomplete", params={"q": "JALAN", "domain": "address", "limit": 10}
            )
            assert r3.status_code == 200
            assert r3.json() == {"suggestions": mock_results}
            assert mock_os.call_count == 1  # Still 1 — cache hit

        # Verify the cache key that was written
        expected_key = "ac:address:jalan:10"
        assert expected_key in cache_store
        assert json.loads(cache_store[expected_key]) == mock_results

    async def test_cache_write_uses_normalized_key(self, async_client):
        """Verify that cache.setex is called with the normalized key."""
        from app import app

        mock_results = ["Jalan Ampang, KL"]

        mock_redis = AsyncMock()
        mock_redis.get = AsyncMock(return_value=None)
        mock_redis.setex = AsyncMock(return_value=True)
        mock_redis.aclose = AsyncMock()
        app.state.redis = mock_redis

        with patch("app.query_opensearch", new_callable=AsyncMock) as mock_os:
            mock_os.return_value = mock_results

            await async_client.get(
                "/autocomplete", params={"q": "  Jalan ", "domain": "address", "limit": 10}
            )

        # Inspect the setex call — first arg is the key
        mock_redis.setex.assert_called_once()
        call_args = mock_redis.setex.call_args
        cache_key_used = call_args[0][0]

        assert cache_key_used == "ac:address:jalan:10"
        # Key should NOT contain the raw un-normalized input
        assert "  Jalan " not in cache_key_used
        assert "Jalan" not in cache_key_used  # No uppercase J
