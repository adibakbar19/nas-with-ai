"""Property test: Cache round-trip preserves suggestions.

**Validates: Requirements 3.2, 3.3, 5.1**

Property 2: For any list of non-empty strings (suggestions), serializing it
to JSON via json.dumps and then deserializing via json.loads SHALL produce
a list equal to the original input. Additionally, verifies the actual
get_cached/set_cached functions preserve data through a mock Redis backend.
"""

from __future__ import annotations

import json
from unittest.mock import AsyncMock

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

# ---------------------------------------------------------------------------
# Strategies
# ---------------------------------------------------------------------------

# Generate lists of non-empty strings (suggestions)
suggestions_strategy = st.lists(
    st.text(min_size=1, alphabet=st.characters(categories=("L", "N", "P", "S", "Z"))),
    min_size=0,
    max_size=50,
)


# ---------------------------------------------------------------------------
# Property 2: JSON round-trip preserves suggestions
# ---------------------------------------------------------------------------


@settings(max_examples=100)
@given(suggestions=suggestions_strategy)
def test_json_roundtrip_preserves_suggestions(suggestions: list[str]) -> None:
    """For any list[str] of non-empty strings, json.loads(json.dumps(suggestions)) == suggestions.

    **Validates: Requirements 3.2, 3.3, 5.1**
    """
    serialized = json.dumps(suggestions)
    deserialized = json.loads(serialized)
    assert deserialized == suggestions


# ---------------------------------------------------------------------------
# Property 2 (extended): get_cached/set_cached round-trip via mock Redis
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@settings(max_examples=100)
@given(suggestions=suggestions_strategy)
async def test_cache_functions_roundtrip_preserves_suggestions(
    suggestions: list[str],
) -> None:
    """Verify that set_cached → get_cached preserves the original suggestions list.

    Uses a mock Redis that stores values in a dict, simulating real Redis
    GET/SETEX behavior without requiring fakeredis.

    **Validates: Requirements 3.2, 3.3, 5.1**
    """
    from cache import get_cached, set_cached

    # In-memory store to simulate Redis
    store: dict[str, str] = {}

    mock_redis = AsyncMock()
    mock_redis.setex = AsyncMock(
        side_effect=lambda key, ttl, value: store.update({key: value})
    )
    mock_redis.get = AsyncMock(side_effect=lambda key: store.get(key))

    cache_key = "ac:address:test:10"
    ttl = 300

    # Write to cache
    await set_cached(mock_redis, cache_key, suggestions, ttl)

    # Read from cache
    result = await get_cached(mock_redis, cache_key)

    assert result == suggestions
