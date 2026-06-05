"""Property-based tests for cache key determinism and normalization.

Property 1: Cache key construction is deterministic and normalized.

For any domain string, query string with arbitrary leading/trailing whitespace
and mixed case, and valid limit integer, the constructed cache key SHALL equal
``ac:{domain}:{q.strip().lower()}:{limit}`` — ensuring that semantically
identical queries always resolve to the same cache entry.

**Validates: Requirements 3.1, 3.4**
"""

from __future__ import annotations

from hypothesis import given
from hypothesis import strategies as st

from app import build_cache_key

# ---------------------------------------------------------------------------
# Strategies
# ---------------------------------------------------------------------------

# Domains: non-empty printable strings without colons (to avoid key confusion)
domain_st = st.text(
    alphabet=st.characters(whitelist_categories=("L", "N"), whitelist_characters="-_"),
    min_size=1,
    max_size=30,
)

# Whitespace characters for padding
_ws_chars = " \t\n\r\x0b\x0c"
whitespace_st = st.text(alphabet=_ws_chars, min_size=0, max_size=10)

# Core query content: at least one non-whitespace character, mixed case
query_core_st = st.text(
    alphabet=st.characters(whitelist_categories=("L", "N", "P")),
    min_size=1,
    max_size=50,
)

# Full query: whitespace-padded, mixed-case string
query_st = st.builds(
    lambda ws_l, core, ws_r: ws_l + core + ws_r,
    whitespace_st,
    query_core_st,
    whitespace_st,
)

# Valid limit: integer in [1, 100]
limit_st = st.integers(min_value=1, max_value=100)


# ---------------------------------------------------------------------------
# Property Tests
# ---------------------------------------------------------------------------


@given(domain=domain_st, query=query_st, limit=limit_st)
def test_cache_key_equals_normalized_format(domain: str, query: str, limit: int) -> None:
    """Cache key matches the canonical normalized format.

    **Validates: Requirements 3.1, 3.4**
    """
    key = build_cache_key(domain, query, limit)
    expected = f"ac:{domain}:{query.strip().lower()}:{limit}"
    assert key == expected


@given(domain=domain_st, query=query_st, limit=limit_st)
def test_cache_key_is_deterministic(domain: str, query: str, limit: int) -> None:
    """Calling build_cache_key twice with identical arguments yields the same result.

    **Validates: Requirements 3.1, 3.4**
    """
    key1 = build_cache_key(domain, query, limit)
    key2 = build_cache_key(domain, query, limit)
    assert key1 == key2


@given(domain=domain_st, query=query_st, limit=limit_st)
def test_cache_key_invariant_to_surrounding_whitespace(
    domain: str, query: str, limit: int
) -> None:
    """Adding extra whitespace around the query does not change the cache key.

    **Validates: Requirements 3.1, 3.4**
    """
    padded = "   " + query + "\t\n"
    key_original = build_cache_key(domain, query, limit)
    key_padded = build_cache_key(domain, padded, limit)
    assert key_original == key_padded


@given(
    domain=domain_st,
    query=st.text(
        alphabet=st.characters(
            whitelist_categories=("N",),
            whitelist_characters="abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ .-",
        ),
        min_size=1,
        max_size=50,
    ),
    limit=limit_st,
)
def test_cache_key_invariant_to_case(domain: str, query: str, limit: int) -> None:
    """Changing the case of the query does not change the cache key.

    Uses ASCII characters only since Python's str.lower() is locale-aware
    for Unicode and upper→lower is not always idempotent for non-ASCII.

    **Validates: Requirements 3.1, 3.4**
    """
    key_lower = build_cache_key(domain, query.lower(), limit)
    key_upper = build_cache_key(domain, query.upper(), limit)
    assert key_lower == key_upper
