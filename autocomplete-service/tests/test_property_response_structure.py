"""Property-based tests for valid response structure.

Property 6: Valid responses always contain a suggestions array of strings.

For any valid request (non-empty q, known domain, valid limit) that does not
trigger a backend error, the HTTP response SHALL be 200 with a JSON body
containing a ``suggestions`` key whose value is a list where every element
is a string.

**Validates: Requirements 6.2**
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

# ---------------------------------------------------------------------------
# Strategies
# ---------------------------------------------------------------------------

# Valid query strings: non-empty, non-whitespace-only (at least one visible char)
_visible_char_st = st.characters(whitelist_categories=("L", "N", "P", "S"))
_query_st = st.builds(
    lambda core: core,
    st.text(alphabet=_visible_char_st, min_size=1, max_size=50),
)

# Valid limits in [1, 100]
_limit_st = st.integers(min_value=1, max_value=100)

# Suggestion strings: non-empty strings (simulating OpenSearch results)
_suggestion_st = st.text(
    alphabet=st.characters(whitelist_categories=("L", "N", "P", "S", "Z")),
    min_size=1,
    max_size=100,
)

# Lists of suggestion strings (0 to 20 items — includes empty list case)
_suggestions_list_st = st.lists(_suggestion_st, min_size=0, max_size=20)


# ---------------------------------------------------------------------------
# Property Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(query=_query_st, limit=_limit_st, suggestions=_suggestions_list_st)
async def test_valid_response_contains_suggestions_array_of_strings(
    async_client, query: str, limit: int, suggestions: list[str]
) -> None:
    """Valid requests return HTTP 200 with a suggestions array of strings.

    **Validates: Requirements 6.2**
    """
    # Mock query_opensearch to return the generated suggestions
    with patch("app.query_opensearch", new_callable=AsyncMock) as mock_search:
        mock_search.return_value = suggestions

        response = await async_client.get(
            "/autocomplete",
            params={"q": query, "domain": "address", "limit": limit},
        )

    # Assert HTTP 200
    assert response.status_code == 200, (
        f"Expected 200 but got {response.status_code}: {response.text}"
    )

    # Assert response body structure
    body = response.json()
    assert "suggestions" in body, f"Response missing 'suggestions' key: {body}"

    result = body["suggestions"]
    assert isinstance(result, list), f"'suggestions' is not a list: {type(result)}"

    # Assert every element is a string
    for i, item in enumerate(result):
        assert isinstance(item, str), (
            f"suggestions[{i}] is not a string: {type(item)} = {item!r}"
        )


@pytest.mark.asyncio
@settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(query=_query_st, limit=_limit_st)
async def test_valid_response_with_empty_suggestions(
    async_client, query: str, limit: int
) -> None:
    """Valid requests with no results still return suggestions as empty list.

    **Validates: Requirements 6.2**
    """
    with patch("app.query_opensearch", new_callable=AsyncMock) as mock_search:
        mock_search.return_value = []

        response = await async_client.get(
            "/autocomplete",
            params={"q": query, "domain": "address", "limit": limit},
        )

    assert response.status_code == 200
    body = response.json()
    assert "suggestions" in body
    assert body["suggestions"] == []
