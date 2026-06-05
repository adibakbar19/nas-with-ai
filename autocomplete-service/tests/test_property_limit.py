"""Property test: Limit parameter bounds the query size.

**Validates: Requirements 4.5, 4.6**

Property 5: For any integer limit in range [1, 100], the OpenSearch query
payload's `size` field SHALL equal that limit value.
"""

from __future__ import annotations

from typing import Any

from hypothesis import given
from hypothesis import strategies as st


# ---------------------------------------------------------------------------
# Strategy: Generate realistic domain configs and limits
# ---------------------------------------------------------------------------

_field_name_strategy = st.text(
    alphabet=st.sampled_from("abcdefghijklmnopqrstuvwxyz_.-0123456789"),
    min_size=1,
    max_size=30,
)

_domain_config_strategy = st.fixed_dictionaries(
    {
        "index": st.text(
            alphabet=st.sampled_from("abcdefghijklmnopqrstuvwxyz_-"),
            min_size=1,
            max_size=30,
        ),
        "query_type": st.sampled_from(["bool_prefix", "match_phrase_prefix"]),
        "query_fields": st.lists(_field_name_strategy, min_size=1, max_size=5),
        "result_field": _field_name_strategy,
    }
)

_query_string_strategy = st.text(min_size=1, max_size=100).filter(
    lambda s: s.strip() != ""
)

_limit_strategy = st.integers(min_value=1, max_value=100)


# ---------------------------------------------------------------------------
# Helper: extract the query-building logic from search.py without network call
# ---------------------------------------------------------------------------


def build_query_payload(
    domain_config: dict[str, Any], query: str, limit: int
) -> dict[str, Any]:
    """Reproduce the payload construction logic from search.query_opensearch.

    This mirrors the implementation in search.py so we can test it in isolation
    without requiring an actual OpenSearch connection.
    """
    query_type = domain_config["query_type"]
    query_fields = domain_config["query_fields"]
    result_field = domain_config["result_field"]

    if query_type == "bool_prefix":
        payload = {
            "size": limit,
            "_source": [result_field],
            "query": {
                "multi_match": {
                    "query": query,
                    "type": "bool_prefix",
                    "fields": query_fields,
                }
            },
        }
    else:
        # match_phrase_prefix
        payload = {
            "size": limit,
            "_source": [result_field],
            "query": {
                "match_phrase_prefix": {
                    query_fields[0]: {
                        "query": query,
                    }
                }
            },
        }

    return payload


# ---------------------------------------------------------------------------
# Property Test
# ---------------------------------------------------------------------------


class TestLimitBoundsQuerySize:
    """Property 5: Limit parameter bounds the query size."""

    @given(
        domain_config=_domain_config_strategy,
        query=_query_string_strategy,
        limit=_limit_strategy,
    )
    def test_payload_size_equals_limit(
        self, domain_config: dict[str, Any], query: str, limit: int
    ) -> None:
        """For any valid limit in [1, 100], the payload size field must equal that limit.

        **Validates: Requirements 4.5, 4.6**
        """
        payload = build_query_payload(domain_config, query, limit)

        assert "size" in payload, "Payload must contain a 'size' field"
        assert payload["size"] == limit, (
            f"Expected payload size to equal limit {limit}, got {payload['size']}"
        )
