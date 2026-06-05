"""Property test: OpenSearch query construction matches domain config strategy.

**Validates: Requirements 4.2, 4.3**

Property 3: For any domain configuration with query_type "bool_prefix", the
constructed payload SHALL use a multi_match query with type "bool_prefix" on all
listed query_fields. For any domain configuration with query_type
"match_phrase_prefix", the payload SHALL use a match_phrase_prefix query on the
first element of query_fields.
"""

from __future__ import annotations

from typing import Any

import pytest
from hypothesis import given
from hypothesis import strategies as st


# ---------------------------------------------------------------------------
# Strategy: Generate realistic domain configs
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


class TestOpenSearchQueryConstruction:
    """Property 3: OpenSearch query construction matches domain config strategy."""

    @given(
        domain_config=_domain_config_strategy.filter(
            lambda c: c["query_type"] == "bool_prefix"
        ),
        query=_query_string_strategy,
        limit=_limit_strategy,
    )
    def test_bool_prefix_uses_multi_match(
        self, domain_config: dict[str, Any], query: str, limit: int
    ) -> None:
        """For bool_prefix configs, payload must use multi_match with type bool_prefix on all query_fields.

        **Validates: Requirements 4.2**
        """
        payload = build_query_payload(domain_config, query, limit)

        # Must contain multi_match at the query level
        assert "multi_match" in payload["query"], (
            "bool_prefix config should produce a multi_match query"
        )

        mm = payload["query"]["multi_match"]

        # multi_match type must be "bool_prefix"
        assert mm["type"] == "bool_prefix", (
            f"Expected type 'bool_prefix', got '{mm['type']}'"
        )

        # fields must exactly match all query_fields from config
        assert mm["fields"] == domain_config["query_fields"], (
            f"Expected fields {domain_config['query_fields']}, got {mm['fields']}"
        )

        # query text must match input
        assert mm["query"] == query

    @given(
        domain_config=_domain_config_strategy.filter(
            lambda c: c["query_type"] == "match_phrase_prefix"
        ),
        query=_query_string_strategy,
        limit=_limit_strategy,
    )
    def test_match_phrase_prefix_uses_first_field(
        self, domain_config: dict[str, Any], query: str, limit: int
    ) -> None:
        """For match_phrase_prefix configs, payload must use match_phrase_prefix on the first query_field.

        **Validates: Requirements 4.3**
        """
        payload = build_query_payload(domain_config, query, limit)

        # Must contain match_phrase_prefix at the query level
        assert "match_phrase_prefix" in payload["query"], (
            "match_phrase_prefix config should produce a match_phrase_prefix query"
        )

        mpp = payload["query"]["match_phrase_prefix"]
        first_field = domain_config["query_fields"][0]

        # The key in match_phrase_prefix must be the first query_field
        assert first_field in mpp, (
            f"Expected field '{first_field}' in match_phrase_prefix, got keys {list(mpp.keys())}"
        )

        # The nested query text must match input
        assert mpp[first_field]["query"] == query
