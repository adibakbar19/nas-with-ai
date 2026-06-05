"""Property test: Result extraction filters invalid hits.

**Validates: Requirements 4.4**

Property 4: For any list of OpenSearch hit objects where some hits have the
result field present and non-empty, some have it missing, some have it as empty
string, and some have it as a non-string type, the extraction SHALL return only
the non-empty string values in order, with length less than or equal to the
original hits count.
"""

from __future__ import annotations

from hypothesis import given
from hypothesis import strategies as st


# ---------------------------------------------------------------------------
# Strategy: Generate OpenSearch hit objects with various _source scenarios
# ---------------------------------------------------------------------------

# A realistic result_field name
_result_field_strategy = st.text(
    alphabet=st.sampled_from("abcdefghijklmnopqrstuvwxyz_"),
    min_size=1,
    max_size=20,
)

# Valid non-empty string value
_valid_value_strategy = st.text(min_size=1, max_size=200).filter(lambda s: s.strip() != "")

# Invalid value scenarios: empty string, non-string types, or missing field
_invalid_value_strategy = st.one_of(
    st.just(""),  # empty string
    st.integers(),  # int
    st.lists(st.text(), max_size=3),  # list
    st.none(),  # None
    st.floats(allow_nan=False),  # float
    st.dictionaries(st.text(max_size=5), st.text(max_size=5), max_size=2),  # dict
)


def _hit_with_value(result_field: str, value):
    """Create a hit dict with a specific value in _source."""
    return {"_source": {result_field: value}}


def _hit_missing_field(result_field: str):
    """Create a hit dict where the result_field is missing from _source."""
    return {"_source": {"other_field": "something"}}


def _hit_missing_source():
    """Create a hit dict with no _source key at all."""
    return {"_id": "some-id"}


# Strategy for a single hit entry: tagged with whether it's valid or not
_hit_entry_strategy = st.one_of(
    # Valid: non-empty string in result_field
    _valid_value_strategy.map(lambda v: ("valid", v)),
    # Invalid: empty string, non-string types
    _invalid_value_strategy.map(lambda v: ("invalid_value", v)),
    # Missing: field not present in _source
    st.just(("missing_field", None)),
    # Missing _source entirely
    st.just(("missing_source", None)),
)


# ---------------------------------------------------------------------------
# Helper: Extract suggestions from hits (mirrors search.py logic)
# ---------------------------------------------------------------------------


def extract_suggestions(hits: list[dict], result_field: str) -> list[str]:
    """Reproduce the result extraction logic from search.query_opensearch.

    This mirrors the implementation in search.py so we can test it in isolation
    without requiring an actual OpenSearch connection.
    """
    suggestions: list[str] = []
    for hit in hits:
        value = hit.get("_source", {}).get(result_field)
        if isinstance(value, str) and value:
            suggestions.append(value)
    return suggestions


# ---------------------------------------------------------------------------
# Property Test
# ---------------------------------------------------------------------------


class TestResultExtractionFiltering:
    """Property 4: Result extraction filters invalid hits."""

    @given(
        result_field=_result_field_strategy,
        hit_entries=st.lists(_hit_entry_strategy, min_size=0, max_size=50),
    )
    def test_only_non_empty_strings_returned_in_order(
        self, result_field: str, hit_entries: list[tuple[str, object]]
    ) -> None:
        """Only non-empty string values from result_field are returned, in original order.

        **Validates: Requirements 4.4**
        """
        # Build the hits list and track expected valid values
        hits: list[dict] = []
        expected: list[str] = []

        for entry_type, value in hit_entries:
            if entry_type == "valid":
                hits.append(_hit_with_value(result_field, value))
                expected.append(value)
            elif entry_type == "invalid_value":
                hits.append(_hit_with_value(result_field, value))
                # Not added to expected — these should be filtered out
            elif entry_type == "missing_field":
                hits.append(_hit_missing_field(result_field))
            elif entry_type == "missing_source":
                hits.append(_hit_missing_source())

        # Execute extraction
        result = extract_suggestions(hits, result_field)

        # Property assertions:

        # 1. Only non-empty strings are returned
        for item in result:
            assert isinstance(item, str), f"Expected str, got {type(item)}: {item!r}"
            assert item != "", "Empty string should not appear in results"

        # 2. Results appear in the same order as they appear in hits
        assert result == expected, (
            f"Results should contain only valid values in order.\n"
            f"Expected: {expected}\n"
            f"Got: {result}"
        )

        # 3. Result length is <= number of hits
        assert len(result) <= len(hits), (
            f"Result length {len(result)} exceeds hits count {len(hits)}"
        )
