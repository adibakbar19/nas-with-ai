"""Fuzzy matching logic using rapidfuzz.

Pure functions — no I/O, no side effects.
"""

from __future__ import annotations

from rapidfuzz import fuzz


def compute_score(
    query_address: str | None,
    query_key: str | None,
    query_postcode: str | None,
    query_street: str | None,
    candidate: dict,
) -> float:
    """Return a match score in [0.0, 1.0] between the query and a candidate."""
    # Exact canonical key match is a perfect score
    if query_key and candidate.get("canonical_address_key"):
        if query_key == candidate["canonical_address_key"]:
            return 1.0

    # Base score: token sort ratio handles word-order differences in Malaysian addresses
    base = fuzz.token_sort_ratio(
        query_address or "",
        candidate.get("address_clean") or "",
    ) / 100.0

    # Postcode bonus
    if query_postcode and candidate.get("postcode") == query_postcode:
        base = min(1.0, base + 0.05)

    # Street name bonus
    if query_street and candidate.get("street_name"):
        street_score = fuzz.ratio(query_street, candidate["street_name"]) / 100.0
        if street_score > 0.8:
            base = min(1.0, base + 0.03)

    return round(base, 4)


def rank_candidates(
    query: dict,
    candidates: list[dict],
) -> list[tuple[dict, float]]:
    """Score all candidates against the query and return them sorted best-first."""
    scored = [
        (
            c,
            compute_score(
                query.get("address_clean"),
                query.get("canonical_address_key"),
                query.get("postcode"),
                query.get("street_name"),
                c,
            ),
        )
        for c in candidates
    ]
    return sorted(scored, key=lambda x: x[1], reverse=True)
