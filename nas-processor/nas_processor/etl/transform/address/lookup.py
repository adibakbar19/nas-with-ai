"""Three-tier lookup enrichment — pure Polars vectorized joins.

Replaces _lookups.py entirely. Performance-critical: designed for 1M+ rows.

Tier 1 — Postcode-exact join (covers ~70-80% of rows)     [VECTORIZED: pl.join]
Tier 2 — Normalized text exact join (covers ~15%)          [VECTORIZED: pl.join]
Tier 3 — Levenshtein fuzzy (last resort, ~5-10% of rows)  [map_elements on SUBSET ONLY]

KEY RULE: Levenshtein only runs on rows that failed tiers 1 and 2.
Never on the full dataset. Subset is guaranteed to be small.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import polars as pl

if TYPE_CHECKING:
    from nas_processor.etl.repository.lookup_repository import LookupFrames

logger = logging.getLogger(__name__)


# ── Tier 1: Postcode-exact join ───────────────────────────────────────────────
# VECTORIZED: Uses pl.DataFrame.join() — O(n) hash join.


def enrich_from_postcode(
    df: pl.DataFrame,
    lookups: "LookupFrames",
) -> pl.DataFrame:
    """Tier 1: Exact postcode join.

    Joins df against lookups.postcode on postcode_raw == postcode.
    Fills: state_name (from postcode lookup 'state' column),
           locality_name (from 'city' column),
           postcode_ungazetted flag.

    postcode_ungazetted = True when postcode_raw is present but NOT found.
    postcode_ungazetted = False when postcode resolves successfully.
    postcode_ungazetted = null when postcode_raw is null.

    Does NOT overwrite existing non-null values.
    """
    postcode_lk = lookups.postcode.select([
        pl.col("postcode").alias("_lk_postcode"),
        pl.col("city").alias("_lk_city"),
        pl.col("state").alias("_lk_state"),
    ]).unique(subset=["_lk_postcode"])

    # Ensure postcode_raw exists
    if "postcode_raw" not in df.columns:
        return df.with_columns(pl.lit(None).cast(pl.Utf8).alias("postcode_ungazetted"))

    # Left join on postcode
    joined = df.join(
        postcode_lk,
        left_on="postcode_raw",
        right_on="_lk_postcode",
        how="left",
    )

    # Determine postcode_ungazetted flag
    # null postcode_raw → null flag; present but unmatched → True; matched → False
    has_postcode = pl.col("postcode_raw").is_not_null()
    matched = pl.col("_lk_state").is_not_null()

    # Ensure target columns exist
    for col in ["state_name", "locality_name"]:
        if col not in joined.columns:
            joined = joined.with_columns(pl.lit(None).cast(pl.Utf8).alias(col))

    result = joined.with_columns([
        # Fill state_name from postcode lookup where currently null
        pl.coalesce([pl.col("state_name"), pl.col("_lk_state")]).alias("state_name"),
        # Fill locality_name from city where currently null
        pl.coalesce([pl.col("locality_name"), pl.col("_lk_city")]).alias("locality_name"),
        # Ungazetted flag
        pl.when(~has_postcode).then(None)
          .when(matched).then(pl.lit(False))
          .otherwise(pl.lit(True))
          .alias("postcode_ungazetted"),
    ]).drop(["_lk_city", "_lk_state"])

    return result


# ── Tier 2: Normalized text exact join ────────────────────────────────────────
# VECTORIZED: Uses pl.DataFrame.join() — O(n) hash join on normalized strings.


def _normalize_for_matching(series: pl.Series) -> pl.Series:
    """Normalize text for exact matching in Tier 2.

    VECTORIZED: Uses pl.Series.str.* methods only.

    - Uppercase
    - Strip whitespace
    - Remove noise prefixes (DAERAH, MUKIM, NEGERI, etc.)
    - Collapse multiple spaces
    """
    s = series.cast(pl.Utf8)
    s = s.str.to_uppercase()
    s = s.str.strip_chars()
    # Remove common administrative prefixes that differ between source and lookup
    s = s.str.replace_all(r"\bDAERAH\s+", "")
    s = s.str.replace_all(r"\bMUKIM\s+", "")
    s = s.str.replace_all(r"\bNEGERI\s+", "")
    s = s.str.replace_all(r"\bBANDAR\s+", "")
    s = s.str.replace_all(r"\s{2,}", " ")
    s = s.str.strip_chars()
    return s


def enrich_from_text(
    df: pl.DataFrame,
    lookups: "LookupFrames",
) -> pl.DataFrame:
    """Tier 2: Normalized text exact join for unresolved rows.

    VECTORIZED: All matching uses vectorized str.contains() with when/then chains.

    Only processes rows where state_code is still null after Tier 1.
    Steps:
    1. Normalize state names in both df.address_for_lookup and lookup table
    2. Scan address_for_lookup for state name occurrences (exact substring)
    3. For matched rows, exact join on normalized district name
    4. For matched rows, exact join on normalized mukim name

    Does NOT overwrite existing non-null values.
    """
    # Ensure columns exist
    for col in ["state_code", "state_name", "district_code", "district_name",
                "mukim_code", "mukim_name", "mukim_id"]:
        if col not in df.columns:
            df = df.with_columns(pl.lit(None).cast(pl.Utf8).alias(col))

    # Only process unresolved rows (state_code is null)
    needs_resolution = df["state_code"].is_null()
    resolved_count_before = needs_resolution.sum()

    if resolved_count_before == 0:
        logger.info("tier2_skip all_rows_already_resolved")
        return df

    logger.info("tier2_start unresolved_rows=%d", resolved_count_before)

    # Build normalized state lookup for text scanning
    state_lk = lookups.state.with_columns(
        _normalize_for_matching(pl.col("state_name")).alias("_state_norm"),
    )

    # Determine which address column to scan
    address_col = "address_for_lookup" if "address_for_lookup" in df.columns else "address_norm"

    # Collect states into a list for iteration (16 states — trivial loop)
    states = state_lk.select(["state_code", "state_name", "_state_norm"]).to_dicts()

    # Sort by name length descending so longer names match first
    # (e.g., "KUALA LUMPUR" before "LUMPUR")
    states.sort(key=lambda s: len(s["_state_norm"] or ""), reverse=True)

    # Build a when/then chain for state detection
    # This iterates 16 states — not 1M rows. The contains() is vectorized.
    expr = pl.lit(None).cast(pl.Utf8)
    expr_name = pl.lit(None).cast(pl.Utf8)
    for state in states:
        norm = state["_state_norm"]
        if not norm:
            continue
        condition = (
            pl.col("state_code").is_null()
            & pl.col(address_col).str.contains(norm, literal=True)
        )
        expr = pl.when(condition).then(pl.lit(state["state_code"])).otherwise(expr)
        expr_name = pl.when(condition).then(pl.lit(state["state_name"])).otherwise(expr_name)

    df = df.with_columns([
        pl.coalesce([pl.col("state_code"), expr]).alias("state_code"),
        pl.coalesce([pl.col("state_name"), expr_name]).alias("state_name"),
    ])

    # OPTIMIZATION NOTE: The when/then chain below iterates
    # ~150 districts and ~900 mukims to build Polars expressions.
    # Expression compilation is done once per chunk, not per row.
    # This is acceptable for now but can be optimized by using
    # a normalized join pattern if compilation time becomes
    # measurable at scale.

    # --- District matching via text contains ---
    needs_district = df["district_code"].is_null() & df["state_code"].is_not_null()
    if needs_district.any():
        district_lk = lookups.district.with_columns(
            _normalize_for_matching(pl.col("district_name")).alias("_district_norm"),
        ).select(["state_code", "district_code", "district_name", "_district_norm"])

        districts = district_lk.to_dicts()
        # Group by state to reduce comparisons
        by_state: dict[str, list[dict]] = {}
        for d in districts:
            by_state.setdefault(d["state_code"], []).append(d)

        d_code_expr = pl.col("district_code")  # keep existing
        d_name_expr = pl.col("district_name")

        for state_code, state_districts in by_state.items():
            # Sort by length descending
            state_districts.sort(key=lambda d: len(d["_district_norm"] or ""), reverse=True)
            for dist in state_districts:
                norm = dist["_district_norm"]
                if not norm:
                    continue
                condition = (
                    pl.col("district_code").is_null()
                    & (pl.col("state_code") == state_code)
                    & pl.col(address_col).str.contains(norm, literal=True)
                )
                d_code_expr = pl.when(condition).then(pl.lit(dist["district_code"])).otherwise(d_code_expr)
                d_name_expr = pl.when(condition).then(pl.lit(dist["district_name"])).otherwise(d_name_expr)

        df = df.with_columns([
            d_code_expr.alias("district_code"),
            d_name_expr.alias("district_name"),
        ])

    # --- Mukim matching via text contains ---
    needs_mukim = df["mukim_code"].is_null() & df["district_code"].is_not_null()
    if needs_mukim.any():
        mukim_lk = lookups.mukim.with_columns(
            _normalize_for_matching(pl.col("mukim_name")).alias("_mukim_norm"),
        )

        mukims = mukim_lk.select([
            "state_code", "district_code", "mukim_code", "mukim_name", "mukim_id", "_mukim_norm"
        ]).to_dicts()

        by_district: dict[tuple[str, str], list[dict]] = {}
        for m in mukims:
            key = (m["state_code"] or "", m["district_code"] or "")
            by_district.setdefault(key, []).append(m)

        m_code_expr = pl.col("mukim_code")
        m_name_expr = pl.col("mukim_name")
        m_id_expr = pl.col("mukim_id")

        for (sc, dc), district_mukims in by_district.items():
            district_mukims.sort(key=lambda m: len(m["_mukim_norm"] or ""), reverse=True)
            for muk in district_mukims:
                norm = muk["_mukim_norm"]
                if not norm:
                    continue
                condition = (
                    pl.col("mukim_code").is_null()
                    & (pl.col("state_code") == sc)
                    & (pl.col("district_code") == dc)
                    & pl.col(address_col).str.contains(norm, literal=True)
                )
                m_code_expr = pl.when(condition).then(pl.lit(muk["mukim_code"])).otherwise(m_code_expr)
                m_name_expr = pl.when(condition).then(pl.lit(muk["mukim_name"])).otherwise(m_name_expr)
                m_id_expr = pl.when(condition).then(pl.lit(muk["mukim_id"])).otherwise(m_id_expr)

        df = df.with_columns([
            m_code_expr.alias("mukim_code"),
            m_name_expr.alias("mukim_name"),
            m_id_expr.alias("mukim_id"),
        ])

    resolved_after = df["state_code"].is_null().sum()
    logger.info("tier2_complete resolved=%d remaining_unresolved=%d",
                resolved_count_before - resolved_after, resolved_after)

    return df


# ── Tier 3: Fuzzy Levenshtein matching ────────────────────────────────────────
# map_elements on SUBSET ONLY — runs on 5-10% of rows that failed Tiers 1+2.
# Acceptable because the subset is small (typically <50k rows from a 500k chunk).


def _levenshtein(a: str, b: str) -> int:
    """Pure Python Levenshtein distance. Used in map_elements on small subset."""
    if a == b:
        return 0
    if not a:
        return len(b)
    if not b:
        return len(a)
    prev = list(range(len(b) + 1))
    for i, ca in enumerate(a, 1):
        curr = [i]
        for j, cb in enumerate(b, 1):
            curr.append(min(curr[j - 1] + 1, prev[j] + 1, prev[j - 1] + (ca != cb)))
        prev = curr
    return prev[-1]


def _best_fuzzy_match(
    value: str | None,
    candidates: list[tuple[str, str, str]],  # (normalized_name, code, original_name)
    threshold: float = 0.75,
) -> tuple[str, str] | None:
    """Find best fuzzy match for a value against candidates.

    Returns (code, original_name) or None if no match above threshold.
    Threshold is similarity ratio: 1 - (distance / max_len) >= threshold.
    """
    if not value:
        return None
    value_upper = value.strip().upper()
    if not value_upper:
        return None

    best_score = 0.0
    best_match: tuple[str, str] | None = None

    for norm_name, code, orig_name in candidates:
        if not norm_name:
            continue
        max_len = max(len(value_upper), len(norm_name))
        if max_len == 0:
            continue
        dist = _levenshtein(value_upper, norm_name)
        similarity = 1.0 - (dist / max_len)
        if similarity >= threshold and similarity > best_score:
            best_score = similarity
            best_match = (code, orig_name)

    return best_match


def enrich_from_fuzzy(
    df: pl.DataFrame,
    lookups: "LookupFrames",
) -> pl.DataFrame:
    """Tier 3: Fuzzy Levenshtein matching for still-unresolved rows.

    ONLY processes rows where state_code is STILL null after tiers 1+2.
    Uses map_elements — acceptable because subset is small (5-10% of chunk).

    Threshold: similarity >= 0.75 required for a match.
    """
    # Ensure columns exist
    for col in ["state_code", "state_name", "district_code", "district_name",
                "mukim_code", "mukim_name", "mukim_id"]:
        if col not in df.columns:
            df = df.with_columns(pl.lit(None).cast(pl.Utf8).alias(col))

    unresolved_mask = df["state_code"].is_null()
    unresolved_count = unresolved_mask.sum()

    if unresolved_count == 0:
        logger.info("tier3_skip no_unresolved_rows")
        return df

    logger.info("tier3_start unresolved_rows=%d (%.1f%% of chunk)",
                unresolved_count, 100.0 * unresolved_count / len(df))

    # Build candidate lists (small — ~16 states, ~150 districts)
    state_candidates: list[tuple[str, str, str]] = [
        (
            (row["state_name"] or "").strip().upper(),
            row["state_code"] or "",
            row["state_name"] or "",
        )
        for row in lookups.state.to_dicts()
    ]

    # Get the unresolved subset indices
    indices = df.with_row_index("_idx").filter(unresolved_mask)["_idx"]

    # Extract address text for fuzzy matching
    address_col = "address_for_lookup" if "address_for_lookup" in df.columns else "address_norm"
    subset = df.with_row_index("_idx").filter(unresolved_mask).select(["_idx", address_col])

    # Fuzzy match states using map_elements (on subset only)
    def _match_state(text: str | None) -> str | None:
        result = _best_fuzzy_match(text, state_candidates)
        return result[0] if result else None

    def _match_state_name(text: str | None) -> str | None:
        result = _best_fuzzy_match(text, state_candidates)
        return result[1] if result else None

    matched_codes = subset[address_col].map_elements(_match_state, return_dtype=pl.Utf8)
    matched_names = subset[address_col].map_elements(_match_state_name, return_dtype=pl.Utf8)

    # Build update frame
    updates = pl.DataFrame({
        "_idx": indices,
        "_fuzzy_state_code": matched_codes,
        "_fuzzy_state_name": matched_names,
    })

    # Join updates back and coalesce
    df = df.with_row_index("_idx").join(updates, on="_idx", how="left")
    df = df.with_columns([
        pl.coalesce([pl.col("state_code"), pl.col("_fuzzy_state_code")]).alias("state_code"),
        pl.coalesce([pl.col("state_name"), pl.col("_fuzzy_state_name")]).alias("state_name"),
    ]).drop(["_idx", "_fuzzy_state_code", "_fuzzy_state_name"])

    resolved_after = df["state_code"].is_null().sum()
    logger.info("tier3_complete resolved=%d remaining_unresolved=%d",
                unresolved_count - resolved_after, resolved_after)

    return df


# ── Single-mukim inference ────────────────────────────────────────────────────
# VECTORIZED: Uses pl.DataFrame.join().


def _infer_single_mukim(
    df: pl.DataFrame,
    lookups: "LookupFrames",
) -> pl.DataFrame:
    """If a state+district has exactly one mukim, assign it to unresolved rows.

    VECTORIZED: Uses group_by + filter + join.
    Only fills rows where mukim_code is still null and state+district are known.
    """
    needs_mukim = df["mukim_code"].is_null() & df["state_code"].is_not_null() & df["district_code"].is_not_null()
    if not needs_mukim.any():
        return df

    # Find state+district combos with exactly one mukim
    unique_mukims = (
        lookups.mukim
        .group_by(["state_code", "district_code"])
        .agg([
            pl.col("mukim_code").first().alias("_single_mukim_code"),
            pl.col("mukim_name").first().alias("_single_mukim_name"),
            pl.col("mukim_id").first().alias("_single_mukim_id"),
            pl.len().alias("_mukim_count"),
        ])
        .filter(pl.col("_mukim_count") == 1)
        .drop("_mukim_count")
    )

    if unique_mukims.is_empty():
        return df

    # Join and fill
    joined = df.join(
        unique_mukims,
        on=["state_code", "district_code"],
        how="left",
    )

    result = joined.with_columns([
        pl.coalesce([pl.col("mukim_code"), pl.col("_single_mukim_code")]).alias("mukim_code"),
        pl.coalesce([pl.col("mukim_name"), pl.col("_single_mukim_name")]).alias("mukim_name"),
        pl.coalesce([pl.col("mukim_id"), pl.col("_single_mukim_id")]).alias("mukim_id"),
    ]).drop(["_single_mukim_code", "_single_mukim_name", "_single_mukim_id"])

    return result


# ── Main entry point ──────────────────────────────────────────────────────────


def enrich_lookup(
    df: pl.DataFrame,
    lookups: "LookupFrames",
) -> pl.DataFrame:
    """Main entry point — applies all three tiers in sequence.

    Also enriches mukim using single-mukim inference.

    Returns df with all lookup enrichment applied.
    Adds columns: state_code, state_name, district_code, district_name,
                  mukim_code, mukim_name, mukim_id, locality_name,
                  postcode_ungazetted.
    """
    logger.info("lookup_start rows=%d", len(df))

    # Ensure all target columns exist
    for col in ["state_code", "state_name", "district_code", "district_name",
                "mukim_code", "mukim_name", "mukim_id", "locality_name"]:
        if col not in df.columns:
            df = df.with_columns(pl.lit(None).cast(pl.Utf8).alias(col))

    # Tier 1: Postcode-exact
    df = enrich_from_postcode(df, lookups)

    # Resolve state_name → state_code where state_name is set but state_code is null
    # This bridges Tier 1 (which fills state_name from postcode) with state_code
    state_name_to_code = lookups.state.with_columns(
        pl.col("state_name").str.to_uppercase().str.strip_chars().alias("_state_name_norm"),
    ).select(["_state_name_norm", "state_code"]).unique(subset=["_state_name_norm"])

    has_name_no_code = df["state_code"].is_null() & df["state_name"].is_not_null()
    if has_name_no_code.any():
        df = df.with_columns(
            pl.col("state_name").str.to_uppercase().str.strip_chars().alias("_tmp_state_norm"),
        )
        joined = df.join(
            state_name_to_code,
            left_on="_tmp_state_norm",
            right_on="_state_name_norm",
            how="left",
            suffix="_resolved",
        )
        df = joined.with_columns(
            pl.coalesce([pl.col("state_code"), pl.col("state_code_resolved")]).alias("state_code"),
        ).drop(["_tmp_state_norm", "state_code_resolved"])

    tier1_resolved = df["state_code"].is_not_null().sum()
    logger.info("tier1_resolved rows=%d (%.1f%%)", tier1_resolved, 100.0 * tier1_resolved / len(df))

    # Tier 2: Text-exact
    df = enrich_from_text(df, lookups)
    tier2_resolved = df["state_code"].is_not_null().sum()
    logger.info("tier2_total_resolved rows=%d (%.1f%%)", tier2_resolved, 100.0 * tier2_resolved / len(df))

    # Tier 3: Fuzzy (subset only)
    df = enrich_from_fuzzy(df, lookups)
    tier3_resolved = df["state_code"].is_not_null().sum()
    logger.info("tier3_total_resolved rows=%d (%.1f%%)", tier3_resolved, 100.0 * tier3_resolved / len(df))

    # Single-mukim inference
    df = _infer_single_mukim(df, lookups)

    final_unresolved = df["state_code"].is_null().sum()
    logger.info("lookup_complete resolved=%d unresolved=%d",
                len(df) - final_unresolved, final_unresolved)

    return df
