"""NASKOD assignment stage — DB-backed sequence generation.

Assigns NAS-{STATE_ABBR}-{DIST_CODE}-{TYPE}{SEQ:06d} identifiers
to new addresses using atomic DB sequence counters.

Rows that already have a NASKOD keep it (re-ingest safety).
Rows without state_code or district_code cannot be assigned.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import polars as pl

if TYPE_CHECKING:
    from nas_processor.etl.repository.naskod_repository import NaskodRepository

logger = logging.getLogger(__name__)


# ── State abbreviation mapping ────────────────────────────────────────────────

_STATE_ABBR: dict[str, str] = {
    "01": "JHR",
    "02": "KDH",
    "03": "KLN",
    "04": "MLK",
    "05": "NSN",
    "06": "PHG",
    "07": "PNG",
    "08": "PRK",
    "09": "PLS",
    "10": "SGR",
    "11": "SBH",
    "12": "SWK",
    "13": "TRG",
    "14": "KUL",
    "15": "LBN",
    "16": "PJY",
}

# ── Address type mapping ──────────────────────────────────────────────────────

_TYPE_MAP: dict[str, str] = {
    "RESIDENTIAL": "R",
    "R": "R",
    "COMMERCIAL": "C",
    "C": "C",
    "INDUSTRIAL": "I",
    "I": "I",
    "GOVERNMENT": "G",
    "G": "G",
    "LAND": "L",
    "L": "L",
}

_DEFAULT_TYPE_CODE = "R"


# ── Pure functions ────────────────────────────────────────────────────────────


def _get_address_type_code(address_type: str | None) -> str:
    """Map address_type value to single-letter code.

    Pure function, no IO.
    Defaults to 'R' (residential) if not recognized.
    """
    if not address_type:
        return _DEFAULT_TYPE_CODE
    normalized = address_type.strip().upper()
    return _TYPE_MAP.get(normalized, _DEFAULT_TYPE_CODE)


def _format_naskod(
    state_code: str,
    district_code: str,
    address_type_code: str,
    seq: int,
) -> str:
    """Format a NASKOD string.

    Example: _format_naskod('14', '01', 'R', 142) → 'NAS-KUL-01-R000142'

    Pure function, no IO.
    """
    code = state_code.zfill(2) if state_code.isdigit() else state_code
    abbr = _STATE_ABBR.get(code, code)
    dist = district_code.zfill(2) if district_code.isdigit() else district_code
    return f"NAS-{abbr}-{dist}-{address_type_code}{seq:06d}"


# ── Main assignment function ──────────────────────────────────────────────────


def assign_naskod(
    df: pl.DataFrame,
    *,
    naskod_repo: "NaskodRepository",
) -> pl.DataFrame:
    """Assign NASKODs to rows without one.

    Steps:
    1. If 'naskod' column not in df, add it as all-null Utf8
    2. Identify new rows: naskod IS NULL AND state_code IS NOT NULL
       AND district_code IS NOT NULL
    3. Determine address_type_code per row
    4. Group new rows by (state_code, district_code, address_type_code)
    5. For each group: call naskod_repo.allocate_many() to claim ranges
    6. Assign seq numbers within each group
    7. Format NASKOD strings
    8. Merge back into main DataFrame

    Rows where naskod was already set → unchanged.
    Rows without state/district → naskod remains null.

    NOTE: The grouping + range claim minimizes DB round trips.
    Max ~800 groups per chunk (16 states × ~10 districts × 5 types).
    """
    # 1. Ensure naskod column exists
    if "naskod" not in df.columns:
        df = df.with_columns(pl.lit(None).cast(pl.Utf8).alias("naskod"))

    # Count categories for logging
    already_assigned = df["naskod"].is_not_null().sum()

    # 2. Identify rows that need NASKOD assignment
    has_state = pl.col("state_code").is_not_null() if "state_code" in df.columns else pl.lit(False)
    has_district = pl.col("district_code").is_not_null() if "district_code" in df.columns else pl.lit(False)
    needs_naskod = df["naskod"].is_null() & has_state & has_district

    # Evaluate the mask
    needs_mask = df.select(needs_naskod.alias("_needs")).to_series()
    eligible_count = needs_mask.sum()
    cannot_assign = len(df) - already_assigned - eligible_count

    if eligible_count == 0:
        logger.info(
            "naskod_skip already_assigned=%d cannot_assign=%d (missing hierarchy)",
            already_assigned, cannot_assign,
        )
        return df

    # 3. Extract eligible rows with their index for merge-back
    eligible = df.with_row_index("_row_idx").filter(needs_mask)

    # Determine address_type_code per row
    if "address_type" in eligible.columns:
        type_codes = eligible["address_type"].map_elements(
            _get_address_type_code, return_dtype=pl.Utf8
        )
    else:
        type_codes = pl.Series("_type_code", [_DEFAULT_TYPE_CODE] * len(eligible))

    eligible = eligible.with_columns(type_codes.alias("_type_code"))

    # 4. Group by (state_code, district_code, _type_code)
    groups = (
        eligible
        .group_by(["state_code", "district_code", "_type_code"])
        .agg(pl.col("_row_idx"))
    )

    # 5. Allocate sequence ranges from DB (one transaction)
    alloc_requests = []
    group_data = groups.to_dicts()
    for g in group_data:
        alloc_requests.append({
            "state_code": g["state_code"],
            "district_code": g["district_code"],
            "address_type": g["_type_code"],
            "count": len(g["_row_idx"]),
        })

    # Batch allocate
    allocated = naskod_repo.allocate_many(alloc_requests)

    # 6-7. Assign sequence numbers and format NASKODs
    naskod_assignments: dict[int, str] = {}  # _row_idx → naskod string

    for g in group_data:
        state = g["state_code"]
        district = g["district_code"]
        type_code = g["_type_code"]
        row_indices = g["_row_idx"]

        key = (state, district, type_code)
        start_seq = allocated[key]

        for offset, row_idx in enumerate(row_indices):
            seq = start_seq + offset
            naskod_str = _format_naskod(state, district, type_code, seq)
            naskod_assignments[row_idx] = naskod_str

    # 8. Build update DataFrame and merge back
    if naskod_assignments:
        update_df = pl.DataFrame({
            "_row_idx": list(naskod_assignments.keys()),
            "_new_naskod": list(naskod_assignments.values()),
        }).cast({"_row_idx": pl.UInt32, "_new_naskod": pl.Utf8})

        # Join and coalesce
        df = df.with_row_index("_row_idx").join(
            update_df, on="_row_idx", how="left"
        ).with_columns(
            pl.coalesce([pl.col("naskod"), pl.col("_new_naskod")]).alias("naskod"),
        ).drop(["_row_idx", "_new_naskod"])

    logger.info(
        "naskod_complete assigned=%d already_had=%d cannot_assign=%d",
        len(naskod_assignments), already_assigned, cannot_assign,
    )

    return df
