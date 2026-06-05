"""Address text normalisation — pure Polars, fully vectorized.

Replaces the old _parsing.py + normalize.py combination.
All operations use pl.Series.str.* methods or pl.Expr — no row iteration.
"""

# NOTE: Polars uses Rust's regex crate which does NOT support
# lookahead (?=...) or lookbehind (?<=...) assertions.
# Use word boundaries \b instead of lookaround for boundary matching.
# See: https://docs.rs/regex/latest/regex/#syntax

from __future__ import annotations

import polars as pl


# ── Abbreviation replacement pairs (applied via str.replace) ──────────────────

_ABBREVIATIONS: list[tuple[str, str]] = [
    (r"\bJLN\b", "JALAN"),
    (r"\bLRG\b", "LORONG"),
    (r"\bPSN\b", "PERSIARAN"),
    (r"\bKG\b", "KAMPUNG"),
    (r"\bTMN\b", "TAMAN"),
    (r"\bBDR\b", "BANDAR"),
    (r"\bSGN\b", "SUNGAI"),
    (r"\bBT\b", "BUKIT"),
    (r"\bPJY\b", "PUTRAJAYA"),
    (r"\bSJ\b", "SUBANG JAYA"),
]

# Noise tokens to remove (standalone words)
_NOISE_PATTERNS: list[str] = [
    r"\bMALAYSIA\b",
    r"\bW\.?P\.?\s*",
    r"\bWILAYAH PERSEKUTUAN\b",
]


# ── Public functions ──────────────────────────────────────────────────────────


def detect_address_column(df: pl.DataFrame, config: dict) -> str:
    """Find which column contains the primary address text.

    Checks config['address_column_candidates'] list first.
    Falls back to first column containing 'address', 'alamat', 'addr'
    in the name (case-insensitive).

    Raises ValueError if no address column found.
    Returns the column name as a string.
    """
    candidates = config.get("address_column_candidates", [
        "address", "full_address", "fulladdress", "addr",
        "alamat_penuh", "alamat", "old_address", "raw_address",
        "source_address", "alamat_baru",
    ])

    # Exact match (case-insensitive) against candidates
    col_lower_map = {c.lower(): c for c in df.columns}
    for candidate in candidates:
        if candidate.lower() in col_lower_map:
            return col_lower_map[candidate.lower()]

    # Fuzzy fallback: first column with address/alamat/addr in name
    for col in df.columns:
        lower = col.lower()
        if any(token in lower for token in ("address", "alamat", "addr")):
            # Skip known non-address columns
            if any(skip in lower for skip in ("address_type", "address_source", "address_id")):
                continue
            return col

    raise ValueError(
        f"No address column found. Columns: {df.columns}. "
        f"Set config['address_column_candidates'] or rename a column."
    )


def normalise_text(series: pl.Series) -> pl.Series:
    """Vectorized text normalization on a Polars Series.

    Operations (all vectorized via str.*):
    - Uppercase
    - Collapse multiple spaces to single space
    - Strip leading/trailing whitespace
    - Normalize separators (semicolons/pipes → comma)
    - Remove noise tokens (MALAYSIA, W.P., etc.)
    - Expand common abbreviations (JLN → JALAN, etc.)
    """
    s = series.cast(pl.Utf8)

    # Uppercase
    s = s.str.to_uppercase()

    # Normalize separators: semicolons and pipes → comma
    s = s.str.replace_all(r"[;|]+", ",")

    # Collapse multiple commas/spaces around commas
    s = s.str.replace_all(r"\s*,\s*", ", ")

    # Remove noise tokens
    for pattern in _NOISE_PATTERNS:
        s = s.str.replace_all(pattern, "")

    # Expand abbreviations
    for pattern, replacement in _ABBREVIATIONS:
        s = s.str.replace_all(pattern, replacement)

    # Collapse multiple spaces
    s = s.str.replace_all(r"\s{2,}", " ")

    # Strip
    s = s.str.strip_chars()

    return s


def extract_postcode(series: pl.Series) -> pl.Series:
    """Extract 5-digit Malaysian postcode from address text.

    Uses str.extract() with a word-boundary pattern (no lookbehind needed).
    Matches exactly 5 consecutive digits bounded by non-digit context.
    Returns pl.Series of Utf8 (null if no postcode found).
    """
    # \b(\d{5})\b works for digit sequences bordered by word boundaries.
    # For cases like "123456" — 6 digits won't match exactly 5 at a boundary.
    # We use a capture group inside a non-digit boundary pattern.
    return series.str.extract(r"\b(\d{5})\b", group_index=1)


def extract_premise_no(series: pl.Series) -> pl.Series:
    """Extract premise/unit number from normalized address.

    Patterns matched (in priority order):
    - NO.? followed by alphanumeric+dashes: NO 12, NO. 12-A
    - Standalone leading number-letter pattern: 12A, 5-2-1

    Uses str.extract() — fully vectorized.
    Returns pl.Series of Utf8 (null if not found).
    """
    # Try "NO" pattern first
    result = series.str.extract(r"\bNO\.?\s*([0-9A-Z][\w/-]*)", group_index=1)

    # For rows where NO pattern didn't match, try leading number
    fallback = series.str.extract(r"^([0-9]+[A-Z]?(?:-[0-9A-Z]+)*)\b", group_index=1)

    # Use NO-pattern result where available, else fallback
    return result.fill_null(fallback)


def extract_street_name(series: pl.Series) -> pl.Series:
    """Extract street name from normalized address.

    Looks for patterns like JALAN/LORONG/PERSIARAN/LEBUH followed by a name.
    The name extends until the next comma or end of string.

    Uses str.extract() — fully vectorized.
    Returns pl.Series of Utf8 (null if not found).
    """
    return series.str.extract(
        r"\b((?:JALAN|LORONG|PERSIARAN|LEBUHRAYA|LEBUH|TAMAN|KAMPUNG)\s+[^,]+)",
        group_index=1,
    )


def normalise_chunk(
    df: pl.DataFrame,
    *,
    config: dict,
    address_col: str | None = None,
) -> pl.DataFrame:
    """Apply all normalisation to a raw chunk.

    Steps:
    1. Detect address column (or use provided address_col)
    2. Set source_address_old = original address value
    3. Normalise address text → address_norm column
    4. Extract postcode from address_norm if not already present
    5. Extract premise_no, street_name from address_norm
    6. Set address_for_lookup = noise-removed text for matching
    7. Set source_address_new = address_norm

    Returns df with new columns added. All existing columns preserved.
    """
    if address_col is None:
        address_col = detect_address_column(df, config)

    # 1. Preserve original address
    result = df.with_columns(
        pl.col(address_col).alias("source_address_old"),
    )

    # 2. Normalise text
    normalised = normalise_text(result["source_address_old"])

    result = result.with_columns(
        normalised.alias("address_norm"),
        normalised.alias("source_address_new"),
    )

    # 3. Extract postcode (only if not already present as a column)
    if "postcode_raw" not in result.columns:
        result = result.with_columns(
            extract_postcode(result["address_norm"]).alias("postcode_raw"),
        )

    # 4. Extract premise_no
    if "premise_no" not in result.columns:
        result = result.with_columns(
            extract_premise_no(result["address_norm"]).alias("premise_no"),
        )

    # 5. Extract street_name
    if "street_name" not in result.columns:
        result = result.with_columns(
            extract_street_name(result["address_norm"]).alias("street_name"),
        )

    # 6. Build address_for_lookup: remove postcode and premise for cleaner matching
    result = result.with_columns(
        result["address_norm"]
        .str.replace_all(r"\b\d{5}\b", "")  # remove postcode
        .str.replace_all(r"\bNO\.?\s*[0-9A-Z][\w/-]*", "")  # remove premise
        .str.replace_all(r"^\s*,\s*", "")  # leading comma
        .str.replace_all(r"\s*,\s*$", "")  # trailing comma
        .str.replace_all(r"\s{2,}", " ")  # collapse spaces
        .str.strip_chars()
        .alias("address_for_lookup"),
    )

    return result
