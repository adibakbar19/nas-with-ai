from typing import Optional

import pandas as pd

from ..address._utils import _levenshtein, _upper_clean


def _best(row, lookup_df: pd.DataFrame, name_col: str, seg_cols: list[str], max_dist_short: int, max_dist_long: int):
    address_norm = row.get("_address_norm") or " "
    best_row = None
    best_key = None
    for _, lookup in lookup_df.iterrows():
        name = _upper_clean(lookup.get(name_col))
        if not name:
            continue
        exact = f" {name} " in address_norm
        distances = [_levenshtein(name, _upper_clean(row.get(col))) for col in seg_cols if _upper_clean(row.get(col))]
        distance = min(distances) if distances else 999
        max_dist = max_dist_short if len(name) <= 5 else max_dist_long
        if not exact and distance > max_dist:
            continue
        key = (0 if exact else 1, distance, -len(name))
        if best_key is None or key < best_key:
            best_key = key
            best_row = lookup
    return best_row


def match_lookup_exact(df: pd.DataFrame, lookup_df: pd.DataFrame, name_col: str, cols_to_keep: list[str]) -> pd.DataFrame:
    out = df.copy()
    for col in cols_to_keep:
        if col not in out.columns:
            out[col] = pd.NA
    lookup = lookup_df.copy()
    lookup[name_col] = lookup[name_col].map(_upper_clean)
    for idx, row in out.iterrows():
        address_norm = row.get("_address_norm") or " "
        candidates = lookup[lookup[name_col].map(lambda name: bool(name and f" {name} " in address_norm))]
        if candidates.empty:
            continue
        match = candidates.assign(_len=candidates[name_col].str.len()).sort_values("_len", ascending=False).iloc[0]
        for col in cols_to_keep:
            out.at[idx, col] = match.get(col)
    return out


def match_lookup_fuzzy(
    df: pd.DataFrame,
    lookup_df: pd.DataFrame,
    name_col: str,
    cols_to_keep: list[str],
    *,
    state_name_col: Optional[str] = None,
    lookup_state_name_col: Optional[str] = None,
    state_code_col: Optional[str] = None,
    district_code_col: Optional[str] = None,
    lookup_state_code_col: Optional[str] = None,
    lookup_district_code_col: Optional[str] = None,
    seg_cols: Optional[list[str]] = None,
    max_dist_short: int = 1,
    max_dist_long: int = 2,
) -> pd.DataFrame:
    out = df.copy()
    for col in cols_to_keep:
        if col not in out.columns:
            out[col] = pd.NA
    seg_cols = seg_cols or ["seg2_norm", "seg3_norm", "seg4_norm", "seg5_norm"]
    for idx, row in out.iterrows():
        subset = lookup_df
        if state_name_col and lookup_state_name_col and state_name_col in out.columns and pd.notna(row.get(state_name_col)):
            subset = subset[subset[lookup_state_name_col].map(_upper_clean) == _upper_clean(row.get(state_name_col))]
        if state_code_col and lookup_state_code_col and pd.notna(row.get(state_code_col)):
            subset = subset[subset[lookup_state_code_col].astype("string") == str(row.get(state_code_col))]
        if district_code_col and lookup_district_code_col and pd.notna(row.get(district_code_col)):
            subset = subset[subset[lookup_district_code_col].astype("string") == str(row.get(district_code_col))]
        match = _best(row, subset, name_col, seg_cols, max_dist_short, max_dist_long)
        if match is None:
            continue
        for col in cols_to_keep:
            out.at[idx, col] = match.get(col)
    return out


def match_mukim_fuzzy(df: pd.DataFrame, mukim_df: pd.DataFrame) -> pd.DataFrame:
    return match_lookup_fuzzy(
        df,
        mukim_df,
        "mukim_name",
        ["mukim_code", "mukim_name", "mukim_id", "district_code", "district_name", "state_code", "state_name"],
        state_code_col="state_code",
        district_code_col="district_code",
        lookup_state_code_col="state_code",
        lookup_district_code_col="district_code",
        seg_cols=["seg2_norm", "seg3_norm", "seg4_norm", "seg5_norm", "tail2_norm", "tail3_norm", "tail4_norm"],
        max_dist_short=1,
        max_dist_long=2,
    )
