"""Apply state, postcode, district, and mukim lookup tables to a normalized address DataFrame."""

import pandas as pd

from ._utils import (
    DataFrame,
    _clean_text,
    _levenshtein,
    _upper_clean,
)


def _best_lookup_match(row, lookup: DataFrame, name_col: str, seg_cols: list[str], max_dist_short=1, max_dist_long=2):
    address_norm = row.get("_address_norm") or " "
    best = None
    best_key = None
    for _, lk in lookup.iterrows():
        name = _upper_clean(lk.get(name_col))
        if not name:
            continue
        exact = f" {name} " in address_norm
        distances = [_levenshtein(name, row.get(seg)) for seg in seg_cols if _clean_text(row.get(seg))]
        distance = min(distances) if distances else 999
        max_dist = max_dist_short if len(name) <= 5 else max_dist_long
        if not exact and distance > max_dist:
            continue
        key = (0 if exact else 1, distance, -len(name))
        if best_key is None or key < best_key:
            best = lk
            best_key = key
    return best


_DISTRICT_SEG_COLS = ["seg2_norm", "seg3_norm", "seg4_norm", "seg5_norm", "tail2_norm", "tail3_norm", "tail4_norm"]


def _apply_state_lookup(df: DataFrame, state_df: DataFrame) -> DataFrame:
    states = state_df.copy()
    states["_state_name_u"] = states["state_name"].map(_upper_clean)
    code_by_name = dict(zip(states["_state_name_u"], states["state_code"]))
    name_by_code = dict(zip(states["state_code"].astype(str), states["_state_name_u"]))
    df["state_name"] = df["state_name"].where(df["state_name"].notna(), df.get("postcode_state_name"))
    df["state_name"] = df["state_name"].map(_upper_clean)
    df["state_code"] = df["state_code"].where(df["state_code"].notna(), df["state_name"].map(code_by_name))
    df["state_name"] = df["state_name"].where(df["state_name"].notna(), df["state_code"].astype("string").map(name_by_code))
    df["state_lookup_valid"] = df["state_code"].astype("string").isin(set(name_by_code))
    return df


def _apply_postcode_lookup(df: DataFrame, postcode_df: DataFrame) -> DataFrame:
    lookup = postcode_df.copy()
    lookup["postcode"] = lookup["postcode"].astype("string")
    lookup["_city"] = lookup.get("city", pd.Series(index=lookup.index, dtype="string")).map(_upper_clean)
    lookup["_state"] = lookup.get("state", pd.Series(index=lookup.index, dtype="string")).map(_upper_clean)
    by_code = lookup.drop_duplicates("postcode").set_index("postcode")
    cities = df["postcode_code"].astype("string").map(by_code["_city"].to_dict())
    states = df["postcode_code"].astype("string").map(by_code["_state"].to_dict())
    df["postcode_city"] = cities
    df["postcode_state_name"] = states
    df["locality_name"] = df["locality_name"].where(df["locality_name"].notna(), cities)
    postcode_name = df.get("postcode_name", pd.Series(pd.NA, index=df.index)).where(
        df.get("postcode_name", pd.Series(pd.NA, index=df.index)).notna(), cities
    )
    df["postcode_name"] = postcode_name.map(lambda v: str(v).title() if _clean_text(v) else pd.NA)
    df["postcode"] = [
        f"{code} {name}" if _clean_text(code) and _clean_text(name) else (_clean_text(code) or pd.NA)
        for code, name in zip(df["postcode_code"], df["postcode_name"])
    ]
    return df


def _apply_district_lookup(df: DataFrame, district_df: DataFrame, district_alias_df: DataFrame | None) -> DataFrame:
    lookup = district_df.copy()
    if district_alias_df is not None and {"state_code", "district_code", "district_alias"}.issubset(district_alias_df.columns):
        aliases = district_alias_df[["state_code", "district_code", "district_alias"]].rename(columns={"district_alias": "district_name"})
        lookup = pd.concat([lookup, aliases], ignore_index=True)
    lookup["district_name"] = lookup["district_name"].map(_upper_clean)
    canonical = district_df.copy()
    canonical["district_name"] = canonical["district_name"].map(_upper_clean)
    canonical_key = canonical.set_index(["state_code", "district_code"])["district_name"].to_dict()

    # Use apply() — eliminates repeated .at[] fragmentation
    def _match_row(row) -> pd.Series:
        subset = lookup
        if _clean_text(row.get("state_code")):
            subset = subset[subset["state_code"].astype("string") == str(row["state_code"])]
        match = _best_lookup_match(row, subset, "district_name", _DISTRICT_SEG_COLS)
        district_code = row.get("district_code")
        district_name = row.get("district_name")
        if match is not None:
            if pd.isna(district_code):
                district_code = match.get("district_code")
            if pd.isna(district_name):
                district_name = match.get("district_name")
        return pd.Series({"district_code": district_code, "district_name": district_name})

    updates = df.apply(_match_row, axis=1)
    df["district_code"] = updates["district_code"]
    df["district_name"] = updates["district_name"]

    df["district_name"] = [
        canonical_key.get((str(state), str(code)), _upper_clean(name))
        for state, code, name in zip(df["state_code"], df["district_code"], df["district_name"])
    ]
    valid_keys = set(canonical_key)
    df["district_lookup_valid"] = [
        (str(state), str(code)) in valid_keys
        for state, code in zip(df["state_code"], df["district_code"])
    ]
    return df


def _apply_mukim_lookup(df: DataFrame, mukim_df: DataFrame) -> DataFrame:
    lookup = mukim_df.copy()
    lookup["mukim_name"] = lookup["mukim_name"].map(_upper_clean)
    _mukim_cols = ["state_code", "state_name", "district_code", "district_name", "mukim_code", "mukim_name", "mukim_id"]

    # First pass: fuzzy match via apply()
    def _match_row(row) -> pd.Series:
        subset = lookup
        if _clean_text(row.get("state_code")):
            subset = subset[subset["state_code"].astype("string") == str(row["state_code"])]
        if _clean_text(row.get("district_code")):
            subset = subset[subset["district_code"].astype("string") == str(row["district_code"])]
        match = _best_lookup_match(row, subset, "mukim_name", _DISTRICT_SEG_COLS)
        result = {col: row.get(col) for col in _mukim_cols}
        if match is not None:
            for col in _mukim_cols:
                if col in match.index and pd.isna(row.get(col)):
                    result[col] = match.get(col)
        return pd.Series(result)

    updates = df.apply(_match_row, axis=1)
    for col in _mukim_cols:
        if col in updates.columns:
            df[col] = updates[col]

    # Second pass: single-mukim inference — vectorized via merge
    grouped = lookup.groupby(["state_code", "district_code"], dropna=False)
    unique_lookup = grouped.filter(lambda g: len(g) == 1)[["state_code", "district_code", "mukim_code", "mukim_name", "mukim_id"]].copy()
    if not unique_lookup.empty:
        unique_lookup["state_code"] = unique_lookup["state_code"].astype("string")
        unique_lookup["district_code"] = unique_lookup["district_code"].astype("string")
        missing_mask = df["mukim_code"].isna()
        if missing_mask.any():
            df_missing = df[missing_mask][["state_code", "district_code"]].copy()
            df_missing["state_code"] = df_missing["state_code"].astype("string")
            df_missing["district_code"] = df_missing["district_code"].astype("string")
            merged = df_missing.merge(unique_lookup, on=["state_code", "district_code"], how="left")
            merged.index = df_missing.index
            for col in ["mukim_code", "mukim_name", "mukim_id"]:
                df.loc[missing_mask, col] = df.loc[missing_mask, col].where(
                    df.loc[missing_mask, col].notna(), merged[col]
                )

    valid_keys = set(zip(
        lookup["state_code"].astype("string"),
        lookup["district_code"].astype("string"),
        lookup["mukim_code"].astype("string"),
    ))
    df["mukim_lookup_valid"] = [
        (str(state), str(district), str(mukim)) in valid_keys
        for state, district, mukim in zip(df["state_code"], df["district_code"], df["mukim_code"])
    ]
    return df
