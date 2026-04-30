"""Spatial enrichment via postcode, admin, and PBT boundary joins."""

import pandas as pd

from ._utils import (
    DataFrame,
    _clean_text,
    _ensure_spatial_defaults,
    _ensure_column,
    _extract_postcode,
    _upper_clean,
)


def _pick_boundary_col(df, candidates: list[str]) -> str | None:
    cols = {str(c).lower(): c for c in df.columns}
    for cand in candidates:
        if str(cand).lower() in cols:
            return cols[str(cand).lower()]
    return None


def _spatial_join_first(df: DataFrame, boundaries, columns: dict[str, str]) -> pd.DataFrame:
    import geopandas as gpd
    from shapely.geometry import Point

    valid = df["latitude"].notna() & df["longitude"].notna()
    if not valid.any():
        return pd.DataFrame(index=df.index)
    points = gpd.GeoDataFrame(
        df.loc[valid].copy(),
        geometry=[Point(lon, lat) for lon, lat in zip(df.loc[valid, "longitude"], df.loc[valid, "latitude"])],
        crs="EPSG:4326",
    )
    # Rename boundary columns to target names before sjoin to avoid conflicts
    # with same-named columns in the address data (e.g. "postcode", "state").
    rename_map = {source: target for target, source in columns.items()}
    geom_col = boundaries.geometry.name
    right = boundaries[[*columns.values(), geom_col]].rename(columns=rename_map)
    joined = gpd.sjoin(points, right, how="left", predicate="within")
    joined = joined[~joined.index.duplicated(keep="first")]
    out = pd.DataFrame(index=df.index)
    for target in columns:
        if target in joined.columns:
            out.loc[joined.index, target] = joined[target]
    return out


def _apply_spatial_enrichment(
    df: DataFrame,
    *,
    config: dict,
    postcode_boundaries=None,
    admin_boundaries=None,
    pbt_boundaries=None,
) -> DataFrame:
    out = _ensure_spatial_defaults(df.copy())
    for col in ["latitude", "longitude"]:
        _ensure_column(out, col)
        out[col] = pd.to_numeric(out[col], errors="coerce")

    boundary_source = str(config.get("boundary_source", "db")).strip().lower()
    if boundary_source != "db":
        raise ValueError("Production ETL requires boundary_source=db. File-based boundary mode is not supported.")

    # Postcode boundary — fills postcode_code/city/state where coordinates are present
    if postcode_boundaries is not None and not postcode_boundaries.empty:
        boundaries = postcode_boundaries
        postcode_col = _pick_boundary_col(boundaries, ["postcode"])
        city_col = _pick_boundary_col(boundaries, ["city"])
        state_col = _pick_boundary_col(boundaries, ["state"])
        if not postcode_col or not city_col or not state_col:
            raise ValueError("Postcode boundary table is missing required postcode/city/state columns.")
        hits = _spatial_join_first(out, boundaries, {"_b_postcode": postcode_col, "_b_city": city_col, "_b_state": state_col})
        if not hits.empty:
            b_postcode = hits["_b_postcode"].map(_extract_postcode)
            existing = out["postcode_code"].map(_extract_postcode)
            conflict = b_postcode.notna() & existing.notna() & (b_postcode != existing)
            apply = b_postcode.notna() & ~conflict
            out["_postcode_boundary_conflict"] = conflict.fillna(False)
            out["_postcode_from_boundary"] = apply.fillna(False)
            out.loc[apply, "postcode_code"] = b_postcode[apply]
            out.loc[apply & out["postcode_city"].isna(), "postcode_city"] = hits.loc[apply, "_b_city"].map(_upper_clean)
            out.loc[apply & out["postcode_state_name"].isna(), "postcode_state_name"] = hits.loc[apply, "_b_state"].map(_upper_clean)

    # Admin boundary — fills state/district/mukim codes and names
    if admin_boundaries is not None:
        state_b, district_b, mukim_b = admin_boundaries
        if not state_b.empty and not district_b.empty and not mukim_b.empty:
            state_name = _pick_boundary_col(state_b, ["state_name"])
            state_code = _pick_boundary_col(state_b, ["state_code"])
            district_name = _pick_boundary_col(district_b, ["district_name"])
            district_code = _pick_boundary_col(district_b, ["district_code"])
            mukim_name = _pick_boundary_col(mukim_b, ["mukim_name"])
            mukim_code = _pick_boundary_col(mukim_b, ["mukim_code"])
            mukim_id = _pick_boundary_col(mukim_b, ["mukim_id"])
            if not state_name or not state_code or not district_name or not district_code or not mukim_name or not mukim_code:
                raise ValueError("Admin boundary tables are missing required state/district/mukim columns.")

            hits = _spatial_join_first(out, state_b, {"_b_state_name": state_name, "_b_state_code": state_code})
            apply = hits.get("_b_state_code", pd.Series(index=out.index)).notna()
            conflict = apply & out["state_code"].notna() & (out["state_code"].astype("string") != hits["_b_state_code"].astype("string"))
            out["_state_boundary_conflict"] = conflict.fillna(False)
            out["_state_from_boundary"] = (apply & ~conflict).fillna(False)
            mask = out["_state_from_boundary"]
            out.loc[mask, "state_code"] = hits.loc[mask, "_b_state_code"]
            out.loc[mask, "state_name"] = hits.loc[mask, "_b_state_name"].map(_upper_clean)

            hits = _spatial_join_first(out, district_b, {"_b_district_name": district_name, "_b_district_code": district_code})
            apply = hits.get("_b_district_code", pd.Series(index=out.index)).notna()
            conflict = apply & out["district_code"].notna() & (out["district_code"].astype("string") != hits["_b_district_code"].astype("string"))
            out["_district_boundary_conflict"] = conflict.fillna(False)
            out["_district_from_boundary"] = (apply & ~conflict).fillna(False)
            mask = out["_district_from_boundary"]
            out.loc[mask, "district_code"] = hits.loc[mask, "_b_district_code"]
            out.loc[mask, "district_name"] = hits.loc[mask, "_b_district_name"].map(_upper_clean)

            mukim_cols = {"_b_mukim_name": mukim_name, "_b_mukim_code": mukim_code}
            if mukim_id:
                mukim_cols["_b_mukim_id"] = mukim_id
            hits = _spatial_join_first(out, mukim_b, mukim_cols)
            apply = hits.get("_b_mukim_code", pd.Series(index=out.index)).notna()
            conflict = apply & out["mukim_code"].notna() & (out["mukim_code"].astype("string") != hits["_b_mukim_code"].astype("string"))
            out["_mukim_boundary_conflict"] = conflict.fillna(False)
            out["_mukim_from_boundary"] = (apply & ~conflict).fillna(False)
            mask = out["_mukim_from_boundary"]
            out.loc[mask, "mukim_code"] = hits.loc[mask, "_b_mukim_code"]
            out.loc[mask, "mukim_name"] = hits.loc[mask, "_b_mukim_name"].map(_upper_clean)
            if "_b_mukim_id" in hits:
                out.loc[mask, "mukim_id"] = hits.loc[mask, "_b_mukim_id"]

    # PBT boundary — fills pbt_id/pbt_name
    if pbt_boundaries is not None and not pbt_boundaries.empty:
        boundaries = pbt_boundaries
        pbt_id_col = _pick_boundary_col(boundaries, ["pbt_id"])
        pbt_name_col = _pick_boundary_col(boundaries, ["pbt_name"])
        if not pbt_name_col:
            raise ValueError("PBT boundary table is missing pbt_name.")
        pbt_cols = {"_pbt_name": pbt_name_col}
        if pbt_id_col:
            pbt_cols["_pbt_id"] = pbt_id_col
        hits = _spatial_join_first(out, boundaries, pbt_cols)
        if "_pbt_id" in hits:
            out["pbt_id"] = out["pbt_id"].where(out["pbt_id"].notna(), hits["_pbt_id"])
        out["pbt_name"] = out["pbt_name"].where(out["pbt_name"].notna(), hits["_pbt_name"])

    return out
