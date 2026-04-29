"""Public API for address normalization: clean, enrich, and validate."""

from typing import Optional

import pandas as pd

from ._lookups import (
    _apply_district_lookup,
    _apply_mukim_lookup,
    _apply_postcode_lookup,
    _apply_state_lookup,
)
from ._parsing import (
    _compose_structured_address,
    _parse_address_row,
    _standardize_common_columns,
)
from ._spatial import _apply_spatial_enrichment
from ._utils import (
    DataFrame,
    _LOCALITY_RE,
    _MUKIM_RE,
    _STREET_RE,
    _clean_text,
    _config_bool,
    _detect_address_col,
    _detect_col,
    _ensure_column,
    _ensure_spatial_defaults,
    _extract_postcode,
    _truthy,
    _upper_clean,
)
from ._validation import _derive_address_type, _finalize_cleaned_addresses, validate_addresses


def _resolve_sub_localities(row) -> pd.Series:
    """Resolve ambiguous sub-locality segments into street/mukim/locality/clean sub fields."""
    sub1 = _upper_clean(row.get("sub_locality_1"))
    sub2 = _upper_clean(row.get("sub_locality_2"))
    sub_values = [v for v in [sub1, sub2] if v]

    street = row.get("street_name")
    if not _clean_text(street):
        street = next((v for v in sub_values if _STREET_RE.search(v)), street)

    mukim = row.get("mukim_name")
    if not _clean_text(mukim):
        candidates = [row.get("locality_name"), sub2, sub1]
        mukim = next(
            (_upper_clean(v) for v in candidates if v and _MUKIM_RE.search(_upper_clean(v) or "")),
            mukim,
        )

    locality = row.get("locality_name")
    if locality and _MUKIM_RE.search(_upper_clean(locality) or ""):
        locality = pd.NA
    if not _clean_text(locality):
        locality = next((v for v in sub_values if _LOCALITY_RE.search(v)), locality)

    exclude = {
        _upper_clean(street),
        _upper_clean(row.get("building_name")),
        _upper_clean(locality),
        _upper_clean(mukim),
    }
    clean_subs = [
        v for v in sub_values
        if v
        and not _STREET_RE.search(v)
        and not _LOCALITY_RE.search(v)
        and not _MUKIM_RE.search(v)
        and v not in exclude
    ]
    return pd.Series({
        "street_name": street,
        "mukim_name": mukim,
        "locality_name": locality,
        "sub_locality_1": clean_subs[0] if len(clean_subs) > 0 else pd.NA,
        "sub_locality_2": clean_subs[1] if len(clean_subs) > 1 else pd.NA,
        "sub_locality_levels": clean_subs,
    })


def clean_addresses(
    df: DataFrame,
    address_col: Optional[str] = None,
    *,
    config: dict,
    lookups=None,
    postcode_boundaries=None,
    admin_boundaries=None,
    pbt_boundaries=None,
    include_spatial: bool = True,
    finalize: bool = True,
) -> DataFrame:
    if lookups is None:
        raise ValueError("lookups must be provided by the orchestration layer.")

    out = _standardize_common_columns(df.copy(), config)
    out = out.replace({float("nan"): pd.NA})

    address_candidates = config.get(
        "address_columns",
        ["address", "full_address", "fulladdress", "addr", "alamat_penuh", "alamat", "old_address", "raw_address"],
    )
    new_address_candidates = config.get("new_address_columns", ["new_address", "alamat_baru", "alamatbaru", "address_new"])
    if address_col is None:
        address_col = _detect_address_col(out, address_candidates)
    new_address_col = _detect_col(out, new_address_candidates)
    if not address_col:
        structured_cols = config.get(
            "structured_address_columns",
            ["premise_no", "house_no", "lot_no", "unit_no", "building_name", "street_name",
             "locality_name", "mukim_name", "district_name", "state_name", "postcode", "country"],
        )
        usable = [col for col in structured_cols if col in out.columns]
        if usable:
            out = _compose_structured_address(out, source_cols=usable, target_col="_structured_source_address")
            address_col = "_structured_source_address"
    if not address_col:
        raise ValueError(
            "Address column not found and no structured address columns available. "
            "Provide address_col or update --config (address_columns/structured_address_columns). "
            f"Columns found: {list(out.columns)}"
        )

    lat_col = _detect_col(out, config.get("latitude_columns", ["latitude", "lat", "y", "coord_lat", "lat_deg", "latitude_deg"]))
    lon_col = _detect_col(out, config.get("longitude_columns", ["longitude", "lon", "lng", "long", "x", "coord_lon", "lon_deg", "lng_deg", "longitude_deg"]))
    if lat_col and lat_col != "latitude" and "latitude" not in out.columns:
        out = out.rename(columns={lat_col: "latitude"})
    if lon_col and lon_col != "longitude" and "longitude" not in out.columns:
        out = out.rename(columns={lon_col: "longitude"})
    _ensure_column(out, "latitude")
    _ensure_column(out, "longitude")
    out["latitude"] = pd.to_numeric(out["latitude"], errors="coerce")
    out["longitude"] = pd.to_numeric(out["longitude"], errors="coerce")
    out["geom"] = [
        f"POINT({lon} {lat})" if pd.notna(lat) and pd.notna(lon) else pd.NA
        for lat, lon in zip(out["latitude"], out["longitude"])
    ]
    out["geometry"] = out["geom"]
    if "record_id" not in out.columns:
        out["record_id"] = [f"row-{i + 1}" for i in range(len(out))]
    out["_row_id"] = range(len(out))

    out["source_address_old"] = out[address_col].map(_clean_text)
    out["source_address_new"] = out[new_address_col].map(_clean_text) if new_address_col else pd.NA
    out["address_for_parse"] = out["source_address_new"].where(out["source_address_new"].notna(), out["source_address_old"])
    out["address_source"] = out["source_address_new"].notna().map(lambda ok: "new" if ok else "old")
    out["is_missing_address"] = out["address_for_parse"].isna()

    parsed = out["address_for_parse"].map(_parse_address_row)
    parsed_df = pd.DataFrame(parsed.tolist(), index=out.index)
    preserve_cols = [
        "premise_no", "lot_no", "unit_no", "floor_no", "building_name",
        "street_name", "locality_name", "mukim_name", "district_name",
        "state_name", "postcode", "address_type",
    ]
    originals = {col: out[col].copy() for col in preserve_cols if col in out.columns}
    for col in parsed_df.columns:
        out[col] = parsed_df[col]
    for col, values in originals.items():
        if col == "lot_no" and not _config_bool(config, "preserve_lot_no_from_source", False):
            continue
        cleaned = values.map(_clean_text)
        if _config_bool(config, "prefer_structured_source_fields", False):
            out[col] = cleaned.where(cleaned.notna(), out[col])
        else:
            out[col] = out[col].where(out[col].notna(), cleaned)
    out["postcode_code"] = out["postcode"].map(_extract_postcode)

    for col in [
        "street_name", "sub_locality_1", "sub_locality_2", "locality_name",
        "mukim_name", "state_name", "state_code", "district_name", "district_code",
        "mukim_code", "mukim_id", "pbt_id", "pbt_name", "_locality_state_name",
    ]:
        _ensure_column(out, col)
    out["sub_locality_levels"] = pd.Series([[] for _ in range(len(out))], index=out.index, dtype=object)

    resolved = out.apply(_resolve_sub_localities, axis=1, result_type="expand")
    for col in resolved.columns:
        out[col] = resolved[col]

    out = _apply_postcode_lookup(out, lookups.postcode)
    out = _apply_state_lookup(out, lookups.state)
    out = _apply_district_lookup(out, lookups.district, lookups.district_alias)
    out = _apply_mukim_lookup(out, lookups.mukim)

    out["floor_level"] = out.get("floor_level", pd.Series(pd.NA, index=out.index)).where(
        out.get("floor_level", pd.Series(pd.NA, index=out.index)).notna(),
        out.get("floor_no", pd.Series(pd.NA, index=out.index)),
    )
    out["country"] = out.get("country", pd.Series(pd.NA, index=out.index)).fillna("MALAYSIA")
    out["address_type_raw"] = out.get("address_type", pd.Series(pd.NA, index=out.index)).map(_clean_text)
    out["address_type"] = out.apply(_derive_address_type, axis=1)

    if include_spatial:
        out = _apply_spatial_enrichment(
            out,
            config=config,
            postcode_boundaries=postcode_boundaries,
            admin_boundaries=admin_boundaries,
            pbt_boundaries=pbt_boundaries,
        )
    else:
        out = _ensure_spatial_defaults(out)

    return _finalize_cleaned_addresses(out) if finalize else out


def clean_text_addresses(
    df: DataFrame,
    address_col: Optional[str] = None,
    *,
    config: dict,
    lookups=None,
) -> DataFrame:
    return clean_addresses(
        df,
        address_col=address_col,
        config=config,
        lookups=lookups,
        include_spatial=False,
        finalize=False,
    )


def enrich_spatial_components(
    df: DataFrame,
    *,
    config: dict,
    postcode_boundaries=None,
    admin_boundaries=None,
    pbt_boundaries=None,
) -> DataFrame:
    return _finalize_cleaned_addresses(
        _apply_spatial_enrichment(
            df,
            config=config,
            postcode_boundaries=postcode_boundaries,
            admin_boundaries=admin_boundaries,
            pbt_boundaries=pbt_boundaries,
        )
    )
