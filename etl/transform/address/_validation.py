"""Address finalization, confidence scoring, canonical form, and validation splitting."""

import re

import pandas as pd

from ._utils import (
    DataFrame,
    _LOCALITY_RE,
    _MUKIM_RE,
    _STREET_RE,
    _clean_text,
    _ensure_column,
    _ensure_spatial_defaults,
    _extract_postcode,
    _falsey_bool,
    _truthy,
    _upper_clean,
)


def _confidence_band(score: int) -> str:
    if score >= 95:
        return "VERIFIED"
    if score >= 85:
        return "HIGH"
    if score >= 70:
        return "REVIEW"
    return "REJECT"


def _derive_address_type(row) -> str:
    signal = " ".join(_upper_clean(row.get(col)) or "" for col in ["building_name", "address_clean", "address_type_raw"])
    patterns = [
        ("HIGHRISE", r"(APARTMENT|APARTMEN|PANGSAPURI|KONDOMINIUM|KONDO|CONDO|FLAT|RUMAH PANGSA|MENARA|TOWER|RESIDENSI|RESIDENCE|SOHO|SOFO|SERVICED RESIDENCE|PPR|PPA1M)"),
        ("INSTITUTIONAL", r"(HOSPITAL|SCHOOL|MOSQUE|SURAU|POLICE STATION|BALAI POLIS|GOVERNMENT BUILDING|PEJABAT)"),
        ("INDUSTRIAL", r"(FACTORY|INDUSTRIAL LOT|PERINDUSTRIAN|INDUSTRI|WAREHOUSE|GUDANG)"),
        ("COMMERCIAL", r"(SHOPLOT|SHOP LOT|SHOPPING MALL|MALL|PLAZA|KOMPLEKS|OFFICE TOWER|WISMA|ARKED)"),
        ("RURAL", r"(ORANG ASLI SETTLEMENT|KAMPUNG|KG|FELDA|FELCRA|LADANG|ESTET|RKT|RPT|RPS)"),
        ("RESIDENTIAL", r"(BUNGALOW|SEMI-D|SEMI D|TERRACE HOUSE|KAMPUNG HOUSE|FELDA HOUSE|ESTATE HOUSE|RUMAH)"),
    ]
    for label, pattern in patterns:
        if re.search(pattern, signal):
            return label
    return "UNKNOWN"


def _canonical_address(row) -> str | None:
    street = _clean_text(row.get("street_name"))
    prefix = _clean_text(row.get("street_name_prefix"))
    if street and prefix and not _STREET_RE.match(street):
        street = f"{prefix} {street}"
    parts = [
        f"LOT {_clean_text(row.get('lot_no'))}" if _clean_text(row.get("lot_no")) else None,
        f"NO {_clean_text(row.get('premise_no'))}" if _clean_text(row.get("premise_no")) else None,
        f"UNIT {_clean_text(row.get('unit_no'))}" if _clean_text(row.get("unit_no")) else None,
        f"LEVEL {_clean_text(row.get('floor_level'))}" if _clean_text(row.get("floor_level")) else None,
        _clean_text(row.get("building_name")),
        street,
        _clean_text(row.get("sub_locality_1")),
        _clean_text(row.get("sub_locality_2")),
        _clean_text(row.get("locality_name")),
        _clean_text(row.get("postcode_code") or row.get("postcode")),
        _clean_text(row.get("state_name")),
    ]
    parts = [p for p in parts if p]
    return ", ".join(parts).upper() if parts else _upper_clean(row.get("address_clean"))


def _finalize_cleaned_addresses(df: DataFrame) -> DataFrame:
    out = _ensure_spatial_defaults(df.copy())
    for col in ["postcode_state_name", "_locality_state_name", "_district_from_candidate", "_mukim_from_candidate"]:
        _ensure_column(out, col)

    out["_state_postcode_conflict"] = (
        out["postcode_code"].notna()
        & out["state_name"].notna()
        & out["postcode_state_name"].notna()
        & (out["state_name"].map(_upper_clean) != out["postcode_state_name"].map(_upper_clean))
    )

    # Vectorized confidence signals — one Series per signal, no iterrows
    sub1 = out["sub_locality_1"].map(lambda v: _upper_clean(v) or "")
    sub2 = out["sub_locality_2"].map(lambda v: _upper_clean(v) or "")

    score = (
        out["postcode_code"].map(lambda v: bool(_extract_postcode(v))).astype(int) * 10
        + out["state_code"].notna().astype(int) * 20
        + out["district_code"].notna().astype(int) * 20
        + out["mukim_code"].notna().astype(int) * 15
        + out["locality_name"].notna().astype(int) * 10
        + out["pbt_id"].notna().astype(int) * 10
        + out["_postcode_from_boundary"].astype(int) * 10
        + (
            out["postcode_state_name"].notna()
            & (out["state_name"].map(_upper_clean) == out["postcode_state_name"].map(_upper_clean))
        ).astype(int) * 5
        + (
            out["_locality_state_name"].notna()
            & (out["state_name"].map(_upper_clean) == out["_locality_state_name"].map(_upper_clean))
        ).astype(int) * 5
        + (out["district_code"].notna() & out["mukim_code"].notna()).astype(int) * 5
        + (out["pbt_id"].notna() & out["state_code"].notna()).astype(int) * 5
        + out["_state_from_boundary"].astype(int) * 10
        + out["_district_from_boundary"].astype(int) * 10
        + out["_mukim_from_boundary"].astype(int) * 10
        - out["_state_boundary_conflict"].astype(int) * 35
        - out["_district_boundary_conflict"].astype(int) * 35
        - out["_mukim_boundary_conflict"].astype(int) * 35
        - out["_postcode_boundary_conflict"].astype(int) * 35
        - out["_state_postcode_conflict"].astype(int) * 25
        - out["locality_name"].map(lambda v: bool(_MUKIM_RE.search(_upper_clean(v) or ""))).astype(int) * 20
        - (sub1.map(lambda v: bool(_STREET_RE.search(v))) | sub2.map(lambda v: bool(_STREET_RE.search(v)))).astype(int) * 15
        - (sub1.map(lambda v: bool(_LOCALITY_RE.search(v))) | sub2.map(lambda v: bool(_LOCALITY_RE.search(v)))).astype(int) * 10
        - (~out["street_name"].map(lambda v: bool(_clean_text(v)))).astype(int) * 10
    ).clip(0, 100).astype(int)

    out["confidence_score"] = score

    # Build correction notes as boolean flag columns then collapse to list per row
    note_flags = pd.DataFrame({
        "district_inferred_from_candidates": out["_district_from_candidate"].map(_truthy),
        "mukim_inferred_from_candidates": out["_mukim_from_candidate"].map(_truthy),
        "postcode_from_boundary": out["_postcode_from_boundary"].astype(bool),
        "postcode_boundary_conflict": out["_postcode_boundary_conflict"].astype(bool),
        "state_from_boundary": out["_state_from_boundary"].astype(bool),
        "district_from_boundary": out["_district_from_boundary"].astype(bool),
        "mukim_from_boundary": out["_mukim_from_boundary"].astype(bool),
        "state_boundary_conflict": out["_state_boundary_conflict"].astype(bool),
        "district_boundary_conflict": out["_district_boundary_conflict"].astype(bool),
        "mukim_boundary_conflict": out["_mukim_boundary_conflict"].astype(bool),
        "state_postcode_conflict": out["_state_postcode_conflict"].astype(bool),
        "postcode_standardized_with_name": (
            out["postcode_code"].map(_clean_text).notna()
            & out["postcode_name"].map(_clean_text).notna()
        ) if "postcode_name" in out.columns else pd.Series(False, index=out.index),
    }, index=out.index)
    out["correction_notes"] = note_flags.apply(
        lambda row: [label for label, flag in row.items() if flag], axis=1
    )

    out["address_clean"] = out.apply(_canonical_address, axis=1)
    return out.drop(columns=[c for c in ["_locality_state_name", "_district_from_candidate", "_mukim_from_candidate"] if c in out.columns])


def validate_addresses(
    df: DataFrame,
    *,
    require_mukim: bool = False,
    dedupe_on: str = "address_clean",
    min_confidence: int = 75,
) -> tuple[DataFrame, DataFrame, DataFrame]:
    out = df.copy()
    for col in [
        "_postcode_boundary_conflict",
        "_state_postcode_conflict",
        "_state_boundary_conflict",
        "_district_boundary_conflict",
        "_mukim_boundary_conflict",
    ]:
        _ensure_column(out, col, False)
        out[col] = out[col].fillna(False).astype(bool)
    if "confidence_score" not in out.columns:
        out = _finalize_cleaned_addresses(out)
    out["confidence_band"] = out["confidence_score"].fillna(0).astype(int).map(_confidence_band)

    dup_rank = (
        out.groupby(dedupe_on, dropna=False).cumcount() + 1
        if dedupe_on in out.columns
        else pd.Series(1, index=out.index)
    )

    # Vectorized error conditions — one boolean Series per error code
    error_masks: dict[str, pd.Series] = {
        "missing_address": (
            out.get("is_missing_address", pd.Series(False, index=out.index)).fillna(True)
            | ~out["address_clean"].map(lambda v: bool(_clean_text(v)))
        ),
        "invalid_postcode": ~out["postcode_code"].map(lambda v: bool(_extract_postcode(v))),
        "postcode_boundary_conflict": out["_postcode_boundary_conflict"],
        "missing_state": out["state_code"].isna(),
        "invalid_state_lookup": out.get("state_lookup_valid", pd.Series(pd.NA, index=out.index)).map(_falsey_bool),
        "missing_district": out["district_code"].isna(),
        "invalid_district_lookup": out.get("district_lookup_valid", pd.Series(pd.NA, index=out.index)).map(_falsey_bool),
        "state_postcode_conflict": out["_state_postcode_conflict"],
        "state_boundary_conflict": out["_state_boundary_conflict"],
        "district_boundary_conflict": out["_district_boundary_conflict"],
        "mukim_boundary_conflict": out["_mukim_boundary_conflict"],
        "low_confidence": out["confidence_score"].fillna(0).lt(min_confidence),
        "duplicate_address": dup_rank.gt(1),
    }
    if require_mukim:
        error_masks["missing_mukim"] = out["mukim_code"].isna()
        error_masks["invalid_mukim_lookup"] = out.get("mukim_lookup_valid", pd.Series(pd.NA, index=out.index)).map(_falsey_bool)

    warning_masks: dict[str, pd.Series] = {
        "missing_coordinates": out["latitude"].isna() | out["longitude"].isna(),
    }

    error_df = pd.DataFrame(error_masks, index=out.index)
    warning_df = pd.DataFrame(warning_masks, index=out.index)

    out["error_reasons"] = error_df.apply(lambda row: [k for k, v in row.items() if v], axis=1)
    out["warning_reasons"] = warning_df.apply(lambda row: [k for k, v in row.items() if v], axis=1)
    out["reason_codes"] = [e + w for e, w in zip(out["error_reasons"], out["warning_reasons"])]
    out["error_reason"] = out["error_reasons"].map("|".join)
    out["warning_reason"] = out["warning_reasons"].map("|".join)
    out["validation_status"] = [
        "FAIL" if e else ("WARNING" if w else "PASS")
        for e, w in zip(out["error_reasons"], out["warning_reasons"])
    ]

    success = out[out["validation_status"] == "PASS"].copy()
    warning = out[out["validation_status"] == "WARNING"].copy()
    failed = out[out["validation_status"] == "FAIL"].copy()
    return success, warning, failed
