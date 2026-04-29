"""Address row parsing and column standardization."""

import re

import pandas as pd

from ._utils import (
    DataFrame,
    _POSTCODE_RE,
    _STREET_RE,
    _clean_text,
    _detect_col,
    _extract_postcode,
    _normalize_col_name,
    _normalize_match,
    _normalize_segment,
    _upper_clean,
)
from .text import normalize_address_text


def _segment(parts: list[str], idx: int) -> str | None:
    if len(parts) <= idx:
        return None
    return _clean_text(parts[idx])


def _standardize_common_columns(df: DataFrame, config: dict) -> DataFrame:
    out = df.copy()
    alias_defaults = {
        "state_name": ["state", "negeri", "nama_negeri", "nm_negeri"],
        "district_name": ["district", "daerah", "nama_daerah", "nm_daerah"],
        "mukim_name": ["mukim", "nama_mukim", "nm_mukim"],
        "locality_name": ["locality", "city", "town", "bandar", "pekan", "nama_bandar"],
        "postcode": ["postcode", "poskod", "postal_code", "zip", "zip_code"],
        "premise_no": ["house_no", "house_number", "no_rumah", "nombor_rumah", "building_no"],
        "lot_no": ["lot", "lot_number", "no_lot", "nombor_lot"],
        "unit_no": ["unit", "unit_number", "no_unit", "nombor_unit"],
        "street_name": ["street", "road_name", "nama_jalan", "jalan"],
        "building_name": ["building", "premise_name", "nama_premis", "nama_premi"],
        "address_type": ["building_type", "property_type", "jenis_bangunan"],
        "country": ["negara"],
    }
    overrides = config.get("column_aliases", {})
    for canonical, defaults in alias_defaults.items():
        if canonical in out.columns:
            continue
        detected = _detect_col(out, overrides.get(canonical, defaults))
        if detected and detected != canonical:
            out = out.rename(columns={detected: canonical})
    return out


def _compose_structured_address(df: DataFrame, *, source_cols: list[str], target_col: str) -> DataFrame:
    out = df.copy()
    usable = [col for col in source_cols if col in out.columns]
    if not usable:
        out[target_col] = pd.NA
        return out

    def compose(row) -> str | None:
        parts = [_clean_text(row.get(col)) for col in usable]
        parts = [p for p in parts if p]
        return ", ".join(parts) if parts else None

    out[target_col] = out.apply(compose, axis=1)
    return out


_BUILDING_RE = re.compile(
    r"\b(APARTMENT|APARTMEN|PANGSAPURI|KONDOMINIUM|CONDO|FLAT|RUMAH\s+PANGSA|MENARA|TOWER|"
    r"WISMA|PLAZA|KOMPLEKS|BLOK|BLOCK|RESIDENSI|RESIDENCE|HOTEL|MALL|ARKED|PPR|PPRT|PPA1M)\b",
    re.I,
)


def _parse_address_row(value) -> dict:
    normalized = normalize_address_text(_clean_text(value), uppercase=True)
    if normalized:
        normalized = re.sub(r"[;|]+", ",", normalized)
    parts = [p.strip() for p in re.split(r"\s*,\s*", normalized or "") if p.strip()]
    seg1 = _segment(parts, 0)
    seg2 = _segment(parts, 1)
    seg3 = _segment(parts, 2)
    seg4 = _segment(parts, 3)
    seg5 = _segment(parts, 4)
    seg1_up = _upper_clean(seg1) or ""
    seg2_up = _upper_clean(seg2) or ""

    lot_no = None
    lot_match = re.search(r"\bLOT\s*([0-9A-Z/-]+)", seg1_up) or re.search(r"\bPT\s*([0-9A-Z/-]+)", seg1_up)
    if lot_match:
        lot_no = lot_match.group(1)

    unit_no = None
    unit_match = re.search(r"\b(?:UNIT|APT|APARTMENT|SUITE|STE)\s*([0-9A-Z/-]+)", seg1_up) or re.search(
        r"\b[A-Z]?\d+-\d+(?:-\d+)?\b", seg1_up
    )
    if unit_match:
        unit_no = unit_match.group(1) if unit_match.groups() else unit_match.group(0)

    floor_no = None
    floor_match = re.search(r"\b(?:FLOOR|LEVEL|LVL|TINGKAT|TKT|ARAS)\s*([0-9A-Z/-]+)", seg1_up)
    if floor_match:
        floor_no = floor_match.group(1)

    premise_no = None
    premise_match = re.search(r"\bNO\.?\s*([0-9A-Z/-]+)", seg1_up) or re.search(r"^([0-9A-Z/-]+)", seg1_up)
    if premise_match and not lot_no and not unit_no and not floor_no:
        premise_no = premise_match.group(1)

    def strip_postcode(seg: str | None) -> str | None:
        if not seg:
            return None
        return _upper_clean(_POSTCODE_RE.sub("", seg))

    seg3_no_zip = strip_postcode(seg3)
    seg4_no_zip = strip_postcode(seg4)
    seg5_no_zip = strip_postcode(seg5)
    locality_name = seg5_no_zip or seg4_no_zip or seg3_no_zip
    sub_locality_1 = seg3_no_zip if seg4 else None
    sub_locality_2 = seg4_no_zip if seg5 else None

    street_prefix = None
    street_name = _upper_clean(seg2)
    street_match = _STREET_RE.match(seg2_up)
    if street_match:
        street_prefix = street_match.group(1)
        stripped = re.sub(rf"^{re.escape(street_prefix)}\s+", "", seg2_up, flags=re.I).strip()
        street_name = stripped or street_name

    building_name = None
    if _BUILDING_RE.search(seg1_up):
        building_name = seg1_up
    elif _BUILDING_RE.search(seg2_up):
        building_name = seg2_up

    tail = _POSTCODE_RE.sub("", normalized or "")
    tail_parts = [p for p in (_normalize_segment(w) for w in tail.split()) if p]

    return {
        "address_clean": normalized or None,
        "premise_no": premise_no,
        "lot_no": lot_no,
        "unit_no": unit_no,
        "floor_no": floor_no,
        "street_name_prefix": street_prefix,
        "street_name": street_name,
        "building_name": building_name,
        "locality_name": locality_name,
        "sub_locality_1": sub_locality_1,
        "sub_locality_2": sub_locality_2,
        "postcode": _extract_postcode(normalized),
        "seg2_norm": _normalize_segment(seg2),
        "seg3_norm": _normalize_segment(seg3),
        "seg4_norm": _normalize_segment(seg4),
        "seg5_norm": _normalize_segment(seg5),
        "tail2_norm": " ".join(tail_parts[-2:]) if len(tail_parts) >= 2 else None,
        "tail3_norm": " ".join(tail_parts[-3:]) if len(tail_parts) >= 3 else None,
        "tail4_norm": " ".join(tail_parts[-4:]) if len(tail_parts) >= 4 else None,
        "_address_norm": _normalize_match(normalized),
    }
