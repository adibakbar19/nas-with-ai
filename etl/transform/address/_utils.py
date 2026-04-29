"""Shared primitive utilities used across address normalization modules."""

import re
from typing import Optional

import pandas as pd


DataFrame = pd.DataFrame

_NULL_STRINGS = {"", "nan", "null", "none", "na", "n/a", "<na>"}
_STREET_RE = re.compile(r"\b(JALAN\s+RAYA|JALANRAYA|JALAN|LORONG|PERSIARAN|LEBUHRAYA|LEBUH)\b", re.I)
_LOCALITY_RE = re.compile(r"\b(BANDAR(?:\s+BARU)?|BANDARAYA|PEKAN|CITY)\b", re.I)
_MUKIM_RE = re.compile(r"\bMUKIM\b", re.I)
_POSTCODE_RE = re.compile(r"(?<!\d)(\d{5})(?!\d)")


def _normalize_col_name(name: str) -> str:
    return "".join(ch for ch in name.lower() if ch.isalnum())


def _config_bool(config: dict, key: str, default: bool) -> bool:
    value = config.get(key, default)
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y"}
    if isinstance(value, (int, float)):
        return bool(value)
    return bool(value)


def _clean_text(value) -> str | None:
    if value is None or pd.isna(value):
        return None
    text = str(value).strip()
    if text.lower() in _NULL_STRINGS:
        return None
    text = re.sub(r"\s+", " ", text)
    return text or None


def _upper_clean(value) -> str | None:
    text = _clean_text(value)
    return text.upper() if text else None


def _truthy(value) -> bool:
    if value is None or pd.isna(value):
        return False
    return bool(value)


def _falsey_bool(value) -> bool:
    if value is None or pd.isna(value):
        return False
    return bool(value) is False


def _normalize_match(value) -> str:
    text = _upper_clean(value) or ""
    text = re.sub(r"[\r\n]+", " ", text)
    text = re.sub(r"[^A-Z0-9 ]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return f" {text} " if text else " "


def _normalize_segment(value) -> str | None:
    text = _upper_clean(value)
    if not text:
        return None
    text = re.sub(r"[^A-Z0-9 ]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text or None


def _extract_postcode(value) -> str | None:
    text = _clean_text(value)
    if not text:
        return None
    match = _POSTCODE_RE.search(text)
    return match.group(1) if match else None


def _detect_col(df: DataFrame, candidates: list[str]) -> Optional[str]:
    cols = {c.lower(): c for c in df.columns}
    norm_cols = {_normalize_col_name(c): c for c in df.columns}
    for cand in candidates:
        if cand.lower() in cols:
            return cols[cand.lower()]
        norm = _normalize_col_name(cand)
        if norm in norm_cols:
            return norm_cols[norm]
    return None


def _detect_address_col(df: DataFrame, candidates: list[str]) -> Optional[str]:
    detected = _detect_col(df, candidates)
    if detected:
        return detected
    matches = []
    for raw in df.columns:
        norm = _normalize_col_name(raw)
        if not ("alamat" in norm or "address" in norm):
            continue
        if any(token in norm for token in ["addresstype", "addresssource", "addressid"]):
            continue
        if norm == "addressclean" or ("new" in norm and "address" in norm) or ("old" in norm and "address" in norm):
            continue
        if "alamat" in norm or "addressline" in norm or "fulladdress" in norm or "rawaddress" in norm or norm == "address":
            matches.append(raw)
    return matches[0] if len(matches) == 1 else None


def _ensure_column(df: DataFrame, name: str, value=pd.NA) -> DataFrame:
    if name not in df.columns:
        df[name] = value
    return df


def _rename_if_exists(df: DataFrame, name: str, new_name: str) -> DataFrame:
    if name in df.columns:
        df = df.rename(columns={name: new_name})
    return df


def _levenshtein(a: str | None, b: str | None) -> int:
    a = a or ""
    b = b or ""
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


def _ensure_spatial_defaults(df: DataFrame) -> DataFrame:
    for col in [
        "_postcode_from_boundary",
        "_postcode_boundary_conflict",
        "_state_from_boundary",
        "_district_from_boundary",
        "_mukim_from_boundary",
        "_state_boundary_conflict",
        "_district_boundary_conflict",
        "_mukim_boundary_conflict",
    ]:
        if col not in df.columns:
            df[col] = False
        df[col] = df[col].fillna(False).astype(bool)
    return df
