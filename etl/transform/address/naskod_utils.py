import re

import pandas as pd


STATE_ABBR_MAP = {
    "01": "JHR",
    "02": "KDH",
    "03": "KTN",
    "04": "MLK",
    "05": "NSN",
    "06": "PHG",
    "07": "PNG",
    "08": "PRK",
    "09": "PLS",
    "10": "SGR",
    "11": "TRG",
    "12": "SBH",
    "13": "SWK",
    "14": "KL",
    "15": "LAB",
    "16": "PJY",
}


def _clean_text(value) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip().upper()


def _state_code(value) -> str | None:
    text = _clean_text(value)
    if not text:
        return None
    if text.isdigit():
        return text.zfill(2)
    return text


def _district_code(value) -> str | None:
    text = _clean_text(value)
    if not text:
        return None
    if text.isdigit():
        return text.zfill(2)
    digits = re.sub(r"\D", "", text)
    return digits.zfill(2) if digits else None


def _address_type_code(value) -> str:
    normalized = _clean_text(value)
    if not normalized:
        return "U"
    if re.match(r"^(RESIDENTIAL|HIGHRISE|RURAL)\b", normalized):
        return "R"
    if re.match(r"^COMMERCIAL\b", normalized):
        return "C"
    if re.match(r"^(OFFICE|INSTITUTIONAL)\b", normalized):
        return "O"
    if re.match(r"^INDUSTRIAL\b", normalized):
        return "I"
    return normalized[:1]


def add_standard_naskod(
    df: pd.DataFrame,
    *,
    output_col: str = "naskod",
    overwrite_existing: bool = True,
    source_col: str = "source_naskod",
) -> pd.DataFrame:
    out = df.copy()
    if output_col in out.columns:
        if not overwrite_existing:
            return out
        if source_col and source_col not in out.columns:
            out[source_col] = out[output_col].astype("string")
        out = out.drop(columns=[output_col])

    if "state_code" not in out.columns or "district_code" not in out.columns:
        out[output_col] = pd.NA
        return out

    state_codes = out["state_code"].map(_state_code)
    district_codes = out["district_code"].map(_district_code)
    state_abbr = state_codes.map(lambda code: STATE_ABBR_MAP.get(code, code if code and re.fullmatch(r"[A-Z]{2,3}", code) else None))
    type_codes = out["address_type"].map(_address_type_code) if "address_type" in out.columns else pd.Series("U", index=out.index)

    seq = (
        pd.DataFrame(
            {
                "_state_abbr": state_abbr,
                "_district_code": district_codes,
                "_type_code": type_codes,
            },
            index=out.index,
        )
        .groupby(["_state_abbr", "_district_code", "_type_code"], dropna=False)
        .cumcount()
        + 1
    )

    values = []
    for state, district, type_code, number in zip(state_abbr, district_codes, type_codes, seq):
        if not state or not district or pd.isna(state) or pd.isna(district):
            values.append(pd.NA)
            continue
        values.append(f"NAS-{state}-{district}-{type_code}{int(number):06d}")
    out[output_col] = values
    return out
