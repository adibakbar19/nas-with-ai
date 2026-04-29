from __future__ import annotations

from typing import Any

import pandas as pd

from .input_normalizer import AddressInputRecord, records_to_dataframe
from .normalize import clean_addresses, validate_addresses


SUCCESS_MIN_SCORE = 85
PARTIAL_MIN_SCORE = 60
AI_COMPONENT_FIELDS = [
    "premise_no",
    "lot_no",
    "unit_no",
    "floor_no",
    "building_name",
    "street_name_prefix",
    "street_name",
    "locality_name",
    "district_name",
    "mukim_name",
    "state_name",
    "postcode",
]


def _is_present(value: Any) -> bool:
    return value is not None and not pd.isna(value) and str(value).strip() != ""


def _json_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    if hasattr(value, "item"):
        try:
            return value.item()
        except (TypeError, ValueError):
            pass
    return value


def _confidence_score_0_1(value: Any) -> float:
    if value is None or pd.isna(value):
        return 0.0
    try:
        score = float(value)
    except (TypeError, ValueError):
        return 0.0
    if score > 1:
        score = score / 100.0
    return round(max(0.0, min(1.0, score)), 4)


def _safe_text(value: Any) -> str | None:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    text = str(value).strip()
    return text or None


def _match_status(score_100: int, row: pd.Series) -> str:
    has_core = all(_is_present(row.get(col)) for col in ["state_code", "district_code", "mukim_code"])
    if score_100 >= SUCCESS_MIN_SCORE and has_core:
        return "success"
    if score_100 >= PARTIAL_MIN_SCORE:
        return "partial"
    return "failed"


def _standardized_address_dict(row: pd.Series) -> dict[str, Any]:
    fields = [
        "address_clean",
        "premise_no",
        "lot_no",
        "unit_no",
        "floor_no",
        "floor_level",
        "building_name",
        "street_name_prefix",
        "street_name",
        "sub_locality_1",
        "sub_locality_2",
        "sub_locality_levels",
        "postcode_code",
        "locality_name",
        "postcode",
        "postcode_name",
        "state_code",
        "state_name",
        "district_code",
        "district_name",
        "mukim_code",
        "mukim_name",
        "mukim_id",
        "pbt_id",
        "pbt_name",
        "country",
        "address_type",
        "naskod",
    ]
    return {field: _json_value(row.get(field)) for field in fields if field in row.index}


def _ai_context(lookups: Any) -> dict[str, Any]:
    state_df = getattr(lookups, "state", None)
    if state_df is None or "state_name" not in state_df.columns:
        return {}
    states = sorted({_safe_text(value) for value in state_df["state_name"].tolist() if _safe_text(value)})
    return {"state_names": states}


def _call_ai_batch(llm_mapper: Any, records: list[AddressInputRecord], *, lookups: Any) -> list[dict[str, Any]]:
    payload_records = [{"record_id": record.record_id, "address": record.address} for record in records]
    if hasattr(llm_mapper, "map_addresses"):
        result = llm_mapper.map_addresses(records=payload_records, context=_ai_context(lookups))
        return result if isinstance(result, list) else []

    results: list[dict[str, Any]] = []
    for record in records:
        result = llm_mapper.map_address(address=record.address, context={})
        if isinstance(result, dict):
            results.append({"record_id": record.record_id, **result})
    return results


def _map_ai_results(records: list[AddressInputRecord], results: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    mapped: dict[str, dict[str, Any]] = {}
    unused = list(results)
    for result in results:
        record_id = _safe_text(result.get("record_id"))
        if record_id:
            mapped[record_id] = result
    for record, result in zip(records, unused):
        mapped.setdefault(record.record_id, result)
    return mapped


def _ai_rows(records: list[AddressInputRecord], results: list[dict[str, Any]]) -> pd.DataFrame:
    by_id = _map_ai_results(records, results)
    rows: list[dict[str, Any]] = []
    for record in records:
        result = by_id.get(record.record_id) or {}
        corrected = _safe_text(result.get("corrected_address")) or record.address
        row: dict[str, Any] = {
            "record_id": record.record_id,
            "address": corrected,
            "original_address": record.address,
            "input_metadata": record.metadata,
            "ai_reason": _safe_text(result.get("reason")),
            "ai_confidence_score": _confidence_score_0_1(result.get("confidence_score")),
        }
        for field in AI_COMPONENT_FIELDS:
            value = _safe_text(result.get(field))
            if value:
                row[field] = value
        rows.append(row)
    return pd.DataFrame(rows)


def _reapply_ai_components(parsed: pd.DataFrame, ai_df: pd.DataFrame) -> pd.DataFrame:
    out = parsed.copy()
    by_id = ai_df.set_index("record_id")
    for idx, row in out.iterrows():
        record_id = row.get("record_id")
        if record_id not in by_id.index:
            continue
        source = by_id.loc[record_id]
        for field in AI_COMPONENT_FIELDS:
            value = source.get(field) if field in source.index else None
            if _safe_text(value):
                out.at[idx, field] = value
    return out


def _parse_with_ai_first(
    records: list[AddressInputRecord],
    *,
    config: dict,
    lookups: Any,
    llm_mapper: Any,
) -> pd.DataFrame:
    ai_results = _call_ai_batch(llm_mapper, records, lookups=lookups)
    df = _ai_rows(records, ai_results)
    ai_config = dict(config)
    ai_config["prefer_structured_source_fields"] = True
    parsed = clean_addresses(
        df,
        address_col="address",
        config=ai_config,
        lookups=lookups,
        include_spatial=False,
        finalize=True,
    )
    parsed = _reapply_ai_components(parsed, df)
    parsed["parser_mode"] = "ai_first"
    parsed["ai_model"] = getattr(llm_mapper, "model_id", "ai-address-parser")
    if "ai_confidence_score" in parsed.columns:
        ai_scores = pd.to_numeric(parsed["ai_confidence_score"], errors="coerce").fillna(0).map(lambda score: int(score * 100))
        parsed["confidence_score"] = [
            max(int(det_score or 0), int(ai_score or 0))
            for det_score, ai_score in zip(parsed["confidence_score"], ai_scores)
        ]
    return parsed


def _parse_deterministic(
    records: list[AddressInputRecord],
    *,
    config: dict,
    lookups: Any,
) -> pd.DataFrame:
    df = records_to_dataframe(records)
    parsed = clean_addresses(
        df,
        address_col="address",
        config=config,
        lookups=lookups,
        include_spatial=False,
        finalize=True,
    )
    parsed["input_metadata"] = df.set_index("record_id").loc[parsed["record_id"], "input_metadata"].tolist()
    parsed["original_address"] = parsed.get("source_address_old")
    parsed["parser_mode"] = "deterministic"
    return parsed


def _format_row(row: pd.Series) -> dict[str, Any]:
    score_100 = int(row.get("confidence_score") or 0)
    status = _match_status(score_100, row)
    raw_reasons = row.get("reason_codes")
    reason_codes = list(raw_reasons) if isinstance(raw_reasons, list) else []
    if status == "partial" and not reason_codes:
        missing = [
            col.replace("_code", "")
            for col in ["state_code", "district_code", "mukim_code"]
            if not _is_present(row.get(col))
        ]
        reason_codes = [f"missing_{name}" for name in missing] or ["assumptions_made"]
    if status == "failed" and not reason_codes:
        reason_codes = ["unable_to_parse"]
    return {
        "record_id": _json_value(row.get("record_id")),
        "source_address": _json_value(row.get("original_address") or row.get("address_for_parse") or row.get("source_address_old")),
        "standardized_address": _standardized_address_dict(row),
        "confidence_score": _confidence_score_0_1(score_100),
        "confidence_band": _json_value(row.get("confidence_band")),
        "matched": status == "success",
        "match_status": status,
        "visual_indicator": {
            "success": "all_fields_matched_and_validated",
            "partial": "missing_components_or_assumptions_made",
            "failed": "unable_to_parse",
        }[status],
        "reason_codes": reason_codes,
        "correction_notes": _json_value(row.get("correction_notes")) if isinstance(row.get("correction_notes"), list) else [],
        "parser_mode": _json_value(row.get("parser_mode")),
        "ai_model": _json_value(row.get("ai_model")),
        "ai_reason": _json_value(row.get("ai_reason")),
    }


def parse_unified_addresses(
    records: list[AddressInputRecord],
    *,
    config: dict,
    lookups: Any,
    llm_mapper: Any | None = None,
    ai_min_confidence: int = SUCCESS_MIN_SCORE,
    require_mukim: bool = True,
) -> dict[str, Any]:
    if not records:
        raise ValueError("At least one address is required.")

    ai_attempted = False
    if llm_mapper is not None:
        ai_attempted = True
        parsed = _parse_with_ai_first(records, config=config, lookups=lookups, llm_mapper=llm_mapper)
    else:
        parsed = _parse_deterministic(records, config=config, lookups=lookups)

    success, warning, failed = validate_addresses(
        parsed,
        require_mukim=require_mukim,
        min_confidence=PARTIAL_MIN_SCORE,
    )
    classified = pd.concat([success, warning, failed], ignore_index=True)
    if "record_id" in classified.columns:
        order = {record.record_id: idx for idx, record in enumerate(records)}
        classified["_input_order"] = classified["record_id"].map(order)
        classified = classified.sort_values("_input_order").drop(columns=["_input_order"])

    items = [_format_row(row) for _, row in classified.iterrows()]
    counts = {
        "success": sum(1 for item in items if item["match_status"] == "success"),
        "partial": sum(1 for item in items if item["match_status"] == "partial"),
        "failed": sum(1 for item in items if item["match_status"] == "failed"),
    }
    return {
        "count": len(items),
        "ai_attempted": ai_attempted,
        "ai_model": getattr(llm_mapper, "model_id", None) if llm_mapper is not None else None,
        "counts": counts,
        "items": items,
    }
