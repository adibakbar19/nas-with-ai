import argparse
import csv
import json
import os
import time
from typing import List, Set

import pandas as pd

from ...audit.audit_log import audit_event, start_audit_run


STATE_NAMES = [
    "JOHOR",
    "KEDAH",
    "KELANTAN",
    "MELAKA",
    "NEGERI SEMBILAN",
    "PAHANG",
    "PERAK",
    "PERLIS",
    "PULAU PINANG",
    "SABAH",
    "SARAWAK",
    "SELANGOR",
    "TERENGGANU",
    "WILAYAH PERSEKUTUAN KUALA LUMPUR",
    "WILAYAH PERSEKUTUAN PUTRAJAYA",
    "WILAYAH PERSEKUTUAN LABUAN",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Use an LLM to normalize low-confidence Malaysian addresses.")
    parser.add_argument("--input", default="output/failed", help="Failed parquet path")
    parser.add_argument("--output", default="data/llm_corrections.csv", help="Corrections CSV output")
    parser.add_argument("--provider", default=os.getenv("NAS_LLM_PROVIDER", "bedrock"), choices=["bedrock", "openai"])
    parser.add_argument("--model", default=os.getenv("NAS_LLM_MODEL"), help="Bedrock model ID or OpenAI model name.")
    parser.add_argument("--min-confidence", type=int, default=60, help="Minimum confidence threshold")
    parser.add_argument("--limit", type=int, default=200, help="Max rows to send to LLM")
    parser.add_argument("--sleep", type=float, default=0.0, help="Seconds to sleep between requests")
    parser.add_argument("--source-col", default=None, help="Override source address column")
    parser.add_argument(
        "--audit-log",
        default=os.getenv("NAS_AUDIT_LOG", "logs/nas_audit.log"),
        help="JSONL audit logfile path",
    )
    return parser.parse_args()


def _detect_address_col(columns: List[str]) -> str | None:
    candidates = [
        "source_address_new",
        "source_address_old",
        "source_address",
        "address",
        "full_address",
        "alamat",
        "raw_address",
        "address_clean",
    ]
    col_map = {c.lower(): c for c in columns}
    for cand in candidates:
        if cand.lower() in col_map:
            return col_map[cand.lower()]
    return None


def _load_existing_sources(path: str) -> Set[str]:
    if not os.path.exists(path):
        return set()
    seen: Set[str] = set()
    with open(path, newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            src = (row.get("source_address") or "").strip()
            if src:
                seen.add(src)
    return seen


def _build_prompt(address: str) -> List[dict]:
    system = (
        "You are an address normalization assistant for Malaysian addresses. "
        "Return JSON only. Fix obvious typos, expand abbreviations, and keep any "
        "postcode (5 digits) if present. Preserve the address meaning."
        f" Use one of these state names if you can infer it: {', '.join(STATE_NAMES)}."
    )
    user = (
        "Normalize this address. Output JSON with keys: "
        "corrected_address, state_name, postcode, reason.\n"
        f"Address: {address}"
    )
    return [{"role": "system", "content": system}, {"role": "user", "content": user}]


def _call_openai(client, model: str, address: str) -> dict | None:
    schema = {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "corrected_address": {"type": "string"},
            "state_name": {"type": "string"},
            "postcode": {"type": "string"},
            "reason": {"type": "string"},
        },
        "required": ["corrected_address", "state_name", "postcode", "reason"],
    }

    response = client.chat.completions.create(
        model=model,
        messages=_build_prompt(address),
        response_format={"type": "json_schema", "json_schema": {"name": "address_fix", "schema": schema, "strict": True}},
    )
    message = response.choices[0].message
    if getattr(message, "refusal", None):
        return None
    content = message.content
    if not content:
        return None
    try:
        return json.loads(content)
    except Exception:
        return None


def _call_bedrock(mapper, address: str) -> dict | None:
    return mapper.map_address(address=address, context={})


def _safe_text(value) -> str:
    if value is None or pd.isna(value):
        return ""
    return str(value).strip()


def _write_result(writer, address: str, result: dict) -> None:
    writer.writerow(
        [
            address,
            _safe_text(result.get("corrected_address")),
            _safe_text(result.get("state_name")),
            _safe_text(result.get("district_name")),
            _safe_text(result.get("mukim_name")),
            _safe_text(result.get("locality_name")),
            _safe_text(result.get("postcode")),
            _safe_text(result.get("confidence_score")),
            _safe_text(result.get("reason")),
        ]
    )


def _results_by_record_id(results: list[dict]) -> dict[str, dict]:
    out: dict[str, dict] = {}
    for result in results:
        record_id = _safe_text(result.get("record_id"))
        if record_id:
            out[record_id] = result
    return out


def main() -> None:
    args = parse_args()
    started = time.time()
    run_id = start_audit_run(args.audit_log, "llm_enrich", vars(args))
    status = "ok"
    written_count = 0
    skipped_existing_count = 0
    request_count = 0
    empty_result_count = 0
    try:
        df = pd.read_parquet(args.input)
        address_col = args.source_col or _detect_address_col(list(df.columns))
        if not address_col:
            raise ValueError("Could not detect an address column in failed data.")

        criteria = df[address_col].notna()
        if "confidence_score" in df.columns:
            criteria = criteria & (pd.to_numeric(df["confidence_score"], errors="coerce") < args.min_confidence)
        else:
            missing_admin = pd.Series(False, index=df.index)
            for col_name in ["state_code", "district_code", "mukim_code"]:
                if col_name in df.columns:
                    missing_admin = missing_admin | df[col_name].isna()
            criteria = criteria & missing_admin

        addresses = [
            str(value)
            for value in df.loc[criteria, address_col].head(args.limit).tolist()
            if pd.notna(value) and str(value).strip()
        ]

        output_dir = os.path.dirname(args.output)
        if output_dir:
            os.makedirs(output_dir, exist_ok=True)
        existing = _load_existing_sources(args.output)

        model = args.model or (
            "gpt-4o-mini" if args.provider == "openai" else os.getenv("BEDROCK_CLAUDE_MODEL_ID", "anthropic.claude-3-haiku-20240307-v1:0")
        )
        if args.provider == "openai":
            api_key = os.getenv("OPENAI_API_KEY")
            if not api_key:
                raise RuntimeError("OPENAI_API_KEY is required when --provider=openai")
            from openai import OpenAI

            client = OpenAI()
        else:
            from .bedrock import BedrockClaudeAddressMapper

            client = BedrockClaudeAddressMapper(model_id=model)
        with open(args.output, "a", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            if handle.tell() == 0:
                writer.writerow(
                    [
                        "source_address",
                        "corrected_address",
                        "state_name",
                        "district_name",
                        "mukim_name",
                        "locality_name",
                        "postcode",
                        "confidence_score",
                        "reason",
                    ]
                )

            pending = []
            for address in addresses:
                if address in existing:
                    skipped_existing_count += 1
                else:
                    pending.append(address)

            if args.provider == "bedrock" and hasattr(client, "map_addresses"):
                batch_size = max(1, int(getattr(client, "batch_size", 25)))
                for offset in range(0, len(pending), batch_size):
                    batch = pending[offset : offset + batch_size]
                    records = [
                        {"record_id": f"addr-{offset + idx + 1}", "address": address}
                        for idx, address in enumerate(batch)
                    ]
                    request_count += 1
                    results = client.map_addresses(records=records, context={})
                    by_id = _results_by_record_id(results)
                    for record in records:
                        result = by_id.get(record["record_id"])
                        if not result:
                            empty_result_count += 1
                            continue
                        _write_result(writer, record["address"], result)
                        written_count += 1
                    if args.sleep:
                        time.sleep(args.sleep)
            else:
                for address in pending:
                    request_count += 1
                    result = _call_openai(client, model, address) if args.provider == "openai" else _call_bedrock(client, address)
                    if not result:
                        empty_result_count += 1
                        continue
                    _write_result(writer, address, result)
                    written_count += 1
                    if args.sleep:
                        time.sleep(args.sleep)

        audit_event(
            args.audit_log,
            "llm_enrich",
            run_id,
            "enrichment_complete",
            input_path=args.input,
            output_path=args.output,
            provider=args.provider,
            model=model,
            addresses_selected=len(addresses),
            request_count=request_count,
            skipped_existing_count=skipped_existing_count,
            empty_result_count=empty_result_count,
            written_count=written_count,
        )
    except Exception as exc:
        status = "error"
        audit_event(
            args.audit_log,
            "llm_enrich",
            run_id,
            "run_error",
            error_type=type(exc).__name__,
            error=str(exc),
            written_count=written_count,
        )
        raise
    finally:
        audit_event(
            args.audit_log,
            "llm_enrich",
            run_id,
            "run_end",
            status=status,
            duration_ms=int((time.time() - started) * 1000),
            written_count=written_count,
        )


if __name__ == "__main__":
    main()
