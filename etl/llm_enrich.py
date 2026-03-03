import argparse
import csv
import json
import os
import time
from typing import List, Set

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from .audit_log import audit_event, start_audit_run


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
    parser = argparse.ArgumentParser(description="Use OpenAI to normalize low-confidence addresses.")
    parser.add_argument("--input", default="output/failed", help="Failed parquet path")
    parser.add_argument("--output", default="data/llm_corrections.csv", help="Corrections CSV output")
    parser.add_argument("--model", default="gpt-4o-mini", help="OpenAI model")
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


def main() -> None:
    args = parse_args()
    started = time.time()
    run_id = start_audit_run(args.audit_log, "llm_enrich", vars(args))
    spark = None
    status = "ok"
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is required for llm_enrich.py")

    from openai import OpenAI

    written_count = 0
    skipped_existing_count = 0
    request_count = 0
    empty_result_count = 0
    try:
        spark = SparkSession.builder.appName("NAS LLM Enrich").getOrCreate()
        df = spark.read.parquet(args.input)
        address_col = args.source_col or _detect_address_col(df.columns)
        if not address_col:
            raise ValueError("Could not detect an address column in failed data.")

        criteria = col(address_col).isNotNull()
        if "confidence_score" in df.columns:
            criteria = criteria & (col("confidence_score") < args.min_confidence)
        else:
            criteria = criteria & (
                col("state_code").isNull()
                | col("district_code").isNull()
                | col("mukim_code").isNull()
            )

        rows = (
            df.select(col(address_col).alias("source_address"))
            .filter(criteria)
            .limit(args.limit)
            .collect()
        )
        addresses = [r["source_address"] for r in rows if r["source_address"]]

        output_dir = os.path.dirname(args.output)
        if output_dir:
            os.makedirs(output_dir, exist_ok=True)
        existing = _load_existing_sources(args.output)

        client = OpenAI()
        with open(args.output, "a", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            if handle.tell() == 0:
                writer.writerow(["source_address", "corrected_address", "state_name", "postcode", "reason"])

            for address in addresses:
                if address in existing:
                    skipped_existing_count += 1
                    continue
                request_count += 1
                result = _call_openai(client, args.model, address)
                if not result:
                    empty_result_count += 1
                    continue
                writer.writerow(
                    [
                        address,
                        result.get("corrected_address", "").strip(),
                        result.get("state_name", "").strip(),
                        result.get("postcode", "").strip(),
                        result.get("reason", "").strip(),
                    ]
                )
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
        if spark is not None:
            spark.stop()
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
