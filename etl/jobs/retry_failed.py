import argparse
import os
import re
import time

import pandas as pd

from ..audit.audit_log import audit_event, start_audit_run
from ..pipeline.orchestrator import load_reference_data
from ._common import DERIVED_COLUMNS, run_retry_pipeline


def _normalize_key(value) -> str:
    if value is None or pd.isna(value):
        return ""
    return re.sub(r"\s+", " ", str(value).strip().upper())


def _detect_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    cols = {c.lower(): c for c in df.columns}
    for cand in candidates:
        if cand.lower() in cols:
            return cols[cand.lower()]
    return None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Retry failed addresses with manual corrections.")
    parser.add_argument("--failed", required=True, help="Failed parquet path")
    parser.add_argument("--corrections", required=True, help="Corrections CSV path")
    parser.add_argument("--success", required=True, help="Success output parquet path")
    parser.add_argument("--warning-out", required=True, help="Warning output parquet path")
    parser.add_argument("--failed-out", required=True, help="Failed output parquet path")
    parser.add_argument("--require-mukim", action="store_true", help="Fail rows missing mukim")
    parser.add_argument("--source-col", default=None, help="Source address column in failed data")
    parser.add_argument("--correction-col", default=None, help="Corrected address column in corrections CSV")
    parser.add_argument("--config", default=None, help="Optional pipeline config path")
    parser.add_argument("--audit-log", default=os.getenv("NAS_AUDIT_LOG", "logs/nas_audit.log"))
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    started = time.time()
    run_id = start_audit_run(args.audit_log, "retry_failed", vars(args))
    status = "ok"
    try:
        refs = load_reference_data(args.config)
        df_failed = pd.read_parquet(args.failed)
        corrections = pd.read_csv(args.corrections, dtype=str)

        source_col = args.source_col or _detect_col(df_failed, ["source_address", "source_address_old", "address", "full_address", "alamat", "raw_address", "address_full"])
        if not source_col:
            raise ValueError("Could not detect source address column in failed dataset.")
        corr_col = args.correction_col or _detect_col(corrections, ["corrected_address", "address_fixed", "address", "full_address", "alamat"])
        if not corr_col:
            raise ValueError("Could not detect corrected address column in corrections CSV.")
        corr_source_col = _detect_col(corrections, ["source_address", "raw_address", "address_original", "original_address", "address_source"])
        if not corr_source_col:
            corr_source_col = source_col if source_col in corrections.columns else None
        if not corr_source_col:
            raise ValueError("Corrections CSV must include a source address column, e.g. source_address.")

        retry = df_failed.copy()
        retry["_src_key"] = retry[source_col].map(_normalize_key)
        corr = corrections[[corr_source_col, corr_col]].copy()
        corr["_src_key"] = corr[corr_source_col].map(_normalize_key)
        corr = corr.drop_duplicates("_src_key").rename(columns={corr_col: "_corrected_address"})
        retry = retry.merge(corr[["_src_key", "_corrected_address"]], on="_src_key", how="left")
        retry["source_address_original"] = retry[source_col]
        retry["address"] = retry["_corrected_address"].where(retry["_corrected_address"].notna(), retry[source_col])
        retry = retry.drop(columns=[col for col in ["_src_key", "_corrected_address"] if col in retry.columns])
        retry = retry.drop(columns=[col for col in retry.columns if col in DERIVED_COLUMNS])

        success, warning, failed_out = run_retry_pipeline(
            retry,
            refs=refs,
            address_col="address",
            require_mukim=args.require_mukim,
            success_path=args.success,
            warning_path=args.warning_out,
            failed_path=args.failed_out,
        )
        audit_event(
            args.audit_log,
            "retry_failed",
            run_id,
            "retry_complete",
            input_failed_count=len(df_failed),
            corrections_count=len(corrections),
            success_count=len(success),
            warning_count=len(warning),
            failed_count=len(failed_out),
        )
    except Exception as exc:
        status = "error"
        audit_event(args.audit_log, "retry_failed", run_id, "run_error", error_type=type(exc).__name__, error=str(exc))
        raise
    finally:
        audit_event(
            args.audit_log,
            "retry_failed",
            run_id,
            "run_end",
            status=status,
            duration_ms=int((time.time() - started) * 1000),
        )


if __name__ == "__main__":
    main()
