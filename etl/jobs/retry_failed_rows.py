import argparse
import os
import time

import pandas as pd

from ..audit.audit_log import audit_event, start_audit_run
from ..pipeline.orchestrator import load_reference_data
from ._common import DERIVED_COLUMNS, run_retry_pipeline


def _detect_address_col(columns: list[str]) -> str | None:
    candidates = [
        "address",
        "source_address_new",
        "source_address_old",
        "source_address",
        "full_address",
        "alamat",
        "raw_address",
        "address_clean",
    ]
    lower_map = {c.lower(): c for c in columns}
    for candidate in candidates:
        if candidate.lower() in lower_map:
            return lower_map[candidate.lower()]
    return None


def _apply_corrections(df_failed: pd.DataFrame, corrections: pd.DataFrame) -> pd.DataFrame:
    if "record_id" not in df_failed.columns:
        raise ValueError("Failed parquet must include 'record_id'.")
    if "record_id" not in corrections.columns:
        raise ValueError("Corrections CSV must include 'record_id'.")
    correction_cols = [c for c in corrections.columns if c != "record_id"]
    if not correction_cols:
        raise ValueError("Corrections CSV has no correction columns. Add at least one column besides record_id.")
    corr = corrections.drop_duplicates("record_id").rename(columns={c: f"_corr_{c}" for c in correction_cols})
    out = df_failed.merge(corr, on="record_id", how="left")
    target_address_col = _detect_address_col(list(df_failed.columns))
    if "_corr_corrected_address" in out.columns:
        if target_address_col:
            out[target_address_col] = out["_corr_corrected_address"].where(out["_corr_corrected_address"].notna(), out[target_address_col])
        else:
            out["address"] = out["_corr_corrected_address"]
    for source_col in correction_cols:
        if source_col == "corrected_address":
            continue
        corr_col = f"_corr_{source_col}"
        if source_col in out.columns:
            out[source_col] = out[corr_col].where(out[corr_col].notna(), out[source_col])
        else:
            out[source_col] = out[corr_col]
    return out.drop(columns=[c for c in out.columns if c.startswith("_corr_")])


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Retry failed rows using record_id based corrections.")
    parser.add_argument("--failed-path", required=True, help="Failed parquet path")
    parser.add_argument("--corrections-csv", required=True, help="Corrections CSV path")
    parser.add_argument("--success-out", required=True, help="Retried success output parquet path")
    parser.add_argument("--warning-out", required=True, help="Retried warning output parquet path")
    parser.add_argument("--failed-out", required=True, help="Retried failed output parquet path")
    parser.add_argument("--require-mukim", action="store_true", help="Fail rows missing mukim")
    parser.add_argument("--config", default=None, help="Optional pipeline config path")
    parser.add_argument("--audit-log", default=os.getenv("NAS_AUDIT_LOG", "logs/nas_audit.log"))
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    started = time.time()
    run_id = start_audit_run(args.audit_log, "retry_failed_rows", vars(args))
    status = "ok"
    try:
        refs = load_reference_data(args.config)
        df_failed = pd.read_parquet(args.failed_path)
        corrections = pd.read_csv(args.corrections_csv, dtype=str)
        retry = _apply_corrections(df_failed, corrections)
        retry = retry.drop(columns=[col for col in retry.columns if col in DERIVED_COLUMNS])
        address_col = _detect_address_col(list(retry.columns))
        success, warning, failed_out = run_retry_pipeline(
            retry,
            refs=refs,
            address_col=address_col,
            require_mukim=args.require_mukim,
            success_path=args.success_out,
            warning_path=args.warning_out,
            failed_path=args.failed_out,
        )
        audit_event(
            args.audit_log,
            "retry_failed_rows",
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
        audit_event(args.audit_log, "retry_failed_rows", run_id, "run_error", error_type=type(exc).__name__, error=str(exc))
        raise
    finally:
        audit_event(
            args.audit_log,
            "retry_failed_rows",
            run_id,
            "run_end",
            status=status,
            duration_ms=int((time.time() - started) * 1000),
        )


if __name__ == "__main__":
    main()
