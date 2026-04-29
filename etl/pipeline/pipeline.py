import argparse
import json
import os
import time
import uuid
from datetime import datetime, timezone

import pandas as pd

from ..audit.audit_log import audit_event, start_audit_run
from ..extract.extract import extract_data
from ..load.loader import load_parquet, load_success_warning_failed
from ..transform import clean_text_addresses, enrich_spatial_components, validate_addresses
from ..transform.address.naskod_utils import add_standard_naskod
from .orchestrator import load_reference_data


def _stage_log(stage: str, event: str, **fields) -> None:
    suffix = " ".join(f"{k}={v}" for k, v in fields.items())
    print(
        f"CHECKPOINT stage={stage} event={event}" + (f" {suffix}" if suffix else ""),
        flush=True,
    )


def _emit_stage(stage: str) -> None:
    print(f"PIPELINE_STAGE:{stage}", flush=True)


def _local_path_from_uri(path: str) -> str | None:
    if path.startswith("file://"):
        return path[len("file://") :]
    if "://" in path:
        return None
    return path


def _derive_job_id(checkpoint_root: str, success_path: str) -> str:
    env_job_id = (os.getenv("JOB_ID") or os.getenv("PIPELINE_JOB_ID") or "").strip()
    if env_job_id:
        return env_job_id
    for raw in [checkpoint_root, success_path]:
        local = _local_path_from_uri(raw)
        if not local:
            continue
        parts = [p for p in os.path.normpath(local).split(os.sep) if p]
        if "uploads" in parts:
            idx = parts.index("uploads")
            if idx + 1 < len(parts):
                return parts[idx + 1]
        if len(parts) >= 2 and parts[-1] == "checkpoints":
            return parts[-2]
    return "unknown_job"


def _read_stage_df(stage_path: str) -> pd.DataFrame:
    return pd.read_parquet(stage_path)


def _stage_completed(stage_path: str) -> bool:
    local = _local_path_from_uri(stage_path)
    return bool(local and os.path.exists(local))


def _write_stage_df(
    df: pd.DataFrame,
    stage_path: str,
    *,
    stage: str,
    job_id: str,
) -> int:
    row_count = len(df)
    _stage_log(stage, "write_start", row_count=row_count, stage_path=stage_path)
    load_parquet(df, stage_path, mode="overwrite")
    metadata_path = f"{stage_path.rstrip('/')}.metadata.json"
    parent = os.path.dirname(metadata_path)
    if parent:
        os.makedirs(parent, exist_ok=True)
    with open(metadata_path, "w", encoding="utf-8") as handle:
        json.dump(
            {
                "stage": stage,
                "job_id": job_id,
                "row_count": row_count,
                "written_at": datetime.now(timezone.utc).isoformat(),
            },
            handle,
        )
    _stage_log(stage, "write_done", row_count=row_count, stage_path=stage_path)
    return row_count


def _attach_record_id(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if "record_id" in out.columns:
        return out
    source_cols = [col for col in out.columns if col != "record_id"]
    if source_cols:
        hashed = pd.util.hash_pandas_object(out[source_cols].astype("string"), index=False).astype("uint64").astype(str)
        out["record_id"] = "rec-" + hashed
    else:
        out["record_id"] = [f"rec-{uuid.uuid4().hex}" for _ in range(len(out))]
    return out


def _status_rows(df: pd.DataFrame, *, stage: str, status: str, error_col: str | None = None) -> pd.DataFrame:
    out = pd.DataFrame({"record_id": df.get("record_id", pd.Series(dtype="string")), "stage": stage, "status": status})
    if error_col and error_col in df.columns:
        out["error_reason"] = df[error_col]
    else:
        out["error_reason"] = pd.NA
    out["updated_at"] = datetime.now(timezone.utc).isoformat()
    return out


def _write_status(status_path: str, frames: list[pd.DataFrame]) -> None:
    if not frames:
        return
    status = pd.concat(frames, ignore_index=True)
    if "record_id" in status.columns:
        status = status.drop_duplicates(["record_id", "stage"], keep="last")
    load_parquet(status, status_path, mode="overwrite")


def _run_stage(
    stage: str,
    stage_path: str,
    transform_fn,
    *,
    audit_log: str,
    run_id: str,
    job_id: str,
    resume: bool,
) -> pd.DataFrame:
    _emit_stage(stage)
    if resume and _stage_completed(stage_path):
        df = _read_stage_df(stage_path)
        _stage_log(stage, "skip_resume", row_count=len(df), stage_path=stage_path)
        return df
    df = transform_fn()
    _write_stage_df(df, stage_path, stage=stage, job_id=job_id)
    audit_event(audit_log, "pipeline", run_id, "stage_checkpoint_written", stage=stage, stage_path=stage_path)
    return df


def _extract_raw(args: argparse.Namespace) -> pd.DataFrame:
    if args.source_type in {"excel", "xlsx"}:
        sheet = int(args.sheet) if str(args.sheet).isdigit() else args.sheet
        df = extract_data(args.source_type, args.input, sheet_name=sheet)
    elif args.source_type == "json":
        df = extract_data(args.source_type, args.input, multiline=args.multiline)
    elif args.source_type == "csv":
        df = extract_data(args.source_type, args.input, sep=args.delimiter, encoding=args.encoding)
    else:
        df = extract_data(args.source_type, args.input)
    return _attach_record_id(df)


_OUTPUT_COLUMNS = [
    "record_id",
    "source_address_old",
    "source_address_new",
    "address_source",
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
    "latitude",
    "longitude",
    "geom",
    "geometry",
    "address_type_raw",
    "address_type",
    "state_code",
    "state_name",
    "district_code",
    "district_name",
    "mukim_code",
    "mukim_name",
    "mukim_id",
    "pbt_id",
    "pbt_name",
    "confidence_score",
    "confidence_band",
    "naskod",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the NAS batch ETL with Pandas and GeoPandas.")
    parser.add_argument("--input", required=True, help="Input file path, glob, or API URL")
    parser.add_argument("--source-type", required=True, choices=["csv", "json", "excel", "xlsx", "api"])
    parser.add_argument("--success", required=True, help="Output parquet path for valid records")
    parser.add_argument("--warning", default=None, help="Output parquet path for warning records")
    parser.add_argument("--failed", required=True, help="Output parquet path for failed records")
    parser.add_argument("--config", default=None, help="Pipeline config JSON")
    parser.add_argument("--sheet", default="0", help="Excel sheet name or index")
    parser.add_argument("--multiline", action="store_true", help="Read JSON as multiline object/array")
    parser.add_argument("--delimiter", default=",", help="CSV delimiter")
    parser.add_argument("--encoding", default="utf-8", help="CSV encoding")
    parser.add_argument("--checkpoint-root", default=None, help="Directory/path prefix for stage parquet checkpoints")
    parser.add_argument("--status-path", default=None, help="Parquet path for per-record status")
    parser.add_argument("--resume", action="store_true", help="Reuse existing stage parquet outputs when present")
    parser.add_argument("--resume-failed-only", action="store_true", help="Accepted for CLI compatibility; reruns the Pandas pipeline")
    parser.add_argument("--audit-log", default="output/audit_log.jsonl")
    return parser.parse_args()


def main():
    args = parse_args()
    started = time.time()
    run_id = start_audit_run(args.audit_log, "pipeline", vars(args))
    status = "ok"
    status_frames: list[pd.DataFrame] = []
    try:
        refs = load_reference_data(args.config)
        warning_path = args.warning or f"{args.success.rstrip('/')}_warnings"
        checkpoint_root = args.checkpoint_root or f"{args.success.rstrip('/')}_checkpoints"
        status_path = args.status_path or f"{checkpoint_root.rstrip('/')}/90_record_status"
        job_id = _derive_job_id(checkpoint_root, args.success)

        stage_kwargs = dict(audit_log=args.audit_log, run_id=run_id, job_id=job_id, resume=args.resume)

        df_raw = _run_stage(
            "extract",
            f"{checkpoint_root.rstrip('/')}/10_extract_raw.parquet",
            lambda: _extract_raw(args),
            **stage_kwargs,
        )
        status_frames.append(_status_rows(df_raw, stage="extract", status="EXTRACTED"))

        df_clean_text = _run_stage(
            "clean_text",
            f"{checkpoint_root.rstrip('/')}/20_clean_text.parquet",
            lambda: clean_text_addresses(df_raw, config=refs.config, lookups=refs.lookups),
            **stage_kwargs,
        )
        status_frames.append(_status_rows(df_clean_text, stage="clean_text", status="TEXT_CLEANED"))

        df_spatial = _run_stage(
            "spatial_validate",
            f"{checkpoint_root.rstrip('/')}/30_spatial_validated.parquet",
            lambda: enrich_spatial_components(
                df_clean_text,
                config=refs.config,
                postcode_boundaries=refs.postcode_boundaries,
                admin_boundaries=refs.admin_boundaries,
                pbt_boundaries=refs.pbt_boundaries,
            ),
            **stage_kwargs,
        )

        df_success, df_warning, df_failed = validate_addresses(df_spatial, require_mukim=True)
        status_frames.append(_status_rows(df_success, stage="validate", status="VALIDATED_PASS"))
        status_frames.append(_status_rows(df_warning, stage="validate", status="VALIDATED_WARNING", error_col="warning_reason"))
        status_frames.append(_status_rows(df_failed, stage="validate", status="VALIDATED_FAIL", error_col="error_reason"))

        _emit_stage("final")
        df_success = add_standard_naskod(df_success, output_col="naskod")
        df_warning = add_standard_naskod(df_warning, output_col="naskod")
        df_failed = add_standard_naskod(df_failed, output_col="naskod")

        df_success = df_success[[col for col in _OUTPUT_COLUMNS if col in df_success.columns]]
        df_warning = df_warning[
            [col for col in _OUTPUT_COLUMNS if col in df_warning.columns]
            + [col for col in ["validation_status", "warning_reason", "warning_reasons", "reason_codes"] if col in df_warning.columns]
        ]
        df_failed = df_failed[
            [col for col in _OUTPUT_COLUMNS if col in df_failed.columns]
            + [col for col in ["validation_status", "error_reason", "reason_codes"] if col in df_failed.columns]
        ]

        load_success_warning_failed(df_success, df_warning, df_failed, args.success, warning_path, args.failed)
        _write_status(status_path, status_frames)
        _stage_log("load", "done", success_count=len(df_success), warning_count=len(df_warning), failed_count=len(df_failed))
        audit_event(
            args.audit_log,
            "pipeline",
            run_id,
            "write_complete",
            input_path=args.input,
            success_path=args.success,
            warning_path=warning_path,
            failed_path=args.failed,
            raw_count=len(df_raw),
            success_count=len(df_success),
            warning_count=len(df_warning),
            failed_count=len(df_failed),
        )
    except Exception as exc:
        status = "error"
        audit_event(args.audit_log, "pipeline", run_id, "run_error", error_type=type(exc).__name__, error=str(exc))
        raise
    finally:
        audit_event(
            args.audit_log,
            "pipeline",
            run_id,
            "run_end",
            status=status,
            duration_ms=int((time.time() - started) * 1000),
        )


if __name__ == "__main__":
    main()
