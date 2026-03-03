import argparse
import json
import os
import time

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    coalesce,
    col,
    concat_ws,
    input_file_name,
    lit,
    row_number,
    sha2,
    struct,
    to_json,
)

from .audit_log import audit_event, start_audit_run
from .extract import extract_data
from .load import load_parquet, load_success_failed
from .naskod_utils import add_standard_naskod
from .sedona_utils import configure_sedona_builder, merge_spark_packages, resolve_sedona_spark_packages
from .transform import clean_addresses, validate_addresses


def _is_corrupt_only(df) -> bool:
    return len(df.columns) == 1 and df.columns[0] == "_corrupt_record"


def _path_exists(spark: SparkSession, path: str) -> bool:
    jvm = spark._jvm
    hconf = spark._jsc.hadoopConfiguration()
    j_path = jvm.org.apache.hadoop.fs.Path(path)
    fs = j_path.getFileSystem(hconf)
    return bool(fs.exists(j_path))


def _stage_done(spark: SparkSession, stage_path: str) -> bool:
    marker = f"{stage_path.rstrip('/')}/_SUCCESS"
    return _path_exists(spark, marker)


def _read_stage_df(spark: SparkSession, stage_path: str):
    return spark.read.parquet(stage_path)


def _write_stage_df(df, stage_path: str) -> None:
    load_parquet(df, stage_path, mode="overwrite")


def _emit_stage(stage: str) -> None:
    print(f"PIPELINE_STAGE:{stage}", flush=True)


def _attach_record_id(df):
    if "record_id" in df.columns:
        return df

    def _quoted_col(name: str):
        # Quote source column names so Spark treats dots/special chars as literal names.
        escaped = name.replace("`", "``")
        return col(f"`{escaped}`")

    payload_cols = [_quoted_col(c) for c in sorted(df.columns)]
    payload_json = to_json(struct(*payload_cols)) if payload_cols else lit("")
    return df.withColumn(
        "record_id",
        sha2(concat_ws("||", coalesce(input_file_name(), lit("")), coalesce(payload_json, lit(""))), 256),
    )


def _status_rows(df, *, stage: str, status: str, error_col: str | None = None):
    out = df.select("record_id").dropna(subset=["record_id"]).dropDuplicates(["record_id"])
    out = out.withColumn("stage", lit(stage))
    out = out.withColumn("status", lit(status))
    if error_col and error_col in df.columns:
        err_df = (
            df.select("record_id", col(error_col).cast("string").alias("error_reason"))
            .dropna(subset=["record_id"])
            .dropDuplicates(["record_id"])
        )
        out = out.join(err_df, "record_id", "left")
    else:
        out = out.withColumn("error_reason", lit(None).cast("string"))
    out = out.withColumn("updated_ms", lit(int(time.time() * 1000)))
    return out


def _upsert_status(spark: SparkSession, status_path: str, updates_df) -> None:
    if _path_exists(spark, status_path):
        existing = _read_stage_df(spark, status_path)
        merged = existing.unionByName(updates_df, allowMissingColumns=True)
    else:
        merged = updates_df
    win = Window.partitionBy("record_id").orderBy(col("updated_ms").desc())
    latest = merged.withColumn("_rn", row_number().over(win)).filter(col("_rn") == 1).drop("_rn")
    _write_stage_df(latest, status_path)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="NAS Batch ETL")
    parser.add_argument("--input", required=True, help="Input path or folder")
    parser.add_argument(
        "--source-type",
        default="csv",
        choices=["csv", "json", "excel", "xlsx", "api"],
        help="Input type (default: csv)",
    )
    parser.add_argument("--sheet", default="0", help="Excel sheet name or index (excel only)")
    parser.add_argument("--multiline", action="store_true", help="JSON multiline (json only)")
    parser.add_argument("--delimiter", default=",", help="CSV delimiter")
    parser.add_argument("--encoding", default="utf-8", help="CSV encoding")
    parser.add_argument("--config", default=None, help="Optional JSON config path for column aliases")
    parser.add_argument(
        "--checkpoint-root",
        default=None,
        help="Checkpoint root folder for staged parquet outputs (default: <success>_checkpoints)",
    )
    parser.add_argument("--resume", action="store_true", help="Resume from completed stage checkpoints")
    parser.add_argument(
        "--status-path",
        default=None,
        help="Record status parquet path (default: <checkpoint-root>/90_record_status)",
    )
    parser.add_argument(
        "--resume-failed-only",
        action="store_true",
        help="Process only records not marked DONE in status store",
    )
    parser.add_argument("--success", required=True, help="Success output path")
    parser.add_argument("--failed", required=True, help="Failed output path")
    parser.add_argument(
        "--audit-log",
        default=os.getenv("NAS_AUDIT_LOG", "logs/nas_audit.log"),
        help="JSONL audit logfile path",
    )
    return parser.parse_args()


def _lookup_source_from_config(config_path: str | None) -> str:
    if not config_path or not os.path.exists(config_path):
        return "files"
    try:
        with open(config_path, "r", encoding="utf-8") as handle:
            config = json.load(handle)
    except (OSError, json.JSONDecodeError):
        return "files"
    return str(config.get("lookup_source", "files")).strip().lower()


def main():
    args = parse_args()
    started = time.time()
    run_id = start_audit_run(args.audit_log, "pipeline", vars(args))
    spark = None
    status = "ok"
    try:
        builder = (
            SparkSession.builder
            .appName("NAS Batch ETL")
            .config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow")
            .config("spark.executor.extraJavaOptions", "-Djava.security.manager=allow")
            .master("local[*]")   # use all local cores
        )
        spark_driver_memory = os.getenv("SPARK_DRIVER_MEMORY")
        spark_executor_memory = os.getenv("SPARK_EXECUTOR_MEMORY")
        spark_shuffle_partitions = os.getenv("SPARK_SQL_SHUFFLE_PARTITIONS")
        if spark_driver_memory:
            builder = builder.config("spark.driver.memory", spark_driver_memory)
        if spark_executor_memory:
            builder = builder.config("spark.executor.memory", spark_executor_memory)
        if spark_shuffle_partitions:
            builder = builder.config("spark.sql.shuffle.partitions", spark_shuffle_partitions)
        builder = configure_sedona_builder(builder)
        jars_packages = resolve_sedona_spark_packages()
        if _lookup_source_from_config(args.config) == "db":
            jars_packages = merge_spark_packages(jars_packages, "org.postgresql:postgresql:42.7.3")
        if jars_packages:
            builder = builder.config("spark.jars.packages", jars_packages)
        spark = builder.getOrCreate()

        input_path = args.input
        success_path = args.success
        failed_path = args.failed
        checkpoint_root = args.checkpoint_root or f"{success_path.rstrip('/')}_checkpoints"
        status_path = args.status_path or f"{checkpoint_root.rstrip('/')}/90_record_status"

        stage_extract = f"{checkpoint_root.rstrip('/')}/10_extract_raw"
        stage_extract_resume_filtered = f"{checkpoint_root.rstrip('/')}/11_extract_resume_filtered"
        stage_clean = f"{checkpoint_root.rstrip('/')}/20_clean"
        stage_success = f"{checkpoint_root.rstrip('/')}/30_validated_success"
        stage_failed = f"{checkpoint_root.rstrip('/')}/31_validated_failed"
        stage_success_final = f"{checkpoint_root.rstrip('/')}/40_success_final"
        stage_failed_final = f"{checkpoint_root.rstrip('/')}/41_failed_final"

        _emit_stage("extract")
        if args.resume and _stage_done(spark, stage_extract):
            df_raw = _read_stage_df(spark, stage_extract)
            audit_event(
                args.audit_log,
                "pipeline",
                run_id,
                "stage_resume",
                stage="extract",
                stage_path=stage_extract,
            )
        else:
            if args.source_type in {"excel", "xlsx"}:
                sheet = int(args.sheet) if str(args.sheet).isdigit() else args.sheet
                df_raw = extract_data(spark, args.source_type, input_path, sheet_name=sheet)
            elif args.source_type == "json":
                df_raw = extract_data(spark, args.source_type, input_path, multiline=args.multiline)
                if _is_corrupt_only(df_raw) and not args.multiline:
                    # Common case: JSON array/file pretty-printed across multiple lines.
                    df_raw = extract_data(spark, args.source_type, input_path, multiline=True)
            elif args.source_type == "csv":
                df_raw = extract_data(
                    spark,
                    args.source_type,
                    input_path,
                    sep=args.delimiter,
                    encoding=args.encoding,
                )
            else:
                df_raw = extract_data(spark, args.source_type, input_path)
            if _is_corrupt_only(df_raw):
                raise ValueError(
                    "Input parsed as _corrupt_record only. "
                    "Check --source-type and file format. "
                    "For multiline JSON, use --source-type json --multiline."
                )
            df_raw = _attach_record_id(df_raw)
            _write_stage_df(df_raw, stage_extract)
            audit_event(
                args.audit_log,
                "pipeline",
                run_id,
                "stage_checkpoint_written",
                stage="extract",
                stage_path=stage_extract,
            )
        df_raw = _attach_record_id(df_raw)

        if args.resume_failed_only and _path_exists(spark, status_path):
            status_df = _read_stage_df(spark, status_path)
            pending_ids = (
                status_df.filter(col("status") != lit("DONE"))
                .select("record_id")
                .dropna(subset=["record_id"])
                .dropDuplicates(["record_id"])
            )
            pending_count = pending_ids.count()
            if pending_count == 0:
                audit_event(
                    args.audit_log,
                    "pipeline",
                    run_id,
                    "no_pending_records",
                    status_path=status_path,
                )
                return
            before_count = df_raw.count()
            df_raw = df_raw.join(pending_ids, "record_id", "inner")
            after_count = df_raw.count()
            audit_event(
                args.audit_log,
                "pipeline",
                run_id,
                "resume_failed_filter",
                status_path=status_path,
                before_count=before_count,
                after_count=after_count,
            )
            # Break lineage from status parquet before status table overwrite to avoid
            # stale file handles during later stages (common in resume-failed-only mode).
            _write_stage_df(df_raw, stage_extract_resume_filtered)
            df_raw = _read_stage_df(spark, stage_extract_resume_filtered)

        _upsert_status(
            spark,
            status_path,
            _status_rows(df_raw, stage="extract", status="EXTRACTED"),
        )

        _emit_stage("clean")
        if args.resume and (not args.resume_failed_only) and _stage_done(spark, stage_clean):
            df_clean = _read_stage_df(spark, stage_clean)
            audit_event(
                args.audit_log,
                "pipeline",
                run_id,
                "stage_resume",
                stage="clean",
                stage_path=stage_clean,
            )
        else:
            df_clean = clean_addresses(df_raw, config_path=args.config)
            _write_stage_df(df_clean, stage_clean)
            audit_event(
                args.audit_log,
                "pipeline",
                run_id,
                "stage_checkpoint_written",
                stage="clean",
                stage_path=stage_clean,
            )
        _upsert_status(
            spark,
            status_path,
            _status_rows(df_clean, stage="clean", status="CLEANED"),
        )

        _emit_stage("validated")
        if args.resume and (not args.resume_failed_only) and _stage_done(spark, stage_success) and _stage_done(spark, stage_failed):
            df_success = _read_stage_df(spark, stage_success)
            df_failed = _read_stage_df(spark, stage_failed)
            audit_event(
                args.audit_log,
                "pipeline",
                run_id,
                "stage_resume",
                stage="validate",
                stage_path_success=stage_success,
                stage_path_failed=stage_failed,
            )
        else:
            df_success, df_failed = validate_addresses(df_clean, require_mukim=True)
            _write_stage_df(df_success, stage_success)
            _write_stage_df(df_failed, stage_failed)
            # Truncate Spark lineage after heavy validate stage to make final write more stable.
            df_success = _read_stage_df(spark, stage_success)
            df_failed = _read_stage_df(spark, stage_failed)
            audit_event(
                args.audit_log,
                "pipeline",
                run_id,
                "stage_checkpoint_written",
                stage="validate",
                stage_path_success=stage_success,
                stage_path_failed=stage_failed,
            )
        _upsert_status(
            spark,
            status_path,
            _status_rows(df_success, stage="validate", status="VALIDATED_PASS").unionByName(
                _status_rows(df_failed, stage="validate", status="VALIDATED_FAIL", error_col="error_reason"),
                allowMissingColumns=True,
            ),
        )

        _emit_stage("final")
        parsed_cols = [
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
            "top_3_candidates",
            "candidate_match_score",
            "correction_notes",
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
        if args.resume and (not args.resume_failed_only) and _stage_done(spark, stage_success_final) and _stage_done(spark, stage_failed_final):
            df_success = _read_stage_df(spark, stage_success_final)
            df_failed = _read_stage_df(spark, stage_failed_final)
            audit_event(
                args.audit_log,
                "pipeline",
                run_id,
                "stage_resume",
                stage="finalize",
                stage_path_success=stage_success_final,
                stage_path_failed=stage_failed_final,
            )
        else:
            df_success = add_standard_naskod(df_success, output_col="naskod")
            df_failed = add_standard_naskod(df_failed, output_col="naskod")
            success_cols = [c for c in parsed_cols if c in df_success.columns]
            failed_cols = [c for c in parsed_cols if c in df_failed.columns] + [
                c for c in ["validation_status", "error_reason", "reason_codes"] if c in df_failed.columns
            ]
            df_success = df_success.select(*success_cols)
            df_failed = df_failed.select(*failed_cols)
            _write_stage_df(df_success, stage_success_final)
            _write_stage_df(df_failed, stage_failed_final)
            audit_event(
                args.audit_log,
                "pipeline",
                run_id,
                "stage_checkpoint_written",
                stage="finalize",
                stage_path_success=stage_success_final,
                stage_path_failed=stage_failed_final,
            )

        _upsert_status(
            spark,
            status_path,
            _status_rows(df_success, stage="finalize", status="DONE").unionByName(
                _status_rows(df_failed, stage="finalize", status="FAILED", error_col="error_reason"),
                allowMissingColumns=True,
            ),
        )

        if args.resume_failed_only and _path_exists(spark, success_path) and _path_exists(spark, failed_path):
            prev_success = _read_stage_df(spark, success_path)
            prev_failed = _read_stage_df(spark, failed_path)
            if "record_id" in prev_success.columns and "record_id" in prev_failed.columns:
                processed_ids = (
                    df_success.select("record_id")
                    .unionByName(df_failed.select("record_id"))
                    .dropna(subset=["record_id"])
                    .dropDuplicates(["record_id"])
                )
                df_success = prev_success.join(processed_ids, "record_id", "left_anti").unionByName(
                    df_success, allowMissingColumns=True
                )
                df_failed = prev_failed.join(processed_ids, "record_id", "left_anti").unionByName(
                    df_failed, allowMissingColumns=True
                )
            else:
                audit_event(
                    args.audit_log,
                    "pipeline",
                    run_id,
                    "resume_failed_merge_skipped",
                    reason="existing_output_missing_record_id",
                )

        raw_count = df_raw.count()
        success_count = df_success.count()
        failed_count = df_failed.count()
        audit_event(
            args.audit_log,
            "pipeline",
            run_id,
            "validation_complete",
            input_path=input_path,
            success_path=success_path,
            failed_path=failed_path,
            raw_count=raw_count,
            success_count=success_count,
            failed_count=failed_count,
        )

        load_success_failed(df_success, df_failed, success_path, failed_path)
        audit_event(
            args.audit_log,
            "pipeline",
            run_id,
            "write_complete",
            success_path=success_path,
            failed_path=failed_path,
        )
    except Exception as exc:
        status = "error"
        audit_event(
            args.audit_log,
            "pipeline",
            run_id,
            "run_error",
            error_type=type(exc).__name__,
            error=str(exc),
        )
        raise
    finally:
        if spark is not None:
            spark.stop()
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
