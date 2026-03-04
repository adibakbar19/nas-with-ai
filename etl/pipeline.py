import argparse
import json
import os
import time
import uuid
from datetime import datetime, timezone

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
    upper,
    when,
)

from .audit_log import audit_event, start_audit_run
from .extract import extract_data
from .load import load_parquet, load_success_failed
from .naskod_utils import add_standard_naskod
from .sedona_utils import configure_sedona_builder, merge_spark_packages, resolve_sedona_spark_packages
from .transform import clean_addresses, validate_addresses


def _is_corrupt_only(df) -> bool:
    return len(df.columns) == 1 and df.columns[0] == "_corrupt_record"


def _fs_and_path(spark: SparkSession, path: str):
    jvm = spark._jvm
    hconf = spark._jsc.hadoopConfiguration()
    j_path = jvm.org.apache.hadoop.fs.Path(path)
    fs = j_path.getFileSystem(hconf)
    return fs, j_path


def _path_exists(spark: SparkSession, path: str) -> bool:
    fs, j_path = _fs_and_path(spark, path)
    return bool(fs.exists(j_path))


def _has_parquet_files(spark: SparkSession, path: str) -> bool:
    if not _path_exists(spark, path):
        return False
    fs, root = _fs_and_path(spark, path)
    stack = [root]
    while stack:
        current = stack.pop()
        for status in fs.listStatus(current):
            candidate = status.getPath()
            name = candidate.getName()
            if status.isDirectory():
                if name.startswith("_") or name.startswith("."):
                    continue
                stack.append(candidate)
                continue
            if name.endswith(".parquet"):
                return True
    return False


def stage_completed(spark: SparkSession, stage_path: str) -> bool:
    normalized = stage_path.rstrip("/")
    return (
        _path_exists(spark, normalized)
        and _path_exists(spark, f"{normalized}/_SUCCESS")
        and _has_parquet_files(spark, normalized)
    )


def _stage_log(stage: str, event: str, **fields) -> None:
    suffix = " ".join(f"{k}={v}" for k, v in fields.items())
    print(
        f"CHECKPOINT stage={stage} event={event}" + (f" {suffix}" if suffix else ""),
        flush=True,
    )


def _delete_path_if_exists(spark: SparkSession, path: str) -> None:
    fs, j_path = _fs_and_path(spark, path)
    if fs.exists(j_path):
        fs.delete(j_path, True)


def _touch_file(spark: SparkSession, path: str, content: str = "") -> None:
    fs, j_path = _fs_and_path(spark, path)
    parent = j_path.getParent()
    if parent is not None and not fs.exists(parent):
        fs.mkdirs(parent)
    stream = fs.create(j_path, True)
    try:
        stream.write(bytearray(content.encode("utf-8")))
    finally:
        stream.close()


def _atomic_replace_path(spark: SparkSession, src_path: str, dst_path: str) -> None:
    fs, src = _fs_and_path(spark, src_path)
    _, dst = _fs_and_path(spark, dst_path)
    if not fs.exists(src):
        raise RuntimeError(f"Temporary stage path missing: {src_path}")

    backup = None
    if fs.exists(dst):
        backup_path = f"{dst_path.rstrip('/')}.bak_{int(time.time() * 1000)}_{uuid.uuid4().hex[:8]}"
        _, backup = _fs_and_path(spark, backup_path)
        _delete_path_if_exists(spark, backup_path)
        if not fs.rename(dst, backup):
            raise RuntimeError(f"Failed to move existing stage path to backup: {dst_path}")

    if not fs.rename(src, dst):
        if backup is not None and not fs.exists(dst):
            fs.rename(backup, dst)
        raise RuntimeError(f"Failed to promote temporary stage path: {src_path} -> {dst_path}")

    if backup is not None and fs.exists(backup):
        fs.delete(backup, True)


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


def _maybe_sync_checkpoint_to_minio(
    stage_path: str,
    *,
    stage: str,
    checkpoint_root: str,
    job_id: str,
) -> None:
    if os.getenv("CHECKPOINT_STORE", "local").strip().lower() != "minio":
        return
    if not stage_path.startswith(checkpoint_root.rstrip("/") + "/") and stage_path.rstrip("/") != checkpoint_root.rstrip("/"):
        return

    local_stage = _local_path_from_uri(stage_path)
    if not local_stage or not os.path.isdir(local_stage):
        _stage_log(stage, "sync_skip", reason="non_local_stage_path", stage_path=stage_path)
        return

    endpoint = os.getenv("MINIO_ENDPOINT", "").strip()
    access_key = os.getenv("MINIO_ACCESS_KEY", "").strip()
    secret_key = os.getenv("MINIO_SECRET_KEY", "").strip()
    bucket = os.getenv("CHECKPOINT_MINIO_BUCKET", "nas-checkpoints").strip() or "nas-checkpoints"
    secure = os.getenv("MINIO_SECURE", "false").strip().lower() in {"1", "true", "yes", "y"}
    if not endpoint or not access_key or not secret_key:
        _stage_log(stage, "sync_skip", reason="minio_env_missing")
        return

    try:
        from minio import Minio  # type: ignore
    except Exception as exc:
        _stage_log(stage, "sync_skip", reason=f"minio_import_failed:{exc}")
        return

    try:
        client = Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=secure)
        if not client.bucket_exists(bucket):
            client.make_bucket(bucket)
        prefix = f"{job_id}/{os.path.basename(stage_path.rstrip('/'))}"
        for root, _, files in os.walk(local_stage):
            for name in files:
                local_file = os.path.join(root, name)
                rel = os.path.relpath(local_file, local_stage).replace(os.sep, "/")
                object_name = f"{prefix}/{rel}"
                client.fput_object(bucket, object_name, local_file)
        _stage_log(stage, "sync_done", bucket=bucket, prefix=prefix)
    except Exception as exc:
        _stage_log(stage, "sync_failed", error=exc)


def _read_stage_df(spark: SparkSession, stage_path: str):
    return spark.read.parquet(stage_path)


def _write_stage_df(
    spark: SparkSession,
    df,
    stage_path: str,
    *,
    stage: str,
    job_id: str,
    checkpoint_root: str,
) -> int:
    row_count = df.count()
    tmp_path = f"{stage_path.rstrip('/')}_tmp_{int(time.time() * 1000)}_{uuid.uuid4().hex[:8]}"
    _delete_path_if_exists(spark, tmp_path)
    _stage_log(stage, "write_tmp_start", row_count=row_count, tmp_path=tmp_path)
    load_parquet(df, tmp_path, mode="overwrite")
    _atomic_replace_path(spark, tmp_path, stage_path)
    if not _path_exists(spark, f"{stage_path.rstrip('/')}/_SUCCESS"):
        _touch_file(spark, f"{stage_path.rstrip('/')}/_SUCCESS")
    metadata = {
        "stage": stage,
        "job_id": job_id,
        "row_count": row_count,
        "written_at": datetime.now(timezone.utc).isoformat(),
    }
    _touch_file(
        spark,
        f"{stage_path.rstrip('/')}/_stage_metadata/metadata.json",
        content=json.dumps(metadata, ensure_ascii=True),
    )
    _maybe_sync_checkpoint_to_minio(
        stage_path,
        stage=stage,
        checkpoint_root=checkpoint_root,
        job_id=job_id,
    )
    _stage_log(stage, "write_done", row_count=row_count, stage_path=stage_path)
    return row_count


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


def _upsert_status(
    spark: SparkSession,
    status_path: str,
    updates_df,
    *,
    checkpoint_root: str,
    job_id: str,
) -> None:
    if _has_parquet_files(spark, status_path):
        try:
            existing = _read_stage_df(spark, status_path)
            merged = existing.unionByName(updates_df, allowMissingColumns=True)
        except Exception as exc:
            _stage_log("record_status", "upsert_recover", reason="read_failed_fallback_updates_only", error=exc)
            merged = updates_df
    else:
        merged = updates_df
    win = Window.partitionBy("record_id").orderBy(col("updated_ms").desc())
    latest = merged.withColumn("_rn", row_number().over(win)).filter(col("_rn") == 1).drop("_rn")
    _write_stage_df(
        spark,
        latest,
        status_path,
        stage="record_status",
        job_id=job_id,
        checkpoint_root=checkpoint_root,
    )


def rebuild_record_status(
    spark: SparkSession,
    *,
    status_path: str,
    validated_success_path: str,
    validated_failed_path: str,
    checkpoint_root: str,
    job_id: str,
) -> int:
    _stage_log("record_status", "rebuild_start", status_path=status_path)
    now_ms = int(time.time() * 1000)
    rebuilt_frames = []

    if _has_parquet_files(spark, validated_success_path):
        success_df = _read_stage_df(spark, validated_success_path)
        if "record_id" in success_df.columns:
            rebuilt_frames.append(
                success_df.select("record_id")
                .dropna(subset=["record_id"])
                .dropDuplicates(["record_id"])
                .withColumn("stage", lit("validate"))
                .withColumn("status", lit("SUCCESS"))
                .withColumn("error_reason", lit(None).cast("string"))
                .withColumn("updated_ms", lit(now_ms))
            )

    if _has_parquet_files(spark, validated_failed_path):
        failed_df = _read_stage_df(spark, validated_failed_path)
        if "record_id" in failed_df.columns:
            status_df = (
                failed_df.select("record_id")
                .dropna(subset=["record_id"])
                .dropDuplicates(["record_id"])
                .withColumn("stage", lit("validate"))
                .withColumn("status", lit("FAILED"))
                .withColumn("updated_ms", lit(now_ms))
            )
            if "error_reason" in failed_df.columns:
                err_df = (
                    failed_df.select("record_id", col("error_reason").cast("string").alias("error_reason"))
                    .dropna(subset=["record_id"])
                    .dropDuplicates(["record_id"])
                )
                status_df = status_df.join(err_df, "record_id", "left")
            else:
                status_df = status_df.withColumn("error_reason", lit(None).cast("string"))
            rebuilt_frames.append(status_df)

    if not rebuilt_frames:
        raise RuntimeError(
            "Cannot rebuild 90_record_status: missing readable validated success/failed checkpoints."
        )

    merged = rebuilt_frames[0]
    for frame in rebuilt_frames[1:]:
        merged = merged.unionByName(frame, allowMissingColumns=True)
    merged = merged.withColumn("_priority", when(col("status") == lit("FAILED"), lit(2)).otherwise(lit(1)))
    win = Window.partitionBy("record_id").orderBy(col("_priority").desc(), col("updated_ms").desc())
    latest = merged.withColumn("_rn", row_number().over(win)).filter(col("_rn") == 1).drop("_rn", "_priority")
    row_count = _write_stage_df(
        spark,
        latest,
        status_path,
        stage="record_status",
        job_id=job_id,
        checkpoint_root=checkpoint_root,
    )
    _stage_log("record_status", "rebuild_done", row_count=row_count, status_path=status_path)
    return row_count


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
            .master("local[*]")
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
        job_id = _derive_job_id(checkpoint_root, success_path)

        stage_extract = f"{checkpoint_root.rstrip('/')}/10_extract_raw"
        stage_extract_resume_filtered = f"{checkpoint_root.rstrip('/')}/11_extract_resume_filtered"
        stage_clean = f"{checkpoint_root.rstrip('/')}/20_clean"
        stage_success = f"{checkpoint_root.rstrip('/')}/30_validated_success"
        stage_failed = f"{checkpoint_root.rstrip('/')}/31_validated_failed"
        stage_success_final = f"{checkpoint_root.rstrip('/')}/40_success_final"
        stage_failed_final = f"{checkpoint_root.rstrip('/')}/41_failed_final"

        _emit_stage("extract")
        _stage_log("extract", "start", resume=args.resume)
        if args.resume and stage_completed(spark, stage_extract):
            df_raw = _read_stage_df(spark, stage_extract)
            extract_count = df_raw.count()
            _stage_log("extract", "skip_resume", row_count=extract_count, stage_path=stage_extract)
            audit_event(
                args.audit_log,
                "pipeline",
                run_id,
                "stage_resume",
                stage="extract",
                stage_path=stage_extract,
            )
        else:
            if args.resume and _path_exists(spark, stage_extract):
                _stage_log("extract", "rerun", reason="incomplete_checkpoint", stage_path=stage_extract)
            else:
                _stage_log("extract", "rerun", reason="resume_disabled_or_missing", stage_path=stage_extract)
            if args.source_type in {"excel", "xlsx"}:
                sheet = int(args.sheet) if str(args.sheet).isdigit() else args.sheet
                df_raw = extract_data(spark, args.source_type, input_path, sheet_name=sheet)
            elif args.source_type == "json":
                df_raw = extract_data(spark, args.source_type, input_path, multiline=args.multiline)
                if _is_corrupt_only(df_raw) and not args.multiline:
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
            extract_count = _write_stage_df(
                spark,
                df_raw,
                stage_extract,
                stage="extract",
                job_id=job_id,
                checkpoint_root=checkpoint_root,
            )
            audit_event(
                args.audit_log,
                "pipeline",
                run_id,
                "stage_checkpoint_written",
                stage="extract",
                stage_path=stage_extract,
            )
        df_raw = _attach_record_id(df_raw)

        if args.resume_failed_only:
            status_df = None
            if not _has_parquet_files(spark, status_path):
                _stage_log("record_status", "rebuild_needed", reason="missing_or_incomplete")
                rebuild_record_status(
                    spark,
                    status_path=status_path,
                    validated_success_path=stage_success,
                    validated_failed_path=stage_failed,
                    checkpoint_root=checkpoint_root,
                    job_id=job_id,
                )
            try:
                status_df = _read_stage_df(spark, status_path)
            except Exception as exc:
                _stage_log("record_status", "rebuild_needed", reason="status_read_failed", error=exc)
                rebuild_record_status(
                    spark,
                    status_path=status_path,
                    validated_success_path=stage_success,
                    validated_failed_path=stage_failed,
                    checkpoint_root=checkpoint_root,
                    job_id=job_id,
                )
                status_df = _read_stage_df(spark, status_path)
            pending_ids = (
                status_df.filter(upper(coalesce(col("status"), lit(""))) != lit("DONE"))
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
            _write_stage_df(
                spark,
                df_raw,
                stage_extract_resume_filtered,
                stage="extract_resume_filtered",
                job_id=job_id,
                checkpoint_root=checkpoint_root,
            )
            df_raw = _read_stage_df(spark, stage_extract_resume_filtered)

        _upsert_status(
            spark,
            status_path,
            _status_rows(df_raw, stage="extract", status="EXTRACTED"),
            checkpoint_root=checkpoint_root,
            job_id=job_id,
        )

        _emit_stage("clean")
        _stage_log("clean", "start", resume=args.resume)
        if args.resume and (not args.resume_failed_only) and stage_completed(spark, stage_clean):
            df_clean = _read_stage_df(spark, stage_clean)
            clean_count = df_clean.count()
            _stage_log("clean", "skip_resume", row_count=clean_count, stage_path=stage_clean)
            audit_event(
                args.audit_log,
                "pipeline",
                run_id,
                "stage_resume",
                stage="clean",
                stage_path=stage_clean,
            )
        else:
            if args.resume and (not args.resume_failed_only) and _path_exists(spark, stage_clean):
                _stage_log("clean", "rerun", reason="incomplete_checkpoint", stage_path=stage_clean)
            else:
                _stage_log("clean", "rerun", reason="resume_disabled_or_missing", stage_path=stage_clean)
            df_clean = clean_addresses(df_raw, config_path=args.config)
            clean_count = _write_stage_df(
                spark,
                df_clean,
                stage_clean,
                stage="clean",
                job_id=job_id,
                checkpoint_root=checkpoint_root,
            )
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
            checkpoint_root=checkpoint_root,
            job_id=job_id,
        )

        _emit_stage("validated")
        _stage_log("validate", "start", resume=args.resume)
        if (
            args.resume
            and (not args.resume_failed_only)
            and stage_completed(spark, stage_success)
            and stage_completed(spark, stage_failed)
        ):
            df_success = _read_stage_df(spark, stage_success)
            df_failed = _read_stage_df(spark, stage_failed)
            validate_success_count = df_success.count()
            validate_failed_count = df_failed.count()
            _stage_log(
                "validate",
                "skip_resume",
                success_rows=validate_success_count,
                failed_rows=validate_failed_count,
                stage_path_success=stage_success,
                stage_path_failed=stage_failed,
            )
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
            if args.resume and (not args.resume_failed_only) and (
                _path_exists(spark, stage_success) or _path_exists(spark, stage_failed)
            ):
                _stage_log("validate", "rerun", reason="incomplete_checkpoint")
            else:
                _stage_log("validate", "rerun", reason="resume_disabled_or_missing")
            df_success, df_failed = validate_addresses(df_clean, require_mukim=True)
            validate_success_count = _write_stage_df(
                spark,
                df_success,
                stage_success,
                stage="validate_success",
                job_id=job_id,
                checkpoint_root=checkpoint_root,
            )
            validate_failed_count = _write_stage_df(
                spark,
                df_failed,
                stage_failed,
                stage="validate_failed",
                job_id=job_id,
                checkpoint_root=checkpoint_root,
            )
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
            checkpoint_root=checkpoint_root,
            job_id=job_id,
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
        _stage_log("finalize", "start", resume=args.resume)
        if (
            args.resume
            and (not args.resume_failed_only)
            and stage_completed(spark, stage_success_final)
            and stage_completed(spark, stage_failed_final)
        ):
            df_success = _read_stage_df(spark, stage_success_final)
            df_failed = _read_stage_df(spark, stage_failed_final)
            final_success_count = df_success.count()
            final_failed_count = df_failed.count()
            _stage_log(
                "finalize",
                "skip_resume",
                success_rows=final_success_count,
                failed_rows=final_failed_count,
                stage_path_success=stage_success_final,
                stage_path_failed=stage_failed_final,
            )
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
            if args.resume and (not args.resume_failed_only) and (
                _path_exists(spark, stage_success_final) or _path_exists(spark, stage_failed_final)
            ):
                _stage_log("finalize", "rerun", reason="incomplete_checkpoint")
            else:
                _stage_log("finalize", "rerun", reason="resume_disabled_or_missing")
            df_success = add_standard_naskod(df_success, output_col="naskod")
            df_failed = add_standard_naskod(df_failed, output_col="naskod")
            success_cols = [c for c in parsed_cols if c in df_success.columns]
            failed_cols = [c for c in parsed_cols if c in df_failed.columns] + [
                c for c in ["validation_status", "error_reason", "reason_codes"] if c in df_failed.columns
            ]
            df_success = df_success.select(*success_cols)
            df_failed = df_failed.select(*failed_cols)
            final_success_count = _write_stage_df(
                spark,
                df_success,
                stage_success_final,
                stage="final_success",
                job_id=job_id,
                checkpoint_root=checkpoint_root,
            )
            final_failed_count = _write_stage_df(
                spark,
                df_failed,
                stage_failed_final,
                stage="final_failed",
                job_id=job_id,
                checkpoint_root=checkpoint_root,
            )
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
            checkpoint_root=checkpoint_root,
            job_id=job_id,
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

        _stage_log("load", "start", success_path=success_path, failed_path=failed_path)
        load_success_failed(df_success, df_failed, success_path, failed_path)
        _stage_log("load", "done", success_count=success_count, failed_count=failed_count)
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
