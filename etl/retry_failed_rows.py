import argparse
import os
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, coalesce

from .audit_log import audit_event, start_audit_run
from .load import load_success_failed
from .naskod_utils import add_standard_naskod
from .sedona_utils import configure_sedona_builder, resolve_sedona_spark_packages
from .transform import clean_addresses, validate_addresses


DERIVED_COLUMNS = {
    "address_clean",
    "premise_no",
    "street_name",
    "locality_name",
    "postcode",
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
    "validation_status",
    "error_reason",
    "error_reasons",
    "reason_codes",
    "_dup_rank",
    "_address_norm",
    "seg2_norm",
    "seg3_norm",
    "seg4_norm",
    "seg5_norm",
}


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


def _apply_corrections(df_failed, corrections):
    if "record_id" not in df_failed.columns:
        raise ValueError("Failed parquet must include 'record_id'.")
    if "record_id" not in corrections.columns:
        raise ValueError("Corrections CSV must include 'record_id'.")

    correction_cols = [c for c in corrections.columns if c != "record_id"]
    if not correction_cols:
        raise ValueError("Corrections CSV has no correction columns. Add at least one column besides record_id.")

    prefixed = corrections.select("record_id", *[col(c).alias(f"_corr_{c}") for c in correction_cols])
    out = df_failed.join(prefixed, "record_id", "left")

    target_address_col = _detect_address_col(df_failed.columns)
    corrected_address_col = "_corr_corrected_address"
    if corrected_address_col in out.columns:
        if target_address_col:
            out = out.withColumn(target_address_col, coalesce(col(corrected_address_col), col(target_address_col)))
        else:
            out = out.withColumn("address", col(corrected_address_col))

    failed_types = dict(df_failed.dtypes)
    for source_col in correction_cols:
        corr_col = f"_corr_{source_col}"
        if source_col == "corrected_address":
            continue
        if source_col in failed_types:
            out = out.withColumn(source_col, coalesce(col(corr_col).cast(failed_types[source_col]), col(source_col)))
        else:
            out = out.withColumn(source_col, col(corr_col))

    drop_cols = [c for c in out.columns if c.startswith("_corr_")]
    if drop_cols:
        out = out.drop(*drop_cols)
    return out


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Retry failed rows using record_id based corrections.")
    parser.add_argument("--failed-path", required=True, help="Failed parquet path")
    parser.add_argument("--corrections-csv", required=True, help="Corrections CSV path")
    parser.add_argument("--success-out", required=True, help="Retried success output parquet path")
    parser.add_argument("--failed-out", required=True, help="Retried failed output parquet path")
    parser.add_argument("--require-mukim", action="store_true", help="Fail rows missing mukim")
    parser.add_argument("--config", default=None, help="Optional pipeline config path")
    parser.add_argument(
        "--audit-log",
        default=os.getenv("NAS_AUDIT_LOG", "logs/nas_audit.log"),
        help="JSONL audit logfile path",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    started = time.time()
    run_id = start_audit_run(args.audit_log, "retry_failed_rows", vars(args))
    spark = None
    status = "ok"
    try:
        builder = SparkSession.builder.appName("NAS Retry Failed Rows")
        builder = configure_sedona_builder(builder)
        jars_packages = resolve_sedona_spark_packages()
        if jars_packages:
            builder = builder.config("spark.jars.packages", jars_packages)
        spark = builder.getOrCreate()

        df_failed = spark.read.parquet(args.failed_path)
        corrections = spark.read.csv(args.corrections_csv, header=True)

        df_retry = _apply_corrections(df_failed, corrections)

        drop_cols = [c for c in df_retry.columns if c in DERIVED_COLUMNS]
        if drop_cols:
            df_retry = df_retry.drop(*drop_cols)

        address_col = _detect_address_col(df_retry.columns)
        df_clean = clean_addresses(df_retry, address_col=address_col, config_path=args.config)
        df_success, df_failed_out = validate_addresses(df_clean, require_mukim=args.require_mukim)
        df_success = add_standard_naskod(df_success, output_col="naskod")
        df_failed_out = add_standard_naskod(df_failed_out, output_col="naskod")

        input_failed_count = df_failed.count()
        corrections_count = corrections.count()
        success_count = df_success.count()
        failed_count = df_failed_out.count()

        load_success_failed(df_success, df_failed_out, args.success_out, args.failed_out)

        audit_event(
            args.audit_log,
            "retry_failed_rows",
            run_id,
            "retry_complete",
            input_failed_count=input_failed_count,
            corrections_count=corrections_count,
            success_count=success_count,
            failed_count=failed_count,
            success_path=args.success_out,
            failed_path=args.failed_out,
        )
    except Exception as exc:
        status = "error"
        audit_event(
            args.audit_log,
            "retry_failed_rows",
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
            "retry_failed_rows",
            run_id,
            "run_end",
            status=status,
            duration_ms=int((time.time() - started) * 1000),
        )


if __name__ == "__main__":
    main()
