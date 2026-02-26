import argparse
import os
import time
from typing import Optional

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, coalesce, regexp_replace, trim, upper

from audit_log import audit_event, start_audit_run
from load import load_success_failed
from naskod_utils import add_standard_naskod
from sedona_utils import configure_sedona_builder
from transform import clean_addresses, validate_addresses


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
    "_dup_rank",
    "_address_norm",
    "seg2_norm",
    "seg3_norm",
    "seg4_norm",
    "seg5_norm",
}


def _normalize_key(c):
    c = upper(trim(c.cast("string")))
    c = regexp_replace(c, r"\s+", " ")
    return c


def _detect_col(df, candidates: list[str]) -> Optional[str]:
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
    parser.add_argument("--failed-out", required=True, help="Failed output parquet path")
    parser.add_argument("--require-mukim", action="store_true", help="Fail rows missing mukim")
    parser.add_argument("--source-col", default=None, help="Source address column in failed data")
    parser.add_argument("--correction-col", default=None, help="Corrected address column in corrections CSV")
    parser.add_argument(
        "--audit-log",
        default=os.getenv("NAS_AUDIT_LOG", "logs/nas_audit.log"),
        help="JSONL audit logfile path",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    started = time.time()
    run_id = start_audit_run(args.audit_log, "retry_failed", vars(args))
    spark = None
    status = "ok"
    try:
        builder = SparkSession.builder.appName("NAS Retry Failed")
        builder = configure_sedona_builder(builder)
        jars_packages = os.getenv("SPARK_JARS_PACKAGES")
        if jars_packages:
            builder = builder.config("spark.jars.packages", jars_packages)
        spark = builder.getOrCreate()

        df_failed = spark.read.parquet(args.failed)
        corrections = spark.read.csv(args.corrections, header=True)

        source_col = args.source_col or _detect_col(
            df_failed,
            ["source_address", "address", "full_address", "alamat", "raw_address", "address_full"],
        )
        if not source_col:
            raise ValueError("Could not detect source address column in failed dataset.")

        corr_col = args.correction_col or _detect_col(
            corrections,
            ["corrected_address", "address_fixed", "address", "full_address", "alamat"],
        )
        if not corr_col:
            raise ValueError("Could not detect corrected address column in corrections CSV.")

        corr_source_col = _detect_col(
            corrections,
            ["source_address", "raw_address", "address_original", "original_address", "address_source"],
        )
        if not corr_source_col:
            if source_col in corrections.columns:
                corr_source_col = source_col
            else:
                raise ValueError(
                    "Corrections CSV must include a source address column (e.g., source_address)."
                )

        df_failed = df_failed.withColumn("_src_key", _normalize_key(col(source_col)))
        corrections = corrections.withColumn("_src_key", _normalize_key(col(corr_source_col)))

        df_joined = df_failed.join(
            corrections.select(col("_src_key"), col(corr_col).alias("_corrected_address")),
            on="_src_key",
            how="left",
        )

        df_retry = (
            df_joined
            .withColumn("source_address_original", col(source_col))
            .withColumn("address", coalesce(col("_corrected_address"), col(source_col)))
            .drop("_src_key", "_corrected_address")
        )

        drop_cols = [c for c in df_retry.columns if c in DERIVED_COLUMNS]
        if drop_cols:
            df_retry = df_retry.drop(*drop_cols)

        df_clean = clean_addresses(df_retry, address_col="address")
        df_success, df_failed_out = validate_addresses(df_clean, require_mukim=args.require_mukim)
        df_success = add_standard_naskod(df_success, output_col="naskod")
        df_failed_out = add_standard_naskod(df_failed_out, output_col="naskod")

        input_failed_count = df_failed.count()
        corrections_count = corrections.count()
        success_count = df_success.count()
        failed_count = df_failed_out.count()
        audit_event(
            args.audit_log,
            "retry_failed",
            run_id,
            "retry_complete",
            input_failed_count=input_failed_count,
            corrections_count=corrections_count,
            success_count=success_count,
            failed_count=failed_count,
            success_path=args.success,
            failed_path=args.failed_out,
        )

        load_success_failed(df_success, df_failed_out, args.success, args.failed_out)
    except Exception as exc:
        status = "error"
        audit_event(
            args.audit_log,
            "retry_failed",
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
            "retry_failed",
            run_id,
            "run_end",
            status=status,
            duration_ms=int((time.time() - started) * 1000),
        )


if __name__ == "__main__":
    main()
