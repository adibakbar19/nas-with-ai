import argparse
import csv
import os
import time
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col,
    coalesce,
    current_timestamp,
    expr,
    lit,
    regexp_extract,
    row_number,
    trim,
    upper,
    when,
)
from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from domain.replacements import LOCALITY_INVALID_PREFIX_REGEX, LOCALITY_PLACEHOLDER_VALUES

from .audit_log import audit_event, start_audit_run
from .build_locality_lookup import build_locality_rows, build_sublocality_rows
from .load_postgres import (
    DEFAULT_PBT_DIR,
    _load_pbt_boundaries,
    _normalize_locality_col,
    _resolve_pbt_dir,
    _run_sql,
    _write_table,
)
from .sedona_utils import (
    configure_common_spark_builder,
    configure_sedona_builder,
    merge_spark_packages,
    quiet_spark_spatial_warnings,
    register_sedona,
    resolve_sedona_spark_packages,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
ENV_FILE = PROJECT_ROOT / ".env"
DEFAULT_STATE_BOUNDARY_DIR = os.path.join("data", "boundary", "01_State_boundary")
DEFAULT_POSTCODE_BOUNDARY_DIR = os.path.join("data", "boundary", "02_Postcode_boundary")
DEFAULT_DISTRICT_BOUNDARY_DIR = os.path.join("data", "boundary", "03_District_boundary")
DEFAULT_MUKIM_BOUNDARY_DIR = os.path.join("data", "boundary", "05_Mukim_boundary")


def _load_env_file(env_file: Path) -> None:
    if not env_file.exists():
        return
    for raw_line in env_file.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip("'").strip('"')
        if key:
            # Preserve explicitly injected env vars (for example Docker service overrides).
            os.environ.setdefault(key, value)


def _build_jdbc_url(args: argparse.Namespace) -> str:
    if args.jdbc_url:
        return args.jdbc_url
    host = args.host or os.getenv("PGHOST", "localhost")
    port = args.port or os.getenv("PGPORT", "5432")
    database = args.database or os.getenv("PGDATABASE", "postgres")
    return f"jdbc:postgresql://{host}:{port}/{database}"


def _escape_sql_literal(value: str) -> str:
    return value.replace("'", "''")


def _pick_existing_col(df, candidates: list[str]) -> str | None:
    cols = {c.lower(): c for c in df.columns}
    for cand in candidates:
        found = cols.get(cand.lower())
        if found:
            return found
    return None


def _rebuild_locality_lookup_csv(*, granite_root: str, lookups_dir: str, manual_path: str | None, output_path: str) -> None:
    rows = build_locality_rows(granite_root, lookups_dir, manual_path)
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    seen: set[tuple[str, str]] = set()
    with open(output_path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["state_name", "locality_name", "locality_norm", "source"])
        for state_name, locality_name, locality_norm, source in rows:
            key = (state_name, locality_norm)
            if key in seen:
                continue
            seen.add(key)
            writer.writerow([state_name, locality_name, locality_norm, source])


def _rebuild_sublocality_lookup_csv(*, granite_root: str, manual_path: str | None, output_path: str) -> None:
    rows = build_sublocality_rows(granite_root, manual_path)
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    seen: set[tuple[str, str]] = set()
    with open(output_path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["state_name", "sub_locality_name", "sub_locality_norm", "source"])
        for state_name, sub_locality_name, sub_locality_norm, source in rows:
            key = (state_name, sub_locality_norm)
            if key in seen:
                continue
            seen.add(key)
            writer.writerow([state_name, sub_locality_name, sub_locality_norm, source])


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Bootstrap NAS lookup tables "
            "(state/district/mukim/locality/sublocality/postcode/pbt + boundary layers) from master data."
        )
    )
    parser.add_argument("--lookups-dir", default="data/lookups", help="Directory containing lookup CSV files")
    parser.add_argument(
        "--granite-root",
        default="data/granite_map_info-master",
        help="granite_map_info-master root (for locality lookup rebuild)",
    )
    parser.add_argument(
        "--locality-lookup",
        default="data/lookups/locality_lookup.csv",
        help="Path to locality lookup CSV",
    )
    parser.add_argument(
        "--locality-manual",
        default="data/lookups/locality_manual.csv",
        help="Optional manual locality CSV used during locality lookup rebuild",
    )
    parser.add_argument(
        "--sublocality-lookup",
        default="data/lookups/sublocality_lookup.csv",
        help="Path to sub-locality lookup CSV (SECTION-level)",
    )
    parser.add_argument(
        "--sublocality-manual",
        default="data/lookups/sublocality_manual.csv",
        help="Optional manual sub-locality CSV used during sub-locality lookup rebuild",
    )
    parser.add_argument(
        "--rebuild-locality-lookup",
        action="store_true",
        help="Rebuild locality_lookup.csv from granite data before loading to DB",
    )
    parser.add_argument(
        "--rebuild-sublocality-lookup",
        action="store_true",
        help="Rebuild sublocality_lookup.csv (from SECTION data) before loading to DB",
    )
    parser.add_argument("--pbt-dir", default=DEFAULT_PBT_DIR, help="PBT shapefiles directory")
    parser.add_argument("--pbt-id-column", default="pbt_id", help="PBT ID column name in shapefile")
    parser.add_argument("--pbt-name-column", default="NAMA_PBT", help="PBT name column name in shapefile")
    parser.add_argument("--state-boundary-dir", default=DEFAULT_STATE_BOUNDARY_DIR, help="State boundary shapefiles directory")
    parser.add_argument(
        "--district-boundary-dir",
        default=DEFAULT_DISTRICT_BOUNDARY_DIR,
        help="District boundary shapefiles directory",
    )
    parser.add_argument("--mukim-boundary-dir", default=DEFAULT_MUKIM_BOUNDARY_DIR, help="Mukim boundary shapefiles directory")
    parser.add_argument(
        "--postcode-boundary-dir",
        default=DEFAULT_POSTCODE_BOUNDARY_DIR,
        help="Postcode boundary shapefiles directory",
    )
    parser.add_argument(
        "--postcode-boundary-postcode-column",
        default="POSTCODE",
        help="Postcode column name in postcode boundary shapefile",
    )
    parser.add_argument(
        "--postcode-boundary-city-column",
        default="TOWN_CITY",
        help="City column name in postcode boundary shapefile",
    )
    parser.add_argument(
        "--postcode-boundary-state-column",
        default="STATE",
        help="State column name in postcode boundary shapefile",
    )
    parser.add_argument("--schema", default=os.getenv("LOOKUP_SCHEMA", "nas_lookup"), help="Target schema for lookup tables")
    parser.add_argument("--jdbc-url", default=None, help="JDBC URL (overrides host/port/database)")
    parser.add_argument("--host", default=None, help="Postgres host (default: PGHOST or localhost)")
    parser.add_argument("--port", default=None, help="Postgres port (default: PGPORT or 5432)")
    parser.add_argument("--database", default=None, help="Postgres database (default: PGDATABASE or postgres)")
    parser.add_argument("--user", default=os.getenv("PGUSER"))
    parser.add_argument("--password", default=os.getenv("PGPASSWORD"))
    parser.add_argument(
        "--version",
        default=None,
        help="Lookup version token written to lookup_version table (default: UTC timestamp)",
    )
    parser.add_argument("--repartition", type=int, default=1, help="Repartition before JDBC write")
    parser.add_argument(
        "--audit-log",
        default=os.getenv("NAS_AUDIT_LOG", "logs/nas_audit.log"),
        help="JSONL audit logfile path",
    )
    return parser.parse_args()


def _build_lookup_tables(
    spark: SparkSession,
    *,
    lookups_dir: str,
    locality_lookup_path: str,
    sublocality_lookup_path: str,
    pbt_boundaries,
    state_boundaries,
    district_boundaries,
    mukim_boundaries,
    postcode_boundaries,
    pbt_id_column: str,
    pbt_name_column: str,
    postcode_boundary_postcode_column: str,
    postcode_boundary_city_column: str,
    postcode_boundary_state_column: str,
) -> dict[str, object]:
    state_raw = spark.read.csv(os.path.join(lookups_dir, "state_codes.csv"), header=True)
    district_raw = spark.read.csv(os.path.join(lookups_dir, "district_codes.csv"), header=True)
    mukim_raw = spark.read.csv(os.path.join(lookups_dir, "mukim_codes.csv"), header=True)
    postcode_raw = spark.read.csv(os.path.join(lookups_dir, "postcodes.csv"), header=True)
    locality_raw = spark.read.csv(locality_lookup_path, header=True)
    sublocality_raw = spark.read.csv(sublocality_lookup_path, header=True)

    state_table = (
        state_raw.select(
            trim(col("state_code")).alias("state_code"),
            upper(trim(col("state_name"))).alias("state_name"),
        )
        .filter(col("state_code").isNotNull() & col("state_name").isNotNull())
        .dropDuplicates(["state_code"])
    )
    state_table = state_table.withColumn(
        "state_id",
        row_number().over(Window.orderBy(col("state_code").asc_nulls_last())),
    ).select("state_id", "state_name", "state_code")

    district_table = (
        district_raw.select(
            trim(col("state_code")).alias("state_code"),
            trim(col("district_code")).alias("district_code"),
            upper(trim(col("district_name"))).alias("district_name"),
        )
        .filter(col("state_code").isNotNull() & col("district_code").isNotNull())
        .dropDuplicates(["state_code", "district_code"])
        .join(state_table.select("state_id", "state_code"), "state_code", "left")
    )
    district_table = district_table.withColumn(
        "district_id",
        row_number().over(Window.orderBy(col("state_code").asc_nulls_last(), col("district_code").asc_nulls_last())),
    ).select("district_id", "district_name", "state_id", "district_code", "state_code")

    mukim_with_ids = (
        mukim_raw.select(
            trim(col("state_code")).alias("state_code"),
            upper(trim(col("state_name"))).alias("state_name"),
            trim(col("district_code")).alias("district_code"),
            upper(trim(col("district_name"))).alias("district_name"),
            trim(col("mukim_code")).alias("mukim_code"),
            upper(trim(col("mukim_name"))).alias("mukim_name"),
            when(col("mukim_id").rlike(r"^\d+$"), col("mukim_id").cast("int")).otherwise(lit(None).cast("int")).alias(
                "mukim_id"
            ),
        )
        .filter(col("state_code").isNotNull() & col("district_code").isNotNull() & col("mukim_name").isNotNull())
        .dropDuplicates(["state_code", "district_code", "mukim_code", "mukim_name"])
        .join(district_table.select("district_id", "district_code", "state_code"), ["district_code", "state_code"], "left")
    )
    mukim_with_ids = mukim_with_ids.withColumn(
        "_mukim_seq",
        row_number().over(
            Window.orderBy(
                col("state_code").asc_nulls_last(),
                col("district_code").asc_nulls_last(),
                col("mukim_code").asc_nulls_last(),
                col("mukim_name").asc_nulls_last(),
            )
        ),
    )
    mukim_with_ids = mukim_with_ids.withColumn("mukim_id", when(col("mukim_id").isNotNull(), col("mukim_id")).otherwise(col("_mukim_seq")))
    mukim_table = mukim_with_ids.select(
        col("mukim_id").cast("int"),
        col("mukim_name"),
        col("district_id").cast("int"),
        col("mukim_code"),
        col("district_code"),
        col("state_code"),
    ).dropDuplicates(["mukim_id"])

    locality_base = (
        locality_raw.select(
            upper(trim(col("state_name"))).alias("state_name"),
            _normalize_locality_col(col("locality_name")).alias("locality_name"),
        )
        .filter(
            col("state_name").isNotNull()
            & col("locality_name").isNotNull()
            & (col("locality_name") != lit(""))
            & col("locality_name").rlike("[A-Z]")
            & (~col("locality_name").rlike(r"^\\d+[A-Z]?$"))
            & (~col("locality_name").isin(*LOCALITY_PLACEHOLDER_VALUES))
            & (~col("locality_name").rlike(LOCALITY_INVALID_PREFIX_REGEX))
        )
        .dropDuplicates(["state_name", "locality_name"])
        .join(state_table.select("state_id", "state_name"), "state_name", "left")
        .filter(col("state_id").isNotNull())
    )
    mukim_state_lookup = (
        mukim_table.join(district_table.select("district_id", "state_id"), "district_id", "left")
        .select(col("mukim_id"), col("state_id"), upper(col("mukim_name")).alias("_mukim_name"))
    )
    locality_with_mukim = locality_base.join(
        mukim_state_lookup,
        (locality_base.state_id == mukim_state_lookup.state_id)
        & (locality_base.locality_name == mukim_state_lookup._mukim_name),
        "left",
    ).drop("_mukim_name")
    locality_table = locality_with_mukim.select("locality_name", "mukim_id").dropDuplicates(["locality_name", "mukim_id"])
    locality_table = locality_table.withColumn(
        "locality_id",
        row_number().over(Window.orderBy(col("locality_name").asc_nulls_last(), col("mukim_id").asc_nulls_last())),
    )
    locality_table = locality_table.withColumn("locality_code", lit(None).cast("string"))
    locality_table = locality_table.withColumn("created_at", current_timestamp())
    locality_table = locality_table.withColumn("updated_at", current_timestamp())

    sublocality_table = (
        sublocality_raw.select(
            upper(trim(col("state_name"))).alias("state_name"),
            _normalize_locality_col(col("sub_locality_name")).alias("sublocality_name"),
        )
        .filter(
            col("state_name").isNotNull()
            & col("sublocality_name").isNotNull()
            & (col("sublocality_name") != lit(""))
            & col("sublocality_name").rlike("[A-Z]")
            & (~col("sublocality_name").rlike(r"^\\d+[A-Z]?$"))
            & (~col("sublocality_name").isin(*LOCALITY_PLACEHOLDER_VALUES))
            & (~col("sublocality_name").rlike(LOCALITY_INVALID_PREFIX_REGEX))
        )
        .dropDuplicates(["state_name", "sublocality_name"])
        .join(state_table.select("state_id", "state_name"), "state_name", "left")
        .filter(col("state_id").isNotNull())
    )
    sublocality_table = sublocality_table.withColumn(
        "sublocality_id",
        row_number().over(Window.orderBy(col("state_name").asc_nulls_last(), col("sublocality_name").asc_nulls_last())),
    )
    sublocality_table = sublocality_table.withColumn("sublocality_code", lit(None).cast("string"))
    sublocality_table = sublocality_table.withColumn("created_at", current_timestamp())
    sublocality_table = sublocality_table.withColumn("updated_at", current_timestamp())

    locality_with_state = (
        locality_table.join(mukim_table.select("mukim_id", "district_id"), "mukim_id", "left")
        .join(district_table.select("district_id", "state_id"), "district_id", "left")
        .join(state_table.select("state_id", "state_name"), "state_id", "left")
        .select(col("locality_id"), col("locality_name"), col("state_name").alias("_locality_state_name"))
    )
    postcode_table = (
        postcode_raw.select(
            regexp_extract(trim(col("postcode").cast("string")), r"(\d{5})", 1).alias("postcode"),
            upper(trim(col("city"))).alias("postcode_name"),
            upper(trim(col("state"))).alias("_postcode_state_name"),
        )
        .filter(col("postcode").isNotNull() & (col("postcode") != lit("")))
        .dropDuplicates(["postcode"])
        .join(
            locality_with_state,
            (col("postcode_name") == col("locality_name"))
            & (col("_postcode_state_name") == col("_locality_state_name")),
            "left",
        )
        .drop("_locality_state_name", "_postcode_state_name")
    )
    postcode_table = postcode_table.withColumn(
        "postcode_id",
        row_number().over(Window.orderBy(col("postcode").asc_nulls_last())),
    ).select("postcode_id", "postcode_name", "locality_id", "postcode")

    state_boundary_schema = StructType(
        [
            StructField("state_code", StringType(), True),
            StructField("state_name", StringType(), True),
            StructField("boundary_geom", StringType(), True),
        ]
    )
    if state_boundaries is not None:
        state_name_col = _pick_existing_col(state_boundaries, ["state_name", "state", "negeri"])
        if not state_name_col:
            raise ValueError(
                "State boundary shapefile missing required state-name column. "
                f"Found: {sorted(state_boundaries.columns)}"
            )
        state_code_col = _pick_existing_col(state_boundaries, ["state_code", "statecode", "state_c"])
        state_boundary_table = state_boundaries.select(
            trim(col(state_code_col).cast("string")).alias("state_code")
            if state_code_col
            else lit(None).cast("string").alias("state_code"),
            upper(trim(col(state_name_col).cast("string"))).alias("state_name"),
            expr("ST_AsText(geom)").alias("boundary_geom"),
        )
        state_lookup = state_table.select(
            col("state_code").alias("_lk_state_code"),
            upper(col("state_name")).alias("_lk_state_name"),
        )
        state_boundary_table = state_boundary_table.join(
            state_lookup,
            state_boundary_table.state_name == col("_lk_state_name"),
            "left",
        )
        state_boundary_table = state_boundary_table.withColumn(
            "state_code",
            coalesce(col("state_code"), col("_lk_state_code")),
        ).drop("_lk_state_code", "_lk_state_name")
    else:
        state_boundary_table = spark.createDataFrame([], schema=state_boundary_schema)
    state_boundary_table = state_boundary_table.filter(
        col("state_name").isNotNull() & col("boundary_geom").isNotNull()
    ).dropDuplicates(["state_code", "state_name"])

    district_boundary_schema = StructType(
        [
            StructField("state_code", StringType(), True),
            StructField("district_code", StringType(), True),
            StructField("district_name", StringType(), True),
            StructField("boundary_geom", StringType(), True),
        ]
    )
    if district_boundaries is not None:
        district_name_col = _pick_existing_col(district_boundaries, ["district_name", "district", "daerah"])
        if not district_name_col:
            raise ValueError(
                "District boundary shapefile missing required district-name column. "
                f"Found: {sorted(district_boundaries.columns)}"
            )
        district_code_col = _pick_existing_col(district_boundaries, ["district_code", "district_c", "districtcode"])
        district_state_code_col = _pick_existing_col(district_boundaries, ["state_code", "statecode", "state_c"])
        district_boundary_table = district_boundaries.select(
            trim(col(district_state_code_col).cast("string")).alias("state_code")
            if district_state_code_col
            else lit(None).cast("string").alias("state_code"),
            trim(col(district_code_col).cast("string")).alias("district_code")
            if district_code_col
            else lit(None).cast("string").alias("district_code"),
            upper(trim(col(district_name_col).cast("string"))).alias("district_name"),
            expr("ST_AsText(geom)").alias("boundary_geom"),
        )
        district_lookup = district_table.select(
            col("district_code").alias("_lk_district_code"),
            col("state_code").alias("_lk_state_code"),
            upper(col("district_name")).alias("_lk_district_name"),
        )
        district_boundary_table = district_boundary_table.join(
            district_lookup,
            district_boundary_table.district_code == col("_lk_district_code"),
            "left",
        )
        district_boundary_table = district_boundary_table.withColumn(
            "state_code",
            coalesce(col("state_code"), col("_lk_state_code")),
        ).withColumn(
            "district_name",
            coalesce(col("district_name"), col("_lk_district_name")),
        ).drop("_lk_district_code", "_lk_state_code", "_lk_district_name")
    else:
        district_boundary_table = spark.createDataFrame([], schema=district_boundary_schema)
    district_boundary_table = district_boundary_table.filter(
        col("district_name").isNotNull() & col("boundary_geom").isNotNull()
    ).dropDuplicates(["state_code", "district_code", "district_name"])

    mukim_boundary_schema = StructType(
        [
            StructField("state_code", StringType(), True),
            StructField("district_code", StringType(), True),
            StructField("district_name", StringType(), True),
            StructField("mukim_code", StringType(), True),
            StructField("mukim_name", StringType(), True),
            StructField("mukim_id", IntegerType(), True),
            StructField("boundary_geom", StringType(), True),
        ]
    )
    if mukim_boundaries is not None:
        mukim_name_col = _pick_existing_col(mukim_boundaries, ["mukim_name", "mukim"])
        if not mukim_name_col:
            raise ValueError(
                "Mukim boundary shapefile missing required mukim-name column. "
                f"Found: {sorted(mukim_boundaries.columns)}"
            )
        mukim_code_col = _pick_existing_col(mukim_boundaries, ["mukim_code", "mukimcode"])
        mukim_id_col = _pick_existing_col(mukim_boundaries, ["mukim_id", "mukimid", "id"])
        mukim_district_code_col = _pick_existing_col(mukim_boundaries, ["district_code", "district_c", "districtcode"])
        mukim_district_name_col = _pick_existing_col(mukim_boundaries, ["district_name", "district", "daerah"])
        mukim_state_code_col = _pick_existing_col(mukim_boundaries, ["state_code", "statecode", "state_c"])
        mukim_boundary_table = mukim_boundaries.select(
            trim(col(mukim_state_code_col).cast("string")).alias("state_code")
            if mukim_state_code_col
            else lit(None).cast("string").alias("state_code"),
            trim(col(mukim_district_code_col).cast("string")).alias("district_code")
            if mukim_district_code_col
            else lit(None).cast("string").alias("district_code"),
            upper(trim(col(mukim_district_name_col).cast("string"))).alias("district_name")
            if mukim_district_name_col
            else lit(None).cast("string").alias("district_name"),
            trim(col(mukim_code_col).cast("string")).alias("mukim_code")
            if mukim_code_col
            else lit(None).cast("string").alias("mukim_code"),
            upper(trim(col(mukim_name_col).cast("string"))).alias("mukim_name"),
            when(col(mukim_id_col).cast("string").rlike(r"^\d+$"), col(mukim_id_col).cast("int")).otherwise(
                lit(None).cast("int")
            ).alias("mukim_id")
            if mukim_id_col
            else lit(None).cast("int").alias("mukim_id"),
            expr("ST_AsText(geom)").alias("boundary_geom"),
        )
        mukim_lookup = mukim_table.select(
            col("mukim_code").alias("_lk_mukim_code"),
            col("mukim_id").cast("int").alias("_lk_mukim_id"),
            col("district_code").alias("_lk_district_code"),
            col("state_code").alias("_lk_state_code"),
            upper(col("mukim_name")).alias("_lk_mukim_name"),
        )
        mukim_boundary_table = mukim_boundary_table.join(
            mukim_lookup,
            (
                (mukim_boundary_table.mukim_code.isNotNull() & (mukim_boundary_table.mukim_code == col("_lk_mukim_code")))
                | (mukim_boundary_table.mukim_id.isNotNull() & (mukim_boundary_table.mukim_id == col("_lk_mukim_id")))
            ),
            "left",
        )
        mukim_boundary_table = mukim_boundary_table.withColumn(
            "state_code",
            coalesce(col("state_code"), col("_lk_state_code")),
        ).withColumn(
            "district_code",
            coalesce(col("district_code"), col("_lk_district_code")),
        ).withColumn(
            "mukim_name",
            coalesce(col("mukim_name"), col("_lk_mukim_name")),
        ).withColumn(
            "mukim_id",
            coalesce(col("mukim_id"), col("_lk_mukim_id")),
        ).drop("_lk_mukim_code", "_lk_mukim_id", "_lk_district_code", "_lk_state_code", "_lk_mukim_name")
    else:
        mukim_boundary_table = spark.createDataFrame([], schema=mukim_boundary_schema)
    mukim_boundary_table = mukim_boundary_table.filter(
        col("mukim_name").isNotNull() & col("boundary_geom").isNotNull()
    ).dropDuplicates(["state_code", "district_code", "mukim_code", "mukim_name", "mukim_id"])

    postcode_boundary_schema = StructType(
        [
            StructField("postcode", StringType(), True),
            StructField("city", StringType(), True),
            StructField("state", StringType(), True),
            StructField("boundary_geom", StringType(), True),
        ]
    )
    if postcode_boundaries is not None:
        postcode_col = _pick_existing_col(postcode_boundaries, [postcode_boundary_postcode_column, "postcode"])
        postcode_city_col = _pick_existing_col(postcode_boundaries, [postcode_boundary_city_column, "city", "town_city"])
        postcode_state_col = _pick_existing_col(postcode_boundaries, [postcode_boundary_state_column, "state"])
        if not postcode_col or not postcode_city_col or not postcode_state_col:
            raise ValueError(
                "Postcode boundary shapefile missing required columns. "
                f"Found: {sorted(postcode_boundaries.columns)}"
            )
        postcode_boundary_table = postcode_boundaries.select(
            regexp_extract(trim(col(postcode_col).cast("string")), r"(\d{5})", 1).alias("postcode"),
            upper(trim(col(postcode_city_col).cast("string"))).alias("city"),
            upper(trim(col(postcode_state_col).cast("string"))).alias("state"),
            expr("ST_AsText(geom)").alias("boundary_geom"),
        )
    else:
        postcode_boundary_table = spark.createDataFrame([], schema=postcode_boundary_schema)
    postcode_boundary_table = postcode_boundary_table.filter(
        col("postcode").isNotNull() & (col("postcode") != lit("")) & col("boundary_geom").isNotNull()
    ).dropDuplicates(["postcode", "city", "state"])

    if pbt_boundaries is not None:
        pbt_cols = {c.lower(): c for c in pbt_boundaries.columns}
        pbt_id_col = pbt_cols.get(pbt_id_column.lower())
        pbt_name_col = pbt_cols.get(pbt_name_column.lower())
        if not pbt_id_col or not pbt_name_col:
            raise ValueError(
                "PBT shapefile missing required columns. "
                f"Found: {sorted(pbt_boundaries.columns)}"
            )
        pbt_table = pbt_boundaries.select(
            col(pbt_id_col).alias("_pbt_id"),
            upper(trim(col(pbt_name_col))).alias("pbt_name"),
            expr("ST_AsText(geom)").alias("boundary_geom"),
        )
        pbt_table = pbt_table.withColumn(
            "pbt_id",
            when(col("_pbt_id").rlike(r"^\d+$"), col("_pbt_id").cast("int")).otherwise(lit(None).cast("int")),
        ).drop("_pbt_id")
    else:
        pbt_schema = StructType(
            [
                StructField("pbt_id", IntegerType(), True),
                StructField("pbt_name", StringType(), True),
                StructField("boundary_geom", StringType(), True),
            ]
        )
        pbt_table = spark.createDataFrame([], schema=pbt_schema)
    pbt_table = pbt_table.dropDuplicates(["pbt_id", "pbt_name"])
    pbt_table = pbt_table.withColumn("is_active", lit(True).cast(BooleanType()))
    pbt_table = pbt_table.withColumn("created_at", current_timestamp().cast(TimestampType()))
    pbt_table = pbt_table.withColumn("updated_at", current_timestamp().cast(TimestampType()))

    return {
        "state": state_table.select("state_id", "state_name", "state_code"),
        "district": district_table.select("district_id", "district_name", "state_id", "district_code"),
        "mukim": mukim_table.select("mukim_id", "mukim_name", "district_id", "mukim_code"),
        "locality": locality_table.select(
            "locality_id",
            "locality_code",
            "locality_name",
            "mukim_id",
            "created_at",
            "updated_at",
        ),
        "sublocality": sublocality_table.select(
            col("sublocality_id").cast("int"),
            "sublocality_code",
            "sublocality_name",
            col("state_id").cast("int"),
            "created_at",
            "updated_at",
        ),
        "postcode": postcode_table,
        "pbt": pbt_table.select(
            "pbt_id",
            "pbt_name",
            "boundary_geom",
            "is_active",
            "created_at",
            "updated_at",
        ),
        "state_boundary": state_boundary_table.select("state_code", "state_name", "boundary_geom"),
        "district_boundary": district_boundary_table.select("state_code", "district_code", "district_name", "boundary_geom"),
        "mukim_boundary": mukim_boundary_table.select(
            "state_code",
            "district_code",
            "district_name",
            "mukim_code",
            "mukim_name",
            "mukim_id",
            "boundary_geom",
        ),
        "postcode_boundary": postcode_boundary_table.select("postcode", "city", "state", "boundary_geom"),
    }


def main() -> None:
    _load_env_file(ENV_FILE)
    args = parse_args()
    if not args.user or not args.password:
        raise ValueError("PGUSER and PGPASSWORD must be provided via .env or CLI (--user/--password).")
    if args.rebuild_locality_lookup:
        _rebuild_locality_lookup_csv(
            granite_root=args.granite_root,
            lookups_dir=args.lookups_dir,
            manual_path=args.locality_manual,
            output_path=args.locality_lookup,
        )
    if args.rebuild_locality_lookup or args.rebuild_sublocality_lookup:
        _rebuild_sublocality_lookup_csv(
            granite_root=args.granite_root,
            manual_path=args.sublocality_manual,
            output_path=args.sublocality_lookup,
        )
    if not os.path.exists(args.locality_lookup):
        raise FileNotFoundError(
            f"Locality lookup CSV not found: {args.locality_lookup}. "
            "Provide --locality-lookup or run with --rebuild-locality-lookup."
        )
    if not os.path.exists(args.sublocality_lookup):
        raise FileNotFoundError(
            f"Sublocality lookup CSV not found: {args.sublocality_lookup}. "
            "Provide --sublocality-lookup or run with --rebuild-sublocality-lookup."
        )

    run_id = start_audit_run(args.audit_log, "bootstrap_lookups", vars(args))
    started = time.time()
    status = "ok"
    spark = None
    try:
        builder = SparkSession.builder.appName("NAS Bootstrap Lookups")
        builder = configure_common_spark_builder(builder)
        builder = configure_sedona_builder(builder)
        jars_packages = resolve_sedona_spark_packages()
        jars_packages = merge_spark_packages(jars_packages, "org.postgresql:postgresql:42.7.3")
        if jars_packages:
            builder = builder.config("spark.jars.packages", jars_packages)
        spark = builder.getOrCreate()
        quiet_spark_spatial_warnings(spark)

        jdbc_url = _build_jdbc_url(args)
        props = {
            "user": args.user,
            "password": args.password,
            "driver": "org.postgresql.Driver",
        }
        schema = (args.schema or "").strip()
        schema_for_check = schema or "public"
        schema_qual = f"{schema}." if schema else ""
        if schema:
            _run_sql(
                spark,
                jdbc_url,
                args.user,
                args.password,
                [f"CREATE SCHEMA IF NOT EXISTS {schema};"],
            )

        pbt_boundaries = None
        state_boundaries = None
        district_boundaries = None
        mukim_boundaries = None
        postcode_boundaries = None
        resolved_pbt_dir = _resolve_pbt_dir(args.pbt_dir)

        boundary_dirs = {
            "state_boundary": args.state_boundary_dir,
            "district_boundary": args.district_boundary_dir,
            "mukim_boundary": args.mukim_boundary_dir,
            "postcode_boundary": args.postcode_boundary_dir,
        }
        resolved_boundary_dirs: dict[str, str | None] = {}
        for key, requested in boundary_dirs.items():
            if requested and os.path.exists(requested):
                resolved_boundary_dirs[key] = requested
            else:
                resolved_boundary_dirs[key] = None
                audit_event(
                    args.audit_log,
                    "bootstrap_lookups",
                    run_id,
                    f"{key}_warning",
                    warning=f"{key} directory not found; table will be empty",
                    requested_path=requested,
                )

        if resolved_pbt_dir or any(resolved_boundary_dirs.values()):
            register_sedona(spark)
        if resolved_pbt_dir:
            pbt_boundaries = _load_pbt_boundaries(spark, resolved_pbt_dir)
        else:
            audit_event(
                args.audit_log,
                "bootstrap_lookups",
                run_id,
                "pbt_boundary_warning",
                warning="PBT boundary directory not found; pbt table will be empty",
                requested_pbt_dir=args.pbt_dir,
            )
        if resolved_boundary_dirs["state_boundary"]:
            state_boundaries = _load_pbt_boundaries(spark, resolved_boundary_dirs["state_boundary"])
        if resolved_boundary_dirs["district_boundary"]:
            district_boundaries = _load_pbt_boundaries(spark, resolved_boundary_dirs["district_boundary"])
        if resolved_boundary_dirs["mukim_boundary"]:
            mukim_boundaries = _load_pbt_boundaries(spark, resolved_boundary_dirs["mukim_boundary"])
        if resolved_boundary_dirs["postcode_boundary"]:
            postcode_boundaries = _load_pbt_boundaries(spark, resolved_boundary_dirs["postcode_boundary"])

        tables = _build_lookup_tables(
            spark,
            lookups_dir=args.lookups_dir,
            locality_lookup_path=args.locality_lookup,
            sublocality_lookup_path=args.sublocality_lookup,
            pbt_boundaries=pbt_boundaries,
            state_boundaries=state_boundaries,
            district_boundaries=district_boundaries,
            mukim_boundaries=mukim_boundaries,
            postcode_boundaries=postcode_boundaries,
            pbt_id_column=args.pbt_id_column,
            pbt_name_column=args.pbt_name_column,
            postcode_boundary_postcode_column=args.postcode_boundary_postcode_column,
            postcode_boundary_city_column=args.postcode_boundary_city_column,
            postcode_boundary_state_column=args.postcode_boundary_state_column,
        )

        dim_mode = "overwrite"
        _write_table(tables["state"], jdbc_url=jdbc_url, table=f"{schema_qual}state", mode=dim_mode, props=props, repartition=args.repartition)
        _write_table(
            tables["district"],
            jdbc_url=jdbc_url,
            table=f"{schema_qual}district",
            mode=dim_mode,
            props=props,
            repartition=args.repartition,
        )
        _write_table(
            tables["mukim"],
            jdbc_url=jdbc_url,
            table=f"{schema_qual}mukim",
            mode=dim_mode,
            props=props,
            repartition=args.repartition,
        )
        _write_table(
            tables["locality"],
            jdbc_url=jdbc_url,
            table=f"{schema_qual}locality",
            mode=dim_mode,
            props=props,
            repartition=args.repartition,
        )
        _write_table(
            tables["sublocality"],
            jdbc_url=jdbc_url,
            table=f"{schema_qual}sublocality",
            mode=dim_mode,
            props=props,
            repartition=args.repartition,
        )
        _write_table(
            tables["postcode"],
            jdbc_url=jdbc_url,
            table=f"{schema_qual}postcode",
            mode=dim_mode,
            props=props,
            repartition=args.repartition,
        )
        _write_table(
            tables["pbt"],
            jdbc_url=jdbc_url,
            table=f"{schema_qual}pbt",
            mode=dim_mode,
            props=props,
            repartition=args.repartition,
        )
        _write_table(
            tables["state_boundary"],
            jdbc_url=jdbc_url,
            table=f"{schema_qual}state_boundary",
            mode=dim_mode,
            props=props,
            repartition=args.repartition,
        )
        _write_table(
            tables["district_boundary"],
            jdbc_url=jdbc_url,
            table=f"{schema_qual}district_boundary",
            mode=dim_mode,
            props=props,
            repartition=args.repartition,
        )
        _write_table(
            tables["mukim_boundary"],
            jdbc_url=jdbc_url,
            table=f"{schema_qual}mukim_boundary",
            mode=dim_mode,
            props=props,
            repartition=args.repartition,
        )
        _write_table(
            tables["postcode_boundary"],
            jdbc_url=jdbc_url,
            table=f"{schema_qual}postcode_boundary",
            mode=dim_mode,
            props=props,
            repartition=args.repartition,
        )

        boundary_geom_tables = [
            ("pbt", "pbt_boundary_geom_idx"),
            ("state_boundary", "state_boundary_geom_idx"),
            ("district_boundary", "district_boundary_geom_idx"),
            ("mukim_boundary", "mukim_boundary_geom_idx"),
            ("postcode_boundary", "postcode_boundary_geom_idx"),
        ]
        boundary_sql: list[str] = []
        for table_name, index_name in boundary_geom_tables:
            boundary_sql.append(
                f"""
                DO $$
                BEGIN
                  IF EXISTS (
                    SELECT 1
                    FROM information_schema.columns
                    WHERE table_name = '{table_name}'
                      AND table_schema = '{schema_for_check}'
                      AND column_name = 'boundary_geom'
                      AND (udt_name IS DISTINCT FROM 'geometry')
                  ) THEN
                    ALTER TABLE {schema_qual}{table_name}
                      ALTER COLUMN boundary_geom
                      TYPE geometry(Geometry, 4326)
                      USING ST_Force2D(ST_GeomFromText(boundary_geom, 4326));
                  END IF;
                END$$;
                """
            )
            boundary_sql.append(
                f"CREATE INDEX IF NOT EXISTS {index_name} ON {schema_qual}{table_name} USING GIST (boundary_geom);"
            )
        _run_sql(
            spark,
            jdbc_url,
            args.user,
            args.password,
            boundary_sql
            + [
                f"""
                CREATE TABLE IF NOT EXISTS {schema_qual}lookup_version (
                  lookup_key text PRIMARY KEY,
                  version text NOT NULL,
                  row_count bigint NOT NULL,
                  updated_at timestamptz NOT NULL DEFAULT now()
                );
                """,
                f"DELETE FROM {schema_qual}lookup_version;",
            ],
        )

        version = args.version or datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
        counts = {name: frame.count() for name, frame in tables.items()}
        for lookup_key, row_count in counts.items():
            key_lit = _escape_sql_literal(lookup_key)
            version_lit = _escape_sql_literal(version)
            _run_sql(
                spark,
                jdbc_url,
                args.user,
                args.password,
                [
                    f"""
                    INSERT INTO {schema_qual}lookup_version (lookup_key, version, row_count, updated_at)
                    VALUES ('{key_lit}', '{version_lit}', {int(row_count)}, now());
                    """
                ],
            )

        audit_event(
            args.audit_log,
            "bootstrap_lookups",
            run_id,
            "bootstrap_complete",
            schema=schema_for_check,
            version=version,
            resolved_pbt_dir=resolved_pbt_dir,
            resolved_boundary_dirs=resolved_boundary_dirs,
            counts=counts,
        )
        print(f"Lookup bootstrap complete. version={version} counts={counts}")
    except Exception as exc:
        status = "error"
        audit_event(
            args.audit_log,
            "bootstrap_lookups",
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
            "bootstrap_lookups",
            run_id,
            "run_end",
            status=status,
            duration_ms=int((time.time() - started) * 1000),
        )


if __name__ == "__main__":
    main()
