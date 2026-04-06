import argparse
import os
import time
from glob import glob

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col,
    coalesce,
    concat,
    concat_ws,
    current_timestamp,
    expr,
    regexp_extract,
    regexp_replace,
    lit,
    lpad,
    max as spark_max,
    monotonically_increasing_id,
    isnan,
    row_number,
    sha2,
    split,
    trim,
    upper,
    when,
    create_map,
)

from domain.replacements import (
    LOCALITY_ABBREVIATION_REPLACEMENTS,
    LOCALITY_INVALID_PREFIX_REGEX,
    LOCALITY_PLACEHOLDER_VALUES,
)

from .audit_log import audit_event, start_audit_run
from .sedona_utils import (
    configure_common_spark_builder,
    configure_sedona_builder,
    merge_spark_packages,
    quiet_spark_spatial_warnings,
    resolve_sedona_spark_packages,
)

DEFAULT_PBT_DIR = os.path.join("data", "boundary", "Sempadan Kawalan PBT")
LEGACY_PBT_DIR = os.path.join("data", "Sempadan Kawalan PBT")


def _escape_sql_literal(value: str) -> str:
    return value.replace("'", "''")


def _quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _build_jdbc_url(args: argparse.Namespace) -> str:
    if args.jdbc_url:
        return args.jdbc_url
    host = args.host or os.getenv("PGHOST", "localhost")
    port = args.port or os.getenv("PGPORT", "5432")
    db = args.database or os.getenv("PGDATABASE", "postgres")
    return f"jdbc:postgresql://{host}:{port}/{db}"


def _normalize_locality_col(value_col):
    out = upper(trim(value_col))
    out = regexp_replace(out, r"\bR\s*&\s*R\b", "R&R")
    out = regexp_replace(out, r"\bR\s+R\b", "R&R")
    for src, dst in LOCALITY_ABBREVIATION_REPLACEMENTS:
        out = regexp_replace(out, rf"\b{src}\b", dst)
    out = regexp_replace(out, r"\s+", " ")
    return trim(out)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load cleaned parquet data into Postgres.")
    parser.add_argument("--input", default="output/cleaned", help="Parquet folder or file")
    parser.add_argument("--table", default="standardized_address", help="Target table (optionally schema.table)")
    parser.add_argument("--mode", default="append", choices=["append", "overwrite"], help="Write mode")
    parser.add_argument("--normalized", action="store_true", help="Load into ERD-style normalized tables")
    parser.add_argument("--lookups-dir", default="data/lookups", help="Lookups directory for codes")
    parser.add_argument("--pbt-dir", default=DEFAULT_PBT_DIR, help="PBT shapefiles directory")
    parser.add_argument("--pbt-id-column", default="pbt_id", help="PBT ID column name in shapefile")
    parser.add_argument("--pbt-name-column", default="NAMA_PBT", help="PBT name column in shapefile")
    parser.add_argument("--schema", default=os.getenv("PGSCHEMA", "nas"), help="Target schema for normalized tables")
    parser.add_argument(
        "--allow-lookup-schema-overwrite",
        action="store_true",
        help="Allow writing normalized tables into LOOKUP_SCHEMA (disabled by default for safety).",
    )
    parser.add_argument(
        "--naskod-suffix-max",
        type=int,
        default=12,
        help="Max length for NASKod suffix (vanity). Standard codes are always 6 digits.",
    )
    parser.add_argument("--jdbc-url", default=None, help="JDBC URL (overrides host/port/database)")
    parser.add_argument("--host", default=None, help="Postgres host (default: PGHOST or localhost)")
    parser.add_argument("--port", default=None, help="Postgres port (default: PGPORT or 5432)")
    parser.add_argument("--database", default=None, help="Postgres database (default: PGDATABASE or postgres)")
    parser.add_argument("--user", default=os.getenv("PGUSER"))
    parser.add_argument("--password", default=os.getenv("PGPASSWORD"))
    parser.add_argument("--repartition", type=int, default=1, help="Repartition before write")
    parser.add_argument(
        "--audit-log",
        default=os.getenv("NAS_AUDIT_LOG", "logs/nas_audit.log"),
        help="JSONL audit logfile path",
    )
    return parser.parse_args()


def _write_table(df, *, jdbc_url: str, table: str, mode: str, props: dict, repartition: int | None = None) -> None:
    if repartition and repartition > 0:
        df = df.repartition(repartition)
    df.write.jdbc(url=jdbc_url, table=table, mode=mode, properties=props)


def _write_table_if_has_rows(df, *, jdbc_url: str, table: str, mode: str, props: dict, repartition: int | None = None) -> None:
    if not _df_has_rows(df):
        return
    _write_table(df, jdbc_url=jdbc_url, table=table, mode=mode, props=props, repartition=repartition)


def _ensure_column(df, name: str, dtype: str = "string"):
    if name in df.columns:
        return df
    return df.withColumn(name, lit(None).cast(dtype))


def _run_sql(spark: SparkSession, jdbc_url: str, user: str, password: str, statements: list[str]) -> None:
    jvm = spark._sc._jvm
    # Ensure JDBC driver is registered for DriverManager under Spark's classloader.
    driver_cls = "org.postgresql.Driver"
    try:
        jvm.org.apache.spark.sql.execution.datasources.jdbc.DriverRegistry.register(driver_cls)
    except Exception:
        # Fallback for older Spark versions or if DriverRegistry is unavailable.
        jvm.java.lang.Class.forName(driver_cls)
    conn = jvm.java.sql.DriverManager.getConnection(jdbc_url, user, password)
    stmt = conn.createStatement()
    try:
        for sql in statements:
            stmt.execute(sql)
    finally:
        stmt.close()
        conn.close()


def _get_column_udt_name(
    spark: SparkSession,
    *,
    jdbc_url: str,
    user: str,
    password: str,
    schema: str,
    table: str,
    column: str,
) -> str | None:
    schema_lit = _escape_sql_literal(schema)
    table_lit = _escape_sql_literal(table)
    column_lit = _escape_sql_literal(column)
    query = (
        "(SELECT udt_name FROM information_schema.columns "
        f"WHERE table_schema = '{schema_lit}' "
        f"AND table_name = '{table_lit}' "
        f"AND column_name = '{column_lit}' "
        "LIMIT 1) meta"
    )
    df = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", query)
        .option("user", user)
        .option("password", password)
        .option("driver", "org.postgresql.Driver")
        .load()
    )
    rows = df.collect()
    if not rows:
        return None
    return rows[0]["udt_name"]


def _table_exists(
    spark: SparkSession,
    *,
    jdbc_url: str,
    user: str,
    password: str,
    schema: str,
    table: str,
) -> bool:
    schema_lit = _escape_sql_literal(schema)
    table_lit = _escape_sql_literal(table)
    query = (
        "(SELECT 1 AS exists_flag FROM information_schema.tables "
        f"WHERE table_schema = '{schema_lit}' AND table_name = '{table_lit}' "
        "LIMIT 1) table_exists_check"
    )
    df = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", query)
        .option("user", user)
        .option("password", password)
        .option("driver", "org.postgresql.Driver")
        .load()
    )
    return bool(df.take(1))


def _read_jdbc_query(
    spark: SparkSession,
    *,
    jdbc_url: str,
    user: str,
    password: str,
    query: str,
):
    return (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", f"({query}) jdbc_query")
        .option("user", user)
        .option("password", password)
        .option("driver", "org.postgresql.Driver")
        .load()
    )


def _df_has_rows(df) -> bool:
    return bool(df.take(1))


def _max_value(df, column_name: str) -> int:
    rows = df.select(spark_max(col(column_name)).alias("max_value")).collect()
    if not rows:
        return 0
    value = rows[0]["max_value"]
    return int(value or 0)


def _normalized_text_expr(column_name: str):
    return coalesce(upper(trim(col(column_name).cast("string"))), lit(""))


def _address_identity_fingerprint_expr():
    return sha2(
        concat_ws(
            "|",
            _normalized_text_expr("premise_no"),
            _normalized_text_expr("building_name"),
            _normalized_text_expr("floor_level"),
            _normalized_text_expr("unit_no"),
            _normalized_text_expr("lot_no"),
            coalesce(col("street_id").cast("string"), lit("")),
            coalesce(col("locality_id").cast("string"), lit("")),
            coalesce(col("mukim_id").cast("string"), lit("")),
            coalesce(col("district_id").cast("string"), lit("")),
            coalesce(col("state_id").cast("string"), lit("")),
            coalesce(col("postcode_id").cast("string"), lit("")),
            coalesce(col("pbt_id").cast("string"), lit("")),
            _normalized_text_expr("country"),
        ),
        256,
    )


def _pbt_match_key_expr():
    return when(
        col("pbt_id").isNotNull(),
        concat(lit("ID:"), col("pbt_id").cast("string")),
    ).otherwise(concat(lit("NAME:"), _normalized_text_expr("pbt_name")))


def _street_match_key_expr():
    return concat_ws(
        "|",
        _normalized_text_expr("street_name_prefix"),
        _normalized_text_expr("street_name"),
        coalesce(col("locality_id").cast("string"), lit("")),
        coalesce(col("pbt_id").cast("string"), lit("")),
    )


def _load_existing_normalized_state(
    spark: SparkSession,
    *,
    jdbc_url: str,
    user: str,
    password: str,
    schema: str,
) -> dict[str, object]:
    result: dict[str, object] = {}
    if _table_exists(
        spark,
        jdbc_url=jdbc_url,
        user=user,
        password=password,
        schema=schema,
        table="address_type",
    ):
        result["address_type"] = _read_jdbc_query(
            spark,
            jdbc_url=jdbc_url,
            user=user,
            password=password,
            query=(
                "SELECT address_type_id, property_type, property_code, ownership_structure "
                f"FROM {schema}.address_type"
            ),
        )
    if _table_exists(
        spark,
        jdbc_url=jdbc_url,
        user=user,
        password=password,
        schema=schema,
        table="locality",
    ):
        result["locality"] = _read_jdbc_query(
            spark,
            jdbc_url=jdbc_url,
            user=user,
            password=password,
            query=(
                "SELECT locality_id, locality_code, locality_name, mukim_id, created_at, updated_at "
                f"FROM {schema}.locality"
            ),
        )
    if _table_exists(
        spark,
        jdbc_url=jdbc_url,
        user=user,
        password=password,
        schema=schema,
        table="pbt",
    ):
        result["pbt"] = _read_jdbc_query(
            spark,
            jdbc_url=jdbc_url,
            user=user,
            password=password,
            query=(
                "SELECT pbt_id, pbt_name, boundary_geom::text AS boundary_geom, is_active, created_at, updated_at "
                f"FROM {schema}.pbt"
            ),
        )
    if _table_exists(
        spark,
        jdbc_url=jdbc_url,
        user=user,
        password=password,
        schema=schema,
        table="street",
    ):
        result["street"] = _read_jdbc_query(
            spark,
            jdbc_url=jdbc_url,
            user=user,
            password=password,
            query=(
                "SELECT street_id, street_name_prefix, street_name, locality_id, pbt_id, created_at, updated_at "
                f"FROM {schema}.street"
            ),
        )
    if _table_exists(
        spark,
        jdbc_url=jdbc_url,
        user=user,
        password=password,
        schema=schema,
        table="standardized_address",
    ):
        result["standardized_address"] = _read_jdbc_query(
            spark,
            jdbc_url=jdbc_url,
            user=user,
            password=password,
            query=(
                "SELECT address_id, premise_no, building_name, floor_level, unit_no, lot_no, "
                "street_id, locality_id, mukim_id, district_id, state_id, postcode_id, country, "
                "pbt_id, validation_status, validation_date, validation_by, checksum "
                f"FROM {schema}.standardized_address"
            ),
        )
    if _table_exists(
        spark,
        jdbc_url=jdbc_url,
        user=user,
        password=password,
        schema=schema,
        table="naskod",
    ):
        result["naskod"] = _read_jdbc_query(
            spark,
            jdbc_url=jdbc_url,
            user=user,
            password=password,
            query=(
                "SELECT naskod_id, address_id, code, is_vanity, generated_at, verified, status "
                f"FROM {schema}.naskod"
            ),
        )
    return result


def _append_standardized_address_with_geom_cast(
    spark: SparkSession,
    *,
    df,
    jdbc_url: str,
    props: dict,
    user: str,
    password: str,
    schema_qual: str,
) -> None:
    tmp_name = f"standardized_address_ingest_tmp_{int(time.time() * 1000)}"
    tmp_table = f"{schema_qual}{tmp_name}"
    target_table = f"{schema_qual}standardized_address"
    _write_table(df, jdbc_url=jdbc_url, table=tmp_table, mode="overwrite", props=props)
    cols = df.columns
    insert_cols = ", ".join(_quote_ident(c) for c in cols)
    select_cols = []
    for c in cols:
        qc = _quote_ident(c)
        if c == "geom":
            select_cols.append(f"CASE WHEN {qc} IS NULL OR {qc} = '' THEN NULL ELSE ST_GeomFromText({qc}, 4326) END")
        else:
            select_cols.append(qc)
    select_sql = ", ".join(select_cols)
    try:
        _run_sql(
            spark,
            jdbc_url,
            user,
            password,
            [f"INSERT INTO {target_table} ({insert_cols}) SELECT {select_sql} FROM {tmp_table};"],
        )
    finally:
        _run_sql(spark, jdbc_url, user, password, [f"DROP TABLE IF EXISTS {tmp_table};"])


def _load_pbt_boundaries(spark: SparkSession, pbt_dir: str):
    shp_paths = sorted(
        path
        for path in glob(os.path.join(os.path.abspath(pbt_dir), "*.shp"))
        if os.path.isfile(path)
    )
    if not shp_paths:
        return None

    frames = []
    for shp_path in shp_paths:
        df = spark.read.format("shapefile").load(shp_path)
        geom_col = "geometry" if "geometry" in df.columns else "geom"
        if geom_col != "geom":
            df = df.withColumnRenamed(geom_col, "geom")

        prj_path = os.path.splitext(shp_path)[0] + ".prj"
        if os.path.exists(prj_path):
            with open(prj_path, "r", encoding="utf-8") as handle:
                prj_wkt = handle.read().strip()
            if prj_wkt:
                prj_wkt_sql = prj_wkt.replace("'", "''")
                df = df.withColumn(
                    "geom",
                    expr(f"ST_Transform(geom, '{prj_wkt_sql}', 'EPSG:4326')"),
                )
        frames.append(df)

    combined = frames[0]
    for frame in frames[1:]:
        combined = combined.unionByName(frame, allowMissingColumns=True)
    return combined


def _resolve_pbt_dir(pbt_dir: str | None) -> str | None:
    candidates: list[str] = []
    if pbt_dir:
        candidates.append(pbt_dir)
        if pbt_dir == LEGACY_PBT_DIR:
            candidates.append(DEFAULT_PBT_DIR)
        elif pbt_dir == DEFAULT_PBT_DIR:
            candidates.append(LEGACY_PBT_DIR)
    candidates.extend([DEFAULT_PBT_DIR, LEGACY_PBT_DIR])
    seen: set[str] = set()
    deduped: list[str] = []
    for candidate in candidates:
        normalized = os.path.normpath(candidate)
        if normalized in seen:
            continue
        seen.add(normalized)
        deduped.append(candidate)
    for candidate in deduped:
        if os.path.exists(candidate):
            return candidate
    return None


def _build_normalized_tables(
    df,
    *,
    spark: SparkSession,
    lookups_dir: str,
    pbt_boundaries=None,
    pbt_id_column: str = "pbt_id",
    pbt_name_column: str = "NAMA_PBT",
    existing_context: dict[str, object] | None = None,
) -> dict[str, object]:
    existing_context = existing_context or {}
    df = _ensure_column(df, "address_clean")
    df = _ensure_column(df, "address_type")
    df = _ensure_column(df, "building_name")
    df = _ensure_column(df, "floor_level")
    df = _ensure_column(df, "floor_no")
    df = _ensure_column(df, "street_name_prefix")
    df = _ensure_column(df, "street_name")
    df = _ensure_column(df, "locality_name")
    df = _ensure_column(df, "postcode")
    df = _ensure_column(df, "postcode_code")
    df = _ensure_column(df, "state_code")
    df = _ensure_column(df, "state_name")
    df = _ensure_column(df, "district_code")
    df = _ensure_column(df, "district_name")
    df = _ensure_column(df, "mukim_code")
    df = _ensure_column(df, "mukim_name")
    df = _ensure_column(df, "mukim_id")
    df = _ensure_column(df, "pbt_id")
    df = _ensure_column(df, "pbt_name")
    df = _ensure_column(df, "country")
    df = _ensure_column(df, "latitude", "double")
    df = _ensure_column(df, "longitude", "double")
    df = _ensure_column(df, "geom")
    df = _ensure_column(df, "geometry")
    df = _ensure_column(df, "validation_status")
    df = _ensure_column(df, "validation_date", "timestamp")
    df = df.withColumn(
        "postcode",
        coalesce(
            regexp_extract(col("postcode_code"), r"(\\d{5})", 1),
            regexp_extract(col("postcode"), r"(\\d{5})", 1),
        ),
    )

    state_df = spark.read.csv(os.path.join(lookups_dir, "state_codes.csv"), header=True)
    district_df = spark.read.csv(os.path.join(lookups_dir, "district_codes.csv"), header=True)
    mukim_df = spark.read.csv(os.path.join(lookups_dir, "mukim_codes.csv"), header=True)
    postcode_df = spark.read.csv(os.path.join(lookups_dir, "postcodes.csv"), header=True)

    state_table = (
        state_df.select(
            col("state_code").alias("state_code"),
            upper(col("state_name")).alias("state_name"),
        )
        .dropna(subset=["state_code"])
        .dropDuplicates(["state_code"])
    )
    state_table = state_table.withColumn(
        "state_id",
        row_number().over(Window.orderBy(col("state_code").asc_nulls_last())),
    )

    district_table = (
        district_df.select(
            col("state_code"),
            col("district_code"),
            upper(col("district_name")).alias("district_name"),
        )
        .dropna(subset=["state_code", "district_code"])
        .dropDuplicates(["state_code", "district_code"])
        .join(state_table.select("state_id", "state_code"), "state_code", "left")
    )
    district_table = district_table.withColumn(
        "district_id",
        row_number().over(Window.orderBy(col("state_code").asc_nulls_last(), col("district_code").asc_nulls_last())),
    )

    mukim_table = (
        mukim_df.select(
            col("mukim_id").cast("int").alias("mukim_id"),
            col("mukim_code"),
            upper(col("mukim_name")).alias("mukim_name"),
            col("district_code"),
            col("state_code"),
        )
        .dropna(subset=["mukim_id"])
        .dropDuplicates(["mukim_id"])
        .join(
            district_table.select("district_id", "district_code", "state_code"),
            ["district_code", "state_code"],
            "left",
        )
    )

    locality_base = df.select(
        _normalize_locality_col(col("locality_name")).alias("locality_name"),
        col("mukim_id").cast("int").alias("mukim_id"),
    ).filter(
        col("locality_name").isNotNull()
        & (col("locality_name") != lit(""))
        & col("locality_name").rlike("[A-Z]")
        & (~col("locality_name").rlike(r"^\\d+[A-Z]?$"))
        & (~col("locality_name").isin(*LOCALITY_PLACEHOLDER_VALUES))
        & (~col("locality_name").rlike(LOCALITY_INVALID_PREFIX_REGEX))
    )
    locality_candidates = locality_base.dropDuplicates(["locality_name", "mukim_id"])
    existing_locality = existing_context.get("locality")
    if existing_locality is not None:
        existing_locality = existing_locality.select(
            col("locality_id").cast("int").alias("locality_id"),
            col("locality_code"),
            _normalize_locality_col(col("locality_name")).alias("locality_name"),
            col("mukim_id").cast("int").alias("mukim_id"),
            col("created_at"),
            col("updated_at"),
        ).dropDuplicates(["locality_name", "mukim_id"])
        locality_new = locality_candidates.join(
            existing_locality.select("locality_name", "mukim_id"),
            ["locality_name", "mukim_id"],
            "leftanti",
        )
        locality_new = locality_new.withColumn(
            "locality_id",
            lit(_max_value(existing_locality, "locality_id"))
            + row_number().over(Window.orderBy(col("locality_name").asc_nulls_last(), col("mukim_id").asc_nulls_last())),
        )
        locality_new = locality_new.withColumn("locality_code", lit(None).cast("string"))
        locality_new = locality_new.withColumn("created_at", current_timestamp())
        locality_new = locality_new.withColumn("updated_at", current_timestamp())
        locality_table = existing_locality.unionByName(
            locality_new.select("locality_id", "locality_code", "locality_name", "mukim_id", "created_at", "updated_at"),
            allowMissingColumns=True,
        )
        locality_write = locality_new.select(
            "locality_id",
            "locality_code",
            "locality_name",
            "mukim_id",
            "created_at",
            "updated_at",
        )
    else:
        locality_table = locality_candidates.withColumn(
            "locality_id",
            row_number().over(Window.orderBy(col("locality_name").asc_nulls_last(), col("mukim_id").asc_nulls_last())),
        )
        locality_table = locality_table.withColumn("locality_code", lit(None).cast("string"))
        locality_table = locality_table.withColumn("created_at", current_timestamp())
        locality_table = locality_table.withColumn("updated_at", current_timestamp())
        locality_write = locality_table.select(
            "locality_id",
            "locality_code",
            "locality_name",
            "mukim_id",
            "created_at",
            "updated_at",
        )
    locality_with_state = (
        locality_table.join(mukim_table.select("mukim_id", "district_id"), "mukim_id", "left")
        .join(district_table.select("district_id", "state_id"), "district_id", "left")
        .join(state_table.select("state_id", "state_name"), "state_id", "left")
        .select(
            col("locality_id"),
            col("locality_name"),
            col("mukim_id"),
            col("state_name").alias("_locality_state_name"),
        )
    )

    postcode_table = (
        postcode_df.select(
            col("postcode"),
            upper(col("city")).alias("postcode_name"),
            upper(col("state")).alias("_postcode_state_name"),
        )
        .dropna(subset=["postcode"])
        .dropDuplicates(["postcode"])
        .join(
            locality_with_state,
            (col("postcode_name") == col("locality_name"))
            & (upper(col("_postcode_state_name")) == upper(col("_locality_state_name"))),
            "left",
        )
        .drop("_locality_state_name")
    )
    postcode_table = (
        postcode_table.withColumn(
            "_rn",
            row_number().over(Window.partitionBy("postcode").orderBy(col("locality_id").asc_nulls_last())),
        )
        .filter(col("_rn") == 1)
        .drop("_rn")
    )
    postcode_table = postcode_table.withColumn(
        "postcode_id",
        row_number().over(Window.orderBy(col("postcode").asc_nulls_last())),
    )
    postcode_table = postcode_table.select(
        "postcode_id",
        "postcode_name",
        "locality_id",
        col("postcode"),
    )

    address_type_candidates = (
        df.select(upper(col("address_type")).alias("property_type"))
        .filter(col("property_type").isNotNull())
        .dropDuplicates(["property_type"])
    )
    existing_address_type = existing_context.get("address_type")
    if existing_address_type is not None:
        existing_address_type = existing_address_type.select(
            col("address_type_id").cast("int").alias("address_type_id"),
            upper(col("property_type")).alias("property_type"),
            col("property_code"),
            col("ownership_structure"),
        ).dropDuplicates(["property_type"])
        address_type_new = address_type_candidates.join(
            existing_address_type.select("property_type"),
            "property_type",
            "leftanti",
        )
        address_type_new = address_type_new.withColumn(
            "address_type_id",
            lit(_max_value(existing_address_type, "address_type_id"))
            + row_number().over(Window.orderBy(col("property_type").asc_nulls_last())),
        )
        address_type_new = address_type_new.withColumn("property_code", col("property_type"))
        address_type_new = address_type_new.withColumn("ownership_structure", lit(None).cast("string"))
        address_type_table = existing_address_type.unionByName(
            address_type_new.select("address_type_id", "property_type", "property_code", "ownership_structure"),
            allowMissingColumns=True,
        )
        address_type_write = address_type_new.select(
            "address_type_id",
            "property_type",
            "property_code",
            "ownership_structure",
        )
    else:
        address_type_table = address_type_candidates.withColumn(
            "address_type_id",
            row_number().over(Window.orderBy(col("property_type").asc_nulls_last())),
        )
        address_type_table = address_type_table.withColumn("property_code", col("property_type"))
        address_type_table = address_type_table.withColumn("ownership_structure", lit(None).cast("string"))
        address_type_write = address_type_table.select(
            "address_type_id",
            "property_type",
            "property_code",
            "ownership_structure",
        )

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
            col(pbt_name_col).alias("pbt_name"),
            expr("ST_AsText(geom)").alias("boundary_geom"),
        )
        pbt_table = pbt_table.withColumn(
            "pbt_id",
            when(col("_pbt_id").rlike(r"^\\d+$"), col("_pbt_id").cast("int")).otherwise(lit(None).cast("int")),
        ).drop("_pbt_id")
    else:
        pbt_table = (
            df.select(
                when(col("pbt_id").rlike(r"^\\d+$"), col("pbt_id").cast("int")).alias("pbt_id"),
                col("pbt_name"),
            )
            .filter(col("pbt_id").isNotNull() | col("pbt_name").isNotNull())
            .dropDuplicates(["pbt_id", "pbt_name"])
        )
        pbt_table = pbt_table.withColumn("boundary_geom", lit(None).cast("string"))

    pbt_candidates = pbt_table.dropDuplicates(["pbt_id", "pbt_name"])
    pbt_candidates = pbt_candidates.withColumn("is_active", lit(True))
    pbt_candidates = pbt_candidates.withColumn("created_at", current_timestamp())
    pbt_candidates = pbt_candidates.withColumn("updated_at", current_timestamp())
    pbt_candidates = pbt_candidates.withColumn("_pbt_match_key", _pbt_match_key_expr())
    existing_pbt = existing_context.get("pbt")
    if existing_pbt is not None:
        existing_pbt = existing_pbt.select(
            col("pbt_id").cast("int").alias("pbt_id"),
            col("pbt_name"),
            col("boundary_geom"),
            col("is_active"),
            col("created_at"),
            col("updated_at"),
        ).dropDuplicates(["pbt_id", "pbt_name"])
        existing_pbt = existing_pbt.withColumn("_pbt_match_key", _pbt_match_key_expr())
        pbt_new = pbt_candidates.join(
            existing_pbt.select("_pbt_match_key"),
            "_pbt_match_key",
            "leftanti",
        ).drop("_pbt_match_key")
        pbt_table = existing_pbt.drop("_pbt_match_key").unionByName(pbt_new, allowMissingColumns=True)
        pbt_write = pbt_new.select("pbt_id", "pbt_name", "boundary_geom", "is_active", "created_at", "updated_at")
    else:
        pbt_table = pbt_candidates.drop("_pbt_match_key")
        pbt_write = pbt_table.select("pbt_id", "pbt_name", "boundary_geom", "is_active", "created_at", "updated_at")

    street_base = df.select(
        col("street_name_prefix"),
        col("street_name"),
        _normalize_locality_col(col("locality_name")).alias("locality_name"),
        col("mukim_id").cast("int").alias("mukim_id"),
        when(col("pbt_id").rlike(r"^\\d+$"), col("pbt_id").cast("int")).alias("pbt_id"),
    ).filter(col("street_name").isNotNull())
    street_base = street_base.join(locality_table, ["locality_name", "mukim_id"], "left")
    street_candidates = street_base.dropDuplicates(["street_name_prefix", "street_name", "locality_id", "pbt_id"])
    street_candidates = street_candidates.withColumn("_street_match_key", _street_match_key_expr())
    existing_street = existing_context.get("street")
    if existing_street is not None:
        existing_street = existing_street.select(
            col("street_id").cast("int").alias("street_id"),
            col("street_name_prefix"),
            col("street_name"),
            col("locality_id").cast("int").alias("locality_id"),
            col("pbt_id").cast("int").alias("pbt_id"),
            col("created_at"),
            col("updated_at"),
        ).dropDuplicates(["street_id"])
        existing_street = existing_street.withColumn("_street_match_key", _street_match_key_expr())
        street_new = street_candidates.join(
            existing_street.select("_street_match_key"),
            "_street_match_key",
            "leftanti",
        ).drop("_street_match_key")
        street_new = street_new.withColumn(
            "street_id",
            lit(_max_value(existing_street, "street_id"))
            + row_number().over(
                Window.orderBy(
                    col("street_name").asc_nulls_last(),
                    col("locality_id").asc_nulls_last(),
                    col("pbt_id").asc_nulls_last(),
                )
            ),
        )
        street_new = street_new.withColumn("created_at", current_timestamp())
        street_new = street_new.withColumn("updated_at", current_timestamp())
        street_table = existing_street.drop("_street_match_key").unionByName(
            street_new.select("street_name_prefix", "street_name", "locality_id", "pbt_id", "street_id", "created_at", "updated_at"),
            allowMissingColumns=True,
        )
        street_write = street_new.select(
            "street_id",
            "street_name_prefix",
            "street_name",
            "locality_id",
            "pbt_id",
            "created_at",
            "updated_at",
        )
    else:
        street_table = street_candidates.drop("_street_match_key")
        street_table = street_table.withColumn(
            "street_id",
            row_number().over(
                Window.orderBy(
                    col("street_name").asc_nulls_last(),
                    col("locality_id").asc_nulls_last(),
                    col("pbt_id").asc_nulls_last(),
                )
            ),
        )
        street_table = street_table.withColumn("created_at", current_timestamp())
        street_table = street_table.withColumn("updated_at", current_timestamp())
        street_write = street_table.select(
            "street_id",
            "street_name_prefix",
            "street_name",
            "locality_id",
            "pbt_id",
            "created_at",
            "updated_at",
        )

    # StandardizedAddress
    addr = df.withColumn("locality_name", _normalize_locality_col(col("locality_name")))
    addr = addr.withColumn(
        "pbt_id_int",
        when(col("pbt_id").rlike(r"^\\d+$"), col("pbt_id").cast("int")).otherwise(lit(None).cast("int")),
    )

    addr = addr.join(state_table.select("state_id", "state_code"), "state_code", "left")
    state_name_lookup = state_table.select(
        col("state_id").alias("_lk_state_id"),
        col("state_name").alias("_lk_state_name"),
    )
    addr = addr.join(state_name_lookup, upper(col("state_name")) == col("_lk_state_name"), "left")
    addr = addr.withColumn("state_id", coalesce(col("state_id"), col("_lk_state_id"))).drop(
        "_lk_state_id",
        "_lk_state_name",
    )
    addr = addr.join(
        district_table.select("district_id", "district_code", "state_code"),
        ["district_code", "state_code"],
        "left",
    )
    district_name_lookup = district_table.select(
        col("district_id").alias("_lk_district_id"),
        col("district_name").alias("_lk_district_name"),
        col("state_id").alias("_lk_state_id"),
    )
    addr = addr.join(
        district_name_lookup,
        (upper(col("district_name")) == col("_lk_district_name")) & (col("state_id") == col("_lk_state_id")),
        "left",
    )
    addr = addr.withColumn("district_id", coalesce(col("district_id"), col("_lk_district_id"))).drop(
        "_lk_district_id",
        "_lk_district_name",
        "_lk_state_id",
    )
    addr = addr.withColumn("mukim_id", col("mukim_id").cast("int"))
    addr = addr.join(
        mukim_table.select(
            col("mukim_id").alias("_lk_mukim_id"),
            col("mukim_code"),
            col("district_code"),
            col("state_code"),
        ),
        ["mukim_code", "district_code", "state_code"],
        "left",
    )
    addr = addr.withColumn("mukim_id", coalesce(col("mukim_id"), col("_lk_mukim_id"))).drop("_lk_mukim_id")
    mukim_name_lookup = mukim_table.select(
        col("mukim_id").alias("_lk_mukim_id"),
        col("mukim_name").alias("_lk_mukim_name"),
        col("district_id").alias("_lk_district_id"),
    )
    addr = addr.join(
        mukim_name_lookup,
        (upper(col("mukim_name")) == col("_lk_mukim_name")) & (col("district_id") == col("_lk_district_id")),
        "left",
    )
    addr = addr.withColumn("mukim_id", coalesce(col("mukim_id"), col("_lk_mukim_id"))).drop(
        "_lk_mukim_id",
        "_lk_mukim_name",
        "_lk_district_id",
    )
    addr = addr.join(locality_table.select("locality_id", "locality_name", "mukim_id"), ["locality_name", "mukim_id"], "left")
    addr = addr.join(postcode_table.select("postcode_id", "postcode"), "postcode", "left")
    addr = addr.join(
        address_type_table.select("address_type_id", "property_type"),
        upper(col("address_type")) == col("property_type"),
        "left",
    ).drop("property_type")
    street_lookup = street_table.select(
        col("street_id"),
        col("street_name_prefix").alias("_lk_street_prefix"),
        col("street_name").alias("_lk_street_name"),
        col("locality_id").alias("_lk_locality_id"),
        col("pbt_id").alias("_lk_pbt_id"),
    )
    addr = addr.join(
        street_lookup,
        (addr.street_name_prefix == col("_lk_street_prefix"))
        & (addr.street_name == col("_lk_street_name"))
        & (addr.locality_id == col("_lk_locality_id"))
        & (addr.pbt_id_int == col("_lk_pbt_id")),
        "left",
    ).drop("_lk_street_prefix", "_lk_street_name", "_lk_locality_id", "_lk_pbt_id")

    lon_num = col("longitude").cast("double")
    lat_num = col("latitude").cast("double")
    geom_from_coords = when(
        lon_num.isNotNull() & lat_num.isNotNull() & (~isnan(lon_num)) & (~isnan(lat_num)),
        concat(
            lit("POINT("),
            lon_num.cast("string"),
            lit(" "),
            lat_num.cast("string"),
            lit(")"),
        ),
    ).otherwise(lit(None).cast("string"))
    addr = addr.withColumn(
        "geom",
        coalesce(col("geom"), col("geometry"), geom_from_coords),
    )
    addr = addr.withColumn("created_at", current_timestamp())
    addr = addr.withColumn("updated_at", current_timestamp())
    addr = addr.withColumn(
        "validation_status",
        coalesce(col("validation_status"), lit("PASS")),
    )
    addr = addr.withColumn("validation_date", current_timestamp())
    addr = addr.withColumn(
        "checksum",
        sha2(coalesce(col("address_clean"), lit("")), 256),
    )

    standardized_address_candidate = addr.select(
        col("premise_no"),
        col("building_name"),
        col("floor_level"),
        col("unit_no"),
        col("lot_no"),
        col("street_id").cast("int"),
        col("locality_id").cast("int"),
        col("mukim_id").cast("int"),
        col("district_id").cast("int"),
        col("state_id").cast("int"),
        col("postcode_id").cast("int"),
        col("country"),
        col("pbt_id_int").alias("pbt_id"),
        col("latitude").cast("double"),
        col("longitude").cast("double"),
        col("geom"),
        col("created_at"),
        col("updated_at"),
        col("address_type_id").cast("int"),
        col("validation_status"),
        col("validation_date"),
        lit(None).cast("int").alias("validation_by"),
        col("checksum"),
    )
    standardized_address_candidate = standardized_address_candidate.withColumn(
        "identity_fingerprint",
        _address_identity_fingerprint_expr(),
    )
    # Collapse duplicate canonical addresses within the same batch before
    # allocating persistent IDs or generating standard NASKOD rows.
    standardized_address_candidate = standardized_address_candidate.dropDuplicates(["identity_fingerprint"])
    existing_standardized = existing_context.get("standardized_address")
    next_address_id_start = 0
    if existing_standardized is not None:
        existing_standardized = existing_standardized.select(
            col("address_id").cast("int").alias("address_id"),
            col("premise_no"),
            col("building_name"),
            col("floor_level"),
            col("unit_no"),
            col("lot_no"),
            col("street_id").cast("int").alias("street_id"),
            col("locality_id").cast("int").alias("locality_id"),
            col("mukim_id").cast("int").alias("mukim_id"),
            col("district_id").cast("int").alias("district_id"),
            col("state_id").cast("int").alias("state_id"),
            col("postcode_id").cast("int").alias("postcode_id"),
            col("country"),
            col("pbt_id").cast("int").alias("pbt_id"),
            col("validation_status"),
            col("validation_date"),
            col("validation_by").cast("int").alias("validation_by"),
            col("checksum"),
        )
        existing_standardized = existing_standardized.withColumn(
            "identity_fingerprint",
            _address_identity_fingerprint_expr(),
        )
        existing_by_checksum = existing_standardized.select(
            col("checksum"),
            col("address_id").alias("_existing_address_id_by_checksum"),
        ).filter(col("checksum").isNotNull()).dropDuplicates(["checksum"])
        existing_by_fingerprint = existing_standardized.select(
            col("identity_fingerprint"),
            col("address_id").alias("_existing_address_id_by_fingerprint"),
        ).dropDuplicates(["identity_fingerprint"])
        standardized_address_candidate = standardized_address_candidate.join(
            existing_by_checksum,
            "checksum",
            "left",
        )
        standardized_address_candidate = standardized_address_candidate.join(
            existing_by_fingerprint,
            "identity_fingerprint",
            "left",
        )
        standardized_address_candidate = standardized_address_candidate.withColumn(
            "_existing_address_id",
            coalesce(
                col("_existing_address_id_by_checksum"),
                col("_existing_address_id_by_fingerprint"),
            ),
        )
        next_address_id_start = _max_value(existing_standardized, "address_id")
    standardized_address_new = standardized_address_candidate.filter(
        col("_existing_address_id").isNull() if "_existing_address_id" in standardized_address_candidate.columns else lit(True)
    )
    standardized_address_new = standardized_address_new.drop(
        "identity_fingerprint",
        "_existing_address_id",
        "_existing_address_id_by_checksum",
        "_existing_address_id_by_fingerprint",
    )
    standardized_address_new = standardized_address_new.withColumn(
        "address_id",
        lit(next_address_id_start)
        + row_number().over(Window.orderBy(monotonically_increasing_id())),
    )
    standardized_address = standardized_address_new.select(
        col("address_id").cast("int"),
        col("premise_no"),
        col("building_name"),
        col("floor_level"),
        col("unit_no"),
        col("lot_no"),
        col("street_id").cast("int"),
        col("locality_id").cast("int"),
        col("mukim_id").cast("int"),
        col("district_id").cast("int"),
        col("state_id").cast("int"),
        col("postcode_id").cast("int"),
        col("country"),
        col("pbt_id").cast("int"),
        col("latitude").cast("double"),
        col("longitude").cast("double"),
        col("geom"),
        col("created_at"),
        col("updated_at"),
        col("address_type_id").cast("int"),
        col("validation_status"),
        col("validation_date"),
        col("validation_by").cast("int"),
        col("checksum"),
    )

    # NASKod generation (standard codes for newly inserted addresses only)
    state_abbr_map = {
        "01": "JHR",
        "02": "KDH",
        "03": "KTN",
        "04": "MLK",
        "05": "NSN",
        "06": "PHG",
        "07": "PNG",
        "08": "PRK",
        "09": "PLS",
        "10": "SGR",
        "11": "TRG",
        "12": "SBH",
        "13": "SWK",
        "14": "KL",
        "15": "LAB",
        "16": "PJY",
    }
    map_expr = create_map([lit(x) for kv in state_abbr_map.items() for x in kv])
    district_codes = district_table.select("district_id", "district_code", "state_id")
    state_codes = state_table.select("state_id", "state_code")
    naskod_base = (
        standardized_address.select("address_id", "district_id", "state_id")
        .join(district_codes, "district_id", "left")
        .join(state_codes, "state_id", "left")
    )
    naskod_base = naskod_base.withColumn("district_code", lpad(col("district_code"), 2, "0"))
    naskod_base = naskod_base.withColumn("state_code", lpad(col("state_code"), 2, "0"))
    naskod_base = naskod_base.withColumn("state_abbr", map_expr.getItem(col("state_code")))
    naskod_base = naskod_base.withColumn(
        "state_abbr",
        when(col("state_abbr").isNull() & col("state_code").rlike(r"^[A-Z]{2,3}$"), col("state_code"))
        .otherwise(col("state_abbr")),
    )
    existing_naskod = existing_context.get("naskod")
    next_naskod_id_start = 0
    if existing_naskod is not None:
        next_naskod_id_start = _max_value(existing_naskod, "naskod_id")
        existing_naskod_max = (
            existing_naskod.filter(col("is_vanity") == lit(False))
            .withColumn("state_abbr", split(col("code"), "-").getItem(1))
            .withColumn("district_code", split(col("code"), "-").getItem(2))
            .withColumn("suffix_num", split(col("code"), "-").getItem(3).cast("int"))
            .groupBy("state_abbr", "district_code")
            .agg(spark_max(col("suffix_num")).alias("existing_max_suffix"))
        )
        naskod_base = naskod_base.join(
            existing_naskod_max,
            ["state_abbr", "district_code"],
            "left",
        )
    else:
        naskod_base = naskod_base.withColumn("existing_max_suffix", lit(0))
    naskod_seq = Window.partitionBy("state_abbr", "district_code").orderBy(col("address_id").asc_nulls_last())
    naskod_base = naskod_base.withColumn("seq", row_number().over(naskod_seq))
    naskod_base = naskod_base.withColumn(
        "suffix_num",
        coalesce(col("existing_max_suffix"), lit(0)) + col("seq"),
    )
    naskod_base = naskod_base.withColumn("suffix", lpad(col("suffix_num").cast("string"), 6, "0"))
    naskod_base = naskod_base.withColumn(
        "code",
        when(
            col("state_abbr").isNotNull() & col("district_code").isNotNull(),
            concat(lit("NAS-"), col("state_abbr"), lit("-"), col("district_code"), lit("-"), col("suffix")),
        ),
    )
    naskod = naskod_base.withColumn(
        "status",
        when(col("code").isNotNull(), lit("active")).otherwise(lit("pending")),
    )
    naskod = naskod.withColumn("is_vanity", lit(False))
    naskod = naskod.withColumn("generated_at", current_timestamp())
    naskod = naskod.withColumn("verified", lit(False))
    naskod = naskod.withColumn(
        "naskod_id",
        lit(next_naskod_id_start) + row_number().over(Window.orderBy(col("address_id").asc_nulls_last())),
    )
    naskod = naskod.select(
        col("naskod_id").cast("int"),
        col("address_id").cast("int"),
        col("code"),
        col("is_vanity"),
        col("generated_at"),
        col("verified"),
        col("status"),
    )

    return {
        "state": state_table.select("state_id", "state_name", "state_code"),
        "district": district_table.select("district_id", "district_name", "state_id", "district_code"),
        "mukim": mukim_table.select("mukim_id", "mukim_name", "district_id", "mukim_code"),
        "locality": locality_write.select(
            "locality_id",
            "locality_code",
            "locality_name",
            "mukim_id",
            "created_at",
            "updated_at",
        ),
        "postcode": postcode_table,
        "pbt": pbt_write.select("pbt_id", "pbt_name", "boundary_geom", "is_active", "created_at", "updated_at"),
        "street": street_write.select(
            "street_id",
            "street_name_prefix",
            "street_name",
            "locality_id",
            "pbt_id",
            "created_at",
            "updated_at",
        ),
        "address_type": address_type_write.select("address_type_id", "property_type", "property_code", "ownership_structure"),
        "naskod": naskod,
        "standardized_address": standardized_address,
    }


def main() -> None:
    args = parse_args()
    if not args.user or not args.password:
        raise ValueError("PGUSER and PGPASSWORD must be provided via .env or CLI (--user/--password).")
    started = time.time()
    run_id = start_audit_run(args.audit_log, "load_postgres", vars(args))
    spark = None
    status = "ok"
    try:
        builder = SparkSession.builder.appName("NAS Load Postgres")
        builder = configure_common_spark_builder(builder)
        builder = configure_sedona_builder(builder)
        jars_packages = resolve_sedona_spark_packages()
        jars_packages = merge_spark_packages(jars_packages, "org.postgresql:postgresql:42.7.3")
        if jars_packages:
            builder = builder.config("spark.jars.packages", jars_packages)
        spark = builder.getOrCreate()
        quiet_spark_spatial_warnings(spark)

        df = spark.read.parquet(args.input)
        if args.repartition and args.repartition > 0:
            df = df.repartition(args.repartition)

        input_count = df.count()
        jdbc_url = _build_jdbc_url(args)
        props = {
            "user": args.user,
            "password": args.password,
            "driver": "org.postgresql.Driver",
        }
        audit_event(
            args.audit_log,
            "load_postgres",
            run_id,
            "input_loaded",
            input_path=args.input,
            input_count=input_count,
            normalized=bool(args.normalized),
            target_table=args.table,
        )
        if args.normalized:
            schema = args.schema or ""
            lookup_schema = (os.getenv("LOOKUP_SCHEMA", "nas_lookup") or "nas_lookup").strip()
            if schema and lookup_schema and schema == lookup_schema and not args.allow_lookup_schema_overwrite:
                raise ValueError(
                    f"Refusing to write normalized ingest tables into lookup schema '{lookup_schema}'. "
                    "Use a different --schema or pass --allow-lookup-schema-overwrite intentionally."
                )
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
            resolved_pbt_dir = _resolve_pbt_dir(args.pbt_dir)
            if resolved_pbt_dir:
                try:
                    from .sedona_utils import register_sedona

                    register_sedona(spark)
                    pbt_boundaries = _load_pbt_boundaries(spark, resolved_pbt_dir)
                    if resolved_pbt_dir != args.pbt_dir:
                        audit_event(
                            args.audit_log,
                            "load_postgres",
                            run_id,
                            "pbt_dir_resolved",
                            requested_pbt_dir=args.pbt_dir,
                            resolved_pbt_dir=resolved_pbt_dir,
                        )
                except Exception as exc:
                    audit_event(
                        args.audit_log,
                        "load_postgres",
                        run_id,
                        "pbt_boundary_warning",
                        warning=str(exc),
                    )
                    print(f"Warning: failed to load PBT boundaries: {exc}")
            else:
                audit_event(
                    args.audit_log,
                    "load_postgres",
                    run_id,
                    "pbt_boundary_warning",
                    warning="PBT directory not found; fallback to source pbt_id/pbt_name values",
                    requested_pbt_dir=args.pbt_dir,
                )

            existing_context = _load_existing_normalized_state(
                spark,
                jdbc_url=jdbc_url,
                user=args.user,
                password=args.password,
                schema=schema_for_check,
            )
            tables = _build_normalized_tables(
                df,
                spark=spark,
                lookups_dir=args.lookups_dir,
                pbt_boundaries=pbt_boundaries,
                pbt_id_column=args.pbt_id_column,
                pbt_name_column=args.pbt_name_column,
                existing_context=existing_context,
            )
            source_locality_count = (
                df.select(trim(col("locality_name")).alias("_locality_name"))
                .filter(col("_locality_name").isNotNull() & (col("_locality_name") != lit("")))
                .count()
            )
            source_pbt_count = (
                df.select(
                    trim(col("pbt_id").cast("string")).alias("_pbt_id"),
                    trim(col("pbt_name")).alias("_pbt_name"),
                )
                .filter(
                    (col("_pbt_id").isNotNull() & (col("_pbt_id") != lit("")))
                    | (col("_pbt_name").isNotNull() & (col("_pbt_name") != lit("")))
                )
                .count()
            )
            locality_count = tables["locality"].count()
            pbt_count = tables["pbt"].count()
            audit_event(
                args.audit_log,
                "load_postgres",
                run_id,
                "normalized_dimension_counts",
                locality_count=locality_count,
                pbt_count=pbt_count,
                source_locality_count=source_locality_count,
                source_pbt_count=source_pbt_count,
                pbt_dir=(resolved_pbt_dir or args.pbt_dir),
                pbt_from_boundaries=bool(pbt_boundaries is not None),
            )
            if locality_count == 0 or pbt_count == 0:
                audit_event(
                    args.audit_log,
                    "load_postgres",
                    run_id,
                    "dimension_empty_warning",
                    locality_count=locality_count,
                    pbt_count=pbt_count,
                    source_locality_count=source_locality_count,
                    source_pbt_count=source_pbt_count,
                )
            dim_mode = "overwrite"
            _write_table(tables["state"], jdbc_url=jdbc_url, table=f"{schema_qual}state", mode=dim_mode, props=props)
            _write_table(
                tables["district"], jdbc_url=jdbc_url, table=f"{schema_qual}district", mode=dim_mode, props=props
            )
            _write_table(tables["mukim"], jdbc_url=jdbc_url, table=f"{schema_qual}mukim", mode=dim_mode, props=props)
            _write_table(
                tables["postcode"], jdbc_url=jdbc_url, table=f"{schema_qual}postcode", mode=dim_mode, props=props
            )
            _write_table_if_has_rows(
                tables["pbt"], jdbc_url=jdbc_url, table=f"{schema_qual}pbt", mode="append", props=props
            )
            _write_table_if_has_rows(
                tables["street"], jdbc_url=jdbc_url, table=f"{schema_qual}street", mode="append", props=props
            )
            _write_table_if_has_rows(
                tables["address_type"], jdbc_url=jdbc_url, table=f"{schema_qual}address_type", mode="append", props=props
            )
            _write_table_if_has_rows(
                tables["locality"], jdbc_url=jdbc_url, table=f"{schema_qual}locality", mode="append", props=props
            )
            _write_table_if_has_rows(
                tables["naskod"], jdbc_url=jdbc_url, table=f"{schema_qual}naskod", mode="append", props=props
            )
            geom_udt_name = _get_column_udt_name(
                spark,
                jdbc_url=jdbc_url,
                user=args.user,
                password=args.password,
                schema=schema_for_check,
                table="standardized_address",
                column="geom",
            )
            if args.mode == "append" and geom_udt_name == "geometry":
                if _df_has_rows(tables["standardized_address"]):
                    _append_standardized_address_with_geom_cast(
                        spark,
                        df=tables["standardized_address"],
                        jdbc_url=jdbc_url,
                        props=props,
                        user=args.user,
                        password=args.password,
                        schema_qual=schema_qual,
                    )
            else:
                _write_table_if_has_rows(
                    tables["standardized_address"],
                    jdbc_url=jdbc_url,
                    table=f"{schema_qual}standardized_address",
                    mode=args.mode,
                    props=props,
                )
            _run_sql(
                spark,
                jdbc_url,
                args.user,
                args.password,
                [
                    f"""
                    DO $$
                    BEGIN
                      IF EXISTS (
                        SELECT 1
                        FROM information_schema.columns
                        WHERE table_name = 'standardized_address'
                          AND table_schema = '{schema_for_check}'
                          AND column_name = 'geom'
                          AND (udt_name IS DISTINCT FROM 'geometry')
                      ) THEN
                        ALTER TABLE {schema_qual}standardized_address
                          ALTER COLUMN geom
                          TYPE geometry(Point, 4326)
                          USING ST_GeomFromText(geom, 4326);
                      END IF;
                    END$$;
                    """,
                    f"CREATE INDEX IF NOT EXISTS standardized_address_geom_idx ON {schema_qual}standardized_address USING GIST (geom);",
                    f"""
                    DO $$
                    BEGIN
                      IF EXISTS (
                        SELECT 1
                        FROM information_schema.columns
                        WHERE table_name = 'pbt'
                          AND table_schema = '{schema_for_check}'
                          AND column_name = 'boundary_geom'
                          AND (udt_name IS DISTINCT FROM 'geometry')
                      ) THEN
                        ALTER TABLE {schema_qual}pbt
                          ALTER COLUMN boundary_geom
                          TYPE geometry(Geometry, 4326)
                          USING ST_Force2D(ST_GeomFromText(boundary_geom, 4326));
                      END IF;
                    END$$;
                    """,
                    f"CREATE INDEX IF NOT EXISTS pbt_boundary_geom_idx ON {schema_qual}pbt USING GIST (boundary_geom);",
                    f"""
                    DO $$
                    BEGIN
                      IF NOT EXISTS (
                        SELECT 1
                        FROM pg_constraint c
                        JOIN pg_class t ON c.conrelid = t.oid
                        JOIN pg_namespace n ON n.oid = t.relnamespace
                        WHERE c.conname = 'naskod_code_format_chk'
                          AND n.nspname = '{schema_for_check}'
                      ) THEN
                        ALTER TABLE {schema_qual}naskod
                          ADD CONSTRAINT naskod_code_format_chk
                          CHECK (
                            code = upper(code)
                            AND code ~ '^NAS-[A-Z]{{2,3}}-[0-9]{{2}}-[A-Z0-9]+$'
                            AND length(split_part(code, '-', 4)) <= {args.naskod_suffix_max}
                          );
                      END IF;
                    END$$;
                    """,
                    f"""
                    DO $$
                    BEGIN
                      IF NOT EXISTS (
                        SELECT 1
                        FROM pg_constraint c
                        JOIN pg_class t ON c.conrelid = t.oid
                        JOIN pg_namespace n ON n.oid = t.relnamespace
                        WHERE c.conname = 'naskod_standard_suffix_chk'
                          AND n.nspname = '{schema_for_check}'
                      ) THEN
                        ALTER TABLE {schema_qual}naskod
                          ADD CONSTRAINT naskod_standard_suffix_chk
                          CHECK (
                            (is_vanity = true)
                            OR (
                              split_part(code, '-', 4) ~ '^[0-9]{{6}}$'
                              AND length(split_part(code, '-', 4)) = 6
                            )
                          );
                      END IF;
                    END$$;
                    """,
                    f"CREATE UNIQUE INDEX IF NOT EXISTS naskod_code_uniq ON {schema_qual}naskod (code);",
                    f"""
                    CREATE UNIQUE INDEX IF NOT EXISTS naskod_standard_per_address_uniq
                    ON {schema_qual}naskod (address_id)
                    WHERE is_vanity = false;
                    """,
                ],
            )
            audit_event(
                args.audit_log,
                "load_postgres",
                run_id,
                "load_complete",
                normalized=True,
                schema=schema_for_check,
                row_count=input_count,
            )
        else:
            _write_table(df, jdbc_url=jdbc_url, table=args.table, mode=args.mode, props=props)
            audit_event(
                args.audit_log,
                "load_postgres",
                run_id,
                "load_complete",
                normalized=False,
                table=args.table,
                row_count=input_count,
            )
    except Exception as exc:
        status = "error"
        audit_event(
            args.audit_log,
            "load_postgres",
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
            "load_postgres",
            run_id,
            "run_end",
            status=status,
            duration_ms=int((time.time() - started) * 1000),
        )


if __name__ == "__main__":
    main()
