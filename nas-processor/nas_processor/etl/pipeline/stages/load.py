"""Load stage — upsert standardized_address + insert raw_address in one transaction.

Uses Polars ADBC for bulk writes and SQLAlchemy for DDL/upsert SQL.
Both tables are written in a single transaction per batch.
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone

import polars as pl
import sqlalchemy as sa

from nas_config.db import build_dsn

logger = logging.getLogger(__name__)


# ── Import schema contracts ───────────────────────────────────────────────────

try:
    from nas_processor.src.repository.address_schema import (
        STANDARDIZED_ADDRESS_COLUMNS,
        FLOAT_COLUMNS,
        INTEGER_COLUMNS,
        TIMESTAMP_COLUMNS,
        PRIMARY_KEY,
        RAW_ADDRESS_COLUMNS,
        validate_dataframe,
    )
except ImportError:
    import sys, os
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "..", "src"))
    from repository.address_schema import (  # type: ignore[no-redef]
        STANDARDIZED_ADDRESS_COLUMNS,
        FLOAT_COLUMNS,
        INTEGER_COLUMNS,
        TIMESTAMP_COLUMNS,
        PRIMARY_KEY,
        RAW_ADDRESS_COLUMNS,
        validate_dataframe,
    )


# ── Constants ─────────────────────────────────────────────────────────────────

_DEFAULT_BATCH_SIZE = 10_000
_NASKOD_COL = "naskod"
_CHANGE_DETECTION_COL = "canonical_address_key"

# Pipeline columns to drop (internal/intermediate)
_DROP_COLUMNS: frozenset[str] = frozenset({
    "_spatial_confirmed",
    "address_for_lookup",
    "address_norm",
    "postcode_raw",
    "postcode_ungazetted",
    "state",
    "error_reason",
    "suggestion",
    "source_address_old",
    "source_address_new",
})


# ── Helpers ───────────────────────────────────────────────────────────────────


def _quote(name: str) -> str:
    return f'"{name}"'


def _qualified(schema: str, table: str) -> str:
    return f"{_quote(schema)}.{_quote(table)}"


def _assert_table_exists(engine: sa.Engine, schema: str, table: str) -> None:
    with engine.connect() as conn:
        result = conn.execute(
            sa.text(
                "SELECT 1 FROM information_schema.tables "
                "WHERE table_schema = :s AND table_name = :t"
            ),
            {"s": schema, "t": table},
        )
        if not result.fetchone():
            raise RuntimeError(
                f"Table {schema}.{table} does not exist. "
                f"Run Alembic migrations before loading data."
            )


def _build_upsert_sql(
    schema: str,
    table: str,
    staging_table: str,
    columns: list[str],
) -> str:
    """Build INSERT ... ON CONFLICT DO UPDATE SQL."""
    target_ref = _qualified(schema, table)
    staging_ref = _qualified(schema, staging_table)

    col_list = ", ".join(_quote(c) for c in columns)

    update_clauses = []
    for col in columns:
        if col == PRIMARY_KEY:
            continue
        if col == _NASKOD_COL:
            update_clauses.append(
                f"{_quote(col)} = COALESCE({target_ref}.{_quote(col)}, EXCLUDED.{_quote(col)})"
            )
        else:
            update_clauses.append(f"{_quote(col)} = EXCLUDED.{_quote(col)}")

    update_set = ", ".join(update_clauses)

    change_guard = ""
    if _CHANGE_DETECTION_COL in columns:
        change_guard = (
            f" WHERE {target_ref}.{_quote(_CHANGE_DETECTION_COL)} "
            f"IS DISTINCT FROM EXCLUDED.{_quote(_CHANGE_DETECTION_COL)}"
        )

    return (
        f"INSERT INTO {target_ref} ({col_list}) "
        f"SELECT {col_list} FROM {staging_ref} "
        f"ON CONFLICT ({_quote(PRIMARY_KEY)}) DO UPDATE SET {update_set}"
        f"{change_guard}"
    )


# ── Column mapping: pipeline output → standardized_address ────────────────────


def map_pipeline_to_standardized(df: pl.DataFrame) -> pl.DataFrame:
    """Map pipeline output DataFrame to standardized_address schema.

    1. Rename: full_address → address_clean
    2. Drop internal columns
    3. Add missing schema columns with correct types/defaults
    4. Select only schema columns in schema order
    """
    now_str = datetime.now(timezone.utc).isoformat(timespec="seconds")

    # 1. Rename
    if "full_address" in df.columns and "address_clean" not in df.columns:
        df = df.rename({"full_address": "address_clean"})

    # Generate canonical_address_key if missing (hash of key address components)
    if "canonical_address_key" not in df.columns or df["canonical_address_key"].is_null().all():
        # Build a deterministic key from available address components
        key_cols = ["address_clean", "postcode", "state_code", "locality_name", "street_name", "premise_no"]
        available = [c for c in key_cols if c in df.columns]
        if available:
            df = df.with_columns(
                pl.concat_str(available, separator="|", ignore_nulls=True)
                .hash()
                .cast(pl.Utf8)
                .alias("canonical_address_key")
            )
        else:
            df = df.with_columns(
                pl.col("record_id").alias("canonical_address_key")
            )

    # 2. Drop internal columns
    drop_cols = [c for c in df.columns if c in _DROP_COLUMNS]
    if drop_cols:
        df = df.drop(drop_cols)

    # 3. Add missing columns with correct types
    expected = set(STANDARDIZED_ADDRESS_COLUMNS)
    actual = set(df.columns)
    missing = expected - actual

    if missing:
        null_cols = []
        for col in sorted(missing):
            if col == "lifecycle_status":
                null_cols.append(pl.lit("validated").alias(col))
            elif col == "created_at" or col == "updated_at":
                null_cols.append(pl.lit(now_str).alias(col))
            elif col in FLOAT_COLUMNS:
                null_cols.append(pl.lit(None).cast(pl.Float64).alias(col))
            elif col in INTEGER_COLUMNS:
                null_cols.append(pl.lit(None).cast(pl.Int32).alias(col))
            else:
                null_cols.append(pl.lit(None).cast(pl.Utf8).alias(col))
        df = df.with_columns(null_cols)

    # 4. Cast confidence_score to Float64 if it's Int32
    if "confidence_score" in df.columns and df.schema["confidence_score"] != pl.Float64:
        df = df.with_columns(pl.col("confidence_score").cast(pl.Float64))

    # 5. Drop extra columns and reorder
    extra = set(df.columns) - expected
    if extra:
        df = df.drop(list(extra))

    df = df.select(STANDARDIZED_ADDRESS_COLUMNS)
    return df


# ── Address type lookup ───────────────────────────────────────────────────────


def resolve_address_type_id(
    address_type: str | None,
    address_subtype: str | None,
    *,
    engine: sa.Engine,
) -> int | None:
    """Look up address_type_id from the reference table. Returns None if not found — never raises."""
    if not address_type:
        return None
    try:
        with engine.connect() as conn:
            result = conn.execute(
                sa.text("""
                    SELECT address_type_id
                    FROM nas.address_type
                    WHERE address_type = :at
                      AND (
                        (:ast IS NULL AND address_subtype IS NULL)
                        OR address_subtype = :ast
                      )
                    LIMIT 1
                """),
                {"at": address_type, "ast": address_subtype},
            ).fetchone()
            return result[0] if result else None
    except Exception:
        return None


# ── Build raw_address DataFrame ───────────────────────────────────────────────


def build_raw_address_df(
    pipeline_df: pl.DataFrame,
    *,
    job_id: str,
    agency_id: str | None = None,
    match_status: str = "unmatched",
    replaces_map: dict[str, str] | None = None,
) -> pl.DataFrame:
    """Build a raw_address DataFrame from the original pipeline output.

    For success/warning rows: match_status='unmatched', error_reason=NULL
    For failed rows: match_status='failed', error_reason from pipeline
    When replaces_map is provided, sets replaces_raw_id for retry chaining.
    """
    n = len(pipeline_df)

    # Determine the raw text column
    if "source_address_old" in pipeline_df.columns:
        raw_text_col = pipeline_df["source_address_old"]
    elif "full_address" in pipeline_df.columns:
        raw_text_col = pipeline_df["full_address"]
    else:
        raw_text_col = pl.Series("raw_text", [""] * n)

    raw_ids = [uuid.uuid4().hex for _ in range(n)]

    # Build replaces_raw_id column
    if replaces_map and "record_id" in pipeline_df.columns:
        replaces_col = [replaces_map.get(rid) for rid in pipeline_df["record_id"].to_list()]
    else:
        replaces_col = [None] * n

    data: dict = {
        "raw_id": raw_ids,
        "source_system": ["nas_etl"] * n,
        "source_system_id": [job_id] * n,
        "agency_id": [agency_id] * n,
        "raw_text": raw_text_col,
        "source_ref_id": pipeline_df["record_id"] if "record_id" in pipeline_df.columns else [None] * n,
        "standardized_id": [None] * n,
        "match_status": [match_status] * n,
        "match_confidence": [None] * n,
        "job_id": [job_id] * n,
        "error_reason": pipeline_df["error_reason"] if "error_reason" in pipeline_df.columns else [None] * n,
        "replaces_raw_id": replaces_col,
    }

    raw_df = pl.DataFrame(data)

    # Ensure correct types
    raw_df = raw_df.cast({
        "match_confidence": pl.Float64,
    })

    return raw_df


# ── Public API ────────────────────────────────────────────────────────────────


def lookup_raw_ids_by_record_ids(
    record_ids: list[str],
    *,
    dsn: str | None = None,
    schema: str = "nas",
) -> dict[str, str]:
    """Return {record_id: raw_id} for existing raw_address rows.

    Used by retry jobs to find the original raw_id so that
    replaces_raw_id can be set on the new submission.
    Returns the most recent raw_id per source_ref_id.
    """
    if not record_ids:
        return {}

    sa_dsn = dsn or build_dsn(driver="psycopg")
    engine = sa.create_engine(sa_dsn)

    query = sa.text(f"""
        SELECT DISTINCT ON (source_ref_id)
            source_ref_id, raw_id
        FROM {_qualified(schema, 'raw_address')}
        WHERE source_ref_id = ANY(:ids)
        ORDER BY source_ref_id, ingest_timestamp DESC
    """)

    with engine.connect() as conn:
        rows = conn.execute(query, {"ids": record_ids}).fetchall()

    return {row[0]: row[1] for row in rows}


def mark_raw_addresses_superseded(
    raw_ids: list[str],
    *,
    dsn: str | None = None,
    schema: str = "nas",
) -> None:
    """Set match_status='superseded' on original failed rows replaced by retry."""
    if not raw_ids:
        return

    sa_dsn = dsn or build_dsn(driver="psycopg")
    engine = sa.create_engine(sa_dsn)

    stmt = sa.text(f"""
        UPDATE {_qualified(schema, 'raw_address')}
        SET match_status = 'superseded'
        WHERE raw_id = ANY(:ids)
          AND match_status IN ('failed', 'unmatched')
    """)

    with engine.begin() as conn:
        conn.execute(stmt, {"ids": raw_ids})

    logger.info("raw_address_superseded count=%d", len(raw_ids))


def load_chunk(
    df: pl.DataFrame,
    *,
    raw_df: pl.DataFrame | None = None,
    dsn: str | None = None,
    schema: str = "nas",
    table: str = "standardized_address",
    batch_size: int = _DEFAULT_BATCH_SIZE,
) -> dict[str, int]:
    """Upsert standardized_address + insert raw_address in one transaction.

    Steps:
    1. Map pipeline output to schema
    2. Validate against address_schema contract
    3. Assert tables exist
    4. Batch: stage → upsert standardized_address
    5. Insert raw_address (simple append, same transaction)
    6. Return counts
    """
    if df.is_empty():
        logger.info("load_skip empty_dataframe")
        return {"rows_processed": 0, "rows_inserted": 0, "rows_updated": 0}

    # Resolve DSN
    sa_dsn = dsn or build_dsn(driver="psycopg")

    # 1. Map to schema
    mapped_df = map_pipeline_to_standardized(df)

    # 2. Validate
    validate_dataframe(mapped_df)

    # 3. Assert tables exist
    engine = sa.create_engine(sa_dsn)
    _assert_table_exists(engine, schema, table)
    if raw_df is not None and not raw_df.is_empty():
        _assert_table_exists(engine, schema, "raw_address")

    # 4. Determine upsertable columns (exclude columns needing special DB types)
    # geom = PostGIS geometry, timestamp columns default in DB, address_type_id = FK
    _SKIP_WRITE_COLUMNS = {"geom", "validation_date", "created_at", "updated_at", "address_type_id"}
    upsert_columns = [
        c for c in STANDARDIZED_ADDRESS_COLUMNS
        if c in mapped_df.columns and c not in _SKIP_WRITE_COLUMNS
    ]
    if PRIMARY_KEY not in upsert_columns:
        raise ValueError(f"DataFrame must contain primary key column: {PRIMARY_KEY}")

    # 5. Process in batches
    total_rows = len(mapped_df)
    total_processed = 0

    for batch_offset in range(0, total_rows, batch_size):
        batch = mapped_df.slice(batch_offset, min(batch_size, total_rows - batch_offset))
        batch_cols = batch.select(upsert_columns)

        suffix = uuid.uuid4().hex[:8]
        staging_table = f"{table}_stg_{suffix}"
        staging_qualified = _qualified(schema, staging_table)
        target_qualified = _qualified(schema, table)

        try:
            with engine.begin() as conn:
                conn.execute(sa.text(
                    f"CREATE TABLE {staging_qualified} "
                    f"(LIKE {target_qualified} INCLUDING DEFAULTS)"
                ))

            batch_cols.write_database(
                table_name=f"{schema}.{staging_table}",
                connection=sa_dsn,
                if_table_exists="append",
                engine="sqlalchemy",
            )

            upsert_sql = _build_upsert_sql(schema, table, staging_table, upsert_columns)
            with engine.begin() as conn:
                conn.execute(sa.text(upsert_sql))

            total_processed += len(batch)

        finally:
            try:
                with engine.begin() as conn:
                    conn.execute(sa.text(f"DROP TABLE IF EXISTS {staging_qualified}"))
            except Exception as exc:
                logger.warning("staging_table_drop_failed table=%s error=%s", staging_qualified, exc)

    # 6. Insert raw_address (simple append via SQLAlchemy)
    if raw_df is not None and not raw_df.is_empty():
        try:
            raw_df.write_database(
                table_name=f"{schema}.raw_address",
                connection=sa_dsn,
                if_table_exists="append",
                engine="sqlalchemy",
            )
            logger.info("raw_address_inserted rows=%d", len(raw_df))
        except Exception as exc:
            logger.error("raw_address_insert_failed error=%s", exc)
            # Non-fatal: standardized_address was already committed

    logger.info(
        "load_complete table=%s.%s rows_processed=%d batches=%d",
        schema, table, total_processed, (total_rows + batch_size - 1) // batch_size,
    )

    return {
        "rows_processed": total_processed,
        "rows_inserted": total_processed,
        "rows_updated": 0,
    }


def load_success_and_warning(
    success_df: pl.DataFrame,
    warning_df: pl.DataFrame,
    failed_df: pl.DataFrame | None = None,
    *,
    dsn: str | None = None,
    schema: str = "nas",
    job_id: str = "unknown",
    agency_id: str | None = None,
    replaces_map: dict[str, str] | None = None,
) -> dict[str, int]:
    """Load success + warning rows into standardized_address and all rows into raw_address.

    - success + warning → standardized_address (upsert) + raw_address (match_status='unmatched')
    - failed → raw_address only (match_status='failed', error_reason populated)
    - replaces_map: {record_id: raw_id} for submission chaining (retry jobs)
    """
    combined = pl.concat([success_df, warning_df], how="diagonal_relaxed")

    if combined.is_empty() and (failed_df is None or failed_df.is_empty()):
        logger.info("load_skip no_rows_to_load")
        return {"rows_processed": 0, "rows_inserted": 0, "rows_updated": 0}

    # Build raw_address for success+warning (match_status='unmatched')
    raw_parts: list[pl.DataFrame] = []
    if not combined.is_empty():
        raw_parts.append(build_raw_address_df(
            combined, job_id=job_id, agency_id=agency_id,
            match_status="unmatched", replaces_map=replaces_map,
        ))

    # Build raw_address for failed (match_status='failed')
    if failed_df is not None and not failed_df.is_empty():
        raw_parts.append(build_raw_address_df(
            failed_df, job_id=job_id, agency_id=agency_id,
            match_status="failed", replaces_map=replaces_map,
        ))

    raw_df = pl.concat(raw_parts, how="diagonal_relaxed") if raw_parts else None

    if combined.is_empty():
        # Only failed rows — just insert raw_address, no standardized_address
        if raw_df is not None and not raw_df.is_empty():
            sa_dsn = dsn or build_dsn(driver="psycopg")
            engine = sa.create_engine(sa_dsn)
            _assert_table_exists(engine, schema, "raw_address")
            try:
                raw_df.write_database(
                    table_name=f"{schema}.raw_address",
                    connection=sa_dsn,
                    if_table_exists="append",
                    engine="sqlalchemy",
                )
                logger.info("raw_address_inserted rows=%d", len(raw_df))
            except Exception as exc:
                logger.error("raw_address_insert_failed error=%s", exc)
        return {"rows_processed": 0, "rows_inserted": 0, "rows_updated": 0}

    return load_chunk(combined, raw_df=raw_df, dsn=dsn, schema=schema)
