"""DB-backed lookup table loader — returns Polars DataFrames."""

import logging
import os
import re
from dataclasses import dataclass

import polars as pl

from nas_config.db import build_dsn


_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class LookupFrames:
    state: pl.DataFrame
    district: pl.DataFrame
    mukim: pl.DataFrame
    postcode: pl.DataFrame
    district_alias: pl.DataFrame | None = None
    locality: pl.DataFrame | None = None
    sublocality: pl.DataFrame | None = None


def _sql_ident(value: str, *, label: str) -> str:
    cleaned = str(value or "").strip()
    if not _IDENT_RE.fullmatch(cleaned):
        raise ValueError(f"Invalid SQL identifier for {label}: {value!r}")
    return cleaned


def _read_sql(dsn: str, sql: str) -> pl.DataFrame:
    """Read a SQL query into a Polars DataFrame with all columns cast to Utf8."""
    import sqlalchemy as sa
    engine = sa.create_engine(dsn)
    with engine.connect() as conn:
        df = pl.read_database(sql, connection=conn)
    return df.with_columns(pl.all().cast(pl.Utf8))


def _try_read_sql(dsn: str, sql: str) -> pl.DataFrame | None:
    """Read a SQL query, returning None if the table does not exist or query fails."""
    try:
        df = _read_sql(dsn, sql)
        return df if not df.is_empty() else None
    except Exception as exc:
        logger.debug("optional lookup query failed: %s", exc)
        return None


def load_lookup_frames(*, config: dict) -> LookupFrames:
    """Load all lookup reference tables from Postgres as Polars DataFrames.

    All columns are cast to pl.Utf8 for consistent string-typed matching.
    Optional tables (district_alias, locality, sublocality) return None
    if the table does not exist or is empty.
    """
    source = str(config.get("lookup_source", "db")).strip().lower()
    if source != "db":
        raise ValueError("Production ETL requires lookup_source=db. File-based lookup mode is not supported.")

    schema = _sql_ident(
        str(config.get("lookup_db_schema", os.getenv("LOOKUP_SCHEMA", os.getenv("PGSCHEMA", "nas_lookup")))).strip()
        or "nas_lookup",
        label="lookup_db_schema",
    )

    dsn = build_dsn(driver="psycopg")

    return LookupFrames(
        state=_read_sql(dsn, f"SELECT state_code, state_name FROM {schema}.state"),
        district=_read_sql(
            dsn,
            f"SELECT state_code, district_code, district_name FROM {schema}.district",
        ),
        mukim=_read_sql(
            dsn,
            f"""
            SELECT m.state_code, st.state_name, m.district_code, d.district_name,
                   m.mukim_code, m.mukim_name, m.mukim_id
            FROM {schema}.mukim m
            LEFT JOIN {schema}.state st ON st.state_code = m.state_code
            LEFT JOIN {schema}.district d ON d.district_code = m.district_code
              AND d.state_code = m.state_code
            """,
        ),
        postcode=_read_sql(dsn, f"SELECT postcode, city, state FROM {schema}.postcode"),
        district_alias=_try_read_sql(
            dsn,
            f"SELECT state_code, district_code, district_alias FROM {schema}.district_aliases",
        ),
        locality=_try_read_sql(
            dsn,
            f"SELECT locality_name, state_name FROM {schema}.locality_lookup",
        ),
        sublocality=_try_read_sql(
            dsn,
            f"SELECT sub_locality_name, state_name FROM {schema}.sublocality_lookup",
        ),
    )
