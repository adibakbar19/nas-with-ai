import os
import re
from dataclasses import dataclass

import pandas as pd


DataFrame = pd.DataFrame
_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


@dataclass(frozen=True)
class LookupFrames:
    state: DataFrame
    district: DataFrame
    mukim: DataFrame
    postcode: DataFrame
    district_alias: DataFrame | None = None
    locality: DataFrame | None = None
    sublocality: DataFrame | None = None


def _db_url_from_config(config: dict, prefix: str = "lookup") -> str:
    url = str(config.get(f"{prefix}_database_url", "") or config.get(f"{prefix}_sqlalchemy_url", "")).strip()
    if url:
        if url.startswith("postgresql://"):
            return url.replace("postgresql://", "postgresql+psycopg://", 1)
        return url
    default_prefix = "lookup" if prefix != "lookup" else prefix
    host = config.get(f"{prefix}_db_host", config.get(f"{default_prefix}_db_host", os.getenv("PGHOST", "localhost")))
    port = str(config.get(f"{prefix}_db_port", config.get(f"{default_prefix}_db_port", os.getenv("PGPORT", "5432"))))
    database = config.get(f"{prefix}_db_name", config.get(f"{default_prefix}_db_name", os.getenv("PGDATABASE", "postgres")))
    user = str(config.get(f"{prefix}_db_user", config.get(f"{default_prefix}_db_user", os.getenv("PGUSER", "")))).strip()
    password = str(
        config.get(f"{prefix}_db_password", config.get(f"{default_prefix}_db_password", os.getenv("PGPASSWORD", "")))
    ).strip()
    if not user or not password:
        raise ValueError(f"{prefix}_source=db requires {prefix}_db_user/password or PGUSER/PGPASSWORD")
    return f"postgresql+psycopg://{user}:{password}@{host}:{port}/{database}"


def _sql_ident(value: str, *, label: str) -> str:
    cleaned = str(value or "").strip()
    if not _IDENT_RE.fullmatch(cleaned):
        raise ValueError(f"Invalid SQL identifier for {label}: {value!r}")
    return cleaned


def load_lookup_frames(*, config: dict) -> LookupFrames:
    import sqlalchemy as sa

    source = str(config.get("lookup_source", "db")).strip().lower()
    if source != "db":
        raise ValueError("Production ETL requires lookup_source=db. File-based lookup mode is not supported.")

    schema = _sql_ident(
        str(config.get("lookup_db_schema", os.getenv("LOOKUP_SCHEMA", os.getenv("PGSCHEMA", "nas_lookup")))).strip()
        or "nas_lookup",
        label="lookup_db_schema",
    )
    engine = sa.create_engine(_db_url_from_config(config, "lookup"))
    with engine.connect() as conn:
        return LookupFrames(
            state=pd.read_sql(f"SELECT state_code, state_name FROM {schema}.state", conn, dtype=str),
            district=pd.read_sql(
                f"SELECT state_code, district_code, district_name FROM {schema}.district",
                conn,
                dtype=str,
            ),
            mukim=pd.read_sql(
                f"SELECT state_code, state_name, district_code, district_name, mukim_code, mukim_name, mukim_id FROM {schema}.mukim",
                conn,
                dtype=str,
            ),
            postcode=pd.read_sql(f"SELECT postcode, city, state FROM {schema}.postcode", conn, dtype=str),
            district_alias=pd.read_sql(
                f"SELECT state_code, district_code, district_alias FROM {schema}.district_aliases",
                conn,
                dtype=str,
            ),
            locality=pd.read_sql(
                f"SELECT locality_name, state_name FROM {schema}.locality_lookup",
                conn,
                dtype=str,
            ),
            sublocality=pd.read_sql(
                f"SELECT sub_locality_name, state_name FROM {schema}.sublocality_lookup",
                conn,
                dtype=str,
            ),
        )
