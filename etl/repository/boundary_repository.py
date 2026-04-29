import os
import re

import sqlalchemy as sa

from .lookup_repository import _db_url_from_config


_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _sql_ident(value: str, *, label: str) -> str:
    cleaned = str(value or "").strip()
    if not _IDENT_RE.fullmatch(cleaned):
        raise ValueError(f"Invalid SQL identifier for {label}: {value!r}")
    return cleaned


def _schema(config: dict) -> str:
    return _sql_ident(
        str(
            config.get("boundary_db_schema")
            or config.get("lookup_db_schema")
            or os.getenv("LOOKUP_SCHEMA")
            or os.getenv("PGSCHEMA")
            or "nas_lookup"
        ).strip()
        or "nas_lookup",
        label="boundary_db_schema",
    )


def _table(config: dict, key: str, default: str) -> str:
    return _sql_ident(str(config.get(key, default)).strip() or default, label=key)


def _read_boundary_table(config: dict, table_name: str, columns: list[str]):
    import geopandas as gpd

    schema = _schema(config)
    table = _sql_ident(table_name, label="boundary table")
    selected = ", ".join(_sql_ident(col, label=f"{table} column") for col in columns)
    sql = f"""
        SELECT {selected}, boundary_geom AS geometry
        FROM {schema}.{table}
        WHERE boundary_geom IS NOT NULL
    """
    engine = sa.create_engine(_db_url_from_config(config, "boundary"))
    with engine.connect() as conn:
        frame = gpd.read_postgis(sql, conn, geom_col="geometry")
    if frame.crs is None:
        frame = frame.set_crs("EPSG:4326", allow_override=True)
    else:
        frame = frame.to_crs("EPSG:4326")
    return frame


def load_postcode_boundaries(config: dict):
    table = _table(config, "postcode_boundary_table", "postcode_boundary")
    return _read_boundary_table(config, table, ["postcode", "city", "state"])


def load_admin_boundaries(config: dict):
    return (
        _read_boundary_table(
            config,
            _table(config, "state_boundary_table", "state_boundary"),
            ["state_code", "state_name"],
        ),
        _read_boundary_table(
            config,
            _table(config, "district_boundary_table", "district_boundary"),
            ["state_code", "district_code", "district_name"],
        ),
        _read_boundary_table(
            config,
            _table(config, "mukim_boundary_table", "mukim_boundary"),
            ["state_code", "district_code", "district_name", "mukim_code", "mukim_name", "mukim_id"],
        ),
    )


def load_pbt_boundaries(config: dict):
    table = _table(config, "pbt_boundary_table", "pbt")
    return _read_boundary_table(config, table, ["pbt_id", "pbt_name"])
