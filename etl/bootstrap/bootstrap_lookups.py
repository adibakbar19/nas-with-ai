"""Bootstrap nas_lookup schema: lookup tables and PostGIS boundary tables.

Run once before starting the API/worker services. Safe to re-run — all tables
are replaced in a single pass. The lookup_version table is written at the end
so bootstrap_lookups_if_needed.py can skip on subsequent container starts.

Usage:
    python -m etl.bootstrap.bootstrap_lookups
"""

from __future__ import annotations

import os
from pathlib import Path

import geopandas as gpd
import pandas as pd
import sqlalchemy as sa
from sqlalchemy import text

PROJECT_ROOT = Path(__file__).resolve().parents[2]
LOOKUPS_DIR = PROJECT_ROOT / "data" / "lookups_clean"
BOUNDARY_DIR = PROJECT_ROOT / "data" / "boundary"

_LOOKUP_KEYS = [
    "state", "district", "mukim", "postcode",
    "locality", "sublocality", "district_aliases",
    "street_type", "street_type_alias", "street_name",
    "state_boundary", "district_boundary", "mukim_boundary",
    "postcode_boundary", "pbt",
]


def _db_url() -> str:
    url = os.getenv("DATABASE_URL", "").strip()
    if url:
        if url.startswith("postgresql://"):
            return url.replace("postgresql://", "postgresql+psycopg://", 1)
        return url
    host = os.getenv("PGHOST", "localhost")
    port = os.getenv("PGPORT", "5432")
    database = os.getenv("PGDATABASE", "postgres")
    user = os.getenv("PGUSER", "postgres")
    password = os.getenv("PGPASSWORD", "postgres")
    return f"postgresql+psycopg://{user}:{password}@{host}:{port}/{database}"


def _schema() -> str:
    return (os.getenv("LOOKUP_SCHEMA") or os.getenv("PGSCHEMA") or "nas_lookup").strip() or "nas_lookup"


def _load_boundary(path: Path, col_map: dict[str, str]) -> gpd.GeoDataFrame:
    gdf = gpd.read_file(path).to_crs("EPSG:4326")
    renamed = {src: dst for src, dst in col_map.items() if src in gdf.columns}
    gdf = gdf.rename(columns=renamed)
    keep = list(col_map.values()) + ["geometry"]
    return gpd.GeoDataFrame(gdf[[c for c in keep if c in gdf.columns]], geometry="geometry", crs="EPSG:4326")


def _load_lookup_tables(engine: sa.Engine, schema: str) -> None:
    print("Loading lookup tables...")
    with engine.begin() as conn:
        pd.read_csv(LOOKUPS_DIR / "state.csv", dtype=str).to_sql(
            "state", conn, schema=schema, if_exists="replace", index=False
        )
        pd.read_csv(LOOKUPS_DIR / "district.csv", dtype=str).to_sql(
            "district", conn, schema=schema, if_exists="replace", index=False
        )
        mukim_df = pd.read_csv(LOOKUPS_DIR / "mukim.csv", dtype=str)
        mukim_df.to_sql("mukim", conn, schema=schema, if_exists="replace", index=False)
        pd.read_csv(LOOKUPS_DIR / "postcode.csv", dtype=str).to_sql(
            "postcode", conn, schema=schema, if_exists="replace", index=False
        )
        pd.read_csv(LOOKUPS_DIR / "district_aliases.csv", dtype=str).to_sql(
            "district_aliases", conn, schema=schema, if_exists="replace", index=False
        )
        pd.read_csv(LOOKUPS_DIR / "locality.csv", dtype=str).to_sql(
            "locality_lookup", conn, schema=schema, if_exists="replace", index=False
        )
        pd.read_csv(LOOKUPS_DIR / "sublocality.csv", dtype=str).to_sql(
            "sublocality_lookup", conn, schema=schema, if_exists="replace", index=False
        )
        pd.read_csv(LOOKUPS_DIR / "street_type.csv", dtype=str).to_sql(
            "street_type", conn, schema=schema, if_exists="replace", index=False
        )
        pd.read_csv(LOOKUPS_DIR / "street_type_alias.csv", dtype=str).to_sql(
            "street_type_alias", conn, schema=schema, if_exists="replace", index=False
        )
        pd.read_csv(LOOKUPS_DIR / "street_name.csv", dtype=str).to_sql(
            "street_name", conn, schema=schema, if_exists="replace", index=False
        )
    print("  state, district, mukim, postcode, district_aliases, locality, sublocality,")
    print("  street_type, street_type_alias, street_name — done")
    return mukim_df


def _load_state_boundary(engine: sa.Engine, schema: str) -> None:
    print("Loading state boundary...")
    gdf = _load_boundary(
        BOUNDARY_DIR / "state_boundary.geojson",
        {"STATE_CODE": "state_code", "STATE": "state_name"},
    )
    gdf["state_name"] = gdf["state_name"].str.strip().str.upper()
    gdf = gdf.rename_geometry("boundary_geom")
    gdf.to_postgis("state_boundary", engine, schema=schema, if_exists="replace", index=False)
    print(f"  {len(gdf)} state polygons — done")


def _load_district_boundary(engine: sa.Engine, schema: str) -> None:
    print("Loading district boundary...")
    gdf = _load_boundary(
        BOUNDARY_DIR / "district_boundary.geojson",
        {"STATE_CODE": "state_code", "DISTRICT": "district_name", "DIVISION_C": "district_code"},
    )
    gdf["district_name"] = gdf["district_name"].str.strip().str.upper()
    gdf["district_code"] = gdf["district_code"].str.strip().str.zfill(2)
    gdf = gdf.rename_geometry("boundary_geom")
    gdf.to_postgis("district_boundary", engine, schema=schema, if_exists="replace", index=False)
    print(f"  {len(gdf)} district polygons — done")


def _load_mukim_boundary(engine: sa.Engine, schema: str, mukim_df: pd.DataFrame) -> None:
    print("Loading mukim boundary...")
    # mukim_code and mukim_id are not in the GeoJSON — join from lookup CSV
    gdf = _load_boundary(
        BOUNDARY_DIR / "mukim_boundary.geojson",
        {"STATE_CODE": "state_code", "DIVISION": "district_name", "MUKIM": "mukim_name"},
    )
    gdf["mukim_name"] = gdf["mukim_name"].str.strip().str.upper()
    gdf["district_name"] = gdf["district_name"].str.strip().str.upper()

    lookup = mukim_df[["state_code", "mukim_name", "mukim_code", "mukim_id"]].copy()
    lookup["state_code"] = lookup["state_code"].str.strip()
    lookup["mukim_name"] = lookup["mukim_name"].str.strip().str.upper()

    merged = gdf.merge(lookup, on=["state_code", "mukim_name"], how="left")
    unmatched = merged["mukim_code"].isna().sum()
    if unmatched:
        print(f"  WARNING: {unmatched} mukim polygons could not be matched to a mukim_code")

    result = gpd.GeoDataFrame(merged, geometry=gdf.geometry.name, crs="EPSG:4326").rename_geometry("boundary_geom")
    result.to_postgis("mukim_boundary", engine, schema=schema, if_exists="replace", index=False)
    print(f"  {len(result)} mukim polygons — done")


def _load_postcode_boundary(engine: sa.Engine, schema: str) -> None:
    print("Loading postcode boundary...")
    gdf = _load_boundary(
        BOUNDARY_DIR / "postcode_boundary.geojson",
        {"POSTCODE": "postcode", "TOWN_CITY": "city", "STATE": "state"},
    )
    gdf["city"] = gdf["city"].str.strip().str.upper()
    gdf["state"] = gdf["state"].str.strip().str.upper()
    gdf = gdf.rename_geometry("boundary_geom")
    gdf.to_postgis("postcode_boundary", engine, schema=schema, if_exists="replace", index=False)
    print(f"  {len(gdf)} postcode polygons — done")


def _load_pbt_boundary(engine: sa.Engine, schema: str) -> None:
    print("Loading PBT boundary...")
    gdf = _load_boundary(
        BOUNDARY_DIR / "pbt_boundary.geojson",
        {"pbt_id": "pbt_id", "pbt_name": "pbt_name"},
    )
    gdf["pbt_id"] = gdf["pbt_id"].str.strip().str.upper()
    gdf["pbt_name"] = gdf["pbt_name"].str.strip().str.upper()
    gdf = gdf.rename_geometry("boundary_geom")
    gdf.to_postgis("pbt", engine, schema=schema, if_exists="replace", index=False)
    print(f"  {len(gdf)} PBT polygons — done")


def _write_lookup_version(engine: sa.Engine, schema: str) -> None:
    rows = pd.DataFrame([
        {"lookup_key": key, "loaded_at": pd.Timestamp.utcnow()}
        for key in _LOOKUP_KEYS
    ])
    with engine.begin() as conn:
        conn.execute(text(
            f'CREATE TABLE IF NOT EXISTS "{schema}".lookup_version '
            f'(lookup_key TEXT PRIMARY KEY, loaded_at TIMESTAMPTZ)'
        ))
    rows.to_sql("lookup_version", engine, schema=schema, if_exists="replace", index=False)


def main() -> None:
    schema = _schema()
    engine = sa.create_engine(_db_url())

    print(f"Bootstrapping schema: {schema}")
    with engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema}"'))
        conn.execute(text("CREATE EXTENSION IF NOT EXISTS postgis"))

    mukim_df = _load_lookup_tables(engine, schema)
    _load_state_boundary(engine, schema)
    _load_district_boundary(engine, schema)
    _load_mukim_boundary(engine, schema, mukim_df)
    _load_postcode_boundary(engine, schema)
    _load_pbt_boundary(engine, schema)
    _write_lookup_version(engine, schema)

    print(f"\nBootstrap complete. Schema '{schema}' is ready.")


if __name__ == "__main__":
    main()
