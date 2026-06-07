"""Spatial enrichment stage — PostGIS boundary joins via psycopg.

No GeoPandas. No pandas. Pure Polars + psycopg + PostGIS SQL.

Only processes rows with non-null latitude AND longitude (source coords).
Resolves mukim_id, mukim_code, mukim_name, district_code, district_name,
state_code, pbt_id, pbt_name from the nas_lookup boundary tables.

Rows without coordinates pass through unchanged.
Graceful degradation if PostGIS query fails.
"""

from __future__ import annotations

import logging
from typing import Any

import polars as pl

logger = logging.getLogger(__name__)

# Uses nas_lookup.mukim.geom (added in migration 0008) for mukim + district
# resolution, and nas_lookup.pbt.boundary_geom for PBT resolution.
# district_name is resolved via a join to nas_lookup.district.
_SPATIAL_QUERY = """
SELECT
    r.record_id,
    m.mukim_id::text AS _sp_mukim_id,
    m.mukim_code     AS _sp_mukim_code,
    m.mukim_name     AS _sp_mukim_name,
    m.district_code  AS _sp_district_code,
    d.district_name  AS _sp_district_name,
    m.state_code     AS _sp_state_code,
    p.pbt_id         AS _sp_pbt_id,
    p.pbt_name       AS _sp_pbt_name
FROM (VALUES {placeholders}) AS r(record_id, lat, lng)
LEFT JOIN {lookup_schema}.mukim m
    ON m.geom IS NOT NULL
    AND ST_Contains(
        m.geom,
        ST_SetSRID(ST_Point(r.lng::float8, r.lat::float8), 4326)
    )
LEFT JOIN {lookup_schema}.district d
    ON d.state_code    = m.state_code
    AND d.district_code = m.district_code
LEFT JOIN {lookup_schema}.pbt p
    ON p.boundary_geom IS NOT NULL
    AND ST_Contains(
        p.boundary_geom,
        ST_SetSRID(ST_Point(r.lng::float8, r.lat::float8), 4326)
    )
WHERE r.lat IS NOT NULL AND r.lng IS NOT NULL
"""

_SP_COLUMNS = [
    "_sp_mukim_id",
    "_sp_mukim_code",
    "_sp_mukim_name",
    "_sp_district_code",
    "_sp_district_name",
    "_sp_state_code",
    "_sp_pbt_id",
    "_sp_pbt_name",
]

_TARGET_COLUMNS = {
    "_sp_mukim_id":      "mukim_id",
    "_sp_mukim_code":    "mukim_code",
    "_sp_mukim_name":    "mukim_name",
    "_sp_district_code": "district_code",
    "_sp_district_name": "district_name",
    "_sp_state_code":    "state_code",
    "_sp_pbt_id":        "pbt_id",
    "_sp_pbt_name":      "pbt_name",
}


def _try_cast_float(series: pl.Series) -> pl.Series:
    return series.cast(pl.Float64, strict=False)


def _query_postgis_batch(
    conn,
    records: list[tuple[str, float, float]],
    lookup_schema: str,
) -> list[dict[str, Any]]:
    if not records:
        return []

    placeholders = ", ".join("(%s, %s, %s)" for _ in records)

    params: list[Any] = []
    for record_id, lat, lng in records:
        params.extend([record_id, lat, lng])

    sql = _SPATIAL_QUERY.format(
        placeholders=placeholders,
        lookup_schema=lookup_schema,
    )

    with conn.cursor() as cur:
        cur.execute(sql, params)
        cols = [desc[0] for desc in cur.description]
        rows = cur.fetchall()

    return [dict(zip(cols, row)) for row in rows]


def enrich_spatial(
    df: pl.DataFrame,
    *,
    dsn: str,
    lookup_schema: str = "nas_lookup",
    batch_size: int = 10_000,
) -> pl.DataFrame:
    """Enrich rows with source coordinates via PostGIS boundary joins.

    For rows with non-null latitude AND longitude:
    - Resolves mukim_id, mukim_code, mukim_name from nas_lookup.mukim
    - Resolves district_code, district_name from nas_lookup.district
    - Resolves pbt_id, pbt_name from nas_lookup.pbt
    - Uses pl.coalesce() — never overwrites existing non-null values
    - Sets _spatial_confirmed = True for rows with a mukim match

    For rows without coordinates → unchanged.
    If PostGIS fails → log warning, return df unchanged.
    """
    if "latitude" not in df.columns or "longitude" not in df.columns:
        logger.debug("spatial_skip no_coordinate_columns")
        return df.with_columns(pl.lit(None).cast(pl.Boolean).alias("_spatial_confirmed"))

    df = df.with_columns([
        _try_cast_float(df["latitude"]).alias("_lat_f"),
        _try_cast_float(df["longitude"]).alias("_lng_f"),
    ])

    has_coords = (
        pl.col("_lat_f").is_not_null()
        & pl.col("_lng_f").is_not_null()
    )
    coord_count = df.filter(has_coords).height

    if coord_count == 0:
        logger.debug("spatial_skip no_valid_coordinates")
        return df.drop(["_lat_f", "_lng_f"]).with_columns(
            pl.lit(None).cast(pl.Boolean).alias("_spatial_confirmed")
        )

    logger.info(
        "spatial_start coordinate_rows=%d total_rows=%d",
        coord_count, len(df),
    )

    coord_df = df.filter(has_coords).select(["record_id", "_lat_f", "_lng_f"])
    all_results: list[dict[str, Any]] = []

    try:
        import psycopg

        # psycopg.connect needs libpq DSN (postgresql://...), not SQLAlchemy
        # driver-qualified URL (postgresql+psycopg://...)
        psycopg_dsn = dsn.replace("postgresql+psycopg://", "postgresql://")
        conn = psycopg.connect(psycopg_dsn)
        try:
            for offset in range(0, coord_count, batch_size):
                batch = coord_df.slice(offset, batch_size)
                records = [
                    (row["record_id"], row["_lat_f"], row["_lng_f"])
                    for row in batch.to_dicts()
                ]
                batch_results = _query_postgis_batch(conn, records, lookup_schema)
                all_results.extend(batch_results)
                logger.info(
                    "spatial_batch offset=%d size=%d matches=%d",
                    offset, len(records), len(batch_results),
                )
        finally:
            conn.close()
    except Exception as exc:
        logger.warning(
            "spatial_enrichment_failed error=%s returning_df_unchanged=true", exc
        )
        return df.drop(["_lat_f", "_lng_f"]).with_columns(
            pl.lit(None).cast(pl.Boolean).alias("_spatial_confirmed")
        )

    if not all_results:
        logger.info(
            "spatial_complete no_boundary_matches coord_rows=%d", coord_count,
        )
        return df.drop(["_lat_f", "_lng_f"]).with_columns(
            pl.lit(None).cast(pl.Boolean).alias("_spatial_confirmed")
        )

    results_df = (
        pl.DataFrame(all_results)
        .with_columns(pl.all().cast(pl.Utf8))
        .with_columns(pl.lit(True).alias("_spatial_confirmed"))
        # A point can fall in multiple mukim polygons (boundary edge cases) — keep first match
        .unique(subset=["record_id"], keep="first")
    )

    # Ensure all target columns exist in df before coalesce
    for target_col in _TARGET_COLUMNS.values():
        if target_col not in df.columns:
            df = df.with_columns(pl.lit(None).cast(pl.Utf8).alias(target_col))

    if "_spatial_confirmed" not in df.columns:
        df = df.with_columns(pl.lit(None).cast(pl.Boolean).alias("_spatial_confirmed"))

    df = df.join(results_df, on="record_id", how="left", suffix="_sp_new")

    coalesce_exprs = []
    for sp_col, target_col in _TARGET_COLUMNS.items():
        if sp_col in df.columns:
            coalesce_exprs.append(
                pl.coalesce([
                    pl.col(target_col),
                    pl.col(sp_col),
                ]).alias(target_col)
            )

    sp_confirmed_new = (
        "_spatial_confirmed_sp_new"
        if "_spatial_confirmed_sp_new" in df.columns
        else None
    )
    if sp_confirmed_new:
        coalesce_exprs.append(
            pl.coalesce([
                pl.col("_spatial_confirmed"),
                pl.col(sp_confirmed_new),
            ]).alias("_spatial_confirmed")
        )

    df = df.with_columns(coalesce_exprs)

    drop_cols = ["_lat_f", "_lng_f"] + [
        c for c in df.columns
        if c in _SP_COLUMNS or c.endswith("_sp_new")
    ]
    df = df.drop([c for c in drop_cols if c in df.columns])

    matched = df.filter(pl.col("_spatial_confirmed") == True).height  # noqa: E712
    logger.info(
        "spatial_complete coord_rows=%d matched=%d unmatched=%d",
        coord_count, matched, coord_count - matched,
    )

    return df
