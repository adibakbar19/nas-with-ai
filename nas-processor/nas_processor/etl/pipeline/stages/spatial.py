"""Spatial enrichment stage — PostGIS boundary joins via psycopg.

No GeoPandas. No pandas. Pure Polars + psycopg + PostGIS SQL.

Only processes rows with non-null latitude AND longitude.
Rows without coordinates pass through unchanged.
Graceful degradation if boundaries table missing.
"""

from __future__ import annotations

import logging
from typing import Any

import polars as pl

logger = logging.getLogger(__name__)

_SPATIAL_QUERY = """
SELECT
    r.record_id,
    b.state_code    AS _sp_state_code,
    b.district_code AS _sp_district_code,
    b.mukim_code    AS _sp_mukim_code,
    b.pbt_id        AS _sp_pbt_id,
    b.pbt_name      AS _sp_pbt_name
FROM (VALUES {placeholders}) AS r(record_id, lat, lng)
LEFT JOIN {lookup_schema}.boundaries b
    ON ST_Contains(
        b.geom,
        ST_SetSRID(ST_Point(r.lng::float8, r.lat::float8), 4326)
    )
WHERE r.lat IS NOT NULL AND r.lng IS NOT NULL
"""

_SP_COLUMNS = [
    "_sp_state_code",
    "_sp_district_code",
    "_sp_mukim_code",
    "_sp_pbt_id",
    "_sp_pbt_name",
]

_TARGET_COLUMNS = {
    "_sp_state_code":    "state_code",
    "_sp_district_code": "district_code",
    "_sp_mukim_code":    "mukim_code",
    "_sp_pbt_id":        "pbt_id",
    "_sp_pbt_name":      "pbt_name",
}


def _try_cast_float(series: pl.Series) -> pl.Series:
    """Attempt to cast a Series to Float64. Returns null for invalid values."""
    return series.cast(pl.Float64, strict=False)


def _query_postgis_batch(
    conn,
    records: list[tuple[str, float, float]],
    lookup_schema: str,
) -> list[dict[str, Any]]:
    """Query PostGIS for a batch of (record_id, lat, lng) tuples.

    Returns list of dicts with spatial enrichment results.
    """
    if not records:
        return []

    # Build VALUES placeholders: (%s, %s, %s), (%s, %s, %s), ...
    placeholders = ", ".join("(%s, %s, %s)" for _ in records)

    # Flatten records into a single list for psycopg
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
    """Enrich rows with coordinates via PostGIS boundary joins.

    For rows with non-null latitude AND longitude:
    - Queries PostGIS boundaries table
    - Fills state_code, district_code, mukim_code, pbt_id, pbt_name
    - Uses pl.coalesce() — never overwrites existing non-null values
    - Sets _spatial_confirmed = True for rows with a spatial match

    For rows without coordinates → unchanged.
    If PostGIS fails → log warning, return df unchanged.
    """
    # Check if lat/lng columns exist at all
    if "latitude" not in df.columns or "longitude" not in df.columns:
        logger.debug("spatial_skip no_coordinate_columns")
        return df.with_columns(pl.lit(None).cast(pl.Boolean).alias("_spatial_confirmed"))

    # Cast lat/lng to Float64 — may be Utf8 from extraction
    df = df.with_columns([
        _try_cast_float(df["latitude"]).alias("_lat_f"),
        _try_cast_float(df["longitude"]).alias("_lng_f"),
    ])

    # Identify rows with valid coordinates
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

    # Extract coordinate rows for batching
    coord_df = df.filter(has_coords).select(["record_id", "_lat_f", "_lng_f"])

    # Build batch records and query PostGIS
    all_results: list[dict[str, Any]] = []

    try:
        import psycopg
        conn = psycopg.connect(dsn)
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
        # Graceful degradation — boundaries table may not be loaded yet
        logger.warning(
            "spatial_enrichment_failed error=%s "
            "returning_df_unchanged=true", exc
        )
        return df.drop(["_lat_f", "_lng_f"]).with_columns(
            pl.lit(None).cast(pl.Boolean).alias("_spatial_confirmed")
        )

    # Build results DataFrame
    if not all_results:
        logger.info(
            "spatial_complete no_boundary_matches coord_rows=%d",
            coord_count,
        )
        return df.drop(["_lat_f", "_lng_f"]).with_columns(
            pl.lit(None).cast(pl.Boolean).alias("_spatial_confirmed")
        )

    results_df = (
        pl.DataFrame(all_results)
        .with_columns(pl.all().cast(pl.Utf8))
        .with_columns(pl.lit(True).alias("_spatial_confirmed"))
    )

    # Ensure target columns exist in df
    for target_col in _TARGET_COLUMNS.values():
        if target_col not in df.columns:
            df = df.with_columns(pl.lit(None).cast(pl.Utf8).alias(target_col))

    if "_spatial_confirmed" not in df.columns:
        df = df.with_columns(pl.lit(None).cast(pl.Boolean).alias("_spatial_confirmed"))

    # Join results back
    df = df.join(results_df, on="record_id", how="left", suffix="_sp_new")

    # Coalesce: keep existing non-null values, fill with spatial results
    coalesce_exprs = []
    for sp_col, target_col in _TARGET_COLUMNS.items():
        if sp_col in df.columns:
            coalesce_exprs.append(
                pl.coalesce([
                    pl.col(target_col),
                    pl.col(sp_col),
                ]).alias(target_col)
            )

    # _spatial_confirmed: True if spatial match found
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

    # Drop working columns
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
