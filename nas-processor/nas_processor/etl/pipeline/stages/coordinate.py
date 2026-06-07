"""Coordinate enrichment stage — populate lat/lon from centroid cache.

For rows without GPS coordinates, assigns a centroid from the most-precise
available administrative boundary:
  mukim_id   → mukim_centroid
  postcode   → postcode_centroid
  district_code → district_centroid
  state_code → state_centroid

Called once per chunk AFTER spatial enrichment (which fills boundary codes
from existing GPS coords) so that the centroid lookup benefits from any
boundary codes the spatial stage just resolved.

centroid_cache is loaded ONCE at pipeline startup, not per chunk.
"""
from __future__ import annotations

import logging
from typing import Any

import polars as pl

logger = logging.getLogger(__name__)

# Malaysia bounding box — reject anything outside
_LAT_MIN, _LAT_MAX = 1.0, 7.5
_LON_MIN, _LON_MAX = 99.5, 119.5

CentroidCache = dict[str, dict[str, tuple[float, float]]]


# ── Cache loader ──────────────────────────────────────────────────────────────


def load_centroid_cache(dsn: str) -> CentroidCache:
    """Load all boundary centroids into memory.

    Returns:
        {
            "mukim":    {mukim_id: (lat, lon), ...},
            "postcode": {postcode: (lat, lon), ...},
            "district": {district_code_with_state: (lat, lon), ...},
            "state":    {state_code: (lat, lon), ...},
        }
    """
    cache: CentroidCache = {"mukim": {}, "postcode": {}, "district": {}, "state": {}}

    try:
        import psycopg

        # psycopg.connect needs a libpq DSN (postgresql://...), not a SQLAlchemy
        # driver-qualified URL (postgresql+psycopg://...)
        psycopg_dsn = dsn.replace("postgresql+psycopg://", "postgresql://")
        conn = psycopg.connect(psycopg_dsn)
        try:
            with conn.cursor() as cur:
                # mukim centroids — key is mukim_id (text)
                rows = cur.execute("""
                    SELECT mukim_id::text,
                           ST_Y(ST_Centroid(geom))::float8 AS lat,
                           ST_X(ST_Centroid(geom))::float8 AS lon
                    FROM nas_lookup.mukim
                    WHERE geom IS NOT NULL
                """).fetchall()
                cache["mukim"] = {r[0]: (r[1], r[2]) for r in rows if r[0]}

                # postcode centroids — key is postcode string
                rows = cur.execute("""
                    SELECT postcode,
                           ST_Y(ST_Centroid(geom))::float8 AS lat,
                           ST_X(ST_Centroid(geom))::float8 AS lon
                    FROM nas_lookup.postcode
                    WHERE geom IS NOT NULL AND postcode IS NOT NULL
                """).fetchall()
                cache["postcode"] = {r[0]: (r[1], r[2]) for r in rows if r[0]}

                # district centroids — key is "state_code:district_code" composite
                rows = cur.execute("""
                    SELECT state_code, district_code,
                           ST_Y(ST_Centroid(geom))::float8 AS lat,
                           ST_X(ST_Centroid(geom))::float8 AS lon
                    FROM nas_lookup.district
                    WHERE geom IS NOT NULL
                      AND state_code IS NOT NULL
                      AND district_code IS NOT NULL
                """).fetchall()
                cache["district"] = {
                    f"{r[0]}:{r[1]}": (r[2], r[3])
                    for r in rows if r[0] and r[1]
                }

                # state centroids — key is state_code
                rows = cur.execute("""
                    SELECT state_code,
                           ST_Y(ST_Centroid(geom))::float8 AS lat,
                           ST_X(ST_Centroid(geom))::float8 AS lon
                    FROM nas_lookup.state
                    WHERE geom IS NOT NULL AND state_code IS NOT NULL
                """).fetchall()
                cache["state"] = {r[0]: (r[1], r[2]) for r in rows if r[0]}

        finally:
            conn.close()

    except Exception as exc:
        logger.error(
            "centroid_cache_load_failed error=%s returning_empty=true", exc
        )
        return {"mukim": {}, "postcode": {}, "district": {}, "state": {}}

    logger.info(
        "centroid_cache_loaded mukim=%d postcode=%d district=%d state=%d",
        len(cache["mukim"]),
        len(cache["postcode"]),
        len(cache["district"]),
        len(cache["state"]),
    )
    return cache


# ── Per-row enrichment ────────────────────────────────────────────────────────


def _enrich_row(
    row: dict[str, Any],
    cache: CentroidCache,
    lat_col: str | None,
    lon_col: str | None,
) -> tuple[float | None, float | None, str | None]:
    """Return (lat, lon, coordinate_level) for a single address row.

    Priority:
      1. Source lat/lon (validated against Malaysia bounds) → "rooftop"
      2. mukim_id in centroid cache             → "mukim_centroid"
      3. postcode in centroid cache              → "postcode_centroid"
      4. state_code:district_code in cache       → "district_centroid"
      5. state_code in centroid cache            → "state_centroid"
      6. Nothing available                       → (None, None, None)
    """
    try:
        # 1. Source GPS
        if lat_col and lon_col:
            raw_lat = row.get(lat_col)
            raw_lon = row.get(lon_col)
            if raw_lat is not None and raw_lon is not None:
                try:
                    lat = float(raw_lat)
                    lon = float(raw_lon)
                    if _LAT_MIN <= lat <= _LAT_MAX and _LON_MIN <= lon <= _LON_MAX:
                        return lat, lon, "rooftop"
                except (TypeError, ValueError):
                    pass

        # 2. mukim_id centroid
        mukim_id = row.get("mukim_id") or ""
        if mukim_id:
            hit = cache["mukim"].get(str(mukim_id).strip())
            if hit:
                return hit[0], hit[1], "mukim_centroid"

        # 3. postcode centroid
        postcode = row.get("postcode") or ""
        if postcode:
            hit = cache["postcode"].get(str(postcode).strip())
            if hit:
                return hit[0], hit[1], "postcode_centroid"

        # 4. district centroid (composite key: state_code:district_code)
        state_code = row.get("state_code") or ""
        district_code = row.get("district_code") or ""
        if state_code and district_code:
            key = f"{state_code.strip()}:{district_code.strip()}"
            hit = cache["district"].get(key)
            if hit:
                return hit[0], hit[1], "district_centroid"

        # 5. state centroid
        if state_code:
            hit = cache["state"].get(str(state_code).strip())
            if hit:
                return hit[0], hit[1], "state_centroid"

    except Exception:
        pass

    return None, None, None


# ── DataFrame enrichment ──────────────────────────────────────────────────────


def enrich_coordinates(
    df: pl.DataFrame,
    centroid_cache: CentroidCache,
    source_lat_col: str | None = None,
    source_lon_col: str | None = None,
) -> pl.DataFrame:
    """Add latitude, longitude, coordinate_level columns to a DataFrame.

    - Does NOT overwrite existing non-null latitude/longitude (uses coalesce).
    - Does NOT write geom — that is handled by load.py via PostGIS ST_MakePoint.
    - Never raises; returns df unchanged on any unexpected error.
    """
    if df.is_empty():
        for col, dtype in [
            ("latitude", pl.Float64),
            ("longitude", pl.Float64),
            ("coordinate_level", pl.Utf8),
        ]:
            if col not in df.columns:
                df = df.with_columns(pl.lit(None).cast(dtype).alias(col))
        return df

    try:
        # Cast lat/lon to Float64 if they arrived as String from the source CSV.
        # This must happen before to_dicts() so _enrich_row gets floats, and
        # before coalesce so the dtype is consistent with _enrich_lat/_enrich_lon.
        for _col in ("latitude", "longitude"):
            if _col in df.columns and df.schema[_col] not in (
                pl.Float64, pl.Float32, pl.Int64, pl.Int32
            ):
                df = df.with_columns(
                    pl.col(_col).cast(pl.Float64, strict=False).alias(_col)
                )

        rows = df.to_dicts()
        lats: list[float | None] = []
        lons: list[float | None] = []
        levels: list[str | None] = []

        for row in rows:
            lat, lon, level = _enrich_row(row, centroid_cache, source_lat_col, source_lon_col)
            lats.append(lat)
            lons.append(lon)
            levels.append(level)

        new_lat = pl.Series("_enrich_lat", lats, dtype=pl.Float64)
        new_lon = pl.Series("_enrich_lon", lons, dtype=pl.Float64)
        new_level = pl.Series("_enrich_level", levels, dtype=pl.Utf8)

        df = df.with_columns([
            new_lat.alias("_enrich_lat"),
            new_lon.alias("_enrich_lon"),
            new_level.alias("_enrich_level"),
        ])

        # Coalesce: keep existing non-null values, fill from enrichment
        lat_expr = (
            pl.coalesce(["latitude", "_enrich_lat"])
            if "latitude" in df.columns
            else pl.col("_enrich_lat")
        )
        lon_expr = (
            pl.coalesce(["longitude", "_enrich_lon"])
            if "longitude" in df.columns
            else pl.col("_enrich_lon")
        )
        level_expr = (
            pl.coalesce(["coordinate_level", "_enrich_level"])
            if "coordinate_level" in df.columns
            else pl.col("_enrich_level")
        )

        df = df.with_columns([
            lat_expr.alias("latitude"),
            lon_expr.alias("longitude"),
            level_expr.alias("coordinate_level"),
        ])

        df = df.drop(["_enrich_lat", "_enrich_lon", "_enrich_level"])

    except Exception as exc:
        logger.warning("coordinate_enrich_failed error=%s", exc)
        for col, dtype in [
            ("latitude", pl.Float64),
            ("longitude", pl.Float64),
            ("coordinate_level", pl.Utf8),
        ]:
            if col not in df.columns:
                df = df.with_columns(pl.lit(None).cast(dtype).alias(col))

    return df


# ── Level count helper for pipeline logging ───────────────────────────────────


def _count_level(
    *dfs: pl.DataFrame,
    level: str | None,
) -> int:
    """Count rows with a specific coordinate_level across multiple DataFrames."""
    total = 0
    for df in dfs:
        if df.is_empty() or "coordinate_level" not in df.columns:
            continue
        if level is None:
            total += df.filter(pl.col("coordinate_level").is_null()).height
        else:
            total += df.filter(pl.col("coordinate_level") == level).height
    return total
