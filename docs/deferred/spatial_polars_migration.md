# Deferred: Spatial enrichment Polars migration

## Status

Deferred — requires GeoPandas → Polars migration

## Problem

The existing _spatial.py uses GeoPandas (Pandas-based)
for PostGIS boundary joins. The new pipeline is
Polars end-to-end and cannot use GeoPandas directly.

## What needs to happen

Rewrite stages/spatial.py to:
- Filter rows with non-null latitude AND longitude
- Use psycopg directly to query PostGIS:
  SELECT boundary_type, boundary_code
  FROM nas_lookup.boundaries
  WHERE ST_Contains(geom, ST_Point(lng, lat))
- Return results as Polars DataFrame
- Merge back using record_id join

## Impact

Addresses with coordinates currently receive no
boundary enrichment. Their confidence scores are
up to 5 points lower than they could be.

Addresses without coordinates are unaffected.

## When to build

After the core pipeline is verified working end-to-end.
Spatial enrichment is an enhancement, not a blocker.
