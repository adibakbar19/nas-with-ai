#!/usr/bin/env python3
"""Load all lookup CSVs into nas_lookup and populate geom columns from boundary tables.

Idempotent — creates unique indexes then uses INSERT ... ON CONFLICT DO UPDATE.
Geometry is copied from the pre-loaded *_boundary tables via ST_Multi(ST_Union(...)).
No GeoJSON parsing required — boundary data was loaded in a prior session.

Usage:
    python scripts/load_reference_data.py --data-dir /app/data
    python scripts/load_reference_data.py            # auto-detects /app/data or data/
"""
from __future__ import annotations

import argparse
import csv
import os
import re
import sys
import unicodedata
from pathlib import Path


# ── DSN ──────────────────────────────────────────────────────────────────────


def _build_dsn() -> str:
    for key in ("POSTGRES_DSN", "DATABASE_URL"):
        v = os.environ.get(key, "").strip()
        if v:
            return v
    user = os.environ.get("PGUSER", "nas").strip() or "nas"
    pwd  = os.environ.get("PGPASSWORD", "nas").strip() or "nas"
    host = os.environ.get("PGHOST", "localhost").strip() or "localhost"
    port = os.environ.get("PGPORT", "5432").strip() or "5432"
    db   = os.environ.get("PGDATABASE", "nas").strip() or "nas"
    return f"postgresql://{user}:{pwd}@{host}:{port}/{db}"


# ── Helpers ───────────────────────────────────────────────────────────────────


def _normalise_name(name: str) -> str:
    """Lowercase, strip punctuation, collapse spaces — for fuzzy name matching."""
    if not name:
        return ""
    name = unicodedata.normalize("NFKD", name).lower()
    name = re.sub(r"[^\w\s]", " ", name)
    return re.sub(r"\s+", " ", name).strip()


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8-sig") as f:
        return [
            {k.strip(): (v.strip() if v else "")
             for k, v in row.items()}
            for row in csv.DictReader(f)
        ]


def _ensure_unique_index(cur, schema: str, table: str, columns: list[str]) -> None:
    idx_name = f"{table}_{'_'.join(columns)}_uniq"
    col_list = ", ".join(columns)
    cur.execute(f"""
        CREATE UNIQUE INDEX IF NOT EXISTS {idx_name}
        ON {schema}.{table} ({col_list})
    """)


def _geom_coverage(cur, table: str, geom_col: str = "geom") -> tuple[int, int]:
    """Return (total, has_geom) for a table."""
    row = cur.execute(
        f"SELECT COUNT(*), COUNT({geom_col}) FROM nas_lookup.{table}"
    ).fetchone()
    return row[0], row[1]


# ── ORDER 1 — state ───────────────────────────────────────────────────────────


def load_state(cur, data_dir: Path) -> dict:
    rows = _read_csv(data_dir / "lookups_clean" / "state.csv")

    _ensure_unique_index(cur, "nas_lookup", "state", ["state_id"])

    cur.executemany("""
        INSERT INTO nas_lookup.state (state_id, state_code, state_name)
        VALUES (%(state_id)s, %(state_code)s, %(state_name)s)
        ON CONFLICT (state_id) DO UPDATE SET
            state_code = EXCLUDED.state_code,
            state_name = EXCLUDED.state_name
    """, rows)

    cur.execute("""
        UPDATE nas_lookup.state s
        SET geom = sub.geom
        FROM (
            SELECT state_code,
                   ST_Multi(ST_Union(boundary_geom)) AS geom
            FROM nas_lookup.state_boundary
            WHERE boundary_geom IS NOT NULL
            GROUP BY state_code
        ) sub
        WHERE s.state_code = sub.state_code
    """)

    total, has_geom = _geom_coverage(cur, "state")
    return {"rows": len(rows), "geom": has_geom, "total": total}


# ── ORDER 2 — district ────────────────────────────────────────────────────────


def load_district(cur, data_dir: Path) -> dict:
    rows = _read_csv(data_dir / "lookups_clean" / "district.csv")

    _ensure_unique_index(cur, "nas_lookup", "district", ["district_id"])

    cur.executemany("""
        INSERT INTO nas_lookup.district
            (district_id, state_id, state_code, district_code, district_name)
        VALUES (%(district_id)s, %(state_id)s, %(state_code)s,
                %(district_code)s, %(district_name)s)
        ON CONFLICT (district_id) DO UPDATE SET
            state_id      = EXCLUDED.state_id,
            state_code    = EXCLUDED.state_code,
            district_code = EXCLUDED.district_code,
            district_name = EXCLUDED.district_name
    """, rows)

    # 2 boundary rows have NULL district_code — they cannot be matched, skip
    cur.execute("""
        UPDATE nas_lookup.district d
        SET geom = sub.geom
        FROM (
            SELECT state_code, district_code,
                   ST_Multi(ST_Union(boundary_geom)) AS geom
            FROM nas_lookup.district_boundary
            WHERE boundary_geom IS NOT NULL
              AND district_code IS NOT NULL
              AND state_code    IS NOT NULL
            GROUP BY state_code, district_code
        ) sub
        WHERE d.state_code    = sub.state_code
          AND d.district_code = sub.district_code
    """)

    total, has_geom = _geom_coverage(cur, "district")
    return {"rows": len(rows), "geom": has_geom, "total": total}


# ── ORDER 3 — mukim ───────────────────────────────────────────────────────────


def load_mukim(cur, data_dir: Path) -> dict:
    rows = _read_csv(data_dir / "lookups_clean" / "mukim.csv")

    _ensure_unique_index(cur, "nas_lookup", "mukim", ["mukim_id"])

    cur.executemany("""
        INSERT INTO nas_lookup.mukim
            (mukim_id, district_id, state_code, district_code, mukim_code, mukim_name)
        VALUES (%(mukim_id)s, %(district_id)s, %(state_code)s,
                %(district_code)s, %(mukim_code)s, %(mukim_name)s)
        ON CONFLICT (mukim_id) DO UPDATE SET
            district_id   = EXCLUDED.district_id,
            state_code    = EXCLUDED.state_code,
            district_code = EXCLUDED.district_code,
            mukim_code    = EXCLUDED.mukim_code,
            mukim_name    = EXCLUDED.mukim_name
    """, rows)

    # mukim_boundary has multiple polygon pieces per mukim_id → union them.
    # ST_MakeValid + ST_CollectionExtract(3) ensures we only keep polygon
    # components (MakeValid can return mixed GeometryCollections).
    cur.execute("""
        UPDATE nas_lookup.mukim m
        SET geom = sub.geom
        FROM (
            SELECT mukim_id,
                   ST_Multi(
                       ST_CollectionExtract(
                           ST_Union(ST_MakeValid(boundary_geom)),
                           3
                       )
                   ) AS geom
            FROM nas_lookup.mukim_boundary
            WHERE boundary_geom IS NOT NULL
              AND mukim_id IS NOT NULL
            GROUP BY mukim_id
        ) sub
        WHERE m.mukim_id = sub.mukim_id
    """)

    total, has_geom = _geom_coverage(cur, "mukim")
    return {"rows": len(rows), "geom": has_geom, "total": total}


# ── ORDER 4 — pbt ─────────────────────────────────────────────────────────────


def load_pbt(cur, data_dir: Path) -> dict:
    rows = _read_csv(data_dir / "lookups_clean" / "pbt.csv")

    # pbt table was loaded from GeoJSON (boundary_geom present, pbt_id NULL for most rows)
    # Match CSV rows to table rows by normalised pbt_name, then update pbt_id + state columns
    existing = cur.execute(
        "SELECT pbt_name FROM nas_lookup.pbt"
    ).fetchall()
    existing_norm = {_normalise_name(r[0]): r[0] for r in existing}

    matched = 0
    unmatched: list[str] = []

    for row in rows:
        norm = _normalise_name(row["pbt_name"])
        if norm in existing_norm:
            cur.execute("""
                UPDATE nas_lookup.pbt
                SET pbt_id     = %(pbt_id)s,
                    state_id   = %(state_id)s,
                    state_name = %(state_name)s
                WHERE lower(trim(pbt_name)) = lower(trim(%(pbt_name)s))
            """, row)
            matched += 1
        else:
            # Try exact match as fallback (already done above — log as unmatched)
            unmatched.append(row["pbt_name"])

    if unmatched:
        print(f"\n  pbt: {len(unmatched)} CSV entries not matched in boundary table:")
        for name in unmatched[:10]:
            print(f"    - {name}")
        if len(unmatched) > 10:
            print(f"    ... ({len(unmatched) - 10} more)")

    total, has_geom = _geom_coverage(cur, "pbt", "boundary_geom")
    return {"rows": len(rows), "geom": has_geom, "total": total, "pbt_matched": matched}


# ── ORDER 5 — locality ────────────────────────────────────────────────────────


def load_locality(cur, data_dir: Path) -> dict:
    rows = _read_csv(data_dir / "lookups_clean" / "locality.csv")

    _ensure_unique_index(cur, "nas_lookup", "locality_lookup", ["locality_id"])

    cur.executemany("""
        INSERT INTO nas_lookup.locality_lookup
            (locality_id, state_id, state_name, locality_name)
        VALUES (%(locality_id)s, %(state_id)s, %(state_name)s, %(locality_name)s)
        ON CONFLICT (locality_id) DO UPDATE SET
            state_id      = EXCLUDED.state_id,
            state_name    = EXCLUDED.state_name,
            locality_name = EXCLUDED.locality_name
    """, rows)

    return {"rows": len(rows), "geom": 0, "total": len(rows)}


# ── ORDER 6 — postcode ────────────────────────────────────────────────────────


def load_postcode(cur, data_dir: Path) -> dict:
    rows = _read_csv(data_dir / "lookups_clean" / "postcode.csv")

    _ensure_unique_index(cur, "nas_lookup", "postcode", ["postcode_id"])

    cur.executemany("""
        INSERT INTO nas_lookup.postcode
            (postcode_id, state_id, locality_id, postcode, city, state)
        VALUES (%(postcode_id)s, %(state_id)s, %(locality_id)s,
                %(postcode)s, %(city)s, %(state)s)
        ON CONFLICT (postcode_id) DO UPDATE SET
            state_id    = EXCLUDED.state_id,
            locality_id = EXCLUDED.locality_id,
            postcode    = EXCLUDED.postcode,
            city        = EXCLUDED.city,
            state       = EXCLUDED.state
    """, rows)

    # Only 884 of 2928 postcodes have boundary data
    cur.execute("""
        UPDATE nas_lookup.postcode p
        SET geom = sub.geom
        FROM (
            SELECT postcode,
                   ST_Multi(ST_Union(boundary_geom)) AS geom
            FROM nas_lookup.postcode_boundary
            WHERE boundary_geom IS NOT NULL
              AND postcode IS NOT NULL
            GROUP BY postcode
        ) sub
        WHERE p.postcode = sub.postcode
    """)

    total, has_geom = _geom_coverage(cur, "postcode")
    return {"rows": len(rows), "geom": has_geom, "total": total}


# ── ORDER 7 — sublocality ─────────────────────────────────────────────────────


def load_sublocality(cur, data_dir: Path) -> dict:
    rows = _read_csv(data_dir / "lookups_clean" / "sublocality.csv")

    _ensure_unique_index(cur, "nas_lookup", "sublocality_lookup", ["sublocality_id"])

    # Batch insert for 98k rows
    for i in range(0, len(rows), 5000):
        batch = rows[i : i + 5000]
        cur.executemany("""
            INSERT INTO nas_lookup.sublocality_lookup
                (sublocality_id, state_id, state_name, sub_locality_name)
            VALUES (%(sublocality_id)s, %(state_id)s, %(state_name)s, %(sub_locality_name)s)
            ON CONFLICT (sublocality_id) DO UPDATE SET
                state_id          = EXCLUDED.state_id,
                state_name        = EXCLUDED.state_name,
                sub_locality_name = EXCLUDED.sub_locality_name
        """, batch)

    return {"rows": len(rows), "geom": 0, "total": len(rows)}


# ── ORDER 8 — street_type ─────────────────────────────────────────────────────


def load_street_type(cur, data_dir: Path) -> dict:
    rows = _read_csv(data_dir / "lookups_clean" / "street_type.csv")

    _ensure_unique_index(cur, "nas_lookup", "street_type", ["street_type_id"])

    cur.executemany("""
        INSERT INTO nas_lookup.street_type (street_type_id, street_type)
        VALUES (%(street_type_id)s, %(street_type)s)
        ON CONFLICT (street_type_id) DO UPDATE SET
            street_type = EXCLUDED.street_type
    """, rows)

    return {"rows": len(rows), "geom": 0, "total": len(rows)}


# ── ORDER 9 — street_type_alias ───────────────────────────────────────────────


def load_street_type_alias(cur, data_dir: Path) -> dict:
    rows = _read_csv(data_dir / "lookups_clean" / "street_type_alias.csv")

    cur.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS street_type_alias_id_raw_uniq
        ON nas_lookup.street_type_alias (street_type_id, raw_type)
    """)

    cur.executemany("""
        INSERT INTO nas_lookup.street_type_alias (street_type_id, raw_type, canonical_type)
        VALUES (%(street_type_id)s, %(raw_type)s, %(canonical_type)s)
        ON CONFLICT (street_type_id, raw_type) DO UPDATE SET
            canonical_type = EXCLUDED.canonical_type
    """, rows)

    return {"rows": len(rows), "geom": 0, "total": len(rows)}


# ── ORDER 10 — street_name ────────────────────────────────────────────────────


def load_street_name(cur, data_dir: Path) -> dict:
    rows = _read_csv(data_dir / "lookups_clean" / "street_name.csv")

    cur.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS street_name_state_name_uniq
        ON nas_lookup.street_name (state_id, street_name)
    """)

    for i in range(0, len(rows), 10000):
        batch = rows[i : i + 10000]
        cur.executemany("""
            INSERT INTO nas_lookup.street_name (state_id, state_name, street_name)
            VALUES (%(state_id)s, %(state_name)s, %(street_name)s)
            ON CONFLICT (state_id, street_name) DO NOTHING
        """, batch)

    return {"rows": len(rows), "geom": 0, "total": len(rows)}


# ── ORDER 11 — district_aliases ───────────────────────────────────────────────


def load_district_aliases(cur, data_dir: Path) -> dict:
    rows = _read_csv(data_dir / "lookups_clean" / "district_aliases.csv")

    cur.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS district_aliases_id_alias_uniq
        ON nas_lookup.district_aliases (district_id, district_alias)
    """)

    cur.executemany("""
        INSERT INTO nas_lookup.district_aliases
            (district_id, state_code, district_code, district_alias)
        VALUES (%(district_id)s, %(state_code)s, %(district_code)s, %(district_alias)s)
        ON CONFLICT DO NOTHING
    """, rows)

    return {"rows": len(rows), "geom": 0, "total": len(rows)}


# ── Geometry coverage report ──────────────────────────────────────────────────


def report_geometry_coverage(cur) -> None:
    rows = cur.execute("""
        SELECT 'mukim' as tbl,
               COUNT(*) as total,
               COUNT(geom) as has_geom,
               ROUND(COUNT(geom)::numeric / NULLIF(COUNT(*),0) * 100, 1) as pct
        FROM nas_lookup.mukim
        UNION ALL
        SELECT 'district', COUNT(*), COUNT(geom),
               ROUND(COUNT(geom)::numeric / NULLIF(COUNT(*),0) * 100, 1)
        FROM nas_lookup.district
        UNION ALL
        SELECT 'state', COUNT(*), COUNT(geom),
               ROUND(COUNT(geom)::numeric / NULLIF(COUNT(*),0) * 100, 1)
        FROM nas_lookup.state
        UNION ALL
        SELECT 'postcode', COUNT(*), COUNT(geom),
               ROUND(COUNT(geom)::numeric / NULLIF(COUNT(*),0) * 100, 1)
        FROM nas_lookup.postcode
        UNION ALL
        SELECT 'pbt', COUNT(*), COUNT(boundary_geom),
               ROUND(COUNT(boundary_geom)::numeric / NULLIF(COUNT(*),0) * 100, 1)
        FROM nas_lookup.pbt
        ORDER BY tbl
    """).fetchall()

    print("\nGeometry coverage:")
    print(f"  {'Table':<12} {'Total':>7} {'HasGeom':>9} {'Pct':>6}")
    print(f"  {'-'*12} {'-'*7} {'-'*9} {'-'*6}")
    for tbl, total, has_geom, pct in rows:
        print(f"  {tbl:<12} {total:>7} {has_geom:>9} {str(pct):>5}%")


# ── Main ──────────────────────────────────────────────────────────────────────


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", default=None,
                        help="Root path containing lookups_clean/ subdirectory")
    args = parser.parse_args()

    if args.data_dir:
        data_dir = Path(args.data_dir)
    else:
        candidates = [Path("/app/data"), Path(__file__).parent.parent / "data", Path.cwd() / "data"]
        data_dir = next((p for p in candidates if p.exists()), None)
        if data_dir is None:
            print("ERROR: Cannot find data/ directory. Pass --data-dir explicitly.", file=sys.stderr)
            return 1

    lookups_dir = data_dir / "lookups_clean"
    if not lookups_dir.exists():
        print(f"ERROR: {lookups_dir} does not exist.", file=sys.stderr)
        return 1

    print(f"Loading reference data from: {data_dir}")

    import psycopg

    dsn = _build_dsn()

    with psycopg.connect(dsn, autocommit=False) as conn:
        with conn.cursor() as cur:
            loaders = [
                ("state",              load_state),
                ("district",           load_district),
                ("mukim",              load_mukim),
                ("pbt",                load_pbt),
                ("locality",           load_locality),
                ("postcode",           load_postcode),
                ("sublocality",        load_sublocality),
                ("street_type",        load_street_type),
                ("street_type_alias",  load_street_type_alias),
                ("street_name",        load_street_name),
                ("district_aliases",   load_district_aliases),
            ]

            results: dict[str, dict] = {}
            for name, loader in loaders:
                print(f"Loading {name}...", end=" ", flush=True)
                try:
                    stats = loader(cur, data_dir)
                    conn.commit()
                    results[name] = stats

                    has_boundary = name in ("state", "district", "mukim", "postcode")
                    if has_boundary:
                        geom_info = f", {stats['geom']}/{stats['total']} geometries"
                    elif name == "pbt":
                        geom_info = (
                            f", {stats['pbt_matched']}/{stats['rows']} CSV matched, "
                            f"{stats['geom']}/{stats['total']} boundary_geom"
                        )
                    else:
                        geom_info = ""
                    print(f"{stats['rows']} rows loaded{geom_info}")

                except Exception as exc:
                    conn.rollback()
                    print(f"\nERROR in {name}: {exc}", file=sys.stderr)
                    import traceback
                    traceback.print_exc()
                    return 1

            with conn.cursor() as cur:
                report_geometry_coverage(cur)

    print("\nReference data load complete")
    print(f"Tables loaded: {len(results)}")

    geom_stats = {
        k: v for k, v in results.items()
        if k in ("state", "district", "mukim", "postcode", "pbt")
    }
    parts = []
    for name, stats in geom_stats.items():
        g = stats.get("geom", 0)
        t = stats.get("total", 1)
        pct = round(g / t * 100) if t else 0
        geom_col = "boundary_geom" if name == "pbt" else "geom"
        parts.append(f"{name} {pct}%")
    print(f"Geometry coverage: {', '.join(parts)}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
