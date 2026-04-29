import argparse
import json
import os
from urllib.parse import quote_plus

try:
    import psycopg
    from psycopg import sql
    from psycopg.rows import dict_row
except Exception:  # pragma: no cover
    psycopg = None
    sql = None
    dict_row = None


def _build_dsn(args: argparse.Namespace) -> str:
    if args.dsn:
        return args.dsn
    host = args.host or os.getenv("PGHOST", "localhost")
    port = args.port or os.getenv("PGPORT", "5432")
    database = args.database or os.getenv("PGDATABASE", "postgres")
    user = quote_plus(str(args.user or os.getenv("PGUSER", "postgres")))
    password = quote_plus(str(args.password or os.getenv("PGPASSWORD", "")))
    return f"postgresql://{user}:{password}@{host}:{port}/{database}"


def _tbl(schema_name: str, table_name: str):
    assert sql is not None
    return sql.SQL("{}.{}").format(sql.Identifier(schema_name), sql.Identifier(table_name))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Spatially reassign state/district/mukim/postcode/PBT on standardized addresses using "
            "the active boundary tables in LOOKUP_SCHEMA. Existing NASKod codes are preserved."
        )
    )
    parser.add_argument("--schema", default=os.getenv("PGSCHEMA", "nas"), help="Runtime schema containing standardized_address")
    parser.add_argument("--lookup-schema", default=os.getenv("LOOKUP_SCHEMA", "nas_lookup"), help="Lookup schema containing active boundaries")
    parser.add_argument(
        "--clear-unmatched",
        action="store_true",
        help="Clear IDs when no spatial boundary match is found. Default preserves current values when unmatched.",
    )
    parser.add_argument("--dsn", default=None, help="Postgres DSN")
    parser.add_argument("--host", default=None)
    parser.add_argument("--port", default=None)
    parser.add_argument("--database", default=None)
    parser.add_argument("--user", default=os.getenv("PGUSER"))
    parser.add_argument("--password", default=os.getenv("PGPASSWORD"))
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply the spatial reassignment. Without this flag the command only prints a dry-run impact summary.",
    )
    return parser.parse_args()


def _connect(dsn: str):
    if psycopg is None or dict_row is None:
        raise RuntimeError("psycopg is required for backfill_spatial_refs.py")
    return psycopg.connect(dsn, row_factory=dict_row)


def _table_exists(*, cur, schema_name: str, table_name: str) -> bool:
    cur.execute(
        """
        SELECT EXISTS (
          SELECT 1
          FROM information_schema.tables
          WHERE table_schema = %s
            AND table_name = %s
        ) AS present
        """,
        (schema_name, table_name),
    )
    row = cur.fetchone()
    return bool(row and row["present"])


def _require_tables(*, cur, runtime_schema: str, lookup_schema: str) -> None:
    required = (
        (runtime_schema, "standardized_address"),
        (lookup_schema, "state"),
        (lookup_schema, "district"),
        (lookup_schema, "mukim"),
        (lookup_schema, "postcode"),
        (lookup_schema, "pbt"),
        (lookup_schema, "state_boundary"),
        (lookup_schema, "district_boundary"),
        (lookup_schema, "mukim_boundary"),
        (lookup_schema, "postcode_boundary"),
    )
    missing = [f"{schema_name}.{table_name}" for schema_name, table_name in required if not _table_exists(cur=cur, schema_name=schema_name, table_name=table_name)]
    if missing:
        raise RuntimeError(f"missing required tables: {', '.join(missing)}")


def _candidate_cte(*, runtime_schema: str, lookup_schema: str, clear_unmatched: bool):
    assert sql is not None
    state_id_expr = sql.SQL("COALESCE(state_match.state_id, sa.state_id)") if not clear_unmatched else sql.SQL("state_match.state_id")
    district_id_expr = sql.SQL("COALESCE(district_match.district_id, sa.district_id)") if not clear_unmatched else sql.SQL("district_match.district_id")
    mukim_id_expr = sql.SQL("COALESCE(mukim_match.mukim_id, sa.mukim_id)") if not clear_unmatched else sql.SQL("mukim_match.mukim_id")
    postcode_id_expr = sql.SQL("COALESCE(postcode_match.postcode_id, sa.postcode_id)") if not clear_unmatched else sql.SQL("postcode_match.postcode_id")
    pbt_id_expr = sql.SQL("COALESCE(pbt_match.pbt_id, sa.pbt_id)") if not clear_unmatched else sql.SQL("pbt_match.pbt_id")
    return sql.SQL(
        """
        WITH candidate AS (
          SELECT
            sa.address_id,
            {state_id_expr} AS next_state_id,
            {district_id_expr} AS next_district_id,
            {mukim_id_expr} AS next_mukim_id,
            {postcode_id_expr} AS next_postcode_id,
            {pbt_id_expr} AS next_pbt_id,
            sa.state_id AS current_state_id,
            sa.district_id AS current_district_id,
            sa.mukim_id AS current_mukim_id,
            sa.postcode_id AS current_postcode_id,
            sa.pbt_id AS current_pbt_id
          FROM {address} sa
          LEFT JOIN LATERAL (
            SELECT COALESCE(sb.state_id, st.state_id) AS state_id
            FROM {state_boundary} sb
            LEFT JOIN {state} st
              ON sb.state_id IS NULL
             AND upper(trim(coalesce(st.state_code::text, ''))) = upper(trim(coalesce(sb.state_code::text, '')))
            WHERE sa.geom IS NOT NULL
              AND sb.boundary_geom IS NOT NULL
              AND ST_Intersects(sb.boundary_geom, sa.geom)
            ORDER BY ST_Area(sb.boundary_geom) ASC NULLS LAST, COALESCE(sb.state_id, st.state_id) ASC
            LIMIT 1
          ) state_match ON TRUE
          LEFT JOIN LATERAL (
            SELECT COALESCE(db.district_id, d.district_id) AS district_id
            FROM {district_boundary} db
            LEFT JOIN {district} d
              ON db.district_id IS NULL
             AND (
               upper(trim(coalesce(d.district_code::text, ''))) = upper(trim(coalesce(db.district_code::text, '')))
               OR upper(trim(coalesce(d.district_name::text, ''))) = upper(trim(coalesce(db.district_name::text, '')))
             )
            LEFT JOIN {state} st
              ON st.state_id = COALESCE(db.state_id, d.state_id)
            WHERE sa.geom IS NOT NULL
              AND db.boundary_geom IS NOT NULL
              AND ST_Intersects(db.boundary_geom, sa.geom)
              AND (
                db.state_id IS NOT NULL
                OR
                nullif(trim(coalesce(db.state_code::text, '')), '') IS NULL
                OR upper(trim(coalesce(st.state_code::text, ''))) = upper(trim(coalesce(db.state_code::text, '')))
              )
            ORDER BY ST_Area(db.boundary_geom) ASC NULLS LAST, COALESCE(db.district_id, d.district_id) ASC
            LIMIT 1
          ) district_match ON TRUE
          LEFT JOIN LATERAL (
            SELECT COALESCE(mb.mukim_id, m.mukim_id) AS mukim_id
            FROM {mukim_boundary} mb
            LEFT JOIN {mukim} m
              ON (
                (mb.mukim_id IS NOT NULL AND m.mukim_id = mb.mukim_id)
                OR (
                  mb.mukim_id IS NULL
                  AND upper(trim(coalesce(m.mukim_code::text, ''))) = upper(trim(coalesce(mb.mukim_code::text, '')))
                )
                OR (
                  mb.mukim_id IS NULL
                  AND nullif(trim(coalesce(mb.mukim_code::text, '')), '') IS NULL
                  AND upper(trim(coalesce(m.mukim_name::text, ''))) = upper(trim(coalesce(mb.mukim_name::text, '')))
                )
              )
            WHERE sa.geom IS NOT NULL
              AND mb.boundary_geom IS NOT NULL
              AND ST_Intersects(mb.boundary_geom, sa.geom)
            ORDER BY ST_Area(mb.boundary_geom) ASC NULLS LAST, COALESCE(mb.mukim_id, m.mukim_id) ASC
            LIMIT 1
          ) mukim_match ON TRUE
          LEFT JOIN LATERAL (
            SELECT COALESCE(pb.postcode_id, p.postcode_id) AS postcode_id
            FROM {postcode_boundary} pb
            LEFT JOIN {postcode} p
              ON pb.postcode_id IS NULL
             AND trim(coalesce(p.postcode::text, '')) = trim(coalesce(pb.postcode::text, ''))
            WHERE sa.geom IS NOT NULL
              AND pb.boundary_geom IS NOT NULL
              AND ST_Intersects(pb.boundary_geom, sa.geom)
            ORDER BY ST_Area(pb.boundary_geom) ASC NULLS LAST, COALESCE(pb.postcode_id, p.postcode_id) ASC
            LIMIT 1
          ) postcode_match ON TRUE
          LEFT JOIN LATERAL (
            SELECT p.pbt_id
            FROM {pbt} p
            WHERE sa.geom IS NOT NULL
              AND p.boundary_geom IS NOT NULL
              AND ST_Intersects(p.boundary_geom, sa.geom)
            ORDER BY ST_Area(p.boundary_geom) ASC NULLS LAST, p.pbt_id ASC
            LIMIT 1
          ) pbt_match ON TRUE
          WHERE sa.geom IS NOT NULL
        )
        """
    ).format(
        address=_tbl(runtime_schema, "standardized_address"),
        state_boundary=_tbl(lookup_schema, "state_boundary"),
        district_boundary=_tbl(lookup_schema, "district_boundary"),
        mukim_boundary=_tbl(lookup_schema, "mukim_boundary"),
        postcode_boundary=_tbl(lookup_schema, "postcode_boundary"),
        state=_tbl(lookup_schema, "state"),
        district=_tbl(lookup_schema, "district"),
        mukim=_tbl(lookup_schema, "mukim"),
        postcode=_tbl(lookup_schema, "postcode"),
        pbt=_tbl(lookup_schema, "pbt"),
        state_id_expr=state_id_expr,
        district_id_expr=district_id_expr,
        mukim_id_expr=mukim_id_expr,
        postcode_id_expr=postcode_id_expr,
        pbt_id_expr=pbt_id_expr,
    )


def _impact_summary(*, cur, runtime_schema: str, lookup_schema: str, clear_unmatched: bool) -> dict[str, int]:
    assert sql is not None
    stmt = sql.SQL(
        """
        {candidate_cte}
        SELECT
          COUNT(*)::INTEGER AS candidate_rows,
          COUNT(*) FILTER (
            WHERE current_state_id IS DISTINCT FROM next_state_id
               OR current_district_id IS DISTINCT FROM next_district_id
               OR current_mukim_id IS DISTINCT FROM next_mukim_id
               OR current_postcode_id IS DISTINCT FROM next_postcode_id
               OR current_pbt_id IS DISTINCT FROM next_pbt_id
          )::INTEGER AS rows_will_change,
          COUNT(*) FILTER (WHERE current_state_id IS DISTINCT FROM next_state_id)::INTEGER AS state_changes,
          COUNT(*) FILTER (WHERE current_district_id IS DISTINCT FROM next_district_id)::INTEGER AS district_changes,
          COUNT(*) FILTER (WHERE current_mukim_id IS DISTINCT FROM next_mukim_id)::INTEGER AS mukim_changes,
          COUNT(*) FILTER (WHERE current_postcode_id IS DISTINCT FROM next_postcode_id)::INTEGER AS postcode_changes,
          COUNT(*) FILTER (WHERE current_pbt_id IS DISTINCT FROM next_pbt_id)::INTEGER AS pbt_changes
        FROM candidate
        """
    ).format(candidate_cte=_candidate_cte(runtime_schema=runtime_schema, lookup_schema=lookup_schema, clear_unmatched=clear_unmatched))
    cur.execute(stmt)
    row = cur.fetchone()
    return {key: int(value or 0) for key, value in dict(row or {}).items()}


def _apply_backfill(*, cur, runtime_schema: str, lookup_schema: str, clear_unmatched: bool) -> int:
    assert sql is not None
    stmt = sql.SQL(
        """
        {candidate_cte}
        , updated AS (
          UPDATE {address} sa
          SET
            state_id = candidate.next_state_id,
            district_id = candidate.next_district_id,
            mukim_id = candidate.next_mukim_id,
            postcode_id = candidate.next_postcode_id,
            pbt_id = candidate.next_pbt_id,
            updated_at = NOW()
          FROM candidate
          WHERE sa.address_id = candidate.address_id
            AND (
              sa.state_id IS DISTINCT FROM candidate.next_state_id
              OR sa.district_id IS DISTINCT FROM candidate.next_district_id
              OR sa.mukim_id IS DISTINCT FROM candidate.next_mukim_id
              OR sa.postcode_id IS DISTINCT FROM candidate.next_postcode_id
              OR sa.pbt_id IS DISTINCT FROM candidate.next_pbt_id
            )
          RETURNING sa.address_id
        )
        SELECT COUNT(*)::INTEGER AS updated_count
        FROM updated
        """
    ).format(
        candidate_cte=_candidate_cte(runtime_schema=runtime_schema, lookup_schema=lookup_schema, clear_unmatched=clear_unmatched),
        address=_tbl(runtime_schema, "standardized_address"),
    )
    cur.execute(stmt)
    row = cur.fetchone()
    return int(row["updated_count"]) if row else 0


def main() -> None:
    args = parse_args()
    dsn = _build_dsn(args)
    with _connect(dsn) as conn, conn.cursor() as cur:
        _require_tables(cur=cur, runtime_schema=args.schema, lookup_schema=args.lookup_schema)
        summary = _impact_summary(
            cur=cur,
            runtime_schema=args.schema,
            lookup_schema=args.lookup_schema,
            clear_unmatched=args.clear_unmatched,
        )
        payload: dict[str, object] = {
            "mode": "apply" if args.apply else "dry_run",
            "runtime_schema": args.schema,
            "lookup_schema": args.lookup_schema,
            "clear_unmatched": bool(args.clear_unmatched),
            "impact": summary,
            "naskod_policy": "preserve_existing_code",
        }
        if args.apply:
            updated_count = _apply_backfill(
                cur=cur,
                runtime_schema=args.schema,
                lookup_schema=args.lookup_schema,
                clear_unmatched=args.clear_unmatched,
            )
            conn.commit()
            payload["updated_count"] = updated_count
        print(json.dumps(payload, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
