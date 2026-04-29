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
            "Backfill standardized_address admin-reference fields after lookup changes. "
            "This job preserves existing NASKod codes and only rewires address reference IDs."
        )
    )
    parser.add_argument(
        "--entity",
        choices=["district", "mukim", "locality", "postcode"],
        required=True,
        help="Reference entity being remapped.",
    )
    parser.add_argument("--from-id", type=int, required=True, help="Current reference id on affected addresses.")
    parser.add_argument("--to-id", type=int, required=True, help="Replacement reference id from updated lookup/runtime tables.")
    parser.add_argument("--schema", default=os.getenv("PGSCHEMA", "nas"), help="Runtime schema containing standardized_address")
    parser.add_argument("--lookup-schema", default=os.getenv("LOOKUP_SCHEMA", "nas_lookup"), help="Lookup source-of-truth schema")
    parser.add_argument("--dsn", default=None, help="Postgres DSN")
    parser.add_argument("--host", default=None)
    parser.add_argument("--port", default=None)
    parser.add_argument("--database", default=None)
    parser.add_argument("--user", default=os.getenv("PGUSER"))
    parser.add_argument("--password", default=os.getenv("PGPASSWORD"))
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply the backfill. Without this flag the command only prints a dry-run impact summary.",
    )
    return parser.parse_args()


def _connect(dsn: str):
    if psycopg is None or dict_row is None:
        raise RuntimeError("psycopg is required for backfill_lookup_refs.py")
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


def _require_tables(*, cur, lookup_schema: str, runtime_schema: str) -> None:
    required = (
        (lookup_schema, "district"),
        (lookup_schema, "mukim"),
        (lookup_schema, "locality"),
        (lookup_schema, "postcode"),
        (runtime_schema, "district"),
        (runtime_schema, "mukim"),
        (runtime_schema, "locality"),
        (runtime_schema, "postcode"),
        (runtime_schema, "street"),
        (runtime_schema, "standardized_address"),
    )
    missing = [f"{schema_name}.{table_name}" for schema_name, table_name in required if not _table_exists(cur=cur, schema_name=schema_name, table_name=table_name)]
    if missing:
        raise RuntimeError(f"missing required tables: {', '.join(missing)}")


def _entity_column(entity: str) -> str:
    return {
        "district": "district_id",
        "mukim": "mukim_id",
        "locality": "locality_id",
        "postcode": "postcode_id",
    }[entity]


def _context_query(entity: str):
    assert sql is not None
    if entity == "district":
        return sql.SQL(
            """
            SELECT
              d.district_id AS entity_id,
              d.district_name AS entity_name,
              d.district_code AS entity_code,
              d.state_id,
              NULL::INTEGER AS district_id,
              NULL::INTEGER AS mukim_id,
              NULL::INTEGER AS locality_id,
              NULL::INTEGER AS postcode_id
            FROM {district} d
            WHERE d.district_id = %s
            """
        )
    if entity == "mukim":
        return sql.SQL(
            """
            SELECT
              m.mukim_id AS entity_id,
              m.mukim_name AS entity_name,
              m.mukim_code AS entity_code,
              d.state_id,
              m.district_id,
              m.mukim_id,
              NULL::INTEGER AS locality_id,
              NULL::INTEGER AS postcode_id
            FROM {mukim} m
            JOIN {district} d ON d.district_id = m.district_id
            WHERE m.mukim_id = %s
            """
        )
    if entity == "locality":
        return sql.SQL(
            """
            SELECT
              l.locality_id AS entity_id,
              l.locality_name AS entity_name,
              l.locality_code AS entity_code,
              d.state_id,
              m.district_id,
              l.mukim_id,
              l.locality_id,
              NULL::INTEGER AS postcode_id
            FROM {locality} l
            LEFT JOIN {mukim} m ON m.mukim_id = l.mukim_id
            LEFT JOIN {district} d ON d.district_id = m.district_id
            WHERE l.locality_id = %s
            """
        )
    return sql.SQL(
        """
        SELECT
          p.postcode_id AS entity_id,
          p.postcode_name AS entity_name,
          p.postcode AS entity_code,
          d.state_id,
          m.district_id,
          l.mukim_id,
          p.locality_id,
          p.postcode_id
        FROM {postcode} p
        LEFT JOIN {locality} l ON l.locality_id = p.locality_id
        LEFT JOIN {mukim} m ON m.mukim_id = l.mukim_id
        LEFT JOIN {district} d ON d.district_id = m.district_id
        WHERE p.postcode_id = %s
        """
    )


def _get_context(*, cur, schema_name: str, entity: str, ref_id: int) -> dict | None:
    assert sql is not None
    stmt = _context_query(entity).format(
        district=_tbl(schema_name, "district"),
        mukim=_tbl(schema_name, "mukim"),
        locality=_tbl(schema_name, "locality"),
        postcode=_tbl(schema_name, "postcode"),
    )
    cur.execute(stmt, (ref_id,))
    row = cur.fetchone()
    return dict(row) if row else None


def _impact_summary(*, cur, runtime_schema: str, entity: str, from_id: int, to_ctx: dict[str, object]) -> dict[str, int]:
    assert sql is not None
    target_mukim = to_ctx.get("mukim_id")
    target_locality = to_ctx.get("locality_id")
    naskod_present = _table_exists(cur=cur, schema_name=runtime_schema, table_name="naskod")
    naskod_join = sql.SQL(
        "LEFT JOIN {naskod} n ON n.address_id = sa.address_id AND n.is_vanity = FALSE"
    ).format(naskod=_tbl(runtime_schema, "naskod")) if naskod_present else sql.SQL("")
    naskod_select = sql.SQL(
        "COUNT(*) FILTER (WHERE n.code IS NOT NULL)::INTEGER AS preserved_naskod_rows,"
    ) if naskod_present else sql.SQL("0::INTEGER AS preserved_naskod_rows,")
    stmt = sql.SQL(
        """
        SELECT
          COUNT(*)::INTEGER AS affected_rows,
          {naskod_select}
          COUNT(*) FILTER (
            WHERE sa.mukim_id IS NOT NULL
              AND (%s IS NULL OR sa.mukim_id <> %s)
          )::INTEGER AS mukim_will_change,
          COUNT(*) FILTER (
            WHERE sa.locality_id IS NOT NULL
              AND (
                %s IS NULL
                OR NOT EXISTS (
                  SELECT 1
                  FROM {locality} l
                  WHERE l.locality_id = sa.locality_id
                    AND l.locality_id = %s
                )
              )
          )::INTEGER AS locality_will_clear_or_change,
          COUNT(*) FILTER (
            WHERE sa.postcode_id IS NOT NULL
              AND (
                %s IS NULL
                OR sa.postcode_id <> %s
              )
          )::INTEGER AS postcode_will_clear_or_change,
          COUNT(*) FILTER (
            WHERE sa.street_id IS NOT NULL
              AND (
                %s IS NULL
                OR NOT EXISTS (
                  SELECT 1
                  FROM {street} st
                  WHERE st.street_id = sa.street_id
                    AND st.locality_id = %s
                )
              )
          )::INTEGER AS street_will_clear
        FROM {address} sa
        {naskod_join}
        WHERE sa.{entity_column} = %s
        """
    ).format(
        locality=_tbl(runtime_schema, "locality"),
        street=_tbl(runtime_schema, "street"),
        address=_tbl(runtime_schema, "standardized_address"),
        naskod_join=naskod_join,
        naskod_select=naskod_select,
        entity_column=sql.Identifier(_entity_column(entity)),
    )
    cur.execute(
        stmt,
        (
            target_mukim,
            target_mukim,
            target_locality,
            target_locality,
            to_ctx.get("postcode_id"),
            to_ctx.get("postcode_id"),
            target_locality,
            target_locality,
            from_id,
        ),
    )
    row = cur.fetchone()
    return {key: int(value or 0) for key, value in dict(row or {}).items()}


def _apply_backfill(*, cur, runtime_schema: str, entity: str, from_id: int, to_ctx: dict[str, object]) -> int:
    assert sql is not None
    stmt = sql.SQL(
        """
        WITH updated AS (
          UPDATE {address} sa
          SET
            state_id = %s,
            district_id = %s,
            mukim_id = CASE
              WHEN %s IS NULL THEN NULL
              ELSE %s
            END,
            locality_id = CASE
              WHEN %s IS NULL THEN NULL
              ELSE %s
            END,
            postcode_id = CASE
              WHEN %s IS NULL THEN NULL
              ELSE %s
            END,
            street_id = CASE
              WHEN sa.street_id IS NOT NULL
               AND %s IS NOT NULL
               AND EXISTS (
                 SELECT 1
                 FROM {street} st
                 WHERE st.street_id = sa.street_id
                   AND st.locality_id = %s
               )
              THEN sa.street_id
              ELSE NULL
            END,
            updated_at = NOW()
          WHERE sa.{entity_column} = %s
          RETURNING sa.address_id
        )
        SELECT COUNT(*)::INTEGER AS updated_count
        FROM updated
        """
    ).format(
        address=_tbl(runtime_schema, "standardized_address"),
        street=_tbl(runtime_schema, "street"),
        entity_column=sql.Identifier(_entity_column(entity)),
    )
    cur.execute(
        stmt,
        (
            to_ctx.get("state_id"),
            to_ctx.get("district_id"),
            to_ctx.get("mukim_id"),
            to_ctx.get("mukim_id"),
            to_ctx.get("locality_id"),
            to_ctx.get("locality_id"),
            to_ctx.get("postcode_id"),
            to_ctx.get("postcode_id"),
            to_ctx.get("locality_id"),
            to_ctx.get("locality_id"),
            from_id,
        ),
    )
    row = cur.fetchone()
    return int(row["updated_count"]) if row else 0


def main() -> None:
    args = parse_args()
    if args.from_id <= 0 or args.to_id <= 0:
        raise SystemExit("from-id and to-id must be positive")
    if args.from_id == args.to_id:
        raise SystemExit("from-id and to-id must differ")

    dsn = _build_dsn(args)
    with _connect(dsn) as conn, conn.cursor() as cur:
        _require_tables(cur=cur, lookup_schema=args.lookup_schema, runtime_schema=args.schema)
        source_lookup = _get_context(cur=cur, schema_name=args.lookup_schema, entity=args.entity, ref_id=args.from_id)
        target_lookup = _get_context(cur=cur, schema_name=args.lookup_schema, entity=args.entity, ref_id=args.to_id)
        source_runtime = _get_context(cur=cur, schema_name=args.schema, entity=args.entity, ref_id=args.from_id)
        target_runtime = _get_context(cur=cur, schema_name=args.schema, entity=args.entity, ref_id=args.to_id)
        if source_lookup is None or source_runtime is None:
            raise SystemExit(f"from-id={args.from_id} not found in lookup/runtime schemas for entity={args.entity}")
        if target_lookup is None or target_runtime is None:
            raise SystemExit(f"to-id={args.to_id} not found in lookup/runtime schemas for entity={args.entity}")

        summary = _impact_summary(
            cur=cur,
            runtime_schema=args.schema,
            entity=args.entity,
            from_id=args.from_id,
            to_ctx=target_runtime,
        )
        payload: dict[str, object] = {
            "entity": args.entity,
            "mode": "apply" if args.apply else "dry_run",
            "runtime_schema": args.schema,
            "lookup_schema": args.lookup_schema,
            "from_reference": source_lookup,
            "to_reference": target_lookup,
            "impact": summary,
            "naskod_policy": "preserve_existing_code",
        }
        if args.apply:
            updated_count = _apply_backfill(
                cur=cur,
                runtime_schema=args.schema,
                entity=args.entity,
                from_id=args.from_id,
                to_ctx=target_runtime,
            )
            conn.commit()
            payload["updated_count"] = updated_count
        print(json.dumps(payload, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
