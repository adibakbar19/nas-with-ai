from __future__ import annotations

import argparse
import json
import os
import re
from urllib.parse import quote_plus

try:
    import psycopg
    from psycopg.rows import dict_row
except Exception:  # pragma: no cover
    psycopg = None
    dict_row = None


_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Validate a staged lookup refresh and optionally apply the first safe "
            "merge/upsert slice for state and postcode."
        )
    )
    parser.add_argument("--schema", default=os.getenv("LOOKUP_SCHEMA", "nas_lookup"), help="Canonical lookup schema")
    parser.add_argument("--runtime-schema", default=os.getenv("PGSCHEMA", "nas"), help="Optional runtime schema to mirror state/postcode into")
    parser.add_argument("--staging-schema", default=os.getenv("LOOKUP_STAGING_SCHEMA", "nas_lookup_staging"), help="Staging schema")
    parser.add_argument("--refresh-batch-id", required=True, help="Refresh batch id from the lookup staging process")
    parser.add_argument("--warn-row-delta-pct", type=float, default=10.0, help="Warn when staged vs canonical row count delta exceeds this percent")
    parser.add_argument("--block-row-delta-pct", type=float, default=25.0, help="Block apply when staged vs canonical row count delta exceeds this percent")
    parser.add_argument("--dsn", default=None, help="Postgres DSN")
    parser.add_argument("--host", default=None)
    parser.add_argument("--port", default=None)
    parser.add_argument("--database", default=None)
    parser.add_argument("--user", default=os.getenv("PGUSER"))
    parser.add_argument("--password", default=os.getenv("PGPASSWORD"))
    parser.add_argument("--apply", action="store_true", help="Apply the safe merge/upsert slice for state and postcode")
    return parser.parse_args()


def _build_dsn(args: argparse.Namespace) -> str:
    if args.dsn:
        return args.dsn
    host = args.host or os.getenv("PGHOST", "localhost")
    port = args.port or os.getenv("PGPORT", "5432")
    database = args.database or os.getenv("PGDATABASE", "postgres")
    user = quote_plus(str(args.user or os.getenv("PGUSER", "postgres")))
    password = quote_plus(str(args.password or os.getenv("PGPASSWORD", "")))
    return f"postgresql://{user}:{password}@{host}:{port}/{database}"


def _connect(dsn: str):
    if psycopg is None or dict_row is None:
        raise RuntimeError("psycopg is required for apply_lookup_refresh.py")
    return psycopg.connect(dsn, row_factory=dict_row)


def _quote_ident(name: str) -> str:
    if not _IDENT_RE.fullmatch(name):
        raise ValueError(f"Invalid identifier: {name!r}")
    return f'"{name}"'


def _table_name(schema_name: str, table_name: str) -> str:
    return f"{_quote_ident(schema_name)}.{_quote_ident(table_name)}"


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


def _count_rows(*, cur, schema_name: str, table_name: str, refresh_batch_id: str | None = None) -> int:
    table_ref = _table_name(schema_name, table_name)
    if refresh_batch_id is None:
        cur.execute(f"SELECT COUNT(*)::INTEGER AS row_count FROM {table_ref}")
    else:
        cur.execute(
            f"SELECT COUNT(*)::INTEGER AS row_count FROM {table_ref} WHERE refresh_batch_id = %s",
            (refresh_batch_id,),
        )
    row = cur.fetchone()
    return int((row or {}).get("row_count") or 0)


def _ensure_tracking_tables(*, cur, staging_schema: str) -> None:
    staging = _quote_ident(staging_schema)
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {staging}")
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {staging}."lookup_refresh_validation" (
          validation_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
          refresh_batch_id TEXT NOT NULL,
          check_name TEXT NOT NULL,
          severity TEXT NOT NULL,
          status TEXT NOT NULL,
          row_count INTEGER NOT NULL,
          details JSONB NOT NULL DEFAULT '{{}}'::jsonb,
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """
    )
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {staging}."lookup_refresh_diff" (
          diff_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
          refresh_batch_id TEXT NOT NULL,
          table_name TEXT NOT NULL,
          inserted_count INTEGER NOT NULL DEFAULT 0,
          updated_count INTEGER NOT NULL DEFAULT 0,
          unchanged_count INTEGER NOT NULL DEFAULT 0,
          removed_candidate_count INTEGER NOT NULL DEFAULT 0,
          details JSONB NOT NULL DEFAULT '{{}}'::jsonb,
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """
    )
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {staging}."lookup_refresh_apply_result" (
          result_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
          refresh_batch_id TEXT NOT NULL,
          status TEXT NOT NULL,
          details JSONB NOT NULL DEFAULT '{{}}'::jsonb,
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """
    )


def _ensure_batch_exists(*, cur, staging_schema: str, refresh_batch_id: str) -> None:
    table_ref = _table_name(staging_schema, "lookup_refresh_batch")
    cur.execute(
        f"SELECT refresh_batch_id FROM {table_ref} WHERE refresh_batch_id = %s LIMIT 1",
        (refresh_batch_id,),
    )
    if cur.fetchone() is None:
        raise ValueError(f"refresh_batch_id not found in {staging_schema}.lookup_refresh_batch: {refresh_batch_id}")


def _duplicate_count_query(*, schema_name: str, table_name: str, refresh_batch_id: str, key_columns: list[str]) -> tuple[str, tuple[object, ...]]:
    cols = ", ".join(_quote_ident(col) for col in key_columns)
    not_blank = " AND ".join(
        f"NULLIF(TRIM(COALESCE({_quote_ident(col)}::text, '')), '') IS NOT NULL" for col in key_columns
    )
    stmt = f"""
        WITH dupes AS (
          SELECT {cols}, COUNT(*)::INTEGER AS duplicate_count
          FROM {_table_name(schema_name, table_name)}
          WHERE refresh_batch_id = %s
            AND {not_blank}
          GROUP BY {cols}
          HAVING COUNT(*) > 1
        )
        SELECT COALESCE(SUM(duplicate_count - 1), 0)::INTEGER AS row_count
        FROM dupes
    """
    return stmt, (refresh_batch_id,)


def _single_count(*, cur, stmt: str, params: tuple[object, ...]) -> int:
    cur.execute(stmt, params)
    row = cur.fetchone()
    return int((row or {}).get("row_count") or 0)


def _pct_delta(*, canonical_rows: int, staged_rows: int) -> float:
    if canonical_rows == 0:
        return 0.0 if staged_rows == 0 else 100.0
    return abs(staged_rows - canonical_rows) * 100.0 / canonical_rows


def _build_validation_rows(
    *,
    cur,
    canonical_schema: str,
    staging_schema: str,
    refresh_batch_id: str,
    warn_row_delta_pct: float,
    block_row_delta_pct: float,
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []

    def add_check(
        name: str,
        severity: str,
        row_count: int,
        details: dict[str, object] | None = None,
        *,
        status_override: str | None = None,
    ) -> None:
        if status_override is not None:
            status = status_override
        elif row_count <= 0:
            status = "pass"
        elif severity == "error":
            status = "blocked"
        else:
            status = "warning"
        rows.append(
            {
                "check_name": name,
                "severity": severity,
                "status": status,
                "row_count": int(row_count),
                "details": details or {},
            }
        )

    state_null_code = _single_count(
        cur=cur,
        stmt=f"""
            SELECT COUNT(*)::INTEGER AS row_count
            FROM {_table_name(staging_schema, "state_stage")}
            WHERE refresh_batch_id = %s
              AND NULLIF(TRIM(COALESCE(state_code::text, '')), '') IS NULL
        """,
        params=(refresh_batch_id,),
    )
    add_check("state_missing_state_code", "error", state_null_code)

    state_null_name = _single_count(
        cur=cur,
        stmt=f"""
            SELECT COUNT(*)::INTEGER AS row_count
            FROM {_table_name(staging_schema, "state_stage")}
            WHERE refresh_batch_id = %s
              AND NULLIF(TRIM(COALESCE(state_name::text, '')), '') IS NULL
        """,
        params=(refresh_batch_id,),
    )
    add_check("state_missing_state_name", "error", state_null_name)

    stmt, params = _duplicate_count_query(
        schema_name=staging_schema,
        table_name="state_stage",
        refresh_batch_id=refresh_batch_id,
        key_columns=["state_code"],
    )
    add_check("state_duplicate_state_code", "error", _single_count(cur=cur, stmt=stmt, params=params))

    postcode_null_code = _single_count(
        cur=cur,
        stmt=f"""
            SELECT COUNT(*)::INTEGER AS row_count
            FROM {_table_name(staging_schema, "postcode_stage")}
            WHERE refresh_batch_id = %s
              AND NULLIF(TRIM(COALESCE(postcode::text, '')), '') IS NULL
        """,
        params=(refresh_batch_id,),
    )
    add_check("postcode_missing_postcode", "error", postcode_null_code)

    postcode_null_name = _single_count(
        cur=cur,
        stmt=f"""
            SELECT COUNT(*)::INTEGER AS row_count
            FROM {_table_name(staging_schema, "postcode_stage")}
            WHERE refresh_batch_id = %s
              AND NULLIF(TRIM(COALESCE(postcode_name::text, '')), '') IS NULL
        """,
        params=(refresh_batch_id,),
    )
    add_check("postcode_missing_postcode_name", "error", postcode_null_name)

    stmt, params = _duplicate_count_query(
        schema_name=staging_schema,
        table_name="postcode_stage",
        refresh_batch_id=refresh_batch_id,
        key_columns=["postcode"],
    )
    add_check("postcode_duplicate_postcode", "error", _single_count(cur=cur, stmt=stmt, params=params))

    unmatched_queries = {
        "state_boundary_unmatched_state_id": "state_boundary_stage",
        "district_boundary_unmatched_district_id": "district_boundary_stage",
        "mukim_boundary_unmatched_mukim_id": "mukim_boundary_stage",
        "postcode_boundary_unmatched_postcode_id": "postcode_boundary_stage",
    }
    unmatched_columns = {
        "state_boundary_unmatched_state_id": "state_id",
        "district_boundary_unmatched_district_id": "district_id",
        "mukim_boundary_unmatched_mukim_id": "mukim_id",
        "postcode_boundary_unmatched_postcode_id": "postcode_id",
    }
    for check_name, table_name in unmatched_queries.items():
        row_count = _single_count(
            cur=cur,
            stmt=f"""
                SELECT COUNT(*)::INTEGER AS row_count
                FROM {_table_name(staging_schema, table_name)}
                WHERE refresh_batch_id = %s
                  AND {unmatched_columns[check_name]} IS NULL
            """,
            params=(refresh_batch_id,),
        )
        add_check(check_name, "error", row_count)

    postcode_unresolved_locality = _single_count(
        cur=cur,
        stmt=f"""
            WITH staged AS (
              SELECT DISTINCT ON (p.postcode)
                TRIM(COALESCE(p.postcode::text, '')) AS postcode,
                UPPER(TRIM(COALESCE(p.postcode_name::text, ''))) AS postcode_name
              FROM {_table_name(staging_schema, "postcode_stage")} p
              WHERE p.refresh_batch_id = %s
                AND NULLIF(TRIM(COALESCE(p.postcode::text, '')), '') IS NOT NULL
              ORDER BY p.postcode, p.staged_at DESC NULLS LAST
            )
            SELECT COUNT(*)::INTEGER AS row_count
            FROM staged s
            LEFT JOIN LATERAL (
              SELECT l.locality_id
              FROM {_table_name(canonical_schema, "locality")} l
              WHERE UPPER(TRIM(COALESCE(l.locality_name::text, ''))) = s.postcode_name
              ORDER BY l.locality_id ASC
              LIMIT 1
            ) loc ON TRUE
            WHERE loc.locality_id IS NULL
        """,
        params=(refresh_batch_id,),
    )
    add_check("postcode_unresolved_locality_hint", "warning", postcode_unresolved_locality)

    for table_name in ("state", "postcode"):
        canonical_rows = _count_rows(cur=cur, schema_name=canonical_schema, table_name=table_name)
        staged_rows = _count_rows(cur=cur, schema_name=staging_schema, table_name=f"{table_name}_stage", refresh_batch_id=refresh_batch_id)
        pct = _pct_delta(canonical_rows=canonical_rows, staged_rows=staged_rows)
        if pct >= block_row_delta_pct:
            severity = "error"
            status_override = None
        elif pct >= warn_row_delta_pct:
            severity = "warning"
            status_override = None
        else:
            severity = "warning"
            status_override = "pass"
        add_check(
            f"{table_name}_row_delta_pct",
            severity,
            int(abs(staged_rows - canonical_rows)),
            {"canonical_rows": canonical_rows, "staged_rows": staged_rows, "delta_pct": round(pct, 3)},
            status_override=status_override,
        )

    return rows


def _persist_validation_rows(*, cur, staging_schema: str, refresh_batch_id: str, rows: list[dict[str, object]]) -> None:
    table_ref = _table_name(staging_schema, "lookup_refresh_validation")
    cur.execute(f"DELETE FROM {table_ref} WHERE refresh_batch_id = %s", (refresh_batch_id,))
    for row in rows:
        cur.execute(
            f"""
            INSERT INTO {table_ref}
              (refresh_batch_id, check_name, severity, status, row_count, details)
            VALUES (%s, %s, %s, %s, %s, %s::jsonb)
            """,
            (
                refresh_batch_id,
                row["check_name"],
                row["severity"],
                row["status"],
                int(row["row_count"]),
                json.dumps(row.get("details") or {}),
            ),
        )


def _state_diff(*, cur, canonical_schema: str, staging_schema: str, refresh_batch_id: str) -> dict[str, object]:
    cur.execute(
        f"""
        WITH staged AS (
          SELECT DISTINCT ON (state_code)
            TRIM(COALESCE(state_code::text, '')) AS state_code,
            UPPER(TRIM(COALESCE(state_name::text, ''))) AS state_name
          FROM {_table_name(staging_schema, "state_stage")}
          WHERE refresh_batch_id = %s
            AND NULLIF(TRIM(COALESCE(state_code::text, '')), '') IS NOT NULL
          ORDER BY state_code, staged_at DESC NULLS LAST
        )
        SELECT
          COUNT(*) FILTER (WHERE c.state_id IS NULL)::INTEGER AS inserted_count,
          COUNT(*) FILTER (
            WHERE c.state_id IS NOT NULL
              AND UPPER(TRIM(COALESCE(c.state_name::text, ''))) IS DISTINCT FROM s.state_name
          )::INTEGER AS updated_count,
          COUNT(*) FILTER (
            WHERE c.state_id IS NOT NULL
              AND UPPER(TRIM(COALESCE(c.state_name::text, ''))) = s.state_name
          )::INTEGER AS unchanged_count
        FROM staged s
        LEFT JOIN {_table_name(canonical_schema, "state")} c
          ON TRIM(COALESCE(c.state_code::text, '')) = s.state_code
        """,
        (refresh_batch_id,),
    )
    row = dict(cur.fetchone() or {})
    cur.execute(
        f"""
        WITH staged AS (
          SELECT DISTINCT ON (state_code)
            TRIM(COALESCE(state_code::text, '')) AS state_code
          FROM {_table_name(staging_schema, "state_stage")}
          WHERE refresh_batch_id = %s
            AND NULLIF(TRIM(COALESCE(state_code::text, '')), '') IS NOT NULL
          ORDER BY state_code, staged_at DESC NULLS LAST
        )
        SELECT COUNT(*)::INTEGER AS row_count
        FROM {_table_name(canonical_schema, "state")} c
        LEFT JOIN staged s
          ON TRIM(COALESCE(c.state_code::text, '')) = s.state_code
        WHERE s.state_code IS NULL
        """,
        (refresh_batch_id,),
    )
    removed = int((cur.fetchone() or {}).get("row_count") or 0)
    return {
        "table_name": "state",
        "inserted_count": int(row.get("inserted_count") or 0),
        "updated_count": int(row.get("updated_count") or 0),
        "unchanged_count": int(row.get("unchanged_count") or 0),
        "removed_candidate_count": removed,
        "details": {},
    }


def _postcode_diff(*, cur, canonical_schema: str, staging_schema: str, refresh_batch_id: str) -> dict[str, object]:
    cur.execute(
        f"""
        WITH staged AS (
          SELECT DISTINCT ON (p.postcode)
            TRIM(COALESCE(p.postcode::text, '')) AS postcode,
            UPPER(TRIM(COALESCE(p.postcode_name::text, ''))) AS postcode_name,
            loc.locality_id AS resolved_locality_id
          FROM {_table_name(staging_schema, "postcode_stage")} p
          LEFT JOIN LATERAL (
            SELECT l.locality_id
            FROM {_table_name(canonical_schema, "locality")} l
            WHERE UPPER(TRIM(COALESCE(l.locality_name::text, ''))) = UPPER(TRIM(COALESCE(p.postcode_name::text, '')))
            ORDER BY l.locality_id ASC
            LIMIT 1
          ) loc ON TRUE
          WHERE p.refresh_batch_id = %s
            AND NULLIF(TRIM(COALESCE(p.postcode::text, '')), '') IS NOT NULL
          ORDER BY p.postcode, p.staged_at DESC NULLS LAST
        )
        SELECT
          COUNT(*) FILTER (WHERE c.postcode_id IS NULL)::INTEGER AS inserted_count,
          COUNT(*) FILTER (
            WHERE c.postcode_id IS NOT NULL
              AND (
                UPPER(TRIM(COALESCE(c.postcode_name::text, ''))) IS DISTINCT FROM s.postcode_name
                OR (s.resolved_locality_id IS NOT NULL AND c.locality_id IS DISTINCT FROM s.resolved_locality_id)
              )
          )::INTEGER AS updated_count,
          COUNT(*) FILTER (
            WHERE c.postcode_id IS NOT NULL
              AND UPPER(TRIM(COALESCE(c.postcode_name::text, ''))) = s.postcode_name
              AND (s.resolved_locality_id IS NULL OR c.locality_id IS NOT DISTINCT FROM s.resolved_locality_id)
          )::INTEGER AS unchanged_count,
          COUNT(*) FILTER (WHERE s.resolved_locality_id IS NULL)::INTEGER AS unresolved_locality_count
        FROM staged s
        LEFT JOIN {_table_name(canonical_schema, "postcode")} c
          ON TRIM(COALESCE(c.postcode::text, '')) = s.postcode
        """,
        (refresh_batch_id,),
    )
    row = dict(cur.fetchone() or {})
    cur.execute(
        f"""
        WITH staged AS (
          SELECT DISTINCT ON (postcode)
            TRIM(COALESCE(postcode::text, '')) AS postcode
          FROM {_table_name(staging_schema, "postcode_stage")}
          WHERE refresh_batch_id = %s
            AND NULLIF(TRIM(COALESCE(postcode::text, '')), '') IS NOT NULL
          ORDER BY postcode, staged_at DESC NULLS LAST
        )
        SELECT COUNT(*)::INTEGER AS row_count
        FROM {_table_name(canonical_schema, "postcode")} c
        LEFT JOIN staged s
          ON TRIM(COALESCE(c.postcode::text, '')) = s.postcode
        WHERE s.postcode IS NULL
        """,
        (refresh_batch_id,),
    )
    removed = int((cur.fetchone() or {}).get("row_count") or 0)
    return {
        "table_name": "postcode",
        "inserted_count": int(row.get("inserted_count") or 0),
        "updated_count": int(row.get("updated_count") or 0),
        "unchanged_count": int(row.get("unchanged_count") or 0),
        "removed_candidate_count": removed,
        "details": {"unresolved_locality_count": int(row.get("unresolved_locality_count") or 0)},
    }


def _persist_diffs(*, cur, staging_schema: str, refresh_batch_id: str, rows: list[dict[str, object]]) -> None:
    table_ref = _table_name(staging_schema, "lookup_refresh_diff")
    cur.execute(f"DELETE FROM {table_ref} WHERE refresh_batch_id = %s", (refresh_batch_id,))
    for row in rows:
        cur.execute(
            f"""
            INSERT INTO {table_ref}
              (refresh_batch_id, table_name, inserted_count, updated_count, unchanged_count, removed_candidate_count, details)
            VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb)
            """,
            (
                refresh_batch_id,
                row["table_name"],
                int(row["inserted_count"]),
                int(row["updated_count"]),
                int(row["unchanged_count"]),
                int(row["removed_candidate_count"]),
                json.dumps(row.get("details") or {}),
            ),
        )


def _overall_status(validation_rows: list[dict[str, object]]) -> str:
    statuses = {str(row["status"]) for row in validation_rows}
    if "blocked" in statuses:
        return "blocked"
    if "warning" in statuses:
        return "warning"
    return "ok"


def _apply_state_upsert(*, cur, target_schema: str, staging_schema: str, refresh_batch_id: str) -> dict[str, int]:
    cur.execute(
        f"""
        WITH staged AS (
          SELECT DISTINCT ON (state_code)
            TRIM(COALESCE(state_code::text, '')) AS state_code,
            UPPER(TRIM(COALESCE(state_name::text, ''))) AS state_name
          FROM {_table_name(staging_schema, "state_stage")}
          WHERE refresh_batch_id = %s
            AND NULLIF(TRIM(COALESCE(state_code::text, '')), '') IS NOT NULL
          ORDER BY state_code, staged_at DESC NULLS LAST
        )
        UPDATE {_table_name(target_schema, "state")} t
        SET state_name = s.state_name
        FROM staged s
        WHERE TRIM(COALESCE(t.state_code::text, '')) = s.state_code
          AND UPPER(TRIM(COALESCE(t.state_name::text, ''))) IS DISTINCT FROM s.state_name
        """,
        (refresh_batch_id,),
    )
    updated = int(cur.rowcount or 0)
    cur.execute(
        f"""
        WITH staged AS (
          SELECT DISTINCT ON (state_code)
            TRIM(COALESCE(state_code::text, '')) AS state_code,
            UPPER(TRIM(COALESCE(state_name::text, ''))) AS state_name
          FROM {_table_name(staging_schema, "state_stage")}
          WHERE refresh_batch_id = %s
            AND NULLIF(TRIM(COALESCE(state_code::text, '')), '') IS NOT NULL
          ORDER BY state_code, staged_at DESC NULLS LAST
        ),
        new_rows AS (
          SELECT
            COALESCE((SELECT MAX(state_id) FROM {_table_name(target_schema, "state")}), 0)
              + ROW_NUMBER() OVER (ORDER BY s.state_code) AS state_id,
            s.state_name,
            s.state_code
          FROM staged s
          LEFT JOIN {_table_name(target_schema, "state")} t
            ON TRIM(COALESCE(t.state_code::text, '')) = s.state_code
          WHERE t.state_id IS NULL
        )
        INSERT INTO {_table_name(target_schema, "state")} (state_id, state_name, state_code)
        SELECT state_id, state_name, state_code
        FROM new_rows
        """,
        (refresh_batch_id,),
    )
    inserted = int(cur.rowcount or 0)
    return {"inserted_count": inserted, "updated_count": updated}


def _apply_postcode_upsert(*, cur, target_schema: str, staging_schema: str, refresh_batch_id: str) -> dict[str, int]:
    staged_cte = f"""
        WITH staged AS (
          SELECT DISTINCT ON (p.postcode)
            TRIM(COALESCE(p.postcode::text, '')) AS postcode,
            UPPER(TRIM(COALESCE(p.postcode_name::text, ''))) AS postcode_name,
            loc.locality_id AS locality_id
          FROM {_table_name(staging_schema, "postcode_stage")} p
          LEFT JOIN LATERAL (
            SELECT l.locality_id
            FROM {_table_name(target_schema, "locality")} l
            WHERE UPPER(TRIM(COALESCE(l.locality_name::text, ''))) = UPPER(TRIM(COALESCE(p.postcode_name::text, '')))
            ORDER BY l.locality_id ASC
            LIMIT 1
          ) loc ON TRUE
          WHERE p.refresh_batch_id = %s
            AND NULLIF(TRIM(COALESCE(p.postcode::text, '')), '') IS NOT NULL
          ORDER BY p.postcode, p.staged_at DESC NULLS LAST
        )
    """
    cur.execute(
        staged_cte
        + f"""
        UPDATE {_table_name(target_schema, "postcode")} t
        SET postcode_name = s.postcode_name,
            locality_id = COALESCE(s.locality_id, t.locality_id)
        FROM staged s
        WHERE TRIM(COALESCE(t.postcode::text, '')) = s.postcode
          AND (
            UPPER(TRIM(COALESCE(t.postcode_name::text, ''))) IS DISTINCT FROM s.postcode_name
            OR (s.locality_id IS NOT NULL AND t.locality_id IS DISTINCT FROM s.locality_id)
          )
        """,
        (refresh_batch_id,),
    )
    updated = int(cur.rowcount or 0)
    cur.execute(
        staged_cte
        + f"""
        , new_rows AS (
          SELECT
            COALESCE((SELECT MAX(postcode_id) FROM {_table_name(target_schema, "postcode")}), 0)
              + ROW_NUMBER() OVER (ORDER BY s.postcode) AS postcode_id,
            s.postcode_name,
            s.locality_id,
            s.postcode
          FROM staged s
          LEFT JOIN {_table_name(target_schema, "postcode")} t
            ON TRIM(COALESCE(t.postcode::text, '')) = s.postcode
          WHERE t.postcode_id IS NULL
        )
        INSERT INTO {_table_name(target_schema, "postcode")} (postcode_id, postcode_name, locality_id, postcode)
        SELECT postcode_id, postcode_name, locality_id, postcode
        FROM new_rows
        """,
        (refresh_batch_id,),
    )
    inserted = int(cur.rowcount or 0)
    return {"inserted_count": inserted, "updated_count": updated}


def _mirror_state_from_canonical(*, cur, canonical_schema: str, runtime_schema: str, staging_schema: str, refresh_batch_id: str) -> dict[str, int]:
    if canonical_schema == runtime_schema or not _table_exists(cur=cur, schema_name=runtime_schema, table_name="state"):
        return {"inserted_count": 0, "updated_count": 0}
    cur.execute(
        f"""
        WITH staged AS (
          SELECT DISTINCT ON (state_code)
            TRIM(COALESCE(state_code::text, '')) AS state_code
          FROM {_table_name(staging_schema, "state_stage")}
          WHERE refresh_batch_id = %s
            AND NULLIF(TRIM(COALESCE(state_code::text, '')), '') IS NOT NULL
          ORDER BY state_code, staged_at DESC NULLS LAST
        ),
        subset AS (
          SELECT c.state_id, c.state_name, c.state_code
          FROM {_table_name(canonical_schema, "state")} c
          JOIN staged s
            ON TRIM(COALESCE(c.state_code::text, '')) = s.state_code
        )
        UPDATE {_table_name(runtime_schema, "state")} r
        SET state_name = s.state_name,
            state_code = s.state_code
        FROM subset s
        WHERE TRIM(COALESCE(r.state_code::text, '')) = TRIM(COALESCE(s.state_code::text, ''))
          AND (
            UPPER(TRIM(COALESCE(r.state_name::text, ''))) IS DISTINCT FROM UPPER(TRIM(COALESCE(s.state_name::text, '')))
          )
        """,
        (refresh_batch_id,),
    )
    updated = int(cur.rowcount or 0)
    cur.execute(
        f"""
        WITH staged AS (
          SELECT DISTINCT ON (state_code)
            TRIM(COALESCE(state_code::text, '')) AS state_code
          FROM {_table_name(staging_schema, "state_stage")}
          WHERE refresh_batch_id = %s
            AND NULLIF(TRIM(COALESCE(state_code::text, '')), '') IS NOT NULL
          ORDER BY state_code, staged_at DESC NULLS LAST
        ),
        subset AS (
          SELECT c.state_id, c.state_name, c.state_code
          FROM {_table_name(canonical_schema, "state")} c
          JOIN staged s
            ON TRIM(COALESCE(c.state_code::text, '')) = s.state_code
        )
        INSERT INTO {_table_name(runtime_schema, "state")} (state_id, state_name, state_code)
        SELECT s.state_id, s.state_name, s.state_code
        FROM subset s
        LEFT JOIN {_table_name(runtime_schema, "state")} r
          ON TRIM(COALESCE(r.state_code::text, '')) = TRIM(COALESCE(s.state_code::text, ''))
        WHERE r.state_id IS NULL
        """,
        (refresh_batch_id,),
    )
    inserted = int(cur.rowcount or 0)
    return {"inserted_count": inserted, "updated_count": updated}


def _mirror_postcode_from_canonical(*, cur, canonical_schema: str, runtime_schema: str, staging_schema: str, refresh_batch_id: str) -> dict[str, int]:
    if canonical_schema == runtime_schema or not _table_exists(cur=cur, schema_name=runtime_schema, table_name="postcode"):
        return {"inserted_count": 0, "updated_count": 0}
    cur.execute(
        f"""
        WITH staged AS (
          SELECT DISTINCT ON (postcode)
            TRIM(COALESCE(postcode::text, '')) AS postcode
          FROM {_table_name(staging_schema, "postcode_stage")}
          WHERE refresh_batch_id = %s
            AND NULLIF(TRIM(COALESCE(postcode::text, '')), '') IS NOT NULL
          ORDER BY postcode, staged_at DESC NULLS LAST
        ),
        subset AS (
          SELECT c.postcode_id, c.postcode_name, c.locality_id, c.postcode
          FROM {_table_name(canonical_schema, "postcode")} c
          JOIN staged s
            ON TRIM(COALESCE(c.postcode::text, '')) = s.postcode
        )
        UPDATE {_table_name(runtime_schema, "postcode")} r
        SET postcode_name = s.postcode_name,
            locality_id = s.locality_id,
            postcode = s.postcode
        FROM subset s
        WHERE TRIM(COALESCE(r.postcode::text, '')) = TRIM(COALESCE(s.postcode::text, ''))
          AND (
            UPPER(TRIM(COALESCE(r.postcode_name::text, ''))) IS DISTINCT FROM UPPER(TRIM(COALESCE(s.postcode_name::text, '')))
            OR r.locality_id IS DISTINCT FROM s.locality_id
          )
        """,
        (refresh_batch_id,),
    )
    updated = int(cur.rowcount or 0)
    cur.execute(
        f"""
        WITH staged AS (
          SELECT DISTINCT ON (postcode)
            TRIM(COALESCE(postcode::text, '')) AS postcode
          FROM {_table_name(staging_schema, "postcode_stage")}
          WHERE refresh_batch_id = %s
            AND NULLIF(TRIM(COALESCE(postcode::text, '')), '') IS NOT NULL
          ORDER BY postcode, staged_at DESC NULLS LAST
        ),
        subset AS (
          SELECT c.postcode_id, c.postcode_name, c.locality_id, c.postcode
          FROM {_table_name(canonical_schema, "postcode")} c
          JOIN staged s
            ON TRIM(COALESCE(c.postcode::text, '')) = s.postcode
        )
        INSERT INTO {_table_name(runtime_schema, "postcode")} (postcode_id, postcode_name, locality_id, postcode)
        SELECT s.postcode_id, s.postcode_name, s.locality_id, s.postcode
        FROM subset s
        LEFT JOIN {_table_name(runtime_schema, "postcode")} r
          ON TRIM(COALESCE(r.postcode::text, '')) = TRIM(COALESCE(s.postcode::text, ''))
        WHERE r.postcode_id IS NULL
        """,
        (refresh_batch_id,),
    )
    inserted = int(cur.rowcount or 0)
    return {"inserted_count": inserted, "updated_count": updated}


def _persist_apply_result(*, cur, staging_schema: str, refresh_batch_id: str, status: str, details: dict[str, object]) -> None:
    table_ref = _table_name(staging_schema, "lookup_refresh_apply_result")
    cur.execute(f"DELETE FROM {table_ref} WHERE refresh_batch_id = %s", (refresh_batch_id,))
    cur.execute(
        f"""
        INSERT INTO {table_ref} (refresh_batch_id, status, details)
        VALUES (%s, %s, %s::jsonb)
        """,
        (refresh_batch_id, status, json.dumps(details)),
    )


def _summary(*, cur, canonical_schema: str, runtime_schema: str, staging_schema: str, refresh_batch_id: str, warn_row_delta_pct: float, block_row_delta_pct: float) -> dict[str, object]:
    table_names = [
        "state",
        "district",
        "mukim",
        "locality",
        "postcode",
        "pbt",
        "state_boundary",
        "district_boundary",
        "mukim_boundary",
        "postcode_boundary",
    ]
    counts = {}
    for table_name in table_names:
        counts[table_name] = {
            "canonical_rows": _count_rows(cur=cur, schema_name=canonical_schema, table_name=table_name),
            "staged_rows": _count_rows(cur=cur, schema_name=staging_schema, table_name=f"{table_name}_stage", refresh_batch_id=refresh_batch_id),
        }
    validation_rows = _build_validation_rows(
        cur=cur,
        canonical_schema=canonical_schema,
        staging_schema=staging_schema,
        refresh_batch_id=refresh_batch_id,
        warn_row_delta_pct=warn_row_delta_pct,
        block_row_delta_pct=block_row_delta_pct,
    )
    _persist_validation_rows(cur=cur, staging_schema=staging_schema, refresh_batch_id=refresh_batch_id, rows=validation_rows)
    diff_rows = [
        _state_diff(cur=cur, canonical_schema=canonical_schema, staging_schema=staging_schema, refresh_batch_id=refresh_batch_id),
        _postcode_diff(cur=cur, canonical_schema=canonical_schema, staging_schema=staging_schema, refresh_batch_id=refresh_batch_id),
    ]
    _persist_diffs(cur=cur, staging_schema=staging_schema, refresh_batch_id=refresh_batch_id, rows=diff_rows)
    return {
        "refresh_batch_id": refresh_batch_id,
        "canonical_schema": canonical_schema,
        "runtime_schema": runtime_schema,
        "staging_schema": staging_schema,
        "table_counts": counts,
        "validation": validation_rows,
        "diff": diff_rows,
        "status": _overall_status(validation_rows),
        "apply_supported": ["state", "postcode"],
    }


def main() -> None:
    args = parse_args()
    dsn = _build_dsn(args)
    with _connect(dsn) as conn, conn.cursor() as cur:
        _ensure_tracking_tables(cur=cur, staging_schema=args.staging_schema)
        _ensure_batch_exists(cur=cur, staging_schema=args.staging_schema, refresh_batch_id=args.refresh_batch_id)
        summary = _summary(
            cur=cur,
            canonical_schema=args.schema,
            runtime_schema=args.runtime_schema,
            staging_schema=args.staging_schema,
            refresh_batch_id=args.refresh_batch_id,
            warn_row_delta_pct=args.warn_row_delta_pct,
            block_row_delta_pct=args.block_row_delta_pct,
        )
        if args.apply:
            if summary["status"] == "blocked":
                _persist_apply_result(
                    cur=cur,
                    staging_schema=args.staging_schema,
                    refresh_batch_id=args.refresh_batch_id,
                    status="blocked",
                    details={"reason": "validation blocked apply", "summary_status": summary["status"]},
                )
                conn.commit()
                raise SystemExit("Validation blocked apply. Inspect lookup_refresh_validation before retrying.")
            apply_details = {
                "canonical": {
                    "state": _apply_state_upsert(
                        cur=cur,
                        target_schema=args.schema,
                        staging_schema=args.staging_schema,
                        refresh_batch_id=args.refresh_batch_id,
                    ),
                    "postcode": _apply_postcode_upsert(
                        cur=cur,
                        target_schema=args.schema,
                        staging_schema=args.staging_schema,
                        refresh_batch_id=args.refresh_batch_id,
                    ),
                },
                "runtime_mirror": {
                    "state": _mirror_state_from_canonical(
                        cur=cur,
                        canonical_schema=args.schema,
                        runtime_schema=args.runtime_schema,
                        staging_schema=args.staging_schema,
                        refresh_batch_id=args.refresh_batch_id,
                    ),
                    "postcode": _mirror_postcode_from_canonical(
                        cur=cur,
                        canonical_schema=args.schema,
                        runtime_schema=args.runtime_schema,
                        staging_schema=args.staging_schema,
                        refresh_batch_id=args.refresh_batch_id,
                    ),
                },
            }
            summary["apply"] = apply_details
            _persist_apply_result(
                cur=cur,
                staging_schema=args.staging_schema,
                refresh_batch_id=args.refresh_batch_id,
                status="applied",
                details=apply_details,
            )
        else:
            _persist_apply_result(
                cur=cur,
                staging_schema=args.staging_schema,
                refresh_batch_id=args.refresh_batch_id,
                status="dry_run",
                details={"summary_status": summary["status"]},
            )
        conn.commit()
        print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
