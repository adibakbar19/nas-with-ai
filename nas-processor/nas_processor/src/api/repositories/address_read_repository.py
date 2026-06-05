"""Read-only repository for nas.standardized_address, raw_address, and address_alias.

All columns sourced from the shared address_schema contract — never hardcode column names here.
"""

from __future__ import annotations

from typing import Any

try:
    import psycopg
    from psycopg import sql
    from psycopg.rows import dict_row
except Exception:  # pragma: no cover
    psycopg = None
    sql = None
    dict_row = None

try:
    from repository.address_schema import STANDARDIZED_ADDRESS_COLUMNS
except ImportError:
    from nas_processor.src.repository.address_schema import STANDARDIZED_ADDRESS_COLUMNS


# All schema columns except geom (PostGIS geometry — not JSON-serializable)
_READ_COLS: list[str] = [c for c in STANDARDIZED_ADDRESS_COLUMNS if c != "geom"]


class AddressReadRepository:
    def __init__(self, *, dsn: str, schema: str) -> None:
        self._dsn = dsn
        self._schema = schema

    def _connect(self):
        if psycopg is None or dict_row is None:
            raise RuntimeError("psycopg is required for AddressReadRepository")
        return psycopg.connect(self._dsn, row_factory=dict_row)

    def _tbl(self, table: str = "standardized_address"):
        assert sql is not None
        return sql.SQL("{}.{}").format(
            sql.Identifier(self._schema), sql.Identifier(table)
        )

    @staticmethod
    def _col_list() -> str:
        return ", ".join(f'sa."{c}"' for c in _READ_COLS)

    # ── Point lookups ──────────────────────────────────────────────────────────

    def get_by_record_id(self, record_id: str) -> dict[str, Any] | None:
        assert sql is not None
        stmt = sql.SQL(
            "SELECT {cols} FROM {tbl} sa "
            "WHERE sa.record_id = %s "
            "AND sa.lifecycle_status NOT IN ('archived') "
            "LIMIT 1"
        ).format(cols=sql.SQL(self._col_list()), tbl=self._tbl())
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(stmt, (record_id,))
            row = cur.fetchone()
            return dict(row) if row else None

    def get_address(self, *, record_id: str) -> dict[str, Any] | None:
        return self.get_by_record_id(record_id)

    def get_by_naskod(self, *, code: str) -> dict[str, Any] | None:
        assert sql is not None
        stmt = sql.SQL(
            "SELECT {cols} FROM {tbl} sa "
            "WHERE sa.naskod = %s "
            "AND sa.lifecycle_status NOT IN ('archived') "
            "LIMIT 1"
        ).format(cols=sql.SQL(self._col_list()), tbl=self._tbl())
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(stmt, (code,))
            row = cur.fetchone()
            return dict(row) if row else None

    # ── Search ─────────────────────────────────────────────────────────────────

    def search_addresses(
        self,
        *,
        q: str | None,
        postcode: str | None,
        state_code: str | None,
        mukim_id: str | None,
        pbt_id: str | None,
        locality_name: str | None,
        confidence_band: str | None,
        lifecycle_status: str | None,
        limit: int = 20,
        offset: int = 0,
    ) -> tuple[list[dict[str, Any]], int]:
        """Return (rows, total_count). All filters optional. Always excludes archived."""
        assert sql is not None

        conditions = [sql.SQL("sa.lifecycle_status NOT IN ('archived')")]
        params: list[Any] = []

        if q:
            conditions.append(sql.SQL("sa.address_clean ILIKE %s"))
            params.append(f"%{q}%")
        if postcode:
            conditions.append(sql.SQL("sa.postcode = %s"))
            params.append(postcode)
        if state_code:
            conditions.append(sql.SQL("sa.state_code = %s"))
            params.append(state_code)
        if mukim_id:
            conditions.append(sql.SQL("sa.mukim_id = %s"))
            params.append(mukim_id)
        if pbt_id:
            conditions.append(sql.SQL("sa.pbt_id = %s"))
            params.append(pbt_id)
        if locality_name:
            conditions.append(sql.SQL("sa.locality_name ILIKE %s"))
            params.append(f"%{locality_name}%")
        if confidence_band:
            conditions.append(sql.SQL("sa.confidence_band = %s"))
            params.append(confidence_band.upper())
        if lifecycle_status:
            conditions.append(sql.SQL("sa.lifecycle_status = %s"))
            params.append(lifecycle_status)

        where = sql.SQL(" AND ").join(conditions)

        stmt = sql.SQL(
            "SELECT {cols}, COUNT(*) OVER() AS _total_count "
            "FROM {tbl} sa "
            "WHERE {where} "
            "ORDER BY "
            "  CASE sa.confidence_band "
            "    WHEN 'HIGH' THEN 1 WHEN 'MEDIUM' THEN 2 "
            "    WHEN 'LOW' THEN 3 WHEN 'FAILED' THEN 4 ELSE 5 END, "
            "  sa.created_at DESC NULLS LAST "
            "LIMIT %s OFFSET %s"
        ).format(
            cols=sql.SQL(self._col_list()),
            tbl=self._tbl(),
            where=where,
        )

        params.extend([limit, offset])
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(stmt, params)
            rows = cur.fetchall()

        if not rows:
            return [], 0

        total = int(rows[0]["_total_count"])
        cleaned = [{k: v for k, v in row.items() if k != "_total_count"} for row in rows]
        return cleaned, total

    # Legacy compatibility for existing AddressReadService.search()
    def search_addresses_legacy(self, *, query: str | None, limit: int) -> list[dict[str, Any]]:
        rows, _ = self.search_addresses(
            q=query, postcode=None, state_code=None, mukim_id=None,
            pbt_id=None, locality_name=None, confidence_band=None,
            lifecycle_status=None, limit=limit, offset=0,
        )
        return rows

    # ── Sub-resource queries ───────────────────────────────────────────────────

    def get_raw_history(self, standardized_id: str) -> list[dict[str, Any]]:
        """All raw_address rows that reference this canonical record (pre- and post-match)."""
        assert sql is not None
        raw_tbl = self._tbl("raw_address")
        stmt = sql.SQL(
            "SELECT r.raw_id, r.raw_text, r.match_status, r.agency_id, "
            "       r.error_reason, r.replaces_raw_id, r.ingest_timestamp, "
            "       r.job_id, r.match_confidence "
            "FROM {tbl} r "
            "WHERE r.standardized_id = %s OR r.source_ref_id = %s "
            "ORDER BY r.ingest_timestamp DESC NULLS LAST"
        ).format(tbl=raw_tbl)
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(stmt, (standardized_id, standardized_id))
            return [dict(r) for r in cur.fetchall()]

    def get_aliases(self, standardized_id: str) -> list[dict[str, Any]]:
        assert sql is not None
        alias_tbl = self._tbl("address_alias")
        stmt = sql.SQL(
            "SELECT alias_id, alias_text, alias_type, source_system, valid_from, valid_until "
            "FROM {tbl} "
            "WHERE standardized_id = %s "
            "  AND (valid_until IS NULL OR valid_until > NOW()) "
            "ORDER BY valid_from DESC NULLS LAST"
        ).format(tbl=alias_tbl)
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(stmt, (standardized_id,))
            return [dict(r) for r in cur.fetchall()]
