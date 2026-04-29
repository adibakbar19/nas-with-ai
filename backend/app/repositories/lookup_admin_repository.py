from typing import Any

try:
    import psycopg
    from psycopg import sql
    from psycopg.rows import dict_row
except Exception:  # pragma: no cover
    psycopg = None
    sql = None
    dict_row = None


class LookupAdminRepository:
    def __init__(self, *, dsn: str, lookup_schema: str, runtime_schema: str) -> None:
        self._dsn = dsn
        self._lookup_schema = lookup_schema
        self._runtime_schema = runtime_schema

    def _connect(self):
        if psycopg is None or dict_row is None:
            raise RuntimeError("psycopg is required for LookupAdminRepository")
        return psycopg.connect(self._dsn, row_factory=dict_row)

    def _tbl(self, schema_name: str, table_name: str):
        assert sql is not None
        return sql.SQL("{}.{}").format(sql.Identifier(schema_name), sql.Identifier(table_name))

    def _table_exists(self, *, cur, schema_name: str, table_name: str) -> bool:
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

    def _require_reference_tables(self, *, cur) -> None:
        missing: list[str] = []
        required_tables = ("state", "district", "mukim", "locality", "postcode")
        for schema_name, label in (
            (self._lookup_schema, "lookup"),
            (self._runtime_schema, "runtime"),
        ):
            for table_name in required_tables:
                if not self._table_exists(cur=cur, schema_name=schema_name, table_name=table_name):
                    missing.append(f"{label}:{schema_name}.{table_name}")
        if missing:
            raise RuntimeError(
                "missing required reference tables: "
                + ", ".join(missing)
                + ". create lookup/runtime tables before using admin lookup updates."
            )

    def _next_id(self, *, cur, table_name: str, id_column: str) -> int:
        assert sql is not None
        stmt = sql.SQL(
            """
            SELECT GREATEST(
              COALESCE((SELECT MAX({id_column}) FROM {lookup_table}), 0),
              COALESCE((SELECT MAX({id_column}) FROM {runtime_table}), 0)
            ) + 1 AS next_id
            """
        ).format(
            id_column=sql.Identifier(id_column),
            lookup_table=self._tbl(self._lookup_schema, table_name),
            runtime_table=self._tbl(self._runtime_schema, table_name),
        )
        cur.execute(stmt)
        row = cur.fetchone()
        return int(row["next_id"]) if row and row["next_id"] is not None else 1

    def _get_state(self, *, cur, schema_name: str, state_id: int) -> dict[str, Any] | None:
        assert sql is not None
        stmt = sql.SQL(
            """
            SELECT state_id, state_name, state_code
            FROM {state}
            WHERE state_id = %s
            LIMIT 1
            """
        ).format(state=self._tbl(schema_name, "state"))
        cur.execute(stmt, (state_id,))
        row = cur.fetchone()
        return dict(row) if row else None

    def _get_district(self, *, cur, schema_name: str, district_id: int) -> dict[str, Any] | None:
        assert sql is not None
        stmt = sql.SQL(
            """
            SELECT
              d.district_id,
              d.district_name,
              d.district_code,
              d.state_id,
              s.state_name,
              s.state_code
            FROM {district} d
            JOIN {state} s
              ON s.state_id = d.state_id
            WHERE d.district_id = %s
            LIMIT 1
            """
        ).format(
            district=self._tbl(schema_name, "district"),
            state=self._tbl(schema_name, "state"),
        )
        cur.execute(stmt, (district_id,))
        row = cur.fetchone()
        return dict(row) if row else None

    def _get_mukim(self, *, cur, schema_name: str, mukim_id: int) -> dict[str, Any] | None:
        assert sql is not None
        stmt = sql.SQL(
            """
            SELECT
              m.mukim_id,
              m.mukim_name,
              m.mukim_code,
              m.district_id,
              d.district_name,
              d.district_code,
              d.state_id,
              s.state_name,
              s.state_code
            FROM {mukim} m
            JOIN {district} d
              ON d.district_id = m.district_id
            JOIN {state} s
              ON s.state_id = d.state_id
            WHERE m.mukim_id = %s
            LIMIT 1
            """
        ).format(
            mukim=self._tbl(schema_name, "mukim"),
            district=self._tbl(schema_name, "district"),
            state=self._tbl(schema_name, "state"),
        )
        cur.execute(stmt, (mukim_id,))
        row = cur.fetchone()
        return dict(row) if row else None

    def _get_locality(self, *, cur, schema_name: str, locality_id: int) -> dict[str, Any] | None:
        assert sql is not None
        stmt = sql.SQL(
            """
            SELECT
              l.locality_id,
              l.locality_name,
              l.locality_code,
              l.mukim_id,
              l.created_at,
              l.updated_at,
              m.mukim_name,
              m.mukim_code,
              m.district_id,
              d.district_name,
              d.district_code,
              d.state_id,
              s.state_name,
              s.state_code
            FROM {locality} l
            LEFT JOIN {mukim} m
              ON m.mukim_id = l.mukim_id
            LEFT JOIN {district} d
              ON d.district_id = m.district_id
            LEFT JOIN {state} s
              ON s.state_id = d.state_id
            WHERE l.locality_id = %s
            LIMIT 1
            """
        ).format(
            locality=self._tbl(schema_name, "locality"),
            mukim=self._tbl(schema_name, "mukim"),
            district=self._tbl(schema_name, "district"),
            state=self._tbl(schema_name, "state"),
        )
        cur.execute(stmt, (locality_id,))
        row = cur.fetchone()
        return dict(row) if row else None

    def _get_postcode(self, *, cur, schema_name: str, postcode_id: int) -> dict[str, Any] | None:
        assert sql is not None
        stmt = sql.SQL(
            """
            SELECT
              p.postcode_id,
              p.postcode_name,
              p.postcode,
              p.locality_id,
              l.locality_name,
              l.locality_code,
              l.mukim_id,
              m.mukim_name,
              m.mukim_code,
              m.district_id,
              d.district_name,
              d.district_code,
              d.state_id,
              s.state_name,
              s.state_code
            FROM {postcode} p
            LEFT JOIN {locality} l
              ON l.locality_id = p.locality_id
            LEFT JOIN {mukim} m
              ON m.mukim_id = l.mukim_id
            LEFT JOIN {district} d
              ON d.district_id = m.district_id
            LEFT JOIN {state} s
              ON s.state_id = d.state_id
            WHERE p.postcode_id = %s
            LIMIT 1
            """
        ).format(
            postcode=self._tbl(schema_name, "postcode"),
            locality=self._tbl(schema_name, "locality"),
            mukim=self._tbl(schema_name, "mukim"),
            district=self._tbl(schema_name, "district"),
            state=self._tbl(schema_name, "state"),
        )
        cur.execute(stmt, (postcode_id,))
        row = cur.fetchone()
        return dict(row) if row else None

    def _get_district_by_code(self, *, cur, schema_name: str, district_code: str, state_id: int) -> dict[str, Any] | None:
        assert sql is not None
        stmt = sql.SQL(
            """
            SELECT district_id, district_name, district_code, state_id
            FROM {district}
            WHERE upper(trim(coalesce(district_code::text, ''))) = upper(trim(%s))
              AND state_id = %s
            LIMIT 1
            """
        ).format(district=self._tbl(schema_name, "district"))
        cur.execute(stmt, (district_code, state_id))
        row = cur.fetchone()
        return dict(row) if row else None

    def _get_mukim_by_code(self, *, cur, schema_name: str, mukim_code: str, district_id: int) -> dict[str, Any] | None:
        assert sql is not None
        stmt = sql.SQL(
            """
            SELECT mukim_id, mukim_name, mukim_code, district_id
            FROM {mukim}
            WHERE upper(trim(coalesce(mukim_code::text, ''))) = upper(trim(%s))
              AND district_id = %s
            LIMIT 1
            """
        ).format(mukim=self._tbl(schema_name, "mukim"))
        cur.execute(stmt, (mukim_code, district_id))
        row = cur.fetchone()
        return dict(row) if row else None

    def _get_locality_by_name(self, *, cur, schema_name: str, locality_name: str, mukim_id: int | None) -> dict[str, Any] | None:
        assert sql is not None
        stmt = sql.SQL(
            """
            SELECT locality_id, locality_name, locality_code, mukim_id
            FROM {locality}
            WHERE upper(trim(coalesce(locality_name::text, ''))) = upper(trim(%s))
              AND (
                (%s IS NULL AND mukim_id IS NULL)
                OR mukim_id = %s
              )
            LIMIT 1
            """
        ).format(locality=self._tbl(schema_name, "locality"))
        cur.execute(stmt, (locality_name, mukim_id, mukim_id))
        row = cur.fetchone()
        return dict(row) if row else None

    def _get_postcode_by_value(self, *, cur, schema_name: str, postcode: str) -> dict[str, Any] | None:
        assert sql is not None
        stmt = sql.SQL(
            """
            SELECT postcode_id, postcode_name, postcode, locality_id
            FROM {postcode}
            WHERE trim(coalesce(postcode::text, '')) = trim(%s)
            LIMIT 1
            """
        ).format(postcode=self._tbl(schema_name, "postcode"))
        cur.execute(stmt, (postcode,))
        row = cur.fetchone()
        return dict(row) if row else None

    def list_states(self) -> list[dict[str, Any]]:
        assert sql is not None
        stmt = sql.SQL(
            """
            SELECT state_id, state_name, state_code
            FROM {state}
            ORDER BY state_code ASC NULLS LAST, state_name ASC
            """
        ).format(state=self._tbl(self._lookup_schema, "state"))
        with self._connect() as conn, conn.cursor() as cur:
            self._require_reference_tables(cur=cur)
            cur.execute(stmt)
            return [dict(row) for row in cur.fetchall()]

    def list_districts(self, *, state_id: int | None) -> list[dict[str, Any]]:
        assert sql is not None
        stmt = sql.SQL(
            """
            SELECT
              d.district_id,
              d.district_name,
              d.district_code,
              d.state_id,
              s.state_name,
              s.state_code
            FROM {district} d
            JOIN {state} s
              ON s.state_id = d.state_id
            WHERE (%s IS NULL OR d.state_id = %s)
            ORDER BY s.state_code ASC NULLS LAST, d.district_code ASC NULLS LAST, d.district_name ASC
            """
        ).format(
            district=self._tbl(self._lookup_schema, "district"),
            state=self._tbl(self._lookup_schema, "state"),
        )
        with self._connect() as conn, conn.cursor() as cur:
            self._require_reference_tables(cur=cur)
            cur.execute(stmt, (state_id, state_id))
            return [dict(row) for row in cur.fetchall()]

    def list_mukim(self, *, district_id: int | None, state_id: int | None) -> list[dict[str, Any]]:
        assert sql is not None
        stmt = sql.SQL(
            """
            SELECT
              m.mukim_id,
              m.mukim_name,
              m.mukim_code,
              m.district_id,
              d.district_name,
              d.district_code,
              d.state_id,
              s.state_name,
              s.state_code
            FROM {mukim} m
            JOIN {district} d ON d.district_id = m.district_id
            JOIN {state} s ON s.state_id = d.state_id
            WHERE (%s IS NULL OR d.state_id = %s)
              AND (%s IS NULL OR m.district_id = %s)
            ORDER BY s.state_code ASC NULLS LAST, d.district_code ASC NULLS LAST, m.mukim_code ASC NULLS LAST, m.mukim_name ASC
            """
        ).format(
            mukim=self._tbl(self._lookup_schema, "mukim"),
            district=self._tbl(self._lookup_schema, "district"),
            state=self._tbl(self._lookup_schema, "state"),
        )
        with self._connect() as conn, conn.cursor() as cur:
            self._require_reference_tables(cur=cur)
            cur.execute(stmt, (state_id, state_id, district_id, district_id))
            return [dict(row) for row in cur.fetchall()]

    def list_localities(self, *, mukim_id: int | None, district_id: int | None, state_id: int | None) -> list[dict[str, Any]]:
        assert sql is not None
        stmt = sql.SQL(
            """
            SELECT
              l.locality_id,
              l.locality_name,
              l.locality_code,
              l.mukim_id,
              l.created_at,
              l.updated_at,
              m.mukim_name,
              m.mukim_code,
              m.district_id,
              d.district_name,
              d.district_code,
              d.state_id,
              s.state_name,
              s.state_code
            FROM {locality} l
            LEFT JOIN {mukim} m ON m.mukim_id = l.mukim_id
            LEFT JOIN {district} d ON d.district_id = m.district_id
            LEFT JOIN {state} s ON s.state_id = d.state_id
            WHERE (%s IS NULL OR d.state_id = %s)
              AND (%s IS NULL OR d.district_id = %s)
              AND (%s IS NULL OR l.mukim_id = %s)
            ORDER BY s.state_code ASC NULLS LAST, d.district_code ASC NULLS LAST, m.mukim_code ASC NULLS LAST, l.locality_name ASC
            """
        ).format(
            locality=self._tbl(self._lookup_schema, "locality"),
            mukim=self._tbl(self._lookup_schema, "mukim"),
            district=self._tbl(self._lookup_schema, "district"),
            state=self._tbl(self._lookup_schema, "state"),
        )
        with self._connect() as conn, conn.cursor() as cur:
            self._require_reference_tables(cur=cur)
            cur.execute(
                stmt,
                (state_id, state_id, district_id, district_id, mukim_id, mukim_id),
            )
            return [dict(row) for row in cur.fetchall()]

    def list_postcodes(self, *, locality_id: int | None, mukim_id: int | None, district_id: int | None, state_id: int | None) -> list[dict[str, Any]]:
        assert sql is not None
        stmt = sql.SQL(
            """
            SELECT
              p.postcode_id,
              p.postcode_name,
              p.postcode,
              p.locality_id,
              l.locality_name,
              l.locality_code,
              l.mukim_id,
              m.mukim_name,
              m.mukim_code,
              m.district_id,
              d.district_name,
              d.district_code,
              d.state_id,
              s.state_name,
              s.state_code
            FROM {postcode} p
            LEFT JOIN {locality} l ON l.locality_id = p.locality_id
            LEFT JOIN {mukim} m ON m.mukim_id = l.mukim_id
            LEFT JOIN {district} d ON d.district_id = m.district_id
            LEFT JOIN {state} s ON s.state_id = d.state_id
            WHERE (%s IS NULL OR d.state_id = %s)
              AND (%s IS NULL OR d.district_id = %s)
              AND (%s IS NULL OR l.mukim_id = %s)
              AND (%s IS NULL OR p.locality_id = %s)
            ORDER BY p.postcode ASC NULLS LAST, p.postcode_name ASC
            """
        ).format(
            postcode=self._tbl(self._lookup_schema, "postcode"),
            locality=self._tbl(self._lookup_schema, "locality"),
            mukim=self._tbl(self._lookup_schema, "mukim"),
            district=self._tbl(self._lookup_schema, "district"),
            state=self._tbl(self._lookup_schema, "state"),
        )
        with self._connect() as conn, conn.cursor() as cur:
            self._require_reference_tables(cur=cur)
            cur.execute(
                stmt,
                (state_id, state_id, district_id, district_id, mukim_id, mukim_id, locality_id, locality_id),
            )
            return [dict(row) for row in cur.fetchall()]

    def get_district(self, *, district_id: int) -> dict[str, Any] | None:
        with self._connect() as conn, conn.cursor() as cur:
            self._require_reference_tables(cur=cur)
            return self._get_district(cur=cur, schema_name=self._lookup_schema, district_id=district_id)

    def get_mukim(self, *, mukim_id: int) -> dict[str, Any] | None:
        with self._connect() as conn, conn.cursor() as cur:
            self._require_reference_tables(cur=cur)
            return self._get_mukim(cur=cur, schema_name=self._lookup_schema, mukim_id=mukim_id)

    def get_locality(self, *, locality_id: int) -> dict[str, Any] | None:
        with self._connect() as conn, conn.cursor() as cur:
            self._require_reference_tables(cur=cur)
            return self._get_locality(cur=cur, schema_name=self._lookup_schema, locality_id=locality_id)

    def get_postcode(self, *, postcode_id: int) -> dict[str, Any] | None:
        with self._connect() as conn, conn.cursor() as cur:
            self._require_reference_tables(cur=cur)
            return self._get_postcode(cur=cur, schema_name=self._lookup_schema, postcode_id=postcode_id)

    def create_district(self, *, district_name: str, district_code: str, state_id: int) -> dict[str, Any]:
        assert sql is not None
        lookup_district = self._tbl(self._lookup_schema, "district")
        runtime_district = self._tbl(self._runtime_schema, "district")
        with self._connect() as conn, conn.cursor() as cur:
            self._require_reference_tables(cur=cur)
            if self._get_state(cur=cur, schema_name=self._lookup_schema, state_id=state_id) is None or self._get_state(cur=cur, schema_name=self._runtime_schema, state_id=state_id) is None:
                raise ValueError("state_id not found in lookup/runtime reference tables")
            if self._get_district_by_code(cur=cur, schema_name=self._lookup_schema, district_code=district_code, state_id=state_id):
                raise ValueError("district_code already exists for the target state")

            district_id = self._next_id(cur=cur, table_name="district", id_column="district_id")
            insert_stmt = sql.SQL(
                """
                INSERT INTO {district} (district_id, district_name, state_id, district_code)
                VALUES (%s, %s, %s, %s)
                """
            )
            cur.execute(insert_stmt.format(district=lookup_district), (district_id, district_name, state_id, district_code))
            cur.execute(insert_stmt.format(district=runtime_district), (district_id, district_name, state_id, district_code))
            conn.commit()
        row = self.get_district(district_id=district_id)
        if row is None:
            raise RuntimeError("district create succeeded but fetch failed")
        return row

    def update_district(self, *, district_id: int, district_name: str | None, district_code: str | None, state_id: int | None) -> dict[str, Any] | None:
        assert sql is not None
        lookup_district = self._tbl(self._lookup_schema, "district")
        runtime_district = self._tbl(self._runtime_schema, "district")
        with self._connect() as conn, conn.cursor() as cur:
            self._require_reference_tables(cur=cur)
            current = self._get_district(cur=cur, schema_name=self._lookup_schema, district_id=district_id)
            if current is None:
                return None

            next_name = district_name if district_name is not None else str(current["district_name"])
            next_code = district_code if district_code is not None else str(current["district_code"])
            next_state_id = state_id if state_id is not None else int(current["state_id"])
            if self._get_state(cur=cur, schema_name=self._lookup_schema, state_id=next_state_id) is None or self._get_state(cur=cur, schema_name=self._runtime_schema, state_id=next_state_id) is None:
                raise ValueError("state_id not found in lookup/runtime reference tables")
            duplicate = self._get_district_by_code(cur=cur, schema_name=self._lookup_schema, district_code=next_code, state_id=next_state_id)
            if duplicate and int(duplicate["district_id"]) != district_id:
                raise ValueError("district_code already exists for the target state")

            update_stmt = sql.SQL(
                """
                UPDATE {district}
                SET district_name = %s,
                    state_id = %s,
                    district_code = %s
                WHERE district_id = %s
                """
            )
            cur.execute(update_stmt.format(district=lookup_district), (next_name, next_state_id, next_code, district_id))
            cur.execute(update_stmt.format(district=runtime_district), (next_name, next_state_id, next_code, district_id))
            conn.commit()
        return self.get_district(district_id=district_id)

    def create_mukim(self, *, mukim_name: str, mukim_code: str, district_id: int) -> dict[str, Any]:
        assert sql is not None
        lookup_mukim = self._tbl(self._lookup_schema, "mukim")
        runtime_mukim = self._tbl(self._runtime_schema, "mukim")
        with self._connect() as conn, conn.cursor() as cur:
            self._require_reference_tables(cur=cur)
            if self._get_district(cur=cur, schema_name=self._lookup_schema, district_id=district_id) is None or self._get_district(cur=cur, schema_name=self._runtime_schema, district_id=district_id) is None:
                raise ValueError("district_id not found in lookup/runtime reference tables")
            if self._get_mukim_by_code(cur=cur, schema_name=self._lookup_schema, mukim_code=mukim_code, district_id=district_id):
                raise ValueError("mukim_code already exists for the target district")

            mukim_id = self._next_id(cur=cur, table_name="mukim", id_column="mukim_id")
            insert_stmt = sql.SQL(
                """
                INSERT INTO {mukim} (mukim_id, mukim_name, district_id, mukim_code)
                VALUES (%s, %s, %s, %s)
                """
            )
            cur.execute(insert_stmt.format(mukim=lookup_mukim), (mukim_id, mukim_name, district_id, mukim_code))
            cur.execute(insert_stmt.format(mukim=runtime_mukim), (mukim_id, mukim_name, district_id, mukim_code))
            conn.commit()
        row = self.get_mukim(mukim_id=mukim_id)
        if row is None:
            raise RuntimeError("mukim create succeeded but fetch failed")
        return row

    def update_mukim(self, *, mukim_id: int, mukim_name: str | None, mukim_code: str | None, district_id: int | None) -> dict[str, Any] | None:
        assert sql is not None
        lookup_mukim = self._tbl(self._lookup_schema, "mukim")
        runtime_mukim = self._tbl(self._runtime_schema, "mukim")
        with self._connect() as conn, conn.cursor() as cur:
            self._require_reference_tables(cur=cur)
            current = self._get_mukim(cur=cur, schema_name=self._lookup_schema, mukim_id=mukim_id)
            if current is None:
                return None
            next_name = mukim_name if mukim_name is not None else str(current["mukim_name"])
            next_code = mukim_code if mukim_code is not None else str(current["mukim_code"] or "")
            next_district_id = district_id if district_id is not None else int(current["district_id"])
            if self._get_district(cur=cur, schema_name=self._lookup_schema, district_id=next_district_id) is None or self._get_district(cur=cur, schema_name=self._runtime_schema, district_id=next_district_id) is None:
                raise ValueError("district_id not found in lookup/runtime reference tables")
            duplicate = self._get_mukim_by_code(cur=cur, schema_name=self._lookup_schema, mukim_code=next_code, district_id=next_district_id)
            if duplicate and int(duplicate["mukim_id"]) != mukim_id:
                raise ValueError("mukim_code already exists for the target district")

            update_stmt = sql.SQL(
                """
                UPDATE {mukim}
                SET mukim_name = %s,
                    district_id = %s,
                    mukim_code = %s
                WHERE mukim_id = %s
                """
            )
            cur.execute(update_stmt.format(mukim=lookup_mukim), (next_name, next_district_id, next_code, mukim_id))
            cur.execute(update_stmt.format(mukim=runtime_mukim), (next_name, next_district_id, next_code, mukim_id))
            conn.commit()
        return self.get_mukim(mukim_id=mukim_id)

    def create_locality(self, *, locality_name: str, locality_code: str | None, mukim_id: int | None) -> dict[str, Any]:
        assert sql is not None
        lookup_locality = self._tbl(self._lookup_schema, "locality")
        runtime_locality = self._tbl(self._runtime_schema, "locality")
        with self._connect() as conn, conn.cursor() as cur:
            self._require_reference_tables(cur=cur)
            if mukim_id is not None:
                if self._get_mukim(cur=cur, schema_name=self._lookup_schema, mukim_id=mukim_id) is None or self._get_mukim(cur=cur, schema_name=self._runtime_schema, mukim_id=mukim_id) is None:
                    raise ValueError("mukim_id not found in lookup/runtime reference tables")
            if self._get_locality_by_name(cur=cur, schema_name=self._lookup_schema, locality_name=locality_name, mukim_id=mukim_id):
                raise ValueError("locality_name already exists for the target mukim")
            locality_id = self._next_id(cur=cur, table_name="locality", id_column="locality_id")
            insert_stmt = sql.SQL(
                """
                INSERT INTO {locality} (locality_id, locality_code, locality_name, mukim_id, created_at, updated_at)
                VALUES (%s, %s, %s, %s, NOW(), NOW())
                """
            )
            cur.execute(insert_stmt.format(locality=lookup_locality), (locality_id, locality_code, locality_name, mukim_id))
            cur.execute(insert_stmt.format(locality=runtime_locality), (locality_id, locality_code, locality_name, mukim_id))
            conn.commit()
        row = self.get_locality(locality_id=locality_id)
        if row is None:
            raise RuntimeError("locality create succeeded but fetch failed")
        return row

    def update_locality(self, *, locality_id: int, locality_name: str | None, locality_code: str | None, mukim_id: int | None) -> dict[str, Any] | None:
        assert sql is not None
        lookup_locality = self._tbl(self._lookup_schema, "locality")
        runtime_locality = self._tbl(self._runtime_schema, "locality")
        with self._connect() as conn, conn.cursor() as cur:
            self._require_reference_tables(cur=cur)
            current = self._get_locality(cur=cur, schema_name=self._lookup_schema, locality_id=locality_id)
            if current is None:
                return None
            next_name = locality_name if locality_name is not None else str(current["locality_name"])
            next_code = locality_code if locality_code is not None else (str(current["locality_code"]) if current.get("locality_code") is not None else None)
            next_mukim_id = mukim_id if mukim_id is not None else (int(current["mukim_id"]) if current.get("mukim_id") is not None else None)
            if next_mukim_id is not None:
                if self._get_mukim(cur=cur, schema_name=self._lookup_schema, mukim_id=next_mukim_id) is None or self._get_mukim(cur=cur, schema_name=self._runtime_schema, mukim_id=next_mukim_id) is None:
                    raise ValueError("mukim_id not found in lookup/runtime reference tables")
            duplicate = self._get_locality_by_name(cur=cur, schema_name=self._lookup_schema, locality_name=next_name, mukim_id=next_mukim_id)
            if duplicate and int(duplicate["locality_id"]) != locality_id:
                raise ValueError("locality_name already exists for the target mukim")

            update_stmt = sql.SQL(
                """
                UPDATE {locality}
                SET locality_name = %s,
                    locality_code = %s,
                    mukim_id = %s,
                    updated_at = NOW()
                WHERE locality_id = %s
                """
            )
            cur.execute(update_stmt.format(locality=lookup_locality), (next_name, next_code, next_mukim_id, locality_id))
            cur.execute(update_stmt.format(locality=runtime_locality), (next_name, next_code, next_mukim_id, locality_id))
            conn.commit()
        return self.get_locality(locality_id=locality_id)

    def create_postcode(self, *, postcode_name: str, postcode: str, locality_id: int | None) -> dict[str, Any]:
        assert sql is not None
        lookup_postcode = self._tbl(self._lookup_schema, "postcode")
        runtime_postcode = self._tbl(self._runtime_schema, "postcode")
        with self._connect() as conn, conn.cursor() as cur:
            self._require_reference_tables(cur=cur)
            if locality_id is not None:
                if self._get_locality(cur=cur, schema_name=self._lookup_schema, locality_id=locality_id) is None or self._get_locality(cur=cur, schema_name=self._runtime_schema, locality_id=locality_id) is None:
                    raise ValueError("locality_id not found in lookup/runtime reference tables")
            if self._get_postcode_by_value(cur=cur, schema_name=self._lookup_schema, postcode=postcode):
                raise ValueError("postcode already exists")
            postcode_id = self._next_id(cur=cur, table_name="postcode", id_column="postcode_id")
            insert_stmt = sql.SQL(
                """
                INSERT INTO {postcode} (postcode_id, postcode_name, locality_id, postcode)
                VALUES (%s, %s, %s, %s)
                """
            )
            cur.execute(insert_stmt.format(postcode=lookup_postcode), (postcode_id, postcode_name, locality_id, postcode))
            cur.execute(insert_stmt.format(postcode=runtime_postcode), (postcode_id, postcode_name, locality_id, postcode))
            conn.commit()
        row = self.get_postcode(postcode_id=postcode_id)
        if row is None:
            raise RuntimeError("postcode create succeeded but fetch failed")
        return row

    def update_postcode(self, *, postcode_id: int, postcode_name: str | None, postcode: str | None, locality_id: int | None) -> dict[str, Any] | None:
        assert sql is not None
        lookup_postcode = self._tbl(self._lookup_schema, "postcode")
        runtime_postcode = self._tbl(self._runtime_schema, "postcode")
        with self._connect() as conn, conn.cursor() as cur:
            self._require_reference_tables(cur=cur)
            current = self._get_postcode(cur=cur, schema_name=self._lookup_schema, postcode_id=postcode_id)
            if current is None:
                return None
            next_name = postcode_name if postcode_name is not None else str(current["postcode_name"])
            next_postcode = postcode if postcode is not None else str(current["postcode"])
            next_locality_id = locality_id if locality_id is not None else (int(current["locality_id"]) if current.get("locality_id") is not None else None)
            if next_locality_id is not None:
                if self._get_locality(cur=cur, schema_name=self._lookup_schema, locality_id=next_locality_id) is None or self._get_locality(cur=cur, schema_name=self._runtime_schema, locality_id=next_locality_id) is None:
                    raise ValueError("locality_id not found in lookup/runtime reference tables")
            duplicate = self._get_postcode_by_value(cur=cur, schema_name=self._lookup_schema, postcode=next_postcode)
            if duplicate and int(duplicate["postcode_id"]) != postcode_id:
                raise ValueError("postcode already exists")

            update_stmt = sql.SQL(
                """
                UPDATE {postcode}
                SET postcode_name = %s,
                    locality_id = %s,
                    postcode = %s
                WHERE postcode_id = %s
                """
            )
            cur.execute(update_stmt.format(postcode=lookup_postcode), (next_name, next_locality_id, next_postcode, postcode_id))
            cur.execute(update_stmt.format(postcode=runtime_postcode), (next_name, next_locality_id, next_postcode, postcode_id))
            conn.commit()
        return self.get_postcode(postcode_id=postcode_id)
