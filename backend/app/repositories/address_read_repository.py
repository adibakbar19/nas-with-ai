from typing import Any

try:
    import psycopg
    from psycopg import sql
    from psycopg.rows import dict_row
except Exception:  # pragma: no cover - optional import for local minimal setup
    psycopg = None
    sql = None
    dict_row = None


class AddressReadRepository:
    def __init__(self, *, dsn: str, schema: str) -> None:
        self._dsn = dsn
        self._schema = schema

    def _connect(self):
        if psycopg is None or dict_row is None:
            raise RuntimeError("psycopg is required for AddressReadRepository")
        return psycopg.connect(self._dsn, row_factory=dict_row)

    def _tbl(self, name: str):
        assert sql is not None
        return sql.SQL("{}.{}").format(sql.Identifier(self._schema), sql.Identifier(name))

    def search_addresses(self, *, query: str | None, limit: int) -> list[dict[str, Any]]:
        query_norm = (query or "").strip()
        query_exact = query_norm or None
        query_like = f"%{query_norm}%" if query_norm else None

        assert sql is not None
        stmt = sql.SQL(
            """
            SELECT
              sa.address_id,
              n.code AS naskod,
              sa.premise_no,
              sa.building_name,
              sa.unit_no,
              sa.lot_no,
              st.street_name_prefix,
              st.street_name,
              l.locality_name,
              m.mukim_name,
              d.district_name,
              s.state_name,
              p.postcode,
              sa.latitude,
              sa.longitude,
              sa.validation_status,
              sa.updated_at
            FROM {sa} sa
            LEFT JOIN {n} n
              ON n.address_id = sa.address_id
             AND n.is_vanity = false
            LEFT JOIN {st} st
              ON st.street_id = sa.street_id
            LEFT JOIN {l} l
              ON l.locality_id = sa.locality_id
            LEFT JOIN {m} m
              ON m.mukim_id = sa.mukim_id
            LEFT JOIN {d} d
              ON d.district_id = sa.district_id
            LEFT JOIN {s} s
              ON s.state_id = sa.state_id
            LEFT JOIN {p} p
              ON p.postcode_id = sa.postcode_id
            WHERE (
              %s IS NULL
              OR sa.address_id::text = %s
              OR n.code ILIKE %s
              OR p.postcode ILIKE %s
              OR concat_ws(
                ', ',
                sa.premise_no,
                sa.building_name,
                st.street_name_prefix,
                st.street_name,
                l.locality_name,
                m.mukim_name,
                d.district_name,
                s.state_name
              ) ILIKE %s
            )
            ORDER BY sa.updated_at DESC NULLS LAST, sa.address_id DESC
            LIMIT %s
            """
        ).format(
            sa=self._tbl("standardized_address"),
            n=self._tbl("naskod"),
            st=self._tbl("street"),
            l=self._tbl("locality"),
            m=self._tbl("mukim"),
            d=self._tbl("district"),
            s=self._tbl("state"),
            p=self._tbl("postcode"),
        )

        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(stmt, (query_exact, query_exact, query_like, query_like, query_like, limit))
            rows = cur.fetchall()
            return [dict(row) for row in rows]

    def get_address(self, *, address_id: int) -> dict[str, Any] | None:
        assert sql is not None
        stmt = sql.SQL(
            """
            SELECT
              sa.address_id,
              n.code AS naskod,
              sa.premise_no,
              sa.building_name,
              sa.unit_no,
              sa.lot_no,
              st.street_name_prefix,
              st.street_name,
              l.locality_name,
              m.mukim_name,
              d.district_name,
              s.state_name,
              p.postcode,
              sa.latitude,
              sa.longitude,
              sa.validation_status,
              sa.updated_at
            FROM {sa} sa
            LEFT JOIN {n} n
              ON n.address_id = sa.address_id
             AND n.is_vanity = false
            LEFT JOIN {st} st
              ON st.street_id = sa.street_id
            LEFT JOIN {l} l
              ON l.locality_id = sa.locality_id
            LEFT JOIN {m} m
              ON m.mukim_id = sa.mukim_id
            LEFT JOIN {d} d
              ON d.district_id = sa.district_id
            LEFT JOIN {s} s
              ON s.state_id = sa.state_id
            LEFT JOIN {p} p
              ON p.postcode_id = sa.postcode_id
            WHERE sa.address_id = %s
            LIMIT 1
            """
        ).format(
            sa=self._tbl("standardized_address"),
            n=self._tbl("naskod"),
            st=self._tbl("street"),
            l=self._tbl("locality"),
            m=self._tbl("mukim"),
            d=self._tbl("district"),
            s=self._tbl("state"),
            p=self._tbl("postcode"),
        )
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(stmt, (address_id,))
            row = cur.fetchone()
            return dict(row) if row else None

    def get_by_naskod(self, *, code: str) -> dict[str, Any] | None:
        assert sql is not None
        stmt = sql.SQL(
            """
            SELECT
              sa.address_id,
              n.code AS naskod,
              sa.premise_no,
              sa.building_name,
              sa.unit_no,
              sa.lot_no,
              st.street_name_prefix,
              st.street_name,
              l.locality_name,
              m.mukim_name,
              d.district_name,
              s.state_name,
              p.postcode,
              sa.latitude,
              sa.longitude,
              sa.validation_status,
              sa.updated_at
            FROM {n} n
            JOIN {sa} sa
              ON sa.address_id = n.address_id
            LEFT JOIN {st} st
              ON st.street_id = sa.street_id
            LEFT JOIN {l} l
              ON l.locality_id = sa.locality_id
            LEFT JOIN {m} m
              ON m.mukim_id = sa.mukim_id
            LEFT JOIN {d} d
              ON d.district_id = sa.district_id
            LEFT JOIN {s} s
              ON s.state_id = sa.state_id
            LEFT JOIN {p} p
              ON p.postcode_id = sa.postcode_id
            WHERE n.code = %s
            LIMIT 1
            """
        ).format(
            n=self._tbl("naskod"),
            sa=self._tbl("standardized_address"),
            st=self._tbl("street"),
            l=self._tbl("locality"),
            m=self._tbl("mukim"),
            d=self._tbl("district"),
            s=self._tbl("state"),
            p=self._tbl("postcode"),
        )
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(stmt, (code,))
            row = cur.fetchone()
            return dict(row) if row else None
