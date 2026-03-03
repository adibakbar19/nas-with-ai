from dataclasses import dataclass
from typing import Any, Protocol

try:
    import psycopg
    from psycopg import sql
except Exception:  # pragma: no cover - optional import for local minimal setup
    psycopg = None
    sql = None


@dataclass
class SpatialValidationResult:
    is_within_postcode_boundary: bool | None
    resolved_postcode: str | None


class AddressRepository(Protocol):
    def get_canonical_address(self, address_id: str) -> dict[str, Any] | None:
        ...

    def validate_postcode_geometry(
        self,
        *,
        postcode: str,
        latitude: float,
        longitude: float,
    ) -> SpatialValidationResult:
        ...


class PostgresAddressRepository:
    def __init__(self, *, dsn: str, schema: str, postcode_boundary_table: str) -> None:
        self._dsn = dsn
        self._schema = schema
        self._postcode_boundary_table = postcode_boundary_table

    def _connect(self):
        if psycopg is None:
            raise RuntimeError("psycopg is required for PostgresAddressRepository")
        return psycopg.connect(self._dsn)

    def get_canonical_address(self, address_id: str) -> dict[str, Any] | None:
        query = sql.SQL(
            """
            SELECT address_id, premise_no, building_name, unit_no, latitude, longitude, validation_status
            FROM {}.standardized_address
            WHERE address_id = %s
            LIMIT 1
            """
        ).format(sql.Identifier(self._schema))

        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(query, (address_id,))
            row = cur.fetchone()
            if row is None:
                return None
            columns = [c.name for c in cur.description]
            return dict(zip(columns, row, strict=False))

    def validate_postcode_geometry(
        self,
        *,
        postcode: str,
        latitude: float,
        longitude: float,
    ) -> SpatialValidationResult:
        query = sql.SQL(
            """
            SELECT
              EXISTS(
                SELECT 1
                FROM {}.{}
                WHERE postcode = %s
                AND ST_Within(
                  ST_SetSRID(ST_Point(%s, %s), 4326),
                  geom
                )
              ) AS is_match,
              (
                SELECT postcode
                FROM {}.{}
                WHERE ST_Within(
                  ST_SetSRID(ST_Point(%s, %s), 4326),
                  geom
                )
                LIMIT 1
              ) AS resolved_postcode
            """
        ).format(
            sql.Identifier(self._schema),
            sql.Identifier(self._postcode_boundary_table),
            sql.Identifier(self._schema),
            sql.Identifier(self._postcode_boundary_table),
        )

        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                query,
                (
                    postcode,
                    longitude,
                    latitude,
                    longitude,
                    latitude,
                ),
            )
            row = cur.fetchone()
            if row is None:
                return SpatialValidationResult(is_within_postcode_boundary=None, resolved_postcode=None)
            return SpatialValidationResult(
                is_within_postcode_boundary=bool(row[0]) if row[0] is not None else None,
                resolved_postcode=row[1],
            )
