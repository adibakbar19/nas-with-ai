"""DB-backed NASKOD sequence allocation.

Provides atomic batch allocation of sequence numbers for NASKOD generation.
Each call to ``allocate_batch`` claims N consecutive sequence numbers for a
given (state_code, district_code, address_type) combination in a single
round-trip using INSERT ... ON CONFLICT DO UPDATE ... RETURNING.

Usage::

    from nas_processor.etl.repository.naskod_repository import NaskodRepository

    repo = NaskodRepository(dsn="postgresql+psycopg://...")
    start_seq = repo.allocate_batch("14", "01", "R", count=500)
    # start_seq = 1 (first allocation)
    # NASKODs: NAS-KL-01-R000001 through NAS-KL-01-R000500

Thread safety: each call opens its own transaction. Safe for concurrent
workers as long as each worker requests its own batch.
"""

from __future__ import annotations

import os
import re
from typing import Any

import sqlalchemy as sa

from nas_config.db import build_dsn


_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

# State code → abbreviation mapping (same as existing naskod_utils.py)
STATE_ABBR_MAP: dict[str, str] = {
    "01": "JHR",
    "02": "KDH",
    "03": "KTN",
    "04": "MLK",
    "05": "NSN",
    "06": "PHG",
    "07": "PNG",
    "08": "PRK",
    "09": "PLS",
    "10": "SGR",
    "11": "TRG",
    "12": "SBH",
    "13": "SWK",
    "14": "KL",
    "15": "LAB",
    "16": "PJY",
}


def _app_schema() -> str:
    schema = (os.getenv("PGSCHEMA") or "nas").strip() or "nas"
    if not _IDENT_RE.fullmatch(schema):
        raise ValueError(f"Invalid schema name: {schema!r}")
    return schema


def _address_type_code(value: str | None) -> str:
    """Map address type label to single-char code."""
    normalized = (value or "").strip().upper()
    if not normalized:
        return "U"
    if re.match(r"^(RESIDENTIAL|HIGHRISE|RURAL)\b", normalized):
        return "R"
    if re.match(r"^COMMERCIAL\b", normalized):
        return "C"
    if re.match(r"^(OFFICE|INSTITUTIONAL)\b", normalized):
        return "O"
    if re.match(r"^INDUSTRIAL\b", normalized):
        return "I"
    return normalized[:1]


class NaskodRepository:
    """Allocates NASKOD sequence numbers from the DB.

    Parameters
    ----------
    dsn : str, optional
        SQLAlchemy connection string. Defaults to build_dsn(driver="psycopg").
    schema : str, optional
        Database schema containing naskod_sequence. Defaults to PGSCHEMA env var.
    """

    def __init__(self, *, dsn: str | None = None, schema: str | None = None) -> None:
        self._dsn = dsn or build_dsn(driver="psycopg")
        self._schema = schema or _app_schema()
        self._engine: sa.Engine | None = None

    @property
    def engine(self) -> sa.Engine:
        if self._engine is None:
            self._engine = sa.create_engine(self._dsn)
        return self._engine

    def allocate_batch(
        self,
        state_code: str,
        district_code: str,
        address_type: str,
        *,
        count: int,
    ) -> int:
        """Atomically allocate ``count`` sequence numbers.

        Returns the first sequence number in the allocated range.
        The allocated range is [returned_value, returned_value + count - 1].

        Uses INSERT ... ON CONFLICT DO UPDATE to handle both the first-ever
        allocation (INSERT) and subsequent allocations (UPDATE) in one statement.
        """
        if count < 1:
            raise ValueError(f"count must be >= 1, got {count}")

        table = f'"{self._schema}"."naskod_sequence"'

        # Atomic upsert: insert new row with next_seq = 1 + count,
        # or update existing row by incrementing next_seq by count.
        # RETURNING gives us the value BEFORE the increment (the start of our range).
        sql = sa.text(f"""
            INSERT INTO {table} (state_code, district_code, address_type, next_seq, updated_at)
            VALUES (:state_code, :district_code, :address_type, 1 + :count, NOW())
            ON CONFLICT (state_code, district_code, address_type)
            DO UPDATE SET
                next_seq = {table}.next_seq + :count,
                updated_at = NOW()
            RETURNING next_seq - :count AS start_seq
        """)

        with self.engine.begin() as conn:
            result = conn.execute(
                sql,
                {
                    "state_code": state_code,
                    "district_code": district_code,
                    "address_type": address_type,
                    "count": count,
                },
            )
            row = result.fetchone()
            assert row is not None, "RETURNING clause must produce a row"
            return int(row[0])

    def allocate_many(
        self,
        groups: list[dict[str, Any]],
    ) -> dict[tuple[str, str, str], int]:
        """Allocate sequences for multiple groups in a single transaction.

        Parameters
        ----------
        groups : list of dicts
            Each dict must have keys: state_code, district_code, address_type, count.

        Returns
        -------
        dict mapping (state_code, district_code, address_type) → start_seq
        """
        if not groups:
            return {}

        table = f'"{self._schema}"."naskod_sequence"'
        sql = sa.text(f"""
            INSERT INTO {table} (state_code, district_code, address_type, next_seq, updated_at)
            VALUES (:state_code, :district_code, :address_type, 1 + :count, NOW())
            ON CONFLICT (state_code, district_code, address_type)
            DO UPDATE SET
                next_seq = {table}.next_seq + :count,
                updated_at = NOW()
            RETURNING state_code, district_code, address_type, next_seq - :count AS start_seq
        """)

        results: dict[tuple[str, str, str], int] = {}
        with self.engine.begin() as conn:
            for group in groups:
                row = conn.execute(sql, group).fetchone()
                assert row is not None
                key = (row[0], row[1], row[2])
                results[key] = int(row[3])

        return results

    def format_naskod(
        self,
        state_code: str,
        district_code: str,
        address_type: str,
        seq: int,
    ) -> str | None:
        """Format a single NASKOD string from its components.

        Returns None if state_code cannot be mapped to an abbreviation.
        """
        code = state_code.zfill(2) if state_code and state_code.isdigit() else state_code
        abbr = STATE_ABBR_MAP.get(code)
        if not abbr:
            return None
        district = district_code.zfill(2) if district_code and district_code.isdigit() else district_code
        type_code = _address_type_code(address_type)
        return f"NAS-{abbr}-{district}-{type_code}{seq:06d}"

    def close(self) -> None:
        """Dispose the engine connection pool."""
        if self._engine is not None:
            self._engine.dispose()
            self._engine = None
