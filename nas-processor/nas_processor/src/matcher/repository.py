"""All DB operations for the matcher — fetch, update, insert.

Uses SQLAlchemy with psycopg driver. Engines are cached per DSN to reuse
the connection pool across calls in the polling loop.
"""

from __future__ import annotations

import logging

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import ARRAY as PgARRAY

logger = logging.getLogger(__name__)

_engines: dict[str, sa.Engine] = {}


def _get_engine(dsn: str) -> sa.Engine:
    if dsn not in _engines:
        _engines[dsn] = sa.create_engine(dsn, pool_pre_ping=True)
    return _engines[dsn]


def _q(schema: str, table: str) -> str:
    return f'"{schema}"."{table}"'


# ── Fetch ──────────────────────────────────────────────────────────────────────


def fetch_unmatched_batch(
    limit: int,
    dsn: str,
    schema: str,
) -> list[dict]:
    """Return up to *limit* unmatched raw_address rows joined to their standardized partner."""
    sql = sa.text(f"""
        SELECT
            r.raw_id,
            r.raw_text,
            r.source_ref_id,
            r.agency_id,
            r.job_id,
            r.error_reason,
            s.address_clean,
            s.canonical_address_key,
            s.postcode,
            s.mukim_id,
            s.pbt_id,
            s.state_code,
            s.street_name,
            s.locality_name,
            s.record_id AS standardized_record_id
        FROM {_q(schema, 'raw_address')} r
        LEFT JOIN {_q(schema, 'standardized_address')} s
            ON r.source_ref_id = s.record_id
        WHERE r.match_status = 'unmatched'
        ORDER BY r.ingest_timestamp ASC
        LIMIT :limit
    """)
    engine = _get_engine(dsn)
    try:
        with engine.connect() as conn:
            rows = conn.execute(sql, {"limit": limit}).mappings().all()
        return [dict(r) for r in rows]
    except Exception:
        logger.exception("fetch_unmatched_batch_failed")
        return []


def fetch_candidates(
    postcode: str | None,
    mukim_id: str | None,
    state_code: str | None,
    exclude_record_id: str,
    limit: int,
    dsn: str,
    schema: str,
) -> list[dict]:
    """Return candidate standardized_address rows in the same spatial bucket."""
    sql = sa.text(f"""
        SELECT
            record_id,
            address_clean,
            canonical_address_key,
            postcode,
            mukim_id,
            street_name,
            locality_name,
            confidence_band
        FROM {_q(schema, 'standardized_address')}
        WHERE lifecycle_status NOT IN ('archived', 'inactive')
          AND record_id != :exclude_record_id
          AND (
              mukim_id = :mukim_id
              OR (mukim_id IS NULL AND postcode = :postcode)
              OR state_code = :state_code
          )
        ORDER BY
            CASE
                WHEN mukim_id = :mukim_id THEN 0
                WHEN postcode = :postcode THEN 1
                ELSE 2
            END
        LIMIT :limit
    """)
    engine = _get_engine(dsn)
    try:
        with engine.connect() as conn:
            rows = conn.execute(
                sql,
                {
                    "exclude_record_id": exclude_record_id,
                    "mukim_id": mukim_id,
                    "postcode": postcode,
                    "state_code": state_code,
                    "limit": limit,
                },
            ).mappings().all()
        return [dict(r) for r in rows]
    except Exception:
        logger.exception(
            "fetch_candidates_failed exclude=%s mukim=%s postcode=%s state=%s",
            exclude_record_id, mukim_id, postcode, state_code,
        )
        return []


# ── Update ─────────────────────────────────────────────────────────────────────


def update_matched(
    raw_id: str,
    standardized_id: str,
    confidence: float,
    dsn: str,
    schema: str,
) -> None:
    sql = sa.text(f"""
        UPDATE {_q(schema, 'raw_address')}
        SET match_status = 'matched',
            standardized_id = :standardized_id,
            match_confidence = :confidence
        WHERE raw_id = :raw_id
    """)
    engine = _get_engine(dsn)
    try:
        with engine.begin() as conn:
            conn.execute(sql, {"raw_id": raw_id, "standardized_id": standardized_id, "confidence": confidence})
    except Exception:
        logger.exception("update_matched_failed raw_id=%s standardized_id=%s", raw_id, standardized_id)
        raise


def update_needs_review(raw_id: str, dsn: str, schema: str) -> None:
    sql = sa.text(f"""
        UPDATE {_q(schema, 'raw_address')}
        SET match_status = 'needs_review'
        WHERE raw_id = :raw_id
    """)
    engine = _get_engine(dsn)
    try:
        with engine.begin() as conn:
            conn.execute(sql, {"raw_id": raw_id})
    except Exception:
        logger.exception("update_needs_review_failed raw_id=%s", raw_id)
        raise


# ── Insert ─────────────────────────────────────────────────────────────────────


def insert_alias(
    standardized_id: str,
    alias_text: str,
    alias_type: str,
    source_system: str,
    dsn: str,
    schema: str,
) -> None:
    sql = sa.text(f"""
        INSERT INTO {_q(schema, 'address_alias')}
            (alias_id, standardized_id, alias_text, alias_type, source_system, valid_from)
        VALUES
            (gen_random_uuid(), :standardized_id, :alias_text, :alias_type, :source_system, NOW())
        ON CONFLICT DO NOTHING
    """)
    engine = _get_engine(dsn)
    try:
        with engine.begin() as conn:
            conn.execute(
                sql,
                {
                    "standardized_id": standardized_id,
                    "alias_text": alias_text,
                    "alias_type": alias_type,
                    "source_system": source_system,
                },
            )
    except Exception:
        logger.exception(
            "insert_alias_failed standardized_id=%s alias_type=%s", standardized_id, alias_type
        )


def insert_review(
    raw_id: str,
    candidate_ids: list[str],
    candidate_scores: list[float],
    reason: str,
    dsn: str,
    schema: str,
) -> None:
    stmt = sa.text(f"""
        INSERT INTO {_q(schema, 'match_review_queue')}
            (review_id, raw_id, candidate_ids, candidate_scores, reason, status, created_at, updated_at)
        VALUES
            (gen_random_uuid()::text, :raw_id, :candidate_ids, :candidate_scores, :reason, 'pending', NOW(), NOW())
    """).bindparams(
        sa.bindparam("raw_id", type_=sa.Text),
        sa.bindparam("candidate_ids", type_=PgARRAY(sa.Text)),
        sa.bindparam("candidate_scores", type_=PgARRAY(sa.Float)),
        sa.bindparam("reason", type_=sa.Text),
    )
    engine = _get_engine(dsn)
    try:
        with engine.begin() as conn:
            conn.execute(
                stmt,
                {
                    "raw_id": raw_id,
                    "candidate_ids": candidate_ids,
                    "candidate_scores": candidate_scores,
                    "reason": reason,
                },
            )
    except Exception:
        logger.exception("insert_review_failed raw_id=%s", raw_id)
        raise
