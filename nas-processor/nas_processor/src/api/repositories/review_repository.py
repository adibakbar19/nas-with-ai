"""Review queue repository — all DB operations for match_review_queue resolution.

Uses psycopg3 with dict_row. resolve_review runs in a single transaction so
partial state is never committed if any step fails.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

try:
    import psycopg
    from psycopg import sql
    from psycopg.rows import dict_row
except Exception:  # pragma: no cover
    psycopg = None
    sql = None
    dict_row = None


class ReviewError(Exception):
    """Raised for known bad-state conditions in review operations."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


def _connect(dsn: str):
    if psycopg is None or dict_row is None:
        raise RuntimeError("psycopg is required for ReviewRepository")
    return psycopg.connect(dsn, row_factory=dict_row)


def _tbl(schema: str, table: str):
    assert sql is not None
    return sql.SQL("{}.{}").format(sql.Identifier(schema), sql.Identifier(table))


# ── Helpers ────────────────────────────────────────────────────────────────────


_COMPARE_FIELDS = ("postcode", "state_code", "locality_name", "street_name")


def _build_candidate(
    raw_row: dict,
    candidate: dict,
    score: float,
    agency_match_count: int,
) -> dict[str, Any]:
    """Attach computed comparison fields to a candidate dict."""
    lat = candidate.get("latitude")
    lon = candidate.get("longitude")
    maps_url = f"https://www.google.com/maps?q={lat},{lon}" if lat is not None and lon is not None else None

    matching: list[str] = []
    differing: list[str] = []
    for field in _COMPARE_FIELDS:
        raw_val = (raw_row.get(field) or "").strip().upper()
        cand_val = (candidate.get(field) or "").strip().upper()
        if raw_val and cand_val:
            (matching if raw_val == cand_val else differing).append(field)

    return {
        **candidate,
        "score": float(score),
        "matching_fields": matching,
        "differing_fields": differing,
        "other_agency_count": agency_match_count,
        "maps_url": maps_url,
    }


def _fetch_agency_counts(cur, candidate_ids: list[str], schema: str) -> dict[str, int]:
    """Count how many matched raw_address rows point to each candidate."""
    if not candidate_ids:
        return {}
    assert sql is not None
    stmt = sql.SQL(
        "SELECT standardized_id, COUNT(*) AS cnt "
        "FROM {tbl} "
        "WHERE standardized_id = ANY(%s) AND match_status='matched' "
        "GROUP BY standardized_id"
    ).format(tbl=_tbl(schema, "raw_address"))
    cur.execute(stmt, (candidate_ids,))
    return {r["standardized_id"]: int(r["cnt"]) for r in cur.fetchall()}


def _fetch_candidates_by_ids(
    cur,
    candidate_ids: list[str],
    scores: list[float],
    schema: str,
    raw_row: dict | None = None,
) -> list[dict[str, Any]]:
    """Return enriched candidate list with scores, comparison fields, and agency counts."""
    if not candidate_ids:
        return []
    assert sql is not None
    stmt = sql.SQL(
        "SELECT record_id, address_clean, postcode, state_name, state_code, "
        "       locality_name, street_name, mukim_name, naskod, "
        "       confidence_band, latitude, longitude "
        "FROM {tbl} WHERE record_id = ANY(%s)"
    ).format(tbl=_tbl(schema, "standardized_address"))
    cur.execute(stmt, (candidate_ids,))
    rows_by_id = {r["record_id"]: dict(r) for r in cur.fetchall()}

    agency_counts = _fetch_agency_counts(cur, candidate_ids, schema)

    result = []
    for cid, score in zip(candidate_ids, scores or []):
        candidate = dict(rows_by_id.get(cid, {"record_id": cid}))
        result.append(_build_candidate(
            raw_row or {},
            candidate,
            score,
            agency_counts.get(cid, 0),
        ))
    return result


def _enrich_rows(rows: list[dict], schema: str, cur) -> list[dict[str, Any]]:
    """Attach enriched candidate details to a list of review rows (single batch query)."""
    if not rows:
        return []

    all_ids: list[str] = []
    for r in rows:
        all_ids.extend(r.get("candidate_ids") or [])
    unique_ids = list(dict.fromkeys(all_ids))

    candidates_map: dict[str, dict] = {}
    agency_counts: dict[str, int] = {}
    if unique_ids:
        assert sql is not None
        cur.execute(
            sql.SQL(
                "SELECT record_id, address_clean, postcode, state_name, state_code, "
                "       locality_name, street_name, mukim_name, naskod, "
                "       confidence_band, latitude, longitude "
                "FROM {tbl} WHERE record_id = ANY(%s)"
            ).format(tbl=_tbl(schema, "standardized_address")),
            (unique_ids,),
        )
        candidates_map = {r["record_id"]: dict(r) for r in cur.fetchall()}
        agency_counts = _fetch_agency_counts(cur, unique_ids, schema)

    enriched = []
    for row in rows:
        d = dict(row)
        cids = d.get("candidate_ids") or []
        scores = d.get("candidate_scores") or []
        d["candidates"] = [
            _build_candidate(
                d,
                dict(candidates_map.get(cid, {"record_id": cid})),
                float(s),
                agency_counts.get(cid, 0),
            )
            for cid, s in zip(cids, scores)
        ]
        enriched.append(d)
    return enriched


# ── Public API ─────────────────────────────────────────────────────────────────


def fetch_pending_reviews(
    *,
    assigned_to: str | None,
    status: str | None,
    agency_id: str | None,
    limit: int,
    offset: int,
    dsn: str,
    schema: str,
) -> tuple[list[dict[str, Any]], int]:
    """Return (enriched_reviews, total_count) with optional filters."""
    assert sql is not None

    conditions = [sql.SQL("1=1")]
    params: list[Any] = []

    if status:
        conditions.append(sql.SQL("q.status = %s"))
        params.append(status)
    if assigned_to:
        conditions.append(sql.SQL("q.assigned_to = %s"))
        params.append(assigned_to)
    if agency_id:
        conditions.append(sql.SQL("r.agency_id = %s"))
        params.append(agency_id)

    where = sql.SQL(" AND ").join(conditions)

    stmt = sql.SQL(
        "SELECT "
        "  q.review_id, q.raw_id, q.candidate_ids, q.candidate_scores, "
        "  q.reason, q.status, q.assigned_to, q.assigned_at, "
        "  q.resolved_at, q.decision, q.resolved_by, q.resolution_notes, "
        "  q.created_at, q.updated_at, "
        "  r.raw_text, r.agency_id, r.error_reason, "
        "  r.match_confidence AS original_confidence, "
        "  COUNT(*) OVER() AS _total_count "
        "FROM {q_tbl} q "
        "JOIN {r_tbl} r ON r.raw_id = q.raw_id "
        "WHERE {where} "
        "ORDER BY q.created_at ASC "
        "LIMIT %s OFFSET %s"
    ).format(
        q_tbl=_tbl(schema, "match_review_queue"),
        r_tbl=_tbl(schema, "raw_address"),
        where=where,
    )

    params.extend([limit, offset])

    with _connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(stmt, params)
        rows = cur.fetchall()
        if not rows:
            return [], 0
        total = int(rows[0]["_total_count"])
        cleaned = [{k: v for k, v in r.items() if k != "_total_count"} for r in rows]
        enriched = _enrich_rows(cleaned, schema, cur)

    return enriched, total


def get_review_by_id(review_id: str, *, dsn: str, schema: str) -> dict[str, Any] | None:
    """Return a single review with full candidate details, or None if not found."""
    assert sql is not None
    stmt = sql.SQL(
        "SELECT "
        "  q.review_id, q.raw_id, q.candidate_ids, q.candidate_scores, "
        "  q.reason, q.status, q.assigned_to, q.assigned_at, "
        "  q.resolved_at, q.decision, q.resolved_by, q.resolution_notes, "
        "  q.created_at, q.updated_at, "
        "  r.raw_text, r.agency_id, r.error_reason, "
        "  r.match_confidence AS original_confidence "
        "FROM {q_tbl} q "
        "JOIN {r_tbl} r ON r.raw_id = q.raw_id "
        "WHERE q.review_id = %s "
        "LIMIT 1"
    ).format(
        q_tbl=_tbl(schema, "match_review_queue"),
        r_tbl=_tbl(schema, "raw_address"),
    )
    with _connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(stmt, (review_id,))
        row = cur.fetchone()
        if row is None:
            return None
        d = dict(row)
        cids = d.get("candidate_ids") or []
        scores = d.get("candidate_scores") or []
        d["candidates"] = _fetch_candidates_by_ids(cur, cids, scores, schema, raw_row=d)
    return d


def assign_review(
    review_id: str,
    *,
    assigned_to: str,
    dsn: str,
    schema: str,
) -> dict[str, Any]:
    """Claim a pending review for a reviewer.

    Raises ReviewError("not_found") if review_id doesn't exist.
    Raises ReviewError("conflict") if already assigned/resolved.
    Returns the updated review dict.
    """
    assert sql is not None
    q_tbl = _tbl(schema, "match_review_queue")

    with _connect(dsn) as conn:
        with conn.cursor() as cur:
            # Lock the row for update
            cur.execute(
                sql.SQL(
                    "SELECT review_id, status, assigned_to FROM {tbl} "
                    "WHERE review_id = %s FOR UPDATE"
                ).format(tbl=q_tbl),
                (review_id,),
            )
            row = cur.fetchone()
            if row is None:
                raise ReviewError("not_found", f"review {review_id!r} not found")
            if row["status"] != "pending":
                raise ReviewError(
                    "conflict",
                    f"review is already {row['status']}"
                    + (f" (assigned to {row['assigned_to']})" if row.get("assigned_to") else ""),
                )

            cur.execute(
                sql.SQL(
                    "UPDATE {tbl} "
                    "SET status='assigned', assigned_to=%s, assigned_at=NOW(), updated_at=NOW() "
                    "WHERE review_id=%s"
                ).format(tbl=q_tbl),
                (assigned_to, review_id),
            )
        conn.commit()

    result = get_review_by_id(review_id, dsn=dsn, schema=schema)
    if result is None:
        raise ReviewError("not_found", f"review {review_id!r} not found after assign")
    return result


def resolve_review(
    review_id: str,
    *,
    decision: str,
    resolved_by: str,
    chosen_candidate_id: str | None,
    resolution_notes: str | None,
    dsn: str,
    schema: str,
) -> dict[str, Any]:
    """Resolve a review with a final decision.

    Runs entirely in one transaction — never leaves partial state.

    Raises ReviewError for:
      "not_found"          — review_id doesn't exist
      "already_resolved"   — status is already resolved
      "invalid_decision"   — decision not in (match, new, reject)
      "invalid_candidate"  — decision=match but chosen_candidate_id missing/invalid
    """
    assert sql is not None
    valid_decisions = {"match", "new", "reject"}
    if decision not in valid_decisions:
        raise ReviewError("invalid_decision", f"decision must be one of {sorted(valid_decisions)}")
    if decision == "match" and not chosen_candidate_id:
        raise ReviewError("invalid_candidate", "chosen_candidate_id is required when decision=match")

    q_tbl = _tbl(schema, "match_review_queue")
    raw_tbl = _tbl(schema, "raw_address")
    alias_tbl = _tbl(schema, "address_alias")

    alias_created = False
    standardized_id: str | None = None

    with _connect(dsn) as conn:
        with conn.cursor() as cur:
            # 1. Lock and fetch the review row
            cur.execute(
                sql.SQL(
                    "SELECT q.review_id, q.raw_id, q.status, q.candidate_ids, q.candidate_scores "
                    "FROM {tbl} q WHERE q.review_id = %s FOR UPDATE"
                ).format(tbl=q_tbl),
                (review_id,),
            )
            review = cur.fetchone()
            if review is None:
                raise ReviewError("not_found", f"review {review_id!r} not found")
            if review["status"] == "resolved":
                raise ReviewError("already_resolved", f"review {review_id!r} is already resolved")

            raw_id = review["raw_id"]
            candidate_ids: list[str] = list(review["candidate_ids"] or [])

            # Validate chosen_candidate_id against the stored candidate list
            if decision == "match":
                if chosen_candidate_id not in candidate_ids:
                    raise ReviewError(
                        "invalid_candidate",
                        f"chosen_candidate_id {chosen_candidate_id!r} is not in the candidate list",
                    )

            # 2. Fetch raw_address (need raw_text, agency_id, source_ref_id)
            cur.execute(
                sql.SQL(
                    "SELECT raw_id, raw_text, agency_id, source_ref_id "
                    "FROM {tbl} WHERE raw_id = %s FOR UPDATE"
                ).format(tbl=raw_tbl),
                (raw_id,),
            )
            raw = cur.fetchone()
            if raw is None:
                raise ReviewError("not_found", f"raw_address {raw_id!r} not found")

            # 3. UPDATE match_review_queue
            cur.execute(
                sql.SQL(
                    "UPDATE {tbl} "
                    "SET status='resolved', decision=%s, resolved_by=%s, "
                    "    resolved_at=NOW(), resolution_notes=%s, updated_at=NOW() "
                    "WHERE review_id=%s"
                ).format(tbl=q_tbl),
                (decision, resolved_by, resolution_notes, review_id),
            )

            # 4. Side effects based on decision
            if decision == "match":
                # 4a. Link raw_address to chosen canonical
                standardized_id = chosen_candidate_id
                cur.execute(
                    sql.SQL(
                        "UPDATE {tbl} "
                        "SET match_status='matched', standardized_id=%s, match_confidence=1.0 "
                        "WHERE raw_id=%s"
                    ).format(tbl=raw_tbl),
                    (chosen_candidate_id, raw_id),
                )
                # Create alias linking the raw text to the chosen canonical
                raw_text = raw["raw_text"] or ""
                agency_id = raw["agency_id"] or "nas_etl"
                cur.execute(
                    sql.SQL(
                        "INSERT INTO {tbl} "
                        "(alias_id, standardized_id, alias_text, alias_type, source_system, valid_from) "
                        "VALUES (gen_random_uuid()::text, %s, %s, 'cross_agency', %s, NOW()) "
                        "ON CONFLICT DO NOTHING"
                    ).format(tbl=alias_tbl),
                    (chosen_candidate_id, raw_text, agency_id),
                )
                alias_created = cur.rowcount > 0

            elif decision == "new":
                # 4b. Confirm as a new canonical — link to its own standardized_address.
                # Verify that source_ref_id actually exists in standardized_address (failed
                # rows may have a source_ref_id that was never loaded into standardized_address).
                source_ref_id = raw["source_ref_id"]
                if not source_ref_id:
                    raise ReviewError(
                        "invalid_decision",
                        "this raw_address has no source_ref_id — cannot confirm as new canonical",
                    )
                cur.execute(
                    sql.SQL(
                        "SELECT record_id FROM {tbl} WHERE record_id = %s LIMIT 1"
                    ).format(tbl=_tbl(schema, "standardized_address")),
                    (source_ref_id,),
                )
                if cur.fetchone() is None:
                    raise ReviewError(
                        "invalid_decision",
                        f"source_ref_id {source_ref_id!r} has no standardized_address record "
                        "(address may have been a failed ETL row)",
                    )
                standardized_id = source_ref_id
                cur.execute(
                    sql.SQL(
                        "UPDATE {tbl} "
                        "SET match_status='matched', standardized_id=%s, match_confidence=1.0 "
                        "WHERE raw_id=%s"
                    ).format(tbl=raw_tbl),
                    (source_ref_id, raw_id),
                )

            else:
                # 4c. Reject
                cur.execute(
                    sql.SQL(
                        "UPDATE {tbl} SET match_status='rejected' WHERE raw_id=%s"
                    ).format(tbl=raw_tbl),
                    (raw_id,),
                )

        conn.commit()

    return {
        "review_id": review_id,
        "decision": decision,
        "raw_id": raw_id,
        "standardized_id": standardized_id,
        "alias_created": alias_created,
        "message": f"review resolved as {decision}",
    }


def get_review_stats(*, dsn: str, schema: str) -> dict[str, Any]:
    """Return aggregate counts by status and decision, plus oldest pending age."""
    assert sql is not None
    q_tbl = _tbl(schema, "match_review_queue")

    with _connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            sql.SQL(
                "SELECT status, COUNT(*) AS cnt FROM {tbl} GROUP BY status"
            ).format(tbl=q_tbl)
        )
        by_status: dict[str, int] = {r["status"]: int(r["cnt"]) for r in cur.fetchall()}

        cur.execute(
            sql.SQL(
                "SELECT decision, COUNT(*) AS cnt FROM {tbl} "
                "WHERE status='resolved' AND decision IS NOT NULL "
                "GROUP BY decision"
            ).format(tbl=q_tbl)
        )
        by_decision: dict[str, int] = {r["decision"]: int(r["cnt"]) for r in cur.fetchall()}

        cur.execute(
            sql.SQL(
                "SELECT EXTRACT(EPOCH FROM (NOW() - MIN(created_at))) / 3600.0 AS hours "
                "FROM {tbl} WHERE status='pending'"
            ).format(tbl=q_tbl)
        )
        row = cur.fetchone()
        oldest_hours = float(row["hours"]) if row and row["hours"] is not None else 0.0

    return {
        "pending": by_status.get("pending", 0),
        "assigned": by_status.get("assigned", 0),
        "resolved": by_status.get("resolved", 0),
        "decisions": {
            "match": by_decision.get("match", 0),
            "new": by_decision.get("new", 0),
            "reject": by_decision.get("reject", 0),
        },
        "oldest_pending_hours": round(oldest_hours, 2),
    }
