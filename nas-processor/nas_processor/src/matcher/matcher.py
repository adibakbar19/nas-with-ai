"""Address matching orchestration — processes batches of unmatched raw_address rows."""

from __future__ import annotations

import logging

from nas_processor.src.matcher.config import MatcherConfig
from nas_processor.src.matcher.repository import (
    fetch_candidates,
    fetch_unmatched_batch,
    insert_alias,
    insert_review,
    update_matched,
    update_needs_review,
)
from nas_processor.src.matcher.scorer import rank_candidates

logger = logging.getLogger(__name__)


class AddressMatcher:
    def __init__(self, config: MatcherConfig) -> None:
        self._cfg = config

    def process_batch(self) -> int:
        """Process one batch of unmatched raw_address rows. Returns number processed."""
        rows = fetch_unmatched_batch(
            limit=self._cfg.batch_size,
            dsn=self._cfg.dsn,
            schema=self._cfg.schema,
        )
        for row in rows:
            try:
                self._process_row(row)
            except Exception:
                logger.exception("process_row_failed raw_id=%s", row.get("raw_id"))
        return len(rows)

    def _process_row(self, row: dict) -> None:
        cfg = self._cfg
        raw_id = row["raw_id"]
        standardized_record_id = row.get("standardized_record_id")

        # Raw address has no standardized partner — ETL failed to create one, skip
        if not standardized_record_id:
            logger.warning("no_standardized_partner raw_id=%s source_ref_id=%s", raw_id, row.get("source_ref_id"))
            return

        candidates = fetch_candidates(
            postcode=row.get("postcode"),
            mukim_id=row.get("mukim_id"),
            state_code=row.get("state_code"),
            exclude_record_id=standardized_record_id,
            limit=cfg.candidate_limit,
            dsn=cfg.dsn,
            schema=cfg.schema,
        )

        if not candidates:
            # No other canonical records in this spatial bucket — this IS the canonical
            update_matched(
                raw_id=raw_id,
                standardized_id=standardized_record_id,
                confidence=1.0,
                dsn=cfg.dsn,
                schema=cfg.schema,
            )
            logger.info("self_linked raw_id=%s standardized_id=%s", raw_id, standardized_record_id)
            return

        ranked = rank_candidates(row, candidates)
        best_candidate, best_score = ranked[0]

        if best_score >= cfg.auto_match_threshold:
            update_matched(
                raw_id=raw_id,
                standardized_id=best_candidate["record_id"],
                confidence=best_score,
                dsn=cfg.dsn,
                schema=cfg.schema,
            )
            alias_type = "cross_agency" if row.get("agency_id") else "non_standard"
            insert_alias(
                standardized_id=best_candidate["record_id"],
                alias_text=row["raw_text"] or "",
                alias_type=alias_type,
                source_system=row.get("agency_id") or "nas_etl",
                dsn=cfg.dsn,
                schema=cfg.schema,
            )
            logger.info(
                "auto_matched raw_id=%s standardized_id=%s score=%.4f alias_type=%s",
                raw_id, best_candidate["record_id"], best_score, alias_type,
            )

        elif best_score >= cfg.review_threshold:
            update_needs_review(raw_id=raw_id, dsn=cfg.dsn, schema=cfg.schema)
            top3 = ranked[:3]

            # Build a descriptive reason so reviewers immediately see WHY it needs review
            reason_parts = [f"best_score={best_score:.2f}"]
            top_cand = best_candidate
            if top_cand.get("postcode") and row.get("postcode"):
                reason_parts.append(
                    "postcode_match" if top_cand["postcode"] == row["postcode"] else "postcode_differs"
                )
            if top_cand.get("state_code") and row.get("state_code"):
                reason_parts.append(
                    "state_match" if top_cand["state_code"] == row["state_code"] else "state_differs"
                )
            if len(ranked) > 1 and ranked[1][1] >= cfg.review_threshold:
                reason_parts.append("multiple_candidates_above_threshold")

            insert_review(
                raw_id=raw_id,
                candidate_ids=[c["record_id"] for c, _ in top3],
                candidate_scores=[s for _, s in top3],
                reason=" | ".join(reason_parts),
                dsn=cfg.dsn,
                schema=cfg.schema,
            )
            logger.info(
                "review_queued raw_id=%s top_score=%.4f candidates=%d",
                raw_id, best_score, len(top3),
            )

        else:
            # Best score too low — raw address represents a genuinely new canonical record
            update_matched(
                raw_id=raw_id,
                standardized_id=standardized_record_id,
                confidence=best_score,
                dsn=cfg.dsn,
                schema=cfg.schema,
            )
            logger.info(
                "new_canonical raw_id=%s standardized_id=%s score=%.4f",
                raw_id, standardized_record_id, best_score,
            )
