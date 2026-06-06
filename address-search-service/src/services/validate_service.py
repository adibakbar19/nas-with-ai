"""Address validation service."""

from __future__ import annotations

import logging
import time

from src.schemas.responses import ValidateResult
from src.services.search_service import search_addresses

logger = logging.getLogger(__name__)

_MIN_MATCH_SCORE = 0.5


def validate_address(
    address: str,
    postcode: str | None = None,
    state_code: str | None = None,
) -> ValidateResult:
    """Validate an address against the canonical index. Never raises."""
    try:
        results, total, _ = search_addresses(
            q=address,
            postcode=postcode,
            state_code=state_code,
            limit=1,
        )

        if not results:
            return ValidateResult(
                input_address=address,
                matched=False,
                match_type="no_match",
                confidence=0.0,
            )

        top = results[0]
        raw_score = top.get("score") or 0.0

        # Normalise score — best_fields scores vary; values above 0.5 indicate good match
        confidence = min(raw_score / 20.0, 1.0) if raw_score else 0.0

        if confidence < _MIN_MATCH_SCORE:
            return ValidateResult(
                input_address=address,
                matched=False,
                match_type="no_match",
                confidence=round(confidence, 4),
            )

        # Determine match type by precision of available data
        if top.get("latitude") is not None and top.get("longitude") is not None:
            match_type = "rooftop"
        elif top.get("street_name"):
            match_type = "street"
        elif top.get("postcode"):
            match_type = "locality"
        else:
            match_type = "no_match"

        return ValidateResult(
            input_address=address,
            matched=True,
            match_type=match_type,
            confidence=round(confidence, 4),
            record_id=top.get("record_id"),
            naskod=top.get("naskod"),
            address_clean=top.get("address_clean"),
            postcode=top.get("postcode"),
            state_name=top.get("state_name"),
            latitude=top.get("latitude"),
            longitude=top.get("longitude"),
        )

    except Exception:
        logger.exception("validate_address_failed address=%r", address)
        return ValidateResult(
            input_address=address,
            matched=False,
            match_type="no_match",
            confidence=0.0,
        )


def batch_validate_addresses(
    addresses: list[str],
) -> tuple[list[ValidateResult], int]:
    """Validate a list of addresses synchronously. Returns (results, query_time_ms)."""
    t0 = time.time()
    results = []
    for addr in addresses:
        try:
            result = validate_address(addr)
        except Exception:
            result = ValidateResult(
                input_address=addr,
                matched=False,
                match_type="no_match",
                confidence=0.0,
            )
        results.append(result)
    return results, int((time.time() - t0) * 1000)
