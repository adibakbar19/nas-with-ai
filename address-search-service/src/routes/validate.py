"""Address validation routes."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Header

from src.schemas.requests import BatchValidateRequest, ValidateRequest
from src.schemas.responses import BatchValidateResponse, ValidateResult
from src.services.validate_service import batch_validate_addresses, validate_address
from config import settings

router = APIRouter(tags=["validate"])


def verify_api_key(
    api_key: str | None = Header(alias="X-API-Key", default=None),
) -> str:
    if not settings.auth_enabled:
        return "anonymous"
    if api_key is None or api_key not in settings.api_keys:
        raise HTTPException(status_code=401, detail="Invalid API key")
    return api_key


@router.post("/validate", response_model=ValidateResult)
def validate(
    body: ValidateRequest,
    _: str = Depends(verify_api_key),
) -> ValidateResult:
    return validate_address(
        address=body.address,
        postcode=body.postcode,
        state_code=body.state_code,
    )


@router.post("/batch-validate", response_model=BatchValidateResponse)
def batch_validate(
    body: BatchValidateRequest,
    _: str = Depends(verify_api_key),
) -> dict:
    if len(body.addresses) > settings.max_batch_size:
        raise HTTPException(
            status_code=400,
            detail=f"Too many addresses: max {settings.max_batch_size}",
        )
    results, elapsed_ms = batch_validate_addresses(body.addresses)
    matched = sum(1 for r in results if r.matched)
    return {
        "results": results,
        "total": len(results),
        "matched": matched,
        "unmatched": len(results) - matched,
        "query_time_ms": elapsed_ms,
    }
