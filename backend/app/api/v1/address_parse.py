from typing import Any

from fastapi import APIRouter, Depends, HTTPException

from backend.app.dependencies import get_address_parse_service
from backend.app.schemas.ingest import AddressParseRequest, AddressParseResponse
from backend.app.security import Agency, get_current_agency, has_permission
from backend.app.services.address_parse_service import AddressParseService
from backend.app.services.errors import ServiceError

router = APIRouter(tags=["address-parse"])


def _require_parse_permission(agency: Agency) -> None:
    if has_permission(agency, "address.parse") or has_permission(agency, "ingest.upload"):
        return
    raise HTTPException(status_code=403, detail="missing permission: address.parse")


@router.post("/api/v1/address/parse", response_model=AddressParseResponse, response_model_exclude_none=True)
def parse_address(
    request: AddressParseRequest,
    agency: Agency = Depends(get_current_agency),
    address_parse_service: AddressParseService = Depends(get_address_parse_service),
) -> dict[str, Any]:
    _require_parse_permission(agency)
    try:
        return address_parse_service.parse(
            address=request.address,
            addresses=request.addresses,
            text=request.text,
            csv_text=request.csv_text,
            csv_address_column=request.csv_address_column,
            use_ai=request.use_ai,
            require_mukim=request.require_mukim,
            ai_min_confidence=request.ai_min_confidence,
            max_records=request.max_records,
        )
    except ServiceError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.detail) from exc
