import re
from typing import Any

from backend.app.repositories.address_repository import AddressRepository, SpatialValidationResult
from backend.app.schemas.address import AddressSearchItem, AddressSearchResponse, ValidateAddressRequest, ValidateAddressResponse
from backend.app.search.elasticsearch_gateway import ElasticsearchSearchGateway


_SPACE_RE = re.compile(r"\s+")


class AddressService:
    def __init__(
        self,
        *,
        repository: AddressRepository,
        search_gateway: ElasticsearchSearchGateway,
    ) -> None:
        self._repository = repository
        self._search_gateway = search_gateway

    def search_addresses(self, *, query: str, size: int) -> AddressSearchResponse:
        hits = self._search_gateway.autocomplete(query=query, size=size)
        items = [AddressSearchItem(**hit) for hit in hits]
        return AddressSearchResponse(query=query, size=size, count=len(items), items=items)

    def validate_address(self, payload: ValidateAddressRequest) -> ValidateAddressResponse:
        normalized = self._normalize_address(payload.raw_address)
        reasons: list[str] = []
        confidence = 100
        spatial_match: bool | None = None
        canonical_record: dict[str, Any] | None = None

        if payload.address_id:
            try:
                canonical_record = self._repository.get_canonical_address(payload.address_id)
            except Exception:
                reasons.append("canonical_lookup_unavailable")
                confidence -= 5
            else:
                if not canonical_record:
                    reasons.append("address_id_not_found")
                    confidence -= 15

        if payload.postcode and payload.latitude is not None and payload.longitude is not None:
            try:
                geo = self._repository.validate_postcode_geometry(
                    postcode=payload.postcode,
                    latitude=payload.latitude,
                    longitude=payload.longitude,
                )
            except Exception:
                reasons.append("spatial_validation_unavailable")
                confidence -= 5
            else:
                spatial_match = geo.is_within_postcode_boundary
                confidence, reasons = self._apply_spatial_rules(
                    confidence=confidence,
                    reasons=reasons,
                    payload=payload,
                    geo_result=geo,
                )
        elif payload.postcode and (payload.latitude is None or payload.longitude is None):
            reasons.append("missing_coordinates_for_spatial_validation")
            confidence -= 10

        if len(normalized) < 10:
            reasons.append("address_too_short")
            confidence -= 20

        confidence = max(0, min(100, confidence))
        is_valid = confidence >= 75 and not any(r in {"postcode_geometry_mismatch", "address_id_not_found"} for r in reasons)

        return ValidateAddressResponse(
            normalized_address=normalized,
            confidence_score=confidence,
            is_valid=is_valid,
            validation_reasons=reasons,
            spatial_match=spatial_match,
            canonical_record=canonical_record,
        )

    @staticmethod
    def _normalize_address(raw_address: str) -> str:
        text = raw_address.strip()
        text = _SPACE_RE.sub(" ", text)
        return text

    @staticmethod
    def _apply_spatial_rules(
        *,
        confidence: int,
        reasons: list[str],
        payload: ValidateAddressRequest,
        geo_result: SpatialValidationResult,
    ) -> tuple[int, list[str]]:
        if geo_result.is_within_postcode_boundary is True:
            return confidence, reasons
        if geo_result.is_within_postcode_boundary is False:
            reasons.append("postcode_geometry_mismatch")
            if geo_result.resolved_postcode and geo_result.resolved_postcode != payload.postcode:
                reasons.append(f"resolved_postcode={geo_result.resolved_postcode}")
            confidence -= 30
            return confidence, reasons

        reasons.append("spatial_validation_unavailable")
        confidence -= 5
        return confidence, reasons
