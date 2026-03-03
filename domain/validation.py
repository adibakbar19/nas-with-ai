from domain.models import AddressInput
from domain.normalization import normalize_postcode


def validate_address_input(payload: AddressInput) -> list[str]:
    reasons: list[str] = []

    raw = (payload.raw_address or "").strip()
    if len(raw) < 3:
        reasons.append("address_too_short")

    if payload.postcode is not None and normalize_postcode(payload.postcode) is None:
        reasons.append("invalid_postcode")

    has_lat = payload.latitude is not None
    has_lon = payload.longitude is not None
    if has_lat != has_lon:
        reasons.append("partial_coordinates")

    if payload.latitude is not None and not (-90.0 <= payload.latitude <= 90.0):
        reasons.append("latitude_out_of_range")
    if payload.longitude is not None and not (-180.0 <= payload.longitude <= 180.0):
        reasons.append("longitude_out_of_range")

    return reasons
