"""Pure NAS domain logic shared by API, ETL, and workers."""

from .exceptions import DomainValidationError
from .models import AddressInput, ConfidenceSignals
from .normalization import extract_postcode, normalize_address_text, normalize_postcode
from .scoring import calculate_confidence, confidence_band
from .validation import validate_address_input

__all__ = [
    "AddressInput",
    "ConfidenceSignals",
    "DomainValidationError",
    "calculate_confidence",
    "confidence_band",
    "extract_postcode",
    "normalize_address_text",
    "normalize_postcode",
    "validate_address_input",
]
