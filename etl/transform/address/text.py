import re

_WHITESPACE_RE = re.compile(r"\s+")
_POSTCODE_RE = re.compile(r"(?<!\d)(\d{5})(?!\d)")


def normalize_address_text(value: str | None, *, uppercase: bool = True) -> str:
    if value is None:
        return ""
    text = value.strip()
    if not text:
        return ""
    if uppercase:
        text = text.upper()
    text = _WHITESPACE_RE.sub(" ", text)
    text = re.sub(r"\s*,\s*", ", ", text)
    text = re.sub(r"[;|]+", ", ", text)
    text = _WHITESPACE_RE.sub(" ", text).strip()
    return text


def normalize_postcode(value: str | None) -> str | None:
    if value is None:
        return None
    cleaned = "".join(ch for ch in value if ch.isdigit())
    if len(cleaned) < 5:
        return None
    return cleaned[:5]


def extract_postcode(value: str | None) -> str | None:
    if not value:
        return None
    match = _POSTCODE_RE.search(value)
    if not match:
        return None
    return match.group(1)
