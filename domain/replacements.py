"""Shared text replacement rules for domain + ETL normalization."""

ADDRESS_ABBREVIATION_REPLACEMENTS: tuple[tuple[str, str], ...] = (
    ("JLN", "JALAN"),
    ("JAL", "JALAN"),
    ("LRG", "LORONG"),
    ("PSRN", "PERSIARAN"),
    ("KG", "KAMPUNG"),
    ("KPG", "KAMPUNG"),
    ("KAMP", "KAMPUNG"),
    ("TMN", "TAMAN"),
    ("LBH", "LEBUH"),
    ("NO", "NO"),
)

LOCALITY_ABBREVIATION_REPLACEMENTS: tuple[tuple[str, str], ...] = (
    ("TMN", "TAMAN"),
    ("KG", "KAMPUNG"),
    ("KPG", "KAMPUNG"),
    ("KAMP", "KAMPUNG"),
    ("JLN", "JALAN"),
    ("LRG", "LORONG"),
    ("LBH", "LEBUH"),
)

LOCALITY_PLACEHOLDER_VALUES: tuple[str, ...] = (
    "NA",
    "N A",
    "NIL",
    "NONE",
    "NULL",
    "TIADA",
    "TIDAK ADA",
)

LOCALITY_INVALID_PREFIXES: tuple[str, ...] = (
    "JALAN",
    "JLN",
    "LORONG",
    "LRG",
    "LEBUH",
)

LOCALITY_INVALID_PREFIX_REGEX = r"^(JALAN|JLN|LORONG|LRG|LEBUH)\b"
