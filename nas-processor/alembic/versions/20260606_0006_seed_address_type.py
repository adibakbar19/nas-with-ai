"""Seed nas.address_type with DMS 2039 reference data.

Drops ownership_structure from address_type (it belongs on standardized_address),
adds a UNIQUE constraint on (address_type, address_subtype), then seeds all
reference rows. Safe to re-run — uses ON CONFLICT DO NOTHING.

Revision ID: 20260606_0006
Revises: 20260605_0005
Create Date: 2026-06-06 00:00:00

"""
from __future__ import annotations

import re

from alembic import op


revision = "20260606_0006"
down_revision = "20260605_0005"
branch_labels = None
depends_on = None

_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _quote_ident(name: str) -> str:
    if not _IDENT_RE.fullmatch(name):
        raise ValueError(f"Invalid identifier: {name!r}")
    return f'"{name}"'


def _schema() -> str:
    schema = op.get_context().config.get_main_option("nas_schema") or "nas"
    if not _IDENT_RE.fullmatch(schema):
        raise ValueError(f"Invalid schema name: {schema!r}")
    return schema


# (address_type, address_subtype, property_code)
_SEED_ROWS: list[tuple[str, str, str]] = [
    # RESIDENTIAL
    ("residential", "landed",        "RES-LND"),
    ("residential", "apartment",     "RES-APT"),
    ("residential", "condominium",   "RES-CON"),
    ("residential", "flat",          "RES-FLT"),
    ("residential", "townhouse",     "RES-TWN"),
    ("residential", "semi_detached", "RES-SEM"),
    ("residential", "bungalow",      "RES-BUN"),
    ("residential", "quarters",      "RES-QTR"),
    ("residential", "rumah_pangsa",  "RES-PAN"),
    # COMMERCIAL
    ("commercial", "shop",         "COM-SHP"),
    ("commercial", "office",       "COM-OFF"),
    ("commercial", "mall",         "COM-MAL"),
    ("commercial", "hotel",        "COM-HTL"),
    ("commercial", "restaurant",   "COM-RST"),
    ("commercial", "soho",         "COM-SOH"),
    ("commercial", "serviced_apt", "COM-SVC"),
    # INDUSTRIAL
    ("industrial", "factory",   "IND-FCT"),
    ("industrial", "warehouse", "IND-WRH"),
    ("industrial", "port",      "IND-PRT"),
    ("industrial", "logistics", "IND-LOG"),
    # GOVERNMENT
    ("government", "federal",    "GOV-FED"),
    ("government", "state",      "GOV-STA"),
    ("government", "pbt",        "GOV-PBT"),
    ("government", "military",   "GOV-MIL"),
    ("government", "police",     "GOV-POL"),
    ("government", "hospital",   "GOV-HOS"),
    ("government", "school",     "GOV-SCH"),
    ("government", "university", "GOV-UNI"),
    ("government", "court",      "GOV-CRT"),
    ("government", "prison",     "GOV-PRN"),
    # POI
    ("poi", "mosque",            "POI-MSQ"),
    ("poi", "temple",            "POI-TPL"),
    ("poi", "church",            "POI-CHR"),
    ("poi", "surau",             "POI-SRU"),
    ("poi", "petrol_station",    "POI-PTR"),
    ("poi", "toll",              "POI-TOL"),
    ("poi", "highway_rest_area", "POI-HRA"),
    ("poi", "cemetery",          "POI-CMT"),
    ("poi", "park",              "POI-PRK"),
    ("poi", "stadium",           "POI-STD"),
    ("poi", "airport",           "POI-APT"),
    ("poi", "train_station",     "POI-TRN"),
    ("poi", "bus_terminal",      "POI-BUS"),
    ("poi", "jetty",             "POI-JTY"),
    ("poi", "clinic",            "POI-CLN"),
    ("poi", "market",            "POI-MKT"),
    # RURAL (DMS 2039 clause 6 non-street addresses)
    ("rural", "kampung",          "RUR-KPG"),
    ("rural", "felda",            "RUR-FLD"),
    ("rural", "felcra",           "RUR-FCR"),
    ("rural", "rumah_panjang",    "RUR-RPJ"),
    ("rural", "kampung_atas_air", "RUR-KAA"),
    ("rural", "estate",           "RUR-EST"),
    ("rural", "tanah_rancangan",  "RUR-TRN"),
    ("rural", "orang_asli",       "RUR-OAS"),
    # PO_BOX (DMS 2039 clause 7)
    ("po_box", "po_box",     "POB-POB"),
    ("po_box", "locked_bag", "POB-LCK"),
    ("po_box", "wdt",        "POB-WDT"),
]


def upgrade() -> None:
    schema = _schema()
    tbl = f"{_quote_ident(schema)}.\"address_type\""

    # ownership_structure belongs on standardized_address, not here
    op.execute(f"ALTER TABLE {tbl} DROP COLUMN IF EXISTS ownership_structure")

    # UNIQUE constraint needed for ON CONFLICT idempotency
    # ADD CONSTRAINT IF NOT EXISTS is not supported — use a DO block instead
    op.execute(f"""
        DO $$
        BEGIN
            ALTER TABLE {tbl}
                ADD CONSTRAINT address_type_subtype_unique
                UNIQUE (address_type, address_subtype);
        EXCEPTION WHEN duplicate_table THEN NULL;
        END $$
    """)

    # Build a single bulk INSERT for all seed rows
    values = ",\n        ".join(
        f"('{at}', '{ast}', '{pc}')"
        for at, ast, pc in _SEED_ROWS
    )
    op.execute(f"""
        INSERT INTO {tbl} (address_type, address_subtype, property_code)
        VALUES
        {values}
        ON CONFLICT (address_type, address_subtype) DO NOTHING
    """)


def downgrade() -> None:
    schema = _schema()
    tbl = f"{_quote_ident(schema)}.\"address_type\""

    op.execute(f"DELETE FROM {tbl}")
    op.execute(f"ALTER TABLE {tbl} DROP CONSTRAINT IF EXISTS address_type_subtype_unique")
    op.execute(f"ALTER TABLE {tbl} ADD COLUMN IF NOT EXISTS ownership_structure TEXT")
