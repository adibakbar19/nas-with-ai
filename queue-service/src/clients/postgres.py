"""SQLAlchemy engine factory for queue-service."""
from __future__ import annotations

import sqlalchemy as sa


def build_engine(dsn: str) -> sa.Engine:
    return sa.create_engine(dsn, pool_pre_ping=True, pool_size=5, max_overflow=10)
