"""Matcher configuration dataclass — all thresholds and tuning knobs in one place."""

from __future__ import annotations

import os
from dataclasses import dataclass, field


@dataclass
class MatcherConfig:
    auto_match_threshold: float = 0.92
    review_threshold: float = 0.70
    candidate_limit: int = 10
    batch_size: int = 100
    poll_interval_seconds: float = 5.0
    dsn: str = ""
    schema: str = "nas"

    @classmethod
    def from_env(cls, dsn: str) -> "MatcherConfig":
        return cls(
            dsn=dsn,
            schema=os.environ.get("PGSCHEMA", "nas").strip() or "nas",
            auto_match_threshold=float(os.environ.get("MATCHER_AUTO_THRESHOLD", "0.92")),
            review_threshold=float(os.environ.get("MATCHER_REVIEW_THRESHOLD", "0.70")),
            batch_size=int(os.environ.get("MATCHER_BATCH_SIZE", "100")),
            poll_interval_seconds=float(os.environ.get("MATCHER_POLL_INTERVAL", "5")),
        )
