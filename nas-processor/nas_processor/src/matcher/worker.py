"""Matcher polling worker — continuously drains unmatched raw_address rows.

Polls the DB directly; no Valkey stream needed.
"""

from __future__ import annotations

import logging
import time

from nas_processor.src.matcher.config import MatcherConfig
from nas_processor.src.matcher.matcher import AddressMatcher

logger = logging.getLogger(__name__)


class MatcherWorker:
    def __init__(self, config: MatcherConfig) -> None:
        self._cfg = config
        self._matcher = AddressMatcher(config)

    def run_forever(self) -> None:
        logger.info("matcher_worker_started batch_size=%d poll_interval=%.1fs", self._cfg.batch_size, self._cfg.poll_interval_seconds)
        while True:
            try:
                processed = self._matcher.process_batch()
                if processed == 0:
                    time.sleep(self._cfg.poll_interval_seconds)
                else:
                    logger.info("matcher_batch_complete rows=%d", processed)
            except Exception:
                logger.exception("matcher_worker_error")
                time.sleep(self._cfg.poll_interval_seconds)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    from nas_config.db import build_dsn
    config = MatcherConfig.from_env(dsn=build_dsn(driver="psycopg"))
    MatcherWorker(config).run_forever()


if __name__ == "__main__":
    main()
