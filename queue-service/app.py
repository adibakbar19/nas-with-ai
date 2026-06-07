"""NAS Queue Service — generic background job queue."""
from __future__ import annotations

import logging
import threading
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI

from config import settings
from src.clients.postgres import build_engine
from src.clients.valkey import ValkeyQueueClient
from src.routes.health import router as health_router
from src.routes.jobs import router as jobs_router
from src.routes.queue import router as queue_router
from src.services.queue_service import QueueService

logging.basicConfig(
    level=getattr(logging, settings.log_level.upper(), logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger(__name__)

_CONSUMER_CLEANUP_INTERVAL = 600  # 10 minutes


def _stale_job_recovery_loop(qs: QueueService, interval: int = 60) -> None:
    while True:
        try:
            n = qs.recover_stale_jobs()
            if n > 0:
                logger.info("stale_jobs_recovered count=%d", n)
            m = qs.recover_stuck_queued_jobs(stuck_after_seconds=300)
            if m > 0:
                logger.warning("stuck_queued_jobs_repushed count=%d", m)
        except Exception:
            logger.exception("stale_recovery_error")
        time.sleep(interval)


def _retry_scheduler_loop(qs: QueueService, interval: int = 30) -> None:
    while True:
        try:
            n = qs.process_retry_queue()
            if n > 0:
                logger.info("retry_jobs_pushed count=%d", n)
        except Exception:
            logger.exception("retry_scheduler_error")
        time.sleep(interval)


def _consumer_cleanup_loop(vc: ValkeyQueueClient, interval: int = _CONSUMER_CLEANUP_INTERVAL) -> None:
    while True:
        time.sleep(interval)
        try:
            cleaned = vc.cleanup_dead_consumers()
            if cleaned:
                logger.info("periodic_consumer_cleanup cleaned=%d", cleaned)
        except Exception:
            logger.exception("consumer_cleanup_error")


@asynccontextmanager
async def lifespan(app: FastAPI):
    engine = build_engine(settings.postgres_dsn)
    valkey = ValkeyQueueClient(
        url=settings.valkey_url,
        stream_key=settings.stream_key,
        stream_group=settings.stream_group,
    )

    # Ensure stream group exists and clean dead consumers on startup
    valkey.ensure_stream_group()
    cleaned = valkey.cleanup_dead_consumers()
    if cleaned:
        logger.info("startup_consumer_cleanup cleaned=%d", cleaned)

    qs = QueueService(engine, valkey, settings)

    app.state.queue_service = qs
    app.state.valkey_client = valkey

    # Background threads — daemon so they die with the process
    threading.Thread(target=_stale_job_recovery_loop, args=(qs,),
                     daemon=True, name="stale-recovery").start()
    threading.Thread(target=_retry_scheduler_loop, args=(qs,),
                     daemon=True, name="retry-scheduler").start()
    threading.Thread(target=_consumer_cleanup_loop, args=(valkey,),
                     daemon=True, name="consumer-cleanup").start()

    logger.info(
        "queue_service_started stream=%s group=%s schema=%s",
        settings.stream_key, settings.stream_group, settings.job_schema,
    )
    yield
    logger.info("queue_service_stopping")
    engine.dispose()


app = FastAPI(
    title="NAS Queue Service",
    version="1.0.0",
    lifespan=lifespan,
)

app.include_router(health_router)
app.include_router(jobs_router)
app.include_router(queue_router)
