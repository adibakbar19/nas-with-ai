"""NAS Event Streaming Service."""
from __future__ import annotations

import logging
import threading
from contextlib import asynccontextmanager

import sqlalchemy as sa
from fastapi import FastAPI

from config import settings
from src.routes.health import router as health_router
from src.routes.webhooks import router as webhooks_router
from src.routes.events import router as events_router
from src.routes.deliveries import router as deliveries_router
from src.subscriber import EventSubscriber
from src.webhook import WebhookDeliverer

logging.basicConfig(
    level=getattr(logging, settings.log_level.upper(), logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    engine = sa.create_engine(settings.postgres_dsn, pool_pre_ping=True)

    subscriber = EventSubscriber(settings, engine)
    deliverer = WebhookDeliverer(settings, engine)

    t1 = threading.Thread(target=subscriber.run_forever, daemon=True, name="subscriber")
    t2 = threading.Thread(target=deliverer.run_forever, daemon=True, name="deliverer")
    t1.start()
    t2.start()

    logger.info(
        "event_streaming_service_started subscriber=running deliverer=running "
        "brokers=%s topics=%s",
        settings.kafka_brokers, settings.kafka_topics,
    )
    yield
    logger.info("event_streaming_service_stopping")


app = FastAPI(
    title="NAS Event Streaming Service",
    version="1.0.0",
    lifespan=lifespan,
)

app.include_router(health_router)
app.include_router(webhooks_router)
app.include_router(events_router)
app.include_router(deliveries_router)
