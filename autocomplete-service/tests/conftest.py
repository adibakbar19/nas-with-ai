"""Shared test configuration and fixtures for the autocomplete-service test suite."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest
from hypothesis import HealthCheck, settings as hypothesis_settings

# ---------------------------------------------------------------------------
# Hypothesis profiles
# ---------------------------------------------------------------------------

hypothesis_settings.register_profile(
    "dev",
    max_examples=100,
    suppress_health_check=[HealthCheck.too_slow],
)
hypothesis_settings.register_profile(
    "ci",
    max_examples=200,
    suppress_health_check=[HealthCheck.too_slow],
)
hypothesis_settings.load_profile("dev")

# ---------------------------------------------------------------------------
# Path setup — ensure the service package is importable from tests
# ---------------------------------------------------------------------------

SERVICE_ROOT = Path(__file__).resolve().parent.parent
if str(SERVICE_ROOT) not in sys.path:
    sys.path.insert(0, str(SERVICE_ROOT))

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def anyio_backend():
    """Force pytest-asyncio to use asyncio backend."""
    return "asyncio"


@pytest.fixture()
async def async_client():
    """Provide an httpx AsyncClient wired to the FastAPI test app."""
    from unittest.mock import AsyncMock

    from httpx import ASGITransport, AsyncClient

    from app import app

    # Provide a mock Redis so the lifespan doesn't require a real connection
    mock_redis = AsyncMock()
    mock_redis.get = AsyncMock(return_value=None)
    mock_redis.setex = AsyncMock(return_value=True)
    mock_redis.aclose = AsyncMock()
    app.state.redis = mock_redis

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client
