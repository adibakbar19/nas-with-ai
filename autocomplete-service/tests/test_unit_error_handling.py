"""Unit tests for error handling and graceful degradation.

Validates Requirements 7.1, 7.2, 7.3, 7.4:
- Redis unreachable on read → proceeds to OpenSearch (transparent 200)
- Redis unreachable on write → returns results without caching (transparent 200)
- OpenSearch timeout → returns 503
- OpenSearch non-2xx → returns 502 with upstream status
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import httpx
import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from app import app


@pytest_asyncio.fixture()
async def client_with_redis_read_failure():
    """Client where Redis raises on GET (read failure)."""
    mock_redis = AsyncMock()
    mock_redis.get = AsyncMock(side_effect=ConnectionError("Redis unreachable"))
    mock_redis.setex = AsyncMock(return_value=True)
    mock_redis.aclose = AsyncMock()
    app.state.redis = mock_redis

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


@pytest_asyncio.fixture()
async def client_with_redis_write_failure():
    """Client where Redis raises on SETEX (write failure)."""
    mock_redis = AsyncMock()
    mock_redis.get = AsyncMock(return_value=None)  # cache miss
    mock_redis.setex = AsyncMock(side_effect=ConnectionError("Redis unreachable"))
    mock_redis.aclose = AsyncMock()
    app.state.redis = mock_redis

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


@pytest_asyncio.fixture()
async def client_for_opensearch_errors():
    """Client with working Redis (cache miss) for testing OpenSearch errors."""
    mock_redis = AsyncMock()
    mock_redis.get = AsyncMock(return_value=None)
    mock_redis.setex = AsyncMock(return_value=True)
    mock_redis.aclose = AsyncMock()
    app.state.redis = mock_redis

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


# ---------------------------------------------------------------------------
# Requirement 7.1: Redis unreachable on read → proceeds to OpenSearch
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_redis_read_failure_proceeds_to_opensearch(client_with_redis_read_failure):
    """When Redis is unreachable on read, the service bypasses cache and queries OpenSearch."""
    mock_suggestions = ["Jalan Ampang, Kuala Lumpur"]

    with patch("app.query_opensearch", new_callable=AsyncMock) as mock_os:
        mock_os.return_value = mock_suggestions

        resp = await client_with_redis_read_failure.get(
            "/autocomplete", params={"q": "jalan", "domain": "address", "limit": 10}
        )

    assert resp.status_code == 200
    data = resp.json()
    assert data["suggestions"] == mock_suggestions
    mock_os.assert_called_once()


# ---------------------------------------------------------------------------
# Requirement 7.2: Redis unreachable on write → returns results without caching
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_redis_write_failure_returns_results(client_with_redis_write_failure):
    """When Redis is unreachable on write, results are still returned (200)."""
    mock_suggestions = ["Jalan Bukit Bintang, Kuala Lumpur", "Jalan Sultan Ismail"]

    with patch("app.query_opensearch", new_callable=AsyncMock) as mock_os:
        mock_os.return_value = mock_suggestions

        resp = await client_with_redis_write_failure.get(
            "/autocomplete", params={"q": "jalan", "domain": "address", "limit": 10}
        )

    assert resp.status_code == 200
    data = resp.json()
    assert data["suggestions"] == mock_suggestions


# ---------------------------------------------------------------------------
# Requirement 7.3: OpenSearch timeout → returns 503
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_opensearch_timeout_returns_503(client_for_opensearch_errors):
    """When OpenSearch is unreachable (ConnectError), service returns 503."""
    with patch("app.query_opensearch", new_callable=AsyncMock) as mock_os:
        mock_os.side_effect = httpx.ConnectError("Connection timed out")

        resp = await client_for_opensearch_errors.get(
            "/autocomplete", params={"q": "jalan", "domain": "address", "limit": 10}
        )

    assert resp.status_code == 503
    data = resp.json()
    assert "detail" in data
    assert "unreachable" in data["detail"].lower() or "OpenSearch" in data["detail"]


# ---------------------------------------------------------------------------
# Requirement 7.4: OpenSearch non-2xx → returns 502 with upstream status
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_opensearch_non_2xx_returns_502(client_for_opensearch_errors):
    """When OpenSearch returns a non-2xx response, service returns 502 with upstream status."""
    # Create a mock response that simulates a 500 from OpenSearch
    mock_response = httpx.Response(
        status_code=500,
        text="Internal Server Error",
        request=httpx.Request("POST", "http://opensearch:9200/nas_addresses/_search"),
    )
    error = httpx.HTTPStatusError(
        "Server Error", request=mock_response.request, response=mock_response
    )

    with patch("app.query_opensearch", new_callable=AsyncMock) as mock_os:
        mock_os.side_effect = error

        resp = await client_for_opensearch_errors.get(
            "/autocomplete", params={"q": "jalan", "domain": "address", "limit": 10}
        )

    assert resp.status_code == 502
    data = resp.json()
    assert "detail" in data
    assert "500" in data["detail"]


@pytest.mark.asyncio
async def test_opensearch_404_returns_502_with_status(client_for_opensearch_errors):
    """When OpenSearch returns 404, service returns 502 with the upstream 404 status."""
    mock_response = httpx.Response(
        status_code=404,
        text="index_not_found_exception",
        request=httpx.Request("POST", "http://opensearch:9200/nas_addresses/_search"),
    )
    error = httpx.HTTPStatusError(
        "Not Found", request=mock_response.request, response=mock_response
    )

    with patch("app.query_opensearch", new_callable=AsyncMock) as mock_os:
        mock_os.side_effect = error

        resp = await client_for_opensearch_errors.get(
            "/autocomplete", params={"q": "test", "domain": "address", "limit": 5}
        )

    assert resp.status_code == 502
    data = resp.json()
    assert "detail" in data
    assert "404" in data["detail"]
