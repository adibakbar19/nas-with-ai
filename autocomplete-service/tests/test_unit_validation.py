"""Unit tests for input validation edge cases.

Validates: Requirements 6.3, 6.4, 6.5, 6.6, 4.7
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest


@pytest.mark.asyncio
class TestEmptyAndWhitespaceQ:
    """Requirement 6.3: Empty/whitespace-only q returns 400."""

    async def test_empty_q(self, async_client):
        resp = await async_client.get("/autocomplete", params={"q": "", "domain": "address"})
        assert resp.status_code == 400
        body = resp.json()
        assert "detail" in body
        assert "q" in body["detail"].lower()

    async def test_whitespace_only_q(self, async_client):
        resp = await async_client.get("/autocomplete", params={"q": "   ", "domain": "address"})
        assert resp.status_code == 400
        body = resp.json()
        assert "detail" in body
        assert "q" in body["detail"].lower()

    async def test_tab_newline_q(self, async_client):
        resp = await async_client.get("/autocomplete", params={"q": "\t\n", "domain": "address"})
        assert resp.status_code == 400
        body = resp.json()
        assert "detail" in body
        assert "q" in body["detail"].lower()


@pytest.mark.asyncio
class TestEmptyDomain:
    """Requirement 6.4: Empty/whitespace-only domain returns 400."""

    async def test_empty_domain(self, async_client):
        resp = await async_client.get("/autocomplete", params={"q": "jalan", "domain": ""})
        assert resp.status_code == 400
        body = resp.json()
        assert "detail" in body
        assert "domain" in body["detail"].lower()

    async def test_whitespace_only_domain(self, async_client):
        resp = await async_client.get("/autocomplete", params={"q": "jalan", "domain": "   "})
        assert resp.status_code == 400
        body = resp.json()
        assert "detail" in body
        assert "domain" in body["detail"].lower()


@pytest.mark.asyncio
class TestUnknownDomain:
    """Requirement 6.5: Unknown domain returns 400 with valid domains listed."""

    async def test_unknown_domain_lists_valid_domains(self, async_client):
        resp = await async_client.get(
            "/autocomplete", params={"q": "jalan", "domain": "nonexistent"}
        )
        assert resp.status_code == 400
        body = resp.json()
        assert "detail" in body
        # The error message should list valid domains
        assert "address" in body["detail"]

    async def test_unknown_domain_typo(self, async_client):
        resp = await async_client.get(
            "/autocomplete", params={"q": "jalan", "domain": "addres"}
        )
        assert resp.status_code == 400
        body = resp.json()
        assert "detail" in body
        assert "address" in body["detail"]


@pytest.mark.asyncio
class TestLimitBoundaryValues:
    """Requirements 6.6, 4.7: Limit boundary validation."""

    async def test_limit_zero_invalid(self, async_client):
        """Limit=0 is below minimum (ge=1), FastAPI returns 422."""
        resp = await async_client.get(
            "/autocomplete", params={"q": "jalan", "domain": "address", "limit": 0}
        )
        # FastAPI's Query(ge=1) returns 422 for out-of-range values
        assert resp.status_code == 422

    async def test_limit_101_invalid(self, async_client):
        """Limit=101 exceeds maximum (le=100), FastAPI returns 422."""
        resp = await async_client.get(
            "/autocomplete", params={"q": "jalan", "domain": "address", "limit": 101}
        )
        # FastAPI's Query(le=100) returns 422 for out-of-range values
        assert resp.status_code == 422

    @patch("app.query_opensearch", new_callable=AsyncMock)
    async def test_limit_1_valid(self, mock_opensearch, async_client):
        """Limit=1 is the minimum valid value."""
        mock_opensearch.return_value = []
        resp = await async_client.get(
            "/autocomplete", params={"q": "jalan", "domain": "address", "limit": 1}
        )
        assert resp.status_code == 200
        body = resp.json()
        assert "suggestions" in body

    @patch("app.query_opensearch", new_callable=AsyncMock)
    async def test_limit_100_valid(self, mock_opensearch, async_client):
        """Limit=100 is the maximum valid value."""
        mock_opensearch.return_value = []
        resp = await async_client.get(
            "/autocomplete", params={"q": "jalan", "domain": "address", "limit": 100}
        )
        assert resp.status_code == 200
        body = resp.json()
        assert "suggestions" in body

    async def test_limit_negative_invalid(self, async_client):
        """Negative limit is below minimum (ge=1), FastAPI returns 422."""
        resp = await async_client.get(
            "/autocomplete", params={"q": "jalan", "domain": "address", "limit": -1}
        )
        assert resp.status_code == 422
