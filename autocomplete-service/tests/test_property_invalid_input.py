"""Property test: Invalid input is consistently rejected.

**Validates: Requirements 6.3, 6.4, 6.5**

Property 7: For any string composed entirely of whitespace (including empty
string) provided as `q`, the service SHALL return HTTP 400. For any string
composed entirely of whitespace provided as `domain`, the service SHALL return
HTTP 400. For any non-empty string not present as a key in DOMAIN_CONFIG
provided as `domain`, the service SHALL return HTTP 400 with the valid domain
names listed in the error detail.
"""

from __future__ import annotations

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from config import DOMAIN_CONFIG

# ---------------------------------------------------------------------------
# Strategies
# ---------------------------------------------------------------------------

# Whitespace-only strings (including empty string): spaces, tabs, newlines
_whitespace_chars = " \t\n\r\x0b\x0c"
whitespace_only_st = st.text(alphabet=_whitespace_chars, min_size=0, max_size=20)

# A valid non-empty query (at least one visible character)
valid_query_st = st.text(
    alphabet=st.characters(whitelist_categories=("L", "N")),
    min_size=1,
    max_size=50,
)

# A valid domain from the config
valid_domain_st = st.sampled_from(list(DOMAIN_CONFIG.keys()))

# Non-domain strings: non-empty strings that are NOT keys in DOMAIN_CONFIG
_valid_domain_keys = set(DOMAIN_CONFIG.keys())
unknown_domain_st = st.text(
    alphabet=st.characters(whitelist_categories=("L", "N"), whitelist_characters="-_"),
    min_size=1,
    max_size=30,
).filter(lambda s: s not in _valid_domain_keys)


# ---------------------------------------------------------------------------
# Property Tests
# ---------------------------------------------------------------------------


@pytest.mark.anyio
class TestInvalidInputRejection:
    """Property 7: Invalid input is consistently rejected."""

    @settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
    @given(q=whitespace_only_st)
    async def test_whitespace_only_q_returns_400(self, q: str, async_client) -> None:
        """Any whitespace-only (or empty) q parameter yields HTTP 400.

        **Validates: Requirements 6.3**
        """
        response = await async_client.get(
            "/autocomplete",
            params={"q": q, "domain": "address", "limit": 10},
        )
        assert response.status_code == 400, (
            f"Expected 400 for whitespace-only q={q!r}, got {response.status_code}"
        )
        body = response.json()
        assert "detail" in body, "Error response must contain 'detail' field"

    @settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
    @given(domain=whitespace_only_st)
    async def test_whitespace_only_domain_returns_400(
        self, domain: str, async_client
    ) -> None:
        """Any whitespace-only (or empty) domain parameter yields HTTP 400.

        **Validates: Requirements 6.4**
        """
        response = await async_client.get(
            "/autocomplete",
            params={"q": "test", "domain": domain, "limit": 10},
        )
        assert response.status_code == 400, (
            f"Expected 400 for whitespace-only domain={domain!r}, got {response.status_code}"
        )
        body = response.json()
        assert "detail" in body, "Error response must contain 'detail' field"

    @settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
    @given(domain=unknown_domain_st)
    async def test_unknown_domain_returns_400_with_valid_domains(
        self, domain: str, async_client
    ) -> None:
        """Any non-empty domain not in DOMAIN_CONFIG yields HTTP 400 with valid domains listed.

        **Validates: Requirements 6.5**
        """
        response = await async_client.get(
            "/autocomplete",
            params={"q": "test", "domain": domain, "limit": 10},
        )
        assert response.status_code == 400, (
            f"Expected 400 for unknown domain={domain!r}, got {response.status_code}"
        )
        body = response.json()
        assert "detail" in body, "Error response must contain 'detail' field"

        # The error detail must list the valid domain names
        detail = body["detail"]
        for valid_domain in DOMAIN_CONFIG.keys():
            assert valid_domain in detail, (
                f"Expected valid domain '{valid_domain}' to be listed in error "
                f"detail: {detail!r}"
            )
