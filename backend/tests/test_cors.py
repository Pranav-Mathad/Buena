"""CORS origins come only from KEYSTONE_CORS_ORIGINS."""

from __future__ import annotations

import httpx
import pytest

from backend.config import Settings


def _preflight(origin: str) -> httpx.Request:
    return httpx.Request(
        "OPTIONS",
        "http://test/health",
        headers={"Origin": origin, "Access-Control-Request-Method": "GET"},
    )


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("http://a.test, http://b.test ,", ["http://a.test", "http://b.test"]),
        ("", []),
    ],
)
def test_origin_list_parsing(raw: str, expected: list[str]) -> None:
    assert Settings(keystone_cors_origins=raw).cors_origin_list() == expected


@pytest.mark.parametrize(
    ("origin", "allowed"),
    [
        ("http://localhost:8080", True),
        ("https://keystone-insight.lovable.app", False),
        ("https://anything.lovableproject.com", False),
        ("https://evil.example", False),
    ],
)
async def test_only_configured_origins_pass_preflight(origin: str, allowed: bool) -> None:
    from backend.main import app  # noqa: PLC0415

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app)) as client:
        response = await client.send(_preflight(origin))

    assert (response.headers.get("access-control-allow-origin") == origin) is allowed
