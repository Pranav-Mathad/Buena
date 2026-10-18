"""FEATURED_PROPERTY resolves by alias or UUID and drives the is_hero flag."""

from __future__ import annotations

import uuid
from collections.abc import AsyncIterator

import httpx
import pytest
from sqlalchemy import text

from backend.config import get_settings
from backend.db.session import get_sessionmaker


def _client() -> httpx.AsyncClient:
    from backend.main import app  # noqa: PLC0415

    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test")


@pytest.fixture
async def unit(db: None) -> AsyncIterator[tuple[uuid.UUID, str]]:
    alias = f"EH-T{uuid.uuid4().hex[:6]}"
    async with get_sessionmaker()() as session:
        pid = (
            await session.execute(
                text(
                    "INSERT INTO properties (name, address, aliases) "
                    "VALUES (:n, 'Featured test', ARRAY[:a]) RETURNING id"
                ),
                {"n": f"Featured {alias}", "a": alias},
            )
        ).scalar_one()
        await session.commit()
    yield pid, alias
    async with get_sessionmaker()() as session:
        await session.execute(text("DELETE FROM properties WHERE id = :p"), {"p": pid})
        await session.commit()


async def _heroes(monkeypatch: pytest.MonkeyPatch, ref: str) -> list[str]:
    monkeypatch.setenv("FEATURED_PROPERTY", ref)
    get_settings.cache_clear()
    async with _client() as client:
        response = await client.get("/properties")
    assert response.status_code == 200
    return [p["id"] for p in response.json() if p["is_hero"]]


async def test_featured_property_by_alias(
    unit: tuple[uuid.UUID, str], monkeypatch: pytest.MonkeyPatch
) -> None:
    pid, alias = unit

    assert await _heroes(monkeypatch, alias) == [str(pid)]


async def test_featured_property_by_uuid(
    unit: tuple[uuid.UUID, str], monkeypatch: pytest.MonkeyPatch
) -> None:
    pid, _ = unit

    assert await _heroes(monkeypatch, str(pid)) == [str(pid)]


@pytest.mark.usefixtures("db")
async def test_unmatched_reference_features_nothing(monkeypatch: pytest.MonkeyPatch) -> None:
    assert await _heroes(monkeypatch, "EH-does-not-exist") == []

    async with _client() as client:
        response = await client.post("/admin/replay/featured")

    assert response.status_code == 404
    assert "EH-does-not-exist" in response.json()["detail"]
