"""POST /debug/trigger_event accepts raw_content or from/subject/body."""

from __future__ import annotations

from collections.abc import AsyncIterator
from uuid import uuid4

import httpx
import pytest
from sqlalchemy import text

from backend.db.session import get_sessionmaker

_REF_PREFIX = "test-trigger-"


def _client() -> httpx.AsyncClient:
    from backend.main import app  # noqa: PLC0415

    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test")


@pytest.fixture
async def source_ref(db: None) -> AsyncIterator[str]:
    ref = f"{_REF_PREFIX}{uuid4().hex[:8]}"
    yield ref
    async with get_sessionmaker()() as session:
        ids = "SELECT id FROM events WHERE source_ref = :ref"
        await session.execute(
            text(f"DELETE FROM facts WHERE source_event_id IN ({ids})"), {"ref": ref}
        )
        await session.execute(text("DELETE FROM events WHERE source_ref = :ref"), {"ref": ref})
        await session.commit()


async def _stored_raw_content(ref: str) -> str:
    async with get_sessionmaker()() as session:
        return str(
            (
                await session.execute(
                    text("SELECT raw_content FROM events WHERE source_ref = :ref"), {"ref": ref}
                )
            ).scalar_one()
        )


async def test_raw_content_is_stored_verbatim(source_ref: str) -> None:
    raw = "From: tenant@example.com\nSubject: Leak\n\nWater is dripping from the ceiling."
    async with _client() as client:
        response = await client.post(
            "/debug/trigger_event", json={"source_ref": source_ref, "raw_content": raw}
        )

    assert response.status_code == 200, response.text
    assert response.json()["inserted"] is True
    assert await _stored_raw_content(source_ref) == raw


async def test_subject_and_body_are_composed_like_imap(source_ref: str) -> None:
    async with _client() as client:
        response = await client.post(
            "/debug/trigger_event",
            json={
                "source_ref": source_ref,
                "from": "tenant@example.com",
                "subject": "Leak",
                "body": "Water is dripping from the ceiling.",
            },
        )

    assert response.status_code == 200, response.text
    assert await _stored_raw_content(source_ref) == (
        "From: tenant@example.com\nSubject: Leak\n\nWater is dripping from the ceiling.\n"
    )


@pytest.mark.parametrize(
    "payload",
    [
        {},
        {"raw_content": "a", "body": "b"},
        {"raw_content": "a", "subject": "s"},
    ],
    ids=["neither", "both", "subject-with-raw-content"],
)
async def test_ambiguous_payloads_are_rejected(payload: dict[str, str]) -> None:
    async with _client() as client:
        response = await client.post("/debug/trigger_event", json=payload)

    assert response.status_code == 422


async def test_blank_content_is_rejected() -> None:
    async with _client() as client:
        response = await client.post("/debug/trigger_event", json={"raw_content": "   "})

    assert response.status_code == 400
