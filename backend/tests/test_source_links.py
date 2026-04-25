"""Tests for Phase 10 Step 10.4 — ``/events/<id>/{source,raw,detail}``.

These exercise the source-link dispatcher: an invoice/letter event
redirects to ``/files/<original_path>``, an email event to ``/raw``,
and a bank event to ``/detail``. Each case is wired via a real DB row
(skipped if dev DB is unreachable).
"""

from __future__ import annotations

import json
from typing import Any
from uuid import UUID, uuid4

import httpx
import pytest
from sqlalchemy import text

from backend.config import get_settings
from backend.db.session import get_sessionmaker
from connectors.migrations import apply_all as ensure_migrations

pytestmark = pytest.mark.asyncio


def _reset_session_cache() -> None:
    from backend.db import session as session_module  # noqa: PLC0415

    session_module.get_engine.cache_clear()
    session_module.get_sessionmaker.cache_clear()


async def _db_reachable() -> bool:
    _reset_session_cache()
    try:
        factory = get_sessionmaker()
        async with factory() as conn:
            await conn.execute(text("SELECT 1"))
        return True
    except Exception:  # noqa: BLE001
        return False


async def _setup_or_skip() -> None:
    if not await _db_reachable():
        pytest.skip(f"dev DB unreachable at {get_settings().database_url}")
    ensure_migrations()


async def _seed_event(
    *,
    source: str,
    metadata: dict[str, Any] | None = None,
    raw_content: str = "raw body",
) -> UUID:
    factory = get_sessionmaker()
    async with factory() as session:
        ref = f"sl-{uuid4().hex[:8]}"
        row = (
            await session.execute(
                text(
                    """
                    INSERT INTO events (source, source_ref, raw_content, metadata)
                    VALUES (:s, :r, :body, CAST(:meta AS JSONB))
                    RETURNING id
                    """
                ),
                {
                    "s": source,
                    "r": ref,
                    "body": raw_content,
                    "meta": json.dumps(metadata or {}),
                },
            )
        ).first()
        await session.commit()
    return UUID(str(row.id))


async def _cleanup_event(event_id: UUID) -> None:
    factory = get_sessionmaker()
    async with factory() as session:
        await session.execute(
            text("DELETE FROM events WHERE id = :eid"), {"eid": event_id}
        )
        await session.commit()


def _async_client() -> httpx.AsyncClient:
    """Build an httpx ASGI client bound to the FastAPI app.

    TestClient/sync httpx run in their own private loop, which collides
    with the asyncpg session pool the rest of the test holds. Routing
    through ``ASGITransport`` keeps everything on one loop.
    """
    from backend.main import app  # noqa: PLC0415

    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://testserver"
    )


# ---------------------------------------------------------------------------
# /events/<id>/source dispatch
# ---------------------------------------------------------------------------


async def test_invoice_source_redirects_to_files() -> None:
    """An invoice event with original_path → 307 to /files/<path>."""
    await _setup_or_skip()
    eid = await _seed_event(
        source="invoice",
        metadata={"original_path": "invoices/2024/HG-INV-001.pdf"},
    )
    try:
        async with _async_client() as client:
            resp = await client.get(f"/events/{eid}/source", follow_redirects=False)
        assert resp.status_code == 307
        assert resp.headers["location"] == "/files/invoices/2024/HG-INV-001.pdf"
    finally:
        await _cleanup_event(eid)


async def test_letter_source_redirects_to_files() -> None:
    """A letter event with original_path → 307 to /files/<path>."""
    await _setup_or_skip()
    eid = await _seed_event(
        source="letter",
        metadata={"original_path": "letters/2025/mahnung_001.pdf"},
    )
    try:
        async with _async_client() as client:
            resp = await client.get(f"/events/{eid}/source", follow_redirects=False)
        assert resp.status_code == 307
        assert resp.headers["location"] == "/files/letters/2025/mahnung_001.pdf"
    finally:
        await _cleanup_event(eid)


async def test_email_source_redirects_to_raw() -> None:
    """An email event → 307 to /events/<id>/raw."""
    await _setup_or_skip()
    eid = await _seed_event(source="email", raw_content="Subject: hi\n\nBody")
    try:
        async with _async_client() as client:
            resp = await client.get(f"/events/{eid}/source", follow_redirects=False)
        assert resp.status_code == 307
        assert resp.headers["location"] == f"/events/{eid}/raw"
    finally:
        await _cleanup_event(eid)


async def test_bank_source_redirects_to_detail() -> None:
    """A bank event → 307 to /events/<id>/detail (JSON envelope)."""
    await _setup_or_skip()
    eid = await _seed_event(
        source="bank",
        metadata={"betrag": "1100.00", "kategorie": "miete"},
    )
    try:
        async with _async_client() as client:
            resp = await client.get(f"/events/{eid}/source", follow_redirects=False)
        assert resp.status_code == 307
        assert resp.headers["location"] == f"/events/{eid}/detail"
    finally:
        await _cleanup_event(eid)


async def test_invoice_without_original_path_falls_through_to_raw() -> None:
    """If a PDF event lost its original_path, /source still redirects somewhere."""
    await _setup_or_skip()
    eid = await _seed_event(source="invoice", metadata={})
    try:
        async with _async_client() as client:
            resp = await client.get(f"/events/{eid}/source", follow_redirects=False)
        assert resp.status_code == 307
        # Falls through to raw rather than emitting a dead /files/None URL.
        assert resp.headers["location"] == f"/events/{eid}/raw"
    finally:
        await _cleanup_event(eid)


async def test_source_404_for_missing_event() -> None:
    """An unknown event_id returns 404, not a redirect."""
    await _setup_or_skip()
    async with _async_client() as client:
        resp = await client.get(f"/events/{uuid4()}/source", follow_redirects=False)
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# /events/<id>/raw + /detail endpoints
# ---------------------------------------------------------------------------


async def test_raw_returns_text_plain() -> None:
    """The /raw endpoint returns the raw_content as text/plain."""
    await _setup_or_skip()
    body = "From: a@b\nSubject: test\n\nThis is the body"
    eid = await _seed_event(source="email", raw_content=body)
    try:
        async with _async_client() as client:
            resp = await client.get(f"/events/{eid}/raw")
        assert resp.status_code == 200
        assert resp.headers["content-type"].startswith("text/plain")
        assert resp.text == body
    finally:
        await _cleanup_event(eid)


async def test_detail_returns_json_envelope() -> None:
    """The /detail endpoint returns id + source + metadata + snippet."""
    await _setup_or_skip()
    eid = await _seed_event(
        source="bank",
        metadata={"betrag": "1100.00", "kategorie": "miete"},
        raw_content="bank line item snippet",
    )
    try:
        async with _async_client() as client:
            resp = await client.get(f"/events/{eid}/detail")
        assert resp.status_code == 200
        data = resp.json()
        assert data["event_id"] == str(eid)
        assert data["source"] == "bank"
        assert data["metadata"] == {"betrag": "1100.00", "kategorie": "miete"}
        assert "bank line" in data["snippet"]
    finally:
        await _cleanup_event(eid)


async def test_raw_404_for_missing_event() -> None:
    """An unknown event id returns 404 from /raw."""
    await _setup_or_skip()
    async with _async_client() as client:
        resp = await client.get(f"/events/{uuid4()}/raw")
    assert resp.status_code == 404
