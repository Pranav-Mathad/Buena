"""Mandatory integration test — protects the demo spine.

Flow under test:
- Insert an email event mentioning Apt 4B.
- Run the worker once.
- Assert the event now has ``processed_at``, a fact row was written with
  ``source_event_id`` pointing at that event, and the rendered markdown
  contains the new fact alongside an ``[source: <event_id>]`` link.

The test talks to the real dev Postgres on :5433 (same one the seeder uses).
It is auto-skipped when the DB is unreachable so CI without Docker doesn't
fail spuriously.
"""

from __future__ import annotations

import uuid

import pytest
from sqlalchemy import text

from backend.config import get_settings
from backend.db.session import get_sessionmaker
from backend.pipeline.events import insert_event
from backend.pipeline.renderer import render_markdown
from backend.pipeline.worker import process_batch

pytestmark = pytest.mark.asyncio


async def _db_reachable() -> bool:
    """Return True if we can SELECT 1 from the configured database."""
    try:
        factory = get_sessionmaker()
        async with factory() as session:
            await session.execute(text("SELECT 1"))
        return True
    except Exception:  # noqa: BLE001 — any failure = skip
        return False


async def _pick_berlin_4b_id() -> uuid.UUID | None:
    """Look up the seeded Berliner 4B property id, if present."""
    factory = get_sessionmaker()
    async with factory() as session:
        row = (
            await session.execute(
                text(
                    "SELECT id FROM properties WHERE name = :name"
                ),
                {"name": "Berliner Strasse 12, Apt 4B"},
            )
        ).first()
    return uuid.UUID(str(row.id)) if row else None


async def test_happy_path_email_creates_sourced_fact() -> None:
    """End-to-end: event insert → worker → sourced fact → markdown render."""
    if not await _db_reachable():
        pytest.skip(f"dev DB unreachable at {get_settings().database_url}")

    property_id = await _pick_berlin_4b_id()
    if property_id is None:
        pytest.skip("seed dataset not loaded — run `python -m seed.seed` first")

    source_ref = f"test-{uuid.uuid4()}"
    raw_content = (
        "From: lukas.weber@tenant.demo\n"
        "Subject: Heating completely out in Apt 4B again\n\n"
        "The radiators are cold this morning and the boiler is rattling "
        "louder than ever. Fourth time this winter."
    )

    factory = get_sessionmaker()
    async with factory() as session:
        event_id, inserted = await insert_event(
            session,
            source="email",
            source_ref=source_ref,
            raw_content=raw_content,
        )
        await session.commit()
    assert inserted is True

    processed = await process_batch(max_events=10)
    assert processed >= 1

    async with factory() as session:
        event_row = (
            await session.execute(
                text(
                    "SELECT processed_at, property_id, processing_error "
                    "FROM events WHERE id = :id"
                ),
                {"id": event_id},
            )
        ).one()
        assert event_row.processed_at is not None
        assert event_row.processing_error is None
        assert uuid.UUID(str(event_row.property_id)) == property_id

        facts = (
            await session.execute(
                text(
                    """
                    SELECT id, section, field, value, confidence
                    FROM facts
                    WHERE source_event_id = :eid
                    ORDER BY section, field
                    """
                ),
                {"eid": event_id},
            )
        ).all()
        assert facts, "expected at least one fact written for the event"

        markdown = await render_markdown(session, property_id)

    # Markdown must reference the event id inline for the sourcing UI.
    assert str(event_id) in markdown, "rendered markdown is missing the source event id"
