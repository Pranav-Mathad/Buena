"""End-to-end pipeline test: email event in, sourced fact and markdown out.

Flow under test:

- Create a throwaway property whose alias the email mentions.
- Insert an email event without a ``property_id`` so the router has to
  resolve it.
- Process that one event through the worker (route, extract, diff,
  validate, apply, materialize).
- Assert the event is marked processed against the right property, at
  least one fact carries ``source_event_id`` = the event, and the rendered
  markdown links that event id.

The test uses :func:`process_specific` rather than draining the queue, so it
never touches other pending events in the database it runs against. It runs
without API keys: the rule-based extractor handles the heating complaint.
Everything it creates is deleted in ``finally``.
"""

from __future__ import annotations

import uuid

import pytest
from sqlalchemy import text

from backend.db.session import get_sessionmaker
from backend.pipeline.events import insert_event
from backend.pipeline.renderer import render_markdown
from backend.pipeline.worker import process_specific

pytestmark = [pytest.mark.asyncio, pytest.mark.usefixtures("db")]


async def _create_property(alias: str) -> tuple[uuid.UUID, uuid.UUID]:
    factory = get_sessionmaker()
    async with factory() as session:
        building_id = (
            await session.execute(
                text("INSERT INTO buildings (address) VALUES (:a) RETURNING id"),
                {"a": f"{alias} building"},
            )
        ).scalar_one()
        property_id = (
            await session.execute(
                text(
                    """
                    INSERT INTO properties (name, address, building_id, aliases)
                    VALUES (:name, :addr, :bid, ARRAY[:alias])
                    RETURNING id
                    """
                ),
                {"name": alias, "addr": f"{alias}, 10115 Berlin", "bid": building_id, "alias": alias},
            )
        ).scalar_one()
        await session.commit()
    return building_id, property_id


async def _delete_created_rows(
    building_id: uuid.UUID, property_id: uuid.UUID, source_ref: str
) -> None:
    factory = get_sessionmaker()
    async with factory() as session:
        await session.execute(
            text("DELETE FROM facts WHERE property_id = :pid"), {"pid": property_id}
        )
        await session.execute(
            text("DELETE FROM events WHERE source = 'email' AND source_ref = :ref"),
            {"ref": source_ref},
        )
        await session.execute(text("DELETE FROM properties WHERE id = :pid"), {"pid": property_id})
        await session.execute(text("DELETE FROM buildings WHERE id = :bid"), {"bid": building_id})
        await session.commit()


async def test_happy_path_email_creates_sourced_fact() -> None:
    """Event insert → worker → sourced fact → markdown render."""
    suffix = uuid.uuid4().hex[:6]
    alias = f"Testweg {suffix} Apt 9Q"
    source_ref = f"test-happy-path-{suffix}"
    building_id, property_id = await _create_property(alias)
    try:
        raw_content = (
            "From: tenant@example.com\n"
            f"Subject: Heating completely out in {alias} again\n\n"
            "The radiators are cold this morning and the boiler is rattling "
            "louder than ever. Fourth time this winter."
        )

        factory = get_sessionmaker()
        async with factory() as session:
            event_id, inserted = await insert_event(
                session, source="email", source_ref=source_ref, raw_content=raw_content
            )
            await session.commit()
        assert inserted is True

        assert await process_specific(event_id) is True

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
            assert event_row.processing_error is None
            assert event_row.processed_at is not None
            assert uuid.UUID(str(event_row.property_id)) == property_id

            facts = (
                await session.execute(
                    text("SELECT section, field FROM facts WHERE source_event_id = :eid"),
                    {"eid": event_id},
                )
            ).all()
            assert facts, "expected at least one fact written for the event"

            markdown = await render_markdown(session, property_id)

        assert str(event_id) in markdown, "rendered markdown is missing the source event id"
    finally:
        await _delete_created_rows(building_id, property_id, source_ref)
