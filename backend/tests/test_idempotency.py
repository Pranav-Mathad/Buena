"""Delivering or processing the same event twice must not duplicate facts."""

from __future__ import annotations

import uuid
from collections.abc import AsyncIterator

import pytest
from sqlalchemy import text

from backend.db.session import get_sessionmaker
from backend.pipeline.events import insert_event
from backend.pipeline.worker import process_specific


@pytest.fixture
async def property_alias(db: None) -> AsyncIterator[str]:
    alias = f"Testweg {uuid.uuid4().hex[:6]} Apt 5D"
    async with get_sessionmaker()() as session:
        pid = (
            await session.execute(
                text(
                    "INSERT INTO properties (name, address, aliases) "
                    "VALUES (:a, :a, ARRAY[:a]) RETURNING id"
                ),
                {"a": alias},
            )
        ).scalar_one()
        await session.commit()
    yield alias
    async with get_sessionmaker()() as session:
        await session.execute(text("DELETE FROM facts WHERE property_id = :p"), {"p": pid})
        await session.execute(
            text("DELETE FROM events WHERE property_id = :p OR source_ref LIKE 'test-idem-%'"),
            {"p": pid},
        )
        await session.execute(text("DELETE FROM properties WHERE id = :p"), {"p": pid})
        await session.commit()


async def _insert(alias: str, ref: str) -> tuple[uuid.UUID, bool]:
    async with get_sessionmaker()() as session:
        event_id, inserted = await insert_event(
            session,
            source="email",
            source_ref=ref,
            raw_content=f"Subject: Heizung\n\nDie Heizung in {alias} ist ausgefallen.",
        )
        await session.commit()
    return event_id, inserted


async def _facts_for(event_id: uuid.UUID) -> list[tuple[str, str, str]]:
    async with get_sessionmaker()() as session:
        rows = (
            await session.execute(
                text(
                    "SELECT section, field, value FROM facts "
                    "WHERE source_event_id = :e ORDER BY section, field"
                ),
                {"e": event_id},
            )
        ).all()
    return [(r.section, r.field, r.value) for r in rows]


async def test_redelivered_event_is_not_inserted_twice(property_alias: str) -> None:
    ref = f"test-idem-{uuid.uuid4().hex[:8]}"
    first_id, first_inserted = await _insert(property_alias, ref)
    second_id, second_inserted = await _insert(property_alias, ref)

    assert first_inserted is True
    assert second_inserted is False
    assert second_id == first_id


async def test_processed_event_is_not_processed_again(property_alias: str) -> None:
    event_id, _ = await _insert(property_alias, f"test-idem-{uuid.uuid4().hex[:8]}")
    assert await process_specific(event_id) is True
    facts = await _facts_for(event_id)
    assert facts

    assert await process_specific(event_id) is False
    assert await _facts_for(event_id) == facts


async def test_reprocessing_the_same_event_adds_no_facts(property_alias: str) -> None:
    """Forced replay (processed_at cleared, as a replay or recovery would) is a no-op."""
    event_id, _ = await _insert(property_alias, f"test-idem-{uuid.uuid4().hex[:8]}")
    await process_specific(event_id)
    facts = await _facts_for(event_id)

    async with get_sessionmaker()() as session:
        await session.execute(
            text("UPDATE events SET processed_at = NULL WHERE id = :e"), {"e": event_id}
        )
        await session.commit()
    assert await process_specific(event_id) is True

    assert await _facts_for(event_id) == facts
