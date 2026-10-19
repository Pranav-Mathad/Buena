"""Maintenance signal rules count reported incidents, not currently valid facts."""

from __future__ import annotations

import uuid
from collections.abc import AsyncIterator
from dataclasses import dataclass
from uuid import UUID

import pytest
from sqlalchemy import text

from backend.db.session import get_sessionmaker
from backend.signals.rules import cross_property_pattern, recurring_maintenance

_REF_PREFIX = "test-recurring-"


@dataclass
class Unit:
    """A throwaway property; ``last_fact`` is the current heating fact, if any."""

    id: UUID
    last_fact: UUID | None = None


@pytest.fixture
async def unit(db: None) -> AsyncIterator[Unit]:
    name = f"Testweg {uuid.uuid4().hex[:6]} Apt 1"
    async with get_sessionmaker()() as session:
        pid = (
            await session.execute(
                text("INSERT INTO properties (name, address) VALUES (:n, :n) RETURNING id"),
                {"n": name},
            )
        ).scalar_one()
        await session.commit()
    yield Unit(id=pid)
    async with get_sessionmaker()() as session:
        await session.execute(
            text("UPDATE facts SET superseded_by = NULL WHERE property_id = :p"), {"p": pid}
        )
        await session.execute(text("DELETE FROM facts WHERE property_id = :p"), {"p": pid})
        await session.execute(
            text("DELETE FROM events WHERE source_ref LIKE :prefix"),
            {"prefix": f"{_REF_PREFIX}%"},
        )
        await session.execute(text("DELETE FROM properties WHERE id = :p"), {"p": pid})
        await session.commit()


async def _report(unit: Unit, *, days_ago: int, event_id: UUID | None = None) -> UUID:
    """Record one heating fact, superseding the unit's current one, as the differ does.

    Without ``event_id`` a new incident (event) is created ``days_ago`` days
    back; with it, the fact belongs to that existing event (an operator edit).
    """
    async with get_sessionmaker()() as session:
        if event_id is None:
            event_id = (
                await session.execute(
                    text(
                        "INSERT INTO events (source, source_ref, property_id, raw_content, "
                        "received_at, processed_at) VALUES ('email', :ref, :p, 'Heizung kalt', "
                        "now() - make_interval(days => :d), now()) RETURNING id"
                    ),
                    {"ref": f"{_REF_PREFIX}{uuid.uuid4().hex}", "p": unit.id, "d": days_ago},
                )
            ).scalar_one()
        fact_id = (
            await session.execute(
                text(
                    "INSERT INTO facts (property_id, section, field, value, source_event_id, "
                    "confidence) VALUES (:p, 'maintenance', 'open_heating_issue', "
                    "'Heizung ausgefallen', :e, 0.8) RETURNING id"
                ),
                {"p": unit.id, "e": event_id},
            )
        ).scalar_one()
        if unit.last_fact is not None:
            await session.execute(
                text("UPDATE facts SET superseded_by = :new WHERE id = :old"),
                {"new": fact_id, "old": unit.last_fact},
            )
        await session.commit()
    unit.last_fact = fact_id
    return event_id


async def _heating_occurrences(unit: Unit) -> int | None:
    """Occurrences in the unit's heating candidate, or None if it did not fire."""
    async with get_sessionmaker()() as session:
        candidates = await recurring_maintenance.evaluate(session)
    for c in candidates:
        if c.property_id == unit.id and c.action_hint["topic"] == "heating":
            return int(c.action_hint["occurrences"])
    return None


async def test_three_reports_of_one_issue_fire_although_each_supersedes_the_last(
    unit: Unit,
) -> None:
    """The extractor reuses the field, so only the newest fact is current."""
    for days_ago in (90, 40, 5):
        await _report(unit, days_ago=days_ago)

    assert await _heating_occurrences(unit) == 3


async def test_an_operator_edit_is_not_a_new_incident(unit: Unit) -> None:
    await _report(unit, days_ago=30)
    second = await _report(unit, days_ago=10)
    await _report(unit, days_ago=0, event_id=second)  # the edit keeps the event

    assert await _heating_occurrences(unit) is None


async def test_the_window_uses_when_the_incident_was_reported(unit: Unit) -> None:
    """A report from 200 days ago, processed today, is outside the 120-day window."""
    for days_ago in (200, 20, 2):
        await _report(unit, days_ago=days_ago)

    assert await _heating_occurrences(unit) is None


@pytest.fixture
async def building_units(db: None) -> AsyncIterator[tuple[UUID, Unit, Unit]]:
    """A pre-1990 test building with two units sharing its boiler."""
    tag = uuid.uuid4().hex[:6]
    async with get_sessionmaker()() as session:
        bid = (
            await session.execute(
                text("INSERT INTO buildings (address, year_built) VALUES (:a, 1962) RETURNING id"),
                {"a": f"Testweg {tag}, 10115 Berlin"},
            )
        ).scalar_one()
        pids = [
            (
                await session.execute(
                    text(
                        "INSERT INTO properties (name, address, building_id) "
                        "VALUES (:n, :n, :b) RETURNING id"
                    ),
                    {"n": f"Testweg {tag} Apt {apt}", "b": bid},
                )
            ).scalar_one()
            for apt in ("1", "2")
        ]
        await session.commit()
    yield bid, Unit(id=pids[0]), Unit(id=pids[1])
    async with get_sessionmaker()() as session:
        for pid in pids:
            await session.execute(
                text("UPDATE facts SET superseded_by = NULL WHERE property_id = :p"), {"p": pid}
            )
            await session.execute(text("DELETE FROM facts WHERE property_id = :p"), {"p": pid})
        await session.execute(
            text("DELETE FROM events WHERE source_ref LIKE :prefix"),
            {"prefix": f"{_REF_PREFIX}%"},
        )
        for pid in pids:
            await session.execute(text("DELETE FROM properties WHERE id = :p"), {"p": pid})
        await session.execute(text("DELETE FROM buildings WHERE id = :b"), {"b": bid})
        await session.commit()


async def _shared_boiler_incidents(building_id: UUID) -> int | None:
    async with get_sessionmaker()() as session:
        candidates = await cross_property_pattern.evaluate(session)
    for c in candidates:
        if c.action_hint.get("building_id") == str(building_id):
            return int(c.action_hint["incidents"])
    return None


async def test_shared_boiler_counts_superseded_reports_across_units(
    building_units: tuple[UUID, Unit, Unit],
) -> None:
    building_id, first, second = building_units
    await _report(first, days_ago=60)
    await _report(first, days_ago=20)  # supersedes the first unit's earlier fact
    await _report(second, days_ago=10)

    assert await _shared_boiler_incidents(building_id) == 3


async def test_shared_boiler_ignores_reports_outside_the_window(
    building_units: tuple[UUID, Unit, Unit],
) -> None:
    building_id, first, second = building_units
    await _report(first, days_ago=200)
    await _report(first, days_ago=20)
    await _report(second, days_ago=10)

    assert await _shared_boiler_incidents(building_id) is None
