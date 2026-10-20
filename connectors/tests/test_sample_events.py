"""The sample-event loader runs synthetic emails through the pipeline, once."""

from __future__ import annotations

import json
import uuid
from collections.abc import AsyncIterator
from pathlib import Path

import pytest
from sqlalchemy import text

from backend.db.session import get_sessionmaker
from connectors import sample_events

_REF_PREFIX = "test-sample-"


@pytest.fixture
async def alias(db: None) -> AsyncIterator[str]:
    """A throwaway property the test emails route to, removed afterwards."""
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
            text("DELETE FROM events WHERE property_id = :p OR source_ref LIKE :prefix"),
            {"p": pid, "prefix": f"{_REF_PREFIX}%"},
        )
        await session.execute(text("DELETE FROM properties WHERE id = :p"), {"p": pid})
        await session.commit()


def _write_events(tmp_path: Path, alias: str) -> Path:
    run = uuid.uuid4().hex[:6]
    events = [
        {
            "source_ref": f"{_REF_PREFIX}{run}-heating",
            "days_ago": 30,
            "raw_content": f"Subject: Heizung\n\nDie Heizung in {alias} ist ausgefallen.",
        },
        {
            "source_ref": f"{_REF_PREFIX}{run}-water",
            "days_ago": 3,
            "raw_content": f"Subject: Wasserschaden\n\nIn {alias} tropft Wasser von der Decke.",
        },
    ]
    path = tmp_path / "events.json"
    path.write_text(json.dumps({"events": events}), encoding="utf-8")
    return path


async def _rows(alias: str) -> list[tuple[int, int]]:
    """``(days since received, facts written)`` per event on the property, oldest first."""
    async with get_sessionmaker()() as session:
        rows = (
            await session.execute(
                text(
                    """
                    SELECT round(extract(epoch FROM now() - e.received_at) / 86400) AS days,
                           (SELECT count(*) FROM facts f WHERE f.source_event_id = e.id) AS facts
                    FROM events e JOIN properties p ON p.id = e.property_id
                    WHERE p.name = :a AND e.processed_at IS NOT NULL
                    ORDER BY e.received_at
                    """
                ),
                {"a": alias},
            )
        ).all()
    return [(int(r.days), int(r.facts)) for r in rows]


async def test_events_are_routed_backdated_and_extracted(alias: str, tmp_path: Path) -> None:
    summary = await sample_events.load_sample_events(_write_events(tmp_path, alias))

    assert summary == sample_events.SampleEventsSummary(inserted=2, already_present=0, processed=2)
    rows = await _rows(alias)
    assert [days for days, _ in rows] == [30, 3]
    assert all(facts > 0 for _, facts in rows)


async def test_a_second_run_changes_nothing(alias: str, tmp_path: Path) -> None:
    path = _write_events(tmp_path, alias)
    await sample_events.load_sample_events(path)
    before = await _rows(alias)

    again = await sample_events.load_sample_events(path)

    assert again == sample_events.SampleEventsSummary(inserted=0, already_present=2, processed=0)
    assert await _rows(alias) == before


def test_committed_sample_file_targets_we_29() -> None:
    events = json.loads(sample_events.DEFAULT_PATH.read_text(encoding="utf-8"))["events"]

    refs = [e["source_ref"] for e in events]
    assert len(refs) == len(set(refs))
    assert all(ref.startswith("sample-") for ref in refs)
    assert all(isinstance(e["days_ago"], int) and e["days_ago"] >= 0 for e in events)
    assert all("WE 29" in e["raw_content"] for e in events)
