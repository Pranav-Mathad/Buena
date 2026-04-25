"""Tests for Phase 10 Step 10.2 — sequential replay engine.

The engine drives :func:`backend.pipeline.worker.process_one`, so we
monkeypatch that to keep tests offline + fast. Pause / resume / stop
are exercised against the public API so the asyncio.Event idiom is
covered.
"""

from __future__ import annotations

import asyncio
from typing import Any
from uuid import UUID, uuid4

import pytest
from sqlalchemy import text

from backend.config import get_settings
from backend.db.session import get_sessionmaker
from backend.services import replay
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


async def _seed_property_with_events(n_events: int = 3) -> tuple[UUID, list[UUID]]:
    """Insert one property + N events for the replay run to chew on."""
    factory = get_sessionmaker()
    async with factory() as session:
        b = (
            await session.execute(
                text(
                    "INSERT INTO buildings (address) VALUES "
                    "('Test Replay Bldg') RETURNING id"
                )
            )
        ).first()
        p = (
            await session.execute(
                text(
                    """
                    INSERT INTO properties (name, address, building_id, aliases)
                    VALUES ('Test Replay Prop', 'addr', :bid, ARRAY['x'])
                    RETURNING id
                    """
                ),
                {"bid": b.id},
            )
        ).first()
        event_ids: list[UUID] = []
        for i in range(n_events):
            ref = f"replay-{uuid4().hex[:8]}"
            e = (
                await session.execute(
                    text(
                        """
                        INSERT INTO events (
                          source, source_ref, raw_content, property_id, processed_at
                        ) VALUES (
                          'email', :ref, :body, :pid, now()
                        )
                        RETURNING id
                        """
                    ),
                    {
                        "ref": ref,
                        "body": f"Test replay body {i}",
                        "pid": p.id,
                    },
                )
            ).first()
            event_ids.append(e.id)
        await session.commit()
    return p.id, event_ids


async def _cleanup(property_id: UUID) -> None:
    """Wipe the test property's footprint."""
    factory = get_sessionmaker()
    async with factory() as session:
        await session.execute(
            text("DELETE FROM replay_runs WHERE property_id = :pid"),
            {"pid": property_id},
        )
        await session.execute(
            text("DELETE FROM facts WHERE property_id = :pid"), {"pid": property_id}
        )
        await session.execute(
            text("DELETE FROM uncertainty_events WHERE property_id = :pid"),
            {"pid": property_id},
        )
        await session.execute(
            text("DELETE FROM rejected_updates WHERE property_id = :pid"),
            {"pid": property_id},
        )
        await session.execute(
            text("DELETE FROM events WHERE property_id = :pid"),
            {"pid": property_id},
        )
        # Building lookup via property no longer possible after delete; we
        # just clean the test row by name.
        await session.execute(
            text(
                "DELETE FROM properties WHERE id = :pid "
            ),
            {"pid": property_id},
        )
        await session.execute(
            text("DELETE FROM buildings WHERE address = 'Test Replay Bldg'")
        )
        await session.commit()


async def test_pause_resume_stop_full_cycle(monkeypatch: pytest.MonkeyPatch) -> None:
    """Start → pause → resume → stop hits every state and the durable row tracks it."""
    await _setup_or_skip()
    pid, _ = await _seed_property_with_events(n_events=3)
    try:
        # Replace process_one with a fast no-op so the test doesn't hit Gemini.
        async def _noop_process_one(_session: Any) -> UUID | None:
            return None

        monkeypatch.setattr(
            replay, "process_one", _noop_process_one, raising=True
        )

        snapshot = await replay.start_run(
            property_id=pid,
            speed_multiplier=10000,  # tight pacing so the test is fast
            source_filter=["email"],
            reset_property=False,
        )
        run_id = UUID(snapshot["run_id"])
        assert snapshot["status"] == "running"
        assert snapshot["total_events"] == 3

        paused = await replay.pause_run(run_id)
        assert paused["status"] == "paused"
        assert paused["paused_at"] is not None

        resumed = await replay.resume_run(run_id)
        assert resumed["status"] == "running"

        stopped = await replay.stop_run(run_id)
        assert stopped["status"] == "stopped"
        assert stopped["completed_at"] is not None
    finally:
        await _cleanup(pid)


async def test_scheduled_pause_fires_at_checkpoint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A scheduled pause at at_seconds=0 puts the run into ``paused`` immediately."""
    await _setup_or_skip()
    pid, _ = await _seed_property_with_events(n_events=2)
    try:
        async def _noop_process_one(_session: Any) -> UUID | None:
            return None

        monkeypatch.setattr(
            replay, "process_one", _noop_process_one, raising=True
        )

        snapshot = await replay.start_run(
            property_id=pid,
            speed_multiplier=10000,
            source_filter=["email"],
            scheduled_pauses=[{"at_seconds": 0.0, "message": "test pause"}],
        )
        run_id = UUID(snapshot["run_id"])

        # Give the driver a tick to observe the at_seconds=0 checkpoint.
        for _ in range(10):
            status = await replay.get_run_status(run_id)
            if status["status"] == "paused":
                break
            await asyncio.sleep(0.05)
        assert status["status"] == "paused", status

        # Resume and wait for completion.
        await replay.resume_run(run_id)
        for _ in range(40):
            status = await replay.get_run_status(run_id)
            if status["status"] in {"completed", "stopped"}:
                break
            await asyncio.sleep(0.1)
        assert status["status"] == "completed", status
    finally:
        await _cleanup(pid)


async def test_reset_property_wipes_facts_only(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``reset_property=True`` clears facts but leaves stammdaten / events."""
    await _setup_or_skip()
    pid, event_ids = await _seed_property_with_events(n_events=1)
    try:
        # Pre-seed a fact so reset has something to delete.
        factory = get_sessionmaker()
        async with factory() as session:
            await session.execute(
                text(
                    """
                    INSERT INTO facts (
                      property_id, section, field, value, source_event_id,
                      confidence, valid_from
                    ) VALUES (
                      :pid, 'overview', 'note', 'pre-replay value', :eid,
                      0.9, now()
                    )
                    """
                ),
                {"pid": pid, "eid": event_ids[0]},
            )
            await session.commit()

        async def _noop_process_one(_session: Any) -> UUID | None:
            return None

        monkeypatch.setattr(
            replay, "process_one", _noop_process_one, raising=True
        )

        snapshot = await replay.start_run(
            property_id=pid,
            speed_multiplier=10000,
            source_filter=["email"],
            reset_property=True,
        )
        run_id = UUID(snapshot["run_id"])
        for _ in range(40):
            status = await replay.get_run_status(run_id)
            if status["status"] in {"completed", "stopped"}:
                break
            await asyncio.sleep(0.1)

        async with factory() as session:
            facts_left = (
                await session.execute(
                    text(
                        "SELECT COUNT(*) FROM facts WHERE property_id = :pid"
                    ),
                    {"pid": pid},
                )
            ).scalar()
            events_left = (
                await session.execute(
                    text(
                        "SELECT COUNT(*) FROM events WHERE property_id = :pid"
                    ),
                    {"pid": pid},
                )
            ).scalar()
            prop_left = (
                await session.execute(
                    text("SELECT COUNT(*) FROM properties WHERE id = :pid"),
                    {"pid": pid},
                )
            ).scalar()
        assert facts_left == 0, "reset_property should drop facts"
        assert events_left == 1, "reset_property must NOT delete events"
        assert prop_left == 1, "reset_property must NOT touch stammdaten"
    finally:
        await _cleanup(pid)


async def test_normalise_pauses_validates_and_sorts() -> None:
    """``_normalise_pauses`` drops bad rows and sorts by ``at_seconds``."""
    raw = [
        {"at_seconds": 30.0, "message": "second"},
        {"at_seconds": 10.0, "message": "first"},
        {"at_seconds": "not_a_number", "message": "drop me"},
        {"message": "missing at_seconds"},
        "not even a dict",
    ]
    out = replay._normalise_pauses(raw)
    assert [p["at_seconds"] for p in out] == [10.0, 30.0]
    assert out[0]["message"] == "first"
    assert out[1]["message"] == "second"
