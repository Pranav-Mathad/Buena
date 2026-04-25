"""Phase 10 Step 10.2 — sequential replay engine.

The replay reads existing events for a property and re-streams them
through the live pipeline at a configurable speed multiplier so the
demo can show the file building itself in real time. Idempotency is
guaranteed by the existing ``events.UNIQUE (source, source_ref)``
constraint plus the optional ``reset_property`` flag — the engine
isn't generating new events; it's re-running already-ingested ones
through the worker after deleting the property's facts and
uncertainty rows.

Key design calls:

- **Stammdaten is preserved.** A reset wipes ``facts``,
  ``uncertainty_events``, and ``rejected_updates`` for the property.
  Owner / building / liegenschaft links stay; the property starts at
  "day zero" but identifies as itself.
- **scheduled_pauses** is a list of ``{at_seconds, message}`` items.
  When the elapsed seconds since stream start cross a checkpoint the
  engine halts, sets ``status='paused'``, and emits a control event.
  The operator (or the demo wrapper) calls ``resume_run`` to continue.
- **In-process state** lives in :data:`_RUNS` keyed by run id. Postgres
  carries the *durable* state for the UI; ``_RUNS`` carries the
  asyncio event handles a single process needs to pause/resume.
  Restarting the API server cancels in-flight runs cleanly because
  the asyncio handles vanish — durable rows then sit at
  ``status='running'`` until an operator manually flips them to
  ``stopped``.
- **Live worker reuse.** Each event re-streamed runs through
  :func:`backend.pipeline.worker.process_one`, so the validator,
  uncertainty writer, and renderer fan-out behave identically to a
  freshly-ingested event.
"""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any
from uuid import UUID

import structlog
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from backend.db.session import get_sessionmaker
from backend.pipeline.events import get_event_bus
from backend.pipeline.worker import process_one

log = structlog.get_logger(__name__)


# Each ``scheduled_pauses`` entry has this shape; we keep validation
# loose here so future demo scripts can stuff extra hint fields the UI
# wants to render without a migration.
_REQUIRED_PAUSE_KEYS = frozenset({"at_seconds", "message"})


@dataclass
class _RunHandle:
    """Per-process control state for one active replay run."""

    run_id: UUID
    property_id: UUID
    speed_multiplier: int
    pause_event: asyncio.Event = field(default_factory=asyncio.Event)
    stop_event: asyncio.Event = field(default_factory=asyncio.Event)
    task: asyncio.Task[None] | None = None

    def __post_init__(self) -> None:
        # Pause event is set when the run is *running*; cleared when paused.
        # asyncio.Event-as-flag idiom: ``await pause_event.wait()`` blocks
        # exactly while the flag is cleared.
        self.pause_event.set()


_RUNS: dict[UUID, _RunHandle] = {}


# ----------------------------------------------------------------------------
# Public API
# ----------------------------------------------------------------------------


async def start_run(
    *,
    property_id: UUID,
    speed_multiplier: int = 10,
    source_filter: list[str] | None = None,
    start_date: datetime | None = None,
    end_date: datetime | None = None,
    scheduled_pauses: list[dict[str, Any]] | None = None,
    reset_property: bool = False,
) -> dict[str, Any]:
    """Spin up a replay run; return the durable row's snapshot."""
    sources = list(source_filter or ["email", "invoice", "bank"])
    pauses = _normalise_pauses(scheduled_pauses or [])
    factory = get_sessionmaker()

    if reset_property:
        async with factory() as session:
            await _reset_property_state(session, property_id)
            await session.commit()

    async with factory() as session:
        total = await _count_events(
            session,
            property_id=property_id,
            sources=sources,
            start_date=start_date,
            end_date=end_date,
        )
        row = (
            await session.execute(
                text(
                    """
                    INSERT INTO replay_runs (
                      property_id, speed_multiplier, source_filter,
                      start_date, end_date, scheduled_pauses,
                      reset_property, status, total_events
                    ) VALUES (
                      :pid, :spd, :sources,
                      :sd, :ed, CAST(:pauses AS JSONB),
                      :reset, 'running', :total
                    )
                    RETURNING id, started_at
                    """
                ),
                {
                    "pid": property_id,
                    "spd": speed_multiplier,
                    "sources": sources,
                    "sd": start_date,
                    "ed": end_date,
                    "pauses": json.dumps(pauses),
                    "reset": reset_property,
                    "total": total,
                },
            )
        ).first()
        await session.commit()

    run_id: UUID = row.id
    handle = _RunHandle(
        run_id=run_id,
        property_id=property_id,
        speed_multiplier=speed_multiplier,
    )
    _RUNS[run_id] = handle
    handle.task = asyncio.create_task(
        _drive_run(
            handle=handle,
            sources=sources,
            start_date=start_date,
            end_date=end_date,
            pauses=pauses,
        )
    )

    log.info(
        "replay.started",
        run_id=str(run_id),
        property_id=str(property_id),
        speed=speed_multiplier,
        total_events=total,
        pauses=len(pauses),
        reset=reset_property,
    )
    return {
        "run_id": str(run_id),
        "property_id": str(property_id),
        "speed_multiplier": speed_multiplier,
        "source_filter": sources,
        "scheduled_pauses": pauses,
        "reset_property": reset_property,
        "status": "running",
        "total_events": total,
        "processed_events": 0,
        "started_at": row.started_at,
    }


async def pause_run(run_id: UUID) -> dict[str, Any]:
    """Pause a running stream — the worker loop will block until resume."""
    handle = _RUNS.get(run_id)
    if handle is not None:
        handle.pause_event.clear()
    await _set_status(run_id, "paused", with_paused_at=True)
    await _publish_control(run_id, "replay.paused", {"reason": "operator"})
    log.info("replay.paused", run_id=str(run_id))
    return await get_run_status(run_id)


async def resume_run(run_id: UUID) -> dict[str, Any]:
    """Resume a paused stream."""
    handle = _RUNS.get(run_id)
    if handle is not None:
        handle.pause_event.set()
    await _set_status(run_id, "running", with_paused_at=False)
    await _publish_control(run_id, "replay.resumed", {})
    log.info("replay.resumed", run_id=str(run_id))
    return await get_run_status(run_id)


async def stop_run(run_id: UUID) -> dict[str, Any]:
    """Cancel a run; in-flight event finishes, no further events stream."""
    handle = _RUNS.get(run_id)
    if handle is not None:
        handle.stop_event.set()
        # Unblock a paused stream so the cancel flag is observed.
        handle.pause_event.set()
        if handle.task is not None and not handle.task.done():
            try:
                await asyncio.wait_for(handle.task, timeout=5.0)
            except asyncio.TimeoutError:
                handle.task.cancel()
    await _set_status(run_id, "stopped")
    await _publish_control(run_id, "replay.stopped", {"reason": "operator"})
    log.info("replay.stopped", run_id=str(run_id))
    return await get_run_status(run_id)


async def get_run_status(run_id: UUID) -> dict[str, Any]:
    """Read the durable row for ``run_id``."""
    factory = get_sessionmaker()
    async with factory() as session:
        row = (
            await session.execute(
                text(
                    """
                    SELECT id, property_id, speed_multiplier, source_filter,
                           start_date, end_date, scheduled_pauses,
                           reset_property, status, total_events,
                           processed_events, last_error,
                           started_at, paused_at, completed_at
                    FROM replay_runs
                    WHERE id = :rid
                    """
                ),
                {"rid": run_id},
            )
        ).first()
    if row is None:
        raise LookupError(f"replay run {run_id} not found")
    return {
        "run_id": str(row.id),
        "property_id": str(row.property_id),
        "speed_multiplier": int(row.speed_multiplier),
        "source_filter": list(row.source_filter or []),
        "start_date": row.start_date,
        "end_date": row.end_date,
        "scheduled_pauses": list(row.scheduled_pauses or []),
        "reset_property": bool(row.reset_property),
        "status": str(row.status),
        "total_events": int(row.total_events or 0),
        "processed_events": int(row.processed_events or 0),
        "last_error": row.last_error,
        "started_at": row.started_at,
        "paused_at": row.paused_at,
        "completed_at": row.completed_at,
    }


# ----------------------------------------------------------------------------
# Internals
# ----------------------------------------------------------------------------


def _normalise_pauses(pauses: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Validate + sort scheduled pause checkpoints."""
    out: list[dict[str, Any]] = []
    for p in pauses:
        if not isinstance(p, dict):
            continue
        if not _REQUIRED_PAUSE_KEYS.issubset(p):
            continue
        try:
            at = float(p["at_seconds"])
        except (TypeError, ValueError):
            continue
        out.append({"at_seconds": at, "message": str(p["message"])[:200]})
    out.sort(key=lambda x: x["at_seconds"])
    return out


async def _count_events(
    session: AsyncSession,
    *,
    property_id: UUID,
    sources: list[str],
    start_date: datetime | None,
    end_date: datetime | None,
) -> int:
    """Count the events the run will replay, honouring filters."""
    where = "property_id = :pid AND source = ANY(:sources)"
    params: dict[str, Any] = {"pid": property_id, "sources": sources}
    if start_date is not None:
        where += " AND received_at >= :sd"
        params["sd"] = start_date
    if end_date is not None:
        where += " AND received_at <= :ed"
        params["ed"] = end_date
    return int(
        (
            await session.execute(
                text(f"SELECT COUNT(*) FROM events WHERE {where}"),
                params,
            )
        ).scalar()
        or 0
    )


async def _reset_property_state(session: AsyncSession, property_id: UUID) -> None:
    """Wipe ``facts``, ``uncertainty_events``, ``rejected_updates``,
    and the ``processed_at`` stamp on events for a clean replay.

    Stammdaten links (owner, building, liegenschaft) are preserved.
    """
    await session.execute(
        text("DELETE FROM facts WHERE property_id = :pid"),
        {"pid": property_id},
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
        text(
            """
            UPDATE events
            SET processed_at = NULL, processing_error = NULL
            WHERE property_id = :pid
            """
        ),
        {"pid": property_id},
    )


async def _set_status(
    run_id: UUID,
    status: str,
    *,
    with_paused_at: bool = False,
    last_error: str | None = None,
) -> None:
    """Update the durable row's status (+ paused_at / completed_at when relevant)."""
    factory = get_sessionmaker()
    async with factory() as session:
        if status == "completed":
            await session.execute(
                text(
                    """
                    UPDATE replay_runs
                    SET status = :st, completed_at = now()
                    WHERE id = :rid
                    """
                ),
                {"st": status, "rid": run_id},
            )
        elif status == "stopped":
            await session.execute(
                text(
                    """
                    UPDATE replay_runs
                    SET status = :st, completed_at = now()
                    WHERE id = :rid
                    """
                ),
                {"st": status, "rid": run_id},
            )
        elif status == "failed":
            await session.execute(
                text(
                    """
                    UPDATE replay_runs
                    SET status = :st, completed_at = now(), last_error = :err
                    WHERE id = :rid
                    """
                ),
                {"st": status, "rid": run_id, "err": last_error},
            )
        elif with_paused_at:
            await session.execute(
                text(
                    """
                    UPDATE replay_runs
                    SET status = :st, paused_at = now()
                    WHERE id = :rid
                    """
                ),
                {"st": status, "rid": run_id},
            )
        else:
            await session.execute(
                text(
                    """
                    UPDATE replay_runs
                    SET status = :st
                    WHERE id = :rid
                    """
                ),
                {"st": status, "rid": run_id},
            )
        await session.commit()


async def _publish_control(
    run_id: UUID, kind: str, payload: dict[str, Any]
) -> None:
    """Push a control message onto the event bus so SSE subscribers see it.

    Replay control events fan out under the property's bus channel
    plus the firehose, so a UI tracking the property naturally picks
    them up.
    """
    handle = _RUNS.get(run_id)
    if handle is None:
        return
    bus = get_event_bus()
    body = {"type": kind, "run_id": str(run_id), **payload}
    await bus.publish(handle.property_id, body)


async def _drive_run(
    *,
    handle: _RunHandle,
    sources: list[str],
    start_date: datetime | None,
    end_date: datetime | None,
    pauses: list[dict[str, Any]],
) -> None:
    """Background task: stream events through process_one one by one."""
    factory = get_sessionmaker()
    started = asyncio.get_event_loop().time()
    pauses_pending = list(pauses)

    try:
        async with factory() as session:
            event_rows = await _fetch_events(
                session,
                property_id=handle.property_id,
                sources=sources,
                start_date=start_date,
                end_date=end_date,
            )

        for index, row in enumerate(event_rows, start=1):
            if handle.stop_event.is_set():
                log.info("replay.drive.stop_observed", run_id=str(handle.run_id))
                return

            elapsed = asyncio.get_event_loop().time() - started
            while pauses_pending and pauses_pending[0]["at_seconds"] <= elapsed:
                pause = pauses_pending.pop(0)
                handle.pause_event.clear()
                await _set_status(
                    handle.run_id, "paused", with_paused_at=True
                )
                await _publish_control(
                    handle.run_id,
                    "replay.scheduled_pause",
                    {
                        "at_seconds": pause["at_seconds"],
                        "message": pause["message"],
                    },
                )
                log.info(
                    "replay.scheduled_pause",
                    run_id=str(handle.run_id),
                    at_seconds=pause["at_seconds"],
                )

            # Block while paused.
            await handle.pause_event.wait()
            if handle.stop_event.is_set():
                return

            # Re-stream by clearing processed_at on this row and
            # immediately calling process_one. The worker's
            # _claim_next will pick it up via SKIP LOCKED. This
            # preserves all the pipeline behaviour (extractor +
            # validator + applier + uncertainty fan-out).
            async with factory() as session:
                await session.execute(
                    text(
                        """
                        UPDATE events
                        SET processed_at = NULL, processing_error = NULL
                        WHERE id = :id
                        """
                    ),
                    {"id": row.id},
                )
                await session.commit()
            async with factory() as session:
                await process_one(session)

            await _bump_progress(handle.run_id, processed=index, last_event_id=row.id)

            # Pace the stream: target one historical day per real
            # second when speed=10 (i.e. 86,400× faster than wall-clock
            # would replay the archive). For Buena's data the
            # inter-event budget collapses to "as fast as the worker
            # runs" because the archive spans months but the Pro
            # extractor takes ~12 s per call. We add a small floor so
            # the SSE stream doesn't flood the client.
            await asyncio.sleep(max(0.02, 1.0 / handle.speed_multiplier))
    except Exception as exc:  # noqa: BLE001 — surface the failure cleanly
        log.exception("replay.drive.error", run_id=str(handle.run_id))
        await _set_status(
            handle.run_id, "failed", last_error=f"{type(exc).__name__}: {exc}"[:500]
        )
        await _publish_control(
            handle.run_id,
            "replay.failed",
            {"error": f"{type(exc).__name__}: {exc}"[:200]},
        )
        return

    await _set_status(handle.run_id, "completed")
    await _publish_control(handle.run_id, "replay.completed", {})
    log.info(
        "replay.completed",
        run_id=str(handle.run_id),
        property_id=str(handle.property_id),
    )


async def _fetch_events(
    session: AsyncSession,
    *,
    property_id: UUID,
    sources: list[str],
    start_date: datetime | None,
    end_date: datetime | None,
) -> list[Any]:
    """Pull every event in window, ordered chronologically."""
    where = "property_id = :pid AND source = ANY(:sources)"
    params: dict[str, Any] = {"pid": property_id, "sources": sources}
    if start_date is not None:
        where += " AND received_at >= :sd"
        params["sd"] = start_date
    if end_date is not None:
        where += " AND received_at <= :ed"
        params["ed"] = end_date
    return list(
        (
            await session.execute(
                text(
                    f"""
                    SELECT id, source, received_at
                    FROM events
                    WHERE {where}
                    ORDER BY received_at ASC, id ASC
                    """
                ),
                params,
            )
        ).all()
    )


async def _bump_progress(run_id: UUID, *, processed: int, last_event_id: UUID) -> None:
    """Record how many events have streamed; used by the SSE UI."""
    factory = get_sessionmaker()
    async with factory() as session:
        await session.execute(
            text(
                """
                UPDATE replay_runs
                SET processed_events = :n, last_event_id = :eid
                WHERE id = :rid
                """
            ),
            {"n": processed, "eid": last_event_id, "rid": run_id},
        )
        await session.commit()


__all__ = [
    "get_run_status",
    "pause_run",
    "resume_run",
    "start_run",
    "stop_run",
]
