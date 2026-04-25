"""Async event worker — the critical path.

One tick of :func:`process_one` does:

1. ``SELECT ... FOR UPDATE SKIP LOCKED`` the oldest unprocessed event.
2. If the event lacks a ``property_id``, run the router.
3. Build a short context excerpt, call the extractor.
4. Run the differ + applier.
5. Stamp ``processed_at``, commit, publish an SSE notification.

The scheduler calls :func:`process_batch` on a tight loop. Postgres is the
queue (Part III: no Redis, no Kafka) — SKIP LOCKED keeps concurrent workers
safe if we ever scale horizontally.
"""

from __future__ import annotations

import time
from typing import Any
from uuid import UUID

import structlog
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from backend.db.session import get_sessionmaker
from backend.pipeline.applier import apply as apply_plan
from backend.pipeline.differ import diff, load_current_facts
from backend.pipeline.events import get_event_bus
from backend.pipeline.extractor import extract
from backend.pipeline.renderer import render_markdown
from backend.pipeline.router import route
from backend.pipeline.validator import persist_rejections, validate

# Importing the constraints package registers every constraint in
# the validator's REGISTRY at module load. The validator only uses
# whatever is registered — silent omission is the failure mode we'd
# never want, so this import is non-optional.
import backend.pipeline.constraints  # noqa: F401

log = structlog.get_logger(__name__)


async def _claim_next(session: AsyncSession) -> dict[str, Any] | None:
    """Atomically lock + return the oldest unprocessed event, or ``None``."""
    result = await session.execute(
        text(
            """
            SELECT id, source, source_ref, raw_content, property_id,
                   received_at, metadata
            FROM events
            WHERE processed_at IS NULL
              AND processing_error IS NULL
            ORDER BY received_at ASC
            FOR UPDATE SKIP LOCKED
            LIMIT 1
            """
        )
    )
    row = result.first()
    if row is None:
        return None
    return {
        "id": row.id,
        "source": row.source,
        "source_ref": row.source_ref,
        "raw_content": row.raw_content,
        "property_id": row.property_id,
        "received_at": row.received_at,
        "metadata": dict(row.metadata or {}),
    }


async def _context_excerpt(session: AsyncSession, property_id: UUID) -> str:
    """Return a short markdown excerpt for the Gemini prompt."""
    markdown = await render_markdown(session, property_id)
    lines = markdown.splitlines()
    return "\n".join(lines[:30])


async def _property_name(session: AsyncSession, property_id: UUID) -> str:
    """Look up a property's display name for the Gemini prompt."""
    result = await session.execute(
        text("SELECT name FROM properties WHERE id = :pid"),
        {"pid": property_id},
    )
    row = result.first()
    return str(row.name) if row else "(unknown)"


async def _mark_processed(
    session: AsyncSession,
    event_id: UUID,
    *,
    property_id: UUID | None,
    error: str | None = None,
) -> None:
    """Set ``processed_at`` (and optionally ``processing_error``) on the event."""
    await session.execute(
        text(
            """
            UPDATE events
            SET processed_at = now(),
                processing_error = :err,
                property_id = COALESCE(:pid, property_id)
            WHERE id = :id
            """
        ),
        {"id": event_id, "pid": property_id, "err": error},
    )


async def process_one(session: AsyncSession) -> UUID | None:
    """Process a single event end-to-end. Returns the event id if work was done."""
    event = await _claim_next(session)
    if event is None:
        return None

    start = time.perf_counter()
    event_id: UUID = event["id"]
    property_id: UUID | None = event["property_id"]

    try:
        if property_id is None:
            match = await route(session, event["raw_content"])
            if match is None:
                log.info("worker.unrouted", event_id=str(event_id))
                await _mark_processed(
                    session, event_id, property_id=None, error="unrouted"
                )
                await session.commit()
                return event_id
            property_id = match.property_id

        property_name = await _property_name(session, property_id)
        excerpt = await _context_excerpt(session, property_id)

        result = await extract(
            property_name=property_name,
            current_context_excerpt=excerpt,
            source=event["source"],
            raw_content=event["raw_content"],
        )

        plan = await diff(
            session,
            property_id=property_id,
            event_source=event["source"],
            proposals=result.facts_to_update,
        )

        # Phase 9 Step 9.2 — constraint validator. Filters proposals
        # the differ accepted but that violate semantic rules (e.g.
        # rent change without an addendum, building floor count
        # change from a free-text email). Rejections persist to
        # rejected_updates for the admin /rejected inbox.
        current_facts = await load_current_facts(session, property_id)
        validated_plan, rejections = validate(
            plan,
            event={
                "source": event["source"],
                "metadata": event.get("metadata") or {},
            },
            current_facts=current_facts,
        )
        if rejections:
            await persist_rejections(
                session,
                event_id=event_id,
                property_id=property_id,
                building_id=None,
                liegenschaft_id=None,
                rejections=rejections,
            )

        written = await apply_plan(
            session,
            property_id=property_id,
            source_event_id=event_id,
            plan=validated_plan,
        )

        # Always drop a summary onto the activity section so the feed stays lively.
        if result.summary:
            await session.execute(
                text(
                    """
                    INSERT INTO facts
                        (property_id, section, field, value, source_event_id,
                         confidence, valid_from)
                    VALUES (:pid, 'activity', :field, :value, :eid, :conf, now())
                    """
                ),
                {
                    "pid": property_id,
                    "field": f"event_{event_id}",
                    "value": result.summary,
                    "eid": event_id,
                    "conf": max(0.5, min(0.95, 0.7 + len(plan.decisions) * 0.05)),
                },
            )

        await _mark_processed(session, event_id, property_id=property_id)
        await session.commit()
        latency_ms = (time.perf_counter() - start) * 1000
        log.info(
            "worker.processed",
            event_id=str(event_id),
            property_id=str(property_id),
            category=result.category,
            facts_written=written,
            extractor=result.source,
            latency_ms=round(latency_ms, 1),
        )
        await get_event_bus().publish(
            property_id,
            {
                "event_id": str(event_id),
                "property_id": str(property_id),
                "category": result.category,
                "summary": result.summary,
                "facts_written": written,
            },
        )
        return event_id
    except Exception as exc:  # noqa: BLE001 — swallow, mark event, keep worker alive
        await session.rollback()
        log.exception("worker.error", event_id=str(event_id))
        async with get_sessionmaker()() as fresh:
            await _mark_processed(
                fresh, event_id, property_id=property_id, error=str(exc)[:500]
            )
            await fresh.commit()
        return event_id


async def process_batch(max_events: int = 20) -> int:
    """Drain up to ``max_events`` events; return how many were touched."""
    factory = get_sessionmaker()
    processed = 0
    while processed < max_events:
        async with factory() as session:
            event_id = await process_one(session)
        if event_id is None:
            break
        processed += 1
    if processed:
        log.debug("worker.batch.done", processed=processed)
    return processed
