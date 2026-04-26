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
from backend.pipeline.applier import apply_uncertainties
from backend.pipeline.differ import diff, load_current_facts
from backend.pipeline.events import get_event_bus
from backend.pipeline.extractor import extract
from backend.pipeline.materializer import propagate_after_fact_write
from backend.pipeline.renderer import render_markdown
from backend.pipeline.router import route
from backend.pipeline.semantic_validator import semantic_validate
from backend.pipeline.validator import (
    Rejection,
    persist_rejections,
    validate,
)
from backend.services import pioneer_llm

# Importing the constraints package registers every constraint in
# the validator's REGISTRY at module load. The validator only uses
# whatever is registered — silent omission is the failure mode we'd
# never want, so this import is non-optional.
import backend.pipeline.constraints  # noqa: F401


def _needs_review_to_uncertainty(rejection: Rejection) -> dict[str, Any]:
    """Map a validator ``needs_review`` Rejection into an uncertainty item.

    The constraint emitted "this proposal *might* be valid but needs a
    human to confirm" — that's exactly the uncertainty inbox semantic,
    so we land it there instead of in ``rejected_updates``.
    """
    return {
        "observation": (
            f"proposed {rejection.section}.{rejection.field} = "
            f"{rejection.proposed_value!r}"
        ),
        "hypothesis": rejection.proposed_value,
        "reason_uncertain": rejection.reason,
        "relevant_section": rejection.section,
        "relevant_field": rejection.field,
        "source": f"validator_needs_review:{rejection.constraint_name}",
    }

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


async def _load_event_stammdaten(
    session: AsyncSession, property_id: UUID
) -> dict[str, dict[str, Any]]:
    """Pull the building + property master record for the validator.

    The validator's strict-mode constraints (floor count, address,
    year built, square meters) compare proposed values against this
    snapshot, so an email claim that contradicts master data gets
    rejected even when no fact row exists yet. Empty dicts are safe —
    constraints fall back to legacy "current is None → seed" behaviour
    when stammdaten is missing.
    """
    row = (
        await session.execute(
            text(
                """
                SELECT
                    b.address    AS building_address,
                    b.year_built AS building_year_built,
                    b.metadata   AS building_metadata,
                    p.metadata   AS property_metadata,
                    p.address    AS property_address
                FROM properties p
                LEFT JOIN buildings b ON b.id = p.building_id
                WHERE p.id = :pid
                """
            ),
            {"pid": property_id},
        )
    ).first()
    if row is None:
        return {}

    b_meta = dict(row.building_metadata or {})
    p_meta = dict(row.property_metadata or {})

    building_snap: dict[str, Any] = {}
    if row.building_address:
        building_snap["address"] = row.building_address
    if row.building_year_built is not None:
        building_snap["year_built"] = row.building_year_built
    # Buena's loader stores floor count as ``etagen`` in
    # buildings.metadata; the legacy seed path uses ``floors``. Read
    # both so either ingest path works.
    floors = b_meta.get("etagen") or b_meta.get("floors")
    if floors is not None:
        building_snap["floor_count"] = floors

    property_snap: dict[str, Any] = {}
    if row.property_address:
        property_snap["address"] = row.property_address
    qm = p_meta.get("wohnflaeche_qm") or p_meta.get("square_meters")
    if qm is not None:
        property_snap["square_meters"] = qm

    return {"building": building_snap, "property": property_snap}


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
    return await _process_event_body(session, event)


async def _claim_specific(
    session: AsyncSession, event_id: UUID
) -> dict[str, Any] | None:
    """Lock and return a specific event by id, regardless of queue position.

    Used by :func:`process_specific` so the debug trigger can extract a
    just-inserted event without draining hundreds of older items first.
    Returns ``None`` if the event is missing or already processed.
    """
    result = await session.execute(
        text(
            """
            SELECT id, source, source_ref, raw_content, property_id,
                   received_at, metadata
            FROM events
            WHERE id = :eid
              AND processed_at IS NULL
              AND processing_error IS NULL
            FOR UPDATE SKIP LOCKED
            """
        ),
        {"eid": event_id},
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


async def process_specific(event_id: UUID) -> bool:
    """Process one specific event end-to-end, ignoring queue order.

    Returns ``True`` if the event was processed (success or graceful
    failure marked on the row) and ``False`` if it was already done or
    locked by another worker.
    """
    factory = get_sessionmaker()
    async with factory() as session:
        event = await _claim_specific(session, event_id)
        if event is None:
            return False
        await _process_event_body(session, event)
        return True


async def _process_event_body(
    session: AsyncSession, event: dict[str, Any]
) -> UUID:
    """Shared end-to-end pipeline for a claimed event row.

    Extracted from :func:`process_one` so :func:`process_specific` can
    reuse the same routing → extract → diff → validate → apply →
    publish flow without copying it. Caller owns the session and must
    have already claimed the row (``FOR UPDATE``).
    """
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

        current_facts = await load_current_facts(session, property_id)
        stammdaten_snapshot = await _load_event_stammdaten(session, property_id)
        validated_plan, rejections = validate(
            plan,
            event={
                "source": event["source"],
                "metadata": event.get("metadata") or {},
                "stammdaten": stammdaten_snapshot,
            },
            current_facts=current_facts,
        )

        # Semantic auditor — Pioneer reads the canonical property file
        # and the new event side-by-side, surfaces every claim that
        # contradicts what's already on file. Catches contradictions
        # the rule-based validator wasn't pre-coded for. Failures here
        # never block the pipeline: if Pioneer is unreachable we keep
        # the rule-based verdict and move on.
        try:
            property_file_md = await render_markdown(session, property_id)
            semantic = await semantic_validate(
                property_file_markdown=property_file_md,
                event_body=event["raw_content"],
                event_source=event["source"],
                proposed_facts=[
                    {
                        "section": d.section,
                        "field": d.field,
                        "value": d.value,
                        "confidence": d.confidence,
                    }
                    for d in validated_plan.decisions
                ],
            )
            if semantic.has_contradictions:
                log.info(
                    "worker.semantic_rejections",
                    event_id=str(event_id),
                    count=len(semantic.rejections),
                )
                rejections = list(rejections) + list(semantic.rejections)
                # Drop validated decisions whose (section, field) the
                # auditor flagged as a hard contradiction. Soft ones
                # stay in the plan; the rejection ride-along is the
                # human-review trail.
                hard_keys = {
                    (r.section, r.field)
                    for r in semantic.rejections
                    if not r.needs_review
                }
                if hard_keys:
                    validated_plan = type(validated_plan)(
                        decisions=[
                            d
                            for d in validated_plan.decisions
                            if (d.section, d.field) not in hard_keys
                        ],
                        skipped=validated_plan.skipped,
                    )
        except pioneer_llm.PioneerUnavailable as exc:
            log.warning(
                "worker.semantic_skipped",
                event_id=str(event_id),
                error=str(exc)[:200],
            )

        hard_rejections = [r for r in rejections if not r.needs_review]
        needs_review_uncertainties = [
            _needs_review_to_uncertainty(r) for r in rejections if r.needs_review
        ]
        if hard_rejections:
            await persist_rejections(
                session,
                event_id=event_id,
                property_id=property_id,
                building_id=None,
                liegenschaft_id=None,
                rejections=hard_rejections,
            )

        uncertainty_items: list[dict[str, Any]] = (
            list(result.uncertain) + needs_review_uncertainties
        )
        uncertainties_written = 0
        if uncertainty_items:
            uncertainties_written = await apply_uncertainties(
                session,
                event_id=event_id,
                property_id=property_id,
                items=uncertainty_items,
            )

        written = await apply_plan(
            session,
            property_id=property_id,
            source_event_id=event_id,
            plan=validated_plan,
            trigger_summary=result.summary,
        )

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
            # Phase 12 — re-render after the activity row lands so the
            # cached markdown's "Activity" section reflects the just-
            # processed event. Cheap when the applier already
            # propagated for this property in the same transaction
            # (renderer reads facts table, idempotent).
            await propagate_after_fact_write(
                session,
                property_id=property_id,
                trigger_event_id=event_id,
                trigger_summary=result.summary,
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
            uncertainties_written=uncertainties_written,
            hard_rejections=len(hard_rejections),
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
