"""Async event worker: the critical path.

One tick of :func:`process_one` claims the oldest unprocessed event with
``SELECT ... FOR UPDATE SKIP LOCKED`` and runs it through
:func:`_process_event_body`: route (if the event has no property), extract,
diff against current facts, rule validation, semantic check, store review
items, apply, add the activity entry, stamp ``processed_at`` and commit, then
publish an SSE notification. Each step is its own function below.

The scheduler calls :func:`process_batch` on a tight loop. Postgres is the
queue; there is no separate broker such as Redis or Kafka. SKIP LOCKED keeps
concurrent workers safe if we ever scale horizontally.
"""

from __future__ import annotations

import time
from collections.abc import Sequence
from typing import Any
from uuid import UUID

import structlog
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

# Importing the constraints package registers every constraint in
# the validator's REGISTRY at module load. The validator only uses
# whatever is registered; silent omission is the failure mode we'd
# never want, so this import is non-optional.
import backend.pipeline.constraints  # noqa: F401
from backend.db.session import get_sessionmaker
from backend.pipeline.applier import apply as apply_plan
from backend.pipeline.applier import apply_uncertainties
from backend.pipeline.differ import DiffPlan, diff, load_current_facts
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
from backend.services.llm import ExtractionResult, LLMNotConfiguredError, LLMUnavailableError


def _needs_review_to_uncertainty(rejection: Rejection) -> dict[str, Any]:
    """Map a validator ``needs_review`` Rejection into an uncertainty item.

    The constraint emitted "this proposal *might* be valid but needs a
    human to confirm"; that's exactly the uncertainty inbox semantic,
    so we land it there instead of in ``rejected_updates``.
    """
    return {
        "observation": (
            f"proposed {rejection.section}.{rejection.field} = {rejection.proposed_value!r}"
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
    rejected even when no fact row exists yet. Empty dicts are safe:
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
    # The sample-dataset loader (``connectors.buena_loader``) stores
    # floor count as ``etagen`` in buildings.metadata; the legacy seed
    # path uses ``floors``. Read both so either ingest path works.
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


async def _claim_specific(session: AsyncSession, event_id: UUID) -> dict[str, Any] | None:
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


async def _process_event_body(session: AsyncSession, event: dict[str, Any]) -> UUID:
    """Run one claimed event through the pipeline, in a single transaction.

    Shared by :func:`process_one` and :func:`process_specific`. The caller owns
    the session and has already locked the row (``FOR UPDATE``). Any exception
    rolls the transaction back and records the error on the event in a fresh
    session, so the lock is released and the worker moves on.
    """
    start = time.perf_counter()
    event_id: UUID = event["id"]
    property_id: UUID | None = event["property_id"]
    try:
        property_id = property_id or await _route(session, event)
        if property_id is None:
            await _mark_processed(session, event_id, property_id=None, error="unrouted")
            await session.commit()
            return event_id

        result = await _extract(session, event, property_id)
        diff_plan = await diff(
            session,
            property_id=property_id,
            event_source=event["source"],
            proposals=result.facts_to_update,
        )
        plan, rejections = await _validate(session, event, property_id, diff_plan)
        plan, rejections = await _semantic_check(session, event, property_id, plan, rejections)
        hard_rejections, uncertainties_written = await _record_review_items(
            session, event_id, property_id, result, rejections
        )
        written = await apply_plan(
            session,
            property_id=property_id,
            source_event_id=event_id,
            plan=plan,
            trigger_summary=result.summary,
        )
        await _record_activity(session, event_id, property_id, result, len(diff_plan.decisions))
        await _mark_processed(session, event_id, property_id=property_id)
        await session.commit()

        log.info(
            "worker.processed",
            event_id=str(event_id),
            property_id=str(property_id),
            category=result.category,
            facts_written=written,
            uncertainties_written=uncertainties_written,
            hard_rejections=hard_rejections,
            extractor=result.source,
            latency_ms=round((time.perf_counter() - start) * 1000, 1),
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
    except Exception as exc:  # noqa: BLE001 (swallow, mark event, keep worker alive)
        await session.rollback()
        log.exception("worker.error", event_id=str(event_id))
        async with get_sessionmaker()() as fresh:
            await _mark_processed(fresh, event_id, property_id=property_id, error=str(exc)[:500])
            await fresh.commit()
        return event_id


async def _route(session: AsyncSession, event: dict[str, Any]) -> UUID | None:
    """Match an event without a property to one by alias; ``None`` if nothing matches."""
    match = await route(session, event["raw_content"])
    if match is None:
        log.info("worker.unrouted", event_id=str(event["id"]))
        return None
    return match.property_id


async def _extract(
    session: AsyncSession, event: dict[str, Any], property_id: UUID
) -> ExtractionResult:
    """Extract facts, giving the model the property's name and current file excerpt."""
    return await extract(
        property_name=await _property_name(session, property_id),
        current_context_excerpt=await _context_excerpt(session, property_id),
        source=event["source"],
        raw_content=event["raw_content"],
    )


async def _validate(
    session: AsyncSession, event: dict[str, Any], property_id: UUID, plan: DiffPlan
) -> tuple[DiffPlan, list[Rejection]]:
    """Apply the rule constraints against current facts and master data."""
    validated, rejections = validate(
        plan,
        event={
            "source": event["source"],
            "metadata": event.get("metadata") or {},
            "stammdaten": await _load_event_stammdaten(session, property_id),
        },
        current_facts=await load_current_facts(session, property_id),
    )
    return validated, list(rejections)


async def _semantic_check(
    session: AsyncSession,
    event: dict[str, Any],
    property_id: UUID,
    plan: DiffPlan,
    rejections: list[Rejection],
) -> tuple[DiffPlan, list[Rejection]]:
    """Ask the LLM for contradictions between the event and the current file.

    Covers fields no rule constraint was written for. It never blocks the
    pipeline: without a provider (logged once at startup) or when the call
    fails, the rule-based verdict stands.
    """
    try:
        semantic = await semantic_validate(
            property_file_markdown=await render_markdown(session, property_id),
            event_body=event["raw_content"],
            event_source=event["source"],
            proposed_facts=[
                {
                    "section": d.section,
                    "field": d.field,
                    "value": d.value,
                    "confidence": d.confidence,
                }
                for d in plan.decisions
            ],
        )
    except LLMNotConfiguredError:
        return plan, rejections
    except LLMUnavailableError as exc:
        log.warning("worker.semantic_failed", event_id=str(event["id"]), error=str(exc)[:200])
        return plan, rejections
    if not semantic.has_contradictions:
        return plan, rejections
    log.info(
        "worker.semantic_rejections", event_id=str(event["id"]), count=len(semantic.rejections)
    )
    return (
        _drop_hard_contradictions(plan, semantic.rejections),
        rejections + list(semantic.rejections),
    )


def _drop_hard_contradictions(plan: DiffPlan, contradictions: Sequence[Rejection]) -> DiffPlan:
    """Remove decisions whose slot has a hard contradiction; soft ones stay.

    Soft contradictions keep their decision in the plan and travel on as
    review items, so a human sees both the written fact and the doubt.
    """
    hard_keys = {(r.section, r.field) for r in contradictions if not r.needs_review}
    if not hard_keys:
        return plan
    return DiffPlan(
        decisions=[d for d in plan.decisions if (d.section, d.field) not in hard_keys],
        skipped=plan.skipped,
    )


async def _record_review_items(
    session: AsyncSession,
    event_id: UUID,
    property_id: UUID,
    result: ExtractionResult,
    rejections: list[Rejection],
) -> tuple[int, int]:
    """Store hard rejections and uncertainties; return how many of each."""
    hard = [r for r in rejections if not r.needs_review]
    if hard:
        await persist_rejections(
            session,
            event_id=event_id,
            property_id=property_id,
            building_id=None,
            liegenschaft_id=None,
            rejections=hard,
        )
    items: list[dict[str, Any]] = list(result.uncertain) + [
        _needs_review_to_uncertainty(r) for r in rejections if r.needs_review
    ]
    written = 0
    if items:
        written = await apply_uncertainties(
            session, event_id=event_id, property_id=property_id, items=items
        )
    return len(hard), written


async def _record_activity(
    session: AsyncSession,
    event_id: UUID,
    property_id: UUID,
    result: ExtractionResult,
    proposed_count: int,
) -> None:
    """Add the event's one-line summary to the activity section and re-render.

    One activity entry per event: NOT EXISTS makes a replayed or recovered
    event a no-op here, as the differ already makes it for extracted facts.
    """
    if not result.summary:
        return
    await session.execute(
        text(
            """
            INSERT INTO facts
                (property_id, section, field, value, source_event_id,
                 confidence, valid_from)
            SELECT :pid, 'activity', :field, :value, :eid, :conf, now()
            WHERE NOT EXISTS (
                SELECT 1 FROM facts
                WHERE source_event_id = :eid AND section = 'activity'
            )
            """
        ),
        {
            "pid": property_id,
            "field": f"event_{event_id}",
            "value": result.summary,
            "eid": event_id,
            "conf": max(0.5, min(0.95, 0.7 + proposed_count * 0.05)),
        },
    )
    # Re-render so the materialized file's activity section includes this
    # event; cheap when the applier already propagated in this transaction.
    await propagate_after_fact_write(
        session,
        property_id=property_id,
        trigger_event_id=event_id,
        trigger_summary=result.summary,
    )


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
