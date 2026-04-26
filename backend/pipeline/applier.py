"""Apply a :class:`DiffPlan` to the facts table.

Writes new fact rows and flags any superseded predecessor. The fact table's
``superseded_by`` column points at the replacement row (bottom-up chain) —
this is what :func:`render_markdown` relies on via ``superseded_by IS NULL``.

Phase 9 Step 9.1 expands the applier surface: alongside committing
facts, it persists a parallel list of uncertainty items (from the
extractor's ``uncertain[]`` field, the confidence-floor demotion path,
and the validator's ``needs_review`` verdicts). Both writes live
in the same transaction so a row in ``uncertainty_events`` is always
linked to a real ``events`` row.
"""

from __future__ import annotations

from typing import Any
from uuid import UUID

import structlog
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from backend.pipeline.differ import DiffPlan
from backend.pipeline.materializer import propagate_after_fact_write

log = structlog.get_logger(__name__)


async def apply(
    session: AsyncSession,
    *,
    property_id: UUID,
    source_event_id: UUID,
    plan: DiffPlan,
    trigger_summary: str | None = None,
) -> int:
    """Persist every decision in ``plan``; return how many facts were written.

    Phase 12 — after the write loop, fan out a materialization update
    for the property scope. The propagation runs in the same session
    so it shares this transaction: if rendering fails, every fact
    write rolls back too, which preserves the "files are never stale
    relative to facts" invariant.
    """
    written = 0
    for decision in plan.decisions:
        result = await session.execute(
            text(
                """
                INSERT INTO facts
                    (property_id, section, field, value, source_event_id,
                     confidence, valid_from)
                VALUES
                    (:pid, :section, :field, :value, :eid, :conf, now())
                RETURNING id
                """
            ),
            {
                "pid": property_id,
                "section": decision.section,
                "field": decision.field,
                "value": decision.value,
                "eid": source_event_id,
                "conf": decision.confidence,
            },
        )
        new_id: UUID = result.scalar_one()
        written += 1

        if decision.supersedes_id is not None:
            await session.execute(
                text(
                    """
                    UPDATE facts
                    SET superseded_by = :new_id, valid_to = now()
                    WHERE id = :old_id AND superseded_by IS NULL
                    """
                ),
                {"new_id": new_id, "old_id": decision.supersedes_id},
            )

    if written > 0:
        # Trigger summary defaults to the first decision's value when the
        # caller didn't pass one — gives a human-readable label on the
        # ``/property_files/changes`` feed without leaking too much.
        summary = trigger_summary
        if summary is None and plan.decisions:
            first = plan.decisions[0]
            summary = f"{first.section}.{first.field}: {first.value}"
        await propagate_after_fact_write(
            session,
            property_id=property_id,
            trigger_event_id=source_event_id,
            trigger_summary=summary,
        )

    log.info(
        "applier.done",
        property_id=str(property_id),
        event_id=str(source_event_id),
        facts_written=written,
    )
    return written


async def apply_uncertainties(
    session: AsyncSession,
    *,
    event_id: UUID,
    property_id: UUID | None,
    items: list[dict[str, Any]],
    building_id: UUID | None = None,
    liegenschaft_id: UUID | None = None,
) -> int:
    """Persist uncertainty items into ``uncertainty_events``.

    Each item is expected to carry ``observation`` (required),
    ``reason_uncertain`` (required), ``relevant_section``, plus
    optional ``hypothesis`` and ``relevant_field``. The ``source``
    field on each item gets passed through; defaults to ``extractor``
    when missing so the inbox can show provenance.

    Caller commits the surrounding transaction. Returns the number of
    rows actually inserted (items with empty ``observation`` are
    skipped to keep the inbox honest).
    """
    written = 0
    for item in items:
        observation = str(item.get("observation") or "").strip()
        if not observation:
            continue
        await session.execute(
            text(
                """
                INSERT INTO uncertainty_events (
                  event_id, property_id, building_id, liegenschaft_id,
                  relevant_section, relevant_field,
                  observation, hypothesis, reason_uncertain, source, status
                ) VALUES (
                  :eid, :pid, :bid, :lid,
                  :section, :field,
                  :observation, :hypothesis, :reason, :src, 'open'
                )
                """
            ),
            {
                "eid": event_id,
                "pid": property_id,
                "bid": building_id,
                "lid": liegenschaft_id,
                "section": str(item.get("relevant_section") or "") or None,
                "field": str(item.get("relevant_field") or "") or None,
                "observation": observation[:1000],
                "hypothesis": (
                    str(item.get("hypothesis") or "")[:1000] or None
                ),
                "reason": str(item.get("reason_uncertain") or "(no reason given)")[:500],
                "src": str(item.get("source") or "extractor"),
            },
        )
        written += 1
    if written:
        log.info(
            "applier.uncertainties_done",
            event_id=str(event_id),
            property_id=str(property_id) if property_id else None,
            written=written,
        )
    return written
