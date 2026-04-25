"""Apply a :class:`DiffPlan` to the facts table.

Writes new fact rows and flags any superseded predecessor. The fact table's
``superseded_by`` column points at the replacement row (bottom-up chain) —
this is what :func:`render_markdown` relies on via ``superseded_by IS NULL``.
"""

from __future__ import annotations

from uuid import UUID

import structlog
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from backend.pipeline.differ import DiffPlan

log = structlog.get_logger(__name__)


async def apply(
    session: AsyncSession,
    *,
    property_id: UUID,
    source_event_id: UUID,
    plan: DiffPlan,
) -> int:
    """Persist every decision in ``plan``; return how many facts were written."""
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

    log.info(
        "applier.done",
        property_id=str(property_id),
        event_id=str(source_event_id),
        facts_written=written,
    )
    return written
