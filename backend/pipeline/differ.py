"""Reconcile proposed facts against the current fact table.

Resolution rules (KEYSTONE Part V):
1. **Source precedence:** ``pdf > erp > email > slack > web > debug`` — a
   proposed fact from a weaker source cannot displace a stronger one unless
   it raises confidence materially (+0.1).
2. **Confidence:** higher confidence wins between equal-source facts.
3. **Recency:** newer wins between otherwise-equal facts.

The differ does not mutate state; it returns a plan that :mod:`applier` acts on.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import UUID

import structlog
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

log = structlog.get_logger(__name__)


SOURCE_RANK: dict[str, int] = {
    "pdf": 5,
    "erp": 4,
    "email": 3,
    "slack": 2,
    "web": 1,
    "debug": 0,
}


@dataclass(frozen=True)
class FactDecision:
    """Instruction for a single proposed fact."""

    section: str
    field: str
    value: str
    confidence: float
    supersedes_id: UUID | None
    # Diagnostic only — why this decision was made.
    reason: str


@dataclass(frozen=True)
class DiffPlan:
    """Batch of :class:`FactDecision` for one event."""

    decisions: list[FactDecision]
    skipped: list[tuple[str, str]]  # (section.field, reason)


async def _current_facts(
    session: AsyncSession, property_id: UUID
) -> dict[tuple[str, str], dict[str, Any]]:
    """Return ``{(section, field): {id, value, confidence, source, created_at}}``."""
    result = await session.execute(
        text(
            """
            SELECT f.id, f.section, f.field, f.value, f.confidence, f.created_at,
                   e.source AS source
            FROM facts f
            LEFT JOIN events e ON e.id = f.source_event_id
            WHERE f.property_id = :pid AND f.superseded_by IS NULL
            """
        ),
        {"pid": property_id},
    )
    out: dict[tuple[str, str], dict[str, Any]] = {}
    for row in result.all():
        out[(row.section, row.field)] = {
            "id": row.id,
            "value": row.value,
            "confidence": float(row.confidence),
            "source": row.source or "debug",
            "created_at": row.created_at,
        }
    return out


def _should_replace(
    new_source: str,
    new_confidence: float,
    existing: dict[str, Any],
    new_value: str,
) -> tuple[bool, str]:
    """Apply precedence + confidence + recency to decide replacement."""
    if existing["value"].strip() == new_value.strip():
        return False, "identical_value"

    new_rank = SOURCE_RANK.get(new_source, 0)
    old_rank = SOURCE_RANK.get(existing["source"], 0)

    if new_rank < old_rank and new_confidence < existing["confidence"] + 0.1:
        return False, f"weaker_source ({new_source}<{existing['source']})"

    if new_rank == old_rank and new_confidence < existing["confidence"]:
        return False, "lower_confidence"

    return True, f"replace ({new_source}@{new_confidence:.2f} > {existing['source']}@{existing['confidence']:.2f})"


async def diff(
    session: AsyncSession,
    *,
    property_id: UUID,
    event_source: str,
    proposals: list[dict[str, Any]],
    now: datetime | None = None,  # noqa: ARG001 — reserved for future recency logic
) -> DiffPlan:
    """Compute a :class:`DiffPlan` for the proposed facts."""
    current = await _current_facts(session, property_id)
    decisions: list[FactDecision] = []
    skipped: list[tuple[str, str]] = []

    for proposal in proposals:
        section = str(proposal["section"])
        field = str(proposal["field"])
        value = str(proposal["value"])
        confidence = float(proposal["confidence"])
        key = (section, field)

        if key in current:
            replace, reason = _should_replace(event_source, confidence, current[key], value)
            if not replace:
                skipped.append((f"{section}.{field}", reason))
                continue
            decisions.append(
                FactDecision(
                    section=section,
                    field=field,
                    value=value,
                    confidence=confidence,
                    supersedes_id=current[key]["id"],
                    reason=reason,
                )
            )
        else:
            decisions.append(
                FactDecision(
                    section=section,
                    field=field,
                    value=value,
                    confidence=confidence,
                    supersedes_id=None,
                    reason="new_fact",
                )
            )

    log.info(
        "differ.plan",
        property_id=str(property_id),
        proposed=len(proposals),
        accepted=len(decisions),
        skipped=len(skipped),
    )
    return DiffPlan(decisions=decisions, skipped=skipped)
