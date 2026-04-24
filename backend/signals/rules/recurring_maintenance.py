"""Rule 1 — ``recurring_maintenance`` (the demo hero).

Fires when a single property has ≥3 maintenance facts containing a shared
issue keyword (``heat``, ``boiler``, ``water``, ``leak``) within the past
120 days. This is the sustained-failure pattern judges recognize at a
glance: three heating complaints on the same unit → something is about to
break.
"""

from __future__ import annotations

import structlog
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from backend.signals.types import SignalCandidate

log = structlog.get_logger(__name__)

WINDOW_DAYS = 120
MIN_OCCURRENCES = 3

# (keyword regex, topic label) — topic is what we key the dedupe on.
KEYWORD_TOPICS: list[tuple[str, str]] = [
    (r"heat|boiler|radiator|hot water", "heating"),
    (r"leak|water damage|p-?trap|drip", "water"),
    (r"electric|wiring|short circuit|breaker", "electrical"),
]


async def evaluate(session: AsyncSession) -> list[SignalCandidate]:
    """Scan every property's maintenance history and emit candidates."""
    candidates: list[SignalCandidate] = []
    for pattern, topic in KEYWORD_TOPICS:
        result = await session.execute(
            text(
                """
                SELECT f.property_id, p.name AS property_name,
                       COUNT(*) AS occurrences,
                       ARRAY_AGG(f.id::text ORDER BY f.created_at) AS fact_ids,
                       ARRAY_AGG(f.source_event_id::text ORDER BY f.created_at)
                         AS event_ids,
                       ARRAY_AGG(f.value ORDER BY f.created_at) AS values
                FROM facts f
                JOIN properties p ON p.id = f.property_id
                WHERE f.section = 'maintenance'
                  AND f.superseded_by IS NULL
                  AND f.created_at >= now() - (:window || ' days')::interval
                  AND (f.value ~* :pattern OR f.field ~* :pattern)
                GROUP BY f.property_id, p.name
                HAVING COUNT(*) >= :min_count
                """
            ),
            {
                "pattern": pattern,
                "window": str(WINDOW_DAYS),
                "min_count": MIN_OCCURRENCES,
            },
        )
        for row in result.all():
            occurrences = int(row.occurrences)
            excerpt_parts = [v for v in (row.values or []) if v]
            context_excerpt = "\n".join(f"- {v}" for v in excerpt_parts[:5])
            evidence = [
                {"event_id": ev, "fact_id": fid}
                for ev, fid in zip(
                    row.event_ids or [],
                    row.fact_ids or [],
                    strict=False,
                )
                if ev or fid
            ]
            candidates.append(
                SignalCandidate(
                    type="recurring_maintenance",
                    severity="urgent",
                    property_id=row.property_id,
                    message=(
                        f"{occurrences} {topic} incidents on {row.property_name} "
                        f"in the last {WINDOW_DAYS} days — sustained failure pattern."
                    ),
                    evidence=evidence,
                    context_excerpt=context_excerpt,
                    action_hint={
                        "type": "dispatch_contractor",
                        "topic": topic,
                        "property_name": row.property_name,
                        "occurrences": occurrences,
                    },
                )
            )

    log.info("rule.recurring_maintenance", candidates=len(candidates))
    return candidates
