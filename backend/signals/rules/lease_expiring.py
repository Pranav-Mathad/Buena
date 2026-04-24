"""Rule 2 — ``lease_expiring``.

Fires when a property's ``lease.end_date`` fact parses to a date within the
next 60 days. The window is intentionally configurable so the demo can
widen it when no real lease is imminent (``LEASE_WINDOW_DAYS``).
"""

from __future__ import annotations

import re
from datetime import date, datetime, timedelta, timezone

import structlog
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from backend.signals.types import SignalCandidate

log = structlog.get_logger(__name__)

LEASE_WINDOW_DAYS = 60

_DATE_RE = re.compile(r"(20\d{2}-\d{2}-\d{2})")


def _parse_end_date(value: str) -> date | None:
    """Pull an ISO-8601 date out of a free-form ``lease.end_date`` fact value."""
    match = _DATE_RE.search(value)
    if not match:
        return None
    try:
        return datetime.strptime(match.group(1), "%Y-%m-%d").date()
    except ValueError:
        return None


async def evaluate(session: AsyncSession) -> list[SignalCandidate]:
    """Return one candidate per property with a lease expiring in the window."""
    today = datetime.now(timezone.utc).date()
    horizon = today + timedelta(days=LEASE_WINDOW_DAYS)

    result = await session.execute(
        text(
            """
            SELECT f.property_id, p.name AS property_name,
                   f.id AS fact_id, f.source_event_id, f.value
            FROM facts f
            JOIN properties p ON p.id = f.property_id
            WHERE f.section = 'lease'
              AND f.field = 'end_date'
              AND f.superseded_by IS NULL
            """
        )
    )

    candidates: list[SignalCandidate] = []
    for row in result.all():
        end = _parse_end_date(row.value or "")
        if end is None:
            continue
        days_left = (end - today).days
        if days_left > LEASE_WINDOW_DAYS or days_left < 0:
            continue

        severity = "high" if days_left <= 21 else "medium"
        context_excerpt = (
            f"Current end date: {end.isoformat()} ({days_left} days away).\n"
            f"Raw fact value: {row.value}"
        )
        candidates.append(
            SignalCandidate(
                type="lease_expiring",
                severity=severity,
                property_id=row.property_id,
                message=(
                    f"Lease on {row.property_name} expires in {days_left} days "
                    f"({end.isoformat()}) — renewal decision needed."
                ),
                evidence=[
                    {
                        "event_id": str(row.source_event_id or ""),
                        "fact_id": str(row.fact_id),
                    }
                ],
                context_excerpt=context_excerpt,
                action_hint={
                    "type": "owner_notification",
                    "subtype": "lease_renewal_proposal",
                    "property_name": row.property_name,
                    "end_date": end.isoformat(),
                    "days_left": days_left,
                },
            )
        )

    log.info("rule.lease_expiring", candidates=len(candidates))
    return candidates
