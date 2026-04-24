"""Rule 4 — ``regulation_change``.

Fires when the Tavily regulation watcher ingests a ``web`` event tagged
``metadata.regulation=true`` that hasn't been signaled yet. Signals are
portfolio-level (``property_id=NULL``) — the UI renders them on the portfolio
view next to the other `cross_property_pattern` banners.
"""

from __future__ import annotations

import structlog
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from backend.signals.types import SignalCandidate

log = structlog.get_logger(__name__)

WINDOW_DAYS = 14  # only surface regulation changes from the last two weeks


async def evaluate(session: AsyncSession) -> list[SignalCandidate]:
    """Scan recent regulation events and emit one candidate per novel headline."""
    result = await session.execute(
        text(
            """
            SELECT id, source_ref,
                   COALESCE(metadata->>'headline', left(raw_content, 120))
                       AS headline,
                   received_at,
                   metadata
            FROM events
            WHERE source = 'web'
              AND (metadata->>'regulation')::boolean IS TRUE
              AND received_at >= now() - (:window || ' days')::interval
            ORDER BY received_at DESC
            """
        ),
        {"window": str(WINDOW_DAYS)},
    )

    candidates: list[SignalCandidate] = []
    for row in result.all():
        headline = row.headline or "Regulation update"
        context_excerpt = (
            f"Headline: {headline}\n"
            f"Source: Tavily web watcher ({row.source_ref})\n"
            f"Ingested: {row.received_at.isoformat()}"
        )
        candidates.append(
            SignalCandidate(
                type="regulation_change",
                severity="medium",
                property_id=None,
                message=(
                    f"Regulation update detected: {headline} — "
                    "review for portfolio impact."
                ),
                evidence=[{"event_id": str(row.id), "fact_id": ""}],
                context_excerpt=context_excerpt,
                action_hint={
                    "type": "regulation_review",
                    "subtype": str(row.source_ref),
                    "headline": headline,
                    "source_ref": row.source_ref,
                },
            )
        )

    log.info("rule.regulation_change", candidates=len(candidates))
    return candidates
