"""Portfolio-level views: banner + summary counts for the dashboard."""

from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

import structlog
from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from backend.db.session import get_session

router = APIRouter(prefix="/portfolio", tags=["portfolio"])
log = structlog.get_logger(__name__)


class PortfolioBanner(BaseModel):
    """Top-of-page banner content for the portfolio view."""

    has_signal: bool
    signal_id: UUID | None = None
    severity: str | None = None
    message: str | None = None
    proposed_action_subject: str | None = None
    created_at: datetime | None = None


class PortfolioSummary(BaseModel):
    """Dashboard counts used in the portfolio header.

    Time-bucketed counts (``events_last_hour`` / ``facts_today``) drive
    the ActivityTicker pulse on the home page. They are pollable, cheap and
    purely numeric so the frontend can animate count-ups without
    needing the underlying rows.
    """

    properties: int
    pending_signals: int
    resolved_signals: int
    pending_portfolio_signals: int
    events_last_hour: int = 0
    facts_today: int = 0


@router.get("/banner", response_model=PortfolioBanner)
async def portfolio_banner(
    session: AsyncSession = Depends(get_session),
) -> PortfolioBanner:
    """Return the top-priority portfolio-level pending signal, if any."""
    row = (
        await session.execute(
            text(
                """
                SELECT id, severity, message, proposed_action, created_at
                FROM signals
                WHERE status = 'pending' AND property_id IS NULL
                ORDER BY
                  CASE severity WHEN 'urgent' THEN 0 WHEN 'high' THEN 1
                                WHEN 'medium' THEN 2 ELSE 3 END,
                  created_at DESC
                LIMIT 1
                """
            )
        )
    ).first()
    if row is None:
        return PortfolioBanner(has_signal=False)
    action: dict[str, Any] = row.proposed_action or {}
    subject = action.get("subject") if isinstance(action, dict) else None
    return PortfolioBanner(
        has_signal=True,
        signal_id=row.id,
        severity=row.severity,
        message=row.message,
        proposed_action_subject=subject,
        created_at=row.created_at,
    )


@router.get("/summary", response_model=PortfolioSummary)
async def portfolio_summary(
    session: AsyncSession = Depends(get_session),
) -> PortfolioSummary:
    """Return quick dashboard counts: properties, signals, portfolio signals."""
    props = (await session.execute(text("SELECT COUNT(*) FROM properties"))).scalar_one()
    pending = (
        await session.execute(text("SELECT COUNT(*) FROM signals WHERE status = 'pending'"))
    ).scalar_one()
    resolved = (
        await session.execute(text("SELECT COUNT(*) FROM signals WHERE status = 'resolved'"))
    ).scalar_one()
    portfolio = (
        await session.execute(
            text(
                """
                SELECT COUNT(*) FROM signals
                WHERE status = 'pending' AND property_id IS NULL
                """
            )
        )
    ).scalar_one()
    events_last_hour = (
        await session.execute(
            text(
                """
                SELECT COUNT(*) FROM events
                WHERE received_at > now() - interval '1 hour'
                """
            )
        )
    ).scalar_one()
    facts_today = (
        await session.execute(
            text(
                """
                SELECT COUNT(*) FROM facts
                WHERE created_at >= date_trunc('day', now())
                """
            )
        )
    ).scalar_one()
    log.info(
        "portfolio.summary",
        properties=int(props),
        pending=int(pending),
        resolved=int(resolved),
        events_last_hour=int(events_last_hour),
        facts_today=int(facts_today),
    )
    return PortfolioSummary(
        properties=int(props),
        pending_signals=int(pending),
        resolved_signals=int(resolved),
        pending_portfolio_signals=int(portfolio),
        events_last_hour=int(events_last_hour),
        facts_today=int(facts_today),
    )


class PortfolioActivityItem(BaseModel):
    """One row in the cross-property activity feed."""

    event_id: UUID
    received_at: datetime
    source: str
    text: str
    property_id: UUID | None
    property_name: str | None


@router.get("/activity", response_model=list[PortfolioActivityItem])
async def portfolio_activity(
    limit: int = Query(default=15, ge=1, le=100),
    include_noise: bool = Query(
        default=False,
        description=(
            "When False (default), only events that wrote ≥1 fact OR are "
            "linked to a non-resolved signal are returned. Filters out "
            "routine bank settlements, auto-replies, and other transit-"
            "only ingestion. Set True for an unfiltered audit trail."
        ),
    ),
    session: AsyncSession = Depends(get_session),
) -> list[PortfolioActivityItem]:
    """Return the most recent events across all properties.

    Each row carries a one-line ``text`` summary, derived in priority
    order from: the extractor's ``activity``-section fact, the event's
    ``metadata.subject`` (email), the first 80 chars of raw content,
    or the bare source label as a last resort.

    By default the result is filtered to events that **changed system
    state**: wrote a fact or surfaced a signal. Routine events that
    just transit through the pipeline (bank settlements, auto-replies)
    are hidden so the activity feed reads as a signal feed rather than
    a tail of the events table.
    """
    noise_filter = (
        ""
        if include_noise
        else """
        WHERE EXISTS (
            SELECT 1 FROM facts f WHERE f.source_event_id = e.id
        )
        OR EXISTS (
            SELECT 1 FROM signals s
            WHERE s.property_id = e.property_id
              AND s.status IN ('pending', 'approved')
              AND s.created_at >= e.received_at - interval '1 day'
              AND s.created_at <= e.received_at + interval '7 days'
        )
        """
    )
    rows = (
        await session.execute(
            text(
                f"""
                SELECT e.id AS event_id,
                       COALESCE(e.received_at, e.processed_at) AS received_at,
                       e.source,
                       e.property_id,
                       p.name AS property_name,
                       e.metadata,
                       LEFT(COALESCE(e.raw_content, ''), 200) AS body_snippet,
                       (
                         SELECT value FROM facts
                         WHERE source_event_id = e.id
                           AND section = 'activity'
                         ORDER BY created_at ASC
                         LIMIT 1
                       ) AS activity_fact_value
                FROM events e
                LEFT JOIN properties p ON p.id = e.property_id
                {noise_filter}
                ORDER BY COALESCE(e.received_at, e.processed_at) DESC
                LIMIT :lim
                """
            ),
            {"lim": limit},
        )
    ).all()

    items: list[PortfolioActivityItem] = []
    for r in rows:
        metadata = dict(r.metadata or {})
        subject = metadata.get("subject") if isinstance(metadata, dict) else None
        if r.activity_fact_value:
            txt = str(r.activity_fact_value)
        elif subject:
            txt = str(subject)
        else:
            snippet = str(r.body_snippet or "").replace("\n", " ").strip()
            txt = snippet[:120] if snippet else f"{r.source} event"
        items.append(
            PortfolioActivityItem(
                event_id=r.event_id,
                received_at=r.received_at,
                source=str(r.source),
                text=txt[:160],
                property_id=r.property_id,
                property_name=r.property_name,
            )
        )
    log.info("portfolio.activity", count=len(items))
    return items
