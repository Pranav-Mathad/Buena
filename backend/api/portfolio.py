"""Portfolio-level views: banner + summary counts for the dashboard."""

from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

import structlog
from fastapi import APIRouter, Depends
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
    """Dashboard counts used in the portfolio header."""

    properties: int
    pending_signals: int
    resolved_signals: int
    pending_portfolio_signals: int


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
    """Return quick dashboard counts — properties, signals, portfolio signals."""
    props = (
        await session.execute(text("SELECT COUNT(*) FROM properties"))
    ).scalar_one()
    pending = (
        await session.execute(
            text("SELECT COUNT(*) FROM signals WHERE status = 'pending'")
        )
    ).scalar_one()
    resolved = (
        await session.execute(
            text("SELECT COUNT(*) FROM signals WHERE status = 'resolved'")
        )
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
    log.info(
        "portfolio.summary",
        properties=int(props),
        pending=int(pending),
        resolved=int(resolved),
    )
    return PortfolioSummary(
        properties=int(props),
        pending_signals=int(pending),
        resolved_signals=int(resolved),
        pending_portfolio_signals=int(portfolio),
    )
