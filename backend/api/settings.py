"""Settings surface — security badge, learning dashboard, regulation trigger."""

from __future__ import annotations

from datetime import datetime
from typing import Any

import structlog
from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from backend.db.session import get_session
from backend.services.aikido import get_badge
from backend.services.pioneer import compute_learning, snapshot_to_json
from backend.services.tavily import watch_regulations
from backend.signals.evaluator import evaluate_all

router = APIRouter(prefix="/settings", tags=["settings"])
log = structlog.get_logger(__name__)


class SecurityBadgeOut(BaseModel):
    """Response for ``GET /settings/security``."""

    status: str
    scanner: str
    last_scan_at: datetime | None
    commit_sha: str | None
    critical: int
    high: int
    medium: int
    details_url: str | None
    source: str


class LearningStatsOut(BaseModel):
    """Response for ``GET /settings/learning``."""

    sample_size: int
    top_priority: str | None
    trend_line: str
    signal_types: list[dict[str, Any]]


class WatchResponse(BaseModel):
    """Response for ``POST /settings/regulation_watch``."""

    ingested_events: int
    new_signals: int


@router.get("/security", response_model=SecurityBadgeOut)
async def security_badge() -> SecurityBadgeOut:
    """Return the Aikido security-scan badge rendered by the Settings page."""
    badge = await get_badge()
    return SecurityBadgeOut(
        status=badge.status,
        scanner=badge.scanner,
        last_scan_at=badge.last_scan_at,
        commit_sha=badge.commit_sha,
        critical=badge.critical,
        high=badge.high,
        medium=badge.medium,
        details_url=badge.details_url,
        source=badge.source,
    )


@router.get("/learning", response_model=LearningStatsOut)
async def learning_dashboard(
    session: AsyncSession = Depends(get_session),
) -> LearningStatsOut:
    """Return the Pioneer / Fastino-compatible approval-rate snapshot."""
    snapshot = await compute_learning(session)
    return LearningStatsOut(**snapshot_to_json(snapshot))


@router.post("/regulation_watch", response_model=WatchResponse)
async def run_regulation_watch(
    session: AsyncSession = Depends(get_session),
) -> WatchResponse:
    """Manually fire the Tavily regulation watcher for demo determinism.

    Runs the watcher, then evaluates the rule set once so any fresh
    regulation events that warrant signals are visible in the inbox
    immediately (instead of waiting for the 30s scheduler tick).
    """
    inserted = await watch_regulations()
    created = await evaluate_all(session)
    await session.commit()
    log.info(
        "settings.regulation_watch",
        inserted=inserted,
        signals_created=created,
    )
    return WatchResponse(ingested_events=inserted, new_signals=created)
