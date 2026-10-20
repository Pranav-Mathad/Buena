"""Settings surface: approval-rate statistics."""

from __future__ import annotations

from typing import Any

import structlog
from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from backend.db.session import get_session
from backend.services.approval_stats import compute_learning, snapshot_to_json

router = APIRouter(prefix="/settings", tags=["settings"])
log = structlog.get_logger(__name__)


class LearningStatsOut(BaseModel):
    """Response for ``GET /settings/learning``."""

    sample_size: int
    top_priority: str | None
    trend_line: str
    signal_types: list[dict[str, Any]]


@router.get("/learning", response_model=LearningStatsOut)
async def learning_dashboard(
    session: AsyncSession = Depends(get_session),
) -> LearningStatsOut:
    """Return approval rates and priority weights per signal type."""
    snapshot = await compute_learning(session)
    return LearningStatsOut(**snapshot_to_json(snapshot))
