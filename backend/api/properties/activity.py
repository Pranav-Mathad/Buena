"""Recent activity feed for one property."""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

import structlog
from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from backend.db.session import get_session

router = APIRouter()
log = structlog.get_logger(__name__)


class ActivityItem(BaseModel):
    """Activity feed row: a processed event + its one-line summary."""

    event_id: UUID
    source: str
    received_at: datetime
    processed_at: datetime | None
    summary: str | None
    facts_written: int


@router.get("/{property_id}/activity", response_model=list[ActivityItem])
async def property_activity(
    property_id: UUID,
    limit: int = Query(default=50, ge=1, le=200),
    session: AsyncSession = Depends(get_session),
) -> list[ActivityItem]:
    """Return the ``limit`` most recent events + extraction summary for this property."""
    result = await session.execute(
        text(
            """
            SELECT e.id, e.source, e.received_at, e.processed_at,
                   sum.value AS summary,
                   (SELECT COUNT(*) FROM facts f WHERE f.source_event_id = e.id) AS facts_written
            FROM events e
            LEFT JOIN LATERAL (
                SELECT value FROM facts
                WHERE source_event_id = e.id AND section = 'activity'
                ORDER BY created_at ASC LIMIT 1
            ) sum ON TRUE
            WHERE e.property_id = :pid
            ORDER BY e.received_at DESC
            LIMIT :lim
            """
        ),
        {"pid": property_id, "lim": limit},
    )
    items = [
        ActivityItem(
            event_id=row.id,
            source=row.source,
            received_at=row.received_at,
            processed_at=row.processed_at,
            summary=row.summary,
            facts_written=int(row.facts_written or 0),
        )
        for row in result.all()
    ]
    log.info("properties.activity", property_id=str(property_id), count=len(items))
    return items
