"""Admin surface — unrouted inbox, failed events, replay control.

Step 3 ships ``GET /admin/unrouted`` so backfilled events that lacked
the IDs needed for routing surface as a real product UX (per Phase 8
plan: this is *core* product, not demo polish). Steps 7 and 9 expand
the surface to include incremental-cursor controls and admin
overrides.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

import structlog
from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from backend.db.session import get_session

router = APIRouter(prefix="/admin", tags=["admin"])
log = structlog.get_logger(__name__)


class UnroutedEvent(BaseModel):
    """One row of the unrouted-inbox listing."""

    event_id: UUID
    source: str
    source_ref: str | None = None
    received_at: datetime
    snippet: str = Field(
        ...,
        description="First ~120 chars of raw_content for human triage.",
    )
    metadata: dict[str, Any]
    suggested_alias: str | None = Field(
        default=None,
        description=(
            "Best guess alias to look up — surfaces metadata.eh_id / mie_id / "
            "invoice_ref so the operator knows what reference was tried."
        ),
    )


class UnroutedResponse(BaseModel):
    """Response envelope for ``GET /admin/unrouted``."""

    total: int
    by_source: dict[str, int]
    events: list[UnroutedEvent]


def _suggested_alias(metadata: dict[str, Any]) -> str | None:
    """Surface the strongest ID that the routing failed to resolve."""
    for key in ("eh_id", "mie_id", "invoice_ref", "buena_referenz_id"):
        value = metadata.get(key)
        if value:
            return str(value)
    return None


@router.get("/unrouted", response_model=UnroutedResponse)
async def list_unrouted(
    source: str | None = Query(
        default=None,
        description="Filter by event source (e.g. 'bank' or 'invoice').",
    ),
    limit: int = Query(default=100, ge=1, le=500),
    session: AsyncSession = Depends(get_session),
) -> UnroutedResponse:
    """List events with ``property_id IS NULL`` for human triage.

    Includes a per-source breakdown so the operator can see at a glance
    where the routing miss rate concentrates (e.g. shared-service bank
    payments without an EH-/MIE- reference).
    """
    breakdown_rows = (
        await session.execute(
            text(
                """
                SELECT source, COUNT(*) AS n
                FROM events
                WHERE property_id IS NULL
                GROUP BY source
                ORDER BY n DESC
                """
            )
        )
    ).all()
    by_source = {row.source: int(row.n) for row in breakdown_rows}

    params: dict[str, Any] = {"lim": limit}
    where = "WHERE property_id IS NULL"
    if source:
        where += " AND source = :source"
        params["source"] = source

    rows = (
        await session.execute(
            text(
                f"""
                SELECT id, source, source_ref, received_at, raw_content, metadata
                FROM events
                {where}
                ORDER BY received_at DESC
                LIMIT :lim
                """
            ),
            params,
        )
    ).all()

    events = [
        UnroutedEvent(
            event_id=r.id,
            source=r.source,
            source_ref=r.source_ref,
            received_at=r.received_at,
            snippet=(r.raw_content or "")[:160],
            metadata=dict(r.metadata or {}),
            suggested_alias=_suggested_alias(dict(r.metadata or {})),
        )
        for r in rows
    ]
    log.info(
        "admin.unrouted",
        total=sum(by_source.values()),
        source_filter=source,
        returned=len(events),
    )
    return UnroutedResponse(
        total=sum(by_source.values()),
        by_source=by_source,
        events=events,
    )
