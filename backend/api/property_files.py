"""Materialized property file API.

Phase 12 — exposes the cached, propagation-aware ``property_files``
table as a first-class resource (separate from ``/properties/{id}``
which is the master-record + structured-card surface).

Two endpoints:

- ``GET /property_files/{property_id}`` — full row shape for a single
  property's materialized markdown.
- ``GET /property_files/changes?since=<ISO>&limit=50`` — recent
  regenerations across the portfolio. The frontend (Phase 13) can
  poll this to drive a "live update" feed without subscribing to
  the in-process event bus. Ordering is ``last_rendered_at DESC``;
  ``trigger_*`` columns let consumers attribute each row to the
  fact write that triggered it.
"""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

import structlog
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from backend.db.session import get_session

router = APIRouter(prefix="/property_files", tags=["property_files"])
log = structlog.get_logger(__name__)


class PropertyFileRow(BaseModel):
    """Full materialized row for a property."""

    property_id: UUID
    content_md: str
    fact_count: int
    last_rendered_at: datetime
    trigger_event_id: UUID | None = None
    trigger_scope: str | None = None
    trigger_summary: str | None = None
    generation_version: int


class PropertyFileChangeRow(BaseModel):
    """One entry in the ``/property_files/changes`` feed."""

    property_id: UUID
    property_name: str
    last_rendered_at: datetime
    trigger_scope: str | None = None
    trigger_summary: str | None = None
    trigger_event_id: UUID | None = None


@router.get("/changes", response_model=list[PropertyFileChangeRow])
async def list_changes(
    since: datetime | None = Query(
        default=None,
        description=(
            "ISO timestamp (UTC). Returns rows with "
            "``last_rendered_at > since``. Omit to get the most "
            "recent ``limit`` regenerations regardless of age."
        ),
    ),
    limit: int = Query(default=50, ge=1, le=200),
    session: AsyncSession = Depends(get_session),
) -> list[PropertyFileChangeRow]:
    """Return recent property-file regenerations.

    Phase 13 will use this as the polling target for a "what changed"
    feed; Phase 12 ships only the data layer.
    """
    if since is None:
        rows = (
            await session.execute(
                text(
                    """
                    SELECT pf.property_id,
                           p.name AS property_name,
                           pf.last_rendered_at,
                           pf.trigger_scope,
                           pf.trigger_summary,
                           pf.trigger_event_id
                    FROM property_files pf
                    JOIN properties p ON p.id = pf.property_id
                    ORDER BY pf.last_rendered_at DESC
                    LIMIT :lim
                    """
                ),
                {"lim": limit},
            )
        ).all()
    else:
        rows = (
            await session.execute(
                text(
                    """
                    SELECT pf.property_id,
                           p.name AS property_name,
                           pf.last_rendered_at,
                           pf.trigger_scope,
                           pf.trigger_summary,
                           pf.trigger_event_id
                    FROM property_files pf
                    JOIN properties p ON p.id = pf.property_id
                    WHERE pf.last_rendered_at > :since
                    ORDER BY pf.last_rendered_at DESC
                    LIMIT :lim
                    """
                ),
                {"since": since, "lim": limit},
            )
        ).all()

    log.info(
        "property_files.changes",
        since=since.isoformat() if since else None,
        rows=len(rows),
    )
    return [
        PropertyFileChangeRow(
            property_id=UUID(str(r.property_id)),
            property_name=r.property_name,
            last_rendered_at=r.last_rendered_at,
            trigger_scope=r.trigger_scope,
            trigger_summary=r.trigger_summary,
            trigger_event_id=(
                UUID(str(r.trigger_event_id)) if r.trigger_event_id else None
            ),
        )
        for r in rows
    ]


@router.get("/{property_id}", response_model=PropertyFileRow)
async def get_property_file(
    property_id: UUID,
    session: AsyncSession = Depends(get_session),
) -> PropertyFileRow:
    """Return the materialized row for one property."""
    row = (
        await session.execute(
            text(
                """
                SELECT property_id, content_md, fact_count, last_rendered_at,
                       trigger_event_id, trigger_scope, trigger_summary,
                       generation_version
                FROM property_files
                WHERE property_id = :pid
                """
            ),
            {"pid": property_id},
        )
    ).first()
    if row is None:
        raise HTTPException(
            status_code=404, detail="property file not materialized yet"
        )
    return PropertyFileRow(
        property_id=UUID(str(row.property_id)),
        content_md=row.content_md,
        fact_count=int(row.fact_count or 0),
        last_rendered_at=row.last_rendered_at,
        trigger_event_id=(
            UUID(str(row.trigger_event_id)) if row.trigger_event_id else None
        ),
        trigger_scope=row.trigger_scope,
        trigger_summary=row.trigger_summary,
        generation_version=int(row.generation_version or 1),
    )
