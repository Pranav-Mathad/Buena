"""Buildings + Liegenschaften (WEG) read API.

Phase 8.1 surfaced the two new ownership tiers introduced in
``connectors.migrations`` 0002. Both endpoints render the same
markdown shape the property view uses, with cross-tier context blocks.

Phase 12 — markdown reads now hit the materialized ``building_files``
and ``liegenschaft_files`` cache first (populated by the applier hook
in :mod:`backend.pipeline.materializer`); live rendering remains as
the fallback path. Per-tier ``/file`` endpoints expose the full row
shape (content + last_rendered_at + trigger metadata) for cache
inspection / external consumers.
"""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

import structlog
from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from backend.db.session import get_session
from backend.pipeline.renderer import (
    render_building_markdown,
    render_liegenschaft_markdown,
)

building_router = APIRouter(prefix="/buildings", tags=["buildings"])
liegenschaft_router = APIRouter(prefix="/liegenschaften", tags=["liegenschaften"])
log = structlog.get_logger(__name__)


class BuildingFileRow(BaseModel):
    """Full materialized row for a building."""

    building_id: UUID
    content_md: str
    fact_count: int
    last_rendered_at: datetime
    trigger_event_id: UUID | None = None
    trigger_scope: str | None = None
    trigger_summary: str | None = None
    generation_version: int


class LiegenschaftFileRow(BaseModel):
    """Full materialized row for a liegenschaft (WEG)."""

    liegenschaft_id: UUID
    content_md: str
    fact_count: int
    last_rendered_at: datetime
    trigger_event_id: UUID | None = None
    trigger_scope: str | None = None
    trigger_summary: str | None = None
    generation_version: int


@building_router.get(
    "/{building_id}/markdown",
    response_class=PlainTextResponse,
)
async def building_markdown(
    building_id: UUID,
    session: AsyncSession = Depends(get_session),
) -> PlainTextResponse:
    """Return the living markdown for a building (Haus).

    Reads the materialized ``building_files`` row when present; falls
    back to a live render on cache miss.
    """
    cached = (
        await session.execute(
            text(
                "SELECT content_md, last_rendered_at FROM building_files "
                "WHERE building_id = :bid"
            ),
            {"bid": building_id},
        )
    ).first()
    if cached is not None:
        log.info(
            "buildings.markdown.cache_hit",
            building_id=str(building_id),
            length=len(cached.content_md),
        )
        return PlainTextResponse(
            content=cached.content_md,
            media_type="text/markdown; charset=utf-8",
            headers={
                "Cache-Control": "public, max-age=30",
                "ETag": f'"{int(cached.last_rendered_at.timestamp())}"',
            },
        )

    try:
        body = await render_building_markdown(session, building_id)
    except ValueError as exc:
        log.info("buildings.markdown.not_found", building_id=str(building_id))
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    log.info(
        "buildings.markdown.cache_miss_render",
        building_id=str(building_id),
        length=len(body),
    )
    return PlainTextResponse(content=body, media_type="text/markdown; charset=utf-8")


@building_router.get(
    "/{building_id}/file",
    response_model=BuildingFileRow,
)
async def building_file(
    building_id: UUID,
    session: AsyncSession = Depends(get_session),
) -> BuildingFileRow:
    """Return the materialized row for a building (cache inspection)."""
    row = (
        await session.execute(
            text(
                """
                SELECT building_id, content_md, fact_count, last_rendered_at,
                       trigger_event_id, trigger_scope, trigger_summary,
                       generation_version
                FROM building_files
                WHERE building_id = :bid
                """
            ),
            {"bid": building_id},
        )
    ).first()
    if row is None:
        raise HTTPException(
            status_code=404, detail="building file not materialized yet"
        )
    return BuildingFileRow(
        building_id=UUID(str(row.building_id)),
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


@liegenschaft_router.get(
    "/{liegenschaft_id}/markdown",
    response_class=PlainTextResponse,
)
async def liegenschaft_markdown(
    liegenschaft_id: UUID,
    session: AsyncSession = Depends(get_session),
) -> PlainTextResponse:
    """Return the living markdown for a Liegenschaft (WEG).

    Reads the materialized ``liegenschaft_files`` row when present;
    falls back to a live render on cache miss.
    """
    cached = (
        await session.execute(
            text(
                "SELECT content_md, last_rendered_at FROM liegenschaft_files "
                "WHERE liegenschaft_id = :lid"
            ),
            {"lid": liegenschaft_id},
        )
    ).first()
    if cached is not None:
        log.info(
            "liegenschaften.markdown.cache_hit",
            liegenschaft_id=str(liegenschaft_id),
            length=len(cached.content_md),
        )
        return PlainTextResponse(
            content=cached.content_md,
            media_type="text/markdown; charset=utf-8",
            headers={
                "Cache-Control": "public, max-age=30",
                "ETag": f'"{int(cached.last_rendered_at.timestamp())}"',
            },
        )

    try:
        body = await render_liegenschaft_markdown(session, liegenschaft_id)
    except ValueError as exc:
        log.info(
            "liegenschaften.markdown.not_found",
            liegenschaft_id=str(liegenschaft_id),
        )
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    log.info(
        "liegenschaften.markdown.cache_miss_render",
        liegenschaft_id=str(liegenschaft_id),
        length=len(body),
    )
    return PlainTextResponse(content=body, media_type="text/markdown; charset=utf-8")


@liegenschaft_router.get(
    "/{liegenschaft_id}/file",
    response_model=LiegenschaftFileRow,
)
async def liegenschaft_file(
    liegenschaft_id: UUID,
    session: AsyncSession = Depends(get_session),
) -> LiegenschaftFileRow:
    """Return the materialized row for a liegenschaft (cache inspection)."""
    row = (
        await session.execute(
            text(
                """
                SELECT liegenschaft_id, content_md, fact_count, last_rendered_at,
                       trigger_event_id, trigger_scope, trigger_summary,
                       generation_version
                FROM liegenschaft_files
                WHERE liegenschaft_id = :lid
                """
            ),
            {"lid": liegenschaft_id},
        )
    ).first()
    if row is None:
        raise HTTPException(
            status_code=404, detail="liegenschaft file not materialized yet"
        )
    return LiegenschaftFileRow(
        liegenschaft_id=UUID(str(row.liegenschaft_id)),
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
