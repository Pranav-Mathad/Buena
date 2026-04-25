"""Buildings + Liegenschaften (WEG) read API.

Phase 8.1 surfaces the two new ownership tiers introduced in
``connectors.migrations`` 0002. Both endpoints render the same
markdown shape the property view uses, with cross-tier context blocks.
"""

from __future__ import annotations

from uuid import UUID

import structlog
from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import PlainTextResponse
from sqlalchemy.ext.asyncio import AsyncSession

from backend.db.session import get_session
from backend.pipeline.renderer import (
    render_building_markdown,
    render_liegenschaft_markdown,
)

building_router = APIRouter(prefix="/buildings", tags=["buildings"])
liegenschaft_router = APIRouter(prefix="/liegenschaften", tags=["liegenschaften"])
log = structlog.get_logger(__name__)


@building_router.get(
    "/{building_id}/markdown",
    response_class=PlainTextResponse,
)
async def building_markdown(
    building_id: UUID,
    session: AsyncSession = Depends(get_session),
) -> PlainTextResponse:
    """Return the living markdown for a building (Haus)."""
    try:
        body = await render_building_markdown(session, building_id)
    except ValueError as exc:
        log.info("buildings.markdown.not_found", building_id=str(building_id))
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    log.info("buildings.markdown.render", building_id=str(building_id), length=len(body))
    return PlainTextResponse(content=body, media_type="text/markdown; charset=utf-8")


@liegenschaft_router.get(
    "/{liegenschaft_id}/markdown",
    response_class=PlainTextResponse,
)
async def liegenschaft_markdown(
    liegenschaft_id: UUID,
    session: AsyncSession = Depends(get_session),
) -> PlainTextResponse:
    """Return the living markdown for a Liegenschaft (WEG)."""
    try:
        body = await render_liegenschaft_markdown(session, liegenschaft_id)
    except ValueError as exc:
        log.info(
            "liegenschaften.markdown.not_found",
            liegenschaft_id=str(liegenschaft_id),
        )
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    log.info(
        "liegenschaften.markdown.render",
        liegenschaft_id=str(liegenschaft_id),
        length=len(body),
    )
    return PlainTextResponse(content=body, media_type="text/markdown; charset=utf-8")
