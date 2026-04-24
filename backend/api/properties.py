"""Properties API — listing + rendered markdown."""

from __future__ import annotations

from uuid import UUID

import structlog
from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from backend.db.session import get_session
from backend.pipeline.renderer import render_markdown

router = APIRouter(prefix="/properties", tags=["properties"])
log = structlog.get_logger(__name__)


class PropertySummary(BaseModel):
    """Compact property record used by the portfolio listing."""

    id: UUID
    name: str
    address: str
    owner_name: str | None = None
    building_year_built: int | None = None


@router.get("", response_model=list[PropertySummary])
async def list_properties(
    session: AsyncSession = Depends(get_session),
) -> list[PropertySummary]:
    """Return every seeded property, joined with owner + building context."""
    result = await session.execute(
        text(
            """
            SELECT p.id, p.name, p.address,
                   o.name AS owner_name,
                   b.year_built AS building_year_built
            FROM properties p
            LEFT JOIN owners o ON o.id = p.owner_id
            LEFT JOIN buildings b ON b.id = p.building_id
            ORDER BY p.created_at ASC
            """
        )
    )
    properties = [
        PropertySummary(
            id=row.id,
            name=row.name,
            address=row.address,
            owner_name=row.owner_name,
            building_year_built=row.building_year_built,
        )
        for row in result.all()
    ]
    log.info("properties.list", count=len(properties))
    return properties


@router.get("/{property_id}/markdown", response_class=PlainTextResponse)
async def property_markdown(
    property_id: UUID,
    session: AsyncSession = Depends(get_session),
) -> PlainTextResponse:
    """Return the rendered markdown document for a single property."""
    try:
        body = await render_markdown(session, property_id)
    except ValueError as exc:
        log.info("properties.markdown.not_found", property_id=str(property_id))
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    log.info("properties.markdown.render", property_id=str(property_id), length=len(body))
    return PlainTextResponse(content=body, media_type="text/markdown; charset=utf-8")
