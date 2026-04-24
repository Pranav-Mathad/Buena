"""Properties API — listing, creation (with Tavily enrichment), markdown, activity."""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

import structlog
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from backend.db.session import get_session
from backend.pipeline.renderer import render_markdown
from backend.services.tavily import enrich_property

router = APIRouter(prefix="/properties", tags=["properties"])
log = structlog.get_logger(__name__)


class PropertySummary(BaseModel):
    """Compact property record used by the portfolio listing."""

    id: UUID
    name: str
    address: str
    owner_name: str | None = None
    building_year_built: int | None = None


class PropertyCreateRequest(BaseModel):
    """Payload for POST /properties."""

    name: str = Field(..., min_length=1)
    address: str = Field(..., min_length=1)
    aliases: list[str] = Field(default_factory=list)
    owner_id: UUID | None = None
    building_id: UUID | None = None


class PropertyCreateResponse(BaseModel):
    """Response for POST /properties."""

    id: UUID
    name: str
    address: str
    tavily_event_id: UUID | None


class EnrichmentResponse(BaseModel):
    """Response for POST /properties/{id}/enrich."""

    property_id: UUID
    tavily_event_id: UUID | None
    already_enriched: bool


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


@router.post("", response_model=PropertyCreateResponse, status_code=201)
async def create_property(
    payload: PropertyCreateRequest,
    session: AsyncSession = Depends(get_session),
) -> PropertyCreateResponse:
    """Create a property and kick off Tavily enrichment (runs once, per Part IV)."""
    result = await session.execute(
        text(
            """
            INSERT INTO properties (name, address, aliases, owner_id, building_id)
            VALUES (:name, :addr, :aliases, :owner, :building)
            RETURNING id
            """
        ),
        {
            "name": payload.name,
            "addr": payload.address,
            "aliases": payload.aliases,
            "owner": payload.owner_id,
            "building": payload.building_id,
        },
    )
    property_id: UUID = result.scalar_one()
    await session.commit()
    log.info("properties.create", property_id=str(property_id), name=payload.name)

    tavily_event_id = await enrich_property(property_id, payload.name, payload.address)
    return PropertyCreateResponse(
        id=property_id,
        name=payload.name,
        address=payload.address,
        tavily_event_id=tavily_event_id,
    )


@router.post("/{property_id}/enrich", response_model=EnrichmentResponse)
async def enrich_existing_property(
    property_id: UUID,
    session: AsyncSession = Depends(get_session),
) -> EnrichmentResponse:
    """Re-run Tavily enrichment for an existing (seeded) property.

    Idempotent — a second call is a no-op. Intended as an admin utility so
    the seeded portfolio can show the "Updated from web sources" badge
    without requiring a reseed.
    """
    row = (
        await session.execute(
            text("SELECT name, address FROM properties WHERE id = :pid"),
            {"pid": property_id},
        )
    ).first()
    if row is None:
        raise HTTPException(status_code=404, detail="property not found")
    before_count = (
        await session.execute(
            text(
                """
                SELECT COUNT(*)
                FROM events
                WHERE property_id = :pid AND source = 'web'
                  AND source_ref LIKE 'tavily:%'
                """
            ),
            {"pid": property_id},
        )
    ).scalar_one()

    event_id = await enrich_property(property_id, row.name, row.address)
    return EnrichmentResponse(
        property_id=property_id,
        tavily_event_id=event_id,
        already_enriched=event_id is None and int(before_count or 0) > 0,
    )


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


class ActivityItem(BaseModel):
    """Activity feed row — a processed event + its one-line summary."""

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
