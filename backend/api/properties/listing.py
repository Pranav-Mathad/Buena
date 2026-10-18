"""Property listing, keyword search and creation."""

from __future__ import annotations

from typing import Any
from uuid import UUID

import structlog
from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from backend.db.session import get_session
from backend.services.featured import resolve_featured_property_id

router = APIRouter()
log = structlog.get_logger(__name__)


class PropertySummary(BaseModel):
    """Compact property record used by the portfolio listing.

    Counts are computed at query time from indexed property_id lookups on
    ``events``, ``facts`` (current only: ``superseded_by IS NULL``),
    ``uncertainty_events`` (``status='open'``), and ``signals``
    (``status='pending'``). At the sample dataset's 56 properties this is
    ~224 index hits per request, single-digit ms total.
    """

    id: UUID
    name: str
    address: str
    owner_name: str | None = None
    building_year_built: int | None = None
    events: int = 0
    facts: int = 0
    needs_review: int = 0
    open_issues: int = 0
    #: True for the property named by ``FEATURED_PROPERTY``.
    is_hero: bool = False


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


class PropertySearchHit(BaseModel):
    """One result from ``GET /properties/search``."""

    id: UUID
    name: str
    address: str
    snippet: str
    score: float


@router.get("", response_model=list[PropertySummary])
async def list_properties(
    session: AsyncSession = Depends(get_session),
) -> list[PropertySummary]:
    """Return every property with owner, building and count context."""
    featured_id = await resolve_featured_property_id(session)
    result = await session.execute(
        text(
            """
            SELECT p.id, p.name, p.address,
                   o.name AS owner_name,
                   b.year_built AS building_year_built,
                   (SELECT COUNT(*) FROM events
                      WHERE property_id = p.id) AS events,
                   (SELECT COUNT(*) FROM facts
                      WHERE property_id = p.id
                        AND superseded_by IS NULL) AS facts,
                   (SELECT COUNT(*) FROM uncertainty_events
                      WHERE property_id = p.id
                        AND status = 'open') AS needs_review,
                   (SELECT COUNT(*) FROM signals
                      WHERE property_id = p.id
                        AND status = 'pending') AS open_issues
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
            events=int(row.events or 0),
            facts=int(row.facts or 0),
            needs_review=int(row.needs_review or 0),
            open_issues=int(row.open_issues or 0),
            is_hero=featured_id is not None and str(row.id) == str(featured_id),
        )
        for row in result.all()
    ]
    log.info("properties.list", count=len(properties))
    return properties


@router.get("/search", response_model=list[PropertySearchHit])
async def search_properties(
    q: str = Query(..., min_length=1, description="Free-text search query"),
    limit: int = Query(default=5, ge=1, le=25),
    session: AsyncSession = Depends(get_session),
) -> list[PropertySearchHit]:
    """Keyword search across property name, address, aliases, and current facts.

    Per-term scoring: the query is split on whitespace and each term
    contributes independently, so ``"heating Berlin"`` picks up a heating
    fact *and* a Berlin address rather than needing both words to appear
    contiguously.

    Weights:
    - direct name/alias term hit      → 1.0
    - address term hit                → 0.8
    - fact value term hit             → 0.5 + min(count / 10, 0.45)
    """
    terms = [t for t in q.lower().split() if t.strip()] or [q.lower()]
    aggregated: dict[str, dict[str, Any]] = {}
    for term in terms:
        pattern = f"%{term}%"
        result = await session.execute(
            text(
                """
                WITH fact_hits AS (
                    SELECT f.property_id,
                           COUNT(*) AS hits,
                           (ARRAY_AGG(f.value ORDER BY f.created_at DESC))[1]
                               AS snippet
                    FROM facts f
                    WHERE f.superseded_by IS NULL
                      AND LOWER(f.value) LIKE :pat
                    GROUP BY f.property_id
                )
                SELECT p.id, p.name, p.address,
                       COALESCE(fh.hits, 0) AS hits,
                       fh.snippet AS fact_snippet,
                       CASE
                         WHEN LOWER(p.name) LIKE :pat THEN 1.0
                         WHEN EXISTS (
                           SELECT 1 FROM UNNEST(p.aliases) a
                           WHERE LOWER(a) LIKE :pat
                         ) THEN 0.95
                         WHEN LOWER(p.address) LIKE :pat THEN 0.8
                         WHEN COALESCE(fh.hits, 0) > 0
                           THEN 0.5 + LEAST(fh.hits::float / 10.0, 0.45)
                         ELSE 0.0
                       END AS score
                FROM properties p
                LEFT JOIN fact_hits fh ON fh.property_id = p.id
                """
            ),
            {"pat": pattern},
        )
        for row in result.all():
            score = float(row.score)
            if score <= 0:
                continue
            slot = aggregated.setdefault(
                str(row.id),
                {
                    "id": row.id,
                    "name": row.name,
                    "address": row.address,
                    "snippet": None,
                    "score": 0.0,
                },
            )
            slot["score"] += score
            if slot["snippet"] is None and row.fact_snippet:
                slot["snippet"] = row.fact_snippet

    hits = sorted(aggregated.values(), key=lambda r: r["score"], reverse=True)[:limit]
    log.info("properties.search", q=q, terms=len(terms), returned=len(hits))
    return [
        PropertySearchHit(
            id=h["id"],
            name=h["name"],
            address=h["address"],
            snippet=(h["snippet"] or f"{h['name']} — {h['address']}")[:220],
            score=min(float(h["score"]), 1.0 * len(terms)),
        )
        for h in hits
    ]


@router.post("", response_model=PropertyCreateResponse, status_code=201)
async def create_property(
    payload: PropertyCreateRequest,
    session: AsyncSession = Depends(get_session),
) -> PropertyCreateResponse:
    """Create a property."""
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
    return PropertyCreateResponse(id=property_id, name=payload.name, address=payload.address)
