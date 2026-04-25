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


class PropertySearchHit(BaseModel):
    """One result from ``GET /properties/search``."""

    id: UUID
    name: str
    address: str
    snippet: str
    score: float


class GraphNode(BaseModel):
    """A node in the property context graph."""

    id: str
    type: str  # 'property' | 'owner' | 'building' | 'tenant' | 'contractor'
    label: str
    metadata: dict[str, object] = {}


class GraphEdge(BaseModel):
    """An edge in the property context graph."""

    source: str
    target: str
    relationship: str


class PropertyGraph(BaseModel):
    """Response for ``GET /properties/{id}/graph``."""

    property_id: UUID
    nodes: list[GraphNode]
    edges: list[GraphEdge]


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


@router.get("/search", response_model=list[PropertySearchHit])
async def search_properties(
    q: str = Query(..., min_length=1, description="Free-text search query"),
    limit: int = Query(default=5, ge=1, le=25),
    session: AsyncSession = Depends(get_session),
) -> list[PropertySearchHit]:
    """Keyword search across property name, address, aliases, and current facts.

    Per-term scoring — the query is split on whitespace and each term
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


@router.get("/{property_id}/graph", response_model=PropertyGraph)
async def property_graph(
    property_id: UUID,
    session: AsyncSession = Depends(get_session),
) -> PropertyGraph:
    """Return the context graph for a property — owner, building, tenants, contractors."""
    prop_row = (
        await session.execute(
            text(
                """
                SELECT p.id, p.name, p.address,
                       o.id AS owner_id, o.name AS owner_name, o.email AS owner_email,
                       b.id AS building_id, b.address AS building_address,
                       b.year_built AS building_year
                FROM properties p
                LEFT JOIN owners o ON o.id = p.owner_id
                LEFT JOIN buildings b ON b.id = p.building_id
                WHERE p.id = :pid
                """
            ),
            {"pid": property_id},
        )
    ).first()
    if prop_row is None:
        raise HTTPException(status_code=404, detail="property not found")

    nodes: list[GraphNode] = [
        GraphNode(
            id=str(prop_row.id),
            type="property",
            label=prop_row.name,
            metadata={"address": prop_row.address},
        )
    ]
    edges: list[GraphEdge] = []

    if prop_row.owner_id is not None:
        nodes.append(
            GraphNode(
                id=str(prop_row.owner_id),
                type="owner",
                label=prop_row.owner_name or "Owner",
                metadata={"email": prop_row.owner_email or ""},
            )
        )
        edges.append(
            GraphEdge(
                source=str(prop_row.id),
                target=str(prop_row.owner_id),
                relationship="owned_by",
            )
        )

    if prop_row.building_id is not None:
        nodes.append(
            GraphNode(
                id=str(prop_row.building_id),
                type="building",
                label=f"Building — {prop_row.building_address}",
                metadata={
                    "address": prop_row.building_address or "",
                    "year_built": prop_row.building_year,
                },
            )
        )
        edges.append(
            GraphEdge(
                source=str(prop_row.id),
                target=str(prop_row.building_id),
                relationship="in_building",
            )
        )

    tenants = await session.execute(
        text(
            """
            SELECT id, name, email
            FROM tenants
            WHERE property_id = :pid
            ORDER BY name
            """
        ),
        {"pid": property_id},
    )
    for tenant in tenants.all():
        nodes.append(
            GraphNode(
                id=str(tenant.id),
                type="tenant",
                label=tenant.name,
                metadata={"email": tenant.email or ""},
            )
        )
        edges.append(
            GraphEdge(
                source=str(prop_row.id),
                target=str(tenant.id),
                relationship="occupied_by",
            )
        )

    contractors = await session.execute(
        text(
            """
            SELECT c.id, c.name, c.specialty, c.rating
            FROM relationships r
            JOIN contractors c ON c.id = r.to_id
            WHERE r.from_id = :pid
              AND r.from_type = 'property'
              AND r.to_type = 'contractor'
              AND r.relationship_type = 'serviced_by'
            """
        ),
        {"pid": property_id},
    )
    for contractor in contractors.all():
        nodes.append(
            GraphNode(
                id=str(contractor.id),
                type="contractor",
                label=contractor.name,
                metadata={
                    "specialty": contractor.specialty or "",
                    "rating": contractor.rating,
                },
            )
        )
        edges.append(
            GraphEdge(
                source=str(prop_row.id),
                target=str(contractor.id),
                relationship="serviced_by",
            )
        )

    log.info(
        "properties.graph",
        property_id=str(property_id),
        nodes=len(nodes),
        edges=len(edges),
    )
    return PropertyGraph(property_id=property_id, nodes=nodes, edges=edges)


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
