"""The property's relationship graph: owner, building, tenants, contractors."""

from __future__ import annotations

from uuid import UUID

import structlog
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from backend.db.session import get_session

router = APIRouter()
log = structlog.get_logger(__name__)


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


@router.get("/{property_id}/graph", response_model=PropertyGraph)
async def property_graph(
    property_id: UUID,
    session: AsyncSession = Depends(get_session),
) -> PropertyGraph:
    """Return the context graph for a property: owner, building, tenants, contractors."""
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
                label=f"Building: {prop_row.building_address}",
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
