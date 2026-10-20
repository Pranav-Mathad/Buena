"""Operator corrections to individual facts."""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

import structlog
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from backend.db.session import get_session
from backend.pipeline.events import get_event_bus
from backend.pipeline.materializer import propagate_after_fact_write

router = APIRouter()
log = structlog.get_logger(__name__)


class FactEditRequest(BaseModel):
    """Operator override of a fact value.

    ``edited_by`` defaults to ``operator``; pass a real user identifier
    when the UI has one. The endpoint sets ``human_edited=true`` on the
    new row, which the differ honors against future extractor proposals
    (see ``backend/pipeline/differ.py`` resolution rules).
    """

    value: str = Field(..., min_length=1, max_length=4_000)
    edited_by: str = Field(default="operator", min_length=1, max_length=120)


class FactEditResponse(BaseModel):
    """Result of a fact override: both the new and the superseded id."""

    fact_id: UUID
    superseded_fact_id: UUID
    property_id: UUID
    section: str
    field: str
    value: str
    edited_by: str
    edited_at: datetime


@router.patch(
    "/{property_id}/facts/{fact_id}",
    response_model=FactEditResponse,
)
async def edit_fact(
    property_id: UUID,
    fact_id: UUID,
    body: FactEditRequest,
    session: AsyncSession = Depends(get_session),
) -> FactEditResponse:
    """Operator override of a single fact.

    Inserts a new fact row carrying the operator's value with
    ``human_edited=true`` and supersedes the existing row, preserving
    the original ``source_event_id`` so the trace remains intact. The
    extractor's resolution rules treat ``human_edited`` rows as sticky:
    future proposals against the same ``(property_id, section, field)``
    coordinate are skipped with a ``preserved_human_edit`` reason.

    Broadcasts a ``fact_update`` payload on the event bus so the
    portfolio bell + per-property SSE listeners refresh in place.
    """
    current = (
        await session.execute(
            text(
                """
                SELECT id, property_id, section, field, value,
                       source_event_id, building_id, liegenschaft_id
                FROM facts
                WHERE id = :fid AND superseded_by IS NULL
                """
            ),
            {"fid": fact_id},
        )
    ).first()
    if current is None:
        raise HTTPException(
            status_code=404,
            detail="fact not found or already superseded",
        )
    if current.property_id != property_id:
        raise HTTPException(
            status_code=400,
            detail="fact does not belong to this property",
        )

    new_value = body.value.strip()
    if not new_value:
        raise HTTPException(status_code=400, detail="value must not be blank")
    if new_value == current.value:
        raise HTTPException(status_code=400, detail="value unchanged")

    inserted = (
        await session.execute(
            text(
                """
                INSERT INTO facts (
                    property_id, section, field, value, source_event_id,
                    confidence, valid_from, building_id, liegenschaft_id,
                    human_edited, edited_by, edited_at
                ) VALUES (
                    :pid, :section, :field, :value, :src, 1.0, now(),
                    :bid, :lid, TRUE, :who, now()
                )
                RETURNING id, edited_at
                """
            ),
            {
                "pid": property_id,
                "section": current.section,
                "field": current.field,
                "value": new_value,
                "src": current.source_event_id,
                "bid": current.building_id,
                "lid": current.liegenschaft_id,
                "who": body.edited_by,
            },
        )
    ).first()
    if inserted is None:  # pragma: no cover (RETURNING always yields a row)
        raise HTTPException(status_code=500, detail="insert failed")

    await session.execute(
        text(
            """
            UPDATE facts
            SET superseded_by = :new_id, valid_to = now()
            WHERE id = :old_id
            """
        ),
        {"new_id": inserted.id, "old_id": current.id},
    )

    edit_summary = f"{body.edited_by} edited {current.section}.{current.field}"
    await propagate_after_fact_write(
        session,
        property_id=property_id,
        building_id=current.building_id,
        liegenschaft_id=current.liegenschaft_id,
        trigger_event_id=current.source_event_id,
        trigger_summary=edit_summary,
    )
    await session.commit()

    log.info(
        "properties.fact.edited",
        property_id=str(property_id),
        new_fact_id=str(inserted.id),
        superseded_fact_id=str(current.id),
        section=current.section,
        field=current.field,
        edited_by=body.edited_by,
    )

    await get_event_bus().publish(
        property_id,
        {
            "event_id": None,
            "property_id": str(property_id),
            "category": "fact.edited",
            "summary": f"{body.edited_by} edited {current.section}.{current.field}",
            "facts_written": 1,
        },
    )

    return FactEditResponse(
        fact_id=inserted.id,
        superseded_fact_id=current.id,
        property_id=property_id,
        section=current.section,
        field=current.field,
        value=new_value,
        edited_by=body.edited_by,
        edited_at=inserted.edited_at,
    )
