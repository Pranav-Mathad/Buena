"""Uncertainty inbox: observations the system would not commit to as facts.

Operators read them per property or across the portfolio and resolve each one
by promoting it to a fact or dismissing it; both are audit-logged.
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

from backend.api.admin._shared import json_param
from backend.db.session import get_session
from backend.pipeline.materializer import (
    propagate_after_fact_write,
)

router = APIRouter()
log = structlog.get_logger(__name__)


class InboxUncertaintyItem(BaseModel):
    """Cross-property uncertainty row for the global Inbox surface."""

    uncertainty_id: UUID
    event_id: UUID
    property_id: UUID | None = None
    property_name: str | None = None
    relevant_section: str | None = None
    observation: str
    hypothesis: str | None = None
    reason_uncertain: str
    source: str
    status: str
    created_at: datetime


class UncertaintyItemModel(BaseModel):
    """One open uncertainty row, ready for the admin UI."""

    uncertainty_id: UUID
    event_id: UUID
    relevant_section: str | None = None
    relevant_field: str | None = None
    observation: str
    hypothesis: str | None = None
    reason_uncertain: str
    source: str
    status: str
    created_at: datetime


class UncertaintyResponse(BaseModel):
    """Response envelope for ``GET /admin/properties/{id}/uncertainties``."""

    total: int
    by_section: dict[str, int]
    by_source: dict[str, int]
    items: list[UncertaintyItemModel]


@router.get(
    "/properties/{property_id}/uncertainties",
    response_model=UncertaintyResponse,
)
async def list_uncertainties_for_property(
    property_id: UUID,
    status_filter: str = Query(
        default="open",
        alias="status",
        pattern=r"^(open|resolved|dismissed|all)$",
        description="Filter by status; ``all`` returns every row.",
    ),
    limit: int = Query(default=200, ge=1, le=500),
    session: AsyncSession = Depends(get_session),
) -> UncertaintyResponse:
    """List the property's open uncertainty events.

    :func:`resolve_uncertainty` is the write side; this endpoint is
    its read-side pair so a UI can list → click → resolve in one round-trip.
    """
    where = "WHERE property_id = :pid"
    params: dict[str, Any] = {"pid": property_id, "lim": limit}
    if status_filter != "all":
        where += " AND status = :st"
        params["st"] = status_filter

    breakdown_section = (
        await session.execute(
            text(
                f"""
                SELECT COALESCE(relevant_section, '(unsectioned)') AS section,
                       COUNT(*) AS n
                FROM uncertainty_events
                {where}
                GROUP BY COALESCE(relevant_section, '(unsectioned)')
                """
            ),
            params,
        )
    ).all()
    by_section = {row.section: int(row.n) for row in breakdown_section}

    breakdown_source = (
        await session.execute(
            text(
                f"""
                SELECT source, COUNT(*) AS n
                FROM uncertainty_events
                {where}
                GROUP BY source
                """
            ),
            params,
        )
    ).all()
    by_source = {row.source: int(row.n) for row in breakdown_source}

    rows = (
        await session.execute(
            text(
                f"""
                SELECT id, event_id, relevant_section, relevant_field,
                       observation, hypothesis, reason_uncertain,
                       source, status, created_at
                FROM uncertainty_events
                {where}
                ORDER BY created_at DESC
                LIMIT :lim
                """
            ),
            params,
        )
    ).all()

    items = [
        UncertaintyItemModel(
            uncertainty_id=row.id,
            event_id=row.event_id,
            relevant_section=row.relevant_section,
            relevant_field=row.relevant_field,
            observation=str(row.observation or ""),
            hypothesis=row.hypothesis,
            reason_uncertain=str(row.reason_uncertain or ""),
            source=str(row.source or "extractor"),
            status=str(row.status),
            created_at=row.created_at,
        )
        for row in rows
    ]
    log.info(
        "admin.uncertainties.list",
        property_id=str(property_id),
        status=status_filter,
        total=sum(by_section.values()),
        returned=len(items),
    )
    return UncertaintyResponse(
        total=sum(by_section.values()),
        by_section=by_section,
        by_source=by_source,
        items=items,
    )


@router.get("/uncertainties", response_model=list[InboxUncertaintyItem])
async def list_uncertainties_global(
    status_filter: str = Query(
        default="open",
        alias="status",
        pattern=r"^(open|resolved|dismissed|all)$",
    ),
    limit: int = Query(default=50, ge=1, le=200),
    session: AsyncSession = Depends(get_session),
) -> list[InboxUncertaintyItem]:
    """Cross-property uncertainty inbox.

    Same shape as ``GET /admin/properties/{id}/uncertainties`` minus the
    by-section / by-source breakdowns, plus a ``property_name`` column
    so the global Inbox can label each row.
    """
    where = ""
    params: dict[str, Any] = {"lim": limit}
    if status_filter != "all":
        where = "WHERE u.status = :st"
        params["st"] = status_filter

    rows = (
        await session.execute(
            text(
                f"""
                SELECT u.id AS uncertainty_id,
                       u.event_id,
                       u.property_id,
                       p.name AS property_name,
                       u.relevant_section,
                       u.observation, u.hypothesis, u.reason_uncertain,
                       u.source, u.status, u.created_at
                FROM uncertainty_events u
                LEFT JOIN properties p ON p.id = u.property_id
                {where}
                ORDER BY u.created_at DESC
                LIMIT :lim
                """
            ),
            params,
        )
    ).all()

    items = [
        InboxUncertaintyItem(
            uncertainty_id=r.uncertainty_id,
            event_id=r.event_id,
            property_id=r.property_id,
            property_name=r.property_name,
            relevant_section=r.relevant_section,
            observation=str(r.observation or ""),
            hypothesis=r.hypothesis,
            reason_uncertain=str(r.reason_uncertain or ""),
            source=str(r.source or "extractor"),
            status=str(r.status),
            created_at=r.created_at,
        )
        for r in rows
    ]
    log.info(
        "admin.uncertainties.global",
        status=status_filter,
        returned=len(items),
    )
    return items


class UncertaintyResolveRequest(BaseModel):
    """Body for ``POST /admin/uncertainties/{id}/resolve``."""

    action: str = Field(
        ...,
        pattern=r"^(promote_to_fact|dismiss)$",
        description=(
            "``promote_to_fact`` writes a fact at "
            "(relevant_section, relevant_field) using ``value`` (or "
            "``hypothesis`` as fallback) and marks the uncertainty "
            "``resolved``. ``dismiss`` closes the uncertainty without "
            "writing a fact."
        ),
    )
    value: str | None = Field(
        default=None,
        max_length=2000,
        description=(
            "Operator-supplied value to commit when promoting. Falls "
            "back to ``hypothesis`` from the uncertainty row if omitted. "
            "Required when promoting an uncertainty whose hypothesis is "
            "NULL."
        ),
    )
    reason: str = Field(..., min_length=3, max_length=2000)
    reviewed_by: str = Field(..., min_length=1, max_length=200)


class UncertaintyResolveResponse(BaseModel):
    """Result of a resolve action."""

    uncertainty_id: UUID
    action: str
    new_status: str
    fact_id: UUID | None = None
    resolved_at: datetime


@router.post(
    "/uncertainties/{uncertainty_id}/resolve",
    response_model=UncertaintyResolveResponse,
)
async def resolve_uncertainty(
    uncertainty_id: UUID,
    body: UncertaintyResolveRequest,
    session: AsyncSession = Depends(get_session),
) -> UncertaintyResolveResponse:
    """Promote an uncertainty to a fact, or dismiss it.

    Either action transitions the uncertainty out of ``open`` and
    records an ``approval_log`` entry naming the operator + reason. The
    audit trail is the point: a human decision overrides the extractor,
    so it must be attributable after the fact. ``promote_to_fact`` requires the
    uncertainty to carry both ``relevant_section`` and
    ``relevant_field``; the value comes from ``body.value`` or, if
    absent, from the uncertainty's ``hypothesis``. A 422 is returned
    when neither is available.

    The fact is written with ``confidence=0.95`` and ``source_event_id``
    pointing at the uncertainty's originating event so the source link
    keeps working.
    """
    from fastapi import HTTPException  # noqa: PLC0415

    row = (
        await session.execute(
            text(
                """
                SELECT id, event_id, property_id, building_id, liegenschaft_id,
                       relevant_section, relevant_field, observation,
                       hypothesis, reason_uncertain, status
                FROM uncertainty_events
                WHERE id = :uid
                FOR UPDATE
                """
            ),
            {"uid": uncertainty_id},
        )
    ).first()
    if row is None:
        raise HTTPException(status_code=404, detail="uncertainty not found")
    if row.status != "open":
        raise HTTPException(
            status_code=409,
            detail=f"uncertainty is already {row.status!r}",
        )

    fact_id: UUID | None = None
    written_value: str | None = None

    if body.action == "promote_to_fact":
        if not row.relevant_section or not row.relevant_field:
            raise HTTPException(
                status_code=422,
                detail=(
                    "cannot promote — uncertainty is missing relevant_section or relevant_field"
                ),
            )
        # Pick the value: explicit override wins, hypothesis second.
        candidate = body.value if body.value not in (None, "") else row.hypothesis
        if not candidate:
            raise HTTPException(
                status_code=422,
                detail=(
                    "cannot promote — no value supplied and uncertainty has "
                    "no hypothesis to fall back on"
                ),
            )
        written_value = str(candidate)

        # Pick the scope column the fact should attach to. Property is the
        # default surface; building / liegenschaft are honoured if the
        # uncertainty was scoped that way.
        scope_column = "property_id"
        scope_value = row.property_id
        if row.property_id is None and row.building_id is not None:
            scope_column = "building_id"
            scope_value = row.building_id
        elif row.property_id is None and row.liegenschaft_id is not None:
            scope_column = "liegenschaft_id"
            scope_value = row.liegenschaft_id
        if scope_value is None:
            raise HTTPException(
                status_code=422,
                detail=("cannot promote — uncertainty has no property/building/liegenschaft scope"),
            )

        fact_row = (
            await session.execute(
                text(
                    f"""
                    INSERT INTO facts (
                      {scope_column}, section, field, value, source_event_id,
                      confidence, valid_from
                    ) VALUES (
                      :sid, :section, :field, :value, :eid, 0.95, now()
                    )
                    RETURNING id
                    """
                ),
                {
                    "sid": scope_value,
                    "section": row.relevant_section,
                    "field": row.relevant_field,
                    "value": written_value,
                    "eid": row.event_id,
                },
            )
        ).one()
        fact_id = UUID(str(fact_row.id))

        await session.execute(
            text(
                """
                UPDATE uncertainty_events
                SET status = 'resolved',
                    resolved_to_fact_id = :fid,
                    resolved_at = now()
                WHERE id = :uid
                """
            ),
            {"fid": fact_id, "uid": uncertainty_id},
        )
        new_status = "resolved"

        # Promotion writes a real fact at the uncertainty's
        # original scope; propagate to the matching tier (and cascade
        # if it's a building or liegenschaft fact).
        propagate_kwargs: dict[str, UUID | None] = {
            "property_id": None,
            "building_id": None,
            "liegenschaft_id": None,
        }
        propagate_kwargs[scope_column] = UUID(str(scope_value))
        await propagate_after_fact_write(
            session,
            **propagate_kwargs,
            trigger_event_id=(UUID(str(row.event_id)) if row.event_id else None),
            trigger_summary=(
                f"uncertainty resolved → {row.relevant_section}."
                f"{row.relevant_field}: {written_value}"
            ),
        )
    else:
        await session.execute(
            text(
                """
                UPDATE uncertainty_events
                SET status = 'dismissed',
                    resolved_at = now()
                WHERE id = :uid
                """
            ),
            {"uid": uncertainty_id},
        )
        new_status = "dismissed"

    await session.execute(
        text(
            """
            INSERT INTO approval_log (
              actor, action, target_type, target_id, payload, created_at
            ) VALUES (
              :actor, :action, 'uncertainty_event', :tid,
              CAST(:payload AS JSONB), now()
            )
            """
        ),
        {
            "actor": body.reviewed_by,
            "action": f"uncertainty.{body.action}",
            "tid": str(uncertainty_id),
            "payload": json_param(
                {
                    "section": row.relevant_section,
                    "field": row.relevant_field,
                    "value_written": written_value,
                    "fact_id": str(fact_id) if fact_id else None,
                    "reason": body.reason,
                    "observation": str(row.observation or "")[:200],
                }
            ),
        },
    )
    resolved_row = (
        await session.execute(
            text("SELECT resolved_at FROM uncertainty_events WHERE id = :uid"),
            {"uid": uncertainty_id},
        )
    ).first()
    await session.commit()
    log.info(
        "admin.uncertainty.resolve",
        uncertainty_id=str(uncertainty_id),
        action=body.action,
        fact_id=str(fact_id) if fact_id else None,
        reviewed_by=body.reviewed_by,
    )
    return UncertaintyResolveResponse(
        uncertainty_id=uncertainty_id,
        action=body.action,
        new_status=new_status,
        fact_id=fact_id,
        resolved_at=(resolved_row.resolved_at if resolved_row else datetime.utcnow()),
    )
