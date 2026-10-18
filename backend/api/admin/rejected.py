"""Rejected-update inbox: proposals a validator refused, and their override.

Overriding writes the proposed fact, records who decided in ``approval_log``,
and re-renders the affected files in the same transaction.
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


class RejectedUpdate(BaseModel):
    """One row of the rejected-updates inbox."""

    rejection_id: UUID
    event_id: UUID
    property_id: UUID | None = None
    proposed_section: str
    proposed_field: str
    proposed_value: str
    proposed_confidence: float | None = None
    constraint_name: str
    reason: str
    required_source_type: str | None = None
    reviewed_status: str
    created_at: datetime
    snippet: str = Field(
        default="",
        description="First ~160 chars of the originating event's raw_content.",
    )


class RejectedUpdatesResponse(BaseModel):
    """Response envelope for ``GET /properties/{id}/rejected``."""

    total: int
    by_status: dict[str, int]
    rows: list[RejectedUpdate]


class InboxRejectedItem(BaseModel):
    """Cross-property rejected-updates row for the global Inbox surface."""

    rejection_id: UUID
    event_id: UUID
    property_id: UUID | None = None
    property_name: str | None = None
    proposed_section: str
    proposed_field: str
    proposed_value: str
    constraint_name: str
    reason: str
    reviewed_status: str
    created_at: datetime
    snippet: str = ""


class OverrideRequest(BaseModel):
    """Body for ``POST /rejected/{id}/override``."""

    reviewed_by: str = Field(..., min_length=1, max_length=200)
    reason: str = Field(..., min_length=4, max_length=2000)
    decision: str = Field(
        default="overridden",
        pattern=r"^(overridden|dismissed)$",
        description=(
            "``overridden`` applies the proposed update + writes an "
            "approval_log entry. ``dismissed`` keeps the original fact "
            "and marks the rejection closed."
        ),
    )


class OverrideResponse(BaseModel):
    """Result of an override action."""

    rejection_id: UUID
    decision: str
    reviewed_status: str
    reviewed_at: datetime
    fact_written: bool


@router.get(
    "/properties/{property_id}/rejected",
    response_model=RejectedUpdatesResponse,
)
async def list_rejected_for_property(
    property_id: UUID,
    status_filter: str | None = Query(
        default=None,
        alias="status",
        description="Filter by reviewed_status (pending / needs_review / overridden / dismissed).",
    ),
    limit: int = Query(default=100, ge=1, le=500),
    session: AsyncSession = Depends(get_session),
) -> RejectedUpdatesResponse:
    """List validator rejections targeted at one property, newest first."""
    breakdown = (
        await session.execute(
            text(
                """
                SELECT reviewed_status, COUNT(*) AS n
                FROM rejected_updates
                WHERE property_id = :pid
                GROUP BY reviewed_status
                """
            ),
            {"pid": property_id},
        )
    ).all()
    by_status = {row.reviewed_status: int(row.n) for row in breakdown}

    params: dict[str, Any] = {"pid": property_id, "lim": limit}
    where = "WHERE r.property_id = :pid"
    if status_filter:
        where += " AND r.reviewed_status = :st"
        params["st"] = status_filter

    rows = (
        await session.execute(
            text(
                f"""
                SELECT
                  r.id, r.event_id, r.property_id,
                  r.proposed_section, r.proposed_field, r.proposed_value,
                  r.proposed_confidence, r.constraint_name, r.reason,
                  r.required_source_type, r.reviewed_status, r.created_at,
                  COALESCE(LEFT(e.raw_content, 160), '') AS snippet
                FROM rejected_updates r
                LEFT JOIN events e ON e.id = r.event_id
                {where}
                ORDER BY r.created_at DESC
                LIMIT :lim
                """
            ),
            params,
        )
    ).all()

    items = [
        RejectedUpdate(
            rejection_id=r.id,
            event_id=r.event_id,
            property_id=r.property_id,
            proposed_section=r.proposed_section,
            proposed_field=r.proposed_field,
            proposed_value=r.proposed_value,
            proposed_confidence=(
                float(r.proposed_confidence) if r.proposed_confidence is not None else None
            ),
            constraint_name=r.constraint_name,
            reason=r.reason,
            required_source_type=r.required_source_type,
            reviewed_status=r.reviewed_status,
            created_at=r.created_at,
            snippet=str(r.snippet or ""),
        )
        for r in rows
    ]
    log.info(
        "admin.rejected.list",
        property_id=str(property_id),
        total=sum(by_status.values()),
        returned=len(items),
    )
    return RejectedUpdatesResponse(
        total=sum(by_status.values()),
        by_status=by_status,
        rows=items,
    )


@router.post(
    "/rejected/{rejection_id}/override",
    response_model=OverrideResponse,
)
async def override_rejection(
    rejection_id: UUID,
    body: OverrideRequest,
    session: AsyncSession = Depends(get_session),
) -> OverrideResponse:
    """Operator override on a rejected update.

    On ``decision="overridden"`` the proposed fact is written + an
    ``approval_log`` row records the audit trail (operator + reason).
    On ``decision="dismissed"`` the rejection is closed without
    writing the fact. Either way the ``rejected_updates`` row's
    ``reviewed_status`` advances and the original rejection stays
    visible for posterity.
    """
    row = (
        await session.execute(
            text(
                """
                SELECT id, event_id, property_id, proposed_section,
                       proposed_field, proposed_value, proposed_confidence,
                       constraint_name, reason, reviewed_status
                FROM rejected_updates
                WHERE id = :rid
                FOR UPDATE
                """
            ),
            {"rid": rejection_id},
        )
    ).first()
    if row is None:
        from fastapi import HTTPException  # noqa: PLC0415

        raise HTTPException(status_code=404, detail="rejection not found")
    if row.reviewed_status not in {"pending", "needs_review"}:
        from fastapi import HTTPException  # noqa: PLC0415

        raise HTTPException(
            status_code=409,
            detail=f"rejection is already {row.reviewed_status!r}",
        )

    fact_written = False
    if body.decision == "overridden" and row.property_id is not None:
        await session.execute(
            text(
                """
                INSERT INTO facts (
                  property_id, section, field, value, source_event_id,
                  confidence, valid_from
                ) VALUES (
                  :pid, :section, :field, :value, :eid, :conf, now()
                )
                """
            ),
            {
                "pid": row.property_id,
                "section": row.proposed_section,
                "field": row.proposed_field,
                "value": row.proposed_value,
                "eid": row.event_id,
                "conf": (
                    float(row.proposed_confidence) if row.proposed_confidence is not None else 0.7
                ),
            },
        )
        fact_written = True
        # An operator override that promotes a rejected proposal to a
        # real fact must refresh the materialized file in the same
        # transaction, or the cached markdown would lag the facts table.
        await propagate_after_fact_write(
            session,
            property_id=UUID(str(row.property_id)),
            trigger_event_id=(UUID(str(row.event_id)) if row.event_id else None),
            trigger_summary=(
                f"override of {row.constraint_name}: {row.proposed_section}.{row.proposed_field}"
            ),
        )

    await session.execute(
        text(
            """
            INSERT INTO approval_log (
              actor, action, target_type, target_id, payload, created_at
            ) VALUES (
              :actor, :action, 'rejected_update', :tid, CAST(:payload AS JSONB), now()
            )
            """
        ),
        {
            "actor": body.reviewed_by,
            "action": f"rejection.{body.decision}",
            "tid": str(rejection_id),
            "payload": json_param(
                {
                    "constraint_name": row.constraint_name,
                    "section": row.proposed_section,
                    "field": row.proposed_field,
                    "value": row.proposed_value,
                    "reason_for_override": body.reason,
                    "fact_written": fact_written,
                }
            ),
        },
    )
    await session.execute(
        text(
            """
            UPDATE rejected_updates
            SET reviewed_status = :st,
                reviewed_at = now(),
                reviewed_by = :by
            WHERE id = :rid
            RETURNING reviewed_at
            """
        ),
        {"st": body.decision, "by": body.reviewed_by, "rid": rejection_id},
    )
    new_row = (
        await session.execute(
            text("SELECT reviewed_at FROM rejected_updates WHERE id = :rid"),
            {"rid": rejection_id},
        )
    ).first()
    await session.commit()
    log.info(
        "admin.rejected.override",
        rejection_id=str(rejection_id),
        decision=body.decision,
        fact_written=fact_written,
        reviewed_by=body.reviewed_by,
    )
    return OverrideResponse(
        rejection_id=rejection_id,
        decision=body.decision,
        reviewed_status=body.decision,
        reviewed_at=new_row.reviewed_at if new_row else datetime.utcnow(),
        fact_written=fact_written,
    )


@router.get("/rejected", response_model=list[InboxRejectedItem])
async def list_rejected_global(
    status_filter: str | None = Query(
        default="pending",
        alias="status",
    ),
    limit: int = Query(default=50, ge=1, le=200),
    session: AsyncSession = Depends(get_session),
) -> list[InboxRejectedItem]:
    """Cross-property rejected-updates inbox."""
    params: dict[str, Any] = {"lim": limit}
    where = ""
    if status_filter:
        where = "WHERE r.reviewed_status = :st"
        params["st"] = status_filter

    rows = (
        await session.execute(
            text(
                f"""
                SELECT r.id AS rejection_id,
                       r.event_id, r.property_id,
                       p.name AS property_name,
                       r.proposed_section, r.proposed_field, r.proposed_value,
                       r.constraint_name, r.reason,
                       r.reviewed_status, r.created_at,
                       COALESCE(LEFT(e.raw_content, 160), '') AS snippet
                FROM rejected_updates r
                LEFT JOIN events e ON e.id = r.event_id
                LEFT JOIN properties p ON p.id = r.property_id
                {where}
                ORDER BY r.created_at DESC
                LIMIT :lim
                """
            ),
            params,
        )
    ).all()
    items = [
        InboxRejectedItem(
            rejection_id=r.rejection_id,
            event_id=r.event_id,
            property_id=r.property_id,
            property_name=r.property_name,
            proposed_section=str(r.proposed_section),
            proposed_field=str(r.proposed_field),
            proposed_value=str(r.proposed_value),
            constraint_name=str(r.constraint_name),
            reason=str(r.reason),
            reviewed_status=str(r.reviewed_status),
            created_at=r.created_at,
            snippet=str(r.snippet or ""),
        )
        for r in rows
    ]
    log.info(
        "admin.rejected.global",
        status=status_filter,
        returned=len(items),
    )
    return items
