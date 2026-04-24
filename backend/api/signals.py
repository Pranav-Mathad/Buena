"""Approval inbox — list / approve / reject / edit / evaluate.

Shape matches what the Lovable UI will bind to:

- ``GET /signals`` — list with filters (``status``, ``severity``, ``type``).
- ``GET /signals/{id}`` — single signal with its proposed_action + evidence.
- ``POST /signals/evaluate`` — manually fire the rule evaluator (demo trigger).
- ``POST /signals/{id}/approve`` — dispatch via the Entire-compatible broker,
  write outbox row + approval log entry, flip status to ``resolved``.
- ``POST /signals/{id}/reject`` — log rejection, flip status to ``rejected``.
- ``POST /signals/{id}/edit`` — allow the human to edit the drafted message
  before approval; writes an ``approval_log`` row with ``decision='edited'``.
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any
from uuid import UUID

import structlog
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from backend.db.session import get_session
from backend.services.entire import get_broker
from backend.signals.evaluator import evaluate_all

router = APIRouter(prefix="/signals", tags=["signals"])
log = structlog.get_logger(__name__)


class SignalSummary(BaseModel):
    """Compact record for the inbox listing."""

    id: UUID
    property_id: UUID | None
    property_name: str | None
    type: str
    severity: str
    message: str
    status: str
    created_at: datetime
    resolved_at: datetime | None


class ProposedActionOut(BaseModel):
    """Serialized shape of ``signals.proposed_action``."""

    type: str | None = None
    channel: str | None = None
    recipient: str | None = None
    subject: str | None = None
    drafted_message: str | None = None
    payload: dict[str, Any] = Field(default_factory=dict)


class SignalDetail(SignalSummary):
    """Full signal with evidence + drafted action."""

    evidence: list[dict[str, Any]]
    proposed_action: ProposedActionOut | None


class EditRequest(BaseModel):
    """Edit a signal's drafted action before approval."""

    subject: str | None = None
    drafted_message: str | None = None


class ActionResponse(BaseModel):
    """Response for approve/reject/edit."""

    signal_id: UUID
    status: str
    outbox_id: UUID | None = None
    dispatched_to: str | None = None


class EvaluateResponse(BaseModel):
    """Response for ``POST /signals/evaluate``."""

    created: int


class ProposeRequest(BaseModel):
    """Payload for ``POST /signals/propose`` — MCP / external-AI entry point."""

    property_id: UUID | None = None
    type: str = Field(..., min_length=1)
    severity: str = Field(default="medium")
    message: str = Field(..., min_length=1)
    action: dict[str, Any] = Field(default_factory=dict)
    evidence: list[dict[str, Any]] = Field(default_factory=list)


class ProposeResponse(BaseModel):
    """Response for ``POST /signals/propose``."""

    signal_id: UUID
    status: str


def _row_to_detail(row: Any) -> SignalDetail:
    """Shared mapper for the single-signal queries."""
    proposed = row.proposed_action or {}
    if isinstance(proposed, str):
        proposed = json.loads(proposed)
    action = ProposedActionOut(**proposed) if proposed else None
    evidence = row.evidence or []
    if isinstance(evidence, str):
        evidence = json.loads(evidence)
    return SignalDetail(
        id=row.id,
        property_id=row.property_id,
        property_name=row.property_name,
        type=row.type,
        severity=row.severity,
        message=row.message,
        status=row.status,
        created_at=row.created_at,
        resolved_at=row.resolved_at,
        evidence=list(evidence),
        proposed_action=action,
    )


@router.get("", response_model=list[SignalSummary])
async def list_signals(
    status: str | None = Query(default="pending"),
    severity: str | None = None,
    signal_type: str | None = Query(default=None, alias="type"),
    property_id: UUID | None = None,
    limit: int = Query(default=50, ge=1, le=200),
    session: AsyncSession = Depends(get_session),
) -> list[SignalSummary]:
    """Return signals filtered by the usual inbox dimensions."""
    params: dict[str, Any] = {"lim": limit}
    clauses: list[str] = []
    if status:
        clauses.append("s.status = :status")
        params["status"] = status
    if severity:
        clauses.append("s.severity = :severity")
        params["severity"] = severity
    if signal_type:
        clauses.append("s.type = :type")
        params["type"] = signal_type
    if property_id:
        clauses.append("s.property_id = :pid")
        params["pid"] = property_id
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    query = f"""
        SELECT s.id, s.property_id, p.name AS property_name,
               s.type, s.severity, s.message, s.status,
               s.created_at, s.resolved_at
        FROM signals s
        LEFT JOIN properties p ON p.id = s.property_id
        {where}
        ORDER BY
          CASE s.severity WHEN 'urgent' THEN 0 WHEN 'high' THEN 1
                          WHEN 'medium' THEN 2 ELSE 3 END,
          s.created_at DESC
        LIMIT :lim
    """
    result = await session.execute(text(query), params)
    return [SignalSummary(**row._mapping) for row in result.all()]


@router.post("/propose", response_model=ProposeResponse, status_code=201)
async def propose_signal(
    payload: ProposeRequest,
    session: AsyncSession = Depends(get_session),
) -> ProposeResponse:
    """Insert a pending signal authored by an external AI (Claude via MCP).

    The human always approves before any outbox side effect — this endpoint
    deliberately does not dispatch.
    """
    severity = payload.severity if payload.severity in {"low", "medium", "high", "urgent"} else "medium"
    action: dict[str, Any] = dict(payload.action)
    action.setdefault("type", "owner_notification")
    action.setdefault("channel", "email")
    action.setdefault("recipient", "owner@keystone.demo")
    action.setdefault("subject", "[Proposed] " + payload.message[:60])
    action.setdefault("drafted_message", payload.message)
    action.setdefault("payload", {})
    action["payload"]["proposed_by"] = "external_ai"

    result = await session.execute(
        text(
            """
            INSERT INTO signals
                (property_id, type, severity, message, evidence,
                 proposed_action, status, created_at)
            VALUES
                (:pid, :type, :sev, :msg, CAST(:evd AS JSONB),
                 CAST(:act AS JSONB), 'pending', now())
            RETURNING id
            """
        ),
        {
            "pid": payload.property_id,
            "type": payload.type,
            "sev": severity,
            "msg": payload.message,
            "evd": json.dumps(payload.evidence),
            "act": json.dumps(action),
        },
    )
    signal_id: UUID = result.scalar_one()
    await session.commit()
    log.info(
        "signals.propose",
        signal_id=str(signal_id),
        type=payload.type,
        source="external_ai",
    )
    return ProposeResponse(signal_id=signal_id, status="pending")


@router.post("/evaluate", response_model=EvaluateResponse)
async def evaluate_now(
    session: AsyncSession = Depends(get_session),
) -> EvaluateResponse:
    """Manually fire the rule evaluator. Useful for demo determinism."""
    created = await evaluate_all(session)
    await session.commit()
    log.info("signals.evaluate", created=created)
    return EvaluateResponse(created=created)


@router.get("/{signal_id}", response_model=SignalDetail)
async def get_signal(
    signal_id: UUID,
    session: AsyncSession = Depends(get_session),
) -> SignalDetail:
    """Return the full signal detail including evidence + draft."""
    row = (
        await session.execute(
            text(
                """
                SELECT s.id, s.property_id, p.name AS property_name,
                       s.type, s.severity, s.message, s.status,
                       s.created_at, s.resolved_at, s.evidence, s.proposed_action
                FROM signals s
                LEFT JOIN properties p ON p.id = s.property_id
                WHERE s.id = :sid
                """
            ),
            {"sid": signal_id},
        )
    ).first()
    if row is None:
        raise HTTPException(status_code=404, detail="signal not found")
    return _row_to_detail(row)


@router.post("/{signal_id}/edit", response_model=SignalDetail)
async def edit_signal(
    signal_id: UUID,
    payload: EditRequest,
    session: AsyncSession = Depends(get_session),
) -> SignalDetail:
    """Edit the drafted action + subject before approval.

    Writes an ``approval_log`` row with ``decision='edited'`` and the delta
    so the learning layer later can measure how often humans edit vs accept.
    """
    row = (
        await session.execute(
            text("SELECT proposed_action, status FROM signals WHERE id = :sid"),
            {"sid": signal_id},
        )
    ).first()
    if row is None:
        raise HTTPException(status_code=404, detail="signal not found")
    if row.status not in {"pending", "approved"}:
        raise HTTPException(
            status_code=409, detail=f"cannot edit signal in status={row.status}"
        )

    action = row.proposed_action or {}
    if isinstance(action, str):
        action = json.loads(action)
    edits: dict[str, Any] = {}
    if payload.subject is not None:
        edits["subject"] = {"before": action.get("subject"), "after": payload.subject}
        action["subject"] = payload.subject
    if payload.drafted_message is not None:
        edits["drafted_message"] = {
            "before": action.get("drafted_message"),
            "after": payload.drafted_message,
        }
        action["drafted_message"] = payload.drafted_message

    await session.execute(
        text(
            """
            UPDATE signals SET proposed_action = CAST(:act AS JSONB)
            WHERE id = :sid
            """
        ),
        {"sid": signal_id, "act": json.dumps(action)},
    )
    await session.execute(
        text(
            """
            INSERT INTO approval_log (signal_id, decision, edits)
            VALUES (:sid, 'edited', CAST(:edits AS JSONB))
            """
        ),
        {"sid": signal_id, "edits": json.dumps(edits)},
    )
    await session.commit()
    log.info("signals.edit", signal_id=str(signal_id), fields=list(edits.keys()))
    return await get_signal(signal_id, session)  # re-read for consistent response


@router.post("/{signal_id}/approve", response_model=ActionResponse)
async def approve_signal(
    signal_id: UUID,
    session: AsyncSession = Depends(get_session),
) -> ActionResponse:
    """Dispatch via the Entire-compatible broker, log, and resolve."""
    row = (
        await session.execute(
            text(
                """
                SELECT proposed_action, status
                FROM signals WHERE id = :sid
                """
            ),
            {"sid": signal_id},
        )
    ).first()
    if row is None:
        raise HTTPException(status_code=404, detail="signal not found")
    if row.status != "pending":
        raise HTTPException(
            status_code=409, detail=f"signal is {row.status}, cannot approve"
        )

    action = row.proposed_action or {}
    if isinstance(action, str):
        action = json.loads(action)
    if not action:
        raise HTTPException(status_code=422, detail="signal has no proposed_action")

    broker = get_broker()
    result = await broker.dispatch(
        session,
        signal_id=signal_id,
        channel=str(action.get("channel") or "email"),
        recipient=str(action.get("recipient") or "owner@keystone.demo"),
        subject=action.get("subject"),
        body=str(action.get("drafted_message") or ""),
    )

    await session.execute(
        text(
            """
            UPDATE signals
            SET status = 'resolved', resolved_at = now()
            WHERE id = :sid
            """
        ),
        {"sid": signal_id},
    )
    await session.execute(
        text(
            """
            INSERT INTO approval_log (signal_id, decision)
            VALUES (:sid, 'approved')
            """
        ),
        {"sid": signal_id},
    )
    await session.commit()
    log.info(
        "signals.approve",
        signal_id=str(signal_id),
        outbox_id=str(result.outbox_id),
        broker=broker.name,
    )
    return ActionResponse(
        signal_id=signal_id,
        status="resolved",
        outbox_id=result.outbox_id,
        dispatched_to=result.recipient,
    )


@router.post("/{signal_id}/reject", response_model=ActionResponse)
async def reject_signal(
    signal_id: UUID,
    session: AsyncSession = Depends(get_session),
) -> ActionResponse:
    """Mark a signal as rejected and log the decision."""
    result = await session.execute(
        text(
            """
            UPDATE signals
            SET status = 'rejected', resolved_at = now()
            WHERE id = :sid AND status = 'pending'
            RETURNING id
            """
        ),
        {"sid": signal_id},
    )
    row = result.first()
    if row is None:
        raise HTTPException(status_code=409, detail="signal not pending or not found")

    await session.execute(
        text(
            """
            INSERT INTO approval_log (signal_id, decision)
            VALUES (:sid, 'rejected')
            """
        ),
        {"sid": signal_id},
    )
    await session.commit()
    log.info("signals.reject", signal_id=str(signal_id))
    return ActionResponse(signal_id=signal_id, status="rejected")
