"""Debug + manual-trigger event endpoints.

``POST /debug/trigger_event`` is the demo-resilience backup when IMAP fails
on venue wifi. It accepts the same raw email payload the poller would
produce and inserts it idempotently.
"""

from __future__ import annotations

import uuid
from typing import Any

import structlog
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from backend.db.session import get_session
from backend.pipeline.events import insert_event
from backend.pipeline.worker import process_batch

router = APIRouter(prefix="/debug", tags=["debug"])
log = structlog.get_logger(__name__)


class TriggerEventRequest(BaseModel):
    """Payload for a manually injected event."""

    source: str = Field(default="email")
    source_ref: str | None = Field(
        default=None,
        description="Optional idempotency key. Auto-generated if omitted.",
    )
    from_: str | None = Field(default=None, alias="from")
    subject: str | None = None
    body: str
    metadata: dict[str, Any] = Field(default_factory=dict)

    model_config = {"populate_by_name": True}


class TriggerEventResponse(BaseModel):
    """Response for the debug/trigger_event endpoint."""

    event_id: uuid.UUID
    inserted: bool
    processed: int


def _build_raw_content(req: TriggerEventRequest) -> str:
    """Assemble the ``raw_content`` in the same shape IMAP produces."""
    from_line = f"From: {req.from_}\n" if req.from_ else ""
    subject_line = f"Subject: {req.subject}\n" if req.subject else ""
    return f"{from_line}{subject_line}\n{req.body}".strip() + "\n"


@router.post("/trigger_event", response_model=TriggerEventResponse)
async def trigger_event(
    payload: TriggerEventRequest,
    session: AsyncSession = Depends(get_session),
) -> TriggerEventResponse:
    """Insert an event synchronously and kick the worker once."""
    if not payload.body.strip():
        raise HTTPException(status_code=400, detail="body must not be empty")

    source_ref = payload.source_ref or f"debug-{uuid.uuid4()}"
    event_id, inserted = await insert_event(
        session,
        source=payload.source,
        source_ref=source_ref,
        raw_content=_build_raw_content(payload),
        metadata=payload.metadata,
    )
    await session.commit()

    # Process immediately so `curl` sees updated markdown inside the same call.
    processed = await process_batch(max_events=5)

    log.info(
        "debug.trigger_event",
        event_id=str(event_id),
        inserted=inserted,
        processed=processed,
    )
    return TriggerEventResponse(
        event_id=event_id, inserted=inserted, processed=processed
    )
