"""Manual event injection.

``POST /debug/trigger_event`` inserts one event and runs it through the
pipeline synchronously, bypassing the queue. It is the way to feed the
system without a connector (no IMAP account, no archive) and the quickest
end-to-end check that routing, extraction and rendering work.

Two request shapes are accepted:

- ``raw_content``: the event text exactly as a connector would store it
  in ``events.raw_content``.
- ``from`` / ``subject`` / ``body``: composed into the same
  ``From:``/``Subject:``/body layout the IMAP poller produces.

Insertion is idempotent on ``(source, source_ref)``.
"""

from __future__ import annotations

import uuid
from typing import Any

import structlog
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field, model_validator
from sqlalchemy.ext.asyncio import AsyncSession

from backend.db.session import get_session
from backend.pipeline.events import insert_event
from backend.pipeline.worker import process_specific

router = APIRouter(prefix="/debug", tags=["debug"])
log = structlog.get_logger(__name__)


class TriggerEventRequest(BaseModel):
    """Payload for a manually injected event: ``raw_content`` or ``body``."""

    source: str = Field(default="email")
    source_ref: str | None = Field(
        default=None,
        description="Optional idempotency key. Auto-generated if omitted.",
    )
    raw_content: str | None = Field(
        default=None,
        description="Event text stored verbatim, as a connector would write it.",
    )
    from_: str | None = Field(default=None, alias="from")
    subject: str | None = None
    body: str | None = Field(
        default=None,
        description="Email body; combined with 'from' and 'subject' into raw_content.",
    )
    metadata: dict[str, Any] = Field(default_factory=dict)

    model_config = {"populate_by_name": True}

    @model_validator(mode="after")
    def _exactly_one_content_field(self) -> TriggerEventRequest:
        if (self.raw_content is None) == (self.body is None):
            raise ValueError("provide exactly one of 'raw_content' or 'body'")
        if self.raw_content is not None and (self.from_ or self.subject):
            raise ValueError("'from' and 'subject' only apply together with 'body'")
        return self


class TriggerEventResponse(BaseModel):
    """Response for the debug/trigger_event endpoint."""

    event_id: uuid.UUID
    inserted: bool
    processed: int


def _build_raw_content(req: TriggerEventRequest) -> str:
    """Return ``raw_content`` as given, or compose it the way IMAP does."""
    if req.raw_content is not None:
        return req.raw_content
    from_line = f"From: {req.from_}\n" if req.from_ else ""
    subject_line = f"Subject: {req.subject}\n" if req.subject else ""
    return f"{from_line}{subject_line}\n{req.body}".strip() + "\n"


@router.post("/trigger_event", response_model=TriggerEventResponse)
async def trigger_event(
    payload: TriggerEventRequest,
    session: AsyncSession = Depends(get_session),
) -> TriggerEventResponse:
    """Insert an event and process it end-to-end before responding.

    Processes this event directly instead of waiting for its turn in the
    queue, so the response reflects its outcome even when older events
    are still pending; the scheduled worker drains those independently.
    """
    content = payload.raw_content if payload.raw_content is not None else payload.body
    if not (content or "").strip():
        raise HTTPException(status_code=400, detail="event content must not be empty")

    source_ref = payload.source_ref or f"debug-{uuid.uuid4()}"
    event_id, inserted = await insert_event(
        session,
        source=payload.source,
        source_ref=source_ref,
        raw_content=_build_raw_content(payload),
        metadata=payload.metadata,
    )
    await session.commit()

    processed = 1 if await process_specific(event_id) else 0

    log.info(
        "debug.trigger_event",
        event_id=str(event_id),
        inserted=inserted,
        processed=processed,
    )
    return TriggerEventResponse(event_id=event_id, inserted=inserted, processed=processed)
