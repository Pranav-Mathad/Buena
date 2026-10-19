"""``POST /draft-reply``: context-aware reply drafting.

The frontend's DraftReply page submits an inbound email or Slack message;
this endpoint pulls the property's recent facts and events, asks the
configured LLM provider to draft a reply that cites prior context where
relevant, and returns the structured payload the page renders.

The endpoint returns 200 whenever the LLM call succeeds, even when no
relevant context exists (``knows_about_incident=false``, ``context=[]``).
It returns 503 when no provider is configured or the call fails.
"""

from __future__ import annotations

import time
from typing import Any
from uuid import UUID

import structlog
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from backend.db.session import get_session
from backend.services import draft_reply as draft_reply_service
from backend.services.llm import LLMUnavailableError

router = APIRouter(prefix="/draft-reply", tags=["draft-reply"])
log = structlog.get_logger(__name__)


class DraftReplyRequest(BaseModel):
    """Payload for ``POST /draft-reply``."""

    property_id: UUID | None = Field(
        default=None,
        description=(
            "Authoritative property reference. When omitted, the "
            "endpoint falls back to ``property_name`` for a best-"
            "effort lookup."
        ),
    )
    property_name: str | None = Field(
        default=None,
        description="Fallback name lookup when ``property_id`` is absent.",
    )
    channel: str = Field(default="Email", description="Email | Slack")
    tone: str = Field(default="Neutral", description="Warm | Neutral | Formal")
    subject: str | None = None
    body: str = Field(..., min_length=1)


class DraftContextItem(BaseModel):
    """One context citation in the drafted reply."""

    id: str
    channel: str
    time: str
    snippet: str
    why_relevant: str = Field(default="", alias="whyRelevant")

    model_config = {"populate_by_name": True}


class DraftReplyResponse(BaseModel):
    """Response shape consumed by the frontend's ``DraftReply`` page."""

    subject: str
    body: str
    context: list[DraftContextItem]
    knows_about_incident: bool
    elapsed_ms: float
    model: str


async def _resolve_property(
    session: AsyncSession,
    *,
    property_id: UUID | None,
    property_name: str | None,
) -> tuple[UUID | None, str]:
    """Resolve the request's property reference.

    Returns ``(resolved_id, display_name)``. ``resolved_id`` may be
    ``None`` when the caller supplied only a name that doesn't match
    any seeded property; in that case we still return the supplied
    name so the LLM can draft a generic holding reply.
    """
    if property_id is not None:
        row = (
            await session.execute(
                text("SELECT name FROM properties WHERE id = :pid"),
                {"pid": property_id},
            )
        ).first()
        if row is None:
            raise HTTPException(status_code=404, detail="property not found")
        return property_id, str(row.name)

    if property_name:
        row = (
            await session.execute(
                text(
                    """
                    SELECT id, name FROM properties
                    WHERE LOWER(name) = LOWER(:n)
                    LIMIT 1
                    """
                ),
                {"n": property_name},
            )
        ).first()
        if row is not None:
            return row.id, str(row.name)
        return None, property_name

    raise HTTPException(
        status_code=422,
        detail="either property_id or property_name is required",
    )


async def _fetch_context(
    session: AsyncSession, property_id: UUID
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Pull the property's current facts + recent events for the prompt."""
    fact_rows = (
        await session.execute(
            text(
                """
                SELECT f.section, f.field, f.value,
                       COALESCE(f.created_at, now()) AS dt
                FROM facts f
                WHERE f.property_id = :pid
                  AND f.superseded_by IS NULL
                ORDER BY f.created_at DESC
                LIMIT 30
                """
            ),
            {"pid": property_id},
        )
    ).all()
    facts = [
        {
            "section": str(r.section),
            "field": str(r.field),
            "value": str(r.value),
            "date": r.dt.date().isoformat(),
        }
        for r in fact_rows
    ]

    event_rows = (
        await session.execute(
            text(
                """
                SELECT e.id, e.source,
                       COALESCE(e.received_at, e.processed_at, now()) AS rec,
                       LEFT(COALESCE(e.raw_content, ''), 240) AS snippet
                FROM events e
                WHERE e.property_id = :pid
                  AND COALESCE(e.received_at, e.processed_at) > now() - interval '90 days'
                ORDER BY rec DESC
                LIMIT 10
                """
            ),
            {"pid": property_id},
        )
    ).all()
    events = [
        {
            "id": str(r.id),
            "source": str(r.source),
            "received_at": r.rec.isoformat() if r.rec else "",
            "snippet": str(r.snippet or "").replace("\n", " ").strip(),
        }
        for r in event_rows
    ]

    return facts, events


@router.post("", response_model=DraftReplyResponse)
async def draft_reply(
    payload: DraftReplyRequest,
    session: AsyncSession = Depends(get_session),
) -> DraftReplyResponse:
    """Draft a context-aware reply to an inbound tenant/owner message."""
    started = time.perf_counter()

    resolved_id, display_name = await _resolve_property(
        session,
        property_id=payload.property_id,
        property_name=payload.property_name,
    )

    facts: list[dict[str, Any]] = []
    events: list[dict[str, Any]] = []
    if resolved_id is not None:
        facts, events = await _fetch_context(session, resolved_id)

    try:
        result = await draft_reply_service.draft_inbound_reply(
            property_name=display_name,
            inbound_channel=payload.channel,
            inbound_subject=payload.subject or "",
            inbound_body=payload.body,
            tone=payload.tone,
            recent_facts=facts,
            recent_events=events,
        )
    except LLMUnavailableError as exc:
        log.warning("draft_reply.unavailable", error=str(exc)[:300])
        raise HTTPException(
            status_code=503,
            detail=f"Reply drafting is unavailable: {exc}",
        ) from exc

    context_items = [
        DraftContextItem(
            id=str(item.get("id") or ""),
            channel=str(item.get("channel") or "email"),
            time=str(item.get("time") or ""),
            snippet=str(item.get("snippet") or "")[:240],
            why_relevant=str(item.get("why_relevant") or item.get("whyRelevant") or "")[:200],
        )
        for item in result["context"]
    ]

    elapsed_ms = (time.perf_counter() - started) * 1000
    log.info(
        "draft_reply.respond",
        property_id=str(resolved_id) if resolved_id else None,
        knows_about_incident=result["knows_about_incident"],
        context_count=len(context_items),
        total_elapsed_ms=round(elapsed_ms, 1),
    )

    return DraftReplyResponse(
        subject=result["subject"],
        body=result["body"],
        context=context_items,
        knows_about_incident=result["knows_about_incident"],
        elapsed_ms=elapsed_ms,
        model=str(result.get("model") or ""),
    )
