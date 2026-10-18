"""Property-scoped questions answered from the property file, with citations."""

from __future__ import annotations

from typing import Any
from uuid import UUID

import structlog
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from backend.db.session import get_session
from backend.pipeline.renderer import (
    render_markdown,
)
from backend.services import ask as ask_service
from backend.services.llm import LLMUnavailableError

router = APIRouter()
log = structlog.get_logger(__name__)


class AskRequest(BaseModel):
    """Payload for ``POST /properties/{id}/ask``."""

    question: str = Field(..., min_length=1, max_length=600)


class AskSourceItem(BaseModel):
    """Source citation used by both ``answered`` and ``insufficient_context``."""

    id: str
    channel: str
    date: str
    snippet: str


class AskResponse(BaseModel):
    """Q&A response with three status branches the frontend renders distinctly.

    Trust rule: an honest "I don't know" beats a confident
    hallucination, because an operator acting on a wrong answer costs
    more than one who goes and checks. The ``insufficient_context`` and ``out_of_scope``
    branches keep ``answer`` ``None`` and surface the partial-context
    or out-of-scope reasoning explicitly.
    """

    status: str  # 'answered' | 'insufficient_context' | 'out_of_scope'
    answer: str | None
    confidence: str
    reasoning: str = ""
    sources: list[AskSourceItem] = Field(default_factory=list)
    partial_context: list[AskSourceItem] = Field(default_factory=list)
    elapsed_ms: float
    retrieved_count: int
    model: str


async def _fetch_ask_context(
    session: AsyncSession, property_id: UUID
) -> tuple[list[dict[str, Any]], dict[str, dict[str, str]]]:
    """Pull recent events for citation + a lookup mapping event_id → citation shape.

    The canonical context for the LLM is the rendered property markdown
    (fetched separately via :func:`render_markdown`). This helper only
    returns the raw event snippets that let Pioneer quote source text
    directly; the citation_lookup joins those event ids back to the
    ``{id, channel, date, snippet}`` shape the frontend expects.
    """
    event_rows = (
        await session.execute(
            text(
                """
                SELECT e.id, e.source,
                       COALESCE(e.received_at, e.processed_at, now()) AS rec,
                       LEFT(COALESCE(e.raw_content, ''), 320) AS snippet
                FROM events e
                WHERE e.property_id = :pid
                  AND COALESCE(e.received_at, e.processed_at) > now() - interval '90 days'
                ORDER BY rec DESC
                LIMIT 12
                """
            ),
            {"pid": property_id},
        )
    ).all()
    events: list[dict[str, Any]] = []
    citation_lookup: dict[str, dict[str, str]] = {}
    for r in event_rows:
        eid = str(r.id)
        snippet = str(r.snippet or "").replace("\n", " ").strip()
        events.append(
            {
                "id": eid,
                "source": str(r.source),
                "received_at": r.rec.isoformat() if r.rec else "",
                "snippet": snippet,
            }
        )
        citation_lookup[eid] = {
            "id": eid,
            "channel": str(r.source),
            "date": r.rec.date().isoformat() if r.rec else "",
            "snippet": snippet[:240],
        }
    return events, citation_lookup


@router.post("/{property_id}/ask", response_model=AskResponse)
async def property_ask(
    property_id: UUID,
    payload: AskRequest,
    session: AsyncSession = Depends(get_session),
) -> AskResponse:
    """Answer a question scoped to one property's facts + recent events."""
    meta_row = (
        await session.execute(
            text("SELECT name FROM properties WHERE id = :pid"),
            {"pid": property_id},
        )
    ).first()
    if meta_row is None:
        raise HTTPException(status_code=404, detail="property not found")

    events, citation_lookup = await _fetch_ask_context(session, property_id)
    try:
        property_file_markdown = await render_markdown(session, property_id)
    except ValueError as exc:
        # Should be unreachable: the property existence check above
        # already 404s, but guard anyway so a half-built prompt is never sent.
        log.warning("properties.ask.render_failed", error=str(exc))
        raise HTTPException(status_code=404, detail="property not found") from exc

    try:
        result = await ask_service.answer_question(
            property_name=str(meta_row.name),
            question=payload.question,
            property_file_markdown=property_file_markdown,
            recent_events=events,
        )
    except LLMUnavailableError as exc:
        # Covers both "no provider configured" (the message names the missing
        # variable) and "provider call failed".
        log.warning("properties.ask.unavailable", error=str(exc)[:300])
        raise HTTPException(
            status_code=503,
            detail=f"Ask is unavailable: {exc}",
        ) from exc

    sources: list[AskSourceItem] = []
    partial_context: list[AskSourceItem] = []
    for eid in result["cited_event_ids"]:
        cite = citation_lookup.get(eid)
        if cite is not None:
            sources.append(AskSourceItem(**cite))
    for eid in result["partial_context_event_ids"]:
        cite = citation_lookup.get(eid)
        if cite is not None:
            partial_context.append(AskSourceItem(**cite))

    log.info(
        "properties.ask.respond",
        property_id=str(property_id),
        status=result["status"],
        sources=len(sources),
        partial=len(partial_context),
        retrieved=len(events),
    )

    return AskResponse(
        status=result["status"],
        answer=result["answer"],
        confidence=result["confidence"],
        reasoning=result["reasoning"],
        sources=sources,
        partial_context=partial_context,
        elapsed_ms=float(result["latency_ms"]),
        retrieved_count=len(events),
        model=str(result["model"]),
    )
