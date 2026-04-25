"""``/events/<id>/...`` — resolve a fact's source link to the original document.

Phase 10 Step 10.4. The renderer emits ``[source: <id>](/events/<id>/source)``
for every fact and uncertainty line. This router dispatches based on
the event's ``source`` field:

- ``invoice`` / ``letter`` PDFs → 307 redirect to ``/files/<original_path>``
- ``email`` → 307 redirect to ``/events/<id>/raw`` (text/plain)
- ``bank`` → 307 redirect to ``/events/<id>/detail`` (JSON envelope)
- anything else → fall through to ``/raw``

The redirect indirection keeps the rendered markdown stable across
source-type evolutions: the link string never changes, only what it
resolves to.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

import structlog
from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import PlainTextResponse, RedirectResponse
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from backend.db.session import get_session

router = APIRouter(prefix="/events", tags=["events"])
log = structlog.get_logger(__name__)


class EventDetail(BaseModel):
    """JSON envelope for ``GET /events/<id>/detail`` (bank + structured)."""

    event_id: UUID
    source: str
    source_ref: str | None
    received_at: datetime | None
    metadata: dict[str, Any]
    snippet: str


@router.get("/{event_id}/source")
async def event_source(
    event_id: UUID,
    session: AsyncSession = Depends(get_session),
) -> RedirectResponse:
    """Smart-redirect to the right surface for this event's source type.

    Looked up once; the redirect target is computed from
    ``event.source`` + ``event.metadata.original_path``. We use 307
    (temporary, preserve method) so a future write-side endpoint at
    the same path doesn't break clients.
    """
    row = (
        await session.execute(
            text(
                """
                SELECT source, metadata
                FROM events
                WHERE id = :eid
                """
            ),
            {"eid": event_id},
        )
    ).first()
    if row is None:
        raise HTTPException(status_code=404, detail="event not found")

    source = str(row.source or "")
    metadata = dict(row.metadata or {})

    if source in {"invoice", "letter"}:
        original_path = metadata.get("original_path")
        if isinstance(original_path, str) and original_path:
            log.info(
                "events.source.redirect",
                event_id=str(event_id),
                source=source,
                target="files",
                original_path=original_path,
            )
            return RedirectResponse(url=f"/files/{original_path}", status_code=307)
        # No path captured — fall through to raw so the operator sees
        # something rather than a dead link.

    if source == "bank":
        log.info(
            "events.source.redirect",
            event_id=str(event_id),
            source=source,
            target="detail",
        )
        return RedirectResponse(
            url=f"/events/{event_id}/detail", status_code=307
        )

    log.info(
        "events.source.redirect",
        event_id=str(event_id),
        source=source,
        target="raw",
    )
    return RedirectResponse(url=f"/events/{event_id}/raw", status_code=307)


@router.get("/{event_id}/raw", response_class=PlainTextResponse)
async def event_raw(
    event_id: UUID,
    session: AsyncSession = Depends(get_session),
) -> PlainTextResponse:
    """Return the event's ``raw_content`` as ``text/plain``.

    Used for emails and as a fallback for unknown source types. PDFs
    redirect through ``/source`` to ``/files`` — calling ``/raw``
    directly on a PDF event returns the redacted body text the
    extractor saw, which is occasionally useful for triage.
    """
    row = (
        await session.execute(
            text(
                "SELECT raw_content, source FROM events WHERE id = :eid"
            ),
            {"eid": event_id},
        )
    ).first()
    if row is None:
        raise HTTPException(status_code=404, detail="event not found")
    log.info(
        "events.raw",
        event_id=str(event_id),
        source=str(row.source),
        chars=len(row.raw_content or ""),
    )
    return PlainTextResponse(content=str(row.raw_content or ""))


@router.get("/{event_id}/detail", response_model=EventDetail)
async def event_detail(
    event_id: UUID,
    session: AsyncSession = Depends(get_session),
) -> EventDetail:
    """Return a JSON envelope for structured-source events (bank + similar).

    The renderer sends bank source-clicks here because the raw line
    item is more useful as JSON than as a free-text blob.
    """
    row = (
        await session.execute(
            text(
                """
                SELECT source, source_ref, received_at, metadata,
                       LEFT(raw_content, 240) AS snippet
                FROM events
                WHERE id = :eid
                """
            ),
            {"eid": event_id},
        )
    ).first()
    if row is None:
        raise HTTPException(status_code=404, detail="event not found")
    return EventDetail(
        event_id=event_id,
        source=str(row.source),
        source_ref=row.source_ref,
        received_at=row.received_at,
        metadata=dict(row.metadata or {}),
        snippet=str(row.snippet or "").replace("\n", " ").strip(),
    )
