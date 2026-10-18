"""The property's raw message inbox, searchable by keyword (used by the Compare page)."""

from __future__ import annotations

from uuid import UUID

import structlog
from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from backend.db.session import get_session

router = APIRouter()
log = structlog.get_logger(__name__)


class InboxEmail(BaseModel):
    """One raw email row for the "before Keystone" Compare view."""

    event_id: UUID
    date: str
    sender: str
    subject: str
    preview: str
    relevant: bool = Field(
        default=False,
        description=(
            "True when the search keyword(s) appeared in the raw body. "
            "Used to highlight matching rows visually."
        ),
    )


class InboxResponse(BaseModel):
    """Response for ``GET /properties/{id}/inbox``."""

    rows: list[InboxEmail]
    total_inbox_size: int = Field(
        ...,
        description="Total email events for this property (the haystack size).",
    )
    relevant_count: int
    keyword: str | None


def _extract_subject_and_from(raw: str) -> tuple[str, str]:
    """Cheap RFC-822-ish subject/from parser. Falls back to first line."""
    subject = ""
    sender = ""
    for line in raw.splitlines():
        lower = line.lower()
        if not subject and lower.startswith("subject:"):
            subject = line.split(":", 1)[1].strip()
        elif not sender and lower.startswith("from:"):
            sender = line.split(":", 1)[1].strip()
        if subject and sender:
            break
    if not subject:
        subject = raw.strip().split("\n", 1)[0][:80]
    return subject, sender


@router.get("/{property_id}/inbox", response_model=InboxResponse)
async def property_inbox(
    property_id: UUID,
    q: str | None = Query(
        default=None,
        description="Optional keyword. Rows whose raw body matches are flagged ``relevant=True``.",
    ),
    limit: int = Query(default=20, ge=1, le=100),
    session: AsyncSession = Depends(get_session),
) -> InboxResponse:
    """Email-style inbox view for the Compare page's "before" panel.

    Returns the property's most-recent email events with a parsed
    ``subject`` / ``from`` / ``preview``. When ``q`` is supplied the
    LIKE filter highlights matches but the list still shows the
    surrounding noise. The Compare page sets this manual workflow, where
    the operator scrolls through everything, beside the single answer
    the property file gives.
    """
    total = (
        await session.execute(
            text(
                """
                SELECT COUNT(*) FROM events
                WHERE property_id = :pid AND source = 'email'
                """
            ),
            {"pid": property_id},
        )
    ).scalar_one()

    pattern = f"%{q}%" if q else None
    relevant_count = 0
    if pattern:
        relevant_count = (
            await session.execute(
                text(
                    """
                    SELECT COUNT(*) FROM events
                    WHERE property_id = :pid AND source = 'email'
                      AND raw_content ILIKE :pat
                    """
                ),
                {"pid": property_id, "pat": pattern},
            )
        ).scalar_one()

    rows = (
        await session.execute(
            text(
                """
                SELECT id,
                       COALESCE(received_at, processed_at) AS dt,
                       LEFT(COALESCE(raw_content, ''), 600) AS body,
                       (CASE WHEN CAST(:pat AS text) IS NOT NULL
                             AND raw_content ILIKE CAST(:pat AS text)
                             THEN TRUE ELSE FALSE END) AS is_relevant
                FROM events
                WHERE property_id = :pid AND source = 'email'
                ORDER BY COALESCE(received_at, processed_at) DESC
                LIMIT :lim
                """
            ),
            {"pid": property_id, "pat": pattern, "lim": limit},
        )
    ).all()

    items: list[InboxEmail] = []
    for r in rows:
        body = str(r.body or "")
        subject, sender = _extract_subject_and_from(body)
        body_text = body
        # Strip headers from preview
        for hdr in ("From:", "Subject:", "Date:", "To:", "Message-ID:"):
            for line in body_text.splitlines():
                if line.lower().startswith(hdr.lower()):
                    body_text = body_text.replace(line + "\n", "")
        preview = body_text.replace("\n", " ").strip()[:120]
        items.append(
            InboxEmail(
                event_id=r.id,
                date=r.dt.date().isoformat() if r.dt else "",
                sender=sender or "(unknown)",
                subject=subject or "(no subject)",
                preview=preview,
                relevant=bool(r.is_relevant),
            )
        )

    log.info(
        "properties.inbox",
        property_id=str(property_id),
        q=q,
        returned=len(items),
        total=int(total),
        relevant=int(relevant_count),
    )
    return InboxResponse(
        rows=items,
        total_inbox_size=int(total),
        relevant_count=int(relevant_count),
        keyword=q,
    )
