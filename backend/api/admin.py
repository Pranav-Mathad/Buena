"""Admin surface — unrouted inbox, failed events, replay control.

Step 3 ships ``GET /admin/unrouted`` so backfilled events that lacked
the IDs needed for routing surface as a real product UX (per Phase 8
plan: this is *core* product, not demo polish). Steps 7 and 9 expand
the surface to include incremental-cursor controls and admin
overrides.
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

from backend.db.session import get_session

router = APIRouter(prefix="/admin", tags=["admin"])
log = structlog.get_logger(__name__)


class UnroutedEvent(BaseModel):
    """One row of the unrouted-inbox listing."""

    event_id: UUID
    source: str
    source_ref: str | None = None
    received_at: datetime
    snippet: str = Field(
        ...,
        description="First ~120 chars of raw_content for human triage.",
    )
    metadata: dict[str, Any]
    suggested_alias: str | None = Field(
        default=None,
        description=(
            "Best guess alias to look up — surfaces metadata.eh_id / mie_id / "
            "invoice_ref so the operator knows what reference was tried."
        ),
    )


class UnroutedResponse(BaseModel):
    """Response envelope for ``GET /admin/unrouted``."""

    total: int
    by_source: dict[str, int]
    events: list[UnroutedEvent]


def _suggested_alias(metadata: dict[str, Any]) -> str | None:
    """Surface the strongest ID that the routing failed to resolve."""
    for key in ("eh_id", "mie_id", "invoice_ref", "buena_referenz_id"):
        value = metadata.get(key)
        if value:
            return str(value)
    return None


@router.get("/unrouted", response_model=UnroutedResponse)
async def list_unrouted(
    source: str | None = Query(
        default=None,
        description="Filter by event source (e.g. 'bank' or 'invoice').",
    ),
    limit: int = Query(default=100, ge=1, le=500),
    session: AsyncSession = Depends(get_session),
) -> UnroutedResponse:
    """List events with ``property_id IS NULL`` for human triage.

    Includes a per-source breakdown so the operator can see at a glance
    where the routing miss rate concentrates (e.g. shared-service bank
    payments without an EH-/MIE- reference).
    """
    breakdown_rows = (
        await session.execute(
            text(
                """
                SELECT source, COUNT(*) AS n
                FROM events
                WHERE property_id IS NULL
                GROUP BY source
                ORDER BY n DESC
                """
            )
        )
    ).all()
    by_source = {row.source: int(row.n) for row in breakdown_rows}

    params: dict[str, Any] = {"lim": limit}
    where = "WHERE property_id IS NULL"
    if source:
        where += " AND source = :source"
        params["source"] = source

    rows = (
        await session.execute(
            text(
                f"""
                SELECT id, source, source_ref, received_at, raw_content, metadata
                FROM events
                {where}
                ORDER BY received_at DESC
                LIMIT :lim
                """
            ),
            params,
        )
    ).all()

    events = [
        UnroutedEvent(
            event_id=r.id,
            source=r.source,
            source_ref=r.source_ref,
            received_at=r.received_at,
            snippet=(r.raw_content or "")[:160],
            metadata=dict(r.metadata or {}),
            suggested_alias=_suggested_alias(dict(r.metadata or {})),
        )
        for r in rows
    ]
    log.info(
        "admin.unrouted",
        total=sum(by_source.values()),
        source_filter=source,
        returned=len(events),
    )
    return UnroutedResponse(
        total=sum(by_source.values()),
        by_source=by_source,
        events=events,
    )


# -----------------------------------------------------------------------------
# Step 7 — Buena incremental cursor
# -----------------------------------------------------------------------------


class CursorStatus(BaseModel):
    """Shape returned by the cursor endpoints."""

    current_day: int
    next_day: int | None
    total_days: int
    exhausted: bool


class AdvanceResponse(CursorStatus):
    """Full advance-one-day result for the demo / admin UI."""

    events_inserted: int = 0
    facts_written: int = 0
    routed_property: int = 0
    routed_building: int = 0
    routed_liegenschaft: int = 0
    unrouted: int = 0
    signals_fired: int = 0
    error_samples: list[str] = Field(default_factory=list)


@router.get("/buena/cursor_status", response_model=CursorStatus)
async def cursor_status() -> CursorStatus:
    """Return the current Buena incremental-feed day cursor."""
    from connectors.incremental_runner import (  # noqa: PLC0415 — local import
        get_cursor_status,
    )

    status = await get_cursor_status()
    return CursorStatus(**status)


@router.post("/buena/advance_day", response_model=AdvanceResponse)
async def advance_day() -> AdvanceResponse:
    """Advance the Buena cursor by one day and process that day's deltas.

    Latency budget: < 3 s. Each Buena day is a small batch (~6 events)
    plus one signal-evaluator pass. Future customers with heavier days
    can move ``evaluate_all`` to a background task without changing
    the response shape.
    """
    from connectors.incremental_runner import (  # noqa: PLC0415
        TOTAL_DAYS,
        advance_one_day,
    )

    result = await advance_one_day()
    return AdvanceResponse(
        current_day=result.day,
        next_day=result.day + 1 if result.day < TOTAL_DAYS else None,
        total_days=TOTAL_DAYS,
        exhausted=result.exhausted,
        events_inserted=result.events_inserted,
        facts_written=result.facts_written,
        routed_property=result.routed_property,
        routed_building=result.routed_building,
        routed_liegenschaft=result.routed_liegenschaft,
        unrouted=result.unrouted,
        signals_fired=result.signals_fired,
        error_samples=list(result.error_samples),
    )


@router.post("/buena/reset_cursor", response_model=CursorStatus)
async def reset_buena_cursor() -> CursorStatus:
    """Reset the Buena cursor to ``0``. Used by demo-reset flows."""
    from connectors.incremental_runner import (  # noqa: PLC0415
        reset_cursor,
    )

    status = await reset_cursor()
    return CursorStatus(**status)
