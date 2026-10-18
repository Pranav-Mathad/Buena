"""Incremental-feed controls: replay the sample dataset's ``incremental/`` days.

A day cursor records which day was ingested last; advancing it ingests the
next day's emails, bank rows and invoices through the normal pipeline.
"""

from __future__ import annotations

import structlog
from fastapi import APIRouter
from pydantic import BaseModel, Field

router = APIRouter()
log = structlog.get_logger(__name__)


class CursorStatus(BaseModel):
    """Shape returned by the cursor endpoints."""

    current_day: int
    next_day: int | None
    total_days: int
    exhausted: bool


class AdvanceResponse(CursorStatus):
    """Full advance-one-day result for the admin UI."""

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
    """Return the current incremental-feed day cursor."""
    from connectors.incremental_runner import (  # noqa: PLC0415
        get_cursor_status,
    )

    status = await get_cursor_status()
    return CursorStatus(**status)


@router.post("/buena/advance_day", response_model=AdvanceResponse)
async def advance_day() -> AdvanceResponse:
    """Advance the cursor by one day and process that day's deltas.

    Latency budget: < 3 s. Each day in the sample dataset is a small
    batch (~6 events) plus one signal-evaluator pass. Feeds with heavier
    days can move ``evaluate_all`` to a background task without changing
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
    """Reset the cursor to ``0`` so the incremental feed replays from day 1."""
    from connectors.incremental_runner import (  # noqa: PLC0415
        reset_cursor,
    )

    status = await reset_cursor()
    return CursorStatus(**status)
