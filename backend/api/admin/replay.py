"""Replay engine controls: re-ingest a property's history at a chosen speed."""

from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

import structlog
from fastapi import APIRouter
from pydantic import BaseModel, Field

router = APIRouter()
log = structlog.get_logger(__name__)


class ScheduledPause(BaseModel):
    """One ``scheduled_pauses`` checkpoint for a replay run."""

    at_seconds: float = Field(..., ge=0)
    message: str = Field(..., max_length=200)


class ReplayStartRequest(BaseModel):
    """Body for ``POST /admin/replay/start``."""

    property_id: UUID
    speed_multiplier: int = Field(default=10, ge=1, le=10000)
    source_filter: list[str] = Field(default_factory=lambda: ["email", "invoice", "bank"])
    start_date: datetime | None = None
    end_date: datetime | None = None
    scheduled_pauses: list[ScheduledPause] = Field(default_factory=list)
    reset_property: bool = Field(default=False)


class ReplayRunStatus(BaseModel):
    """Snapshot of one ``replay_runs`` row for the API."""

    run_id: UUID
    property_id: UUID
    speed_multiplier: int
    source_filter: list[str]
    start_date: datetime | None = None
    end_date: datetime | None = None
    scheduled_pauses: list[dict[str, Any]] = Field(default_factory=list)
    reset_property: bool = False
    status: str
    total_events: int
    processed_events: int
    last_error: str | None = None
    started_at: datetime
    paused_at: datetime | None = None
    completed_at: datetime | None = None


@router.post("/replay/start", response_model=ReplayRunStatus)
async def replay_start(body: ReplayStartRequest) -> ReplayRunStatus:
    """Spin up a replay run for ``property_id``.

    Streams events through the live pipeline at ``speed_multiplier`` ×
    real time. ``reset_property=True`` wipes facts / uncertainties /
    rejections for that property first so the replay rebuilds the
    property file from "day zero".
    Stammdaten links are preserved.
    """
    from backend.services import replay  # noqa: PLC0415

    snapshot = await replay.start_run(
        property_id=body.property_id,
        speed_multiplier=body.speed_multiplier,
        source_filter=list(body.source_filter),
        start_date=body.start_date,
        end_date=body.end_date,
        scheduled_pauses=[p.model_dump() for p in body.scheduled_pauses],
        reset_property=body.reset_property,
    )
    return ReplayRunStatus(**snapshot)


@router.post("/replay/{run_id}/pause", response_model=ReplayRunStatus)
async def replay_pause(run_id: UUID) -> ReplayRunStatus:
    """Pause a running replay; the in-flight event finishes first."""
    from backend.services import replay  # noqa: PLC0415

    return ReplayRunStatus(**await replay.pause_run(run_id))


@router.post("/replay/{run_id}/resume", response_model=ReplayRunStatus)
async def replay_resume(run_id: UUID) -> ReplayRunStatus:
    """Resume a paused replay."""
    from backend.services import replay  # noqa: PLC0415

    return ReplayRunStatus(**await replay.resume_run(run_id))


@router.post("/replay/{run_id}/stop", response_model=ReplayRunStatus)
async def replay_stop(run_id: UUID) -> ReplayRunStatus:
    """Stop a replay; durable status flips to ``stopped``."""
    from backend.services import replay  # noqa: PLC0415

    return ReplayRunStatus(**await replay.stop_run(run_id))


@router.get("/replay/{run_id}/status", response_model=ReplayRunStatus)
async def replay_status(run_id: UUID) -> ReplayRunStatus:
    """Read the current durable status of a replay run."""
    from backend.services import replay  # noqa: PLC0415

    try:
        snapshot = await replay.get_run_status(run_id)
    except LookupError as exc:
        from fastapi import HTTPException  # noqa: PLC0415

        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return ReplayRunStatus(**snapshot)


@router.post("/replay/featured", response_model=ReplayRunStatus)
async def replay_featured_property() -> ReplayRunStatus:
    """Replay the featured property's history with preset parameters.

    A body-free shortcut for :func:`replay.start_run`:

    - property: ``FEATURED_PROPERTY`` (UUID or alias, default ``EH-029``)
    - speed: ``REPLAY_SPEED_MULTIPLIER`` (default 10)
    - sources: email, invoice, bank; the property's facts are reset first
    - one checkpoint pause at 50 s, where the replayed history reaches the
      email that the floor-count constraint rejects

    Use ``POST /admin/replay/start`` for any other combination.
    """
    from fastapi import HTTPException  # noqa: PLC0415

    from backend.config import get_settings  # noqa: PLC0415
    from backend.db.session import get_sessionmaker  # noqa: PLC0415
    from backend.services import replay  # noqa: PLC0415
    from backend.services.featured import resolve_featured_property_id  # noqa: PLC0415

    settings = get_settings()
    async with get_sessionmaker()() as session:
        property_id = await resolve_featured_property_id(session)
    if property_id is None:
        raise HTTPException(
            status_code=404,
            detail=f"FEATURED_PROPERTY={settings.featured_property!r} matches no property",
        )
    snapshot = await replay.start_run(
        property_id=property_id,
        speed_multiplier=settings.replay_speed_multiplier,
        source_filter=["email", "invoice", "bank"],
        scheduled_pauses=[{"at_seconds": 50.0, "message": "constraint checkpoint"}],
        reset_property=True,
    )
    log.info(
        "admin.replay_featured.started",
        run_id=snapshot["run_id"],
        property_id=snapshot["property_id"],
    )
    return ReplayRunStatus(**snapshot)
