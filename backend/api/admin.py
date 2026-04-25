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


# -----------------------------------------------------------------------------
# Phase 9 Step 9.2 — rejected_updates inbox
# -----------------------------------------------------------------------------


class RejectedUpdate(BaseModel):
    """One row of the rejected-updates inbox."""

    rejection_id: UUID
    event_id: UUID
    property_id: UUID | None = None
    proposed_section: str
    proposed_field: str
    proposed_value: str
    proposed_confidence: float | None = None
    constraint_name: str
    reason: str
    required_source_type: str | None = None
    reviewed_status: str
    created_at: datetime
    snippet: str = Field(
        default="",
        description="First ~160 chars of the originating event's raw_content.",
    )


class RejectedUpdatesResponse(BaseModel):
    """Response envelope for ``GET /properties/{id}/rejected``."""

    total: int
    by_status: dict[str, int]
    rows: list[RejectedUpdate]


class OverrideRequest(BaseModel):
    """Body for ``POST /rejected/{id}/override``."""

    reviewed_by: str = Field(..., min_length=1, max_length=200)
    reason: str = Field(..., min_length=4, max_length=2000)
    decision: str = Field(
        default="overridden",
        pattern=r"^(overridden|dismissed)$",
        description=(
            "``overridden`` applies the proposed update + writes an "
            "approval_log entry. ``dismissed`` keeps the original fact "
            "and marks the rejection closed."
        ),
    )


class OverrideResponse(BaseModel):
    """Result of an override action."""

    rejection_id: UUID
    decision: str
    reviewed_status: str
    reviewed_at: datetime
    fact_written: bool


@router.get(
    "/properties/{property_id}/rejected",
    response_model=RejectedUpdatesResponse,
)
async def list_rejected_for_property(
    property_id: UUID,
    status_filter: str | None = Query(
        default=None,
        alias="status",
        description="Filter by reviewed_status (pending / needs_review / overridden / dismissed).",
    ),
    limit: int = Query(default=100, ge=1, le=500),
    session: AsyncSession = Depends(get_session),
) -> RejectedUpdatesResponse:
    """List validator rejections targeted at one property, newest first."""
    breakdown = (
        await session.execute(
            text(
                """
                SELECT reviewed_status, COUNT(*) AS n
                FROM rejected_updates
                WHERE property_id = :pid
                GROUP BY reviewed_status
                """
            ),
            {"pid": property_id},
        )
    ).all()
    by_status = {row.reviewed_status: int(row.n) for row in breakdown}

    params: dict[str, Any] = {"pid": property_id, "lim": limit}
    where = "WHERE r.property_id = :pid"
    if status_filter:
        where += " AND r.reviewed_status = :st"
        params["st"] = status_filter

    rows = (
        await session.execute(
            text(
                f"""
                SELECT
                  r.id, r.event_id, r.property_id,
                  r.proposed_section, r.proposed_field, r.proposed_value,
                  r.proposed_confidence, r.constraint_name, r.reason,
                  r.required_source_type, r.reviewed_status, r.created_at,
                  COALESCE(LEFT(e.raw_content, 160), '') AS snippet
                FROM rejected_updates r
                LEFT JOIN events e ON e.id = r.event_id
                {where}
                ORDER BY r.created_at DESC
                LIMIT :lim
                """
            ),
            params,
        )
    ).all()

    items = [
        RejectedUpdate(
            rejection_id=r.id,
            event_id=r.event_id,
            property_id=r.property_id,
            proposed_section=r.proposed_section,
            proposed_field=r.proposed_field,
            proposed_value=r.proposed_value,
            proposed_confidence=(
                float(r.proposed_confidence)
                if r.proposed_confidence is not None
                else None
            ),
            constraint_name=r.constraint_name,
            reason=r.reason,
            required_source_type=r.required_source_type,
            reviewed_status=r.reviewed_status,
            created_at=r.created_at,
            snippet=str(r.snippet or ""),
        )
        for r in rows
    ]
    log.info(
        "admin.rejected.list",
        property_id=str(property_id),
        total=sum(by_status.values()),
        returned=len(items),
    )
    return RejectedUpdatesResponse(
        total=sum(by_status.values()),
        by_status=by_status,
        rows=items,
    )


@router.post(
    "/rejected/{rejection_id}/override",
    response_model=OverrideResponse,
)
async def override_rejection(
    rejection_id: UUID,
    body: OverrideRequest,
    session: AsyncSession = Depends(get_session),
) -> OverrideResponse:
    """Operator override on a rejected update.

    On ``decision="overridden"`` the proposed fact is written + an
    ``approval_log`` row records the audit trail (operator + reason).
    On ``decision="dismissed"`` the rejection is closed without
    writing the fact. Either way the ``rejected_updates`` row's
    ``reviewed_status`` advances and the original rejection stays
    visible for posterity.
    """
    row = (
        await session.execute(
            text(
                """
                SELECT id, event_id, property_id, proposed_section,
                       proposed_field, proposed_value, proposed_confidence,
                       constraint_name, reason, reviewed_status
                FROM rejected_updates
                WHERE id = :rid
                FOR UPDATE
                """
            ),
            {"rid": rejection_id},
        )
    ).first()
    if row is None:
        from fastapi import HTTPException  # noqa: PLC0415

        raise HTTPException(status_code=404, detail="rejection not found")
    if row.reviewed_status not in {"pending", "needs_review"}:
        from fastapi import HTTPException  # noqa: PLC0415

        raise HTTPException(
            status_code=409,
            detail=f"rejection is already {row.reviewed_status!r}",
        )

    fact_written = False
    if body.decision == "overridden" and row.property_id is not None:
        await session.execute(
            text(
                """
                INSERT INTO facts (
                  property_id, section, field, value, source_event_id,
                  confidence, valid_from
                ) VALUES (
                  :pid, :section, :field, :value, :eid, :conf, now()
                )
                """
            ),
            {
                "pid": row.property_id,
                "section": row.proposed_section,
                "field": row.proposed_field,
                "value": row.proposed_value,
                "eid": row.event_id,
                "conf": (
                    float(row.proposed_confidence)
                    if row.proposed_confidence is not None
                    else 0.7
                ),
            },
        )
        fact_written = True

    await session.execute(
        text(
            """
            INSERT INTO approval_log (
              actor, action, target_type, target_id, payload, created_at
            ) VALUES (
              :actor, :action, 'rejected_update', :tid, CAST(:payload AS JSONB), now()
            )
            """
        ),
        {
            "actor": body.reviewed_by,
            "action": f"rejection.{body.decision}",
            "tid": str(rejection_id),
            "payload": _json(
                {
                    "constraint_name": row.constraint_name,
                    "section": row.proposed_section,
                    "field": row.proposed_field,
                    "value": row.proposed_value,
                    "reason_for_override": body.reason,
                    "fact_written": fact_written,
                }
            ),
        },
    )
    await session.execute(
        text(
            """
            UPDATE rejected_updates
            SET reviewed_status = :st,
                reviewed_at = now(),
                reviewed_by = :by
            WHERE id = :rid
            RETURNING reviewed_at
            """
        ),
        {"st": body.decision, "by": body.reviewed_by, "rid": rejection_id},
    )
    new_row = (
        await session.execute(
            text(
                "SELECT reviewed_at FROM rejected_updates WHERE id = :rid"
            ),
            {"rid": rejection_id},
        )
    ).first()
    await session.commit()
    log.info(
        "admin.rejected.override",
        rejection_id=str(rejection_id),
        decision=body.decision,
        fact_written=fact_written,
        reviewed_by=body.reviewed_by,
    )
    return OverrideResponse(
        rejection_id=rejection_id,
        decision=body.decision,
        reviewed_status=body.decision,
        reviewed_at=new_row.reviewed_at if new_row else datetime.utcnow(),
        fact_written=fact_written,
    )


def _json(value: dict[str, Any]) -> str:
    """Serialize a small dict for SQL JSONB binding."""
    import json  # noqa: PLC0415

    return json.dumps(value)


# -----------------------------------------------------------------------------
# Phase 10 Step 10.2 — replay engine
# -----------------------------------------------------------------------------


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
    rejections for that property first so the demo opens at "day zero".
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


@router.post("/demo/replay", response_model=ReplayRunStatus)
async def demo_replay() -> ReplayRunStatus:
    """Thin wrapper that runs the canonical demo replay on the hero property.

    All parameters come from configuration:

    - ``KEYSTONE_DEMO_HERO_PROPERTY`` env var (default: WE 29 from
      Step 6's hero pin) → ``property_id``.
    - ``KEYSTONE_DEMO_SPEED_MULTIPLIER`` (default 10) →
      ``speed_multiplier``.
    - Hardcoded: ``source_filter=['email','invoice','bank']``,
      ``reset_property=True``, and a single
      ``scheduled_pauses[0] = {at_seconds: 50, message: 'validator beat'}``
      that drives the Phase 10 demo's Beat 3 (validator rejecting the
      "8 floors" email live).

    Everything is configurable via :func:`replay.start_run` for ad-hoc
    runs; this endpoint just removes the body so the demo button is a
    one-click affair.
    """
    from backend.config import get_settings  # noqa: PLC0415
    from backend.services import replay  # noqa: PLC0415

    settings = get_settings()
    snapshot = await replay.start_run(
        property_id=UUID(settings.keystone_demo_hero_property),
        speed_multiplier=settings.keystone_demo_speed_multiplier,
        source_filter=["email", "invoice", "bank"],
        scheduled_pauses=[
            {"at_seconds": 50.0, "message": "validator beat"},
        ],
        reset_property=True,
    )
    log.info(
        "admin.demo_replay.started",
        run_id=snapshot["run_id"],
        property_id=snapshot["property_id"],
    )
    return ReplayRunStatus(**snapshot)


# -----------------------------------------------------------------------------
# Phase 9 Step 9.1 — uncertainty inbox (read-only for now)
# -----------------------------------------------------------------------------


class UncertaintyItemModel(BaseModel):
    """One open uncertainty row, ready for the admin UI."""

    uncertainty_id: UUID
    event_id: UUID
    relevant_section: str | None = None
    relevant_field: str | None = None
    observation: str
    hypothesis: str | None = None
    reason_uncertain: str
    source: str
    status: str
    created_at: datetime


class UncertaintyResponse(BaseModel):
    """Response envelope for ``GET /admin/properties/{id}/uncertainties``."""

    total: int
    by_section: dict[str, int]
    by_source: dict[str, int]
    items: list[UncertaintyItemModel]


# -----------------------------------------------------------------------------
# Phase 10 Step 10.3 — onboarding view
# -----------------------------------------------------------------------------


class OnboardingResponse(BaseModel):
    """``GET /admin/properties/{id}/onboarding`` envelope."""

    property_id: UUID
    markdown: str
    cached: bool = Field(
        ...,
        description=(
            "True when the briefing came from the metadata.onboarding cache. "
            "False means Gemini Pro was invoked this request."
        ),
    )


@router.get(
    "/properties/{property_id}/onboarding",
    response_model=OnboardingResponse,
)
async def get_property_onboarding(
    property_id: UUID,
    session: AsyncSession = Depends(get_session),
) -> OnboardingResponse:
    """Render the first-time-read onboarding view for a property.

    Five sections: stammdaten + counts, open issues, Gemini Pro 12-month
    briefing (the only LLM call), recurring patterns, pointer index.
    The briefing is cached on ``properties.metadata.onboarding`` keyed by
    a hash of the latest fact/uncertainty/rejection timestamp — so a
    re-read is free, but any mutation invalidates.
    """
    from backend.services import onboarding  # noqa: PLC0415

    last_fact, last_unc, last_rej = await onboarding._last_mutation_timestamps(
        session, property_id
    )
    cache_key = onboarding._cache_key(last_fact, last_unc, last_rej)
    cached_briefing = await onboarding._read_cache(session, property_id, cache_key)

    try:
        markdown = await onboarding.render_onboarding(session, property_id)
    except ValueError as exc:
        from fastapi import HTTPException  # noqa: PLC0415

        raise HTTPException(status_code=404, detail=str(exc)) from exc

    log.info(
        "admin.onboarding.render",
        property_id=str(property_id),
        markdown_chars=len(markdown),
        cached=cached_briefing is not None,
    )
    return OnboardingResponse(
        property_id=property_id,
        markdown=markdown,
        cached=cached_briefing is not None,
    )


@router.get(
    "/properties/{property_id}/uncertainties",
    response_model=UncertaintyResponse,
)
async def list_uncertainties_for_property(
    property_id: UUID,
    status_filter: str = Query(
        default="open",
        alias="status",
        pattern=r"^(open|resolved|dismissed|all)$",
        description="Filter by status; ``all`` returns every row.",
    ),
    limit: int = Query(default=200, ge=1, le=500),
    session: AsyncSession = Depends(get_session),
) -> UncertaintyResponse:
    """List the property's open uncertainty events.

    Phase 10 Step 10.6 added the write surface
    (:func:`resolve_uncertainty`); this endpoint stays the read-side
    pair so a UI can list → click → resolve in one round-trip.
    """
    where = "WHERE property_id = :pid"
    params: dict[str, Any] = {"pid": property_id, "lim": limit}
    if status_filter != "all":
        where += " AND status = :st"
        params["st"] = status_filter

    breakdown_section = (
        await session.execute(
            text(
                f"""
                SELECT COALESCE(relevant_section, '(unsectioned)') AS section,
                       COUNT(*) AS n
                FROM uncertainty_events
                {where}
                GROUP BY COALESCE(relevant_section, '(unsectioned)')
                """
            ),
            params,
        )
    ).all()
    by_section = {row.section: int(row.n) for row in breakdown_section}

    breakdown_source = (
        await session.execute(
            text(
                f"""
                SELECT source, COUNT(*) AS n
                FROM uncertainty_events
                {where}
                GROUP BY source
                """
            ),
            params,
        )
    ).all()
    by_source = {row.source: int(row.n) for row in breakdown_source}

    rows = (
        await session.execute(
            text(
                f"""
                SELECT id, event_id, relevant_section, relevant_field,
                       observation, hypothesis, reason_uncertain,
                       source, status, created_at
                FROM uncertainty_events
                {where}
                ORDER BY created_at DESC
                LIMIT :lim
                """
            ),
            params,
        )
    ).all()

    items = [
        UncertaintyItemModel(
            uncertainty_id=row.id,
            event_id=row.event_id,
            relevant_section=row.relevant_section,
            relevant_field=row.relevant_field,
            observation=str(row.observation or ""),
            hypothesis=row.hypothesis,
            reason_uncertain=str(row.reason_uncertain or ""),
            source=str(row.source or "extractor"),
            status=str(row.status),
            created_at=row.created_at,
        )
        for row in rows
    ]
    log.info(
        "admin.uncertainties.list",
        property_id=str(property_id),
        status=status_filter,
        total=sum(by_section.values()),
        returned=len(items),
    )
    return UncertaintyResponse(
        total=sum(by_section.values()),
        by_section=by_section,
        by_source=by_source,
        items=items,
    )


# -----------------------------------------------------------------------------
# Phase 10 Step 10.6 — uncertainty action endpoints
# -----------------------------------------------------------------------------


class UncertaintyResolveRequest(BaseModel):
    """Body for ``POST /admin/uncertainties/{id}/resolve``."""

    action: str = Field(
        ...,
        pattern=r"^(promote_to_fact|dismiss)$",
        description=(
            "``promote_to_fact`` writes a fact at "
            "(relevant_section, relevant_field) using ``value`` (or "
            "``hypothesis`` as fallback) and marks the uncertainty "
            "``resolved``. ``dismiss`` closes the uncertainty without "
            "writing a fact."
        ),
    )
    value: str | None = Field(
        default=None,
        max_length=2000,
        description=(
            "Operator-supplied value to commit when promoting. Falls "
            "back to ``hypothesis`` from the uncertainty row if omitted. "
            "Required when promoting an uncertainty whose hypothesis is "
            "NULL."
        ),
    )
    reason: str = Field(..., min_length=3, max_length=2000)
    reviewed_by: str = Field(..., min_length=1, max_length=200)


class UncertaintyResolveResponse(BaseModel):
    """Result of a resolve action."""

    uncertainty_id: UUID
    action: str
    new_status: str
    fact_id: UUID | None = None
    resolved_at: datetime


@router.post(
    "/uncertainties/{uncertainty_id}/resolve",
    response_model=UncertaintyResolveResponse,
)
async def resolve_uncertainty(
    uncertainty_id: UUID,
    body: UncertaintyResolveRequest,
    session: AsyncSession = Depends(get_session),
) -> UncertaintyResolveResponse:
    """Promote an uncertainty to a fact, or dismiss it.

    Phase 10 Step 10.6: the trust layer's first write surface. Either
    action transitions the uncertainty out of ``open`` and records an
    ``approval_log`` entry naming the operator + reason — the audit
    trail is the whole point. ``promote_to_fact`` requires the
    uncertainty to carry both ``relevant_section`` and
    ``relevant_field``; the value comes from ``body.value`` or, if
    absent, from the uncertainty's ``hypothesis``. A 422 is returned
    when neither is available.

    The fact is written with ``confidence=0.95`` and ``source_event_id``
    pointing at the uncertainty's originating event so the source link
    keeps working.
    """
    from fastapi import HTTPException  # noqa: PLC0415

    row = (
        await session.execute(
            text(
                """
                SELECT id, event_id, property_id, building_id, liegenschaft_id,
                       relevant_section, relevant_field, observation,
                       hypothesis, reason_uncertain, status
                FROM uncertainty_events
                WHERE id = :uid
                FOR UPDATE
                """
            ),
            {"uid": uncertainty_id},
        )
    ).first()
    if row is None:
        raise HTTPException(status_code=404, detail="uncertainty not found")
    if row.status != "open":
        raise HTTPException(
            status_code=409,
            detail=f"uncertainty is already {row.status!r}",
        )

    fact_id: UUID | None = None
    written_value: str | None = None

    if body.action == "promote_to_fact":
        if not row.relevant_section or not row.relevant_field:
            raise HTTPException(
                status_code=422,
                detail=(
                    "cannot promote — uncertainty is missing relevant_section "
                    "or relevant_field"
                ),
            )
        # Pick the value: explicit override wins, hypothesis second.
        candidate = body.value if body.value not in (None, "") else row.hypothesis
        if not candidate:
            raise HTTPException(
                status_code=422,
                detail=(
                    "cannot promote — no value supplied and uncertainty has "
                    "no hypothesis to fall back on"
                ),
            )
        written_value = str(candidate)

        # Pick the scope column the fact should attach to. Property is the
        # default surface; building / liegenschaft are honoured if the
        # uncertainty was scoped that way.
        scope_column = "property_id"
        scope_value = row.property_id
        if row.property_id is None and row.building_id is not None:
            scope_column = "building_id"
            scope_value = row.building_id
        elif row.property_id is None and row.liegenschaft_id is not None:
            scope_column = "liegenschaft_id"
            scope_value = row.liegenschaft_id
        if scope_value is None:
            raise HTTPException(
                status_code=422,
                detail=(
                    "cannot promote — uncertainty has no property/building/"
                    "liegenschaft scope"
                ),
            )

        fact_row = (
            await session.execute(
                text(
                    f"""
                    INSERT INTO facts (
                      {scope_column}, section, field, value, source_event_id,
                      confidence, valid_from
                    ) VALUES (
                      :sid, :section, :field, :value, :eid, 0.95, now()
                    )
                    RETURNING id
                    """
                ),
                {
                    "sid": scope_value,
                    "section": row.relevant_section,
                    "field": row.relevant_field,
                    "value": written_value,
                    "eid": row.event_id,
                },
            )
        ).first()
        fact_id = UUID(str(fact_row.id))

        await session.execute(
            text(
                """
                UPDATE uncertainty_events
                SET status = 'resolved',
                    resolved_to_fact_id = :fid,
                    resolved_at = now()
                WHERE id = :uid
                """
            ),
            {"fid": fact_id, "uid": uncertainty_id},
        )
        new_status = "resolved"
    else:
        await session.execute(
            text(
                """
                UPDATE uncertainty_events
                SET status = 'dismissed',
                    resolved_at = now()
                WHERE id = :uid
                """
            ),
            {"uid": uncertainty_id},
        )
        new_status = "dismissed"

    await session.execute(
        text(
            """
            INSERT INTO approval_log (
              actor, action, target_type, target_id, payload, created_at
            ) VALUES (
              :actor, :action, 'uncertainty_event', :tid,
              CAST(:payload AS JSONB), now()
            )
            """
        ),
        {
            "actor": body.reviewed_by,
            "action": f"uncertainty.{body.action}",
            "tid": str(uncertainty_id),
            "payload": _json(
                {
                    "section": row.relevant_section,
                    "field": row.relevant_field,
                    "value_written": written_value,
                    "fact_id": str(fact_id) if fact_id else None,
                    "reason": body.reason,
                    "observation": str(row.observation or "")[:200],
                }
            ),
        },
    )
    resolved_row = (
        await session.execute(
            text("SELECT resolved_at FROM uncertainty_events WHERE id = :uid"),
            {"uid": uncertainty_id},
        )
    ).first()
    await session.commit()
    log.info(
        "admin.uncertainty.resolve",
        uncertainty_id=str(uncertainty_id),
        action=body.action,
        fact_id=str(fact_id) if fact_id else None,
        reviewed_by=body.reviewed_by,
    )
    return UncertaintyResolveResponse(
        uncertainty_id=uncertainty_id,
        action=body.action,
        new_status=new_status,
        fact_id=fact_id,
        resolved_at=(
            resolved_row.resolved_at if resolved_row else datetime.utcnow()
        ),
    )
