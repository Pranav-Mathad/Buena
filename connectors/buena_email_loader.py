"""Stream Buena ``.eml`` archive into events + facts.

Step 6 mirrors the structured backfill loop (see
:mod:`connectors.buena_event_loader`) but with two free-text-only
concerns:

- **Routing escalates** — :func:`backend.pipeline.router.route_text_event`
  applies the WEG keyword precedence so e.g. a Brandschutznachweis
  email lands at the Liegenschaft instead of being greedily claimed by
  a property's address token-overlap.
- **Extraction is cost-bounded** — every Gemini call charges the
  durable :mod:`connectors.cost_ledger`. When cumulative spend hits the
  cap the loop aborts cleanly and the next invocation refuses to start
  until the operator runs ``--reset-cost-ledger``. Resumability is
  free: ``events.UNIQUE (source, source_ref)`` absorbs re-runs.

Persistent failure (3 attempts that raise) is captured in the
``failed_events`` table for human triage in Step 9.
"""

from __future__ import annotations

import asyncio
from collections.abc import Iterable
from dataclasses import dataclass, field as dc_field
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any
from uuid import UUID

import structlog
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from backend.db.session import get_sessionmaker
from backend.pipeline.applier import apply as apply_plan
from backend.pipeline.differ import diff
from backend.pipeline.events import insert_event
from backend.pipeline.extractor import extract as run_extractor
from backend.pipeline.renderer import render_markdown
from backend.pipeline.router import StructuredRoute, route_text_event
from backend.services.gemini import ExtractionResult
from connectors import buena_archive, cost_ledger, eml_archive
from connectors.base import ConnectorEvent
from connectors.cost_ledger import CostCapExceeded
from connectors.migrations import apply_all as ensure_migrations

log = structlog.get_logger(__name__)


# Cost ledger label dedicated to Step 6 email backfill. Kept separate
# from ``step5_eval`` so the operator can read the email-only spend
# without subtracting other run costs.
LEDGER_LABEL = "step6_email_backfill"

# Gemini 2.5 Pro public pricing as of 2026-04-25 (USD per 1M tokens).
# Mirrors :mod:`eval.runner` so eval and production share rates.
PRO_PROMPT_USD_PER_M = Decimal("1.25")
PRO_COMPLETION_USD_PER_M = Decimal("10.0")
FLASH_PROMPT_USD_PER_M = Decimal("0.075")
FLASH_COMPLETION_USD_PER_M = Decimal("0.30")

# After successful extraction, events whose ``received_at`` is older
# than ``today − HISTORICAL_THRESHOLD_DAYS`` get stamped
# ``processed_at = received_at`` so Phase 9's validator/uncertainty
# layers don't silently re-process pre-validator extractions. New
# events stamp ``processed_at = now()`` as usual.
HISTORICAL_THRESHOLD_DAYS = 30


@dataclass
class EmailBackfillSummary:
    """Counters returned to the CLI."""

    label: str = "buena_email"
    total_seen: int = 0
    inserted_now: int = 0
    routed_property: int = 0
    routed_building: int = 0
    routed_liegenschaft: int = 0
    unrouted: int = 0
    extracted_facts: int = 0
    extraction_attempts: int = 0
    extractor_errors: int = 0
    failed_events: int = 0
    aborted_on_cost_cap: bool = False
    cumulative_usd: str = "0"
    cap_usd: str = "0"
    miss_reasons: dict[str, int] = dc_field(default_factory=dict)
    error_samples: list[str] = dc_field(default_factory=list)
    top_property_event_counts: dict[str, int] = dc_field(default_factory=dict)

    def as_json(self) -> dict[str, Any]:
        """Serializable snapshot for the CLI's ``--json`` mode."""
        return {
            "label": self.label,
            "total_seen": self.total_seen,
            "inserted_now": self.inserted_now,
            "routed_property": self.routed_property,
            "routed_building": self.routed_building,
            "routed_liegenschaft": self.routed_liegenschaft,
            "unrouted": self.unrouted,
            "extracted_facts": self.extracted_facts,
            "extraction_attempts": self.extraction_attempts,
            "extractor_errors": self.extractor_errors,
            "failed_events": self.failed_events,
            "aborted_on_cost_cap": self.aborted_on_cost_cap,
            "cumulative_usd": self.cumulative_usd,
            "cap_usd": self.cap_usd,
            "miss_reasons": dict(self.miss_reasons),
            "error_samples": list(self.error_samples),
            "top_property_event_counts": dict(self.top_property_event_counts),
        }


def _gemini_call_cost(model: str, prompt_tokens: int, completion_tokens: int) -> Decimal:
    """Convert token counts → USD using public 2.5 pricing."""
    is_pro = "pro" in model.lower()
    prompt_rate = PRO_PROMPT_USD_PER_M if is_pro else FLASH_PROMPT_USD_PER_M
    completion_rate = PRO_COMPLETION_USD_PER_M if is_pro else FLASH_COMPLETION_USD_PER_M
    return (
        Decimal(prompt_tokens) * prompt_rate / Decimal(1_000_000)
        + Decimal(completion_tokens) * completion_rate / Decimal(1_000_000)
    ).quantize(Decimal("0.000001"))


def _is_historical(received_at: datetime | None, *, threshold_days: int) -> bool:
    """``True`` if ``received_at`` is older than ``threshold_days`` ago.

    Matches the live worker's stamping policy so backfilled events
    don't re-run through Phase 9's validator/uncertainty layers.
    """
    if received_at is None:
        return False
    if received_at.tzinfo is None:
        received_at = received_at.replace(tzinfo=timezone.utc)
    age_days = (datetime.now(tz=timezone.utc) - received_at).days
    return age_days > threshold_days


async def _stamp_event(
    session: AsyncSession,
    *,
    event_id: UUID,
    route: StructuredRoute,
    received_at: datetime | None,
    historical: bool,
    error: str | None,
) -> None:
    """Persist scope + processed_at + processing_error in a single update."""
    if historical and received_at is not None:
        await session.execute(
            text(
                """
                UPDATE events
                SET property_id     = :pid,
                    building_id     = :bid,
                    liegenschaft_id = :lid,
                    processed_at    = :ts,
                    processing_error = :err
                WHERE id = :id
                """
            ),
            {
                "id": event_id,
                "pid": route.property_id,
                "bid": route.building_id,
                "lid": route.liegenschaft_id,
                "ts": received_at,
                "err": error,
            },
        )
    else:
        await session.execute(
            text(
                """
                UPDATE events
                SET property_id     = :pid,
                    building_id     = :bid,
                    liegenschaft_id = :lid,
                    processed_at    = now(),
                    processing_error = :err
                WHERE id = :id
                """
            ),
            {
                "id": event_id,
                "pid": route.property_id,
                "bid": route.building_id,
                "lid": route.liegenschaft_id,
                "err": error,
            },
        )


async def _should_retry(
    factory: Any,
    *,
    event_id: UUID,
    dead_letter_after: int,
) -> bool:
    """Decide whether to re-attempt an already-inserted event.

    True only when the event errored on a prior run AND its
    ``failed_events.retry_count`` is still below the dead-letter
    ceiling. Successfully-processed events stay quiet.
    """
    async with factory() as session:
        row = (
            await session.execute(
                text(
                    """
                    SELECT
                      e.processing_error,
                      e.processed_at,
                      COALESCE(f.retry_count, 0) AS retry_count
                    FROM events e
                    LEFT JOIN failed_events f ON f.event_id = e.id
                    WHERE e.id = :id
                    """
                ),
                {"id": event_id},
            )
        ).first()
    if row is None:
        return False
    has_error = bool(row.processing_error) or int(row.retry_count) > 0
    if not has_error:
        return False
    return int(row.retry_count) < dead_letter_after


async def _record_failure(
    session: AsyncSession,
    *,
    event_id: UUID,
    error: str,
    dead_letter_after: int,
) -> bool:
    """Increment the failed_events row for ``event_id``.

    Returns ``True`` when ``retry_count`` has hit (or exceeded) the
    configured ``dead_letter_after`` ceiling — at that point the row
    is the operator's signal that the event is permanently failed.
    """
    truncated = (error or "")[:1000]
    result = await session.execute(
        text(
            """
            INSERT INTO failed_events (event_id, retry_count, last_error)
            VALUES (:eid, 1, :err)
            ON CONFLICT (event_id) DO UPDATE
              SET retry_count = failed_events.retry_count + 1,
                  last_error  = EXCLUDED.last_error,
                  last_attempted_at = now()
            RETURNING retry_count
            """
        ),
        {"eid": event_id, "err": truncated},
    )
    retry_count = int(result.scalar_one())
    return retry_count >= dead_letter_after


async def _property_name(session: AsyncSession, property_id: UUID) -> str:
    """Lookup display name for the extractor prompt."""
    result = await session.execute(
        text("SELECT name FROM properties WHERE id = :pid"),
        {"pid": property_id},
    )
    row = result.first()
    return str(row.name) if row else "(unknown)"


async def _context_excerpt(session: AsyncSession, property_id: UUID) -> str:
    """First 30 lines of the property's current markdown."""
    markdown = await render_markdown(session, property_id)
    return "\n".join(markdown.splitlines()[:30])


async def _extract_and_apply(
    session: AsyncSession,
    *,
    event_id: UUID,
    source: str,
    raw_content: str,
    property_id: UUID,
) -> tuple[ExtractionResult, int]:
    """Run extractor → diff → apply for a property-routed event.

    Returns ``(result, facts_written)``. The caller is responsible for
    committing the session.
    """
    property_name = await _property_name(session, property_id)
    excerpt = await _context_excerpt(session, property_id)

    result = await run_extractor(
        property_name=property_name,
        current_context_excerpt=excerpt,
        source=source,
        raw_content=raw_content,
    )

    plan = await diff(
        session,
        property_id=property_id,
        event_source=source,
        proposals=result.facts_to_update,
    )
    written = await apply_plan(
        session,
        property_id=property_id,
        source_event_id=event_id,
        plan=plan,
    )
    return result, written


async def _process_event(
    factory: Any,
    ev: ConnectorEvent,
    *,
    summary: EmailBackfillSummary,
    label: str,
    cap_usd: Decimal,
    dead_letter_after: int,
    reprocess_historical: bool,
) -> bool:
    """Full single-event pipeline. Returns ``True`` to keep iterating.

    Returns ``False`` when the cost cap has been hit — the caller stops
    the outer loop. Persistent extraction failures don't stop the loop;
    they land in ``failed_events``.
    """
    summary.total_seen += 1

    # --- 1. Insert (idempotent) ---------------------------------------------
    async with factory() as session:
        event_id, inserted = await insert_event(
            session,
            source=ev.source,
            source_ref=ev.source_ref,
            raw_content=ev.raw_content,
            metadata=ev.metadata,
        )
        await session.commit()
        if inserted:
            summary.inserted_now += 1
        else:
            # Existing event. Two cases:
            #   a) successfully processed before → skip silently.
            #   b) previously errored, retry_count < dead_letter_after →
            #      re-attempt extraction so the dead-letter counter
            #      advances across invocations.
            should_retry = await _should_retry(
                factory,
                event_id=event_id,
                dead_letter_after=dead_letter_after,
            )
            if not should_retry:
                return True

    # --- 2. Route via the WEG-aware text router ----------------------------
    async with factory() as session:
        route: StructuredRoute = await route_text_event(
            session, ev.raw_content, metadata=ev.metadata
        )
        if route.property_id is not None:
            summary.routed_property += 1
        elif route.building_id is not None:
            summary.routed_building += 1
        elif route.liegenschaft_id is not None:
            summary.routed_liegenschaft += 1
        else:
            summary.unrouted += 1
            summary.miss_reasons[route.reason] = (
                summary.miss_reasons.get(route.reason, 0) + 1
            )

    historical = _is_historical(
        ev.received_at, threshold_days=HISTORICAL_THRESHOLD_DAYS
    )

    # --- 3. Extract facts (only when the event landed at a property) -------
    error_text: str | None = None
    if route.property_id is not None:
        # Cost-cap pre-flight: cleanest abort before issuing the call.
        state = cost_ledger.get_state(label)
        if state is not None and state.exhausted:
            log.warning(
                "buena_email.cost_cap_pre_check",
                cumulative=str(state.cumulative_usd),
                cap=str(state.cap_usd),
            )
            summary.aborted_on_cost_cap = True
            summary.cumulative_usd = str(state.cumulative_usd)
            summary.cap_usd = str(state.cap_usd)
            return False

        summary.extraction_attempts += 1
        try:
            async with factory() as session:
                result, written = await _extract_and_apply(
                    session,
                    event_id=event_id,
                    source=ev.source,
                    raw_content=ev.raw_content,
                    property_id=route.property_id,
                )
                await session.commit()
            summary.extracted_facts += int(written or 0)

            # Charge the ledger for any Gemini tokens consumed.
            if (
                getattr(result, "source", "") == "gemini"
                and getattr(result, "prompt_tokens", None) is not None
                and getattr(result, "completion_tokens", None) is not None
            ):
                cost = _gemini_call_cost(
                    str(getattr(result, "model", "") or ""),
                    int(result.prompt_tokens or 0),
                    int(result.completion_tokens or 0),
                )
                try:
                    cost_ledger.charge(label, cost)
                except CostCapExceeded as exc:
                    log.warning(
                        "buena_email.cost_cap_post_charge",
                        cumulative=str(exc.cumulative),
                        cap=str(exc.cap),
                    )
                    summary.aborted_on_cost_cap = True
                    summary.cumulative_usd = str(exc.cumulative)
                    summary.cap_usd = str(exc.cap)
                    # Stamp this last event before bailing out.
                    async with factory() as session:
                        await _stamp_event(
                            session,
                            event_id=event_id,
                            route=route,
                            received_at=ev.received_at,
                            historical=(
                                historical and not reprocess_historical
                            ),
                            error=None,
                        )
                        await session.commit()
                    return False
        except Exception as exc:  # noqa: BLE001 — keep loop alive
            summary.extractor_errors += 1
            error_text = f"{type(exc).__name__}: {exc}"[:500]
            if len(summary.error_samples) < 5:
                summary.error_samples.append(error_text)
            log.exception(
                "buena_email.extractor_error", source_ref=ev.source_ref
            )
            async with factory() as session:
                hit_dead_letter = await _record_failure(
                    session,
                    event_id=event_id,
                    error=error_text,
                    dead_letter_after=dead_letter_after,
                )
                await session.commit()
            if hit_dead_letter:
                summary.failed_events += 1

    # --- 4. Stamp event (scope + processed_at + processing_error) ----------
    async with factory() as session:
        await _stamp_event(
            session,
            event_id=event_id,
            route=route,
            received_at=ev.received_at,
            historical=(historical and not reprocess_historical),
            error=error_text,
        )
        await session.commit()

    return True


async def _populate_top_properties(
    factory: Any, summary: EmailBackfillSummary
) -> None:
    """Top 10 properties by inserted email-event count (sanity check)."""
    async with factory() as session:
        result = await session.execute(
            text(
                """
                SELECT p.name, COUNT(*) AS n
                FROM events e
                JOIN properties p ON p.id = e.property_id
                WHERE e.source = 'email'
                GROUP BY p.name
                ORDER BY n DESC
                LIMIT 10
                """
            )
        )
        for row in result.all():
            summary.top_property_event_counts[str(row.name)] = int(row.n)


async def backfill_emails(
    *,
    root: Path | None = None,
    cap_usd: Decimal = Decimal("20.00"),
    dead_letter_after: int = 3,
    reprocess_historical: bool = False,
    limit: int | None = None,
) -> EmailBackfillSummary:
    """Drive the full Buena ``.eml`` archive through the live pipeline.

    Args:
        root: Override ``EXTRACTED_ROOT``; defaults to the Buena archive.
        cap_usd: Hard ceiling on the durable cost ledger for label
            ``step6_email_backfill``.
        dead_letter_after: Persistent-failure ceiling — once an event's
            ``failed_events.retry_count`` reaches this number, the row
            stays as the operator's dead-letter signal.
        reprocess_historical: When ``False`` (default), events older
            than 30 days are stamped ``processed_at = received_at`` after
            successful extraction so Phase 9's validator/uncertainty
            layers don't re-process pre-validator extractions. Phase 9
            migrations flip this to ``True`` once those layers ship.
        limit: Stop after ``limit`` ``.eml`` files. ``None`` means full
            archive.
    """
    extracted_root = root if root is not None else buena_archive.require_root()
    emails_dir = extracted_root / "emails"

    ensure_migrations()
    cost_ledger.ensure_label(LEDGER_LABEL, cap_usd)

    factory = get_sessionmaker()
    summary = EmailBackfillSummary()
    summary.cap_usd = str(cap_usd)

    # Pre-flight: refuse to start if a previous run already exhausted the cap.
    state = cost_ledger.get_state(LEDGER_LABEL)
    if state is not None and state.exhausted:
        log.warning(
            "buena_email.cost_cap_pre_run",
            cumulative=str(state.cumulative_usd),
            cap=str(state.cap_usd),
        )
        summary.aborted_on_cost_cap = True
        summary.cumulative_usd = str(state.cumulative_usd)
        return summary

    iterator: Iterable[ConnectorEvent] = eml_archive.walk_directory(emails_dir)
    seen = 0
    for ev in iterator:
        if limit is not None and seen >= limit:
            break
        seen += 1
        proceed = await _process_event(
            factory,
            ev,
            summary=summary,
            label=LEDGER_LABEL,
            cap_usd=cap_usd,
            dead_letter_after=dead_letter_after,
            reprocess_historical=reprocess_historical,
        )
        if not proceed:
            break

    state_after = cost_ledger.get_state(LEDGER_LABEL)
    if state_after is not None:
        summary.cumulative_usd = str(state_after.cumulative_usd)
        summary.cap_usd = str(state_after.cap_usd)

    await _populate_top_properties(factory, summary)
    log.info("buena_email.done", **summary.as_json())
    return summary


def run_backfill_emails(
    *,
    extracted_root: str | None = None,
    cap_usd: Decimal = Decimal("20.00"),
    dead_letter_after: int = 3,
    reprocess_historical: bool = False,
    limit: int | None = None,
) -> EmailBackfillSummary:
    """Sync wrapper used by ``connectors.cli``."""
    root = buena_archive.require_root(extracted_root)
    return asyncio.run(
        backfill_emails(
            root=root,
            cap_usd=cap_usd,
            dead_letter_after=dead_letter_after,
            reprocess_historical=reprocess_historical,
            limit=limit,
        )
    )


__all__ = [
    "LEDGER_LABEL",
    "EmailBackfillSummary",
    "backfill_emails",
    "run_backfill_emails",
]
