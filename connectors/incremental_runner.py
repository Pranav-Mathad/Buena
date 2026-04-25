"""Step 7 — Buena ``incremental/`` snapshots as an admin-controlled live feed.

Buena ships 10 days of synthetic deltas (``day-01`` … ``day-10``) under
``Extracted/incremental/``. Each day contains a small batch (~4 emails
+ 1 bank txn + 1 invoice) that approximates one operating day for the
WEG. The runner:

- Persists the day cursor in ``system_state['buena_day_cursor']`` so
  it survives server restarts.
- Exposes :func:`advance_one_day` to fan one day's events through
  :func:`backend.pipeline.events.insert_event`, route them with the
  same routers the live worker uses, and await
  :func:`backend.signals.evaluator.evaluate_all` so any signals fire
  in the same tick.
- After day-10 returns ``{"exhausted": True}`` so the demo UI can
  surface the "no more days" state.

The latency budget is **< 3 s per call** — the day batch is tiny and
``evaluate_all`` is cheap on Buena-scale data. If a future customer
has heavier days we can split the response (return immediately, run
``evaluate_all`` on a background task) without changing the API
contract.

Email events go through Gemini via the same extractor the live worker
uses; the cost ledger label is ``step7_incremental`` with a small
default cap (``$2.00``) so ten days cost at most a few cents above
the empty-feed floor.
"""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass, field as dc_field
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
from backend.pipeline.router import StructuredRoute, route_structured, route_text_event
from backend.pipeline.structured_extractors import (
    extract_bank_facts,
    extract_invoice_facts,
    stamp_processed,
)
from backend.signals.evaluator import evaluate_all
from connectors import buena_archive, cost_ledger
from connectors.base import ConnectorEvent
from connectors.migrations import apply_all as ensure_migrations

log = structlog.get_logger(__name__)


CURSOR_KEY = "buena_day_cursor"
TOTAL_DAYS = 10
LEDGER_LABEL = "step7_incremental"
DEFAULT_CAP_USD = Decimal("2.00")


@dataclass
class AdvanceResult:
    """One day's outcome."""

    day: int
    events_inserted: int = 0
    facts_written: int = 0
    routed_property: int = 0
    routed_building: int = 0
    routed_liegenschaft: int = 0
    unrouted: int = 0
    signals_fired: int = 0
    exhausted: bool = False
    error_samples: list[str] = dc_field(default_factory=list)

    def as_json(self) -> dict[str, Any]:
        """Serializable view for the admin endpoint."""
        return {
            "day": self.day,
            "events_inserted": self.events_inserted,
            "facts_written": self.facts_written,
            "routed_property": self.routed_property,
            "routed_building": self.routed_building,
            "routed_liegenschaft": self.routed_liegenschaft,
            "unrouted": self.unrouted,
            "signals_fired": self.signals_fired,
            "exhausted": self.exhausted,
            "error_samples": list(self.error_samples),
        }


# --- Cursor primitives ------------------------------------------------------


async def _read_cursor(session: AsyncSession) -> int:
    """Return the last-advanced day index (0 if never advanced)."""
    result = await session.execute(
        text("SELECT value FROM system_state WHERE key = :k"), {"k": CURSOR_KEY}
    )
    row = result.first()
    if row is None:
        return 0
    value = row.value if isinstance(row.value, dict) else json.loads(row.value or "{}")
    return int(value.get("day", 0))


async def _write_cursor(session: AsyncSession, day: int) -> None:
    """Persist the new cursor day."""
    await session.execute(
        text(
            """
            INSERT INTO system_state (key, value)
            VALUES (:k, CAST(:v AS JSONB))
            ON CONFLICT (key) DO UPDATE
              SET value = EXCLUDED.value, updated_at = now()
            """
        ),
        {"k": CURSOR_KEY, "v": json.dumps({"day": day})},
    )


async def get_cursor_status() -> dict[str, Any]:
    """Read-only — returns the current day + exhausted flag for the UI."""
    factory = get_sessionmaker()
    async with factory() as session:
        day = await _read_cursor(session)
    return {
        "current_day": day,
        "next_day": day + 1 if day < TOTAL_DAYS else None,
        "total_days": TOTAL_DAYS,
        "exhausted": day >= TOTAL_DAYS,
    }


async def reset_cursor() -> dict[str, Any]:
    """Reset the cursor to ``0``. Used by the admin demo-reset path."""
    factory = get_sessionmaker()
    async with factory() as session:
        await _write_cursor(session, 0)
        await session.commit()
    log.info("incremental.cursor_reset")
    return await get_cursor_status()


# --- Per-event ingestion ----------------------------------------------------


async def _process_event(
    factory: Any, ev: ConnectorEvent, *, summary: AdvanceResult
) -> None:
    """Insert one ConnectorEvent and run the matching extractor.

    Email events go through the Gemini extractor (fact-write via
    ``differ.diff`` + ``applier.apply``). Bank / invoice events take
    the structured-extractor path that already powers the backfill.
    Routing is the same routers the live worker uses, so the
    incremental feed exercises the production code path end-to-end.
    """
    async with factory() as session:
        event_id, inserted = await insert_event(
            session,
            source=ev.source,
            source_ref=ev.source_ref,
            raw_content=ev.raw_content,
            metadata=ev.metadata,
        )
        await session.commit()
        if not inserted:
            return
        summary.events_inserted += 1

    # Route — text router for emails, structured router otherwise.
    async with factory() as session:
        if ev.source == "email":
            route: StructuredRoute = await route_text_event(
                session, ev.raw_content, metadata=ev.metadata
            )
        else:
            route = await route_structured(
                session, ev.metadata, event_source=ev.source
            )
    if route.property_id is not None:
        summary.routed_property += 1
    elif route.building_id is not None:
        summary.routed_building += 1
    elif route.liegenschaft_id is not None:
        summary.routed_liegenschaft += 1
    else:
        summary.unrouted += 1

    # Fact writers
    written = 0
    try:
        if ev.source == "email" and route.property_id is not None:
            written = await _extract_email_and_apply(
                factory, event_id=event_id, ev=ev, property_id=route.property_id
            )
        elif ev.source == "bank":
            async with factory() as session:
                written = (
                    await extract_bank_facts(
                        session,
                        event_id=event_id,
                        property_id=route.property_id,
                        building_id=route.building_id,
                        liegenschaft_id=route.liegenschaft_id,
                        metadata=ev.metadata,
                    )
                ) or 0
                await session.commit()
        elif ev.source == "invoice":
            async with factory() as session:
                written = (
                    await extract_invoice_facts(
                        session,
                        event_id=event_id,
                        property_id=route.property_id,
                        building_id=route.building_id,
                        liegenschaft_id=route.liegenschaft_id,
                        metadata=ev.metadata,
                    )
                ) or 0
                await session.commit()
    except Exception as exc:  # noqa: BLE001 — keep one bad event from breaking the day
        if len(summary.error_samples) < 3:
            summary.error_samples.append(f"{type(exc).__name__}: {exc}"[:300])
        log.exception("incremental.event_error", source_ref=ev.source_ref)

    summary.facts_written += int(written or 0)

    async with factory() as session:
        await stamp_processed(
            session,
            event_id,
            property_id=route.property_id,
            building_id=route.building_id,
            liegenschaft_id=route.liegenschaft_id,
            received_at=ev.received_at,
        )
        await session.commit()


async def _extract_email_and_apply(
    factory: Any, *, event_id: UUID, ev: ConnectorEvent, property_id: UUID
) -> int:
    """Extract an email event's facts and apply them. Returns facts written."""
    async with factory() as session:
        result = await session.execute(
            text("SELECT name FROM properties WHERE id = :pid"), {"pid": property_id}
        )
        property_name = str((result.first() or ("(unknown)",))[0])
        markdown = await render_markdown(session, property_id)
    excerpt = "\n".join(markdown.splitlines()[:30])

    extracted = await run_extractor(
        property_name=property_name,
        current_context_excerpt=excerpt,
        source=ev.source,
        raw_content=ev.raw_content,
    )

    async with factory() as session:
        plan = await diff(
            session,
            property_id=property_id,
            event_source=ev.source,
            proposals=extracted.facts_to_update,
        )
        written = await apply_plan(
            session,
            property_id=property_id,
            source_event_id=event_id,
            plan=plan,
        )
        await session.commit()
    return int(written or 0)


# --- Public API -------------------------------------------------------------


async def advance_one_day(*, root: Path | None = None) -> AdvanceResult:
    """Advance the cursor by one day and process that day's deltas.

    Returns an :class:`AdvanceResult` whose ``exhausted`` flag is True
    once the cursor is past day-10. Subsequent calls after exhaustion
    are no-ops that keep returning the exhausted-shaped result so the
    UI can render the "no more days" state idempotently.
    """
    extracted_root = root if root is not None else buena_archive.require_root()
    ensure_migrations()
    cost_ledger.ensure_label(LEDGER_LABEL, DEFAULT_CAP_USD)

    factory = get_sessionmaker()
    async with factory() as session:
        current = await _read_cursor(session)
        next_day = current + 1
        if next_day > TOTAL_DAYS:
            return AdvanceResult(day=current, exhausted=True)

    summary = AdvanceResult(day=next_day)

    for ev in buena_archive.iter_incremental_day(extracted_root, next_day):
        await _process_event(factory, ev, summary=summary)

    # Signals — same evaluator the live worker tick runs.
    try:
        async with factory() as session:
            summary.signals_fired = int(await evaluate_all(session) or 0)
            await session.commit()
    except Exception as exc:  # noqa: BLE001
        log.exception("incremental.signals_error")
        if len(summary.error_samples) < 3:
            summary.error_samples.append(
                f"signals: {type(exc).__name__}: {exc}"[:300]
            )

    # Persist the cursor *after* the day's work lands so a crash mid-day
    # leaves the cursor pointing at the same day for retry.
    async with factory() as session:
        await _write_cursor(session, next_day)
        await session.commit()

    summary.exhausted = next_day >= TOTAL_DAYS
    log.info("incremental.day_advanced", **summary.as_json())
    return summary


def run_advance_one_day() -> AdvanceResult:
    """Sync wrapper for callers outside the event loop (CLI / tests)."""
    return asyncio.run(advance_one_day())


__all__ = [
    "CURSOR_KEY",
    "DEFAULT_CAP_USD",
    "LEDGER_LABEL",
    "TOTAL_DAYS",
    "AdvanceResult",
    "advance_one_day",
    "get_cursor_status",
    "reset_cursor",
    "run_advance_one_day",
]
