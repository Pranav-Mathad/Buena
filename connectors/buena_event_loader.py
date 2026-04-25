"""Stream Buena structured events into the events + facts tables.

Phase 8 Step 3 backfills the deterministic data sources (bank
transactions, invoice PDFs) directly: each event lands in ``events``
via :func:`backend.pipeline.events.insert_event`, the structured router
in :mod:`backend.pipeline.router` resolves a property when possible,
and :mod:`backend.pipeline.structured_extractors` writes facts
synchronously. Stamping ``processed_at = received_at`` keeps the live
worker hands-off (KEYSTONE Part X "historical events stamped" rule).

Idempotency:

- ``events.UNIQUE (source, source_ref)`` absorbs re-runs.
- Fact writers compare with the current row and short-circuit on
  identical values.
- ``relationships(serviced_by)`` edges check for duplicates before
  insert.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import structlog

from backend.db.session import get_sessionmaker
from backend.pipeline.events import insert_event
from backend.pipeline.router import StructuredRoute, route_structured
from backend.pipeline.structured_extractors import (
    extract_bank_facts,
    extract_invoice_facts,
    stamp_processed,
)
from connectors import buena_archive
from connectors.base import ConnectorEvent, DataMissing
from connectors.migrations import apply_all as ensure_migrations

log = structlog.get_logger(__name__)


@dataclass
class BackfillSummary:
    """What :func:`backfill_*` returns to the CLI.

    The four-tier counts (``routed_property / routed_building /
    routed_liegenschaft / unrouted``) sum to ``inserted_now`` and let the
    operator read the per-tier miss rates that Phase 8.1 verification
    requires.
    """

    label: str
    total_seen: int = 0
    inserted_now: int = 0
    routed_property: int = 0
    routed_building: int = 0
    routed_liegenschaft: int = 0
    unrouted: int = 0
    facts_written: int = 0
    miss_reasons: dict[str, int] | None = None

    @property
    def routed(self) -> int:
        """Total events that resolved to *some* scope (any tier)."""
        return (
            self.routed_property
            + self.routed_building
            + self.routed_liegenschaft
        )

    def as_json(self) -> dict[str, Any]:
        """Serializable view for the CLI's ``--json`` mode."""
        return {
            "label": self.label,
            "total_seen": self.total_seen,
            "inserted_now": self.inserted_now,
            "routed_property": self.routed_property,
            "routed_building": self.routed_building,
            "routed_liegenschaft": self.routed_liegenschaft,
            "routed_total": self.routed,
            "unrouted": self.unrouted,
            "facts_written": self.facts_written,
            "miss_reasons": self.miss_reasons or {},
        }


FactWriter = Callable[..., Any]


async def _ingest_one(
    factory: Any,
    ev: ConnectorEvent,
    *,
    fact_writer: FactWriter,
    summary: BackfillSummary,
) -> None:
    """Insert one event + write structured facts in a single transaction."""
    async with factory() as session:
        event_id, inserted = await insert_event(
            session,
            source=ev.source,
            source_ref=ev.source_ref,
            raw_content=ev.raw_content,
            metadata=ev.metadata,
        )

        if not inserted:
            # The event already exists from a prior run. Don't touch facts;
            # don't double-count routed/unrouted. Idempotent return.
            await session.commit()
            return

        route: StructuredRoute = await route_structured(
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
            if summary.miss_reasons is None:
                summary.miss_reasons = {}
            summary.miss_reasons[route.reason] = (
                summary.miss_reasons.get(route.reason, 0) + 1
            )

        # Write facts (no-op when no scope is resolved).
        written = await fact_writer(
            session,
            event_id=event_id,
            property_id=route.property_id,
            building_id=route.building_id,
            liegenschaft_id=route.liegenschaft_id,
            metadata=ev.metadata,
        )
        summary.facts_written += int(written or 0)

        await stamp_processed(
            session,
            event_id,
            property_id=route.property_id,
            building_id=route.building_id,
            liegenschaft_id=route.liegenschaft_id,
            received_at=_normalise_ts(ev.received_at),
        )
        await session.commit()
        summary.inserted_now += 1


def _normalise_ts(received: datetime | None) -> datetime | None:
    """Make sure ``received_at`` is tz-aware (Postgres expects timestamptz)."""
    if received is None:
        return None
    if received.tzinfo is None:
        from datetime import timezone  # noqa: PLC0415

        return received.replace(tzinfo=timezone.utc)
    return received


async def _drive(
    iterator: Iterable[ConnectorEvent],
    *,
    label: str,
    fact_writer: FactWriter,
) -> BackfillSummary:
    """Common driver for both bank + invoice backfills."""
    ensure_migrations()
    factory = get_sessionmaker()
    summary = BackfillSummary(label=label)
    for ev in iterator:
        summary.total_seen += 1
        try:
            await _ingest_one(
                factory, ev, fact_writer=fact_writer, summary=summary
            )
        except Exception:  # noqa: BLE001 — keep the backfill moving
            log.exception(
                "backfill.error",
                label=label,
                source_ref=ev.source_ref,
            )
    log.info("backfill.done", **summary.as_json())
    return summary


# -----------------------------------------------------------------------------
# Public entry points used by connectors/cli.py
# -----------------------------------------------------------------------------


async def backfill_bank(*, root: Path | None = None) -> BackfillSummary:
    """Backfill every row of ``Extracted/bank/bank_index.csv``."""
    extracted = root if root is not None else buena_archive.require_root()
    return await _drive(
        buena_archive.iter_bank(extracted),
        label="buena_bank",
        fact_writer=extract_bank_facts,
    )


async def backfill_invoices(*, root: Path | None = None) -> BackfillSummary:
    """Backfill every PDF under ``Extracted/rechnungen/`` (filename only).

    PDF text is *not* extracted by default — Step 3 only needs the
    filename for ID + document_type, which the connector populates from
    heuristics. ``extract_invoice_facts`` records the latest invoice +
    serviced_by edge when the property resolves.
    """
    extracted = root if root is not None else buena_archive.require_root()
    return await _drive(
        buena_archive.iter_invoices(extracted, read_text=False, use_llm=False),
        label="buena_invoice",
        fact_writer=extract_invoice_facts,
    )


# -----------------------------------------------------------------------------
# Sync wrappers — the CLI is sync; running coroutines via asyncio.run keeps the
# entry points small.
# -----------------------------------------------------------------------------


def run_backfill_bank(extracted_root: str | None = None) -> BackfillSummary:
    """Sync wrapper used by ``connectors.cli``."""
    root = buena_archive.require_root(extracted_root)
    return asyncio.run(backfill_bank(root=root))


def run_backfill_invoices(extracted_root: str | None = None) -> BackfillSummary:
    """Sync wrapper used by ``connectors.cli``."""
    root = buena_archive.require_root(extracted_root)
    return asyncio.run(backfill_invoices(root=root))


# -----------------------------------------------------------------------------
# One-time re-route migration
# -----------------------------------------------------------------------------


@dataclass
class RerouteSummary:
    """Result of a one-shot re-route over already-ingested events."""

    label: str
    scanned: int = 0
    moved_to_property: int = 0
    moved_to_building: int = 0
    moved_to_liegenschaft: int = 0
    still_unrouted: int = 0
    facts_written: int = 0
    miss_reasons: dict[str, int] | None = None

    def as_json(self) -> dict[str, Any]:
        """Serializable view for the CLI's ``--json`` mode."""
        return {
            "label": self.label,
            "scanned": self.scanned,
            "moved_to_property": self.moved_to_property,
            "moved_to_building": self.moved_to_building,
            "moved_to_liegenschaft": self.moved_to_liegenschaft,
            "still_unrouted": self.still_unrouted,
            "facts_written": self.facts_written,
            "miss_reasons": self.miss_reasons or {},
        }


async def _reroute_one(
    factory: Any,
    *,
    event_id: str,
    source: str,
    metadata: dict[str, Any],
    summary: RerouteSummary,
) -> None:
    """Re-evaluate routing for one already-ingested event."""
    from sqlalchemy import text  # noqa: PLC0415 — local import keeps imports tidy

    async with factory() as session:
        route: StructuredRoute = await route_structured(
            session, metadata, event_source=source
        )
        if route.property_id is not None:
            summary.moved_to_property += 1
        elif route.building_id is not None:
            summary.moved_to_building += 1
        elif route.liegenschaft_id is not None:
            summary.moved_to_liegenschaft += 1
        else:
            summary.still_unrouted += 1
            if summary.miss_reasons is None:
                summary.miss_reasons = {}
            summary.miss_reasons[route.reason] = (
                summary.miss_reasons.get(route.reason, 0) + 1
            )
            await session.commit()
            return

        # Stamp the event with its new scope.
        await session.execute(
            text(
                """
                UPDATE events
                SET property_id     = :pid,
                    building_id     = :bid,
                    liegenschaft_id = :lid
                WHERE id = :id
                """
            ),
            {
                "id": event_id,
                "pid": route.property_id,
                "bid": route.building_id,
                "lid": route.liegenschaft_id,
            },
        )

        # Write any facts that follow from the new scope. Same writer
        # logic as the backfill — identical writes short-circuit.
        writer = (
            extract_bank_facts
            if source == "bank"
            else extract_invoice_facts
            if source == "invoice"
            else None
        )
        if writer is not None:
            from uuid import UUID  # noqa: PLC0415

            written = await writer(
                session,
                event_id=UUID(event_id),
                property_id=route.property_id,
                building_id=route.building_id,
                liegenschaft_id=route.liegenschaft_id,
                metadata=metadata,
            )
            summary.facts_written += int(written or 0)

        await session.commit()


async def re_route_unrouted(
    sources: list[str] | None = None,
) -> RerouteSummary:
    """Walk every event with no scope set and re-evaluate routing.

    One-time migration step after Phase 8.1 schema/router changes —
    surfaces in ``DECISIONS.md`` as a documented re-attribution pass,
    not as ongoing pipeline behavior. Idempotent: re-running on already-
    re-routed events finds nothing new to scan.
    """
    from sqlalchemy import text  # noqa: PLC0415

    ensure_migrations()
    factory = get_sessionmaker()
    summary = RerouteSummary(label="re_route")

    src_filter = ""
    params: dict[str, Any] = {}
    if sources:
        src_filter = "AND source = ANY(:sources)"
        params["sources"] = sources

    async with factory() as session:
        rows = (
            await session.execute(
                text(
                    f"""
                    SELECT id, source, metadata
                    FROM events
                    WHERE property_id IS NULL
                      AND building_id IS NULL
                      AND liegenschaft_id IS NULL
                      {src_filter}
                    ORDER BY received_at ASC
                    """
                ),
                params,
            )
        ).all()

    for row in rows:
        summary.scanned += 1
        try:
            await _reroute_one(
                factory,
                event_id=str(row.id),
                source=str(row.source),
                metadata=dict(row.metadata or {}),
                summary=summary,
            )
        except Exception:  # noqa: BLE001
            log.exception("reroute.error", event_id=str(row.id))

    log.info("reroute.done", **summary.as_json())
    return summary


def run_re_route(sources: list[str] | None = None) -> RerouteSummary:
    """Sync wrapper for ``connectors.cli``."""
    return asyncio.run(re_route_unrouted(sources))


__all__ = [
    "BackfillSummary",
    "RerouteSummary",
    "backfill_bank",
    "backfill_invoices",
    "re_route_unrouted",
    "run_backfill_bank",
    "run_backfill_invoices",
    "run_re_route",
    "DataMissing",
]
