"""Load the synthetic sample events and run them through the pipeline.

The synthetic master data gives every unit a Stammdaten record but no
history, so on a fresh install the featured property (WE 29 by default)
would show master data only. This loader inserts the handful of synthetic
emails in ``Extracted_demo/sample_events/`` and processes each one with the
worker's own ``process_specific``: routing, rule-based extraction (or the
configured LLM), validation, apply and file rendering, exactly as a live
email would go.

Idempotent: events are keyed on ``(source, source_ref)``, so a second run
inserts nothing, and an event that is already processed is skipped. Each
newly inserted event is backdated by its ``days_ago`` so the history spans
months, as a real inbox would.
"""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from pathlib import Path

import structlog
from sqlalchemy import text

from backend.db.session import get_sessionmaker
from backend.pipeline.events import insert_event
from backend.pipeline.worker import process_specific

log = structlog.get_logger(__name__)

DEFAULT_PATH = (
    Path(__file__).resolve().parent.parent
    / "Extracted_demo"
    / "sample_events"
    / "featured_emails.json"
)


@dataclass(frozen=True)
class SampleEventsSummary:
    """What one run of the loader did."""

    inserted: int
    already_present: int
    processed: int


async def load_sample_events(path: Path = DEFAULT_PATH) -> SampleEventsSummary:
    """Insert and process every event in ``path``; see the module docstring."""
    events = json.loads(path.read_text(encoding="utf-8"))["events"]
    factory = get_sessionmaker()
    inserted = already_present = processed = 0
    for event in events:
        async with factory() as session:
            event_id, is_new = await insert_event(
                session,
                source="email",
                source_ref=event["source_ref"],
                raw_content=event["raw_content"],
            )
            if is_new:
                await session.execute(
                    text(
                        "UPDATE events SET received_at = now() - make_interval(days => :d) "
                        "WHERE id = :id"
                    ),
                    {"d": int(event["days_ago"]), "id": event_id},
                )
            await session.commit()
        if is_new:
            inserted += 1
        else:
            already_present += 1
        # Also picks up an event inserted by an earlier run that stopped
        # before processing it.
        if await process_specific(event_id):
            processed += 1
    summary = SampleEventsSummary(inserted, already_present, processed)
    log.info("sample_events.loaded", path=str(path), **summary.__dict__)
    return summary


def run_load_sample_events(path: Path = DEFAULT_PATH) -> SampleEventsSummary:
    """Sync wrapper for the CLI subcommand."""
    return asyncio.run(load_sample_events(path))
