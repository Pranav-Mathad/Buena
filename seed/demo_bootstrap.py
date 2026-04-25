"""One-shot demo bootstrap — get a fresh DB to a visibly-alive state.

Phase 6 calls for "starter signals so portfolio is visibly alive at demo
start". This script wraps the existing seed + enrichment + watcher +
evaluator pipelines so a single command lands the DB exactly where the
2-minute demo expects:

1. Apply schema + seed dataset (idempotent).
2. Run Tavily enrichment for every property (idempotent — skips already-
   enriched ones, so the 🌐 badge is on every property).
3. Run the Tavily regulation watcher once (offline mode if no key).
4. Fire the rule evaluator so the inbox + portfolio banner are live.

Runnable as ``python -m seed.demo_bootstrap``. Safe to run repeatedly —
every step uses the same idempotency contracts the runtime does.
"""

from __future__ import annotations

import asyncio
import sys
from typing import Any
from uuid import UUID

import structlog
from sqlalchemy import text

from backend.config import get_settings
from backend.db.session import get_sessionmaker
from backend.logging import configure_logging
from backend.services.tavily import enrich_property, watch_regulations
from backend.signals.evaluator import evaluate_all
from seed.seed import seed as seed_dataset

log = structlog.get_logger("demo_bootstrap")


async def _enrich_all_properties() -> int:
    """Run Tavily enrichment on every property; return how many were enriched now."""
    factory = get_sessionmaker()
    async with factory() as session:
        result = await session.execute(
            text("SELECT id, name, address FROM properties ORDER BY created_at")
        )
        rows = list(result.all())

    enriched_now = 0
    for row in rows:
        event_id = await enrich_property(
            UUID(str(row.id)), row.name, row.address
        )
        if event_id is not None:
            enriched_now += 1
    log.info(
        "demo_bootstrap.tavily_enrich",
        properties=len(rows),
        newly_enriched=enriched_now,
    )
    return enriched_now


async def _evaluate_signals() -> int:
    """Fire the rule evaluator once; commit any new signals."""
    factory = get_sessionmaker()
    async with factory() as session:
        created = await evaluate_all(session)
        await session.commit()
    return created


async def _summary() -> dict[str, Any]:
    """Return a compact `{properties, pending_signals, …}` snapshot."""
    factory = get_sessionmaker()
    async with factory() as session:
        properties = (
            await session.execute(text("SELECT COUNT(*) FROM properties"))
        ).scalar_one()
        pending = (
            await session.execute(
                text("SELECT COUNT(*) FROM signals WHERE status = 'pending'")
            )
        ).scalar_one()
        portfolio_pending = (
            await session.execute(
                text(
                    """
                    SELECT COUNT(*) FROM signals
                    WHERE status = 'pending' AND property_id IS NULL
                    """
                )
            )
        ).scalar_one()
        web_facts_per_property = (
            await session.execute(
                text(
                    """
                    SELECT f.property_id AS property_id,
                           COUNT(*) AS web_facts
                    FROM facts f
                    JOIN events e ON e.id = f.source_event_id
                    WHERE f.superseded_by IS NULL AND e.source = 'web'
                    GROUP BY f.property_id
                    """
                )
            )
        ).all()
    web_min = min((int(r.web_facts) for r in web_facts_per_property), default=0)
    return {
        "properties": int(properties),
        "pending_signals": int(pending),
        "pending_portfolio_signals": int(portfolio_pending),
        "min_web_facts_per_property": web_min,
        "properties_with_web_facts": len(web_facts_per_property),
    }


async def run() -> dict[str, Any]:
    """Execute the bootstrap end-to-end and return a summary dict."""
    settings = get_settings()
    log.info("demo_bootstrap.start", db=settings.database_url_sync.split("@")[-1])
    seed_summary = seed_dataset(settings.database_url_sync)
    enriched_now = await _enrich_all_properties()
    regulation_events = await watch_regulations()
    signals_created = await _evaluate_signals()
    snapshot = await _summary()
    log.info(
        "demo_bootstrap.done",
        seed=seed_summary,
        enriched_now=enriched_now,
        regulation_events=regulation_events,
        signals_created=signals_created,
        **snapshot,
    )
    return {
        "seed": seed_summary,
        "enriched_now": enriched_now,
        "regulation_events": regulation_events,
        "signals_created": signals_created,
        **snapshot,
    }


def main() -> int:
    """CLI entry point."""
    configure_logging()
    try:
        asyncio.run(run())
    except Exception:  # noqa: BLE001 — surface any failure
        log.exception("demo_bootstrap.failed")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
