"""One-shot hero-property backfill.

Phase 8 was scoped down after the Step 6 concurrency=4 silent-hang
on Gemini Pro. The hero property gets *complete* coverage so the
demo's onboarding view reads as rich; the rest of the archive stays
partial (251 events / 85 facts already in the DB) and is documented
as "post-hackathon" in DECISIONS.md.

This script reuses :mod:`connectors.buena_email_loader` end-to-end,
just with a property-alias filter wrapping the email walker. Runs
sequentially (concurrency=1) and relies on the new 60-second
``GEMINI_EXTRACT_TIMEOUT_S`` so a single hung call can't freeze the
whole run.

Usage::

    PYTHONPATH=. .venv/bin/python -m scripts.hero_backfill \\
        --property-id 509393da-6806-49ef-9e59-3da0213008cd
"""

from __future__ import annotations

import argparse
import asyncio
import re
import sys
from collections.abc import Iterator
from decimal import Decimal
from typing import Any

import structlog
from sqlalchemy import text

from backend.db.session import get_sessionmaker
from backend.logging import configure_logging
from connectors import buena_archive, buena_email_loader, cost_ledger, eml_archive
from connectors.base import ConnectorEvent
from connectors.buena_email_loader import (
    EmailBackfillSummary,
    LEDGER_LABEL,
    _is_historical,
    _process_event,
)
from connectors.migrations import apply_all as ensure_migrations

log = structlog.get_logger("hero_backfill")


HERO_LEDGER_LABEL = "step6_hero_backfill"
HERO_CAP_USD = Decimal("3.00")  # hero is one property; 100 emails ≈ $0.50


async def _hero_aliases(property_id: str) -> list[str]:
    """Pull the property's aliases for the alias-match regex."""
    factory = get_sessionmaker()
    async with factory() as session:
        row = (
            await session.execute(
                text(
                    "SELECT name, aliases FROM properties WHERE id = :id"
                ),
                {"id": property_id},
            )
        ).first()
    if row is None:
        raise SystemExit(f"property {property_id} not found")
    aliases = list(row.aliases or [])
    if row.name not in aliases:
        aliases.append(row.name)
    return aliases


def _filter_predicate(aliases: list[str]) -> Any:
    """Return ``True`` when an event's body or metadata mentions any alias.

    Skip the over-broad aliases (``HAUS-NN`` matches the whole building,
    ``Immanuelkirchstraße 26`` matches every property in the building).
    Keep specific ones: ``EH-NNN``, ``WE NN``, the full canonical name,
    and floor descriptors like ``4. OG mitte``.
    """
    keepers: list[str] = []
    for a in aliases:
        a_clean = a.strip()
        if not a_clean:
            continue
        if a_clean.startswith("HAUS-"):
            continue
        if a_clean.lower() == "immanuelkirchstraße 26":
            continue
        keepers.append(a_clean)
    if not keepers:
        raise SystemExit("no specific aliases left after filtering")
    pattern = re.compile(
        "|".join(re.escape(k) for k in keepers), re.IGNORECASE
    )

    def matches(ev: ConnectorEvent) -> bool:
        haystack = ev.raw_content + " " + " ".join(
            str(v) for v in ev.metadata.values() if isinstance(v, str)
        )
        return pattern.search(haystack) is not None

    return matches


async def run(property_id: str) -> EmailBackfillSummary:
    """Run the hero backfill against the live extractor (sequential)."""
    ensure_migrations()
    cost_ledger.ensure_label(HERO_LEDGER_LABEL, HERO_CAP_USD)

    aliases = await _hero_aliases(property_id)
    log.info(
        "hero.aliases", property_id=property_id, aliases=aliases
    )

    keep = _filter_predicate(aliases)
    extracted_root = buena_archive.require_root()
    emails_dir = extracted_root / "emails"

    async def filtered_walk() -> Iterator[ConnectorEvent]:
        return (ev for ev in eml_archive.walk_directory(emails_dir) if keep(ev))

    summary = EmailBackfillSummary(label="hero_backfill")
    summary.cap_usd = str(HERO_CAP_USD)
    summary.concurrency = 1

    factory = get_sessionmaker()
    known_sender_domains = await buena_email_loader._load_known_sender_domains(factory)
    ledger_lock = asyncio.Lock()

    iterator = await filtered_walk()
    seen = 0
    for ev in iterator:
        seen += 1
        # Re-bind LEDGER_LABEL on the fly so charges land on the hero ledger.
        # (Avoids monkeypatching at module scope which would persist.)
        original_label = buena_email_loader.LEDGER_LABEL
        buena_email_loader.LEDGER_LABEL = HERO_LEDGER_LABEL
        try:
            proceed = await _process_event(
                factory,
                ev,
                summary=summary,
                label=HERO_LEDGER_LABEL,
                cap_usd=HERO_CAP_USD,
                dead_letter_after=3,
                reprocess_historical=False,
                ledger_lock=ledger_lock,
                known_sender_domains=known_sender_domains,
            )
        finally:
            buena_email_loader.LEDGER_LABEL = original_label
        if not proceed:
            log.warning("hero.cost_cap_hit", processed=seen)
            break

    state = cost_ledger.get_state(HERO_LEDGER_LABEL)
    if state is not None:
        summary.cumulative_usd = str(state.cumulative_usd)
        summary.cap_usd = str(state.cap_usd)

    log.info("hero.done", **summary.as_json())
    return summary


def main(argv: list[str] | None = None) -> int:
    """CLI entry: ``python -m scripts.hero_backfill --property-id <uuid>``."""
    configure_logging()
    parser = argparse.ArgumentParser(prog="scripts.hero_backfill")
    parser.add_argument("--property-id", required=True)
    args = parser.parse_args(argv)
    summary = asyncio.run(run(args.property_id))
    print()
    print(f"hero backfill summary  (label={LEDGER_LABEL})")
    print(f"  total_seen           = {summary.total_seen}")
    print(f"  inserted_now         = {summary.inserted_now}")
    print(f"  routed_property      = {summary.routed_property}")
    print(f"  routed_liegenschaft  = {summary.routed_liegenschaft}")
    print(f"  unrouted             = {summary.unrouted}")
    print(f"  extraction_attempts  = {summary.extraction_attempts}")
    print(f"  extracted_facts      = {summary.extracted_facts}")
    print(f"  extractor_errors     = {summary.extractor_errors}")
    print(f"  historical_stamped   = {summary.historical_stamped}")
    print(f"  cost_ledger          ${summary.cumulative_usd} / ${summary.cap_usd}")
    print(f"  aborted_on_cost_cap  = {summary.aborted_on_cost_cap}")
    return 5 if summary.aborted_on_cost_cap else 0


if __name__ == "__main__":
    sys.exit(main())
