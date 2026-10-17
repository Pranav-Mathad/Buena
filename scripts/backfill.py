"""Targeted email backfill for a single property.

Walks the email archive, keeps only messages that mention one of the
property's unit-specific aliases, and pushes each through the same
per-event pipeline as the full archive loader
(:func:`connectors.buena_email_loader._process_event`): insert, route,
extract, apply. Use it to give one property complete coverage without
paying for the whole archive.

The run is sequential on purpose. A single extractor call that never
returns is bounded by ``GEMINI_EXTRACT_TIMEOUT_S``; running one event at a
time means a stuck call delays the run but cannot pile up behind other
in-flight calls. Spend is charged to a durable cost-ledger label, so the
cap holds across re-runs, and ``(source, source_ref)`` idempotency means a
re-run skips events that are already in the database.

Usage::

    python -m scripts.backfill --property <property-uuid>
    python -m scripts.backfill --property <property-uuid> --cap-usd 1.50

Requires the email archive under ``Extracted/`` (see
:func:`connectors.buena_archive.require_root`) and an extractor API key;
without a key the rule-based extractor runs and the ledger stays at zero.
"""

from __future__ import annotations

import argparse
import asyncio
import re
import sys
from collections.abc import Callable
from decimal import Decimal

import structlog
from sqlalchemy import text

from backend.db.session import get_sessionmaker
from backend.logging import configure_logging
from connectors import buena_archive, buena_email_loader, cost_ledger, eml_archive
from connectors.base import ConnectorEvent
from connectors.buena_email_loader import EmailBackfillSummary, _process_event
from connectors.migrations import apply_all as ensure_migrations

log = structlog.get_logger("scripts.backfill")

DEFAULT_LEDGER_LABEL = "property_backfill"
DEFAULT_CAP_USD = Decimal("3.00")


async def _load_property(property_id: str) -> tuple[list[str], str]:
    """Return the property's aliases (name included) and its building address."""
    factory = get_sessionmaker()
    async with factory() as session:
        row = (
            await session.execute(
                text(
                    """
                    SELECT p.name, p.aliases, b.address AS building_address
                    FROM properties p
                    LEFT JOIN buildings b ON b.id = p.building_id
                    WHERE p.id = :id
                    """
                ),
                {"id": property_id},
            )
        ).first()
    if row is None:
        raise SystemExit(f"property {property_id} not found")
    aliases = list(row.aliases or [])
    if row.name not in aliases:
        aliases.append(row.name)
    return aliases, str(row.building_address or "")


def specific_aliases(aliases: list[str], building_address: str) -> list[str]:
    """Drop aliases that identify the whole building rather than this unit.

    ``HAUS-NN`` ids and bare street names (any alias that is a prefix of the
    building's street, e.g. ``Musterstraße 26``) match every unit in the
    building, so keeping them would pull in other units' mail. What remains
    is unit-specific: ``EH-NNN``, ``WE NN``, the full unit name and floor
    descriptors such as ``4. OG mitte``.
    """
    street = building_address.split(",", 1)[0].strip().lower()
    keepers: list[str] = []
    for alias in aliases:
        cleaned = alias.strip()
        if not cleaned or cleaned.upper().startswith("HAUS-"):
            continue
        if street and street.startswith(cleaned.lower()):
            continue
        keepers.append(cleaned)
    return keepers


def alias_matcher(keepers: list[str]) -> Callable[[ConnectorEvent], bool]:
    """Return a predicate that is true when an event mentions any alias."""
    if not keepers:
        raise SystemExit("no unit-specific aliases left after filtering")
    pattern = re.compile("|".join(re.escape(k) for k in keepers), re.IGNORECASE)

    def matches(ev: ConnectorEvent) -> bool:
        haystack = ev.raw_content + " " + " ".join(
            str(v) for v in ev.metadata.values() if isinstance(v, str)
        )
        return pattern.search(haystack) is not None

    return matches


async def run(property_id: str, *, label: str, cap_usd: Decimal) -> EmailBackfillSummary:
    """Backfill one property's email history through the live extractor."""
    ensure_migrations()
    cost_ledger.ensure_label(label, cap_usd)

    aliases, building_address = await _load_property(property_id)
    keepers = specific_aliases(aliases, building_address)
    log.info("backfill.aliases", property_id=property_id, aliases=keepers)
    keep = alias_matcher(keepers)

    emails_dir = buena_archive.require_root() / "emails"
    events = (ev for ev in eml_archive.walk_directory(emails_dir) if keep(ev))

    summary = EmailBackfillSummary(label=label)
    summary.cap_usd = str(cap_usd)
    summary.concurrency = 1

    factory = get_sessionmaker()
    known_sender_domains = await buena_email_loader._load_known_sender_domains(factory)
    ledger_lock = asyncio.Lock()

    seen = 0
    for ev in events:
        seen += 1
        proceed = await _process_event(
            factory,
            ev,
            summary=summary,
            label=label,
            cap_usd=cap_usd,
            dead_letter_after=3,
            reprocess_historical=False,
            ledger_lock=ledger_lock,
            known_sender_domains=known_sender_domains,
        )
        if not proceed:
            log.warning("backfill.cost_cap_hit", processed=seen)
            break

    state = cost_ledger.get_state(label)
    if state is not None:
        summary.cumulative_usd = str(state.cumulative_usd)
        summary.cap_usd = str(state.cap_usd)

    log.info("backfill.done", **summary.as_json())
    return summary


def main(argv: list[str] | None = None) -> int:
    """CLI entry point. Exit code 5 means the cost cap stopped the run."""
    configure_logging()
    parser = argparse.ArgumentParser(
        prog="python -m scripts.backfill",
        description="Backfill one property's email history through the extractor.",
    )
    parser.add_argument("--property", required=True, help="property UUID")
    parser.add_argument(
        "--ledger-label",
        default=DEFAULT_LEDGER_LABEL,
        help=f"cost-ledger label to charge (default: {DEFAULT_LEDGER_LABEL})",
    )
    parser.add_argument(
        "--cap-usd",
        type=Decimal,
        default=DEFAULT_CAP_USD,
        help=f"spend cap for the label in USD (default: {DEFAULT_CAP_USD})",
    )
    args = parser.parse_args(argv)
    summary = asyncio.run(run(args.property, label=args.ledger_label, cap_usd=args.cap_usd))
    print()
    print(f"backfill summary  (label={args.ledger_label})")
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
