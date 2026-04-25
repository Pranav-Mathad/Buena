"""Connector CLI — single entry point for every customer ingest path.

Usage::

    python -m connectors.cli load-stammdaten --source buena
    python -m connectors.cli load-stammdaten --source buena --json

Each subcommand is intentionally small: it parses arguments, calls the
matching connector / loader module, and prints a short human or JSON
summary. The heavy lifting lives in the connector modules.
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import Any

import structlog

from backend.logging import configure_logging
from connectors.base import DataMissing
from connectors.buena_event_loader import (
    BackfillSummary,
    run_backfill_bank,
    run_backfill_invoices,
)
from connectors.buena_loader import load_from_disk

log = structlog.get_logger("connectors.cli")


SUPPORTED_SOURCES = ("buena",)


def _format_summary(summary: dict[str, int]) -> str:
    """Pretty multi-line summary for the human path."""
    return (
        "Stammdaten load summary\n"
        f"  owners        total={summary['owners_total']:>4}  "
        f"new={summary['owners_inserted_now']:>4}\n"
        f"  buildings     total={summary['buildings_total']:>4}  "
        f"new={summary['buildings_inserted_now']:>4}\n"
        f"  contractors   total={summary['contractors_total']:>4}  "
        f"new={summary['contractors_inserted_now']:>4}\n"
        f"  properties    total={summary['properties_total']:>4}  "
        f"new={summary['properties_inserted_now']:>4}\n"
        f"  tenants       total={summary['tenants_total']:>4}  "
        f"new={summary['tenants_inserted_now']:>4}  "
        f"skipped_inactive={summary['tenants_skipped_inactive']}\n"
        f"  relationships idempotent_writes={summary['relationships_total']}\n"
    )


def _cmd_load_stammdaten(args: argparse.Namespace) -> int:
    """Handle the ``load-stammdaten`` subcommand."""
    if args.source not in SUPPORTED_SOURCES:
        print(
            f"unknown --source: {args.source!r}; "
            f"supported: {', '.join(SUPPORTED_SOURCES)}",
            file=sys.stderr,
        )
        return 2

    try:
        summary = load_from_disk(extracted_root=args.extracted_root)
    except DataMissing as exc:
        print(f"buena dataset missing: {exc}", file=sys.stderr)
        return 3

    payload: dict[str, Any] = summary.as_json()
    if args.json:
        print(json.dumps(payload, indent=2))
    else:
        print(_format_summary(payload))
    return 0


def _format_backfill(summary: BackfillSummary) -> str:
    """Pretty multi-line summary for the bank/invoice backfills."""
    miss = summary.miss_reasons or {}
    top = sorted(miss.items(), key=lambda kv: kv[1], reverse=True)[:5]
    miss_lines = "\n".join(f"      {n:>4}  {reason}" for reason, n in top) or "      (none)"
    pct = (
        f"{(summary.unrouted / summary.inserted_now * 100):.1f}%"
        if summary.inserted_now
        else "n/a"
    )
    return (
        f"{summary.label} backfill summary\n"
        f"  total_seen      = {summary.total_seen}\n"
        f"  inserted_now    = {summary.inserted_now}\n"
        f"  routed          = {summary.routed}\n"
        f"  unrouted        = {summary.unrouted}  ({pct} of inserted)\n"
        f"  facts_written   = {summary.facts_written}\n"
        f"  top_miss_reasons:\n{miss_lines}\n"
    )


def _cmd_backfill_bank(args: argparse.Namespace) -> int:
    """Handle the ``backfill-bank`` subcommand."""
    if args.source not in SUPPORTED_SOURCES:
        print(
            f"unknown --source: {args.source!r}; "
            f"supported: {', '.join(SUPPORTED_SOURCES)}",
            file=sys.stderr,
        )
        return 2
    try:
        summary = run_backfill_bank(extracted_root=args.extracted_root)
    except DataMissing as exc:
        print(f"buena dataset missing: {exc}", file=sys.stderr)
        return 3
    if args.json:
        print(json.dumps(summary.as_json(), indent=2))
    else:
        print(_format_backfill(summary))
    return 0


def _cmd_backfill_invoices(args: argparse.Namespace) -> int:
    """Handle the ``backfill-invoices`` subcommand."""
    if args.source not in SUPPORTED_SOURCES:
        print(
            f"unknown --source: {args.source!r}; "
            f"supported: {', '.join(SUPPORTED_SOURCES)}",
            file=sys.stderr,
        )
        return 2
    try:
        summary = run_backfill_invoices(extracted_root=args.extracted_root)
    except DataMissing as exc:
        print(f"buena dataset missing: {exc}", file=sys.stderr)
        return 3
    if args.json:
        print(json.dumps(summary.as_json(), indent=2))
    else:
        print(_format_backfill(summary))
    return 0


def build_parser() -> argparse.ArgumentParser:
    """Construct the argparse parser. Exposed for tests."""
    parser = argparse.ArgumentParser(
        prog="connectors.cli",
        description="Customer data ingestion — Buena is the first composer.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    load = sub.add_parser(
        "load-stammdaten",
        help="Upsert master data (owners/buildings/properties/tenants/contractors).",
    )
    load.add_argument(
        "--source",
        required=True,
        choices=SUPPORTED_SOURCES,
        help="Which customer composer to use.",
    )
    load.add_argument(
        "--extracted-root",
        default=None,
        help="Override EXTRACTED_ROOT (default: ./Extracted/).",
    )
    load.add_argument(
        "--json",
        action="store_true",
        help="Emit a single JSON summary instead of a human-readable block.",
    )
    load.set_defaults(func=_cmd_load_stammdaten)

    bank = sub.add_parser(
        "backfill-bank",
        help="Stream the customer bank ledger into events + financial facts.",
    )
    bank.add_argument("--source", required=True, choices=SUPPORTED_SOURCES)
    bank.add_argument("--extracted-root", default=None)
    bank.add_argument("--json", action="store_true")
    bank.set_defaults(func=_cmd_backfill_bank)

    invoices = sub.add_parser(
        "backfill-invoices",
        help="Walk the customer invoice archive into events + maintenance facts.",
    )
    invoices.add_argument("--source", required=True, choices=SUPPORTED_SOURCES)
    invoices.add_argument("--extracted-root", default=None)
    invoices.add_argument("--json", action="store_true")
    invoices.set_defaults(func=_cmd_backfill_invoices)

    return parser


def main(argv: list[str] | None = None) -> int:
    """CLI entry point."""
    configure_logging()
    parser = build_parser()
    args = parser.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    sys.exit(main())
