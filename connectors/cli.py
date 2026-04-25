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
from connectors import cost_ledger as cost_ledger_module
from connectors.buena_email_loader import (
    LEDGER_LABEL as EMAIL_LEDGER_LABEL,
    EmailBackfillSummary,
    run_backfill_emails,
)
from connectors.buena_event_loader import (
    BackfillSummary,
    RerouteSummary,
    run_backfill_bank,
    run_backfill_invoices,
    run_re_route,
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
    inserted = summary.inserted_now or 1  # avoid div-by-zero in pct calcs
    pct_unrouted = (summary.unrouted / inserted * 100) if summary.inserted_now else 0.0
    return (
        f"{summary.label} backfill summary\n"
        f"  total_seen          = {summary.total_seen}\n"
        f"  inserted_now        = {summary.inserted_now}\n"
        f"  routed_property     = {summary.routed_property}\n"
        f"  routed_building     = {summary.routed_building}\n"
        f"  routed_liegenschaft = {summary.routed_liegenschaft}\n"
        f"  routed_total        = {summary.routed}\n"
        f"  unrouted            = {summary.unrouted}  ({pct_unrouted:.1f}% of inserted)\n"
        f"  facts_written       = {summary.facts_written}\n"
        f"  top_miss_reasons:\n{miss_lines}\n"
    )


def _format_reroute(summary: RerouteSummary) -> str:
    """Pretty multi-line summary for the one-time re-route migration."""
    miss = summary.miss_reasons or {}
    top = sorted(miss.items(), key=lambda kv: kv[1], reverse=True)[:5]
    miss_lines = "\n".join(f"      {n:>4}  {reason}" for reason, n in top) or "      (none)"
    return (
        "re-route summary (one-time migration step)\n"
        f"  scanned                = {summary.scanned}\n"
        f"  moved_to_property      = {summary.moved_to_property}\n"
        f"  moved_to_building      = {summary.moved_to_building}\n"
        f"  moved_to_liegenschaft  = {summary.moved_to_liegenschaft}\n"
        f"  still_unrouted         = {summary.still_unrouted}\n"
        f"  facts_written          = {summary.facts_written}\n"
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


def _format_email_backfill(summary: EmailBackfillSummary) -> str:
    """Pretty multi-line summary for the email backfill."""
    inserted = summary.inserted_now or 1
    pct_routed = (
        (summary.routed_property + summary.routed_building + summary.routed_liegenschaft)
        / inserted * 100 if summary.inserted_now else 0.0
    )
    pct_unrouted = (summary.unrouted / inserted * 100) if summary.inserted_now else 0.0
    miss_lines = (
        "\n".join(
            f"      {n:>4}  {reason}"
            for reason, n in sorted(
                summary.miss_reasons.items(), key=lambda kv: kv[1], reverse=True
            )[:5]
        )
        or "      (none)"
    )
    err_lines = (
        "\n".join(f"      {s}" for s in summary.error_samples) or "      (none)"
    )
    top_props = (
        "\n".join(
            f"      {n:>4}  {name}"
            for name, n in sorted(
                summary.top_property_event_counts.items(),
                key=lambda kv: kv[1],
                reverse=True,
            )[:10]
        )
        or "      (no property-routed events yet)"
    )
    return (
        f"{summary.label} backfill summary  (concurrency={summary.concurrency})\n"
        f"  total_seen           = {summary.total_seen}\n"
        f"  inserted_now         = {summary.inserted_now}\n"
        f"  routed_property      = {summary.routed_property}\n"
        f"  routed_building      = {summary.routed_building}\n"
        f"  routed_liegenschaft  = {summary.routed_liegenschaft}\n"
        f"  unrouted             = {summary.unrouted}  "
        f"({pct_unrouted:.1f}% of inserted)\n"
        f"  routed_total_pct     = {pct_routed:.1f}% of inserted\n"
        f"  extraction_attempts  = {summary.extraction_attempts}\n"
        f"  extracted_facts      = {summary.extracted_facts}\n"
        f"  extractor_errors     = {summary.extractor_errors}\n"
        f"  failed_events_dead   = {summary.failed_events}\n"
        f"  historical_stamped   = {summary.historical_stamped}\n"
        f"  aborted_on_cost_cap  = {summary.aborted_on_cost_cap}\n"
        f"  cost_ledger          ${summary.cumulative_usd} / ${summary.cap_usd}\n"
        f"  unrouted_reason_buckets:\n{miss_lines}\n"
        f"  error_samples:\n{err_lines}\n"
        f"  top_10_properties:\n{top_props}\n"
    )


def _cmd_backfill_emails(args: argparse.Namespace) -> int:
    """Handle the ``backfill-emails`` subcommand."""
    if args.source not in SUPPORTED_SOURCES:
        print(
            f"unknown --source: {args.source!r}; "
            f"supported: {', '.join(SUPPORTED_SOURCES)}",
            file=sys.stderr,
        )
        return 2

    if args.reset_cost_ledger:
        # Friction gate — ledger reset is intentional. The y/N prompt
        # makes it impossible to wipe spend by reflex.
        prompt = (
            f"Reset cost ledger row '{EMAIL_LEDGER_LABEL}' "
            "(deletes durable spend record)? [y/N] "
        )
        try:
            answer = input(prompt).strip().lower()
        except EOFError:
            answer = ""
        if answer != "y":
            print("aborted: cost ledger NOT reset", file=sys.stderr)
            return 4
        cost_ledger_module.reset_label(EMAIL_LEDGER_LABEL)
        print(f"cost ledger row '{EMAIL_LEDGER_LABEL}' reset")

    from decimal import Decimal as _Decimal  # noqa: PLC0415 — local import

    try:
        summary = run_backfill_emails(
            extracted_root=args.extracted_root,
            cap_usd=_Decimal(str(args.max_total_cost_usd)),
            dead_letter_after=args.dead_letter_after,
            reprocess_historical=args.reprocess_historical,
            limit=args.limit,
            concurrency=args.concurrency,
        )
    except DataMissing as exc:
        print(f"buena dataset missing: {exc}", file=sys.stderr)
        return 3

    if args.json:
        print(json.dumps(summary.as_json(), indent=2))
    else:
        print(_format_email_backfill(summary))

    # Non-zero exit when the run aborted on the cost cap so CI / cron
    # workflows surface it as a failure rather than silent success.
    return 5 if summary.aborted_on_cost_cap else 0


def _cmd_re_route(args: argparse.Namespace) -> int:
    """Handle the one-time ``re-route`` subcommand.

    Walks every event with no scope set and re-evaluates routing using
    the current rules. Stamps property_id / building_id / liegenschaft_id
    on rows that now match. Idempotent: a second call finds nothing to
    scan unless new events are added in between.
    """
    sources = list(args.sources) if args.sources else None
    summary = run_re_route(sources)
    if args.json:
        print(json.dumps(summary.as_json(), indent=2))
    else:
        print(_format_reroute(summary))
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

    emails = sub.add_parser(
        "backfill-emails",
        help=(
            "Walk the customer .eml archive through the live extraction "
            "pipeline. Cost-bounded by --max-total-cost-usd against the "
            "durable cost ledger; resumable across runs."
        ),
    )
    emails.add_argument("--source", required=True, choices=SUPPORTED_SOURCES)
    emails.add_argument("--extracted-root", default=None)
    emails.add_argument(
        "--max-total-cost-usd",
        type=float,
        default=20.00,
        help="Hard ceiling on the cost ledger for label "
        f"'{EMAIL_LEDGER_LABEL}'. Default: 20.00.",
    )
    emails.add_argument(
        "--dead-letter-after",
        type=int,
        default=3,
        help="failed_events.retry_count ceiling before an event is "
        "considered permanently failed. Default: 3.",
    )
    emails.add_argument(
        "--reprocess-historical",
        action="store_true",
        help="When set, events older than 30 days are stamped "
        "processed_at=now() rather than processed_at=received_at. "
        "Phase 9 migration flips this on once the validator/uncertainty "
        "layers ship; default keeps backfill quiet for the live worker.",
    )
    emails.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Stop after N .eml files. Use for dry-runs without burning "
        "the full cost cap.",
    )
    emails.add_argument(
        "--concurrency",
        type=int,
        default=1,
        help="Number of parallel extraction workers. Default 1 "
        "(sequential). At N>1 the cost ledger charge() call is "
        "serialized via asyncio.Lock; worst-case overshoot if the "
        "cap is breached mid-flight is bounded by N * max_call_cost. "
        "See DECISIONS.md for the auditable bound.",
    )
    emails.add_argument(
        "--reset-cost-ledger",
        action="store_true",
        help="Friction-gated: prompts y/N then deletes the durable "
        f"spend row for label '{EMAIL_LEDGER_LABEL}' before starting.",
    )
    emails.add_argument("--json", action="store_true")
    emails.set_defaults(func=_cmd_backfill_emails)

    reroute = sub.add_parser(
        "re-route",
        help=(
            "ONE-TIME migration: re-evaluate routing on every event that "
            "has no scope set. Idempotent — safe to re-run."
        ),
    )
    reroute.add_argument(
        "--sources",
        nargs="*",
        default=None,
        help="Optional list of event sources to limit the rescan to "
        "(default: all sources).",
    )
    reroute.add_argument("--json", action="store_true")
    reroute.set_defaults(func=_cmd_re_route)

    return parser


def main(argv: list[str] | None = None) -> int:
    """CLI entry point."""
    configure_logging()
    parser = build_parser()
    args = parser.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    sys.exit(main())
