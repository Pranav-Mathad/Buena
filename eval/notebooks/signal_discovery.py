"""Step 8 — signal discovery on Buena real data.

Run after Step 6's email backfill lands so the SQL has data to chew
on. Produces a markdown report (printed to stdout, redirect to
``eval/runs/YYYY-MM-DD-signal-discovery.md``) covering:

1. Top properties by event volume (90 d).
2. Overlapping maintenance categories per property.
3. Email response-time distribution per thread_id.
4. Bank late- / missing-payment patterns per property.
5. Contractor concentration per building.
6. Owner communication volume per property.
7. Unrouted ``verwendungszweck`` strings frequency table.
8. Threads with no resolution after 14+ days.

Each section emits a heading, a top-N table, and a one-line takeaway
on what kind of signal rule the data suggests. Step 8's quality bar
(``precision ≥ 80% on the eval set``) means each candidate must be
validated end-to-end before promotion to ``signal_rule_definitions``;
the discovery script just surfaces the candidates.

Usage::

    python -m eval.notebooks.signal_discovery
    python -m eval.notebooks.signal_discovery --out eval/runs/$(date +%Y-%m-%d)-signal-discovery.md

The script is intentionally read-only — no fact writes, no event
mutations. Safe to run while the backfill / live worker are active.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any

import structlog

from backend.config import get_settings  # noqa: F401 — kept for downstream tweaks
from backend.logging import configure_logging

log = structlog.get_logger("eval.signal_discovery")


# -----------------------------------------------------------------------------
# SQL queries — single source of truth so the report mirrors what runs.
# -----------------------------------------------------------------------------


SQL_TOP_PROPERTIES_BY_VOLUME = """
SELECT p.name AS property_name,
       COUNT(*) AS event_count,
       COUNT(*) FILTER (WHERE e.source = 'email')   AS email_count,
       COUNT(*) FILTER (WHERE e.source = 'bank')    AS bank_count,
       COUNT(*) FILTER (WHERE e.source = 'invoice') AS invoice_count
FROM events e
JOIN properties p ON p.id = e.property_id
WHERE e.received_at >= now() - INTERVAL '90 days'
GROUP BY p.name
ORDER BY event_count DESC
LIMIT 20;
"""


SQL_OVERLAPPING_MAINTENANCE = """
-- A property is "overlapping" when it has 2+ open maintenance facts
-- whose extracted categories differ. Useful for the "this property
-- has multiple chronic issues" signal candidate.
SELECT p.name,
       COUNT(DISTINCT f.field) AS distinct_open_issues,
       array_agg(DISTINCT f.field ORDER BY f.field) AS issues
FROM facts f
JOIN properties p ON p.id = f.property_id
WHERE f.section IN ('maintenance', 'building_maintenance')
  AND f.superseded_by IS NULL
  AND f.field LIKE 'open_%'
GROUP BY p.name
HAVING COUNT(DISTINCT f.field) >= 2
ORDER BY distinct_open_issues DESC
LIMIT 20;
"""


SQL_EMAIL_RESPONSE_TIME = """
-- For each thread, the distribution of inter-event lag (in hours)
-- between consecutive incoming emails. Skewed-right distributions
-- with a long tail point at "ignored thread" candidates.
WITH thread_events AS (
  SELECT (metadata->>'thread_id') AS thread_id,
         received_at,
         LAG(received_at) OVER (
           PARTITION BY (metadata->>'thread_id')
           ORDER BY received_at
         ) AS prev_received_at
  FROM events
  WHERE source = 'email'
    AND metadata->>'thread_id' IS NOT NULL
)
SELECT percentile_disc(0.5)  WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (received_at - prev_received_at)) / 3600.0) AS p50_hours,
       percentile_disc(0.9)  WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (received_at - prev_received_at)) / 3600.0) AS p90_hours,
       percentile_disc(0.99) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (received_at - prev_received_at)) / 3600.0) AS p99_hours,
       MAX(EXTRACT(EPOCH FROM (received_at - prev_received_at)) / 3600.0)              AS max_hours,
       COUNT(*)                                                                        AS lag_samples
FROM thread_events
WHERE prev_received_at IS NOT NULL;
"""


SQL_BANK_LATE_PATTERNS = """
-- Per-property: months in the last 12 with NO 'miete' bank inflow.
-- Properties with > 1 missing-month land near the top.
WITH months AS (
  SELECT generate_series(
           date_trunc('month', now()) - INTERVAL '11 months',
           date_trunc('month', now()),
           INTERVAL '1 month'
         ) AS month_start
),
property_month_received AS (
  SELECT p.id   AS property_id,
         p.name AS property_name,
         m.month_start
  FROM properties p
  CROSS JOIN months m
),
miete_inflows AS (
  SELECT property_id,
         date_trunc('month', received_at) AS month_start
  FROM events
  WHERE source = 'bank'
    AND metadata->>'kategorie' = 'miete'
    AND property_id IS NOT NULL
)
SELECT pmr.property_name,
       COUNT(*) FILTER (WHERE m.property_id IS NULL) AS months_missing,
       COUNT(*) FILTER (WHERE m.property_id IS NOT NULL) AS months_paid
FROM property_month_received pmr
LEFT JOIN miete_inflows m
  ON m.property_id = pmr.property_id
 AND m.month_start = pmr.month_start
GROUP BY pmr.property_name
HAVING COUNT(*) FILTER (WHERE m.property_id IS NULL) >= 2
ORDER BY months_missing DESC
LIMIT 20;
"""


SQL_CONTRACTOR_CONCENTRATION = """
-- For each building, the fraction of invoices coming from a single
-- contractor. ≥ 60% concentration is a "single point of dependency"
-- candidate signal.
SELECT b.address                                        AS building_address,
       (e.metadata->>'gegen_name')                      AS contractor,
       COUNT(*)                                         AS invoice_count,
       ROUND(
         100.0 * COUNT(*) /
         SUM(COUNT(*)) OVER (PARTITION BY b.address),
         1
       )                                                AS pct_of_building_invoices
FROM events e
JOIN buildings b ON b.id = e.building_id
WHERE e.source = 'invoice'
  AND e.received_at >= now() - INTERVAL '12 months'
  AND e.metadata->>'gegen_name' IS NOT NULL
GROUP BY b.address, e.metadata->>'gegen_name'
HAVING SUM(COUNT(*)) OVER (PARTITION BY b.address) >= 5
ORDER BY pct_of_building_invoices DESC
LIMIT 20;
"""


SQL_OWNER_COMM_VOLUME = """
SELECT p.name,
       COUNT(*) AS owner_comm_events
FROM events e
JOIN properties p ON p.id = e.property_id
LEFT JOIN facts f
  ON f.source_event_id = e.id
 AND f.section IN ('overview', 'owner_communication', 'liegenschaft_overview')
WHERE e.source = 'email'
  AND e.received_at >= now() - INTERVAL '90 days'
  AND f.id IS NOT NULL
GROUP BY p.name
HAVING COUNT(*) >= 3
ORDER BY owner_comm_events DESC
LIMIT 20;
"""


SQL_UNROUTED_VERWENDUNGSZWECK = """
-- Most common verwendungszweck strings on unrouted bank rows. Top
-- entries are the keyword-discovery candidates Step 8 wants to surface
-- for promotion into the WEG-keyword list or the alias seed.
SELECT (metadata->>'verwendungszweck') AS verwendungszweck,
       COUNT(*)                        AS occurrences
FROM events
WHERE source = 'bank'
  AND property_id IS NULL
  AND building_id IS NULL
  AND liegenschaft_id IS NULL
  AND metadata->>'verwendungszweck' IS NOT NULL
GROUP BY metadata->>'verwendungszweck'
ORDER BY occurrences DESC
LIMIT 30;
"""


SQL_THREADS_NO_RESOLUTION = """
-- Threads where the latest message is > 14 days old AND no fact
-- was written that came from a downstream message in that thread.
-- This is a "stale conversation" candidate — high precision when
-- we filter to threads that started with maintenance / complaint
-- categories.
WITH thread_last AS (
  SELECT (metadata->>'thread_id') AS thread_id,
         MAX(received_at)         AS last_received_at,
         COUNT(*)                 AS messages
  FROM events
  WHERE source = 'email'
    AND metadata->>'thread_id' IS NOT NULL
  GROUP BY metadata->>'thread_id'
),
thread_first_property AS (
  SELECT DISTINCT ON ((metadata->>'thread_id'))
         (metadata->>'thread_id') AS thread_id,
         property_id,
         (metadata->>'subject')   AS subject
  FROM events
  WHERE source = 'email'
    AND metadata->>'thread_id' IS NOT NULL
  ORDER BY metadata->>'thread_id', received_at ASC
)
SELECT tl.thread_id,
       p.name                  AS property_name,
       tfp.subject,
       tl.messages,
       (now() - tl.last_received_at) AS staleness
FROM thread_last tl
JOIN thread_first_property tfp ON tfp.thread_id = tl.thread_id
LEFT JOIN properties p ON p.id = tfp.property_id
WHERE tl.last_received_at < now() - INTERVAL '14 days'
  AND tl.messages >= 2
ORDER BY tl.last_received_at ASC
LIMIT 20;
"""


# -----------------------------------------------------------------------------
# Renderer
# -----------------------------------------------------------------------------


def _render_section(
    title: str, sql: str, takeaway: str, rows: list[dict[str, Any]]
) -> str:
    """Format one section as Markdown."""
    if not rows:
        return f"## {title}\n\n_No rows returned._\n\n**Takeaway:** {takeaway}\n"
    headers = list(rows[0].keys())
    lines = [
        f"## {title}",
        "",
        "| " + " | ".join(headers) + " |",
        "|" + "|".join("---" for _ in headers) + "|",
    ]
    for r in rows:
        lines.append("| " + " | ".join(str(r.get(h, "")) for h in headers) + " |")
    lines += ["", f"**Takeaway:** {takeaway}", ""]
    return "\n".join(lines)


SECTIONS: list[tuple[str, str, str]] = [
    (
        "1. Top properties by event volume (90 d)",
        SQL_TOP_PROPERTIES_BY_VOLUME,
        "Concentration is signal: a single property carrying > 30 % of email "
        "traffic is a candidate for an 'overheated property' alert.",
    ),
    (
        "2. Properties with overlapping open maintenance issues",
        SQL_OVERLAPPING_MAINTENANCE,
        "A property with 3+ distinct open maintenance fields has a "
        "compounding-issues signal candidate.",
    ),
    (
        "3. Email-thread response-time distribution",
        SQL_EMAIL_RESPONSE_TIME,
        "Use p90 + p99 to set the threshold for an 'ignored thread' "
        "rule (Section 8 below catches the long tail directly).",
    ),
    (
        "4. Bank inflow gaps per property (12 mo)",
        SQL_BANK_LATE_PATTERNS,
        "≥ 2 missing months is the precision-80 % candidate cutoff "
        "(needs validation on the eval set).",
    ),
    (
        "5. Contractor concentration per building",
        SQL_CONTRACTOR_CONCENTRATION,
        "≥ 60 % of invoices from one contractor is the dependency-risk "
        "candidate.",
    ),
    (
        "6. Owner-communication volume per property",
        SQL_OWNER_COMM_VOLUME,
        "Spikes vs the 90-day baseline are the 'owner is escalating' "
        "candidate (compute baseline in a follow-up notebook).",
    ),
    (
        "7. Unrouted bank verwendungszweck top-30",
        SQL_UNROUTED_VERWENDUNGSZWECK,
        "Promotion candidates: any string occurring 5+ times that has a "
        "WEG-shaped semantic adds to the WEG-keyword list with precision "
        "validation on the eval set.",
    ),
    (
        "8. Stale threads (no traffic in 14+ days)",
        SQL_THREADS_NO_RESOLUTION,
        "Filter to threads that started with category ∈ {maintenance, "
        "complaint, payment} for the precision-80 % rule.",
    ),
]


# -----------------------------------------------------------------------------
# CLI
# -----------------------------------------------------------------------------


async def _execute_all() -> str:
    """Run every section against the live DB and return Markdown."""
    from sqlalchemy import text  # noqa: PLC0415 — local import, lazy DB binding
    from backend.db.session import get_sessionmaker  # noqa: PLC0415

    factory = get_sessionmaker()
    parts: list[str] = ["# Buena signal-discovery report", ""]
    async with factory() as session:
        for title, sql, takeaway in SECTIONS:
            try:
                rows = (await session.execute(text(sql))).mappings().all()
                parts.append(_render_section(title, sql, takeaway, [dict(r) for r in rows]))
            except Exception as exc:  # noqa: BLE001 — keep the report going
                log.exception("signal_discovery.section_error", title=title)
                parts.append(
                    f"## {title}\n\n_Section failed: {type(exc).__name__}: {exc}_\n"
                )
    return "\n".join(parts)


def main(argv: list[str] | None = None) -> int:
    """Print the report to stdout (or write to ``--out``)."""
    import asyncio  # noqa: PLC0415

    configure_logging()
    parser = argparse.ArgumentParser(prog="eval.notebooks.signal_discovery")
    parser.add_argument(
        "--out",
        default=None,
        help="Optional Markdown destination "
        "(e.g. eval/runs/2026-04-26-signal-discovery.md).",
    )
    args = parser.parse_args(argv)

    markdown = asyncio.run(_execute_all())
    if args.out:
        path = Path(args.out)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(markdown, encoding="utf-8")
        log.info("signal_discovery.written", path=str(path))
    print(markdown)
    return 0


if __name__ == "__main__":
    sys.exit(main())
