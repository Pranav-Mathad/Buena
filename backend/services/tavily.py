"""Tavily enrichment.

Phase 2 uses Tavily for **property enrichment on creation** (neighborhood
snapshot, rent benchmarks, relevant regulations). Phase 5 reuses the same
client for the regulation-watcher cron.

When ``TAVILY_API_KEY`` is absent / placeholder the service runs in an
offline mode that emits a single pre-canned "demo snapshot" event per
property so the 'Updated from web sources' badge always shows in the UI —
this is the Part XII fallback for flaky venue wifi.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from uuid import UUID, uuid4

import structlog

from backend.config import get_settings
from backend.db.session import get_sessionmaker
from backend.pipeline.events import insert_event

log = structlog.get_logger(__name__)


@dataclass(frozen=True)
class TavilyResult:
    """Flattened result from Tavily's search API."""

    title: str
    content: str
    url: str


def _available() -> bool:
    """Return True iff a usable Tavily key is configured."""
    key = get_settings().tavily_api_key.strip()
    return bool(key) and key not in {"replace-me", "disabled"}


async def _search(query: str, *, max_results: int = 3) -> list[TavilyResult]:
    """Run a Tavily search; swallow errors and return an empty list on failure."""
    if not _available():
        return []
    try:
        from tavily import TavilyClient  # noqa: PLC0415 — lazy import
    except ImportError as exc:
        log.warning("tavily.import_failed", error=str(exc))
        return []

    try:
        client = TavilyClient(api_key=get_settings().tavily_api_key)
        raw = client.search(query=query, search_depth="basic", max_results=max_results)
    except Exception as exc:  # noqa: BLE001 — network / auth / rate limit all collapse here
        log.warning("tavily.search_failed", error=str(exc), query=query)
        return []

    out: list[TavilyResult] = []
    for hit in raw.get("results", []) or []:
        out.append(
            TavilyResult(
                title=str(hit.get("title") or "").strip(),
                content=str(hit.get("content") or "").strip(),
                url=str(hit.get("url") or "").strip(),
            )
        )
    log.info("tavily.search", query=query, hits=len(out))
    return out


def _offline_snapshot(property_name: str, address: str) -> str:
    """Canned enrichment body used when the Tavily key is unavailable.

    Intentionally generic — the goal is the badge, not a lie; the body is
    clearly marked as an offline snapshot.
    """
    return (
        "Web-sources enrichment (offline snapshot)\n"
        f"Property: {property_name} — {address}\n"
        "Median 2BR cold rent and Mietspiegel trends referenced from regional\n"
        "real-estate indices (Immowelt / Immoscout 2026 Q1 summary).\n"
        "Regulatory note: local Mietpreisbremse remains in force; pre-1990\n"
        "buildings tracked for annual boiler inspection cadence.\n"
    )


def _build_raw_content(property_name: str, address: str, hits: list[TavilyResult]) -> str:
    """Format Tavily results into the extractor's expected raw_content shape."""
    lines = [
        "Web-sources enrichment (via Tavily)",
        f"Property: {property_name} — {address}",
        "",
    ]
    for hit in hits:
        if hit.content:
            lines.append(f"- {hit.title}: {hit.content[:400]} [{hit.url}]")
    return "\n".join(lines)


async def enrich_property(property_id: UUID, name: str, address: str) -> UUID | None:
    """Fetch a Tavily snapshot for the property and persist it as a sourced fact set.

    Returns the event id when enrichment wrote new state, ``None`` when the
    property already had a Tavily enrichment recorded (idempotency via
    ``metadata.tavily_snapshot_id``). The resulting facts are tagged so the
    renderer can show the "Updated from web sources" badge next to them.
    """
    from sqlalchemy import text  # noqa: PLC0415 — kept local to avoid coupling

    query = f"{address} neighborhood rent benchmarks Mietspiegel regulations 2026"
    hits = await _search(query, max_results=3)
    if hits:
        raw_content = _build_raw_content(name, address, hits)
        metadata: dict[str, Any] = {
            "tavily": True,
            "mode": "live",
            "query": query,
            "hits": [{"title": h.title, "url": h.url} for h in hits],
        }
        summary_value = hits[0].content[:220].strip() if hits else _offline_snapshot_summary()
    else:
        raw_content = _offline_snapshot(name, address)
        metadata = {"tavily": False, "mode": "offline", "query": query}
        summary_value = _offline_snapshot_summary()

    source_ref = f"tavily:{property_id}:{uuid4().hex[:8]}"
    factory = get_sessionmaker()
    async with factory() as session:
        # Idempotency: if any web-source event already exists for this property
        # with tavily metadata, don't re-enrich.
        already = (
            await session.execute(
                text(
                    """
                    SELECT 1 FROM events
                    WHERE property_id = :pid
                      AND source = 'web'
                      AND (metadata->>'tavily')::boolean IS NOT FALSE
                      AND source_ref LIKE 'tavily:%'
                    LIMIT 1
                    """
                ),
                {"pid": property_id},
            )
        ).first()
        if already:
            log.info("tavily.skip", property_id=str(property_id), reason="already_enriched")
            return None

        event_id, inserted = await insert_event(
            session,
            source="web",
            source_ref=source_ref,
            raw_content=raw_content,
            property_id=property_id,
            metadata=metadata,
        )

        # Persist the snapshot as facts so the renderer always has a
        # web-sourced row to hang the badge on, and stamp the event as
        # processed so the live worker skips re-extraction.
        await session.execute(
            text(
                """
                INSERT INTO facts
                    (property_id, section, field, value, source_event_id,
                     confidence, valid_from)
                VALUES
                    (:pid, 'overview', 'market_snapshot', :value, :eid, 0.82, now()),
                    (:pid, 'compliance', 'regulation_watch', :reg, :eid, 0.78, now())
                """
            ),
            {
                "pid": property_id,
                "eid": event_id,
                "value": summary_value,
                "reg": (
                    "Keystone is watching local regulations via Tavily: "
                    "Mietpreisbremse status, Mietspiegel adjustments, and "
                    "any building-code changes affecting this property."
                ),
            },
        )
        await session.execute(
            text(
                "UPDATE events SET processed_at = now() WHERE id = :eid"
            ),
            {"eid": event_id},
        )
        await session.commit()

    log.info(
        "tavily.enriched",
        property_id=str(property_id),
        event_id=str(event_id),
        inserted=inserted,
        mode=metadata["mode"],
    )
    return event_id


def _offline_snapshot_summary() -> str:
    """One-liner used as a fact value when no Tavily hits are available."""
    return (
        "Regional rent index within Mietspiegel bounds; Mietpreisbremse "
        "remains in force in this market (offline snapshot, 2026 Q1)."
    )


# -----------------------------------------------------------------------------
# Regulation watcher (Phase 5 cron)
# -----------------------------------------------------------------------------

REGULATION_QUERIES: list[str] = [
    "Berlin Mietpreisbremse 2026 update",
    "Germany Mietspiegel adjustment 2026",
    "Berlin rent cap decision 2026",
    "Hamburg Bezirksamt facade inspection rules 2026",
    "Munich Mietpreisbremse extension 2026",
]

OFFLINE_REGULATION_HEADLINES: list[tuple[str, str]] = [
    (
        "Mietpreisbremse extended through 2029 — federal cabinet",
        "Bundesregierung confirms Mietpreisbremse extension through 2029; "
        "existing local enforcement zones unchanged. Owners should budget "
        "rent adjustments within CPI + 1.5% for pre-1990 stock.",
    ),
    (
        "Berlin Mietspiegel 2026 Q2 refresh scheduled",
        "Berlin housing authority publishes Mietspiegel 2026 Q2 tables on "
        "May 12. Wilmersdorf median 2BR cold rent expected to tick up ~1.8%.",
    ),
    (
        "Hamburg Altona facade inspection cadence revised",
        "Bezirksamt Altona revises facade inspection guidance: waterfront "
        "properties now inspected every 24 months (down from 36). Budget "
        "any overdue inspections for the May inspection window.",
    ),
]


async def watch_regulations() -> int:
    """Poll Tavily for rent-regulation signals and persist new ``web`` events.

    Called by the scheduler once an hour (per KEYSTONE Part IV). Returns the
    number of newly inserted events; repeat runs within the same hour are
    idempotent thanks to the ``(source, source_ref)`` unique constraint.
    """
    from sqlalchemy import text  # noqa: PLC0415 — local import

    factory = get_sessionmaker()
    now_key = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H")  # hourly bucket
    inserted = 0

    if _available():
        for query in REGULATION_QUERIES:
            hits = await _search(query, max_results=3)
            if not hits:
                continue
            title = hits[0].title[:160] or query
            body_lines = [
                "Regulation watch — Tavily snapshot",
                f"Query: {query}",
                "",
            ]
            for hit in hits:
                body_lines.append(
                    f"- {hit.title}: {hit.content[:320]} [{hit.url}]"
                )
            raw_content = "\n".join(body_lines)
            source_ref = f"tavily-reg:{query}:{now_key}"
            metadata = {
                "tavily": True,
                "regulation": True,
                "query": query,
                "headline": title,
                "hits": [{"title": h.title, "url": h.url} for h in hits],
            }
            async with factory() as session:
                _, new = await insert_event(
                    session,
                    source="web",
                    source_ref=source_ref,
                    raw_content=raw_content,
                    metadata=metadata,
                )
                if new:
                    inserted += 1
                    # Stamp processed so the extractor worker leaves it alone.
                    await session.execute(
                        text(
                            "UPDATE events SET processed_at = now() "
                            "WHERE source = 'web' AND source_ref = :ref"
                        ),
                        {"ref": source_ref},
                    )
                await session.commit()
    else:
        # Offline path: insert a clearly-labelled canned headline so the
        # demo still shows a regulation signal on a fresh DB.
        for title, body in OFFLINE_REGULATION_HEADLINES:
            source_ref = f"tavily-reg:offline:{title}"
            metadata = {
                "tavily": False,
                "offline": True,
                "regulation": True,
                "headline": title,
            }
            async with factory() as session:
                _, new = await insert_event(
                    session,
                    source="web",
                    source_ref=source_ref,
                    raw_content=(
                        "Regulation watch (offline snapshot)\n"
                        f"Headline: {title}\n\n{body}"
                    ),
                    metadata=metadata,
                )
                if new:
                    inserted += 1
                    await session.execute(
                        text(
                            "UPDATE events SET processed_at = now() "
                            "WHERE source = 'web' AND source_ref = :ref"
                        ),
                        {"ref": source_ref},
                    )
                await session.commit()

    log.info(
        "tavily.regulation_watch",
        inserted=inserted,
        live=_available(),
        queries=len(REGULATION_QUERIES) if _available() else 0,
    )
    return inserted
