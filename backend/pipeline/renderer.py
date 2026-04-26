"""Render a property / building / Liegenschaft's current facts into markdown.

The renderer is the canonical read path. Phase 8.1 widens it from
"property only" to the three-tier hierarchy
(:func:`render_markdown` for property,
:func:`render_building_markdown` for Haus,
:func:`render_liegenschaft_markdown` for WEG).

A property's markdown ends with a **Building Context** block (most
recent N events for its building) and a **WEG Context** block (most
recent N for its Liegenschaft). Walking up the hierarchy honours the
PM mental model that a unit is part of a building, which is part of a
WEG — and events at higher tiers genuinely affect the unit even when
not directly attributed to it.

Every fact line carries the inline ``[source: <event_id>]`` link.
Web-sourced facts (Tavily) get a 🌐 badge.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from uuid import UUID

import structlog
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

log = structlog.get_logger(__name__)


SECTION_ORDER: tuple[str, ...] = (
    "overview",
    "tenants",
    "lease",
    "maintenance",
    "financials",
    "compliance",
    "activity",
    "patterns",
)

SECTION_TITLES: dict[str, str] = {
    "overview": "Overview",
    "tenants": "Tenants",
    "lease": "Lease",
    "maintenance": "Maintenance",
    "financials": "Financials",
    "compliance": "Compliance",
    "activity": "Activity",
    "patterns": "Patterns",
    "building_financials": "Building financials",
    "building_maintenance": "Building maintenance",
    "liegenschaft_financials": "WEG financials",
    "liegenschaft_maintenance": "WEG maintenance",
}

#: German section titles surfaced when a property's source-event language
#: majority is German (Phase 8 Step 5).
SECTION_TITLES_DE: dict[str, str] = {
    "overview": "Überblick",
    "tenants": "Mieter",
    "lease": "Mietvertrag",
    "maintenance": "Wartung",
    "financials": "Finanzen",
    "compliance": "Compliance",
    "activity": "Aktivität",
    "patterns": "Muster",
    "building_financials": "Haus-Finanzen",
    "building_maintenance": "Haus-Wartung",
    "liegenschaft_financials": "WEG-Finanzen",
    "liegenschaft_maintenance": "WEG-Wartung",
    "liegenschaft_compliance": "WEG-Compliance",
    "building_compliance": "Haus-Compliance",
}

CONTEXT_LABELS: dict[str, dict[str, str]] = {
    "en": {
        "building_context": "Building Context",
        "weg_context": "WEG Context",
        "building_subtitle": "Recent activity at the parent building",
        "weg_subtitle": "Recent activity at the WEG (Liegenschaft)",
        "open_building": "Open building view",
        "open_weg": "Open WEG view",
        "needs_review": "Needs Review",
        "stammdaten": "Stammdaten",
        "stammdaten_subtitle": "Master record from the lease/owner registry",
        "unit": "Unit",
        "tenant": "Current tenant",
        "owner": "Owner",
        "lease_start": "Lease start",
        "lease_end": "Lease end",
        "lease_open_ended": "open-ended",
        "rent_cold": "Cold rent",
        "operating_costs": "Operating costs prepayment",
        "deposit": "Deposit",
        "size_qm": "Size",
        "rooms": "Rooms",
        "no_active_tenant": "No active tenant on file",
        "older_label": "Older entries",
        "older_subtitle": "Compact pointers — click any item to open the original",
    },
    "de": {
        "building_context": "Hauskontext",
        "weg_context": "WEG-Kontext",
        "building_subtitle": "Letzte Aktivitäten am übergeordneten Haus",
        "weg_subtitle": "Letzte Aktivitäten in der WEG (Liegenschaft)",
        "open_building": "Hausansicht öffnen",
        "open_weg": "WEG-Ansicht öffnen",
        "needs_review": "Zu prüfen",
        "stammdaten": "Stammdaten",
        "stammdaten_subtitle": "Stammdaten aus dem Mietvertrags- und Eigentümerregister",
        "unit": "Einheit",
        "tenant": "Aktueller Mieter",
        "owner": "Eigentümer",
        "lease_start": "Mietbeginn",
        "lease_end": "Mietende",
        "lease_open_ended": "unbefristet",
        "rent_cold": "Kaltmiete",
        "operating_costs": "Nebenkosten-Vorauszahlung",
        "deposit": "Kaution",
        "size_qm": "Wohnfläche",
        "rooms": "Zimmer",
        "no_active_tenant": "Kein aktiver Mieter erfasst",
        "older_label": "Ältere Einträge",
        "older_subtitle": "Kompakte Verweise — anklicken öffnet das Original",
    },
}

#: How many recent events to surface in the per-tier context blocks.
CONTEXT_LIMIT: int = 5


#: Three-tier bucketing thresholds (Phase 12+).
#: - Active (≤ ACTIVE_DAYS): full detail, confidence inline, full source link.
#: - Recent (ACTIVE_DAYS < age ≤ RECENT_DAYS): tight one-liner with date + truncated value.
#: - Archive (> RECENT_DAYS): not rendered per-fact at all — collapsed into a
#:   single per-year roll-up block per section ("2024 — 12 entries; last: …").
#:
#: Picked so a property with decades of history stays bounded: an active
#: window of 90 days keeps the demo's "what's happening this quarter" beat
#: dense, while everything older compresses into a few summary lines.
ACTIVE_DAYS: int = 90
RECENT_DAYS: int = 365

# Backward-compat alias used by ``_is_compact``.
ACTIVE_CUTOFF_DAYS: int = RECENT_DAYS

#: Maximum characters of the value text we keep when emitting a
#: compact line. Anything longer gets ``…`` appended; the full text
#: is one click away via the source link.
COMPACT_VALUE_PREVIEW_CHARS: int = 100

#: The activity section is chronological — every processed event writes
#: a bullet there. Without a per-section cap, a property with 1000
#: events produces a 200 KB markdown body even when most of those
#: events landed today (so date-based bucketing doesn't compress them).
#: Cap the *active* render at this many entries; older entries fold
#: into the per-year archive roll-up alongside truly old facts.
ACTIVITY_ACTIVE_LIMIT: int = 15

#: Sections subject to ``ACTIVITY_ACTIVE_LIMIT``. Keep this list short —
#: fact-bearing sections (lease, financials, compliance) should never
#: silently drop facts; activity is the only chronological log section.
_CAPPED_SECTIONS: frozenset[str] = frozenset({"activity"})

#: Tier label returned by :func:`_classify_tier`.
FactTier = str  # 'active' | 'recent' | 'archive'


@dataclass(frozen=True)
class PropertyHeader:
    """Lightweight header info used to title the rendered markdown."""

    name: str
    address: str


@dataclass(frozen=True)
class FactRow:
    """A single current fact row as returned from the database."""

    section: str
    field: str
    value: str
    source_event_id: UUID | None
    confidence: float
    source: str | None
    # ``occurred_at`` is the original event's ``received_at`` — the moment
    # the fact came into the world, NOT when extraction happened. The
    # renderer buckets on this so a 20-year-old letter extracted today
    # still gets the compact render.
    occurred_at: datetime | None


@dataclass(frozen=True)
class Stammdaten:
    """Master-record snapshot for a property.

    Stammdaten lives outside the events/facts pipeline — it's the
    authoritative lease/owner registry loaded once at ingest. Surfacing
    it in the markdown closes the gap where a freshly-onboarded
    property has no extracted facts yet but still has a real tenant,
    rent, and lease window on file.
    """

    unit_label: str | None
    size_qm: float | None
    rooms: float | None
    lage: str | None
    tenant_name: str | None
    tenant_active: bool
    mietbeginn: str | None
    mietende: str | None
    kaltmiete: float | None
    nk_vorauszahlung: float | None
    kaution: float | None
    owner_name: str | None


async def _detect_property_language(
    session: AsyncSession,
    property_id: UUID,
    *,
    sample_limit: int = 8,
) -> str:
    """Return ``'de'`` or ``'en'`` based on the property's recent events.

    Heuristic: pull the last ``sample_limit`` event raw_contents,
    detect each, return the majority. Falls back to ``'en'`` when the
    sample is empty or the language detector fails on every row.
    """
    from backend.services.lang import detect_language  # noqa: PLC0415

    rows = (
        await session.execute(
            text(
                """
                SELECT raw_content FROM events
                WHERE property_id = :pid
                ORDER BY received_at DESC
                LIMIT :lim
                """
            ),
            {"pid": property_id, "lim": sample_limit},
        )
    ).all()
    if not rows:
        return "en"
    counts: dict[str, int] = {"de": 0, "en": 0}
    for r in rows:
        code = detect_language(str(r.raw_content or ""))
        if code in counts:
            counts[code] += 1
    if counts["de"] > counts["en"]:
        return "de"
    return "en"


async def _fetch_header(session: AsyncSession, property_id: UUID) -> PropertyHeader | None:
    """Look up the property's display name + address."""
    row = (
        await session.execute(
            text("SELECT name, address FROM properties WHERE id = :pid"),
            {"pid": property_id},
        )
    ).first()
    if row is None:
        return None
    return PropertyHeader(name=row.name, address=row.address)


async def _fetch_stammdaten(
    session: AsyncSession, property_id: UUID
) -> Stammdaten | None:
    """Pull the master-record snapshot for a property.

    Joins ``properties.metadata`` (unit dimensions), the active row in
    ``tenants`` (tenant identity + lease + rent), and ``owners.name``.
    Returns ``None`` only if the property itself doesn't exist; an
    otherwise empty stammdaten object still renders ("no active tenant
    on file") so operators can see what's missing.
    """
    row = (
        await session.execute(
            text(
                """
                SELECT
                    p.metadata AS p_meta,
                    o.name     AS owner_name,
                    t.name     AS tenant_name,
                    t.metadata AS t_meta
                FROM properties p
                LEFT JOIN owners o ON o.id = p.owner_id
                LEFT JOIN LATERAL (
                    SELECT t.name, t.metadata
                    FROM tenants t
                    WHERE t.property_id = p.id
                    ORDER BY
                        (t.metadata->>'active' = 'true') DESC,
                        (t.metadata->>'mietende') DESC NULLS FIRST,
                        t.move_in_date DESC NULLS LAST
                    LIMIT 1
                ) t ON TRUE
                WHERE p.id = :pid
                """
            ),
            {"pid": property_id},
        )
    ).first()
    if row is None:
        return None

    p_meta = dict(row.p_meta or {})
    t_meta = dict(row.t_meta or {})

    def _num(v: object) -> float | None:
        if v is None or v == "":
            return None
        try:
            return float(v)  # type: ignore[arg-type]
        except (TypeError, ValueError):
            return None

    return Stammdaten(
        unit_label=p_meta.get("einheit_nr") or p_meta.get("buena_eh_id"),
        size_qm=_num(p_meta.get("wohnflaeche_qm")),
        rooms=_num(p_meta.get("zimmer")),
        lage=p_meta.get("lage") or None,
        tenant_name=row.tenant_name,
        tenant_active=bool(t_meta.get("active")) or t_meta.get("mietende") in (None, ""),
        mietbeginn=t_meta.get("mietbeginn") or None,
        mietende=t_meta.get("mietende") or None,
        kaltmiete=_num(t_meta.get("kaltmiete")),
        nk_vorauszahlung=_num(t_meta.get("nk_vorauszahlung")),
        kaution=_num(t_meta.get("kaution")),
        owner_name=row.owner_name,
    )


async def _fetch_current_facts(session: AsyncSession, property_id: UUID) -> list[FactRow]:
    """Return all current (non-superseded) facts for the property.

    Pulls the originating event's ``received_at`` alongside each fact
    so the renderer can bucket on event-time (not extraction-time).
    """
    result = await session.execute(
        text(
            """
            SELECT f.section, f.field, f.value, f.source_event_id, f.confidence,
                   f.created_at, e.source AS source,
                   e.received_at AS occurred_at
            FROM facts f
            LEFT JOIN events e ON e.id = f.source_event_id
            WHERE f.property_id = :pid
              AND f.superseded_by IS NULL
            ORDER BY f.section, f.created_at ASC, f.field ASC
            """
        ),
        {"pid": property_id},
    )
    return [
        FactRow(
            section=row.section,
            field=row.field,
            value=row.value,
            source_event_id=row.source_event_id,
            confidence=float(row.confidence),
            source=row.source,
            occurred_at=row.occurred_at,
        )
        for row in result.all()
    ]


def _format_field(field: str) -> str:
    """Turn ``snake_case`` field names into human-readable titles."""
    return field.replace("_", " ").strip().capitalize()


def _format_eur(value: float | None) -> str | None:
    """Render a euro amount with thousand separators, or ``None``."""
    if value is None:
        return None
    return f"€{value:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def _format_stammdaten_block(s: Stammdaten, lang: str) -> list[str]:
    """Emit ``## Stammdaten`` lines. Empty values are skipped silently."""
    labels = CONTEXT_LABELS[lang]
    rows: list[tuple[str, str]] = []

    if s.unit_label:
        rows.append((labels["unit"], s.unit_label))
    if s.lage:
        rows.append(("Lage" if lang == "de" else "Location", s.lage))
    if s.size_qm is not None:
        size_str = f"{s.size_qm:.1f} m²".replace(".0 ", " ")
        rows.append((labels["size_qm"], size_str))
    if s.rooms is not None:
        rooms_str = f"{s.rooms:.1f}".rstrip("0").rstrip(".") or "0"
        rows.append((labels["rooms"], rooms_str))
    if s.owner_name:
        rows.append((labels["owner"], s.owner_name))

    if s.tenant_name:
        rows.append((labels["tenant"], s.tenant_name))
        if s.mietbeginn:
            rows.append((labels["lease_start"], s.mietbeginn))
        rows.append(
            (
                labels["lease_end"],
                s.mietende if s.mietende else labels["lease_open_ended"],
            )
        )
        kalt = _format_eur(s.kaltmiete)
        if kalt:
            rows.append((labels["rent_cold"], kalt))
        nk = _format_eur(s.nk_vorauszahlung)
        if nk:
            rows.append((labels["operating_costs"], nk))
        kaution = _format_eur(s.kaution)
        if kaution:
            rows.append((labels["deposit"], kaution))

    out: list[str] = [
        f"## {labels['stammdaten']}",
        "",
        f"_{labels['stammdaten_subtitle']}_",
        "",
    ]
    if not s.tenant_name:
        out.append(f"- **{labels['tenant']}:** {labels['no_active_tenant']}")
    out.extend(f"- **{label}:** {value}" for label, value in rows)
    out.append("")
    return out


def _classify_tier(fact: FactRow, *, now: datetime | None = None) -> FactTier:
    """Bucket a fact into ``active`` / ``recent`` / ``archive``.

    Facts with no ``occurred_at`` (manual writes, master-data backfill)
    are treated as ``active`` — operators want them visible and
    editable rather than buried in an archive roll-up.
    """
    occurred = fact.occurred_at
    if occurred is None:
        return "active"
    if occurred.tzinfo is None:
        occurred = occurred.replace(tzinfo=timezone.utc)
    reference = now or datetime.now(timezone.utc)
    age = reference - occurred
    if age <= timedelta(days=ACTIVE_DAYS):
        return "active"
    if age <= timedelta(days=RECENT_DAYS):
        return "recent"
    return "archive"


def _is_compact(fact: FactRow, *, now: datetime | None = None) -> bool:
    """Backward-compat predicate kept for the legacy two-tier callers.

    True for anything outside the active window (i.e., ``recent`` *or*
    ``archive``). The Phase 12 renderer uses :func:`_classify_tier`
    directly; this stays so external callers that imported the old
    name don't break.
    """
    occurred = fact.occurred_at
    if occurred is None:
        return False
    if occurred.tzinfo is None:
        occurred = occurred.replace(tzinfo=timezone.utc)
    reference = now or datetime.now(timezone.utc)
    return (reference - occurred) > timedelta(days=ACTIVE_CUTOFF_DAYS)


def _truncate(text_value: str, *, limit: int = COMPACT_VALUE_PREVIEW_CHARS) -> str:
    """One-line preview of a fact value, ellipsised if it overflows."""
    cleaned = " ".join(text_value.split())
    if len(cleaned) <= limit:
        return cleaned
    return cleaned[: limit - 1].rstrip() + "…"


def _format_fact_line_active(fact: FactRow) -> str:
    """Detailed render for facts inside the active window.

    Same shape as the original Phase 10 line — value verbatim,
    confidence inline, full source link + optional web badge.
    """
    source = (
        f"[source: {fact.source_event_id}](/events/{fact.source_event_id}/source)"
        if fact.source_event_id is not None
        else "[source: unknown]"
    )
    badge = (
        " 🌐 _Updated from web sources_"
        if (fact.source or "").lower() == "web"
        else ""
    )
    return (
        f"- **{_format_field(fact.field)}:** {fact.value} "
        f"_(confidence {fact.confidence:.2f})_ {source}{badge}"
    )


def _format_fact_line_compact(fact: FactRow) -> str:
    """Tight one-liner used for the **recent** tier (90 d–1 y).

    Reads as ``YYYY-MM — Field name: short preview… [open detail](link)``.
    The ``[open detail]`` link points to the source-resolving endpoint
    so an operator can read the original event without leaving the file.
    """
    if fact.occurred_at is not None:
        occurred = fact.occurred_at
        if occurred.tzinfo is None:
            occurred = occurred.replace(tzinfo=timezone.utc)
        date_str = occurred.strftime("%Y-%m")
    else:
        date_str = "unknown"

    detail_link = (
        f"[open detail](/events/{fact.source_event_id}/source)"
        if fact.source_event_id is not None
        else "_(no source on file)_"
    )
    preview = _truncate(fact.value)
    return (
        f"- *{date_str}* — **{_format_field(fact.field)}:** {preview} "
        f"{detail_link}"
    )


def _format_archive_block(facts: list[FactRow], *, lang: str) -> list[str]:
    """Per-year roll-up for the **archive** tier (> 1 y).

    Twelve invoices from 2024 collapse to a single line — operators get
    the count + a click-through to the most recent of those archive
    events, instead of twelve bullets pushing the file size past 60 KB.
    """
    if not facts:
        return []
    by_year: dict[str, list[FactRow]] = {}
    for fact in facts:
        if fact.occurred_at is None:
            continue
        occurred = fact.occurred_at
        if occurred.tzinfo is None:
            occurred = occurred.replace(tzinfo=timezone.utc)
        by_year.setdefault(str(occurred.year), []).append(fact)
    if not by_year:
        return []

    out: list[str] = []
    for year in sorted(by_year.keys(), reverse=True):
        rows = by_year[year]
        # Newest in the year, by occurred_at, drives the link target.
        latest = max(
            rows,
            key=lambda f: f.occurred_at  # type: ignore[arg-type,return-value]
            or datetime.min.replace(tzinfo=timezone.utc),
        )
        link = (
            f"[open detail](/events/{latest.source_event_id}/source)"
            if latest.source_event_id is not None
            else "_(no source on file)_"
        )
        last_label = "letzter" if lang == "de" else "last"
        entries_label = "Einträge" if lang == "de" else "entries"
        latest_summary = _truncate(latest.value, limit=60)
        out.append(
            f"- **{year}** — {len(rows)} {entries_label} "
            f"({last_label}: {latest_summary}) {link}"
        )
    return out


def _format_fact_line(fact: FactRow, *, now: datetime | None = None) -> str:
    """Dispatcher between active (detailed) and compact (one-liner) renders."""
    if _is_compact(fact, now=now):
        return _format_fact_line_compact(fact)
    return _format_fact_line_active(fact)


async def _building_for_property(
    session: AsyncSession, property_id: UUID
) -> tuple[UUID | None, str | None]:
    """Return ``(building_id, building_address)`` for a property, both ``None`` if absent."""
    row = (
        await session.execute(
            text(
                """
                SELECT b.id, b.address
                FROM properties p
                LEFT JOIN buildings b ON b.id = p.building_id
                WHERE p.id = :pid
                """
            ),
            {"pid": property_id},
        )
    ).first()
    if row is None or row.id is None:
        return None, None
    return UUID(str(row.id)), row.address


async def _liegenschaft_for_building(
    session: AsyncSession, building_id: UUID
) -> tuple[UUID | None, str | None]:
    """Return ``(liegenschaft_id, name)`` for a building."""
    row = (
        await session.execute(
            text(
                """
                SELECT l.id, l.name
                FROM buildings b
                LEFT JOIN liegenschaften l ON l.id = b.liegenschaft_id
                WHERE b.id = :bid
                """
            ),
            {"bid": building_id},
        )
    ).first()
    if row is None or row.id is None:
        return None, None
    return UUID(str(row.id)), row.name


async def _recent_events_for_scope(
    session: AsyncSession,
    *,
    scope: str,  # 'building' | 'liegenschaft'
    scope_id: UUID,
    limit: int = CONTEXT_LIMIT,
) -> list[dict[str, str | int]]:
    """Pull the ``limit`` most recent events attached to a non-property scope."""
    column = "building_id" if scope == "building" else "liegenschaft_id"
    rows = (
        await session.execute(
            text(
                f"""
                SELECT id, source, source_ref, received_at,
                       LEFT(raw_content, 120) AS snippet,
                       metadata
                FROM events
                WHERE {column} = :sid
                ORDER BY received_at DESC
                LIMIT :lim
                """
            ),
            {"sid": scope_id, "lim": limit},
        )
    ).all()
    out: list[dict[str, str | int]] = []
    for r in rows:
        meta = dict(r.metadata or {})
        kategorie = meta.get("kategorie") or meta.get("document_type") or r.source
        out.append(
            {
                "id": str(r.id),
                "source": r.source,
                "received_at": r.received_at.isoformat() if r.received_at else "",
                "kategorie": str(kategorie),
                "snippet": (r.snippet or "").replace("\n", " ").strip(),
                "filename": str(meta.get("filename") or ""),
                "head_chars": int(meta.get("head_chars") or 0),
            }
        )
    return out


def _context_body(event: dict[str, str | int]) -> str:
    """Render the human-facing body of a context event.

    For PDF sources whose text hasn't been extracted yet we use the
    forward-looking phrasing ``"Invoice <filename> — awaiting
    extraction"`` (Phase 9 trust-layer ethos: honest about epistemic
    state). Once text is extracted (``head_chars > 0``) the snippet
    drives the display normally.
    """
    source = str(event.get("source") or "")
    filename = str(event.get("filename") or "")
    head_chars = int(event.get("head_chars") or 0)

    if source in {"invoice", "letter"} and head_chars == 0 and filename:
        label = "Invoice" if source == "invoice" else "Letter"
        return f"{label} {filename} — awaiting extraction"

    snippet = str(event.get("snippet") or "")
    return snippet[:90] or "(no body)"


def _format_context_event(event: dict[str, str | int]) -> str:
    """Render one per-tier context event as a markdown bullet with source link."""
    when = str(event.get("received_at") or "")[:10] if event.get("received_at") else "?"
    return (
        f"- *{when}* · `{event['source']}`/{event['kategorie']} — "
        f"{_context_body(event)} "
        f"[source: {event['id']}](/events/{event['id']}/source)"
    )


async def _fetch_facts_by_scope(
    session: AsyncSession,
    *,
    scope: str,  # 'property' | 'building' | 'liegenschaft'
    scope_id: UUID,
) -> list[FactRow]:
    """Generic fact loader covering the three tiers."""
    column = {
        "property": "property_id",
        "building": "building_id",
        "liegenschaft": "liegenschaft_id",
    }[scope]
    other_columns = ["property_id", "building_id", "liegenschaft_id"]
    other_clauses = " AND ".join(
        f"f.{col} IS NULL" for col in other_columns if col != column
    )
    sql = f"""
        SELECT f.section, f.field, f.value, f.source_event_id, f.confidence,
               f.created_at, e.source AS source,
               e.received_at AS occurred_at
        FROM facts f
        LEFT JOIN events e ON e.id = f.source_event_id
        WHERE f.{column} = :sid
          AND {other_clauses}
          AND f.superseded_by IS NULL
        ORDER BY f.section, f.created_at ASC, f.field ASC
        """
    result = await session.execute(text(sql), {"sid": scope_id})
    return [
        FactRow(
            section=row.section,
            field=row.field,
            value=row.value,
            source_event_id=row.source_event_id,
            confidence=float(row.confidence),
            source=row.source,
            occurred_at=row.occurred_at,
        )
        for row in result.all()
    ]


@dataclass(frozen=True)
class UncertaintyRow:
    """One open uncertainty event, ready for the renderer."""

    id: UUID
    event_id: UUID
    section: str
    field: str | None
    observation: str
    reason_uncertain: str
    source: str


async def _fetch_open_uncertainties(
    session: AsyncSession, property_id: UUID
) -> list[UncertaintyRow]:
    """Pull every ``status='open'`` uncertainty for a property, grouped per section."""
    rows = (
        await session.execute(
            text(
                """
                SELECT id, event_id, relevant_section, relevant_field,
                       observation, reason_uncertain, source
                FROM uncertainty_events
                WHERE property_id = :pid
                  AND status = 'open'
                ORDER BY relevant_section NULLS LAST, created_at DESC
                """
            ),
            {"pid": property_id},
        )
    ).all()
    return [
        UncertaintyRow(
            id=row.id,
            event_id=row.event_id,
            section=str(row.relevant_section or "(unsectioned)"),
            field=str(row.relevant_field) if row.relevant_field else None,
            observation=str(row.observation or ""),
            reason_uncertain=str(row.reason_uncertain or ""),
            source=str(row.source or "extractor"),
        )
        for row in rows
    ]


def _format_uncertainty_line(item: UncertaintyRow) -> str:
    """One-line rendering of an open uncertainty inside a section block."""
    snippet = item.observation
    if len(snippet) > 160:
        snippet = snippet[:157].rstrip() + "…"
    return (
        f"- _Unclear: {snippet} — {item.reason_uncertain}_ "
        f"[source: event {item.event_id}](/events/{item.event_id}/source)"
    )


@dataclass(frozen=True)
class RejectionRow:
    """One ``reviewed_status='pending'`` rejection at a property scope."""

    id: UUID
    event_id: UUID
    section: str
    field: str
    proposed_value: str
    constraint_name: str
    reason: str
    occurred_at: datetime | None


async def _fetch_pending_rejections(
    session: AsyncSession, property_id: UUID
) -> list[RejectionRow]:
    """Pull every ``reviewed_status='pending'`` rejection for a property.

    The renderer surfaces these inline, beneath the active fact at the
    same ``(section, field)`` coordinate, as ⚠ conflict markers — so an
    operator reading the file sees both the accepted fact and the
    proposal the validator rejected, with a one-click review link.
    """
    rows = (
        await session.execute(
            text(
                """
                SELECT r.id, r.event_id, r.proposed_section, r.proposed_field,
                       r.proposed_value, r.constraint_name, r.reason,
                       e.received_at AS occurred_at
                FROM rejected_updates r
                LEFT JOIN events e ON e.id = r.event_id
                WHERE r.property_id = :pid
                  AND r.reviewed_status = 'pending'
                ORDER BY r.created_at DESC
                """
            ),
            {"pid": property_id},
        )
    ).all()
    return [
        RejectionRow(
            id=row.id,
            event_id=row.event_id,
            section=str(row.proposed_section or ""),
            field=str(row.proposed_field or ""),
            proposed_value=str(row.proposed_value or ""),
            constraint_name=str(row.constraint_name or ""),
            reason=str(row.reason or ""),
            occurred_at=row.occurred_at,
        )
        for row in rows
    ]


def _format_conflict_line(rej: RejectionRow, *, lang: str) -> str:
    """Indented ⚠ line emitted under a fact whose proposal was rejected.

    The link target hands the operator straight to the rejection inbox
    where they can override or dismiss with reason.
    """
    when = ""
    if rej.occurred_at is not None:
        occurred = rej.occurred_at
        if occurred.tzinfo is None:
            occurred = occurred.replace(tzinfo=timezone.utc)
        when = f" ({occurred.strftime('%Y-%m-%d')})"
    label = "Konflikt" if lang == "de" else "Conflict"
    review = "Überprüfen" if lang == "de" else "Review"
    preview = _truncate(rej.proposed_value, limit=80)
    return (
        f"  - ⚠ **{label}:** {preview}{when} — flagged by "
        f"`{rej.constraint_name}`. "
        f"[{review} →](/admin/rejected/{rej.id})"
    )


def _format_open_conflicts_block(
    unmatched: list[RejectionRow], *, lang: str
) -> list[str]:
    """Standalone block listing rejections that don't pair to a current fact.

    These are typically validator-rejected proposals at scopes that
    don't surface as a property fact (e.g. ``building_overview.floor_count``
    flagged by ``building_floor_count_immutable``). Surfacing them at the
    bottom of the file keeps the trust-layer narrative visible to the
    operator: "the validator caught these; here's the one-click review."
    """
    if not unmatched:
        return []
    title = (
        "Offene Konflikte zur Überprüfung"
        if lang == "de"
        else "Open conflicts pending review"
    )
    review = "Überprüfen" if lang == "de" else "Review"
    out: list[str] = [f"## {title}", ""]
    for rej in unmatched:
        when = ""
        if rej.occurred_at is not None:
            occurred = rej.occurred_at
            if occurred.tzinfo is None:
                occurred = occurred.replace(tzinfo=timezone.utc)
            when = f" ({occurred.strftime('%Y-%m-%d')})"
        preview = _truncate(rej.proposed_value, limit=100)
        section_label = _format_field(rej.section)
        field_label = _format_field(rej.field) if rej.field else ""
        coord = (
            f"{section_label} · {field_label}" if field_label else section_label
        )
        out.append(
            f"- ⚠ **{coord}:** {preview}{when} — flagged by "
            f"`{rej.constraint_name}`. "
            f"[{review} →](/admin/rejected/{rej.id})"
        )
    out.append("")
    return out


def _emit_sections(
    facts: list[FactRow],
    lines: list[str],
    *,
    lang: str = "en",
    uncertainties: list[UncertaintyRow] | None = None,
    rejections: list[RejectionRow] | None = None,
) -> dict[str, dict[str, int]]:
    """Append one ``## Section`` block per non-empty section to ``lines``.

    Three-tier render (Phase 12+):
      * **Active** (≤ 90 d): full bullet — value verbatim, confidence inline,
        full source link, optional 🌐 web badge.
      * **Recent** (90 d–1 y): tight one-liner — ``YYYY-MM · field: preview… [open detail]``.
      * **Archive** (> 1 y): collapses to a per-year roll-up
        (``2024 — 12 entries; last: …``) under an ``### Archive`` heading.

    When a pending ``rejected_updates`` row exists at the same
    ``(section, field)`` coordinate as an active fact, an indented
    ⚠ Conflict line is emitted beneath the fact with a one-click review
    link to the rejection inbox.

    Returns a per-section counts dict so the caller can populate the
    frontmatter / content_index without re-iterating the facts list.
    """
    titles = SECTION_TITLES_DE if lang == "de" else SECTION_TITLES
    labels = CONTEXT_LABELS[lang]
    needs_review_label = labels["needs_review"]
    archive_label = "Archiv" if lang == "de" else "Archive"
    archive_subtitle = (
        "Ältere Einträge gruppiert nach Jahr — anklicken öffnet das Original"
        if lang == "de"
        else "Older entries grouped by year — click any line to open the original"
    )
    now = datetime.now(timezone.utc)

    def _title(section: str) -> str:
        label = titles.get(section)
        if label is not None:
            return label
        return SECTION_TITLES.get(section, section.replace("_", " ").title())

    by_section: dict[str, list[FactRow]] = {section: [] for section in SECTION_ORDER}
    for fact in facts:
        by_section.setdefault(fact.section, []).append(fact)

    uncertainty_by_section: dict[str, list[UncertaintyRow]] = {}
    for item in uncertainties or []:
        uncertainty_by_section.setdefault(item.section, []).append(item)

    rejection_by_key: dict[tuple[str, str], list[RejectionRow]] = {}
    for rej in rejections or []:
        rejection_by_key.setdefault((rej.section, rej.field), []).append(rej)

    counts_by_section: dict[str, dict[str, int]] = {}

    def _render(section: str) -> None:
        rows = by_section.get(section, [])
        unc = uncertainty_by_section.get(section, [])
        if not rows and not unc:
            return

        active_rows: list[FactRow] = []
        recent_rows: list[FactRow] = []
        archive_rows: list[FactRow] = []
        for fact in rows:
            tier = _classify_tier(fact, now=now)
            if tier == "active":
                active_rows.append(fact)
            elif tier == "recent":
                recent_rows.append(fact)
            else:
                archive_rows.append(fact)

        # Cap chronological-log sections (e.g. activity) so a property
        # with 1000 events processed today doesn't blow up the file.
        # Active rows are sorted newest-first; rows beyond the limit
        # fold straight into the archive tier where they collapse to
        # one per-year roll-up bullet (``2026 — 135 entries``) instead
        # of 135 separate lines.
        if section in _CAPPED_SECTIONS and len(active_rows) > ACTIVITY_ACTIVE_LIMIT:
            active_rows.sort(
                key=lambda f: (
                    f.occurred_at or datetime.min.replace(tzinfo=timezone.utc)
                ),
                reverse=True,
            )
            overflow = active_rows[ACTIVITY_ACTIVE_LIMIT:]
            active_rows = active_rows[:ACTIVITY_ACTIVE_LIMIT]
            archive_rows = overflow + archive_rows

        recent_rows.sort(
            key=lambda f: (
                f.occurred_at or datetime.min.replace(tzinfo=timezone.utc)
            ),
            reverse=True,
        )

        counts_by_section[section] = {
            "fact_count": len(rows),
            "active_count": len(active_rows),
            "recent_count": len(recent_rows),
            "archive_count": len(archive_rows),
            "uncertainty_count": len(unc),
        }

        lines.append(f"## {_title(section)}")
        lines.append("")

        for fact in active_rows:
            lines.append(_format_fact_line_active(fact))
            for rej in rejection_by_key.get((fact.section, fact.field), []):
                lines.append(_format_conflict_line(rej, lang=lang))

        if recent_rows:
            if active_rows:
                lines.append("")
            lines.extend(_format_fact_line_compact(fact) for fact in recent_rows)

        if archive_rows:
            if active_rows or recent_rows:
                lines.append("")
            lines.append(f"### {archive_label}")
            lines.append("")
            lines.append(f"_{archive_subtitle}_")
            lines.append("")
            lines.extend(_format_archive_block(archive_rows, lang=lang))

        if unc:
            if active_rows or recent_rows or archive_rows:
                lines.append("")
            lines.append(f"### {needs_review_label}")
            lines.append("")
            lines.extend(_format_uncertainty_line(item) for item in unc)
        lines.append("")

    for section in SECTION_ORDER:
        _render(section)
    extras = sorted(
        s
        for s in set(by_section.keys()) | set(uncertainty_by_section.keys())
        if s not in SECTION_ORDER
    )
    for section in extras:
        _render(section)
    return counts_by_section


def _format_frontmatter(
    *,
    property_id: UUID,
    name: str,
    address: str,
    lang: str,
    rendered_at: datetime,
    facts: list[FactRow],
    uncertainties: list[UncertaintyRow],
    rejections: list[RejectionRow],
    counts_by_section: dict[str, dict[str, int]],
    coverage_present: int,
    coverage_expected: int,
    building_address: str | None,
    liegenschaft_name: str | None,
) -> list[str]:
    """Emit the YAML frontmatter block at the top of every property file.

    Stays *deterministic* — same fact set + same coverage answers, same
    frontmatter. Trigger metadata (``trigger_event_id`` / ``trigger_summary``)
    is intentionally **not** in the frontmatter — it lives on the
    ``property_files`` row's columns instead. Mixing them in here would
    make the markdown body churn on every fact write even when content
    didn't change.
    """
    confidences = [f.confidence for f in facts]
    avg_conf = sum(confidences) / len(confidences) if confidences else 0.0
    low_conf = sum(1 for c in confidences if c < 0.7)
    bucket_totals = {
        "active": sum(c.get("active_count", 0) for c in counts_by_section.values()),
        "recent": sum(c.get("recent_count", 0) for c in counts_by_section.values()),
        "archive": sum(c.get("archive_count", 0) for c in counts_by_section.values()),
    }
    out: list[str] = [
        "---",
        f"property_id: {property_id}",
        f"name: {name}",
        f"address: {address}",
        f"tier: property",
        f"language: {lang}",
        f"as_of: {rendered_at.strftime('%Y-%m-%dT%H:%M:%SZ')}",
        f"fact_count: {len(facts)}",
        f"confidence_avg: {avg_conf:.2f}",
        f"low_confidence_count: {low_conf}",
        f"open_uncertainties: {len(uncertainties)}",
        f"open_rejections: {len(rejections)}",
        f"coverage: {coverage_present}/{coverage_expected}",
        "buckets:",
        f"  active: {bucket_totals['active']}",
        f"  recent: {bucket_totals['recent']}",
        f"  archive: {bucket_totals['archive']}",
    ]
    if building_address or liegenschaft_name:
        out.append("parent:")
        if building_address:
            out.append(f"  building: {building_address}")
        if liegenschaft_name:
            out.append(f"  liegenschaft: {liegenschaft_name}")
    out.append("---")
    out.append("")
    return out


@dataclass(frozen=True)
class RenderResult:
    """Returned by :func:`render_property_full` — markdown + structured index."""

    markdown: str
    content_index: dict[str, object]


async def render_property_full(
    session: AsyncSession, property_id: UUID
) -> RenderResult:
    """Render the living markdown + structured ``content_index`` together.

    Three new responsibilities (Phase 12+):
      1. **YAML frontmatter** at the top — stable, agent-readable.
      2. **Three-tier bucketing** in section emit (active / recent / archive).
      3. **⚠ Conflict markers** beneath active facts when a pending
         rejected proposal exists at the same coordinate.
      4. **## Coverage** scorecard — what's known, what's missing.
    """
    from backend.pipeline.coverage import (  # noqa: PLC0415 — local import
        compute_coverage,
        coverage_to_index,
        render_coverage_block,
    )

    header = await _fetch_header(session, property_id)
    if header is None:
        raise ValueError(f"Property {property_id} not found")

    facts = await _fetch_current_facts(session, property_id)
    uncertainties = await _fetch_open_uncertainties(session, property_id)
    rejections = await _fetch_pending_rejections(session, property_id)
    stammdaten = await _fetch_stammdaten(session, property_id)
    lang = await _detect_property_language(session, property_id)
    labels = CONTEXT_LABELS[lang]

    building_id, building_address = await _building_for_property(session, property_id)
    liegenschaft_id: UUID | None = None
    liegenschaft_name: str | None = None
    if building_id is not None:
        liegenschaft_id, liegenschaft_name = await _liegenschaft_for_building(
            session, building_id
        )

    rendered_at = datetime.now(timezone.utc)
    coverage = compute_coverage(stammdaten, facts, lang=lang)

    log.debug(
        "renderer.fetch",
        property_id=str(property_id),
        fact_count=len(facts),
        uncertainty_count=len(uncertainties),
        rejection_count=len(rejections),
        has_stammdaten=stammdaten is not None,
        lang=lang,
    )

    body_lines: list[str] = [f"# {header.name}", "", f"_{header.address}_", ""]
    if stammdaten is not None:
        body_lines.extend(_format_stammdaten_block(stammdaten, lang))
    counts_by_section = _emit_sections(
        facts,
        body_lines,
        lang=lang,
        uncertainties=uncertainties,
        rejections=rejections,
    )
    # Emit any rejections that didn't pair with a current fact at the
    # same (section, field) coordinate as a standalone block. Without
    # this, building/liegenschaft-scope rejections attached to the
    # property would never surface in the property's markdown.
    fact_keys = {(f.section, f.field) for f in facts}
    unmatched_rejections = [
        r for r in rejections if (r.section, r.field) not in fact_keys
    ]
    body_lines.extend(_format_open_conflicts_block(unmatched_rejections, lang=lang))
    body_lines.extend(render_coverage_block(coverage, lang=lang))

    # Building Context
    if building_id is not None:
        events = await _recent_events_for_scope(
            session, scope="building", scope_id=building_id
        )
        if events:
            body_lines.append(f"## {labels['building_context']}")
            body_lines.append("")
            body_lines.append(
                f"_{labels['building_subtitle']} — "
                f"{building_address or building_id}_"
            )
            body_lines.append("")
            body_lines.extend(_format_context_event(ev) for ev in events)
            body_lines.append(
                f"\n[{labels['open_building']}](/buildings/{building_id}/markdown)\n"
            )

    # WEG Context
    if liegenschaft_id is not None:
        events = await _recent_events_for_scope(
            session, scope="liegenschaft", scope_id=liegenschaft_id
        )
        if events:
            body_lines.append(f"## {labels['weg_context']}")
            body_lines.append("")
            body_lines.append(
                f"_{labels['weg_subtitle']} — "
                f"{liegenschaft_name or liegenschaft_id}_"
            )
            body_lines.append("")
            body_lines.extend(_format_context_event(ev) for ev in events)
            body_lines.append(
                f"\n[{labels['open_weg']}](/liegenschaften/"
                f"{liegenschaft_id}/markdown)\n"
            )

    frontmatter = _format_frontmatter(
        property_id=property_id,
        name=header.name,
        address=header.address,
        lang=lang,
        rendered_at=rendered_at,
        facts=facts,
        uncertainties=uncertainties,
        rejections=rejections,
        counts_by_section=counts_by_section,
        coverage_present=coverage.total_present,
        coverage_expected=coverage.total_expected,
        building_address=building_address,
        liegenschaft_name=liegenschaft_name,
    )

    markdown = "\n".join(frontmatter + body_lines).rstrip() + "\n"

    confidences = [f.confidence for f in facts]
    avg_conf = sum(confidences) / len(confidences) if confidences else 0.0
    content_index: dict[str, object] = {
        "schema_version": 1,
        "tier": "property",
        "language": lang,
        "as_of": rendered_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "fact_count": len(facts),
        "confidence_avg": round(avg_conf, 4),
        "low_confidence_count": sum(1 for c in confidences if c < 0.7),
        "open_uncertainties": len(uncertainties),
        "open_rejections": len(rejections),
        "buckets": {
            "active": sum(c.get("active_count", 0) for c in counts_by_section.values()),
            "recent": sum(c.get("recent_count", 0) for c in counts_by_section.values()),
            "archive": sum(c.get("archive_count", 0) for c in counts_by_section.values()),
        },
        "sections": counts_by_section,
        "coverage": coverage_to_index(coverage),
        "conflicts": [
            {
                "rejection_id": str(r.id),
                "section": r.section,
                "field": r.field,
                "constraint": r.constraint_name,
                "review_url": f"/admin/rejected/{r.id}",
            }
            for r in rejections
        ],
        "parent": {
            "building": building_address,
            "liegenschaft": liegenschaft_name,
        },
    }

    return RenderResult(markdown=markdown, content_index=content_index)


async def render_markdown(session: AsyncSession, property_id: UUID) -> str:
    """Backward-compat wrapper — returns just the markdown body.

    Existing callers (``GET /properties/{id}/markdown`` fallback,
    ``test_pipeline_happy_path``) keep their string contract; the
    Phase 12 materializer should use :func:`render_property_full`
    so it can populate the ``content_index`` JSONB column too.

    Raises:
        ValueError: if the property does not exist.
    """
    return (await render_property_full(session, property_id)).markdown


async def render_building_markdown(
    session: AsyncSession, building_id: UUID
) -> str:
    """Render the living markdown for a building (Haus)."""
    row = (
        await session.execute(
            text(
                """
                SELECT b.id, b.address, l.id AS liegenschaft_id, l.name AS lname
                FROM buildings b
                LEFT JOIN liegenschaften l ON l.id = b.liegenschaft_id
                WHERE b.id = :bid
                """
            ),
            {"bid": building_id},
        )
    ).first()
    if row is None:
        raise ValueError(f"Building {building_id} not found")

    facts = await _fetch_facts_by_scope(
        session, scope="building", scope_id=building_id
    )
    lines: list[str] = [
        f"# Building {row.address}",
        "",
        f"_Building UUID {row.id}_",
        "",
    ]
    _emit_sections(facts, lines)

    if row.liegenschaft_id is not None:
        events = await _recent_events_for_scope(
            session, scope="liegenschaft", scope_id=UUID(str(row.liegenschaft_id))
        )
        if events:
            lines.append("## WEG Context")
            lines.append("")
            lines.append(
                f"_Recent activity at the WEG — "
                f"{row.lname or row.liegenschaft_id}_"
            )
            lines.append("")
            lines.extend(_format_context_event(ev) for ev in events)
            lines.append(
                f"\n[Open WEG view](/liegenschaften/"
                f"{row.liegenschaft_id}/markdown)\n"
            )
    return "\n".join(lines).rstrip() + "\n"


async def render_liegenschaft_markdown(
    session: AsyncSession, liegenschaft_id: UUID
) -> str:
    """Render the living markdown for a Liegenschaft (WEG)."""
    row = (
        await session.execute(
            text("SELECT name FROM liegenschaften WHERE id = :lid"),
            {"lid": liegenschaft_id},
        )
    ).first()
    if row is None:
        raise ValueError(f"Liegenschaft {liegenschaft_id} not found")

    facts = await _fetch_facts_by_scope(
        session, scope="liegenschaft", scope_id=liegenschaft_id
    )
    lines: list[str] = [
        f"# WEG — {row.name}",
        "",
        f"_Liegenschaft UUID {liegenschaft_id}_",
        "",
    ]
    _emit_sections(facts, lines)
    return "\n".join(lines).rstrip() + "\n"
