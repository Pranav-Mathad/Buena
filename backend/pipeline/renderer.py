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
    },
}

#: How many recent events to surface in the per-tier context blocks.
CONTEXT_LIMIT: int = 5


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
    """Return all current (non-superseded) facts for the property."""
    result = await session.execute(
        text(
            """
            SELECT f.section, f.field, f.value, f.source_event_id, f.confidence,
                   f.created_at, e.source AS source
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


def _format_fact_line(fact: FactRow) -> str:
    """Render a single fact as a bullet with inline source + optional web badge.

    Phase 10 Step 10.4: source links resolve to ``/events/<id>/source``,
    which redirects to the right surface for the event's source type
    (PDF → ``/files/<path>``, email → ``/raw``, bank → ``/detail``).
    The renderer never has to know which is which — the dispatch lives
    in :mod:`backend.api.source_links`.
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
               f.created_at, e.source AS source
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


def _emit_sections(
    facts: list[FactRow],
    lines: list[str],
    *,
    lang: str = "en",
    uncertainties: list[UncertaintyRow] | None = None,
) -> None:
    """Append one ``## Section`` block per non-empty section to ``lines``.

    German titles are picked when ``lang='de'`` and a German label is
    defined for the section in :data:`SECTION_TITLES_DE`; otherwise the
    English label (or the section name) is used.

    Phase 9 Step 9.1: when ``uncertainties`` is provided, each section
    that has ``status='open'`` uncertainty rows ends with a
    **Needs Review** subsection listing them. Sections that have only
    uncertainties (no facts yet) still get rendered so the operator
    can see what was noticed.
    """
    titles = SECTION_TITLES_DE if lang == "de" else SECTION_TITLES
    needs_review_label = CONTEXT_LABELS[lang]["needs_review"]

    def _title(section: str) -> str:
        label = titles.get(section)
        if label is not None:
            return label
        # Fall through to English for sections without German labels.
        return SECTION_TITLES.get(section, section.replace("_", " ").title())

    by_section: dict[str, list[FactRow]] = {section: [] for section in SECTION_ORDER}
    for fact in facts:
        by_section.setdefault(fact.section, []).append(fact)

    uncertainty_by_section: dict[str, list[UncertaintyRow]] = {}
    for item in uncertainties or []:
        uncertainty_by_section.setdefault(item.section, []).append(item)

    rendered: set[str] = set()

    def _render(section: str) -> None:
        rows = by_section.get(section, [])
        unc = uncertainty_by_section.get(section, [])
        if not rows and not unc:
            return
        lines.append(f"## {_title(section)}")
        lines.append("")
        lines.extend(_format_fact_line(fact) for fact in rows)
        if unc:
            if rows:
                lines.append("")
            lines.append(f"### {needs_review_label}")
            lines.append("")
            lines.extend(_format_uncertainty_line(item) for item in unc)
        lines.append("")
        rendered.add(section)

    for section in SECTION_ORDER:
        _render(section)
    extras = sorted(
        s
        for s in set(by_section.keys()) | set(uncertainty_by_section.keys())
        if s not in SECTION_ORDER
    )
    for section in extras:
        _render(section)


async def render_markdown(session: AsyncSession, property_id: UUID) -> str:
    """Render the living markdown document for a property.

    Phase 8.1: ends with **Building Context** and **WEG Context**
    subsections — the most recent ``CONTEXT_LIMIT`` events for the
    property's parent building and Liegenschaft. Both are read-only
    pointers; full views live at ``/buildings/{id}/markdown`` and
    ``/liegenschaften/{id}/markdown``.

    Raises:
        ValueError: if the property does not exist.
    """
    header = await _fetch_header(session, property_id)
    if header is None:
        raise ValueError(f"Property {property_id} not found")

    facts = await _fetch_current_facts(session, property_id)
    uncertainties = await _fetch_open_uncertainties(session, property_id)
    stammdaten = await _fetch_stammdaten(session, property_id)
    lang = await _detect_property_language(session, property_id)
    labels = CONTEXT_LABELS[lang]
    log.debug(
        "renderer.fetch",
        property_id=str(property_id),
        fact_count=len(facts),
        uncertainty_count=len(uncertainties),
        has_stammdaten=stammdaten is not None,
        lang=lang,
    )

    lines: list[str] = [f"# {header.name}", "", f"_{header.address}_", ""]
    if stammdaten is not None:
        lines.extend(_format_stammdaten_block(stammdaten, lang))
    _emit_sections(facts, lines, lang=lang, uncertainties=uncertainties)

    # Building Context
    building_id, building_address = await _building_for_property(session, property_id)
    if building_id is not None:
        events = await _recent_events_for_scope(
            session, scope="building", scope_id=building_id
        )
        if events:
            lines.append(f"## {labels['building_context']}")
            lines.append("")
            lines.append(
                f"_{labels['building_subtitle']} — "
                f"{building_address or building_id}_"
            )
            lines.append("")
            lines.extend(_format_context_event(ev) for ev in events)
            lines.append(
                f"\n[{labels['open_building']}](/buildings/{building_id}/markdown)\n"
            )

    # WEG Context
    if building_id is not None:
        liegenschaft_id, liegenschaft_name = await _liegenschaft_for_building(
            session, building_id
        )
        if liegenschaft_id is not None:
            events = await _recent_events_for_scope(
                session, scope="liegenschaft", scope_id=liegenschaft_id
            )
            if events:
                lines.append(f"## {labels['weg_context']}")
                lines.append("")
                lines.append(
                    f"_{labels['weg_subtitle']} — "
                    f"{liegenschaft_name or liegenschaft_id}_"
                )
                lines.append("")
                lines.extend(_format_context_event(ev) for ev in events)
                lines.append(
                    f"\n[{labels['open_weg']}](/liegenschaften/"
                    f"{liegenschaft_id}/markdown)\n"
                )

    return "\n".join(lines).rstrip() + "\n"


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
