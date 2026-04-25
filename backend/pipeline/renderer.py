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


def _format_fact_line(fact: FactRow) -> str:
    """Render a single fact as a bullet with inline source + optional web badge."""
    source = (
        f"[source: {fact.source_event_id}](#event-{fact.source_event_id})"
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
) -> list[dict[str, str]]:
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
    out: list[dict[str, str]] = []
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
            }
        )
    return out


def _format_context_event(event: dict[str, str]) -> str:
    """Render one per-tier context event as a markdown bullet with source link."""
    when = event["received_at"][:10] if event["received_at"] else "?"
    return (
        f"- *{when}* · `{event['source']}`/{event['kategorie']} — "
        f"{event['snippet'][:90] or '(no body)'} "
        f"[source: {event['id']}](#event-{event['id']})"
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


def _emit_sections(
    facts: list[FactRow],
    lines: list[str],
) -> None:
    """Append one ``## Section`` block per non-empty section to ``lines``."""
    by_section: dict[str, list[FactRow]] = {section: [] for section in SECTION_ORDER}
    for fact in facts:
        by_section.setdefault(fact.section, []).append(fact)
    for section in SECTION_ORDER:
        rows = by_section.get(section, [])
        if not rows:
            continue
        lines.append(f"## {SECTION_TITLES.get(section, section.title())}")
        lines.append("")
        lines.extend(_format_fact_line(fact) for fact in rows)
        lines.append("")
    extras = [
        section
        for section in by_section
        if section not in SECTION_ORDER and by_section[section]
    ]
    for section in sorted(extras):
        lines.append(
            f"## {SECTION_TITLES.get(section, section.replace('_', ' ').title())}"
        )
        lines.append("")
        lines.extend(_format_fact_line(fact) for fact in by_section[section])
        lines.append("")


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
    log.debug(
        "renderer.fetch",
        property_id=str(property_id),
        fact_count=len(facts),
    )

    lines: list[str] = [f"# {header.name}", "", f"_{header.address}_", ""]
    _emit_sections(facts, lines)

    # Building Context
    building_id, building_address = await _building_for_property(session, property_id)
    if building_id is not None:
        events = await _recent_events_for_scope(
            session, scope="building", scope_id=building_id
        )
        if events:
            lines.append("## Building Context")
            lines.append("")
            lines.append(
                f"_Recent activity at the parent building — "
                f"{building_address or building_id}_"
            )
            lines.append("")
            lines.extend(_format_context_event(ev) for ev in events)
            lines.append(
                f"\n[Open building view](/buildings/{building_id}/markdown)\n"
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
                lines.append("## WEG Context")
                lines.append("")
                lines.append(
                    f"_Recent activity at the WEG (Liegenschaft) — "
                    f"{liegenschaft_name or liegenschaft_id}_"
                )
                lines.append("")
                lines.extend(_format_context_event(ev) for ev in events)
                lines.append(
                    f"\n[Open WEG view](/liegenschaften/"
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
