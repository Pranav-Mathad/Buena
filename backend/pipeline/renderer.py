"""Render a property's current facts into markdown with inline source links.

The renderer is the canonical read path: it queries every non-superseded fact
for a property, groups them by section, and emits markdown whose claims are
each back-linked to the event that produced them via ``[source: <event_id>]``.

This mirrors the design of Part I — **every fact has a source** — and is what
both the UI and MCP surface consume.
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
}


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
            SELECT section, field, value, source_event_id, confidence, created_at
            FROM facts
            WHERE property_id = :pid
              AND superseded_by IS NULL
            ORDER BY section, created_at ASC, field ASC
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
        )
        for row in result.all()
    ]


def _format_field(field: str) -> str:
    """Turn ``snake_case`` field names into human-readable titles."""
    return field.replace("_", " ").strip().capitalize()


def _format_fact_line(fact: FactRow) -> str:
    """Render a single fact as a bullet with an inline source link."""
    source = (
        f"[source: {fact.source_event_id}](#event-{fact.source_event_id})"
        if fact.source_event_id is not None
        else "[source: unknown]"
    )
    return (
        f"- **{_format_field(fact.field)}:** {fact.value} "
        f"_(confidence {fact.confidence:.2f})_ {source}"
    )


async def render_markdown(session: AsyncSession, property_id: UUID) -> str:
    """Render the living markdown document for a property.

    Args:
        session: Active async SQLAlchemy session.
        property_id: UUID of the property to render.

    Returns:
        A markdown string whose sections match :data:`SECTION_ORDER`. Every
        fact line is annotated with an inline ``[source: <event_id>]`` link
        pointing at the event that produced it.

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

    by_section: dict[str, list[FactRow]] = {section: [] for section in SECTION_ORDER}
    for fact in facts:
        by_section.setdefault(fact.section, []).append(fact)

    lines: list[str] = [f"# {header.name}", "", f"_{header.address}_", ""]
    for section in SECTION_ORDER:
        rows = by_section.get(section, [])
        if not rows:
            continue
        lines.append(f"## {SECTION_TITLES.get(section, section.title())}")
        lines.append("")
        lines.extend(_format_fact_line(fact) for fact in rows)
        lines.append("")

    # Sections that weren't in the canonical order but exist in the data.
    extras = [
        section
        for section in by_section
        if section not in SECTION_ORDER and by_section[section]
    ]
    for section in sorted(extras):
        lines.append(f"## {section.title()}")
        lines.append("")
        lines.extend(_format_fact_line(fact) for fact in by_section[section])
        lines.append("")

    return "\n".join(lines).rstrip() + "\n"
