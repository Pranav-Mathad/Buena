"""Onboarding view — the "first-time read" surface for a property.

A property manager who's never seen this property before should be able
to read this document in 60 seconds and know:

1. Stammdaten + how much we already know (deterministic).
2. What's open right now — uncertainties, pending rejections, active
   maintenance facts (deterministic).
3. A 5-to-7-bullet briefing of the last 12 months, hard-instructed to
   call out gaps rather than invent (Gemini Pro).
4. What patterns the validator and inbox have noticed — recurring
   conflicts, watch-out items (deterministic).
5. Where to look for more — pointer index by section with the most
   recent source events linked.

Section 3 is the only LLM call. It is cached on
``properties.metadata.onboarding`` keyed by a hash of the
last-fact / last-uncertainty / last-rejection timestamps so a re-read
is free; mutating any of those three invalidates the cache on the
next render. We do *not* hook regen onto fact insertion — that would
push Gemini latency onto the write path. Read-time regen is the
right trade.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

import structlog
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from backend.pipeline.renderer import (
    UncertaintyRow,
    _fetch_current_facts,
    _fetch_open_uncertainties,
)
from backend.services import gemini, pioneer_llm

log = structlog.get_logger(__name__)


# How many bullets per "open issues" / "watch out" subsection. The
# briefing aims for 60-second readability — if the deterministic
# sections sprawl, the reader bounces. Ship-quality, not coverage.
TOP_N: int = 3
TOP_N_PATTERNS: int = 5
ACTIVITY_WINDOW_DAYS: int = 365
ONBOARDING_CACHE_VERSION: int = 2


@dataclass(frozen=True)
class _PropertyHeader:
    """Stammdaten + parent links needed for the title block."""

    property_id: UUID
    name: str
    address: str
    aliases: list[str]
    metadata: dict[str, Any]
    building_id: UUID | None
    building_address: str | None
    liegenschaft_id: UUID | None
    liegenschaft_name: str | None


@dataclass(frozen=True)
class _RejectionRow:
    """One ``rejected_updates`` row, slim."""

    id: UUID
    event_id: UUID
    section: str
    field: str
    value: str
    constraint_name: str
    reason: str
    reviewed_status: str
    created_at: datetime


# -----------------------------------------------------------------------------
# Data loaders
# -----------------------------------------------------------------------------


async def _load_header(
    session: AsyncSession, property_id: UUID
) -> _PropertyHeader | None:
    """Load property + building + Liegenschaft in one round-trip."""
    row = (
        await session.execute(
            text(
                """
                SELECT p.id, p.name, p.address, p.aliases, p.metadata,
                       p.building_id,
                       b.address AS building_address,
                       l.id AS liegenschaft_id,
                       l.name AS liegenschaft_name
                FROM properties p
                LEFT JOIN buildings b ON b.id = p.building_id
                LEFT JOIN liegenschaften l ON l.id = b.liegenschaft_id
                WHERE p.id = :pid
                """
            ),
            {"pid": property_id},
        )
    ).first()
    if row is None:
        return None
    return _PropertyHeader(
        property_id=UUID(str(row.id)),
        name=str(row.name),
        address=str(row.address or ""),
        aliases=list(row.aliases or []),
        metadata=dict(row.metadata or {}),
        building_id=UUID(str(row.building_id)) if row.building_id else None,
        building_address=row.building_address,
        liegenschaft_id=(
            UUID(str(row.liegenschaft_id)) if row.liegenschaft_id else None
        ),
        liegenschaft_name=row.liegenschaft_name,
    )


async def _load_pending_rejections(
    session: AsyncSession, property_id: UUID
) -> list[_RejectionRow]:
    """Pending or needs_review rejections — newest first."""
    rows = (
        await session.execute(
            text(
                """
                SELECT id, event_id, proposed_section, proposed_field,
                       proposed_value, constraint_name, reason,
                       reviewed_status, created_at
                FROM rejected_updates
                WHERE property_id = :pid
                  AND reviewed_status IN ('pending', 'needs_review')
                ORDER BY created_at DESC
                """
            ),
            {"pid": property_id},
        )
    ).all()
    return [
        _RejectionRow(
            id=r.id,
            event_id=r.event_id,
            section=str(r.proposed_section),
            field=str(r.proposed_field),
            value=str(r.proposed_value or ""),
            constraint_name=str(r.constraint_name),
            reason=str(r.reason or ""),
            reviewed_status=str(r.reviewed_status),
            created_at=r.created_at,
        )
        for r in rows
    ]


async def _load_activity_histogram(
    session: AsyncSession, property_id: UUID, *, days: int = ACTIVITY_WINDOW_DAYS
) -> dict[str, int]:
    """Count events in the last ``days`` per source × dominant signal."""
    rows = (
        await session.execute(
            text(
                f"""
                SELECT source, COUNT(*) AS n
                FROM events
                WHERE property_id = :pid
                  AND received_at >= now() - INTERVAL '{int(days)} days'
                GROUP BY source
                ORDER BY n DESC
                """
            ),
            {"pid": property_id},
        )
    ).all()
    return {str(r.source): int(r.n) for r in rows}


async def _load_total_event_count(
    session: AsyncSession, property_id: UUID
) -> int:
    """All-time event count for the property."""
    row = (
        await session.execute(
            text("SELECT COUNT(*) AS n FROM events WHERE property_id = :pid"),
            {"pid": property_id},
        )
    ).first()
    return int(row.n) if row else 0


async def _load_active_tenant(
    session: AsyncSession, property_id: UUID
) -> str | None:
    """Best-guess current tenant name from the tenants table."""
    row = (
        await session.execute(
            text(
                """
                SELECT name FROM tenants
                WHERE property_id = :pid
                ORDER BY move_in_date DESC NULLS LAST
                LIMIT 1
                """
            ),
            {"pid": property_id},
        )
    ).first()
    return str(row.name) if row else None


async def _last_mutation_timestamps(
    session: AsyncSession, property_id: UUID
) -> tuple[datetime | None, datetime | None, datetime | None]:
    """Newest fact / uncertainty / rejection timestamps — feeds the cache key."""
    row = (
        await session.execute(
            text(
                """
                SELECT
                  (SELECT MAX(created_at) FROM facts
                    WHERE property_id = :pid) AS last_fact,
                  (SELECT MAX(created_at) FROM uncertainty_events
                    WHERE property_id = :pid) AS last_unc,
                  (SELECT MAX(created_at) FROM rejected_updates
                    WHERE property_id = :pid) AS last_rej
                """
            ),
            {"pid": property_id},
        )
    ).first()
    if row is None:
        return None, None, None
    return row.last_fact, row.last_unc, row.last_rej


# -----------------------------------------------------------------------------
# Cache
# -----------------------------------------------------------------------------


def _cache_key(
    last_fact: datetime | None,
    last_unc: datetime | None,
    last_rej: datetime | None,
) -> str:
    """Hash of the three "newest mutation" timestamps + version stamp.

    Any change to facts, uncertainties or rejections invalidates the
    cache. Gemini Pro is regenerated; deterministic sections rebuild
    cheaply on every request anyway.
    """
    parts = [
        str(ONBOARDING_CACHE_VERSION),
        last_fact.isoformat() if last_fact else "-",
        last_unc.isoformat() if last_unc else "-",
        last_rej.isoformat() if last_rej else "-",
    ]
    return hashlib.sha1("|".join(parts).encode("utf-8")).hexdigest()[:16]


async def _read_cache(
    session: AsyncSession, property_id: UUID, key: str
) -> str | None:
    """Return the cached briefing text if its key matches ``key``."""
    row = (
        await session.execute(
            text("SELECT metadata FROM properties WHERE id = :pid"),
            {"pid": property_id},
        )
    ).first()
    if row is None:
        return None
    cached = (row.metadata or {}).get("onboarding") or {}
    if cached.get("cache_key") != key:
        return None
    text_blob = cached.get("briefing")
    return str(text_blob) if isinstance(text_blob, str) else None


async def _write_cache(
    session: AsyncSession,
    property_id: UUID,
    *,
    key: str,
    briefing: str,
) -> None:
    """Persist the briefing under ``properties.metadata.onboarding``."""
    payload = {
        "cache_key": key,
        "briefing": briefing,
        "as_of": datetime.now(tz=UTC).isoformat(),
        "version": ONBOARDING_CACHE_VERSION,
    }
    await session.execute(
        text(
            """
            UPDATE properties
            SET metadata = jsonb_set(
              COALESCE(metadata, '{}'::jsonb),
              '{onboarding}',
              CAST(:payload AS JSONB),
              true
            )
            WHERE id = :pid
            """
        ),
        {"pid": property_id, "payload": json.dumps(payload)},
    )
    await session.commit()


# -----------------------------------------------------------------------------
# Section renderers (deterministic)
# -----------------------------------------------------------------------------


_STAMM_FIELDS_DE: tuple[tuple[str, str], ...] = (
    ("kaltmiete", "Kaltmiete"),
    ("nk_vorauszahlung", "Nebenkosten-Vorauszahlung"),
    ("kaution", "Kaution"),
    ("miteigentumsanteil", "Miteigentumsanteil"),
    ("wohnflaeche_qm", "Wohnfläche (m²)"),
    ("mietbeginn", "Mietbeginn"),
    ("mietende", "Mietende"),
    ("lage", "Lage"),
)
_STAMM_FIELDS_EN: tuple[tuple[str, str], ...] = (
    ("kaltmiete", "Cold rent (EUR)"),
    ("nk_vorauszahlung", "Operating-cost prepayment (EUR)"),
    ("kaution", "Deposit (EUR)"),
    ("miteigentumsanteil", "Co-ownership share"),
    ("wohnflaeche_qm", "Floor area (sqm)"),
    ("mietbeginn", "Lease start"),
    ("mietende", "Lease end"),
    ("lage", "Location/floor"),
)


def _render_property_in_60s(
    header: _PropertyHeader,
    *,
    fact_count: int,
    uncertainty_count: int,
    rejection_count: int,
    total_events: int,
    active_tenant: str | None,
    activity: dict[str, int],
    lang: str,
) -> list[str]:
    """Render the deterministic header card."""
    lines: list[str] = []
    lines.append("# " + header.name)
    lines.append("")

    parent_bits: list[str] = []
    if header.address:
        parent_bits.append(f"_{header.address}_")
    if header.building_address:
        parent_bits.append(
            f"Building: {header.building_address}"
        )
    if header.liegenschaft_name:
        parent_bits.append(f"WEG: {header.liegenschaft_name}")
    if parent_bits:
        lines.append(" · ".join(parent_bits))
        lines.append("")

    section_title = (
        "## Property in 60 seconds" if lang == "en" else "## Objekt in 60 Sekunden"
    )
    lines.append(section_title)
    lines.append("")

    fields = _STAMM_FIELDS_DE if lang == "de" else _STAMM_FIELDS_EN
    stamm_lines: list[str] = []
    for key, label in fields:
        value = header.metadata.get(key)
        if value in (None, "", []):
            continue
        stamm_lines.append(f"- **{label}:** {value}")
    if stamm_lines:
        lines.extend(stamm_lines)
        lines.append("")

    if active_tenant:
        tenant_label = "Current tenant" if lang == "en" else "Aktueller Mieter"
        lines.append(f"- **{tenant_label}:** {active_tenant}")
        lines.append("")

    summary_label = "What we know" if lang == "en" else "Was wir wissen"
    lines.append(f"### {summary_label}")
    lines.append("")
    lines.append(
        f"- **Facts on file:** {fact_count}"
        if lang == "en"
        else f"- **Erfasste Fakten:** {fact_count}"
    )
    lines.append(
        f"- **Open uncertainties:** {uncertainty_count}"
        if lang == "en"
        else f"- **Offene Unsicherheiten:** {uncertainty_count}"
    )
    lines.append(
        f"- **Pending rejections:** {rejection_count}"
        if lang == "en"
        else f"- **Offene Ablehnungen:** {rejection_count}"
    )
    lines.append(
        f"- **Total events ingested:** {total_events}"
        if lang == "en"
        else f"- **Eingegangene Ereignisse insgesamt:** {total_events}"
    )
    if activity:
        breakdown = ", ".join(
            f"{src}={n}" for src, n in sorted(activity.items(), key=lambda x: -x[1])
        )
        recent_label = (
            "Last 12 months by source" if lang == "en" else "Letzte 12 Monate nach Quelle"
        )
        lines.append(f"- **{recent_label}:** {breakdown}")
    lines.append("")
    return lines


def _render_open_issues(
    uncertainties: list[UncertaintyRow],
    rejections: list[_RejectionRow],
    active_maintenance: list[Any],
    *,
    lang: str,
) -> list[str]:
    """Top-N open issues, deterministic — ordered by impact + recency."""
    title = "## Open issues right now" if lang == "en" else "## Offene Punkte jetzt"
    lines: list[str] = [title, ""]

    rendered_anything = False

    if active_maintenance:
        sub = (
            "### Active maintenance"
            if lang == "en"
            else "### Aktuelle Wartung"
        )
        lines.append(sub)
        lines.append("")
        for fact in active_maintenance[:TOP_N]:
            lines.append(
                f"- **{fact.field.replace('_', ' ').capitalize()}:** "
                f"{fact.value} "
                f"_(confidence {fact.confidence:.2f})_ "
                f"[source: {fact.source_event_id}]"
                f"(/events/{fact.source_event_id}/source)"
            )
        lines.append("")
        rendered_anything = True

    if uncertainties:
        sub = (
            "### Top open uncertainties"
            if lang == "en"
            else "### Top offene Unsicherheiten"
        )
        lines.append(sub)
        lines.append("")
        for u in uncertainties[:TOP_N]:
            obs = u.observation
            if len(obs) > 140:
                obs = obs[:137].rstrip() + "…"
            lines.append(
                f"- _{u.section}_ — {obs} "
                f"(_{u.reason_uncertain}_) "
                f"[source: event {u.event_id}](/events/{u.event_id}/source)"
            )
        lines.append("")
        rendered_anything = True

    if rejections:
        sub = (
            "### Pending rejections"
            if lang == "en"
            else "### Offene Ablehnungen"
        )
        lines.append(sub)
        lines.append("")
        for r in rejections[:TOP_N]:
            value = r.value
            if len(value) > 80:
                value = value[:77].rstrip() + "…"
            lines.append(
                f"- `{r.constraint_name}` — proposed "
                f"**{r.section}.{r.field}** = `{value}` — {r.reason} "
                f"[source: event {r.event_id}](/events/{r.event_id}/source)"
            )
        lines.append("")
        rendered_anything = True

    if not rendered_anything:
        msg = (
            "No open issues — nothing waiting on you right now."
            if lang == "en"
            else "Keine offenen Punkte — derzeit nichts zu erledigen."
        )
        lines.append(f"_{msg}_")
        lines.append("")
    return lines


def _render_watch_out_for(
    rejections_all: list[_RejectionRow],
    uncertainties: list[UncertaintyRow],
    *,
    lang: str,
) -> list[str]:
    """Recurring patterns — what to keep an eye on."""
    title = "## Watch out for" if lang == "en" else "## Worauf achten"
    lines: list[str] = [title, ""]

    constraint_counts: dict[str, int] = {}
    for r in rejections_all:
        constraint_counts[r.constraint_name] = constraint_counts.get(
            r.constraint_name, 0
        ) + 1
    constraint_top = sorted(
        constraint_counts.items(), key=lambda x: -x[1]
    )[:TOP_N_PATTERNS]
    constraint_top = [c for c in constraint_top if c[1] >= 2]

    section_unc_counts: dict[str, int] = {}
    for u in uncertainties:
        section_unc_counts[u.section] = section_unc_counts.get(u.section, 0) + 1
    unc_top = sorted(
        section_unc_counts.items(), key=lambda x: -x[1]
    )[:TOP_N_PATTERNS]
    unc_top = [u for u in unc_top if u[1] >= 2]

    rendered_anything = False
    if constraint_top:
        sub = (
            "### Recurring rejected updates"
            if lang == "en"
            else "### Häufig abgelehnte Änderungen"
        )
        lines.append(sub)
        lines.append("")
        for name, n in constraint_top:
            lines.append(f"- `{name}` triggered {n} times")
        lines.append("")
        rendered_anything = True

    if unc_top:
        sub = (
            "### Sections with the most uncertainty"
            if lang == "en"
            else "### Bereiche mit den meisten Unsicherheiten"
        )
        lines.append(sub)
        lines.append("")
        for section, n in unc_top:
            lines.append(f"- _{section}_ — {n} unresolved")
        lines.append("")
        rendered_anything = True

    if not rendered_anything:
        msg = (
            "No recurring patterns yet — too little history to draw conclusions."
            if lang == "en"
            else "Noch keine wiederkehrenden Muster — zu wenig Historie."
        )
        lines.append(f"_{msg}_")
        lines.append("")
    return lines


def _render_pointer_index(
    facts: list[Any],
    header: _PropertyHeader,
    *,
    lang: str,
) -> list[str]:
    """Per-section pointer index: where to look for more on each topic."""
    title = (
        "## Where to look for more"
        if lang == "en"
        else "## Wo mehr zu finden ist"
    )
    lines: list[str] = [title, ""]

    by_section: dict[str, list[Any]] = {}
    for f in facts:
        by_section.setdefault(f.section, []).append(f)
    if not by_section:
        msg = (
            "No facts on file yet — once events stream in, source links will appear here."
            if lang == "en"
            else "Noch keine Fakten — sobald Ereignisse einlaufen, erscheinen hier Quellverweise."
        )
        lines.append(f"_{msg}_")
        lines.append("")
    else:
        for section in sorted(by_section.keys()):
            rows = by_section[section]
            head = rows[:TOP_N]
            lines.append(f"- **{section}** ({len(rows)} facts)")
            for f in head:
                if f.source_event_id is None:
                    continue
                lines.append(
                    f"  - {f.field.replace('_', ' ')} → "
                    f"[source: {f.source_event_id}]"
                    f"(/events/{f.source_event_id}/source)"
                )
        lines.append("")

    if header.building_id:
        link_label = (
            f"Open building view ({header.building_address or header.building_id})"
            if lang == "en"
            else f"Hausansicht öffnen ({header.building_address or header.building_id})"
        )
        lines.append(
            f"- [{link_label}](/buildings/{header.building_id}/markdown)"
        )
    if header.liegenschaft_id:
        link_label = (
            f"Open WEG view ({header.liegenschaft_name or header.liegenschaft_id})"
            if lang == "en"
            else f"WEG-Ansicht öffnen ({header.liegenschaft_name or header.liegenschaft_id})"
        )
        lines.append(
            f"- [{link_label}](/liegenschaften/{header.liegenschaft_id}/markdown)"
        )
    lines.append("")
    return lines


# -----------------------------------------------------------------------------
# Section renderer — Gemini Pro briefing (cached)
# -----------------------------------------------------------------------------


def _summarise_facts(facts: list[Any], *, max_lines: int = 30) -> str:
    """Compact bullet list of current facts for the briefing prompt."""
    if not facts:
        return ""
    bullets: list[str] = []
    for f in facts[:max_lines]:
        bullets.append(
            f"- {f.section}.{f.field} = {f.value} "
            f"(confidence {f.confidence:.2f})"
        )
    if len(facts) > max_lines:
        bullets.append(f"- (+{len(facts) - max_lines} more facts not shown)")
    return "\n".join(bullets)


def _summarise_uncertainties(
    uncertainties: list[UncertaintyRow], *, max_lines: int = 12
) -> str:
    """Compact bullet list of open uncertainties for the briefing prompt."""
    if not uncertainties:
        return ""
    bullets: list[str] = []
    for u in uncertainties[:max_lines]:
        obs = u.observation
        if len(obs) > 160:
            obs = obs[:157].rstrip() + "…"
        bullets.append(
            f"- {u.section}: {obs} (reason: {u.reason_uncertain})"
        )
    if len(uncertainties) > max_lines:
        bullets.append(
            f"- (+{len(uncertainties) - max_lines} more uncertainties not shown)"
        )
    return "\n".join(bullets)


def _summarise_rejections(
    rejections: list[_RejectionRow], *, max_lines: int = 8
) -> str:
    """Compact bullet list of pending rejections for the briefing prompt."""
    if not rejections:
        return ""
    bullets: list[str] = []
    for r in rejections[:max_lines]:
        value = r.value
        if len(value) > 80:
            value = value[:77].rstrip() + "…"
        bullets.append(
            f"- {r.constraint_name}: rejected {r.section}.{r.field}={value} "
            f"(reason: {r.reason})"
        )
    return "\n".join(bullets)


def _summarise_activity(activity: dict[str, int]) -> str:
    """Source breakdown of recent activity for the briefing prompt."""
    if not activity:
        return ""
    return "\n".join(
        f"- {src}: {n} events"
        for src, n in sorted(activity.items(), key=lambda x: -x[1])
    )


async def _render_key_context(
    session: AsyncSession,
    property_id: UUID,
    header: _PropertyHeader,
    facts: list[Any],
    uncertainties: list[UncertaintyRow],
    rejections: list[_RejectionRow],
    activity: dict[str, int],
    *,
    lang: str,
    cache_key: str,
) -> list[str]:
    """The Gemini-Pro 5–7 bullet briefing, cached on properties.metadata."""
    title = (
        "## Key context (last 12 months)"
        if lang == "en"
        else "## Wichtiger Kontext (letzte 12 Monate)"
    )

    cached = await _read_cache(session, property_id, cache_key)
    if cached is not None:
        log.info(
            "onboarding.briefing.cache_hit",
            property_id=str(property_id),
            cache_key=cache_key,
        )
        return [title, "", cached, ""]

    facts_summary = _summarise_facts(facts)
    uncertainties_summary = _summarise_uncertainties(uncertainties)
    rejections_summary = _summarise_rejections(rejections)
    activity_summary = _summarise_activity(activity)

    briefing: str | None = None

    # Pioneer (Claude) is the primary path. The briefing always renders
    # in English — Pioneer's prompt enforces that regardless of how the
    # source events are written.
    if pioneer_llm.is_available():
        try:
            briefing = await pioneer_llm.draft_onboarding_briefing(
                property_name=header.name,
                facts_summary=facts_summary,
                uncertainties_summary=uncertainties_summary,
                rejections_summary=rejections_summary,
                activity_summary=activity_summary,
            )
        except pioneer_llm.PioneerUnavailable as exc:
            log.warning(
                "onboarding.briefing.pioneer_unavailable",
                property_id=str(property_id),
                error=str(exc)[:200],
            )

    # Gemini is a fallback for environments where Pioneer is missing.
    if briefing is None and gemini.is_available():
        try:
            briefing = await gemini.draft_onboarding_briefing(
                property_name=header.name,
                facts_summary=facts_summary,
                uncertainties_summary=uncertainties_summary,
                rejections_summary=rejections_summary,
                activity_summary=activity_summary,
                lang="en",
            )
        except gemini.GeminiUnavailable as exc:
            log.warning(
                "onboarding.briefing.gemini_unavailable",
                property_id=str(property_id),
                error=str(exc)[:200],
            )

    if briefing is None:
        msg = (
            "Briefing unavailable — neither Pioneer nor Gemini is "
            "reachable. All deterministic sections above are still "
            "complete."
        )
        return [title, "", msg, ""]

    await _write_cache(
        session, property_id, key=cache_key, briefing=briefing
    )
    log.info(
        "onboarding.briefing.generated",
        property_id=str(property_id),
        cache_key=cache_key,
        chars=len(briefing),
    )
    return [title, "", briefing, ""]


# -----------------------------------------------------------------------------
# Public entry point
# -----------------------------------------------------------------------------


async def render_onboarding(
    session: AsyncSession, property_id: UUID
) -> str:
    """Render the full onboarding markdown for a property.

    Five sections, in reading order:

    1. Property in 60 seconds — stammdaten, what we know.
    2. Open issues right now — uncertainties, rejections, active maintenance.
    3. Key context (last 12 months) — Gemini Pro briefing, cached.
    4. Watch out for — recurring patterns.
    5. Where to look for more — pointer index.

    Section 3 is the only LLM call; if Gemini is unavailable the
    section degrades to a one-line note and the rest of the document
    still renders. Re-rendering is free unless facts / uncertainties /
    rejections have moved since the last render (cache key invalidates).
    """
    header = await _load_header(session, property_id)
    if header is None:
        raise ValueError(f"Property {property_id} not found")

    # Briefings always render in English regardless of the property's
    # source-event majority. The briefing is read by the Hausverwaltung
    # operator (English-speaking team), not by the tenant — keeping the
    # output language fixed avoids confusing a new property manager who
    # opens a German-only property file and gets a German briefing.
    # The original event-language detection still drives the property
    # markdown's section titles via the renderer, just not this briefing.
    lang = "en"

    facts = await _fetch_current_facts(session, property_id)
    uncertainties = await _fetch_open_uncertainties(session, property_id)
    rejections = await _load_pending_rejections(session, property_id)
    activity = await _load_activity_histogram(session, property_id)
    total_events = await _load_total_event_count(session, property_id)
    active_tenant = await _load_active_tenant(session, property_id)
    last_fact, last_unc, last_rej = await _last_mutation_timestamps(
        session, property_id
    )

    cache_key = _cache_key(last_fact, last_unc, last_rej)

    active_maintenance = [
        f
        for f in facts
        if f.section == "maintenance"
        and any(
            kw in f.field.lower()
            for kw in (
                "open_",
                "leak",
                "damage",
                "lost",
                "broken",
                "defect",
                "outage",
            )
        )
    ]

    lines: list[str] = []
    lines.extend(
        _render_property_in_60s(
            header,
            fact_count=len(facts),
            uncertainty_count=len(uncertainties),
            rejection_count=len(rejections),
            total_events=total_events,
            active_tenant=active_tenant,
            activity=activity,
            lang=lang,
        )
    )
    lines.extend(
        _render_open_issues(
            uncertainties, rejections, active_maintenance, lang=lang
        )
    )
    lines.extend(
        await _render_key_context(
            session,
            property_id,
            header,
            facts,
            uncertainties,
            rejections,
            activity,
            lang=lang,
            cache_key=cache_key,
        )
    )
    # "Watch out for" wants ALL rejections (resolved + pending) for pattern-
    # spotting — load a separate cohort for it.
    rejections_all = await _load_all_rejections(session, property_id)
    lines.extend(
        _render_watch_out_for(rejections_all, uncertainties, lang=lang)
    )
    lines.extend(_render_pointer_index(facts, header, lang=lang))

    log.info(
        "onboarding.render",
        property_id=str(property_id),
        lang=lang,
        facts=len(facts),
        uncertainties=len(uncertainties),
        rejections=len(rejections),
        events=total_events,
        cache_key=cache_key,
    )
    return "\n".join(lines).rstrip() + "\n"


async def _load_all_rejections(
    session: AsyncSession, property_id: UUID
) -> list[_RejectionRow]:
    """Every rejection ever recorded — pattern recognition wants the full set."""
    rows = (
        await session.execute(
            text(
                """
                SELECT id, event_id, proposed_section, proposed_field,
                       proposed_value, constraint_name, reason,
                       reviewed_status, created_at
                FROM rejected_updates
                WHERE property_id = :pid
                ORDER BY created_at DESC
                """
            ),
            {"pid": property_id},
        )
    ).all()
    return [
        _RejectionRow(
            id=r.id,
            event_id=r.event_id,
            section=str(r.proposed_section),
            field=str(r.proposed_field),
            value=str(r.proposed_value or ""),
            constraint_name=str(r.constraint_name),
            reason=str(r.reason or ""),
            reviewed_status=str(r.reviewed_status),
            created_at=r.created_at,
        )
        for r in rows
    ]
