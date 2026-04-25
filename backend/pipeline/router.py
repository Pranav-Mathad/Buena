"""Route inbound events to a property.

Strategy:

- **Free-text events (email, slack, web)** → :func:`route` runs:

  1. Exact alias / name substring match (case-insensitive). Longest hit
     wins.
  2. Token-overlap fallback above a threshold.

- **Structured events (bank, invoice, erp)** → :func:`route_structured`
  uses metadata IDs only. No token-overlap, because verwendungszweck
  strings share lots of nouns ("Wartung", "Hausmeister") with property
  aliases and would produce wrong matches. Order:

  1. ``metadata.eh_id`` → property whose ``aliases`` contains the EH-NNN.
  2. ``metadata.mie_id`` → tenant by ``metadata.buena_mie_id`` →
     occupied property.
  3. ``metadata.invoice_ref`` → events with ``source='invoice'`` whose
     filename contains that INV-NNN → that event's resolved property.
     (Sequencing matters — invoices need to be loaded before bank rows
     for this path to be useful; until then it's a documented miss.)

Unmatched events stay with ``property_id IS NULL`` and surface in the
``GET /admin/unrouted`` inbox for human triage.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any
from uuid import UUID

import structlog
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

log = structlog.get_logger(__name__)

_TOKEN_RE = re.compile(r"[A-Za-zÄÖÜäöüß0-9]{2,}")
_STOPWORDS = {
    "apt", "apartment", "the", "and", "for", "with", "from", "this",
    "that", "strasse", "str",
}


@dataclass(frozen=True)
class PropertyRoute:
    """Routing candidate with score + matched alias for diagnostics."""

    property_id: UUID
    name: str
    score: float
    matched_alias: str


def _tokenize(text_in: str) -> set[str]:
    """Lowercase alphanumeric tokens, stopwords dropped."""
    return {t.lower() for t in _TOKEN_RE.findall(text_in) if t.lower() not in _STOPWORDS}


async def _load_routing_corpus(
    session: AsyncSession,
) -> list[tuple[UUID, str, list[str]]]:
    """Return ``(property_id, name, aliases_including_name)`` for all properties."""
    result = await session.execute(
        text("SELECT id, name, aliases FROM properties")
    )
    corpus: list[tuple[UUID, str, list[str]]] = []
    for row in result.all():
        aliases = list(row.aliases or [])
        if row.name not in aliases:
            aliases.append(row.name)
        corpus.append((row.id, row.name, aliases))
    return corpus


async def route(
    session: AsyncSession,
    raw_content: str,
    *,
    min_score: float = 0.3,
) -> PropertyRoute | None:
    """Pick the most likely property for ``raw_content``.

    Returns ``None`` when no candidate clears ``min_score`` — the caller should
    route the event to an 'unrouted inbox' (Phase 1 TODO: surface in UI; for
    now we log and leave the event with ``property_id IS NULL``).
    """
    haystack = raw_content.lower()
    corpus = await _load_routing_corpus(session)

    # Pass 1: substring alias match; longest hit wins.
    best: PropertyRoute | None = None
    for property_id, name, aliases in corpus:
        for alias in aliases:
            needle = alias.lower().strip()
            if needle and needle in haystack:
                score = 0.6 + min(len(needle) / 60.0, 0.39)
                if best is None or score > best.score:
                    best = PropertyRoute(
                        property_id=property_id,
                        name=name,
                        score=score,
                        matched_alias=alias,
                    )

    if best is not None:
        log.info(
            "router.match.alias",
            property_id=str(best.property_id),
            name=best.name,
            alias=best.matched_alias,
            score=round(best.score, 2),
        )
        return best

    # Pass 2: token-overlap fallback.
    event_tokens = _tokenize(raw_content)
    if not event_tokens:
        return None

    scored: list[PropertyRoute] = []
    for property_id, name, aliases in corpus:
        alias_tokens: set[str] = set()
        for alias in aliases:
            alias_tokens |= _tokenize(alias)
        if not alias_tokens:
            continue
        overlap = event_tokens & alias_tokens
        if not overlap:
            continue
        score = len(overlap) / len(alias_tokens)
        scored.append(
            PropertyRoute(
                property_id=property_id,
                name=name,
                score=score,
                matched_alias=", ".join(sorted(overlap)),
            )
        )

    scored.sort(key=lambda r: r.score, reverse=True)
    if scored and scored[0].score >= min_score:
        chosen = scored[0]
        log.info(
            "router.match.tokens",
            property_id=str(chosen.property_id),
            name=chosen.name,
            score=round(chosen.score, 2),
            matched=chosen.matched_alias,
        )
        return chosen

    log.info("router.nomatch", tokens=len(event_tokens))
    return None


# -----------------------------------------------------------------------------
# Structured-event routing (bank, invoice, erp)
# -----------------------------------------------------------------------------


# Phase 8.1 — keyword set for liegenschaft (WEG) routing. The list is
# drawn from real Buena verwendungszweck strings; matching is
# case-insensitive with word boundaries because the dataset mixes
# `Hausgeld / HAUSGELD / hausgeld` freely.
WEG_KEYWORDS: tuple[str, ...] = (
    "hausgeld",
    "verwaltergebühr",
    "verwaltergebuehr",
    "gemeinschaftskosten",
    "hausverwaltung",
    "weg",
    "sonderumlage",
    "instandhaltungsrücklage",
    "instandhaltungsruecklage",
    "instandhaltungsruecklage",
    "instandhaltungsrücklage",
    "kontofuehrungsgebuehr",
    "kontoführungsgebühr",
)
WEG_KATEGORIE: frozenset[str] = frozenset(
    {"hausgeld", "dienstleister", "versorger", "sonstige"}
)
_WEG_KEYWORD_RE = re.compile(
    r"\b(?:" + "|".join(re.escape(k) for k in WEG_KEYWORDS) + r")\b",
    re.IGNORECASE,
)
_HAUS_RE = re.compile(r"\bHAUS-\d+\b", re.IGNORECASE)


@dataclass(frozen=True)
class StructuredRoute:
    """Outcome of :func:`route_structured`.

    Exactly one of ``property_id``, ``building_id``, ``liegenschaft_id``
    is set when the event is routed; all three are ``None`` for genuinely
    unrouted events. ``method`` names the precedence rule that fired.
    """

    property_id: UUID | None = None
    building_id: UUID | None = None
    liegenschaft_id: UUID | None = None
    method: str = "unrouted"  # 'eh_alias' | 'mie_to_eh' | 'invoice_ref'
                              # | 'haus_alias' | 'weg_keyword'
                              # | 'weg_kategorie' | 'inherit_from_invoice'
                              # | 'unrouted'
    reason: str = ""

    @property
    def is_routed(self) -> bool:
        """True iff at least one of the three scope IDs is set."""
        return any(
            x is not None
            for x in (self.property_id, self.building_id, self.liegenschaft_id)
        )


async def _route_by_eh_alias(
    session: AsyncSession, eh_id: str
) -> UUID | None:
    """Look up a property whose ``aliases`` array contains ``eh_id``."""
    row = (
        await session.execute(
            text(
                """
                SELECT id FROM properties
                WHERE :eh = ANY(aliases)
                LIMIT 1
                """
            ),
            {"eh": eh_id},
        )
    ).first()
    return UUID(str(row.id)) if row else None


async def _route_by_mie_id(
    session: AsyncSession, mie_id: str
) -> UUID | None:
    """``MIE-NNN`` → tenant.metadata.buena_mie_id → occupied property."""
    row = (
        await session.execute(
            text(
                """
                SELECT property_id FROM tenants
                WHERE metadata->>'buena_mie_id' = :mie
                  AND property_id IS NOT NULL
                LIMIT 1
                """
            ),
            {"mie": mie_id},
        )
    ).first()
    return UUID(str(row.property_id)) if row else None


async def _route_by_invoice_ref(
    session: AsyncSession, invoice_ref: str
) -> tuple[UUID | None, UUID | None, UUID | None]:
    """Inherit attribution from a prior invoice event referenced by ``invoice_ref``.

    Returns ``(property_id, building_id, liegenschaft_id)`` from the
    invoice — at most one is set; all three None means the invoice
    exists but lacks attribution itself.
    """
    row = (
        await session.execute(
            text(
                """
                SELECT property_id, building_id, liegenschaft_id
                FROM events
                WHERE source = 'invoice'
                  AND (property_id IS NOT NULL
                       OR building_id IS NOT NULL
                       OR liegenschaft_id IS NOT NULL)
                  AND (metadata->>'filename') LIKE '%' || :inv || '%'
                ORDER BY received_at DESC
                LIMIT 1
                """
            ),
            {"inv": invoice_ref},
        )
    ).first()
    if row is None:
        return (None, None, None)
    return (
        UUID(str(row.property_id)) if row.property_id else None,
        UUID(str(row.building_id)) if row.building_id else None,
        UUID(str(row.liegenschaft_id)) if row.liegenschaft_id else None,
    )


async def _route_by_haus_alias(
    session: AsyncSession, haus_id: str
) -> UUID | None:
    """``HAUS-NN`` → buildings table via ``metadata->>'buena_haus_id'``."""
    row = (
        await session.execute(
            text(
                """
                SELECT id FROM buildings
                WHERE metadata->>'buena_haus_id' = :haus
                LIMIT 1
                """
            ),
            {"haus": haus_id.upper()},
        )
    ).first()
    return UUID(str(row.id)) if row else None


async def _default_liegenschaft(session: AsyncSession) -> UUID | None:
    """Return the WEG when there is exactly one in the database.

    The liegenschaft-keyword + kategorie heuristics know an event is
    WEG-level but Buena doesn't carry a per-Liegenschaft identifier in
    bank rows. With only one Liegenschaft loaded the choice is
    unambiguous; multi-Liegenschaft tenants will need a richer matcher.
    """
    row = (
        await session.execute(
            text("SELECT id FROM liegenschaften LIMIT 2")
        )
    ).all()
    if len(row) == 1:
        return UUID(str(row[0].id))
    return None


async def route_structured(
    session: AsyncSession,
    metadata: dict[str, Any],
    *,
    event_source: str | None = None,
) -> StructuredRoute:
    """Pick the scope a structured event belongs to using metadata IDs only.

    Phase 8.1 precedence — first match wins:

    1. ``eh_id``       → property (unit)
    2. ``mie_id``      → tenant → property
    3. ``invoice_ref`` → inherit attribution from the matching invoice
    4. ``haus_id`` (or ``HAUS-NN`` in raw_content/verwendungszweck) → building
    5. Liegenschaft (WEG) when:
       - ``kategorie`` ∈ ``WEG_KATEGORIE``, or
       - ``verwendungszweck`` (or any text field on the event) matches
         a WEG keyword (case-insensitive, word-boundary), or
       - ``event_source == 'invoice'`` *and* a single Liegenschaft exists
         (Buena invariant: every invoice in the archive is a WEG bill —
         filenames carry contractor + invoice number but no per-unit
         attribution)

    Reasons surfaced in ``StructuredRoute.reason`` are kept short + stable
    so the ``GET /admin/unrouted`` diagnostic is skim-friendly.
    """
    eh_id = metadata.get("eh_id")
    mie_id = metadata.get("mie_id")
    invoice_ref = metadata.get("invoice_ref")
    haus_id_meta = metadata.get("haus_id")
    kategorie = (metadata.get("kategorie") or "").lower().strip()
    verwendungszweck = str(metadata.get("verwendungszweck") or "")
    raw_text = " ".join(
        str(metadata.get(k) or "")
        for k in ("verwendungszweck", "filename", "subject", "from")
    )

    # 1. Property (unit)
    if eh_id:
        property_id = await _route_by_eh_alias(session, eh_id)
        if property_id is not None:
            return StructuredRoute(
                property_id=property_id,
                method="eh_alias",
                reason=f"matched {eh_id}",
            )

    # 2. Property via tenant
    if mie_id:
        property_id = await _route_by_mie_id(session, mie_id)
        if property_id is not None:
            return StructuredRoute(
                property_id=property_id,
                method="mie_to_eh",
                reason=f"matched {mie_id}",
            )

    # 3. Inherit from a prior invoice's attribution (any tier)
    if invoice_ref:
        prop_id, bld_id, lie_id = await _route_by_invoice_ref(session, invoice_ref)
        if any((prop_id, bld_id, lie_id)):
            return StructuredRoute(
                property_id=prop_id,
                building_id=bld_id,
                liegenschaft_id=lie_id,
                method="inherit_from_invoice",
                reason=f"linked via {invoice_ref}",
            )

    # 4. Building via HAUS-NN
    haus_id = str(haus_id_meta or "").strip()
    if not haus_id:
        match = _HAUS_RE.search(raw_text)
        if match:
            haus_id = match.group(0).upper()
    if haus_id:
        building_id = await _route_by_haus_alias(session, haus_id)
        if building_id is not None:
            return StructuredRoute(
                building_id=building_id,
                method="haus_alias",
                reason=f"matched {haus_id}",
            )

    # 5. Liegenschaft (WEG) — kategorie / keyword / source heuristics
    is_weg_kategorie = kategorie in WEG_KATEGORIE and kategorie != "hausgeld"
    # `hausgeld` as a kategorie is per-unit (verwendungszweck has EH-NNN);
    # it only collapses to WEG when EH-/MIE-/HAUS- all missed above and
    # the `Hausgeld` *keyword* still appears. We catch that via the
    # keyword match to avoid double-attributing valid per-unit hausgeld
    # rows.
    keyword_match = _WEG_KEYWORD_RE.search(raw_text or verwendungszweck)
    is_weg_invoice = event_source == "invoice"

    if is_weg_kategorie or keyword_match or is_weg_invoice:
        lie_id = await _default_liegenschaft(session)
        if lie_id is not None:
            if is_weg_kategorie:
                return StructuredRoute(
                    liegenschaft_id=lie_id,
                    method="weg_kategorie",
                    reason=f"kategorie={kategorie}",
                )
            if keyword_match:
                return StructuredRoute(
                    liegenschaft_id=lie_id,
                    method="weg_keyword",
                    reason=f"keyword '{keyword_match.group(0)}'",
                )
            return StructuredRoute(
                liegenschaft_id=lie_id,
                method="weg_invoice",
                reason="invoice without per-unit attribution",
            )

    # Unrouted — produce a precise diagnostic
    if not any((eh_id, mie_id, invoice_ref, haus_id, kategorie)):
        reason = "no EH-/MIE-/INV-/HAUS-/kategorie in metadata"
    else:
        seen = ", ".join(s for s in (eh_id, mie_id, invoice_ref, haus_id) if s)
        reason = (
            f"refs {seen or '(none)'} kat={kategorie or '(none)'} unresolved"
        )
    return StructuredRoute(method="unrouted", reason=reason)
