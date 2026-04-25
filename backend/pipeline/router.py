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


@dataclass(frozen=True)
class StructuredRoute:
    """Outcome of :func:`route_structured`."""

    property_id: UUID | None
    method: str  # 'eh_alias' | 'mie_to_eh' | 'invoice_ref' | 'unrouted'
    reason: str  # human-readable diagnostic surfaced in /admin/unrouted


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
) -> UUID | None:
    """Find the invoice event whose filename contains ``invoice_ref``."""
    row = (
        await session.execute(
            text(
                """
                SELECT property_id FROM events
                WHERE source = 'invoice'
                  AND property_id IS NOT NULL
                  AND (metadata->>'filename') LIKE '%' || :inv || '%'
                ORDER BY received_at DESC
                LIMIT 1
                """
            ),
            {"inv": invoice_ref},
        )
    ).first()
    return UUID(str(row.property_id)) if row else None


async def route_structured(
    session: AsyncSession,
    metadata: dict[str, Any],
) -> StructuredRoute:
    """Pick the property a structured event belongs to using metadata IDs only.

    Reasons surfaced in ``StructuredRoute.reason`` are kept short + stable
    so the unrouted-inbox diagnostic in ``GET /admin/unrouted`` is
    skim-friendly.
    """
    eh_id = metadata.get("eh_id")
    mie_id = metadata.get("mie_id")
    invoice_ref = metadata.get("invoice_ref")

    if eh_id:
        property_id = await _route_by_eh_alias(session, eh_id)
        if property_id is not None:
            return StructuredRoute(property_id, "eh_alias", f"matched {eh_id}")

    if mie_id:
        property_id = await _route_by_mie_id(session, mie_id)
        if property_id is not None:
            return StructuredRoute(property_id, "mie_to_eh", f"matched {mie_id}")

    if invoice_ref:
        property_id = await _route_by_invoice_ref(session, invoice_ref)
        if property_id is not None:
            return StructuredRoute(
                property_id, "invoice_ref", f"linked via {invoice_ref}"
            )

    if not (eh_id or mie_id or invoice_ref):
        reason = "no EH-/MIE-/INV- in metadata (likely shared-service payment)"
    else:
        seen = ", ".join(s for s in (eh_id, mie_id, invoice_ref) if s)
        reason = f"refs {seen} present but unresolved"
    return StructuredRoute(None, "unrouted", reason)
