"""Route inbound events to a property.

Strategy (simple, explainable, demo-safe):

1. Exact alias / name substring match against each property's ``name +
   aliases`` set (case-insensitive). Longest alias wins when several match.
2. Token-overlap fallback: score each property by how many alias tokens
   appear in the event text and pick the highest scorer above a threshold.

No fuzzy libraries; regex + Python set arithmetic is plenty for the demo.
Unmatched events are flagged so the worker can park them for a human.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
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
