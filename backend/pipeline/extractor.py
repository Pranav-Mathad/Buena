"""Extract structured facts from an event.

Phase 8 Step 5 reshapes this module:

- Detect the event language with :mod:`backend.services.lang` and
  dispatch to the matching prompt template (German vs English).
- Default extractor model is **Gemini Pro** (per Step 5: extraction
  quality matters more than throughput on the email path). Flash is
  reserved for the auxiliary category-classification fallback below.
- Rule fallback consults :mod:`backend.pipeline.lexicon` so the
  keyword set lives in one place and covers German + English.

Both paths return the same :class:`ExtractionResult` so downstream
stages don't need to branch.
"""

from __future__ import annotations

import re
from typing import Any

import structlog

from backend.pipeline.lexicon import (
    HEATING,
    KEY_LOSS,
    LEASE,
    OWNER_COMM,
    PAYMENT,
    TENANT_CHANGE,
    WATER,
    WINDOW_DOOR,
    Topic,
    categorize,
)
from backend.services.gemini import (
    ExtractionResult,
    GeminiUnavailable,
    extract_facts as gemini_extract,
    is_available as gemini_available,
)
from backend.services.lang import detect_language

log = structlog.get_logger(__name__)


# Phase 9 Step 9.1 — confidence floor. Facts the extractor produced
# below this threshold are demoted to ``uncertain[]`` regardless of
# whether the model itself flagged them, so the applier never writes
# a low-confidence fact into the canonical ``facts`` table.
# Hardcoded for now; a per-section config table is a Phase 9+ item.
CONFIDENCE_FLOOR: float = 0.7


_SUBJECT_RE = re.compile(r"^subject:\s*(.+)$", re.IGNORECASE | re.MULTILINE)
_FROM_RE = re.compile(r"^from:\s*(.+)$", re.IGNORECASE | re.MULTILINE)


def _apply_confidence_floor(result: ExtractionResult) -> ExtractionResult:
    """Demote sub-floor facts to ``uncertain[]`` so they don't reach the applier.

    The extractor (Gemini *or* rule fallback) sometimes emits facts at
    confidence 0.6 — high enough to be worth surfacing, low enough to
    not warrant overwriting a canonical fact. Step 9.1's contract is
    that those land in ``uncertainty_events`` instead of ``facts``.
    """
    keep: list[dict[str, Any]] = []
    demoted: list[dict[str, Any]] = []
    for fact in result.facts_to_update:
        try:
            confidence = float(fact.get("confidence", 0))
        except (TypeError, ValueError):
            confidence = 0.0
        if confidence < CONFIDENCE_FLOOR:
            demoted.append(
                {
                    "observation": str(fact.get("value", ""))[:500],
                    "hypothesis": str(fact.get("value", ""))[:500],
                    "reason_uncertain": (
                        f"low extractor confidence ({confidence:.2f}) "
                        f"below floor {CONFIDENCE_FLOOR:.2f}"
                    ),
                    "relevant_section": str(fact.get("section", "")),
                    "relevant_field": str(fact.get("field", "")) or None,
                }
            )
        else:
            keep.append(fact)
    if not demoted:
        return result
    log.info(
        "extractor.confidence_floor",
        kept=len(keep),
        demoted=len(demoted),
        floor=CONFIDENCE_FLOOR,
    )
    return ExtractionResult(
        category=result.category,
        priority=result.priority,
        facts_to_update=keep,
        uncertain=list(result.uncertain) + demoted,
        summary=result.summary,
        raw=result.raw,
        latency_ms=result.latency_ms,
        model=result.model,
        source=result.source,
        prompt_tokens=result.prompt_tokens,
        completion_tokens=result.completion_tokens,
    )


def _extract_subject(raw: str) -> str:
    """Pull the ``Subject:`` line out of an email; fall back to the first line."""
    match = _SUBJECT_RE.search(raw)
    return match.group(1).strip() if match else raw.strip().split("\n", 1)[0][:80]


# Topic → (category, priority, section, field) tuples used by the rule
# fallback to emit a single fact when one of the lexicon families fires.
# Field names align with the field_vocabulary.json contract so the rule
# fallback's output isn't penalised by the eval scorer just for naming.
_TOPIC_DISPATCH: dict[str, tuple[str, str, str, str]] = {
    HEATING.name:        ("maintenance", "high",   "maintenance", "open_heating_issue"),
    WATER.name:          ("maintenance", "urgent", "maintenance", "open_water_leak"),
    KEY_LOSS.name:       ("maintenance", "medium", "maintenance", "key_lost"),
    WINDOW_DOOR.name:    ("maintenance", "medium", "maintenance", "defective_window"),
    LEASE.name:          ("lease",       "medium", "lease",       "renewal_discussion"),
    TENANT_CHANGE.name:  ("tenant_change","medium","lease",       "termination_notice"),
    PAYMENT.name:        ("payment",     "medium", "financials",  "payment_mention"),
    OWNER_COMM.name:     ("owner_communication", "medium", "overview", "sale_intent"),
}


def _rule_based(
    *, source: str, raw_content: str, lang: str
) -> ExtractionResult:
    """Deterministic fallback when Gemini is unavailable or errors out.

    Uses :mod:`backend.pipeline.lexicon` for the keyword set so the
    German + English vocabularies coexist. Only emits a fact when one
    of the topic regexes fires; otherwise returns ``category=other``
    with an empty facts list (the right answer for chitchat /
    auto-replies).
    """
    subject = _extract_subject(raw_content)
    sender = _FROM_RE.search(raw_content)
    sender_hint = sender.group(1).strip() if sender else ""

    match = categorize(raw_content, lang=lang)
    if match is None:
        return ExtractionResult(
            category="other",
            priority="low",
            facts_to_update=[],
            summary=subject,
            raw={"rule": "passthrough", "lang": lang},
            source="rule",
        )

    topic, _ = match
    category, priority, section, field = _TOPIC_DISPATCH[topic.name]
    note = f"{subject}"
    if sender_hint:
        note = f"{subject} (gemeldet via {source} von {sender_hint})" if lang == "de" \
            else f"{subject} (reported via {source} from {sender_hint})"
    return ExtractionResult(
        category=category,
        priority=priority,
        facts_to_update=[
            {
                "section": section,
                "field": field,
                "value": note,
                "confidence": 0.78,
            }
        ],
        summary=note[:140],
        raw={"rule": topic.name, "lang": lang},
        source="rule",
    )


async def extract(
    *,
    property_name: str,
    current_context_excerpt: str,
    source: str,
    raw_content: str,
) -> ExtractionResult:
    """Run Gemini Pro if possible; otherwise fall back to the lexicon rules.

    ``lang`` is detected from ``raw_content`` and threaded into both
    paths so the prompt + lexicon align with the email body's language.
    """
    lang = detect_language(raw_content)

    if gemini_available():
        try:
            result = await gemini_extract(
                property_name=property_name,
                current_context_excerpt=current_context_excerpt,
                source=source,
                raw_content=raw_content,
                lang=lang,
            )
            return _apply_confidence_floor(result)
        except GeminiUnavailable as exc:
            log.warning("extractor.gemini_unavailable", error=str(exc), lang=lang)

    log.info("extractor.fallback.rule_based", source=source, lang=lang)
    return _apply_confidence_floor(
        _rule_based(source=source, raw_content=raw_content, lang=lang)
    )
