"""Extract structured facts from an event.

The hot path is one Gemini Flash call (Part VII). When Gemini is unavailable
(no API key, transient outage) we fall back to a deterministic keyword-based
extractor so the demo never bricks on network failure — this is the
mitigation enumerated in Part XII.

Both paths return the same :class:`ExtractionResult` so downstream stages
don't need to branch.
"""

from __future__ import annotations

import re

import structlog

from backend.services.gemini import (
    ExtractionResult,
    GeminiUnavailable,
    extract_facts as gemini_extract,
    is_available as gemini_available,
)

log = structlog.get_logger(__name__)


_HEATING_RE = re.compile(r"heat|radiator|boiler|hot water|no warm", re.IGNORECASE)
_LEAK_RE = re.compile(r"leak|drip|water damage|p-?trap", re.IGNORECASE)
_PAYMENT_RE = re.compile(r"rent|payment|deposit|eur|€", re.IGNORECASE)
_LEASE_RE = re.compile(r"lease|renewal|term|vertrag|mietvertrag", re.IGNORECASE)
_COMPLIANCE_RE = re.compile(r"permit|inspection|bezirksamt|versicherung|compliance", re.IGNORECASE)
_SUBJECT_RE = re.compile(r"^subject:\s*(.+)$", re.IGNORECASE | re.MULTILINE)
_FROM_RE = re.compile(r"^from:\s*(.+)$", re.IGNORECASE | re.MULTILINE)


def _extract_subject(raw: str) -> str:
    match = _SUBJECT_RE.search(raw)
    return match.group(1).strip() if match else raw.strip().split("\n", 1)[0][:80]


def _rule_based(*, source: str, raw_content: str) -> ExtractionResult:
    """Deterministic fallback used when Gemini is unavailable.

    Covers the demo-day email shapes:
    - Heating complaint  → ``maintenance.heating_issue_<yyyymm>`` fact
    - Water leak         → ``maintenance.water_issue``
    - Payment / deposit  → ``financials.payment_mention``
    - Lease / renewal    → ``lease.renewal_discussion``
    - Compliance letter  → ``compliance.note``
    Anything else returns ``category=other`` with an empty facts list.
    """
    subject = _extract_subject(raw_content)
    sender = _FROM_RE.search(raw_content)
    sender_hint = sender.group(1).strip() if sender else ""

    if _HEATING_RE.search(raw_content):
        return ExtractionResult(
            category="maintenance",
            priority="high",
            facts_to_update=[
                {
                    "section": "maintenance",
                    "field": "latest_heating_issue",
                    "value": f"{subject} (reported via {source}"
                             f"{f' from {sender_hint}' if sender_hint else ''}).",
                    "confidence": 0.82,
                }
            ],
            summary=f"Heating complaint: {subject}",
            raw={"rule": "heating"},
            source="rule",
        )

    if _LEAK_RE.search(raw_content):
        return ExtractionResult(
            category="maintenance",
            priority="medium",
            facts_to_update=[
                {
                    "section": "maintenance",
                    "field": "latest_water_issue",
                    "value": f"{subject} (reported via {source}).",
                    "confidence": 0.78,
                }
            ],
            summary=f"Plumbing / leak: {subject}",
            raw={"rule": "leak"},
            source="rule",
        )

    if _PAYMENT_RE.search(raw_content):
        return ExtractionResult(
            category="payment",
            priority="low",
            facts_to_update=[
                {
                    "section": "financials",
                    "field": "payment_mention",
                    "value": subject,
                    "confidence": 0.7,
                }
            ],
            summary=f"Financial mention: {subject}",
            raw={"rule": "payment"},
            source="rule",
        )

    if _LEASE_RE.search(raw_content):
        return ExtractionResult(
            category="lease",
            priority="medium",
            facts_to_update=[
                {
                    "section": "lease",
                    "field": "renewal_discussion",
                    "value": subject,
                    "confidence": 0.7,
                }
            ],
            summary=f"Lease update: {subject}",
            raw={"rule": "lease"},
            source="rule",
        )

    if _COMPLIANCE_RE.search(raw_content):
        return ExtractionResult(
            category="compliance",
            priority="medium",
            facts_to_update=[
                {
                    "section": "compliance",
                    "field": "note",
                    "value": subject,
                    "confidence": 0.72,
                }
            ],
            summary=f"Compliance note: {subject}",
            raw={"rule": "compliance"},
            source="rule",
        )

    return ExtractionResult(
        category="other",
        priority="low",
        facts_to_update=[],
        summary=subject,
        raw={"rule": "passthrough"},
        source="rule",
    )


async def extract(
    *,
    property_name: str,
    current_context_excerpt: str,
    source: str,
    raw_content: str,
) -> ExtractionResult:
    """Run Gemini if possible; otherwise fall back to rules."""
    if gemini_available():
        try:
            return await gemini_extract(
                property_name=property_name,
                current_context_excerpt=current_context_excerpt,
                source=source,
                raw_content=raw_content,
            )
        except GeminiUnavailable as exc:
            log.warning("extractor.gemini_unavailable", error=str(exc))

    log.info("extractor.fallback.rule_based", source=source)
    return _rule_based(source=source, raw_content=raw_content)
