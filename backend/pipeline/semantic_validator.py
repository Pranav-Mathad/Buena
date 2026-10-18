"""Semantic event validator.

The rule-based validator (:mod:`backend.pipeline.validator`) catches
known immutable-field contradictions (floor count, address, year
built, …) using hand-coded constraints. The semantic validator
complements it: it asks the configured LLM provider to read the
**canonical property file** and the **new event** side by side and
surface every claim that contradicts what we already know, not just
the dozen fields someone hardcoded.

Why both layers exist:

- Rule-based constraints are **deterministic**: same input, same
  verdict, no LLM cost. They guard the high-stakes immutable
  fields with explicit reasons and required-source-type metadata.
- The semantic validator is **general**: it covers fields nobody
  thought to write a constraint for ("event mentions a 7th floor in a
  4-floor building", "event names a different tenant than stammdaten",
  "event claims rent of €5000 when the lease says €1100"). It costs
  one LLM call per event, runs after extraction, and emits the
  same :class:`Rejection` shape so the existing rejected_updates
  inbox surfaces both kinds uniformly.

The validator never hallucinates new facts. Pure contradictions of
existing context are the only thing it surfaces.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import structlog

from backend.pipeline.validator import Rejection
from backend.services.llm import require_provider

log = structlog.get_logger(__name__)


SYSTEM_HINT = (
    "You audit a new property-management event against the property's "
    "canonical record. You only surface CONTRADICTIONS — facts in the "
    "event that disagree with what's already on file. Novel information "
    "(new maintenance issues, new payments, new requests) is fine and "
    "must NOT be flagged. Respond with a single JSON object that "
    "strictly matches the schema. No prose, no markdown fences."
)


PROMPT_TEMPLATE = """You audit one new event against the existing canonical
record for a property. Your output decides whether each claim in the
event is consistent with what's already on file.

PROPERTY FILE (canonical, condensed record — Stammdaten at top, then
sectioned facts with confidence and source links):
\"\"\"
{property_file}
\"\"\"

NEW EVENT (source: {source}):
\"\"\"
{event_body}
\"\"\"

PROPOSED FACTS the extractor pulled from the event (already filtered
through the rule-based validator; these would be applied unless you
flag a contradiction):
{proposed_block}

DEFINITIONS

- A "contradiction" is a claim in the event that DIRECTLY conflicts
  with a fact in the PROPERTY FILE. Examples:
  • event mentions "7. Stockwerk" / "7th floor" but the building has
    4 floors per stammdaten
  • event names tenant "Hans Müller" but the active tenant on file is
    "Anna Schmidt"
  • event claims rent €5000 when stammdaten says kaltmiete €1100
  • event refers to address "Hauptstraße 12" when the property file
    shows "Musterstraße 26"
- "Novel" information (new maintenance issue, new bank payment, new
  email request) is NOT a contradiction. Do not flag novel claims.
- "Stammdaten" entries (master record at top of the file) are
  authoritative — only an authoritative document can revise them.
  Free-text events that contradict stammdaten are HARD rejections.
- A current fact with confidence ≥ 0.9 is also strong; contradictions
  there are HARD rejections. Lower-confidence facts are SOFT — flag
  them as needs_review.

SEVERITY

- "hard": stammdaten contradiction OR contradicts a high-confidence
  fact, AND the event source is free-text (email / slack / letter).
- "soft": contradicts a lower-confidence fact, or the event has
  document_type that *could* legitimately revise the record (e.g.
  structural_permit for floor count). These need a human review.

Always be conservative. If a claim is ambiguous, don't flag it —
returning "all_consistent" is the default.

JSON SCHEMA

{{
  "verdict": "all_consistent" | "has_contradictions",
  "contradictions": [
    {{
      "claim": "<short quote or paraphrase from the event>",
      "conflict_with": "<short quote or paraphrase from the file>",
      "section": "<best-fit section, e.g. building_overview, tenants, lease, financials, maintenance>",
      "field": "<best-fit field, e.g. floor_count, tenant_name, kaltmiete, address>",
      "severity": "hard" | "soft",
      "reason": "<one sentence explaining the contradiction in plain language>"
    }}
  ]
}}"""


@dataclass(frozen=True)
class SemanticVerdict:
    """Output shape of one semantic-validator call."""

    verdict: str
    rejections: tuple[Rejection, ...]
    latency_ms: float
    model: str

    @property
    def has_contradictions(self) -> bool:
        return self.verdict == "has_contradictions" and bool(self.rejections)


def _format_proposed_block(facts: list[dict[str, Any]]) -> str:
    """Bullet-list extracted proposals so the LLM can also rule on them."""
    if not facts:
        return "  (no proposed facts — the extractor only saw novel/maintenance content)"
    lines: list[str] = []
    for f in facts[:20]:
        lines.append(
            f"  - section={f.get('section')!r}, field={f.get('field')!r}, "
            f"value={str(f.get('value', ''))[:200]!r}, "
            f"confidence={f.get('confidence')}"
        )
    return "\n".join(lines)


def _to_rejection(item: dict[str, Any]) -> Rejection | None:
    """Translate one LLM contradiction dict into a :class:`Rejection`."""
    section = str(item.get("section") or "").strip() or "overview"
    field = str(item.get("field") or "").strip() or "claim"
    claim = str(item.get("claim") or "").strip()
    conflict = str(item.get("conflict_with") or "").strip()
    reason = str(item.get("reason") or "").strip()
    severity = str(item.get("severity") or "soft").strip().lower()

    if not claim or not reason:
        # Be defensive — skip malformed rows the model emitted.
        return None

    rendered_value = claim if not conflict else f"{claim} (file says: {conflict})"
    return Rejection(
        section=section,
        field=field,
        proposed_value=rendered_value[:1000],
        proposed_confidence=0.0,  # synthetic — surfaced by the auditor, not the extractor
        constraint_name="semantic_consistency",
        reason=reason[:500],
        required_source_type=None,
        needs_review=severity != "hard",
    )


async def semantic_validate(
    *,
    property_file_markdown: str,
    event_body: str,
    event_source: str,
    proposed_facts: list[dict[str, Any]],
    timeout_s: float = 30.0,
) -> SemanticVerdict:
    """Audit one event against the canonical property file.

    Returns a verdict whose ``rejections`` tuple is ready to merge into the
    worker's rule-based rejection list; an empty tuple means nothing
    contradicts.

    Raises :class:`LLMNotConfiguredError` when no provider is configured and
    :class:`LLMUnavailableError` when the call fails. Callers treat both as
    "skip semantic validation for this event": the rule-based constraints
    still run and remain authoritative.
    """
    provider = require_provider()

    prompt = PROMPT_TEMPLATE.format(
        property_file=property_file_markdown.strip() or "(empty property file)",
        source=event_source or "unknown",
        event_body=event_body.strip()[:4000] or "(empty body)",
        proposed_block=_format_proposed_block(proposed_facts),
    )

    log.info(
        "semantic_validator.start",
        provider=provider.name,
        model=provider.model,
        prompt_chars=len(prompt),
        proposed_count=len(proposed_facts),
        event_source=event_source,
    )

    completion = await provider.complete_json(
        system=SYSTEM_HINT, prompt=prompt, temperature=0.0, timeout_s=timeout_s
    )
    data = completion.data
    latency_ms = completion.latency_ms
    model = completion.model
    verdict_raw = str(data.get("verdict") or "").strip()
    if verdict_raw not in {"all_consistent", "has_contradictions"}:
        # Tolerate model mistakes — treat as consistent rather than
        # auto-rejecting everything.
        verdict_raw = "all_consistent"

    rejections: list[Rejection] = []
    raw_contradictions = data.get("contradictions") or []
    if isinstance(raw_contradictions, list):
        for item in raw_contradictions:
            if not isinstance(item, dict):
                continue
            r = _to_rejection(item)
            if r is not None:
                rejections.append(r)

    log.info(
        "semantic_validator.ok",
        model=model,
        latency_ms=round(latency_ms, 1),
        verdict=verdict_raw,
        rejection_count=len(rejections),
    )

    return SemanticVerdict(
        verdict=verdict_raw if rejections else "all_consistent",
        rejections=tuple(rejections),
        latency_ms=latency_ms,
        model=model,
    )


__all__ = [
    "SemanticVerdict",
    "semantic_validate",
]
