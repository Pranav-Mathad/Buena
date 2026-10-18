"""Extraction contract shared by every LLM provider.

The JSON schema, the per-language prompt templates
(``backend/services/prompts/extraction_{de,en}.md``) and the controlled field
vocabulary (``field_vocabulary.json``) live here so that Gemini, Pioneer and
any future provider extract into exactly the same ``(section, field)`` space,
and the eval harness measures one contract regardless of backend.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path
from typing import Any

# Gemini's ``response_schema`` accepts a strict OpenAPI 3.0 subset:
# ``minimum`` / ``maximum`` / ``$defs`` are rejected. We lean on the
# prompt to enforce the [0, 1] range on ``confidence``; downstream code
# clamps it defensively.
EXTRACTION_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "category": {
            "type": "string",
            "enum": [
                "maintenance",
                "lease",
                "payment",
                "complaint",
                "compliance",
                "tenant_change",
                "owner_communication",
                "other",
            ],
        },
        "priority": {"type": "string", "enum": ["low", "medium", "high", "urgent"]},
        "facts_to_update": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "section": {
                        "type": "string",
                        "enum": [
                            "overview",
                            "tenants",
                            "lease",
                            "maintenance",
                            "financials",
                            "compliance",
                            "activity",
                            "patterns",
                            "building_financials",
                            "building_maintenance",
                            "building_compliance",
                            "liegenschaft_financials",
                            "liegenschaft_maintenance",
                            "liegenschaft_compliance",
                        ],
                    },
                    "field": {"type": "string"},
                    "value": {"type": "string"},
                    "confidence": {"type": "number"},
                },
                "required": ["section", "field", "value", "confidence"],
            },
        },
        "uncertain": {
            "type": "array",
            "description": (
                "Items the extractor noticed but cannot commit to as a fact. "
                "Vague mentions, ambiguous references, hearsay, second-hand "
                "claims. The user prefers 'I noticed something unclear' over "
                "'I think this is true.'"
            ),
            "items": {
                "type": "object",
                "properties": {
                    "observation": {
                        "type": "string",
                        "description": "What was noticed, quoted or near-quoted from the source.",
                    },
                    "hypothesis": {
                        "type": "string",
                        "description": (
                            "Optional candidate value if there is one; leave "
                            "empty when only the observation matters."
                        ),
                    },
                    "reason_uncertain": {
                        "type": "string",
                        "description": (
                            "Why this didn't become a fact "
                            "(vague, hearsay, ambiguous reference, ...)."
                        ),
                    },
                    "relevant_section": {
                        "type": "string",
                        "description": (
                            "Best guess at the section a future fact would land "
                            "in (overview, tenants, lease, maintenance, financials, "
                            "compliance, building_*, liegenschaft_*)."
                        ),
                    },
                    "relevant_field": {
                        "type": "string",
                        "description": "Optional best guess at the field name.",
                    },
                },
                "required": ["observation", "reason_uncertain", "relevant_section"],
            },
        },
        "summary": {"type": "string"},
    },
    "required": ["category", "priority", "facts_to_update", "summary"],
}

_PROMPT_DIR = Path(__file__).resolve().parents[1] / "prompts"
_VOCABULARY_PATH = _PROMPT_DIR / "field_vocabulary.json"


@lru_cache(maxsize=4)
def _load_prompt_template(lang: str) -> str:
    """Read the per-language Markdown prompt with file-system caching."""
    name = "extraction_de.md" if lang == "de" else "extraction_en.md"
    path = _PROMPT_DIR / name
    if not path.is_file():
        raise FileNotFoundError(f"missing prompt template: {path}")
    return path.read_text(encoding="utf-8")


@lru_cache(maxsize=1)
def _vocabulary_block() -> str:
    """Render the JSON vocabulary into a markdown block for the prompts.

    Result example::

        - **maintenance** (4 fields)
          - `open_water_damage` — e.g. "Wasserschaden + Schimmel WE 32 …"
          - `key_lost` — e.g. "Mieter WE 02 hat Wohnungsschlüssel verloren …"
          - `defective_window` — e.g. "Küchenfenster WE 50 schließt nicht mehr …"
        - **financials** (3 fields)
          - …
    """
    if not _VOCABULARY_PATH.is_file():
        return "_(no vocabulary file present — emit conservative field names)_"
    payload = json.loads(_VOCABULARY_PATH.read_text(encoding="utf-8"))
    lines: list[str] = []
    for section, body in payload.get("sections", {}).items():
        fields = body.get("fields", [])
        if not fields:
            lines.append(f"- **{section}** (0 fields — section reserved)")
            continue
        lines.append(f"- **{section}** ({len(fields)} fields)")
        for f in fields:
            example = ""
            if f.get("examples"):
                example = f' — e.g. "{f["examples"][0]}"'
            lines.append(f"  - `{f['name']}`{example}")
    return "\n".join(lines)


def render_extraction_prompt(
    *,
    property_name: str,
    current_context_excerpt: str,
    source: str,
    raw_content: str,
    lang: str,
) -> str:
    """Fill the per-language extraction template with event and context."""
    template = _load_prompt_template(lang)
    return template.format(
        property_name=property_name,
        current_context_excerpt=current_context_excerpt or "(no prior context)",
        source=source,
        raw_content=raw_content,
        vocabulary_block=_vocabulary_block(),
    )


@dataclass
class ExtractionResult:
    """Structured output of one extraction, from any provider or the rules.

    ``source`` names what produced it (``gemini``, ``pioneer`` or ``rule``).
    Token counts let callers such as ``eval.runner`` attribute spend; they are
    ``None`` when no model was called.
    """

    category: str
    priority: str
    facts_to_update: list[dict[str, Any]]
    summary: str
    # Items noticed but not committed to as facts. Each entry:
    # {observation, hypothesis?, reason_uncertain, relevant_section,
    # relevant_field?}. Empty when nothing needed flagging.
    uncertain: list[dict[str, Any]] = field(default_factory=list)
    raw: dict[str, Any] = field(default_factory=dict)
    latency_ms: float = 0.0
    model: str = ""
    source: str = "rule"
    prompt_tokens: int | None = None
    completion_tokens: int | None = None
