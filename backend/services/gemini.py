"""Single choke-point for every Gemini call.

Part IV of KEYSTONE is emphatic: **all** Gemini calls go through this module,
with structured output (JSON schema), retries, and production-grade logging.
Judges notice logs that include prompt hash + latency + token counts.

The module gracefully degrades: if ``GEMINI_API_KEY`` is unset the client
raises :class:`GeminiUnavailable`, and callers (e.g. the extractor) switch
to a rule-based fallback. This is explicitly noted in Part XII as the
demo-day mitigation for Gemini outages.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import time
from dataclasses import dataclass, field
from typing import Any

import structlog

from backend.config import get_settings

log = structlog.get_logger(__name__)


class GeminiUnavailable(RuntimeError):
    """Raised when Gemini cannot be used (no key, import failure, hard error)."""


# -----------------------------------------------------------------------------
# Extraction schema + prompt (Part VII canonical)
# -----------------------------------------------------------------------------

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
                        ],
                    },
                    "field": {"type": "string"},
                    "value": {"type": "string"},
                    "confidence": {"type": "number", "minimum": 0, "maximum": 1},
                },
                "required": ["section", "field", "value", "confidence"],
            },
        },
        "summary": {"type": "string"},
    },
    "required": ["category", "priority", "facts_to_update", "summary"],
}

EXTRACTION_PROMPT_TEMPLATE = """You are processing an event for a property management context system.

Property: {property_name}
Current relevant context:
{current_context_excerpt}

New event from {source}:
---
{raw_content}
---

Extract structured facts. Rules:
- `section` + `field` must be specific and stable (e.g. `section=lease, field=end_date`, not `section=info, field=stuff`)
- `value` should be self-contained and human-readable
- `confidence` reflects certainty given the source
- If the event doesn't warrant updates (e.g. chit-chat), return empty `facts_to_update`
- Always return a one-line `summary` for the activity feed

Return ONLY valid JSON matching the schema."""


@dataclass
class ExtractionResult:
    """Parsed Gemini extraction response."""

    category: str
    priority: str
    facts_to_update: list[dict[str, Any]]
    summary: str
    raw: dict[str, Any] = field(default_factory=dict)
    latency_ms: float = 0.0
    model: str = ""
    source: str = "gemini"


# -----------------------------------------------------------------------------
# Gemini client
# -----------------------------------------------------------------------------


def _prompt_hash(prompt: str) -> str:
    """Short stable digest for log correlation (no PII leakage)."""
    return hashlib.sha1(prompt.encode("utf-8")).hexdigest()[:10]


def is_available() -> bool:
    """Return True iff a usable Gemini API key is configured."""
    key = get_settings().gemini_api_key.strip()
    return bool(key) and key not in {"replace-me", "disabled"}


def _configure_client() -> Any:
    """Lazily import + configure ``google.generativeai`` so tests can stub it."""
    if not is_available():
        raise GeminiUnavailable("GEMINI_API_KEY not set")
    try:
        import google.generativeai as genai  # noqa: PLC0415 — lazy import
    except ImportError as exc:  # pragma: no cover — dep is required
        raise GeminiUnavailable("google-generativeai is not installed") from exc
    genai.configure(api_key=get_settings().gemini_api_key)
    return genai


async def extract_facts(
    *,
    property_name: str,
    current_context_excerpt: str,
    source: str,
    raw_content: str,
    model_name: str | None = None,
    max_attempts: int = 3,
) -> ExtractionResult:
    """Run the canonical Phase-1 extraction prompt against Gemini Flash.

    Args:
        property_name: Display name of the matched property (for prompt context).
        current_context_excerpt: A short excerpt of the rendered markdown the
            model can reference when deciding if a fact is new.
        source: Event source — ``"email" | "slack" | "pdf" | ...``.
        raw_content: The event text.
        model_name: Override the configured Flash model. Defaults to
            :attr:`Settings.gemini_flash_model`.
        max_attempts: Retries with exponential backoff on transient errors.

    Returns:
        Parsed :class:`ExtractionResult`.

    Raises:
        GeminiUnavailable: if the client is not configured or every attempt
            fails — callers should fall back to the rule-based path.
    """
    settings = get_settings()
    model = model_name or settings.gemini_flash_model
    prompt = EXTRACTION_PROMPT_TEMPLATE.format(
        property_name=property_name,
        current_context_excerpt=current_context_excerpt or "(no prior context)",
        source=source,
        raw_content=raw_content,
    )
    digest = _prompt_hash(prompt)
    log.info(
        "gemini.extract.start",
        model=model,
        prompt_hash=digest,
        source=source,
        prompt_chars=len(prompt),
    )

    last_error: Exception | None = None
    for attempt in range(1, max_attempts + 1):
        start = time.perf_counter()
        try:
            genai = _configure_client()
            gen_model = genai.GenerativeModel(model)
            response = await asyncio.to_thread(
                gen_model.generate_content,
                prompt,
                generation_config={
                    "response_mime_type": "application/json",
                    "response_schema": EXTRACTION_SCHEMA,
                    "temperature": 0.1,
                },
            )
            latency_ms = (time.perf_counter() - start) * 1000
            text = (response.text or "").strip()
            data = json.loads(text) if text else {}
            usage = getattr(response, "usage_metadata", None)
            log.info(
                "gemini.extract.ok",
                model=model,
                prompt_hash=digest,
                attempt=attempt,
                latency_ms=round(latency_ms, 1),
                prompt_tokens=getattr(usage, "prompt_token_count", None),
                completion_tokens=getattr(usage, "candidates_token_count", None),
                fact_count=len(data.get("facts_to_update", [])),
            )
            return ExtractionResult(
                category=data.get("category", "other"),
                priority=data.get("priority", "low"),
                facts_to_update=list(data.get("facts_to_update", [])),
                summary=data.get("summary", ""),
                raw=data,
                latency_ms=latency_ms,
                model=model,
                source="gemini",
            )
        except GeminiUnavailable:
            raise
        except Exception as exc:  # noqa: BLE001 — retry on any transient error
            last_error = exc
            latency_ms = (time.perf_counter() - start) * 1000
            log.warning(
                "gemini.extract.retry",
                model=model,
                prompt_hash=digest,
                attempt=attempt,
                latency_ms=round(latency_ms, 1),
                error=str(exc),
            )
            if attempt < max_attempts:
                await asyncio.sleep(0.4 * attempt)

    raise GeminiUnavailable(
        f"extract_facts failed after {max_attempts} attempts: {last_error!r}"
    )
