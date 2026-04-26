"""Pioneer LLM gateway — Anthropic Claude via OpenAI-compatible API.

Drop-in alternative to :func:`backend.services.gemini.extract_facts` for
Phase 11 Step 1 vocab-fix verification while Gemini Pro quota is exhausted.

NOT wired into the production extractor (``backend.pipeline.extractor``).
Phase 8 Step 5 pinned Gemini Pro for production extraction; switching the
production path requires a DECISIONS.md entry plus an eval re-run to
confirm precision did not regress.

Naming note: ``backend.services.pioneer`` already exists as the Phase 5
approval-learning layer (a different partner subsystem with the same
sponsor name). This module is the LLM gateway and is intentionally a
separate file so callers ``import`` only what they need.

The extraction schema, prompt template, and ``ExtractionResult`` dataclass
are reused verbatim from :mod:`backend.services.gemini` so callers swap
one import. The OpenAI-compatible Pioneer gateway is reached over plain
HTTPS using ``httpx``; no new dependency needed.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import re
import time
from typing import Any

import httpx
import structlog

from backend.config import get_settings
from backend.services.gemini import (
    EXTRACTION_SCHEMA,
    ExtractionResult,
    render_extraction_prompt,
)

log = structlog.get_logger(__name__)


PIONEER_BASE_URL = "https://api.pioneer.ai/v1"
PIONEER_DEFAULT_MODEL = "claude-sonnet-4-6"
PIONEER_EXTRACT_TIMEOUT_S: float = 60.0


class PioneerUnavailable(RuntimeError):
    """Raised when Pioneer cannot be used (no key, transport error, hard error)."""


def _prompt_hash(prompt: str) -> str:
    return hashlib.sha1(prompt.encode("utf-8")).hexdigest()[:10]


def is_available() -> bool:
    """Return True iff a usable Pioneer API key is configured."""
    key = get_settings().pioneer_api_key.strip()
    return bool(key) and key not in {"replace-me", "disabled"}


_JSON_FENCE_RE = re.compile(r"```(?:json)?\s*(\{.*?\})\s*```", re.DOTALL)


def _parse_json_payload(text: str) -> dict[str, Any]:
    """Best-effort JSON extraction from a model response.

    The OpenAI-compatible gateway honors ``response_format=json_object`` for
    Anthropic models, but a defensive parser keeps the verifier working
    even if the gateway echoes a markdown fence or a leading apology line.
    """
    text = text.strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    fence = _JSON_FENCE_RE.search(text)
    if fence:
        return json.loads(fence.group(1))
    start = text.find("{")
    end = text.rfind("}")
    if start >= 0 and end > start:
        return json.loads(text[start : end + 1])
    raise ValueError(f"could not parse JSON from response (head 200): {text[:200]!r}")


SYSTEM_HINT = (
    "You are a structured-extraction worker for a property-management "
    "platform. Respond with a single JSON object that strictly matches "
    "the schema described in the user message. No prose, no markdown "
    "fences — only the JSON object."
)


async def extract_facts(
    *,
    property_name: str,
    current_context_excerpt: str,
    source: str,
    raw_content: str,
    lang: str = "en",
    model_name: str | None = None,
    max_attempts: int = 3,
    timeout_s: float = PIONEER_EXTRACT_TIMEOUT_S,
) -> ExtractionResult:
    """Run the canonical extraction prompt against Claude via Pioneer.

    Same call shape as :func:`backend.services.gemini.extract_facts` so
    callers can swap one import. Returns an :class:`ExtractionResult` with
    ``source="pioneer"`` — the verifier already routes its cost-ledger
    branch on this field, so Pioneer calls do not charge the Gemini
    ledger.
    """
    settings = get_settings()
    if not is_available():
        raise PioneerUnavailable("PIONEER_API_KEY not set")

    model = model_name or PIONEER_DEFAULT_MODEL
    prompt = render_extraction_prompt(
        property_name=property_name,
        current_context_excerpt=current_context_excerpt,
        source=source,
        raw_content=raw_content,
        lang=lang,
    )
    digest = _prompt_hash(prompt)
    schema_block = (
        "\n\nReturn one JSON object matching this schema "
        "(top-level keys: category, priority, facts_to_update[], "
        "uncertain[], summary):\n"
        + json.dumps(EXTRACTION_SCHEMA, indent=2)
    )
    user_message = prompt + schema_block

    headers = {
        "Authorization": f"Bearer {settings.pioneer_api_key}",
        "Content-Type": "application/json",
    }
    body: dict[str, Any] = {
        "model": model,
        "messages": [
            {"role": "system", "content": SYSTEM_HINT},
            {"role": "user", "content": user_message},
        ],
        "temperature": 0.1,
        "response_format": {"type": "json_object"},
    }

    log.info(
        "pioneer_llm.extract.start",
        model=model,
        prompt_hash=digest,
        source=source,
        lang=lang,
        prompt_chars=len(user_message),
    )

    last_error: Exception | None = None
    for attempt in range(1, max_attempts + 1):
        start = time.perf_counter()
        try:
            async with httpx.AsyncClient(timeout=timeout_s) as client:
                response = await client.post(
                    f"{PIONEER_BASE_URL}/chat/completions",
                    headers=headers,
                    json=body,
                )
            response.raise_for_status()
            payload = response.json()
            latency_ms = (time.perf_counter() - start) * 1000
            choice = payload["choices"][0]
            text = choice["message"].get("content") or ""
            data = _parse_json_payload(text)
            usage = payload.get("usage") or {}
            log.info(
                "pioneer_llm.extract.ok",
                model=model,
                prompt_hash=digest,
                attempt=attempt,
                latency_ms=round(latency_ms, 1),
                prompt_tokens=usage.get("prompt_tokens"),
                completion_tokens=usage.get("completion_tokens"),
                fact_count=len(data.get("facts_to_update", [])),
            )
            return ExtractionResult(
                category=data.get("category", "other"),
                priority=data.get("priority", "low"),
                facts_to_update=list(data.get("facts_to_update", [])),
                uncertain=list(data.get("uncertain", [])),
                summary=data.get("summary", ""),
                raw=data,
                latency_ms=latency_ms,
                model=model,
                source="pioneer",
                prompt_tokens=usage.get("prompt_tokens"),
                completion_tokens=usage.get("completion_tokens"),
            )
        except PioneerUnavailable:
            raise
        except Exception as exc:  # noqa: BLE001 — retry on any transient error
            last_error = exc
            latency_ms = (time.perf_counter() - start) * 1000
            log.warning(
                "pioneer_llm.extract.retry",
                model=model,
                prompt_hash=digest,
                attempt=attempt,
                latency_ms=round(latency_ms, 1),
                error=str(exc),
            )
            if attempt < max_attempts:
                await asyncio.sleep(0.4 * attempt)

    raise PioneerUnavailable(
        f"extract_facts failed after {max_attempts} attempts: {last_error!r}"
    )


# -----------------------------------------------------------------------------
# Onboarding briefing
# -----------------------------------------------------------------------------


_BRIEFING_SYSTEM_HINT = (
    "You are briefing a Hausverwaltung operator who is opening a property "
    "for the first time. Plain prose only — no markdown formatting "
    "(**, *, _, `, #), no headings, no markdown links. Output one bullet "
    "per line, prefix each with '- '. Always write in English regardless "
    "of the source-event language; the operator team reads English."
)


_BRIEFING_PROMPT = """You are briefing a property manager who is reading {property_name} for the first time.

Write 5 to 7 bullets. Each bullet is at most 25 words. The reader has 60 seconds.

HARD RULES
- Honesty over completeness. If a key piece is missing, say so plainly. Use the literal prefix "Gap:" and name what is missing.
- Do not invent dates, amounts, names, or facts. If something is uncertain, treat it as a gap, not as a fact.
- Lead with what is operationally relevant TODAY (open issues, recent changes), then context, then risks to watch.
- Plain prose, no markdown formatting, no preamble like "Here is the briefing".
- One bullet per line, prefix each with "- ".
- Always write in English, even when the underlying facts are in German. You may quote short German phrases (proper nouns, "Kaltmiete", street names) verbatim, but the surrounding prose is English.

CURRENT FACTS (authoritative — already validated)
{facts_summary}

OPEN UNCERTAINTIES (noticed but not committed — call out as gaps)
{uncertainties_summary}

VALIDATOR REJECTIONS (proposed but rejected — note as known conflicts)
{rejections_summary}

RECENT EVENT ACTIVITY (last 12 months, by category)
{activity_summary}

Write the bullets now."""


async def draft_onboarding_briefing(
    *,
    property_name: str,
    facts_summary: str,
    uncertainties_summary: str,
    rejections_summary: str,
    activity_summary: str,
    model_name: str | None = None,
    max_attempts: int = 2,
    timeout_s: float = 45.0,
) -> str:
    """Render the onboarding briefing bullets via Claude (Pioneer).

    Always English regardless of source-event language — the briefing
    is read by the operator, not the tenant. Returns the bullet block
    as plain text (one ``- `` line per bullet, no markdown formatting,
    no surrounding heading).

    Raises :class:`PioneerUnavailable` if every attempt fails so the
    caller can fall back to a deterministic placeholder.
    """
    settings = get_settings()
    if not is_available():
        raise PioneerUnavailable("PIONEER_API_KEY not set")

    model = model_name or PIONEER_DEFAULT_MODEL
    no_facts = "(no facts yet)"
    none_label = "(none)"
    no_activity = "(no recent activity)"
    prompt = _BRIEFING_PROMPT.format(
        property_name=property_name,
        facts_summary=facts_summary or no_facts,
        uncertainties_summary=uncertainties_summary or none_label,
        rejections_summary=rejections_summary or none_label,
        activity_summary=activity_summary or no_activity,
    )
    digest = _prompt_hash(prompt)

    headers = {
        "Authorization": f"Bearer {settings.pioneer_api_key}",
        "Content-Type": "application/json",
    }
    body: dict[str, Any] = {
        "model": model,
        "messages": [
            {"role": "system", "content": _BRIEFING_SYSTEM_HINT},
            {"role": "user", "content": prompt},
        ],
        "temperature": 0.2,
    }

    last_error: Exception | None = None
    for attempt in range(1, max_attempts + 1):
        start = time.perf_counter()
        try:
            log.info(
                "pioneer_llm.briefing.start",
                model=model,
                attempt=attempt,
                prompt_hash=digest,
                prompt_chars=len(prompt),
            )
            async with httpx.AsyncClient(timeout=timeout_s) as client:
                response = await client.post(
                    f"{PIONEER_BASE_URL}/chat/completions",
                    headers=headers,
                    json=body,
                )
            response.raise_for_status()
            payload = response.json()
            text = payload["choices"][0]["message"].get("content") or ""
            text = text.strip()
            if not text:
                raise PioneerUnavailable("empty briefing response")
            latency_ms = (time.perf_counter() - start) * 1000
            log.info(
                "pioneer_llm.briefing.ok",
                model=model,
                latency_ms=round(latency_ms, 1),
                attempt=attempt,
                chars=len(text),
                prompt_hash=digest,
            )
            return text
        except PioneerUnavailable:
            raise
        except Exception as exc:  # noqa: BLE001 — retry on transient errors
            last_error = exc
            latency_ms = (time.perf_counter() - start) * 1000
            log.warning(
                "pioneer_llm.briefing.retry",
                model=model,
                attempt=attempt,
                latency_ms=round(latency_ms, 1),
                error=str(exc)[:200],
                prompt_hash=digest,
            )
            if attempt < max_attempts:
                await asyncio.sleep(0.4 * attempt)

    raise PioneerUnavailable(
        f"draft_onboarding_briefing failed after {max_attempts} attempts: "
        f"{last_error!r}"
    )
