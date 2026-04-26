"""Property-scoped Q&A service.

Powers ``POST /properties/{id}/ask``. Per Phase 9 trust-layer rules,
the answer must say "I don't know" when the context doesn't support a
confident answer — confabulation is the failure mode this whole
project pushes back against.

The LLM picks one of three statuses:

- ``answered`` — context contains the answer; ``cited_event_ids``
  identifies the supporting events.
- ``insufficient_context`` — related context exists but doesn't answer
  the question; ``partial_context_event_ids`` lists what we do know.
- ``out_of_scope`` — the question is about data Keystone doesn't
  track (ERP-side: exact rent payment dates, IBAN, tax IDs, …).

The API layer maps the cited IDs back to full source records so the
frontend always has ``{id, channel, date, snippet}`` shaped citations.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import Any

import httpx
import structlog

from backend.config import get_settings
from backend.services import pioneer_llm

log = structlog.get_logger(__name__)


@dataclass
class _EventRef:
    id: str
    source: str
    received_at: str
    snippet: str


SYSTEM_HINT = (
    "You answer questions about a German Hausverwaltung property using "
    "only the facts and events provided. Honesty over completeness — "
    "if the context doesn't support a confident answer, say so. "
    "Respond with a single JSON object that strictly matches the schema. "
    "No prose, no markdown fences — only the JSON object. "
    "Inside text fields (answer, reasoning), use plain prose only — no "
    "markdown formatting (**, *, _, `, #), no headings, no bullet "
    "points, no markdown links. Text will be displayed as plain text in "
    "the UI; markdown markers leak through and look broken to the user."
)


PROMPT_TEMPLATE = """You answer questions about property "{property_name}".

QUESTION
\"\"\"
{question}
\"\"\"

PROPERTY FILE (the canonical, condensed markdown record for this
property — Stammdaten at top, then sectioned facts with confidence and
source links, then Building / WEG context. This is your primary
source: everything Keystone knows about this property is here.)
\"\"\"
{property_file}
\"\"\"

RECENT EVENTS (last 90 days, newest first — raw snippets to support
direct quotation. The event ids match the [source: ...] links in the
PROPERTY FILE above. Cite these ids in cited_event_ids.)
{events_block}

DECIDE ON A STATUS
- "answered": the facts/events above contain a concrete answer. List
  the event ids you used in ``cited_event_ids``. ``answer`` is the
  reply text. ``confidence`` is ``high`` | ``medium`` | ``low`` based
  on how directly the context supports the answer.
- "insufficient_context": some related context exists but does NOT
  answer the question. ``answer`` MUST be null. Put the most relevant
  event ids in ``partial_context_event_ids`` so the user sees what you
  did find. Set ``reasoning`` to one sentence explaining why the
  context falls short.
- "out_of_scope": the question is about data Keystone doesn't track
  in this layer — exact rent payment dates, IBAN, bank balances, tax
  IDs, full lease termination logic. ``answer`` MUST be null.
  ``reasoning`` names what would be needed (e.g. "lives in the ERP").

LANGUAGE — STRICT MATCH
Detect the language of the QUESTION above. Then write BOTH ``answer``
and ``reasoning`` in that exact language.
- If the question is in German, both fields are in German. Quote
  German source text verbatim where helpful (names, addresses, "WE",
  "Kaltmiete").
- If the question is in English, both fields are in English. You may
  quote short German proper nouns verbatim, but the surrounding prose
  is English.
- Never mix languages mid-sentence beyond proper-noun quotes. Do not
  translate proper nouns, addresses, or German legal terms when the
  question is in German.

JSON SCHEMA
{{
  "status": "answered" | "insufficient_context" | "out_of_scope",
  "answer": "<text>" | null,
  "confidence": "high" | "medium" | "low" | "insufficient context",
  "reasoning": "<text>",
  "cited_event_ids": ["<id from events list above>"],
  "partial_context_event_ids": ["<id from events list above>"]
}}"""




def _format_events_block(events: list[_EventRef]) -> str:
    if not events:
        return "  (no recent events)"
    lines: list[str] = []
    for e in events[:12]:
        snippet = e.snippet.replace("\n", " ")[:200]
        lines.append(f"  - id={e.id} source={e.source} at={e.received_at}: {snippet}")
    return "\n".join(lines)


async def answer_question(
    *,
    property_name: str,
    question: str,
    property_file_markdown: str,
    recent_events: list[dict[str, Any]],
    model_name: str | None = None,
    timeout_s: float = 30.0,
) -> dict[str, Any]:
    """Ask Claude (via Pioneer) a property-scoped question.

    The model reads the canonical rendered markdown for the property
    (stammdaten + sectioned facts + uncertainties + parent-tier
    context) plus a small block of raw recent-event snippets that
    enable direct quotation. Whatever the markdown contains is
    answerable; whatever it doesn't, the model says "I don't know".

    Returns parsed JSON dict with status/answer/confidence/reasoning
    plus the cited / partial-context event-id lists. The API layer
    joins those IDs back to event records.

    Raises :class:`pioneer_llm.PioneerUnavailable` when Pioneer can't
    be reached or every attempt fails.
    """
    settings = get_settings()
    if not pioneer_llm.is_available():
        raise pioneer_llm.PioneerUnavailable("PIONEER_API_KEY not set")

    events = [_EventRef(**e) for e in recent_events]

    prompt = PROMPT_TEMPLATE.format(
        property_name=property_name,
        question=question.strip() or "(empty question)",
        property_file=property_file_markdown.strip() or "(empty property file)",
        events_block=_format_events_block(events),
    )

    model = model_name or pioneer_llm.PIONEER_DEFAULT_MODEL
    headers = {
        "Authorization": f"Bearer {settings.pioneer_api_key}",
        "Content-Type": "application/json",
    }
    body: dict[str, Any] = {
        "model": model,
        "messages": [
            {"role": "system", "content": SYSTEM_HINT},
            {"role": "user", "content": prompt},
        ],
        "temperature": 0.1,
        "response_format": {"type": "json_object"},
    }

    log.info(
        "ask.start",
        model=model,
        property=property_name,
        prompt_chars=len(prompt),
        property_file_chars=len(property_file_markdown),
        event_count=len(events),
    )

    start = time.perf_counter()
    try:
        async with httpx.AsyncClient(timeout=timeout_s) as client:
            response = await client.post(
                f"{pioneer_llm.PIONEER_BASE_URL}/chat/completions",
                headers=headers,
                json=body,
            )
        response.raise_for_status()
        payload = response.json()
        choice = payload["choices"][0]
        text = choice["message"].get("content") or ""
        data = pioneer_llm._parse_json_payload(text)
    except httpx.HTTPError as exc:
        latency_ms = (time.perf_counter() - start) * 1000
        log.warning(
            "ask.error",
            model=model,
            latency_ms=round(latency_ms, 1),
            error=str(exc),
        )
        raise pioneer_llm.PioneerUnavailable(f"ask failed: {exc!r}") from exc

    latency_ms = (time.perf_counter() - start) * 1000
    usage = payload.get("usage") or {}

    status = str(data.get("status") or "").strip()
    if status not in {"answered", "insufficient_context", "out_of_scope"}:
        # Treat unknown statuses as insufficient_context — safer than
        # surfacing a possibly hallucinated answer.
        status = "insufficient_context"

    log.info(
        "ask.ok",
        model=model,
        latency_ms=round(latency_ms, 1),
        prompt_tokens=usage.get("prompt_tokens"),
        completion_tokens=usage.get("completion_tokens"),
        status=status,
        cited=len(data.get("cited_event_ids") or []),
        partial=len(data.get("partial_context_event_ids") or []),
    )

    return {
        "status": status,
        "answer": data.get("answer"),
        "confidence": str(data.get("confidence") or "").strip()
        or ("high" if status == "answered" else "insufficient context"),
        "reasoning": str(data.get("reasoning") or ""),
        "cited_event_ids": [str(x) for x in (data.get("cited_event_ids") or [])],
        "partial_context_event_ids": [
            str(x) for x in (data.get("partial_context_event_ids") or [])
        ],
        "model": model,
        "latency_ms": latency_ms,
    }
