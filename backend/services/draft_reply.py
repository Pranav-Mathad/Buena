"""Inbound-reply drafting service.

Powers ``POST /draft-reply``. Distinct from the signal drafter
(``backend.signals.drafter``), which drafts *outbound* signal messages:
this module drafts replies to *inbound* tenant/owner messages, citing
prior facts and events as context.

The endpoint contract matches the existing ``DraftReply`` page in the
frontend:

- input: ``{property_id?, property_name?, channel, tone, subject?, body}``
- output: ``{subject, body, context[], knows_about_incident, elapsed_ms}``

The LLM call routes through Pioneer (Claude) by default — Phase 11
parked extraction on Pioneer while Gemini Pro quota is exhausted, and
keeping the drafter on the same backbone avoids two LLM dependencies.
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
class _Fact:
    section: str
    field: str
    value: str
    date: str


@dataclass
class _EventRef:
    id: str
    source: str
    received_at: str
    snippet: str


SYSTEM_HINT = (
    "You are drafting a reply on behalf of a German property manager "
    "(Hausverwaltung). Respond with a single JSON object that strictly "
    "matches the schema described in the user message. No prose, no "
    "markdown fences — only the JSON object. Match the language of the "
    "inbound message in the reply body. Inside text fields (subject, "
    "body, why_relevant, snippet), use plain prose only — no markdown "
    "formatting (**, *, _, `, #), no markdown links, no bullet markers. "
    "The reply body will be sent verbatim to a tenant or owner via "
    "email/Slack; markdown markers look broken to the recipient."
)


PROMPT_TEMPLATE = """You draft a reply to the inbound message below.

INBOUND MESSAGE
Channel: {channel}
Subject: {subject}
Body:
\"\"\"
{body}
\"\"\"

DESIRED TONE: {tone}

PROPERTY CONTEXT — {property_name}

Current facts (most recent first):
{facts_block}

Recent events (last 30 days, newest first):
{events_block}

TASK
1. Decide whether anything in the context is materially related to the
   inbound message. Set ``knows_about_incident`` accordingly.
2. If yes, list the relevant items in ``context`` with a one-sentence
   ``why_relevant`` and use them to inform the reply body. Reference
   them concretely (dates, amounts, parties) — no vague gestures.
3. If no, draft a short holding reply ("we will look into this and
   come back within X working days") with an empty ``context`` array.
4. Write the reply body in the same language as the inbound message
   (German if the inbound is German, English if English).
5. ``subject``: when the channel is ``Email``, prefix the inbound
   subject with ``Re:`` (or ``AW:`` for German). When the channel is
   ``Slack``, leave the subject as an empty string.

Respond with one JSON object matching this schema:
{{
  "subject": "string",
  "body": "string",
  "knows_about_incident": true | false,
  "context": [
    {{
      "id": "<event id from the events list above, or empty>",
      "channel": "email|bank|invoice|letter|stammdaten",
      "time": "human-readable relative time (e.g. 'vor 2 Tagen', 'this morning')",
      "snippet": "short quote/excerpt under 140 chars",
      "why_relevant": "one sentence under 100 chars"
    }}
  ]
}}"""


def _format_facts_block(facts: list[_Fact]) -> str:
    if not facts:
        return "  (no facts on file)"
    lines: list[str] = []
    for f in facts[:30]:
        lines.append(f"  - [{f.section}.{f.field}] {f.value} (as of {f.date})")
    return "\n".join(lines)


def _format_events_block(events: list[_EventRef]) -> str:
    if not events:
        return "  (no recent events)"
    lines: list[str] = []
    for e in events[:10]:
        snippet = e.snippet.replace("\n", " ")[:160]
        lines.append(f"  - id={e.id} source={e.source} at={e.received_at}: {snippet}")
    return "\n".join(lines)


async def draft_inbound_reply(
    *,
    property_name: str,
    inbound_channel: str,
    inbound_subject: str,
    inbound_body: str,
    tone: str,
    recent_facts: list[dict[str, Any]],
    recent_events: list[dict[str, Any]],
    model_name: str | None = None,
    timeout_s: float = 30.0,
) -> dict[str, Any]:
    """Run the draft-reply prompt against Claude via Pioneer.

    ``recent_facts`` and ``recent_events`` come from the API layer's
    Postgres queries — this service stays SQL-free so it's easy to
    unit-test with fixture data.

    Returns the parsed JSON dict. Raises
    :class:`pioneer_llm.PioneerUnavailable` when Pioneer cannot be
    used or every attempt failed.
    """
    settings = get_settings()
    if not pioneer_llm.is_available():
        raise pioneer_llm.PioneerUnavailable("PIONEER_API_KEY not set")

    facts = [_Fact(**f) for f in recent_facts]
    events = [_EventRef(**e) for e in recent_events]

    prompt = PROMPT_TEMPLATE.format(
        channel=inbound_channel,
        subject=inbound_subject or "(no subject)",
        body=inbound_body.strip() or "(empty)",
        tone=tone,
        property_name=property_name,
        facts_block=_format_facts_block(facts),
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
        "temperature": 0.2,
        "response_format": {"type": "json_object"},
    }

    log.info(
        "draft_reply.start",
        model=model,
        channel=inbound_channel,
        property=property_name,
        prompt_chars=len(prompt),
        fact_count=len(facts),
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
            "draft_reply.error",
            model=model,
            latency_ms=round(latency_ms, 1),
            error=str(exc),
        )
        raise pioneer_llm.PioneerUnavailable(
            f"draft_reply failed: {exc!r}"
        ) from exc

    latency_ms = (time.perf_counter() - start) * 1000
    usage = payload.get("usage") or {}
    log.info(
        "draft_reply.ok",
        model=model,
        latency_ms=round(latency_ms, 1),
        prompt_tokens=usage.get("prompt_tokens"),
        completion_tokens=usage.get("completion_tokens"),
        knows_about_incident=bool(data.get("knows_about_incident")),
        context_count=len(data.get("context") or []),
    )

    # Defensive shape coercion — Pioneer returns the schema almost
    # always, but we don't want a stray missing field to 500 the API.
    return {
        "subject": str(data.get("subject") or ""),
        "body": str(data.get("body") or ""),
        "knows_about_incident": bool(data.get("knows_about_incident", False)),
        "context": list(data.get("context") or []),
        "model": model,
        "latency_ms": latency_ms,
    }
