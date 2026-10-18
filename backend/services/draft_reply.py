"""Inbound-reply drafting service.

Powers ``POST /draft-reply``. Distinct from the signal drafter
(``backend.signals.drafter``), which drafts *outbound* signal messages:
this module drafts replies to *inbound* tenant/owner messages, citing
prior facts and events as context.

The endpoint contract matches the existing ``DraftReply`` page in the
frontend:

- input: ``{property_id?, property_name?, channel, tone, subject?, body}``
- output: ``{subject, body, context[], knows_about_incident, elapsed_ms}``

The call goes to the configured LLM provider (:mod:`backend.services.llm`).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import structlog

from backend.services.llm import require_provider

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
    timeout_s: float = 30.0,
) -> dict[str, Any]:
    """Draft a reply to an inbound message with the configured LLM provider.

    ``recent_facts`` and ``recent_events`` come from the API layer's queries;
    this service stays SQL-free so it is easy to test with fixture data.

    Returns the parsed draft. Raises :class:`LLMNotConfiguredError` when no
    provider is configured and :class:`LLMUnavailableError` when the call fails.
    """
    provider = require_provider()
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

    log.info(
        "draft_reply.start",
        provider=provider.name,
        model=provider.model,
        channel=inbound_channel,
        property=property_name,
        prompt_chars=len(prompt),
        fact_count=len(facts),
        event_count=len(events),
    )

    completion = await provider.complete_json(
        system=SYSTEM_HINT, prompt=prompt, temperature=0.2, timeout_s=timeout_s
    )
    data = completion.data
    model = completion.model
    latency_ms = completion.latency_ms
    log.info(
        "draft_reply.ok",
        model=model,
        latency_ms=round(latency_ms, 1),
        prompt_tokens=completion.prompt_tokens,
        completion_tokens=completion.completion_tokens,
        knows_about_incident=bool(data.get("knows_about_incident")),
        context_count=len(data.get("context") or []),
    )

    # Coerce defensively: models return the schema almost always, but a
    # missing field must not turn into a 500.
    return {
        "subject": str(data.get("subject") or ""),
        "body": str(data.get("body") or ""),
        "knows_about_incident": bool(data.get("knows_about_incident", False)),
        "context": list(data.get("context") or []),
        "model": model,
        "latency_ms": latency_ms,
    }
