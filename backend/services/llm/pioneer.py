"""Pioneer provider: an OpenAI-compatible chat-completions gateway.

Calls ``POST {PIONEER_BASE_URL}/chat/completions`` over ``httpx`` with
``PIONEER_API_KEY``; the model is ``PIONEER_MODEL`` (default
``claude-sonnet-4-6``). The gateway has no native schema enforcement, so
extraction embeds ``EXTRACTION_SCHEMA`` in the user message, requests
``response_format=json_object``, and parses defensively.
"""

from __future__ import annotations

import json
import time
from typing import Any

import httpx
import structlog

from backend.config import get_settings
from backend.services.llm.base import (
    JsonCompletion,
    LLMNotConfiguredError,
    parse_json_payload,
    prompt_hash,
    with_retries,
)
from backend.services.llm.extraction import (
    EXTRACTION_SCHEMA,
    ExtractionResult,
    render_extraction_prompt,
)

log = structlog.get_logger(__name__)

PIONEER_BASE_URL = "https://api.pioneer.ai/v1"
EXTRACT_TIMEOUT_S = 60.0
EXTRACT_ATTEMPTS = 3

_EXTRACTION_SYSTEM = (
    "You are a structured-extraction worker for a property-management "
    "platform. Respond with a single JSON object that strictly matches "
    "the schema described in the user message. No prose, no markdown "
    "fences, only the JSON object."
)


def is_configured() -> bool:
    """True when a usable ``PIONEER_API_KEY`` is set."""
    key = get_settings().pioneer_api_key.strip()
    return bool(key) and key not in {"replace-me", "disabled"}


class PioneerProvider:
    """:class:`~backend.services.llm.base.LLMProvider` backed by the Pioneer gateway."""

    name = "pioneer"

    def __init__(self, model: str | None = None) -> None:
        self.model = model or get_settings().pioneer_model

    def is_configured(self) -> bool:
        return is_configured()

    async def _chat(
        self,
        *,
        system: str | None,
        prompt: str,
        temperature: float,
        json_mode: bool,
    ) -> tuple[str, dict[str, Any], float]:
        """One chat-completions request; returns (content, usage, latency_ms)."""
        if not is_configured():
            raise LLMNotConfiguredError("PIONEER_API_KEY is not set")
        messages = [{"role": "system", "content": system}] if system else []
        messages.append({"role": "user", "content": prompt})
        body: dict[str, Any] = {
            "model": self.model,
            "messages": messages,
            "temperature": temperature,
        }
        if json_mode:
            body["response_format"] = {"type": "json_object"}
        start = time.perf_counter()
        async with httpx.AsyncClient(timeout=None) as client:
            response = await client.post(
                f"{PIONEER_BASE_URL}/chat/completions",
                headers={"Authorization": f"Bearer {get_settings().pioneer_api_key}"},
                json=body,
            )
        response.raise_for_status()
        payload = response.json()
        content = payload["choices"][0]["message"].get("content") or ""
        return content, payload.get("usage") or {}, (time.perf_counter() - start) * 1000

    async def extract_facts(
        self,
        *,
        property_name: str,
        current_context_excerpt: str,
        source: str,
        raw_content: str,
        lang: str,
    ) -> ExtractionResult:
        prompt = render_extraction_prompt(
            property_name=property_name,
            current_context_excerpt=current_context_excerpt,
            source=source,
            raw_content=raw_content,
            lang=lang,
        )
        user_message = (
            prompt + "\n\nReturn one JSON object matching this schema (top-level keys: "
            "category, priority, facts_to_update[], uncertain[], summary):\n"
            + json.dumps(EXTRACTION_SCHEMA, indent=2)
        )
        digest = prompt_hash(prompt)
        log.info("pioneer.extract.start", model=self.model, prompt_hash=digest, lang=lang)

        async def call() -> ExtractionResult:
            content, usage, latency_ms = await self._chat(
                system=_EXTRACTION_SYSTEM, prompt=user_message, temperature=0.1, json_mode=True
            )
            data = parse_json_payload(content)
            log.info(
                "pioneer.extract.ok",
                model=self.model,
                prompt_hash=digest,
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
                model=self.model,
                source=self.name,
                prompt_tokens=usage.get("prompt_tokens"),
                completion_tokens=usage.get("completion_tokens"),
            )

        return await with_retries(
            call,
            max_attempts=EXTRACT_ATTEMPTS,
            timeout_s=EXTRACT_TIMEOUT_S,
            event="pioneer.extract",
            model=self.model,
            prompt_hash=digest,
        )

    async def complete_json(
        self,
        *,
        system: str,
        prompt: str,
        temperature: float = 0.1,
        timeout_s: float = 30.0,
        max_attempts: int = 1,
    ) -> JsonCompletion:
        async def call() -> JsonCompletion:
            content, usage, latency_ms = await self._chat(
                system=system, prompt=prompt, temperature=temperature, json_mode=True
            )
            return JsonCompletion(
                data=parse_json_payload(content),
                model=self.model,
                latency_ms=latency_ms,
                prompt_tokens=usage.get("prompt_tokens"),
                completion_tokens=usage.get("completion_tokens"),
            )

        return await with_retries(
            call,
            max_attempts=max_attempts,
            timeout_s=timeout_s,
            event="pioneer.complete_json",
            model=self.model,
            prompt_hash=prompt_hash(prompt),
        )

    async def complete_text(
        self,
        *,
        system: str | None,
        prompt: str,
        temperature: float = 0.2,
        timeout_s: float = 45.0,
        max_attempts: int = 2,
    ) -> str:
        async def call() -> str:
            content, _usage, _latency = await self._chat(
                system=system, prompt=prompt, temperature=temperature, json_mode=False
            )
            text = content.strip()
            if not text:
                raise ValueError("empty response")
            return text

        return await with_retries(
            call,
            max_attempts=max_attempts,
            timeout_s=timeout_s,
            event="pioneer.complete_text",
            model=self.model,
            prompt_hash=prompt_hash(prompt),
        )
