"""Google Gemini provider.

Uses the ``google-generativeai`` SDK. Its ``generate_content`` call is
blocking, so every call runs in a worker thread under the shared retry helper's
timeout; a hung request becomes a retryable timeout rather than a frozen
event loop. Extraction passes ``EXTRACTION_SCHEMA`` as ``response_schema`` so
Gemini returns schema-conformant JSON natively.

Model: ``GEMINI_PRO_MODEL`` for every call. ``GEMINI_FLASH_MODEL`` is used only
by the PDF document-type classifier (``connectors/document_type.py``), which
calls :func:`configure_client` directly.
"""

from __future__ import annotations

import asyncio
import json
import time
from typing import Any

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

#: Per-attempt deadline for extraction: above the 37 s p99 observed during the
#: email backfill, low enough that a silent hang fails one attempt quickly.
EXTRACT_TIMEOUT_S = 60.0
EXTRACT_ATTEMPTS = 3


def is_configured() -> bool:
    """True when a usable ``GEMINI_API_KEY`` is set."""
    key = get_settings().gemini_api_key.strip()
    return bool(key) and key not in {"replace-me", "disabled"}


def configure_client() -> Any:
    """Import and configure ``google.generativeai``; raise if no key is set."""
    if not is_configured():
        raise LLMNotConfiguredError("GEMINI_API_KEY is not set")
    import google.generativeai as genai  # noqa: PLC0415 - optional at import time

    genai.configure(api_key=get_settings().gemini_api_key)  # type: ignore[attr-defined]
    return genai


def _usage(response: Any) -> tuple[int | None, int | None]:
    usage = getattr(response, "usage_metadata", None)
    return (
        getattr(usage, "prompt_token_count", None),
        getattr(usage, "candidates_token_count", None),
    )


class GeminiProvider:
    """:class:`~backend.services.llm.base.LLMProvider` backed by Gemini."""

    name = "gemini"

    def __init__(self, model: str | None = None) -> None:
        self.model = model or get_settings().gemini_pro_model

    def is_configured(self) -> bool:
        return is_configured()

    async def _generate(
        self,
        prompt: str,
        *,
        system: str | None,
        generation_config: dict[str, Any],
    ) -> Any:
        genai = configure_client()
        kwargs: dict[str, Any] = {"system_instruction": system} if system else {}
        model = genai.GenerativeModel(self.model, **kwargs)
        return await asyncio.to_thread(
            model.generate_content, prompt, generation_config=generation_config
        )

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
        digest = prompt_hash(prompt)
        log.info("gemini.extract.start", model=self.model, prompt_hash=digest, lang=lang)

        async def call() -> ExtractionResult:
            start = time.perf_counter()
            response = await self._generate(
                prompt,
                system=None,
                generation_config={
                    "response_mime_type": "application/json",
                    "response_schema": EXTRACTION_SCHEMA,
                    "temperature": 0.1,
                },
            )
            latency_ms = (time.perf_counter() - start) * 1000
            text = (response.text or "").strip()
            data = json.loads(text) if text else {}
            prompt_tokens, completion_tokens = _usage(response)
            log.info(
                "gemini.extract.ok",
                model=self.model,
                prompt_hash=digest,
                latency_ms=round(latency_ms, 1),
                prompt_tokens=prompt_tokens,
                completion_tokens=completion_tokens,
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
                prompt_tokens=prompt_tokens,
                completion_tokens=completion_tokens,
            )

        return await with_retries(
            call,
            max_attempts=EXTRACT_ATTEMPTS,
            timeout_s=EXTRACT_TIMEOUT_S,
            event="gemini.extract",
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
        digest = prompt_hash(prompt)

        async def call() -> JsonCompletion:
            start = time.perf_counter()
            response = await self._generate(
                prompt,
                system=system,
                generation_config={
                    "response_mime_type": "application/json",
                    "temperature": temperature,
                },
            )
            prompt_tokens, completion_tokens = _usage(response)
            return JsonCompletion(
                data=parse_json_payload(response.text or ""),
                model=self.model,
                latency_ms=(time.perf_counter() - start) * 1000,
                prompt_tokens=prompt_tokens,
                completion_tokens=completion_tokens,
            )

        return await with_retries(
            call,
            max_attempts=max_attempts,
            timeout_s=timeout_s,
            event="gemini.complete_json",
            model=self.model,
            prompt_hash=digest,
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
        digest = prompt_hash(prompt)

        async def call() -> str:
            response = await self._generate(
                prompt, system=system, generation_config={"temperature": temperature}
            )
            text = (response.text or "").strip()
            if not text:
                raise ValueError("empty response")
            return text

        return await with_retries(
            call,
            max_attempts=max_attempts,
            timeout_s=timeout_s,
            event="gemini.complete_text",
            model=self.model,
            prompt_hash=digest,
        )
