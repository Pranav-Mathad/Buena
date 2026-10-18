"""Provider-neutral LLM interface.

Every feature that calls a model (extraction, signal drafting, onboarding
briefings, Ask, reply drafting, the semantic validator) depends on
:class:`LLMProvider`, never on a vendor SDK. A provider offers three calls:

- :meth:`LLMProvider.extract_facts`: the extraction prompt and schema shared by
  all providers (``backend.services.llm.extraction``), returning an
  :class:`~backend.services.llm.extraction.ExtractionResult`.
- :meth:`LLMProvider.complete_json`: a system and user prompt in, one parsed
  JSON object out.
- :meth:`LLMProvider.complete_text`: a prompt in, plain text out.

Failures are reported as :class:`LLMUnavailableError`; :class:`LLMNotConfiguredError` is
the subclass for "no API key for the selected provider". Callers treat either
as a reason to use their deterministic fallback, so a missing or failing
provider degrades features instead of breaking them.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import re
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Protocol, TypeVar, runtime_checkable

import structlog

if TYPE_CHECKING:
    from backend.services.llm.extraction import ExtractionResult

log = structlog.get_logger(__name__)

T = TypeVar("T")

#: Backoff between retry attempts is ``RETRY_BACKOFF_S * attempt``.
RETRY_BACKOFF_S = 0.4


class LLMUnavailableError(RuntimeError):
    """No usable provider, or every attempt failed. Callers should fall back."""


class LLMNotConfiguredError(LLMUnavailableError):
    """The selected provider has no API key configured."""


@dataclass(frozen=True)
class JsonCompletion:
    """A parsed JSON response plus the metadata worth logging."""

    data: dict[str, Any]
    model: str
    latency_ms: float
    prompt_tokens: int | None = None
    completion_tokens: int | None = None


@runtime_checkable
class LLMProvider(Protocol):
    """What every LLM backend implements."""

    #: Short identifier, also written to ``ExtractionResult.source``.
    name: str
    #: Model used for every call.
    model: str

    def is_configured(self) -> bool:
        """True when the provider has the credentials it needs."""
        ...

    async def extract_facts(
        self,
        *,
        property_name: str,
        current_context_excerpt: str,
        source: str,
        raw_content: str,
        lang: str,
    ) -> ExtractionResult:
        """Run the shared extraction prompt (3 attempts, 60 s timeout each)."""
        ...

    async def complete_json(
        self,
        *,
        system: str,
        prompt: str,
        temperature: float = 0.1,
        timeout_s: float = 30.0,
        max_attempts: int = 1,
    ) -> JsonCompletion:
        """Return the model's reply parsed as one JSON object."""
        ...

    async def complete_text(
        self,
        *,
        system: str | None,
        prompt: str,
        temperature: float = 0.2,
        timeout_s: float = 45.0,
        max_attempts: int = 2,
    ) -> str:
        """Return the model's reply as plain text (non-empty)."""
        ...


def prompt_hash(prompt: str) -> str:
    """Short, stable identifier for a prompt, for correlating log lines."""
    return hashlib.sha1(prompt.encode("utf-8")).hexdigest()[:10]


_JSON_FENCE_RE = re.compile(r"```(?:json)?\s*(\{.*?\})\s*```", re.DOTALL)


def parse_json_payload(text: str) -> dict[str, Any]:
    """Parse one JSON object from a model reply.

    Accepts a bare object, an object inside a Markdown code fence, or an object
    surrounded by stray prose, because models occasionally add either even when
    asked for JSON only.
    """
    text = text.strip()
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        fence = _JSON_FENCE_RE.search(text)
        if fence:
            parsed = json.loads(fence.group(1))
        else:
            start, end = text.find("{"), text.rfind("}")
            if start < 0 or end <= start:
                raise ValueError(f"no JSON object in response: {text[:200]!r}") from None
            parsed = json.loads(text[start : end + 1])
    if not isinstance(parsed, dict):
        raise ValueError(f"expected a JSON object, got {type(parsed).__name__}")
    return parsed


async def with_retries(
    call: Callable[[], Awaitable[T]],
    *,
    max_attempts: int,
    timeout_s: float,
    event: str,
    **log_fields: Any,
) -> T:
    """Run ``call`` with a per-attempt timeout and linear backoff.

    Each attempt is bounded by ``asyncio.wait_for``: a provider call that never
    returns (observed with a blocking SDK call) becomes a timeout instead of a
    frozen worker. :class:`LLMNotConfiguredError` is raised immediately; any other
    error is logged and retried. After the last attempt the error is wrapped in
    :class:`LLMUnavailableError`.
    """
    last_error: Exception | None = None
    for attempt in range(1, max_attempts + 1):
        start = time.perf_counter()
        try:
            return await asyncio.wait_for(call(), timeout=timeout_s)
        except LLMNotConfiguredError:
            raise
        except Exception as exc:  # noqa: BLE001 - any provider error is retryable
            last_error = exc
            log.warning(
                f"{event}.retry",
                attempt=attempt,
                latency_ms=round((time.perf_counter() - start) * 1000, 1),
                error=str(exc)[:300],
                **log_fields,
            )
            if attempt < max_attempts:
                await asyncio.sleep(RETRY_BACKOFF_S * attempt)
    raise LLMUnavailableError(f"{event} failed after {max_attempts} attempt(s): {last_error!r}")
