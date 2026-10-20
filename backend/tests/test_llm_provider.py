"""LLM provider selection, the extractor's use of it, and the shared retry helper."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from backend.config import get_settings
from backend.pipeline import extractor
from backend.services import llm
from backend.services.llm.base import (
    LLMNotConfiguredError,
    LLMUnavailableError,
    parse_json_payload,
    with_retries,
)
from backend.services.llm.extraction import ExtractionResult
from backend.services.llm.gemini import GeminiProvider
from backend.services.llm.pioneer import PioneerProvider


def _configure(monkeypatch: pytest.MonkeyPatch, **env: str) -> None:
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    get_settings.cache_clear()


def test_no_provider_without_a_key() -> None:
    assert llm.get_provider() is None
    with pytest.raises(LLMNotConfiguredError, match="GEMINI_API_KEY"):
        llm.require_provider()


def test_gemini_is_the_default_provider(monkeypatch: pytest.MonkeyPatch) -> None:
    _configure(monkeypatch, GEMINI_API_KEY="test-key")

    provider = llm.get_provider()

    assert isinstance(provider, GeminiProvider)
    assert provider.model == get_settings().gemini_pro_model


def test_llm_provider_selects_pioneer(monkeypatch: pytest.MonkeyPatch) -> None:
    _configure(monkeypatch, LLM_PROVIDER="pioneer", PIONEER_API_KEY="test-key")

    assert isinstance(llm.get_provider(), PioneerProvider)


def test_no_silent_fallback_to_the_other_provider(monkeypatch: pytest.MonkeyPatch) -> None:
    _configure(monkeypatch, LLM_PROVIDER="pioneer", GEMINI_API_KEY="test-key")

    assert llm.get_provider() is None
    assert "PIONEER_API_KEY" in llm.not_configured_message()


class _FakeProvider:
    name = "fake"
    model = "fake-1"

    def __init__(self, fail: bool = False) -> None:
        self.fail = fail

    async def extract_facts(self, **_: Any) -> ExtractionResult:
        if self.fail:
            raise LLMUnavailableError("provider down")
        return ExtractionResult(
            category="maintenance",
            priority="high",
            facts_to_update=[
                {"section": "maintenance", "field": "x", "value": "y", "confidence": 0.95}
            ],
            summary="from provider",
            source=self.name,
        )


async def test_extractor_uses_the_configured_provider(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(extractor, "get_provider", lambda: _FakeProvider())

    result = await extractor.extract(
        property_name="P", current_context_excerpt="", source="email", raw_content="Heizung kalt"
    )

    assert result.source == "fake"
    assert result.summary == "from provider"


async def test_extractor_falls_back_to_rules_when_provider_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(extractor, "get_provider", lambda: _FakeProvider(fail=True))

    result = await extractor.extract(
        property_name="P",
        current_context_excerpt="",
        source="email",
        raw_content="Subject: Heizung\n\nDie Heizung ist kalt.",
    )

    assert result.source == "rule"


async def test_with_retries_turns_a_hang_into_unavailable() -> None:
    calls = 0

    async def hang() -> str:
        nonlocal calls
        calls += 1
        await asyncio.sleep(10)
        return "never"

    with pytest.raises(LLMUnavailableError, match="2 attempt"):
        await with_retries(hang, max_attempts=2, timeout_s=0.01, event="test.call")
    assert calls == 2


async def test_with_retries_does_not_retry_missing_configuration() -> None:
    calls = 0

    async def unconfigured() -> str:
        nonlocal calls
        calls += 1
        raise LLMNotConfiguredError("no key")

    with pytest.raises(LLMNotConfiguredError):
        await with_retries(unconfigured, max_attempts=3, timeout_s=1, event="test.call")
    assert calls == 1


@pytest.mark.parametrize(
    "text",
    ['{"a": 1}', '```json\n{"a": 1}\n```', 'Sure, here it is: {"a": 1} Thanks.'],
)
def test_parse_json_payload_accepts_common_wrappings(text: str) -> None:
    assert parse_json_payload(text) == {"a": 1}


def test_parse_json_payload_rejects_non_objects() -> None:
    with pytest.raises(ValueError):
        parse_json_payload("[1, 2]")
