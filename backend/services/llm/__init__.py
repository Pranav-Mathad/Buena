"""LLM access for every feature, behind one provider interface.

``LLM_PROVIDER`` selects the backend (``gemini``, the default, or
``pioneer``). :func:`get_provider` returns it when its API key is set and
``None`` otherwise; features then use their deterministic fallback (rule-based
extraction, template drafts, a briefing notice) or, for Ask and reply
drafting, answer 503 with :func:`not_configured_message`. There is no silent
cross-provider fallback: the configured provider is the one that runs.
"""

from __future__ import annotations

import structlog

from backend.config import get_settings
from backend.services.llm.base import (
    JsonCompletion,
    LLMNotConfiguredError,
    LLMProvider,
    LLMUnavailableError,
)
from backend.services.llm.extraction import ExtractionResult
from backend.services.llm.gemini import GeminiProvider
from backend.services.llm.pioneer import PioneerProvider

log = structlog.get_logger(__name__)

_PROVIDERS: dict[str, type[GeminiProvider] | type[PioneerProvider]] = {
    "gemini": GeminiProvider,
    "pioneer": PioneerProvider,
}
_KEY_FOR = {"gemini": "GEMINI_API_KEY", "pioneer": "PIONEER_API_KEY"}


def get_provider() -> LLMProvider | None:
    """The configured provider, or ``None`` when its API key is missing."""
    provider = _PROVIDERS[get_settings().llm_provider]()
    return provider if provider.is_configured() else None


def require_provider() -> LLMProvider:
    """Like :func:`get_provider`, but raise :class:`LLMNotConfiguredError` instead of ``None``."""
    provider = get_provider()
    if provider is None:
        raise LLMNotConfiguredError(not_configured_message())
    return provider


def not_configured_message() -> str:
    """Explain which variable is missing, suitable for an API error body."""
    selected = get_settings().llm_provider
    return (
        f"No LLM provider is configured: LLM_PROVIDER={selected} requires "
        f"{_KEY_FOR[selected]}. Set it (or choose another provider with LLM_PROVIDER)."
    )


def log_configuration() -> None:
    """Log the LLM setup once at startup, so missing keys are visible without per-event noise."""
    settings = get_settings()
    provider = get_provider()
    if provider is not None:
        log.info("llm.provider", provider=provider.name, model=provider.model)
        return
    others = [name for name, cls in _PROVIDERS.items() if name != settings.llm_provider]
    configured_others = [name for name in others if _PROVIDERS[name]().is_configured()]
    log.warning(
        "llm.provider.not_configured",
        provider=settings.llm_provider,
        missing=_KEY_FOR[settings.llm_provider],
        effect=(
            "rule-based extraction, template signal drafts, semantic check skipped, "
            "briefing notice; Ask and reply drafting return 503"
        ),
        hint=(
            f"{', '.join(configured_others)} is configured; set LLM_PROVIDER to use it"
            if configured_others
            else None
        ),
    )


__all__ = [
    "ExtractionResult",
    "JsonCompletion",
    "LLMNotConfiguredError",
    "LLMProvider",
    "LLMUnavailableError",
    "get_provider",
    "log_configuration",
    "not_configured_message",
    "require_provider",
]
