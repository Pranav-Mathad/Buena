"""Pioneer gateway constants used by Ask, reply drafting and the semantic validator.

Transitional: those three still build their own chat-completions requests. The
provider interface in :mod:`backend.services.llm` replaces this module.
"""

from __future__ import annotations

from typing import Any

from backend.services.llm.base import parse_json_payload
from backend.services.llm.pioneer import PIONEER_BASE_URL, is_configured

PIONEER_DEFAULT_MODEL = "claude-sonnet-4-6"


class PioneerUnavailable(RuntimeError):
    """Raised when Pioneer cannot be used (no key, transport error)."""


def is_available() -> bool:
    return is_configured()


def _parse_json_payload(text: str) -> dict[str, Any]:
    return parse_json_payload(text)


__all__ = ["PIONEER_BASE_URL", "PIONEER_DEFAULT_MODEL", "PioneerUnavailable", "is_available"]
