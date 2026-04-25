"""Language detection for the pipeline.

Two callers:

- :mod:`backend.pipeline.extractor` picks per-language prompts.
- :mod:`backend.pipeline.renderer` toggles German section labels when
  a property's recent source events are mostly German.

We use ``langdetect`` because it ships with no external dependencies
and is good enough for paragraph-length emails. For very short texts
(< 30 chars) detection is unreliable, so the helper falls back to
``en`` to keep behaviour stable.
"""

from __future__ import annotations

from typing import Final

import structlog

log = structlog.get_logger(__name__)

DEFAULT_LANG: Final[str] = "en"
MIN_DETECT_CHARS: Final[int] = 30


def detect_language(text: str) -> str:
    """Return an ISO-639-1 language code (``"de"``, ``"en"``, …).

    Defaults to :data:`DEFAULT_LANG` when the text is too short or
    detection fails. We also pin a deterministic seed so two runs of
    the eval pick the same language label for the same input —
    ``langdetect`` is non-deterministic by default.
    """
    if not text or len(text) < MIN_DETECT_CHARS:
        return DEFAULT_LANG
    try:
        from langdetect import DetectorFactory, detect  # noqa: PLC0415

        DetectorFactory.seed = 0
        code = str(detect(text)).lower()
    except Exception as exc:  # noqa: BLE001 — collapse to default on any error
        log.debug("lang.detect_failed", error=str(exc))
        return DEFAULT_LANG
    if code in {"de", "en"}:
        return code
    # Map close cousins to the closest supported prompt language.
    if code in {"nl", "af"}:
        return "de"
    return DEFAULT_LANG
