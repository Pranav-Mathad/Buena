"""Classify a PDF into one of the canonical document types.

Pure function over ``(filename, head_text)``. Filename heuristics run
first (cheap, deterministic). When heuristics return ``None`` the
classifier falls back to Gemini Flash on the first 800 chars of
extracted text — and only when the Phase 8 cost ledger has remaining
budget under the ``pdf_doctype`` sub-label (default sub-cap ``$2``).

Phase 9 :mod:`backend.pipeline.validator` reads the resulting label
out of ``event.metadata.document_type`` to enforce constraints like
*"rent changes require a lease_addendum"*.
"""

from __future__ import annotations

import json
import re
from decimal import Decimal
from typing import Final, Literal, get_args

import structlog

from connectors import cost_ledger
from connectors.cost_ledger import CostCapExceeded

log = structlog.get_logger(__name__)


DocumentType = Literal[
    "lease",
    "lease_addendum",
    "kaufvertrag",
    "structural_permit",
    "vermessungsprotokoll",
    "invoice",
    "mahnung",
    "other",
]

#: All valid labels at runtime — used by the Gemini fallback to validate
#: the model output. ``get_args`` gives us the literal members.
DOCUMENT_TYPES: Final[tuple[str, ...]] = get_args(DocumentType)

DOCTYPE_LEDGER_LABEL: Final = "pdf_doctype"
DEFAULT_DOCTYPE_SUBCAP_USD: Final = Decimal("2.00")
PDF_CLASSIFY_HEAD_CHARS: Final = 800
# Approximate $-cost of one classifier call. Gemini Flash pricing is
# around $0.075 per million input tokens; the ~800-char prompt + short
# JSON response is well under 1k tokens, so we book 0.0001 per call.
ESTIMATED_CLASSIFY_COST_USD: Final = Decimal("0.0001")


def _filename_heuristic(filename: str) -> DocumentType | None:
    """Cheap, deterministic classifier over the filename only."""
    name = filename.lower()
    # Buena's filenames: 20240124_DL-011_INV-00005.pdf,
    #                    20250403_mahnung_LTR-042.pdf, etc.
    rules: tuple[tuple[re.Pattern[str], DocumentType], ...] = (
        (re.compile(r"(^|[_./-])(rechnung|invoice|inv-)"), "invoice"),
        (re.compile(r"(^|[_./-])mahnung"), "mahnung"),
        (re.compile(r"(^|[_./-])(mietvertrag|lease)(?!_addendum)"), "lease"),
        (re.compile(r"(^|[_./-])(addendum|nachtrag|mietvertrag.*nachtrag)"), "lease_addendum"),
        (re.compile(r"(^|[_./-])(kaufvertrag|sale|deed)"), "kaufvertrag"),
        (re.compile(r"(^|[_./-])(baugenehmigung|permit|structural)"), "structural_permit"),
        (re.compile(r"(^|[_./-])(vermessung|survey|protokoll)"), "vermessungsprotokoll"),
    )
    for pattern, label in rules:
        if pattern.search(name):
            return label
    return None


_GEMINI_PROMPT_TEMPLATE: Final = """You classify property-management documents.

Filename: {filename}
First {head_chars} characters of extracted text:
---
{head_text}
---

Choose ONE label from this exact list — answer with JSON only:
{labels_json}

Output schema: {{"document_type": "<label>"}}
Return "other" if uncertain. Do NOT invent labels."""


def _classify_with_gemini(
    filename: str, head_text: str, *, sub_cap_usd: Decimal
) -> DocumentType:
    """Call Gemini Flash; charge the cost ledger; coerce to a known label.

    Returns ``"other"`` when Gemini is unavailable, the cost cap is
    exhausted, the model returns an unknown label, or any error
    surfaces. Callers should treat ``"other"`` as "unknown" and Phase 9
    constraints will reject it where a specific subtype is required.
    """
    # 1. Pre-flight ledger charge — short-circuit before issuing any
    # request when the sub-cap is exhausted.
    cost_ledger.ensure_label(DOCTYPE_LEDGER_LABEL, sub_cap_usd)
    try:
        cost_ledger.charge(DOCTYPE_LEDGER_LABEL, ESTIMATED_CLASSIFY_COST_USD)
    except CostCapExceeded as exc:
        log.warning("doctype.cap_exhausted", filename=filename, error=str(exc))
        return "other"

    # 2. Lazy import so non-LLM tests don't require google-generativeai.
    try:
        from backend.services.gemini import (  # noqa: PLC0415
            GeminiUnavailable,
            _configure_client,  # type: ignore[attr-defined]
        )
    except Exception as exc:  # noqa: BLE001 — service module unavailable
        log.warning("doctype.gemini_import_failed", error=str(exc))
        return "other"

    try:
        genai = _configure_client()
    except GeminiUnavailable as exc:
        log.info("doctype.gemini_unavailable", reason=str(exc))
        return "other"

    prompt = _GEMINI_PROMPT_TEMPLATE.format(
        filename=filename,
        head_chars=PDF_CLASSIFY_HEAD_CHARS,
        head_text=head_text[:PDF_CLASSIFY_HEAD_CHARS],
        labels_json=json.dumps(list(DOCUMENT_TYPES)),
    )
    try:
        from backend.config import get_settings  # noqa: PLC0415

        model = genai.GenerativeModel(get_settings().gemini_flash_model)
        response = model.generate_content(
            prompt,
            generation_config={
                "response_mime_type": "application/json",
                "temperature": 0,
            },
        )
        text = (response.text or "").strip()
        data = json.loads(text) if text else {}
    except Exception as exc:  # noqa: BLE001 — collapse all model errors
        log.warning("doctype.gemini_error", filename=filename, error=str(exc))
        return "other"

    label = str(data.get("document_type", "other")).lower()
    if label not in DOCUMENT_TYPES:
        log.warning("doctype.gemini_unknown_label", returned=label)
        return "other"
    return label  # type: ignore[return-value]


def classify(
    filename: str,
    head_text: str,
    *,
    use_llm: bool = True,
    sub_cap_usd: Decimal = DEFAULT_DOCTYPE_SUBCAP_USD,
) -> DocumentType:
    """Return the best-fit :data:`DocumentType` for the document.

    Args:
        filename: Just the basename. Heuristics run on this only.
        head_text: First ~800 chars of extracted text. Used by the
            Gemini fallback when heuristics don't match.
        use_llm: Set ``False`` in tests / offline runs to skip the
            Gemini fallback entirely. Heuristics still apply.
        sub_cap_usd: Cap allocated to the ``pdf_doctype`` ledger label.

    Returns:
        One of :data:`DOCUMENT_TYPES`. ``"other"`` whenever the
        classifier is uncertain — never guesses.
    """
    via_filename = _filename_heuristic(filename)
    if via_filename is not None:
        log.debug("doctype.matched_filename", filename=filename, label=via_filename)
        return via_filename

    if not use_llm or not head_text.strip():
        return "other"

    return _classify_with_gemini(filename, head_text, sub_cap_usd=sub_cap_usd)
