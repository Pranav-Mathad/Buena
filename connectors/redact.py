"""PII redaction at ingestion (not at render).

This module is the **only** place in the codebase that touches raw
IBANs, full phone numbers, or full email addresses on their way *into*
the database. Every connector calls these helpers before yielding a
:class:`connectors.base.ConnectorEvent`.

Tests assert that no raw German IBAN (``DE\\d{20}``) and no full E.164
phone number survives a round-trip through any connector. See
``connectors/tests/test_redact.py``.

.. todo::
    Confirm Buena anonymisation status before the public demo. Until
    confirmed, treat **all** Buena PII as real and redact aggressively.
"""

from __future__ import annotations

import re
from typing import Final

# Strict patterns — used by :func:`scrub_text` to scrub free-form
# strings (email body, verwendungszweck, PDF text) before persistence.
_GERMAN_IBAN_RE: Final = re.compile(r"\bDE\d{20}\b")
_OTHER_IBAN_RE: Final = re.compile(
    r"\b(?:[A-Z]{2}\d{2})[A-Z0-9]{11,30}\b"
)  # fallback for non-DE IBANs
_E164_PHONE_RE: Final = re.compile(r"\+?\d[\d\s\-/]{7,}\d")
_EMAIL_RE: Final = re.compile(
    r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b"
)


def iban_last4(iban: str | None) -> str | None:
    """Return ``****<last4>`` for a non-empty IBAN, ``None`` otherwise.

    Args:
        iban: Raw IBAN string. ``None`` and the empty string both pass
            through unchanged.

    Returns:
        ``"****1349"`` for ``"DE94120300004034471349"`` etc. Always 8
        characters (4 stars + 4 digits) when input is non-empty.
    """
    if not iban:
        return None
    cleaned = "".join(ch for ch in iban if not ch.isspace()).upper()
    if len(cleaned) < 4:
        return "****"
    return f"****{cleaned[-4:]}"


def phone_last4(phone: str | None, *, keep_country_code: bool = True) -> str | None:
    """Redact a phone to country code + last-4. ``None`` passes through.

    Args:
        phone: Raw phone string.
        keep_country_code: When True (default), keep the leading ``+NN``
            country code if the input starts with ``+``. When False or
            no leading ``+`` is present, only the last 4 digits survive.

    Returns:
        ``"+49 *** **5678"`` style string (last 4 digits preserved
        intact at the end).
    """
    if not phone:
        return None
    digits = "".join(ch for ch in phone if ch.isdigit())
    if len(digits) < 4:
        return "****"
    last4 = digits[-4:]
    if keep_country_code and phone.lstrip().startswith("+"):
        cleaned = phone.lstrip().lstrip("+")
        head = cleaned.split()[0] if " " in cleaned else cleaned
        cc = "".join(c for c in head if c.isdigit())[:3] or "??"
        return f"+{cc} *** **{last4}"
    return f"*** **{last4}"


def email_redact(
    email: str | None,
    *,
    keep_local_part: bool = True,
    replace_domain: str = "@example.com",
) -> str | None:
    """Replace the email domain (and optionally the local part) with a sentinel.

    Args:
        email: Raw email address.
        keep_local_part: When True (default), keep the part before the
            ``@`` so the demo can still show "marcus.dowerg" without
            leaking the real provider. When False, the whole address
            collapses to ``redacted@example.com``.
        replace_domain: Domain to substitute. Must include the leading
            ``@``.
    """
    if not email or "@" not in email:
        return email
    local, _ = email.rsplit("@", 1)
    if not keep_local_part:
        return f"redacted{replace_domain}"
    return f"{local}{replace_domain}"


def scrub_text(text: str) -> str:
    """Replace all detected IBANs / phones / emails inside free-form text.

    Used by connectors that yield raw bodies (email body, PDF text,
    bank ``verwendungszweck``) before persistence. Substitutions:

    - DE IBAN ``DE94120300004034471349`` → ``****1349``
    - Other IBAN (``GBxx…``)              → ``****<last4>``
    - Email                               → local-part + ``@example.com``
    - E.164-ish phone                     → ``*** *** **<last4>``

    Returns the scrubbed string. Idempotent — running scrub_text on
    already-scrubbed output is a no-op.
    """

    def _iban_de(match: re.Match[str]) -> str:
        return iban_last4(match.group(0)) or "****"

    def _iban_other(match: re.Match[str]) -> str:
        # Don't double-scrub already-redacted markers.
        candidate = match.group(0)
        if candidate.startswith("****"):
            return candidate
        return iban_last4(candidate) or "****"

    def _phone(match: re.Match[str]) -> str:
        candidate = match.group(0)
        digits = "".join(ch for ch in candidate if ch.isdigit())
        if len(digits) < 8:
            # Below 8 digits is likely a year, postal code, or IBAN
            # tail (already redacted) — leave it alone.
            return candidate
        return phone_last4(candidate, keep_country_code=True) or "****"

    def _email(match: re.Match[str]) -> str:
        return email_redact(match.group(0)) or match.group(0)

    out = _GERMAN_IBAN_RE.sub(_iban_de, text)
    out = _OTHER_IBAN_RE.sub(_iban_other, out)
    out = _EMAIL_RE.sub(_email, out)
    out = _E164_PHONE_RE.sub(_phone, out)
    return out


def assert_no_raw_iban(value: str) -> None:
    """Defensive assertion used in tests + worker hot path.

    Raises:
        AssertionError: if the value contains a ``DE\\d{20}`` substring.
    """
    if _GERMAN_IBAN_RE.search(value):
        raise AssertionError(
            "raw German IBAN found in scrubbed output — connector "
            "redaction missed something"
        )
