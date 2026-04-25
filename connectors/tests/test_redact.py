"""Redaction tests — non-negotiable per Phase 8 ground rules."""

from __future__ import annotations

from connectors import redact


class TestIbanLast4:
    def test_returns_last4_for_de_iban(self) -> None:
        assert redact.iban_last4("DE94120300004034471349") == "****1349"

    def test_handles_spaces(self) -> None:
        assert redact.iban_last4("DE94 1203 0000 4034 4713 49") == "****1349"

    def test_returns_none_for_empty(self) -> None:
        assert redact.iban_last4("") is None
        assert redact.iban_last4(None) is None

    def test_short_input_returns_stars(self) -> None:
        assert redact.iban_last4("DE9") == "****"


class TestPhoneLast4:
    def test_e164_keeps_country_code(self) -> None:
        out = redact.phone_last4("+49 30 1234 5678")
        assert out is not None
        assert out.startswith("+49")
        assert out.endswith("78")

    def test_local_format(self) -> None:
        out = redact.phone_last4("030 12345678", keep_country_code=False)
        assert out is not None
        assert out.endswith("78")
        assert "***" in out

    def test_returns_none_for_empty(self) -> None:
        assert redact.phone_last4("") is None
        assert redact.phone_last4(None) is None


class TestEmailRedact:
    def test_keeps_local_part_replaces_domain(self) -> None:
        assert redact.email_redact("marcus.dowerg@outlook.com") == (
            "marcus.dowerg@example.com"
        )

    def test_collapse_when_local_dropped(self) -> None:
        out = redact.email_redact("marcus.dowerg@outlook.com", keep_local_part=False)
        assert out == "redacted@example.com"

    def test_passes_through_non_email(self) -> None:
        assert redact.email_redact("not an email") == "not an email"


class TestScrubText:
    def test_de_iban_in_freeform_text(self) -> None:
        text = "Rent paid via DE94120300004034471349 on 2024-01-01."
        out = redact.scrub_text(text)
        assert "****1349" in out
        assert "DE94120300004034471349" not in out

    def test_email_inside_subject(self) -> None:
        text = "Forward to klaus.weber@residence.de please"
        out = redact.scrub_text(text)
        assert "klaus.weber@example.com" in out
        assert "@residence.de" not in out

    def test_e164_phone(self) -> None:
        text = "Call me at +49 30 1234 5678 today"
        out = redact.scrub_text(text)
        assert "+49 30 1234 5678" not in out

    def test_idempotent(self) -> None:
        text = "Pay to ****1349 — call shipped@example.com"
        assert redact.scrub_text(text) == text

    def test_no_german_iban_survives_after_scrub(self) -> None:
        """Hard product invariant from the Phase 8 ground rules."""
        body = (
            "Sehr geehrter Herr,\n"
            "Bitte überweisen Sie an DE94100701240494519832\n"
            "Kontakt: slawomir.soelzer@hausmeister-mueller.de, "
            "Tel: +49 (0) 8367 36576"
        )
        scrubbed = redact.scrub_text(body)
        # The contract: assert_no_raw_iban does not raise.
        redact.assert_no_raw_iban(scrubbed)
        assert "DE94100701240494519832" not in scrubbed

    def test_assert_no_raw_iban_raises_on_real_iban(self) -> None:
        import pytest  # noqa: PLC0415

        with pytest.raises(AssertionError):
            redact.assert_no_raw_iban("Not scrubbed: DE94100701240494519832")
