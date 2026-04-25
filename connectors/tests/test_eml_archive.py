"""Tests for the .eml walker — uses fabricated EMLs in a tmp dir."""

from __future__ import annotations

from datetime import timezone
from pathlib import Path

import pytest

from connectors import eml_archive, redact


def _write_eml(path: Path, body: str = "Heizung kalt seit gestern.") -> None:
    """Write a minimal RFC-822-ish .eml file."""
    raw = (
        "Message-ID: <abc123@example.com>\r\n"
        "From: lukas.weber@tenant.demo\r\n"
        "To: hv@example.com\r\n"
        "Subject: Heizung Apt 4B\r\n"
        "Date: Thu, 24 Apr 2026 09:00:00 +0000\r\n"
        f"In-Reply-To: <root-thread@example.com>\r\n"
        f"References: <root-thread@example.com>\r\n"
        "Content-Type: text/plain; charset=utf-8\r\n"
        "\r\n"
        f"{body}\r\n"
        "Bitte zahlen Sie an DE94120300004034471349.\r\n"
        "Phone: +49 30 1234 5678\r\n"
    )
    path.write_text(raw, encoding="utf-8")


def test_parse_one_redacts_body_and_metadata(tmp_path: Path) -> None:
    eml = tmp_path / "20260424_090000_EMAIL-00001.eml"
    _write_eml(eml)
    event = eml_archive.parse_one(eml, root=tmp_path)

    assert event.source == "email"
    assert event.source_ref == "abc123@example.com"
    assert "DE94120300004034471349" not in event.raw_content
    assert "****1349" in event.raw_content
    assert event.metadata["from"].endswith("@example.com")
    assert event.metadata["thread_id"] == "root-thread@example.com"
    assert event.metadata["in_reply_to"] == "root-thread@example.com"
    redact.assert_no_raw_iban(event.raw_content)


def test_parse_one_uses_filename_timestamp_when_date_missing(tmp_path: Path) -> None:
    eml = tmp_path / "20260424_090000_EMAIL-00002.eml"
    eml.write_text(
        "From: x@y\r\nSubject: ohne datum\r\n\r\nbody\r\n",
        encoding="utf-8",
    )
    event = eml_archive.parse_one(eml, root=tmp_path)
    # filename has 09:00:00 UTC; tolerate parser dropping seconds
    assert event.received_at.tzinfo is not None
    assert event.received_at.astimezone(timezone.utc).hour == 9


def test_walk_directory_yields_one_per_eml(tmp_path: Path) -> None:
    for i in range(3):
        eml = tmp_path / f"20260424_09000{i}_EMAIL-{i:05d}.eml"
        _write_eml(eml, body=f"body {i}")
        # Each must have a unique Message-ID for source_ref dedupe
        eml.write_text(
            eml.read_text().replace("abc123", f"abc{i:03d}"),
            encoding="utf-8",
        )

    events = list(eml_archive.walk_directory(tmp_path))
    assert len(events) == 3
    refs = {e.source_ref for e in events}
    assert len(refs) == 3


def test_no_raw_iban_survives_walk(tmp_path: Path) -> None:
    eml = tmp_path / "20260424_090000_EMAIL-00001.eml"
    _write_eml(eml)
    for event in eml_archive.walk_directory(tmp_path):
        redact.assert_no_raw_iban(event.raw_content)
        for v in event.metadata.values():
            if isinstance(v, str):
                redact.assert_no_raw_iban(v)


def test_missing_directory_yields_nothing(tmp_path: Path) -> None:
    # walk_directory should log + return rather than raise.
    events = list(eml_archive.walk_directory(tmp_path / "nonexistent"))
    assert events == []


def test_corrupt_file_does_not_kill_walk(tmp_path: Path) -> None:
    good = tmp_path / "20260424_090000_EMAIL-00001.eml"
    bad = tmp_path / "20260424_090001_EMAIL-00002.eml"
    _write_eml(good)
    bad.write_bytes(b"\x00\x01\x02\x03")  # not a valid email

    # Even with the bad file present, the walk completes.
    events = list(eml_archive.walk_directory(tmp_path))
    assert any(e.source_ref == "abc123@example.com" for e in events)


@pytest.mark.parametrize(
    "subject,expected_subject_contains",
    [
        ("Heizung kalt", "Heizung kalt"),
        ("Mahnung 2/3", "Mahnung 2/3"),
    ],
)
def test_subject_passes_through_when_safe(
    tmp_path: Path, subject: str, expected_subject_contains: str
) -> None:
    eml = tmp_path / "20260424_090000_EMAIL-00001.eml"
    eml.write_text(
        f"Message-ID: <m@e>\r\nFrom: a@b\r\nSubject: {subject}\r\n"
        "Date: Thu, 24 Apr 2026 09:00:00 +0000\r\n\r\nbody",
        encoding="utf-8",
    )
    event = eml_archive.parse_one(eml, root=tmp_path)
    assert expected_subject_contains in event.metadata["subject"]


@pytest.mark.parametrize(
    "subject,body_first_line",
    [
        ("Fwd: Heizung kalt", "From: lukas.weber@tenant.demo"),
        ("WG: Mahnung 2/3", "Von: amtsgericht@example.org"),
        ("FW: Schimmelmeldung", "From: maria.schulz@tenant.demo"),
    ],
)
def test_forwarded_emails_capture_inner_sender(
    tmp_path: Path, subject: str, body_first_line: str
) -> None:
    """``Fwd:``/``WG:`` prefix → inner_sender pulled from the quoted body."""
    eml = tmp_path / "20260424_090000_EMAIL-00099.eml"
    eml.write_text(
        f"Message-ID: <fwd-99@example.com>\r\n"
        f"From: pm@hausverwaltung.demo\r\n"
        f"Subject: {subject}\r\n"
        "Date: Thu, 24 Apr 2026 09:00:00 +0000\r\n"
        "Content-Type: text/plain; charset=utf-8\r\n"
        "\r\n"
        f"{body_first_line}\r\n"
        "Sent: yesterday\r\n"
        "Subject: original subject\r\n"
        "\r\n"
        "urspruenglicher Inhalt\r\n",
        encoding="utf-8",
    )
    event = eml_archive.parse_one(eml, root=tmp_path)
    assert event.metadata["is_forward"] is True
    assert event.metadata["inner_sender"] is not None


def test_non_forward_email_has_no_inner_sender(tmp_path: Path) -> None:
    """``Re:``/``Aw:`` replies preserve the outer From; no inner_sender capture."""
    eml = tmp_path / "20260424_090000_EMAIL-00100.eml"
    eml.write_text(
        "Message-ID: <reply-100@example.com>\r\n"
        "From: tenant@example.demo\r\n"
        "Subject: Re: Wartungstermin\r\n"
        "Date: Thu, 24 Apr 2026 09:00:00 +0000\r\n"
        "\r\n"
        "Vielen Dank.\r\n",
        encoding="utf-8",
    )
    event = eml_archive.parse_one(eml, root=tmp_path)
    assert event.metadata["is_forward"] is False
    assert event.metadata["inner_sender"] is None
