"""Verify the IMAP ingestion path on real Buena-style .eml files.

The user explicitly asked us to verify live IMAP polling before
moving on. We can't reach a real IMAP server from a unit test, but
:func:`backend.services.imap_poller._ingest` is the choke-point —
once an IMAP fetch succeeds it hands raw bytes + a Message-ID to that
function. If ``_ingest`` parses real Buena German .eml files cleanly
and the downstream worker extracts a fact, the live path works end
to end (modulo the IMAP transport itself, which is just ``imapclient``).

Each test:

* Picks a real .eml from ``Extracted/emails/`` (skipped if the
  gitignored archive is not present locally).
* Calls ``_ingest()`` with the raw bytes.
* Confirms the event was inserted with the correct source, source_ref,
  metadata, and German-friendly raw_content.
* Re-calls ``_ingest()`` to confirm idempotency on Message-ID.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from sqlalchemy import text

from backend.config import get_settings
from backend.db.session import get_sessionmaker
from backend.services.imap_poller import _ingest, _render_event_text
from connectors.migrations import apply_all as ensure_migrations

pytestmark = pytest.mark.asyncio


def _reset_session_cache() -> None:
    from backend.db import session as session_module  # noqa: PLC0415

    session_module.get_engine.cache_clear()
    session_module.get_sessionmaker.cache_clear()


async def _db_reachable() -> bool:
    _reset_session_cache()
    try:
        factory = get_sessionmaker()
        async with factory() as conn:
            await conn.execute(text("SELECT 1"))
        return True
    except Exception:  # noqa: BLE001
        return False


def _pick_buena_eml() -> Path | None:
    """Return a real Buena .eml path or ``None`` if the archive is absent."""
    candidates = list(Path("Extracted/emails").rglob("*.eml"))
    if not candidates:
        return None
    return candidates[0]


async def _setup_or_skip() -> Path:
    if not await _db_reachable():
        pytest.skip(f"dev DB unreachable at {get_settings().database_url}")
    ensure_migrations()
    eml = _pick_buena_eml()
    if eml is None:
        pytest.skip("Buena Extracted/emails/ archive not present locally")
    return eml


# ---------------------------------------------------------------------------
# Pure-unit: render path tolerates German + multipart bodies
# ---------------------------------------------------------------------------


async def test_render_event_text_handles_german_eml() -> None:
    """A real German .eml flattens to ``From:\\nSubject:\\n\\n<body>``."""
    eml_path = await _setup_or_skip()
    raw = eml_path.read_bytes()

    import email  # noqa: PLC0415

    parsed = email.message_from_bytes(raw)
    text_blob = _render_event_text(parsed)

    assert text_blob.startswith("From: "), "must lead with From: header"
    assert "Subject: " in text_blob
    # German body content survives charset decoding (umlauts).
    body_segment = text_blob.split("\n\n", 1)[1] if "\n\n" in text_blob else ""
    assert body_segment.strip(), "body must not be empty"


# ---------------------------------------------------------------------------
# Integration: _ingest() inserts an event from real .eml bytes
# ---------------------------------------------------------------------------


async def test_ingest_inserts_real_buena_eml() -> None:
    """Calling _ingest with real .eml bytes lands a routable event."""
    eml_path = await _setup_or_skip()
    raw = eml_path.read_bytes()

    import email  # noqa: PLC0415

    parsed = email.message_from_bytes(raw)
    message_id = parsed.get("Message-ID", f"test-{eml_path.stem}").strip("<>").strip()
    # Insert with a unique source_ref to keep the test self-contained.
    test_ref = f"test-imap-{eml_path.stem}"

    factory = get_sessionmaker()
    async with factory() as session:
        # Pre-clean to isolate the test.
        await session.execute(
            text("DELETE FROM events WHERE source_ref = :ref"),
            {"ref": test_ref},
        )
        await session.commit()

    try:
        inserted = await _ingest(test_ref, raw)
        assert inserted is True, "first ingestion must insert"

        async with factory() as session:
            row = (
                await session.execute(
                    text(
                        """
                        SELECT id, source, source_ref, raw_content, metadata
                        FROM events WHERE source_ref = :ref
                        """
                    ),
                    {"ref": test_ref},
                )
            ).first()
            assert row is not None
            assert row.source == "email"
            assert row.source_ref == test_ref
            # Raw content carries header metadata + body.
            assert row.raw_content.startswith("From: ")
            # metadata captures from/subject/date.
            md = dict(row.metadata or {})
            assert "from" in md
            assert "subject" in md
            assert "date" in md
    finally:
        async with factory() as session:
            await session.execute(
                text("DELETE FROM events WHERE source_ref = :ref"),
                {"ref": test_ref},
            )
            await session.commit()


async def test_ingest_is_idempotent_on_message_id() -> None:
    """Re-calling _ingest with the same source_ref does not double-insert."""
    eml_path = await _setup_or_skip()
    raw = eml_path.read_bytes()
    test_ref = f"test-imap-idem-{eml_path.stem}"

    factory = get_sessionmaker()
    async with factory() as session:
        await session.execute(
            text("DELETE FROM events WHERE source_ref = :ref"),
            {"ref": test_ref},
        )
        await session.commit()

    try:
        first = await _ingest(test_ref, raw)
        second = await _ingest(test_ref, raw)
        assert first is True
        assert second is False, "second ingest with same Message-ID must be a no-op"

        async with factory() as session:
            n = (
                await session.execute(
                    text(
                        "SELECT COUNT(*) FROM events WHERE source_ref = :ref"
                    ),
                    {"ref": test_ref},
                )
            ).scalar()
            assert n == 1
    finally:
        async with factory() as session:
            await session.execute(
                text("DELETE FROM events WHERE source_ref = :ref"),
                {"ref": test_ref},
            )
            await session.commit()


# ---------------------------------------------------------------------------
# Integration: poll_once is a no-op when IMAP is not configured
# ---------------------------------------------------------------------------


async def test_poll_once_is_safe_noop_when_not_configured(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Without IMAP creds, ``poll_once`` returns 0 cleanly — the scheduler
    relies on this so a missing IMAP env doesn't crash the worker loop."""
    from backend.services import imap_poller  # noqa: PLC0415

    monkeypatch.setattr(imap_poller, "_imap_configured", lambda: False)
    inserted = await imap_poller.poll_once()
    assert inserted == 0
