"""IMAP inbox poller.

Polls the configured mailbox every tick (APScheduler, ~10s per KEYSTONE
Phase 1). Each new message becomes an ``email`` event, idempotent on
``Message-ID`` so replaying the inbox after a restart is safe.
"""

from __future__ import annotations

import email
from email.message import Message
from typing import Any

import structlog
from imapclient import IMAPClient

from backend.config import get_settings
from backend.db.session import get_sessionmaker
from backend.pipeline.events import insert_event

log = structlog.get_logger(__name__)


def _imap_configured() -> bool:
    """Return True when IMAP env is real enough to attempt a connection."""
    s = get_settings()
    configured = bool(
        s.imap_host and s.imap_user and s.imap_password
    ) and s.imap_password != "replace-me"

    log.info(
        "imap.config.check",
        host=s.imap_host,
        user=s.imap_user,
        configured=configured,
    )
    return configured


def _flatten_body(msg: Message) -> str:
    """Extract a plain-text body from a potentially multipart message."""
    if msg.is_multipart():
        parts: list[str] = []
        for part in msg.walk():
            if part.get_content_type() == "text/plain" and not part.is_multipart():
                payload = part.get_payload(decode=True) or b""
                try:
                    parts.append(
                        payload.decode(
                            part.get_content_charset() or "utf-8", "replace"
                        )
                    )
                except LookupError:
                    parts.append(payload.decode("utf-8", "replace"))
        if parts:
            return "\n".join(parts).strip()

    payload = msg.get_payload(decode=True) or b""
    if isinstance(payload, bytes):
        return payload.decode(
            msg.get_content_charset() or "utf-8", "replace"
        ).strip()
    return str(payload)


def _render_event_text(msg: Message) -> str:
    """Flatten From/Subject/body into raw_content."""
    from_ = msg.get("From", "")
    subject = msg.get("Subject", "")
    body = _flatten_body(msg)
    return f"From: {from_}\nSubject: {subject}\n\n{body}"


async def _ingest(message_id: str, raw: bytes) -> bool:
    """Parse a raw RFC822 message, insert + process it."""
    parsed = email.message_from_bytes(raw)
    content = _render_event_text(parsed)

    metadata: dict[str, Any] = {
        "from": parsed.get("From"),
        "subject": parsed.get("Subject"),
        "date": parsed.get("Date"),
    }

    factory = get_sessionmaker()

    async with factory() as session:
        event_id, inserted = await insert_event(
            session,
            source="email",
            source_ref=message_id,
            raw_content=content,
            metadata=metadata,
        )
        await session.commit()

    log.info(
        "imap.ingest.result",
        message_id=message_id,
        inserted=inserted,
    )

    if inserted:
        from backend.pipeline.worker import process_specific

        try:
            await process_specific(event_id)
        except Exception:
            log.exception("imap.process.error", event_id=str(event_id))

    return inserted


async def poll_once() -> int:
    """Fetch new messages once."""
    log.info("imap.poll.start")

    if not _imap_configured():
        log.warning("imap.skip", reason="not_configured")
        return 0

    settings = get_settings()
    inserted = 0

    try:
        log.info(
            "imap.connecting",
            host=settings.imap_host,
            port=settings.imap_port,
        )

        with IMAPClient(
            settings.imap_host,
            port=settings.imap_port,
            ssl=True,
        ) as client:
            client.login(settings.imap_user, settings.imap_password)

            log.info("imap.login.success")

            client.select_folder(settings.imap_mailbox, readonly=False)

            log.info("imap.folder.selected", mailbox=settings.imap_mailbox)

            # 🔥 DEBUG MODE: change to ALL to confirm emails exist
            uids = client.search(["UNSEEN"])
            log.info("imap.search.result", count=len(uids))

            if not uids:
                return 0

            fetched = client.fetch(uids, ["RFC822", "ENVELOPE"])

            for uid, data in fetched.items():
                raw = data.get(b"RFC822")
                envelope = data.get(b"ENVELOPE")

                message_id = (
                    envelope.message_id.decode("utf-8", "replace")
                    if envelope and envelope.message_id
                    else f"uid-{uid}"
                )

                if not raw:
                    log.warning("imap.empty.raw", uid=uid)
                    continue

                try:
                    if await _ingest(message_id, raw):
                        inserted += 1
                        client.add_flags(uid, [b"\\Seen"])
                except Exception:
                    log.exception(
                        "imap.ingest.error",
                        message_id=message_id,
                    )

    except Exception:
        log.exception("imap.poll.error")
        return 0

    log.info("imap.poll.done", inserted=inserted)
    return inserted