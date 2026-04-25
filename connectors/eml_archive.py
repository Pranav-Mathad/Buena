"""Walk a directory of ``.eml`` files and yield :class:`ConnectorEvent`s.

For each message:

- ``source_ref`` = ``Message-ID`` if present, otherwise
  ``sha256(From + Date + Subject)[:16]``.
- ``raw_content`` = ``"From: …\\nSubject: …\\n\\n<body>"`` (plain-text
  body), with PII scrubbed.
- ``metadata`` = parsed headers (``from``, ``to``, ``subject``,
  ``date``, ``message_id``, ``thread_id``, ``in_reply_to``,
  ``references``, ``relative_path``).
- ``received_at`` = parsed ``Date`` header, falling back to filename
  timestamp prefix (``YYYYMMDD_HHMMSS_…``) and finally the file mtime.

PII redaction is applied to both the header values and the body via
:mod:`connectors.redact`. Threading metadata is captured but not
resolved here — the composite at the property level decides how to
chain related events.
"""

from __future__ import annotations

import email
import hashlib
import re
from collections.abc import Iterator
from datetime import datetime, timezone
from email.message import Message
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any

import structlog

from connectors import redact
from connectors.base import ConnectorEvent

log = structlog.get_logger(__name__)


_FILENAME_TS_RE = re.compile(r"(\d{8})_(\d{6})_")


def _parse_filename_timestamp(name: str) -> datetime | None:
    """Parse the ``YYYYMMDD_HHMMSS_`` prefix that Buena's EMLs use."""
    match = _FILENAME_TS_RE.match(name)
    if not match:
        return None
    try:
        return datetime.strptime(
            f"{match.group(1)}{match.group(2)}", "%Y%m%d%H%M%S"
        ).replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def _flatten_body(msg: Message) -> str:
    """Extract a plain-text body from a potentially-multipart message."""
    if msg.is_multipart():
        parts: list[str] = []
        for part in msg.walk():
            if part.get_content_type() == "text/plain" and not part.is_multipart():
                payload = part.get_payload(decode=True) or b""
                if isinstance(payload, bytes):
                    charset = part.get_content_charset() or "utf-8"
                    try:
                        parts.append(payload.decode(charset, "replace"))
                    except LookupError:
                        parts.append(payload.decode("utf-8", "replace"))
        if parts:
            return "\n".join(parts).strip()
    payload = msg.get_payload(decode=True) or b""
    if isinstance(payload, bytes):
        charset = msg.get_content_charset() or "utf-8"
        return payload.decode(charset, "replace").strip()
    return str(payload).strip()


def _parse_date_header(raw_date: str | None) -> datetime | None:
    """Parse an RFC-2822 Date header, normalising to UTC."""
    if not raw_date:
        return None
    try:
        dt = parsedate_to_datetime(raw_date)
    except (TypeError, ValueError):
        return None
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _stable_message_id(headers: dict[str, str]) -> str:
    """Fallback source_ref when ``Message-ID`` is missing."""
    canonical = (
        headers.get("from", "")
        + "|"
        + headers.get("date", "")
        + "|"
        + headers.get("subject", "")
    ).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()[:16]


def _references_list(value: str | None) -> list[str]:
    """Parse a ``References`` / ``In-Reply-To`` header into individual IDs."""
    if not value:
        return []
    # IDs look like ``<abc@host>``; split on whitespace and keep angle-bracket forms.
    return [tok for tok in value.split() if tok.startswith("<") and tok.endswith(">")]


def parse_one(path: Path, *, root: Path | None = None) -> ConnectorEvent:
    """Parse a single ``.eml`` file into a :class:`ConnectorEvent`."""
    raw_bytes = path.read_bytes()
    msg = email.message_from_bytes(raw_bytes)

    headers: dict[str, str] = {
        "from": str(msg.get("From", "") or ""),
        "to": str(msg.get("To", "") or ""),
        "subject": str(msg.get("Subject", "") or ""),
        "date": str(msg.get("Date", "") or ""),
        "message_id": str(msg.get("Message-ID", "") or ""),
        "in_reply_to": str(msg.get("In-Reply-To", "") or ""),
        "references": str(msg.get("References", "") or ""),
    }

    body = _flatten_body(msg)

    # Redact every header that might leak PII.
    redacted_headers: dict[str, str] = {
        "from": redact.scrub_text(headers["from"]),
        "to": redact.scrub_text(headers["to"]),
        "subject": redact.scrub_text(headers["subject"]),
        "date": headers["date"],
        "message_id": headers["message_id"],
        "in_reply_to": headers["in_reply_to"],
        "references": headers["references"],
    }

    redacted_body = redact.scrub_text(body)
    raw_content = (
        f"From: {redacted_headers['from']}\n"
        f"Subject: {redacted_headers['subject']}\n\n"
        f"{redacted_body}"
    )

    message_id = headers["message_id"].strip("<>") or _stable_message_id(headers)

    received_at = (
        _parse_date_header(headers["date"]) or _parse_filename_timestamp(path.name)
    )
    if received_at is None:
        received_at = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)

    relative_path = (
        str(path.relative_to(root)) if root is not None and path.is_relative_to(root)
        else str(path)
    )

    metadata: dict[str, Any] = {
        "from": redacted_headers["from"],
        "to": redacted_headers["to"],
        "subject": redacted_headers["subject"],
        "date": redacted_headers["date"],
        "message_id": message_id,
        "thread_id": (
            _references_list(headers["references"])[0].strip("<>")
            if _references_list(headers["references"])
            else (
                headers["in_reply_to"].strip("<>")
                if headers["in_reply_to"]
                else message_id
            )
        ),
        "in_reply_to": headers["in_reply_to"].strip("<>") or None,
        "references": [r.strip("<>") for r in _references_list(headers["references"])],
        "relative_path": relative_path,
    }

    return ConnectorEvent(
        source="email",
        source_ref=message_id,
        raw_content=raw_content,
        metadata=metadata,
        received_at=received_at,
    )


def walk_directory(root: Path) -> Iterator[ConnectorEvent]:
    """Yield :class:`ConnectorEvent`s for every ``.eml`` under ``root``."""
    if not root.exists():
        log.warning("eml.archive.missing", root=str(root))
        return
    paths = sorted(root.rglob("*.eml"))
    log.info("eml.archive.start", root=str(root), files=len(paths))
    for path in paths:
        try:
            yield parse_one(path, root=root)
        except Exception:  # noqa: BLE001 — keep the walk going
            log.exception("eml.archive.parse_error", path=str(path))
