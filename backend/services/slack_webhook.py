"""Slack webhook ingestion + HMAC signature verification.

Implements the exact scheme documented at
https://api.slack.com/authentication/verifying-requests-from-slack:
``v0:{timestamp}:{raw_body}`` HMAC-SHA256 with the signing secret, compared
in constant time against the ``X-Slack-Signature`` header.

Requests older than 5 minutes are rejected (replay protection).
"""

from __future__ import annotations

import hashlib
import hmac
import time

import structlog

log = structlog.get_logger(__name__)

REPLAY_WINDOW_SECONDS = 60 * 5


def verify_signature(
    *,
    signing_secret: str,
    timestamp: str | None,
    signature: str | None,
    raw_body: bytes,
    now: float | None = None,
) -> bool:
    """Return True iff the request is a legitimate Slack delivery.

    The function is safe to call with missing secrets / headers — it simply
    returns ``False``. Callers should respond with HTTP 401 in that case.
    """
    if not signing_secret or not timestamp or not signature:
        return False
    try:
        ts = int(timestamp)
    except ValueError:
        return False
    current = now if now is not None else time.time()
    if abs(current - ts) > REPLAY_WINDOW_SECONDS:
        log.warning("slack.replay_rejected", delta=abs(current - ts))
        return False

    base_string = f"v0:{ts}:".encode() + raw_body
    digest = hmac.new(signing_secret.encode("utf-8"), base_string, hashlib.sha256).hexdigest()
    expected = f"v0={digest}"
    return hmac.compare_digest(expected, signature)


def format_slack_event(payload: dict) -> tuple[str, str]:
    """Turn a Slack ``event_callback`` payload into ``(source_ref, raw_content)``.

    Uses ``{team_id}:{channel}:{ts}`` as the idempotency key and renders a
    human-readable body the extractor can work with.
    """
    event = payload.get("event", {}) or {}
    team_id = str(payload.get("team_id", "no_team"))
    channel = str(event.get("channel", "no_channel"))
    ts = str(event.get("ts", event.get("event_ts", time.time())))
    source_ref = f"{team_id}:{channel}:{ts}"

    user = str(event.get("user", "unknown"))
    text = str(event.get("text", "")).strip()
    raw_content = (
        f"From: Slack user {user} in channel {channel}\n"
        f"Subject: Slack message\n\n{text}"
    )
    return source_ref, raw_content
