"""External-system webhooks. Phase 2 ships the Slack ingestion endpoint."""

from __future__ import annotations

import json

import structlog
from fastapi import APIRouter, Depends, Header, HTTPException, Request
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from backend.config import get_settings
from backend.db.session import get_session
from backend.pipeline.events import insert_event
from backend.pipeline.worker import process_batch
from backend.services.slack_webhook import format_slack_event, verify_signature

router = APIRouter(prefix="/webhooks", tags=["webhooks"])
log = structlog.get_logger(__name__)


class SlackAck(BaseModel):
    """Response envelope for the Slack webhook."""

    ok: bool = True
    event_id: str | None = None
    inserted: bool | None = None
    processed: int | None = None
    challenge: str | None = None


@router.post("/slack", response_model=SlackAck)
async def slack_webhook(
    request: Request,
    x_slack_request_timestamp: str | None = Header(default=None),
    x_slack_signature: str | None = Header(default=None),
    session: AsyncSession = Depends(get_session),
) -> SlackAck:
    """Ingest a Slack event_callback into the pipeline.

    The endpoint additionally answers the Slack ``url_verification`` challenge
    so the URL can be registered from the Slack UI.
    """
    raw = await request.body()
    settings = get_settings()

    if settings.slack_signing_secret and settings.slack_signing_secret != "replace-me":
        if not verify_signature(
            signing_secret=settings.slack_signing_secret,
            timestamp=x_slack_request_timestamp,
            signature=x_slack_signature,
            raw_body=raw,
        ):
            log.warning("slack.signature_invalid")
            raise HTTPException(status_code=401, detail="invalid signature")

    try:
        payload = json.loads(raw or b"{}")
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=400, detail=f"bad JSON: {exc}") from exc

    if payload.get("type") == "url_verification":
        challenge = str(payload.get("challenge", ""))
        log.info("slack.url_verification")
        return SlackAck(challenge=challenge)

    if payload.get("type") != "event_callback":
        log.info("slack.ignored", payload_type=payload.get("type"))
        return SlackAck()

    source_ref, raw_content = format_slack_event(payload)
    event_id, inserted = await insert_event(
        session,
        source="slack",
        source_ref=source_ref,
        raw_content=raw_content,
        metadata={"slack_event": payload.get("event", {})},
    )
    await session.commit()
    processed = await process_batch(max_events=3)

    log.info(
        "slack.ingested",
        event_id=str(event_id),
        inserted=inserted,
        processed=processed,
    )
    return SlackAck(
        event_id=str(event_id),
        inserted=inserted,
        processed=processed,
    )
