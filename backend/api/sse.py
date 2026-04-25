"""Server-sent events for live markdown updates.

``GET /properties/{id}/events`` streams JSON envelopes whenever the worker
finishes processing an event attached to that property. The browser / Lovable
front-end re-fetches the markdown on each push.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from uuid import UUID

import structlog
from fastapi import APIRouter
from fastapi.responses import StreamingResponse

from backend.pipeline.events import get_event_bus

router = APIRouter(prefix="/properties", tags=["sse"])
log = structlog.get_logger(__name__)


async def _event_stream(property_id: UUID) -> AsyncIterator[bytes]:
    """Yield SSE-formatted lines for one subscriber."""
    bus = get_event_bus()
    queue = await bus.subscribe(property_id)
    log.info("sse.subscribe", property_id=str(property_id))
    try:
        # Initial hello so clients learn they're connected.
        yield b"event: hello\ndata: {}\n\n"
        while True:
            try:
                payload = await asyncio.wait_for(queue.get(), timeout=15.0)
            except asyncio.TimeoutError:
                # Heartbeat keeps intermediaries (nginx, etc.) from closing.
                yield b": keep-alive\n\n"
                continue
            body = json.dumps(payload).encode("utf-8")
            yield b"event: fact_update\ndata: " + body + b"\n\n"
    finally:
        await bus.unsubscribe(property_id, queue)
        log.info("sse.unsubscribe", property_id=str(property_id))


@router.get("/{property_id}/events")
async def property_events(property_id: UUID) -> StreamingResponse:
    """Live stream of fact updates for a single property."""
    return StreamingResponse(
        _event_stream(property_id),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )
