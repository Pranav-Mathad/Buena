"""Shared helpers for enqueuing + signaling events.

Both the REST ``POST /debug/trigger_event`` endpoint and the IMAP poller need
the same two primitives: insert an event idempotently, and notify anything
listening on the property's SSE stream when a new fact lands.

The notification layer is a plain in-process ``asyncio.Queue`` fan-out — no
Redis, no Kafka (Part III: Postgres is the queue).
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any
from uuid import UUID

import structlog
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

log = structlog.get_logger(__name__)


@dataclass
class EventBus:
    """Per-property in-memory fan-out. Subscribers get a queue; publishers broadcast."""

    subscribers: dict[UUID | None, set[asyncio.Queue[dict[str, Any]]]] = field(
        default_factory=dict
    )
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    async def subscribe(self, property_id: UUID | None) -> asyncio.Queue[dict[str, Any]]:
        """Register a subscriber for ``property_id`` (``None`` = firehose)."""
        queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=64)
        async with self._lock:
            self.subscribers.setdefault(property_id, set()).add(queue)
        return queue

    async def unsubscribe(
        self, property_id: UUID | None, queue: asyncio.Queue[dict[str, Any]]
    ) -> None:
        """Remove a queue from the registry."""
        async with self._lock:
            for key in (property_id, None):
                bucket = self.subscribers.get(key)
                if bucket and queue in bucket:
                    bucket.discard(queue)

    async def publish(self, property_id: UUID | None, payload: dict[str, Any]) -> None:
        """Broadcast to anyone listening on ``property_id`` or the firehose."""
        async with self._lock:
            targets = set(self.subscribers.get(property_id, set()))
            targets |= self.subscribers.get(None, set())
        for queue in targets:
            try:
                queue.put_nowait(payload)
            except asyncio.QueueFull:
                log.warning("eventbus.queue_full", property_id=str(property_id))


_bus = EventBus()


def get_event_bus() -> EventBus:
    """Return the process-wide singleton bus."""
    return _bus


async def insert_event(
    session: AsyncSession,
    *,
    source: str,
    source_ref: str,
    raw_content: str,
    property_id: UUID | None = None,
    metadata: dict[str, Any] | None = None,
) -> tuple[UUID, bool]:
    """Insert an event; idempotent on ``(source, source_ref)``.

    Returns ``(event_id, inserted_now)``. When ``inserted_now`` is False the
    event was already in the table and the caller should skip processing.
    """
    result = await session.execute(
        text(
            """
            INSERT INTO events (source, source_ref, property_id, raw_content, metadata)
            VALUES (:source, :ref, :pid, :raw, COALESCE(CAST(:meta AS JSONB), '{}'::jsonb))
            ON CONFLICT (source, source_ref) DO NOTHING
            RETURNING id
            """
        ),
        {
            "source": source,
            "ref": source_ref,
            "pid": property_id,
            "raw": raw_content,
            "meta": _json_or_none(metadata),
        },
    )
    row = result.first()
    if row is not None:
        return UUID(str(row.id)), True

    existing = await session.execute(
        text("SELECT id FROM events WHERE source = :source AND source_ref = :ref"),
        {"source": source, "ref": source_ref},
    )
    return UUID(str(existing.scalar_one())), False


def _json_or_none(value: dict[str, Any] | None) -> str | None:
    """Tiny helper to keep SQL bind params JSON-ready."""
    import json  # noqa: PLC0415 — local import, hot path is rare

    return None if value is None else json.dumps(value)
