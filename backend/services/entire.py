"""Entire-compatible approval layer.

Per KEYSTONE Part IV, the approval inbox is "Entire-powered". Until the
Entire SDK is available (or swapped in), we ship a Protocol-based
``EntireBroker`` interface and a ``LocalEntireBroker`` reference
implementation that writes to Postgres. Callers (the signals API) depend on
the Protocol, so adding a real Entire adapter is a one-file change.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Protocol, runtime_checkable
from uuid import UUID

import structlog
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

log = structlog.get_logger(__name__)


@dataclass(frozen=True)
class DispatchResult:
    """What a broker returns when it has successfully routed the approval."""

    outbox_id: UUID
    channel: str
    recipient: str
    sent_at: datetime


@runtime_checkable
class EntireBroker(Protocol):
    """Swap-in point for the real Entire SDK when we have the keys."""

    name: str

    async def dispatch(
        self,
        session: AsyncSession,
        *,
        signal_id: UUID,
        channel: str,
        recipient: str,
        subject: str | None,
        body: str,
    ) -> DispatchResult:
        """Broker the message after the human approves the signal."""
        ...


class LocalEntireBroker:
    """Reference implementation — writes to the ``outbox`` table.

    The pitch line stays accurate ("Entire-compatible approval layer") because
    the signature + semantics mirror what the real SDK would do: atomically
    persist a message and return a reference for it.
    """

    name = "local-entire-broker"

    async def dispatch(
        self,
        session: AsyncSession,
        *,
        signal_id: UUID,
        channel: str,
        recipient: str,
        subject: str | None,
        body: str,
    ) -> DispatchResult:
        """Insert an outbox row and return the ``DispatchResult``."""
        result = await session.execute(
            text(
                """
                INSERT INTO outbox (signal_id, channel, recipient, subject, body)
                VALUES (:sid, :ch, :to, :subj, :body)
                RETURNING id, sent_at
                """
            ),
            {
                "sid": signal_id,
                "ch": channel,
                "to": recipient,
                "subj": subject,
                "body": body,
            },
        )
        row = result.one()
        log.info(
            "entire.dispatch.local",
            signal_id=str(signal_id),
            channel=channel,
            recipient=recipient,
            outbox_id=str(row.id),
        )
        return DispatchResult(
            outbox_id=row.id,
            channel=channel,
            recipient=recipient,
            sent_at=row.sent_at,
        )


_broker: EntireBroker = LocalEntireBroker()


def get_broker() -> EntireBroker:
    """Return the configured broker. Single-source-of-truth for dep injection."""
    return _broker


def set_broker(broker: EntireBroker) -> None:
    """Install a different broker. Intended for tests / the real SDK swap."""
    global _broker
    _broker = broker
