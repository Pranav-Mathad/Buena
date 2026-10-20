"""Approval broker: what happens to a signal's message once a human approves it.

The signals API depends only on the :class:`ApprovalBroker` protocol. The
shipped implementation, :class:`LocalApprovalBroker`, records the message in
the ``outbox`` table and sends nothing; a broker that actually delivers email
or Slack messages can be installed with :func:`set_broker` without touching
callers. Whatever the implementation, it should still write the outbox row so
every approved action stays auditable.
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
class ApprovalBroker(Protocol):
    """Dispatches the message of an approved signal."""

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
        """Deliver (or record) the message for an approved signal."""
        ...


class LocalApprovalBroker:
    """Writes the message to ``outbox`` in the caller's transaction; sends nothing.

    Runs inside the approval request's session, so the outbox row, the status
    change and the ``approval_log`` entry commit together.
    """

    name = "local-outbox"

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
            "approval.dispatch.local",
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


_broker: ApprovalBroker = LocalApprovalBroker()


def get_broker() -> ApprovalBroker:
    """Return the installed broker."""
    return _broker


def set_broker(broker: ApprovalBroker) -> None:
    """Install a different broker, e.g. a test double or a delivering implementation."""
    global _broker
    _broker = broker
