"""Connector Protocol — common shape every customer integration honors.

A connector exposes two iterators:

- :meth:`Connector.pull` — full backfill. Yields events from oldest to
  newest. Idempotent: re-running on the same input produces the same
  ``(source, source_ref)`` tuples and is absorbed by the events
  ``UNIQUE (source, source_ref)`` constraint.
- :meth:`Connector.stream` — incremental updates only. Used by the
  scheduler / admin-driven "advance one day" path.

Both iterators yield :class:`ConnectorEvent` dataclasses whose fields
mirror exactly what :func:`backend.pipeline.events.insert_event`
expects, so the call site is a one-liner:

.. code-block:: python

    async for ev in connector.pull():
        await insert_event(
            session,
            source=ev.source,
            source_ref=ev.source_ref,
            raw_content=ev.raw_content,
            metadata=ev.metadata,
            property_id=ev.property_id,
        )
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Iterator
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Protocol, runtime_checkable
from uuid import UUID


@dataclass(frozen=True)
class ConnectorEvent:
    """Source-agnostic event the pipeline can ingest.

    Attributes:
        source: Stable label for this event family (e.g. ``"email"``,
            ``"bank"``, ``"invoice"``, ``"letter"``).
        source_ref: Stable per-event identifier; the
            ``(source, source_ref)`` pair must be unique across all
            history (see ``events`` table).
        raw_content: Human-readable body. PII MUST be redacted by the
            connector before yielding (see :mod:`connectors.redact`).
        metadata: Structured context (parsed headers, currency amounts,
            classifier outputs, …). PII redacted.
        property_id: Optional pre-routed property UUID. ``None`` means
            "let the router decide" — the worker / structured extractor
            will route on ingest.
        received_at: When the event happened from the customer's
            perspective. Defaults to ingestion time but should be the
            real timestamp whenever the source carries one (email Date,
            bank Valuta, invoice Datum, …).
        document_type: For PDF-derived events, the classified type
            (lease, invoice, mahnung, …). ``None`` for non-PDF sources.
            Phase 9 constraints read this field.
    """

    source: str
    source_ref: str
    raw_content: str
    metadata: dict[str, Any] = field(default_factory=dict)
    property_id: UUID | None = None
    received_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    document_type: str | None = None


@runtime_checkable
class Connector(Protocol):
    """Customer-agnostic data source.

    Every customer integration registers a connector that satisfies this
    Protocol. The composite (e.g. :mod:`connectors.buena_archive`) wires
    multiple connectors against a single customer's directory shape.
    """

    name: str

    def pull(self) -> Iterator[ConnectorEvent]:
        """Yield every event from the source, oldest first."""
        ...

    def stream(self) -> Iterator[ConnectorEvent]:
        """Yield only events newer than the connector's last cursor."""
        ...


@runtime_checkable
class AsyncConnector(Protocol):
    """Async variant for connectors that hit the network on each yield."""

    name: str

    def pull(self) -> AsyncIterator[ConnectorEvent]:
        """Yield every event from the source, oldest first."""
        ...

    def stream(self) -> AsyncIterator[ConnectorEvent]:
        """Yield only events newer than the connector's last cursor."""
        ...


class ConnectorError(RuntimeError):
    """Recoverable connector error. Message is safe to surface to the user."""


class DataMissing(ConnectorError):
    """The customer dataset is not present at the configured path."""
