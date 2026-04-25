"""Durable LLM-spend ledger.

Persists cumulative spend per ``source_label`` across CLI invocations
so a single user-defined cap (``--max-total-cost-usd``) governs the
entire backfill, not just one run. Hitting the cap mid-run sets
``hit_at`` and aborts the process cleanly; subsequent invocations read
the same row and abort *immediately* before issuing any LLM calls.

Reset is intentional and friction-gated — see
:func:`reset_label` and the ``--reset-cost-ledger`` CLI flag, which
prompts for ``y/N``.

Wiring:

- Every Gemini call site (extractor, drafter, doctype classifier) calls
  :func:`charge` *before* issuing the request, with an estimated cost.
  ``charge`` returns the running cumulative; the caller compares it
  against the cap and short-circuits when exceeded.
- For best-fit accounting, the caller can call :func:`record_actual`
  after the response lands to reconcile against measured token usage.
  Phase 8 keeps this simple: estimated cost == actual cost.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

import psycopg2
import structlog

from backend.config import get_settings
from connectors.migrations import apply_all as ensure_migrations

log = structlog.get_logger(__name__)


@dataclass(frozen=True)
class LedgerState:
    """Snapshot of one ``source_label`` row."""

    source_label: str
    cumulative_usd: Decimal
    cap_usd: Decimal
    exhausted: bool


class CostCapExceeded(RuntimeError):
    """Raised when a charge would push spend beyond ``cap_usd``."""

    def __init__(self, label: str, cumulative: Decimal, cap: Decimal) -> None:
        super().__init__(
            f"cost cap reached for label={label!r}: "
            f"cumulative=${cumulative:.4f} cap=${cap:.4f}"
        )
        self.label = label
        self.cumulative = cumulative
        self.cap = cap


def _connect(url: str | None = None) -> psycopg2.extensions.connection:
    """Open a sync Postgres connection. Internal."""
    return psycopg2.connect(url or get_settings().database_url_sync)


def get_state(label: str, *, url: str | None = None) -> LedgerState | None:
    """Return the current ledger row for ``label``, or ``None`` if absent."""
    with _connect(url) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT source_label, cumulative_usd, cap_usd, hit_at
            FROM cost_ledger
            WHERE source_label = %s
            """,
            (label,),
        )
        row = cur.fetchone()
    if row is None:
        return None
    return LedgerState(
        source_label=row[0],
        cumulative_usd=Decimal(row[1]),
        cap_usd=Decimal(row[2]),
        exhausted=row[3] is not None,
    )


def ensure_label(label: str, cap_usd: Decimal | float | str, *, url: str | None = None) -> LedgerState:
    """Create the ledger row if missing; return current state."""
    cap = Decimal(str(cap_usd))
    ensure_migrations(url)
    with _connect(url) as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO cost_ledger (source_label, cumulative_usd, cap_usd)
            VALUES (%s, 0, %s)
            ON CONFLICT (source_label) DO UPDATE
              SET cap_usd = EXCLUDED.cap_usd,
                  updated_at = now()
            RETURNING source_label, cumulative_usd, cap_usd, hit_at
            """,
            (label, cap),
        )
        row = cur.fetchone()
        conn.commit()
    return LedgerState(
        source_label=row[0],
        cumulative_usd=Decimal(row[1]),
        cap_usd=Decimal(row[2]),
        exhausted=row[3] is not None,
    )


def charge(
    label: str,
    amount_usd: Decimal | float | str,
    *,
    url: str | None = None,
) -> LedgerState:
    """Add ``amount_usd`` to ``label`` and return the post-charge state.

    Raises:
        CostCapExceeded: if the post-charge cumulative would exceed the
            stored ``cap_usd``. The row's ``hit_at`` is set on first
            breach so future calls also short-circuit.
    """
    delta = Decimal(str(amount_usd))
    if delta < 0:
        raise ValueError("cost charge must be non-negative")

    with _connect(url) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT cumulative_usd, cap_usd, hit_at FROM cost_ledger "
            "WHERE source_label = %s FOR UPDATE",
            (label,),
        )
        row = cur.fetchone()
        if row is None:
            raise RuntimeError(
                f"cost_ledger has no row for label={label!r}; call ensure_label first"
            )
        cumulative = Decimal(row[0]) + delta
        cap = Decimal(row[1])
        already_hit = row[2] is not None

        if already_hit or cumulative > cap:
            cur.execute(
                """
                UPDATE cost_ledger
                SET cumulative_usd = %s,
                    hit_at = COALESCE(hit_at, now()),
                    updated_at = now()
                WHERE source_label = %s
                """,
                (cumulative, label),
            )
            conn.commit()
            log.warning(
                "cost_ledger.cap_hit",
                label=label,
                cumulative=str(cumulative),
                cap=str(cap),
            )
            raise CostCapExceeded(label, cumulative, cap)

        cur.execute(
            """
            UPDATE cost_ledger
            SET cumulative_usd = %s, updated_at = now()
            WHERE source_label = %s
            """,
            (cumulative, label),
        )
        conn.commit()
    log.debug(
        "cost_ledger.charge",
        label=label,
        delta=str(delta),
        cumulative=str(cumulative),
        cap=str(cap),
    )
    return LedgerState(
        source_label=label,
        cumulative_usd=cumulative,
        cap_usd=cap,
        exhausted=False,
    )


def reset_label(label: str, *, url: str | None = None) -> None:
    """Delete the ledger row for ``label``. Used by ``--reset-cost-ledger``.

    The CLI prompts ``y/N`` before calling this.
    """
    with _connect(url) as conn, conn.cursor() as cur:
        cur.execute("DELETE FROM cost_ledger WHERE source_label = %s", (label,))
        conn.commit()
    log.info("cost_ledger.reset", label=label)
