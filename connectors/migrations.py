"""Additive migrations for Phase 8+ tables.

Phase 0's ``backend/db/schema.sql`` is the canonical schema for the
demo data model (Part VI of KEYSTONE). Everything Phase 8 introduces
is **additive** — new tables created with ``CREATE TABLE IF NOT
EXISTS`` so re-running is idempotent and existing demo databases
upgrade in place without a destructive reset.

Run :func:`apply_all` (sync) at CLI startup. Tests do the same against
the dev Postgres on :5433.
"""

from __future__ import annotations

import structlog

import psycopg2

from backend.config import get_settings

log = structlog.get_logger(__name__)


# Each migration is (name, sql). Names are descriptive only — the
# CREATE TABLE IF NOT EXISTS is what makes them safe to replay.
_MIGRATIONS: list[tuple[str, str]] = [
    (
        "0001_cost_ledger",
        """
        CREATE TABLE IF NOT EXISTS cost_ledger (
          id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
          source_label TEXT NOT NULL,
          cumulative_usd NUMERIC(12, 4) NOT NULL DEFAULT 0,
          cap_usd NUMERIC(12, 4) NOT NULL,
          hit_at TIMESTAMPTZ,
          updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          UNIQUE (source_label)
        );
        CREATE INDEX IF NOT EXISTS idx_cost_ledger_label ON cost_ledger(source_label);
        """,
    ),
]


def apply_all(connection_url: str | None = None) -> int:
    """Run every additive migration on the configured Postgres.

    Returns:
        The number of migrations actually executed (in our scheme that's
        always ``len(_MIGRATIONS)`` — Postgres absorbs the no-ops via
        ``IF NOT EXISTS``).
    """
    url = connection_url or get_settings().database_url_sync
    log.debug("connectors.migrations.apply", count=len(_MIGRATIONS))
    with psycopg2.connect(url) as conn:
        conn.autocommit = False
        with conn.cursor() as cur:
            for name, sql in _MIGRATIONS:
                cur.execute(sql)
                log.info("connectors.migrations.applied", name=name)
        conn.commit()
    return len(_MIGRATIONS)
