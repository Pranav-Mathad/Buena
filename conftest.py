"""Shared pytest fixtures for the backend, connectors and eval suites.

Database tests run against the Postgres at ``DATABASE_URL`` /
``DATABASE_URL_SYNC`` (``docker compose up -d postgres`` provides one). Three
fixtures make that safe to do repeatedly against a developer's database:

``_fresh_engine`` (autouse)
    The application caches one async engine per process. Its asyncpg pool is
    bound to the event loop that created it, while pytest-asyncio gives every
    test a new loop, so a cached engine from an earlier test fails with
    "attached to a different loop". Each test gets a fresh engine and the old
    one is disposed afterwards.

``db``
    Requested by tests that need Postgres. Checks connectivity once per
    session and applies the schema and migrations. An unreachable database is
    a test *failure* with instructions, never a silent skip: a skipped
    integration test looks exactly like a passing one in a summary line.

``_no_leaked_rows`` (autouse, session)
    Counts rows in the core tables before and after the run and errors if any
    count changed. Tests must clean up what they create; this keeps the suite
    from polluting the database it runs against.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Iterator

import psycopg2
import pytest

from backend.config import get_settings
from backend.db import session as db_session

_GUARDED_TABLES = (
    "liegenschaften",
    "buildings",
    "properties",
    "tenants",
    "events",
    "facts",
    "signals",
    "approval_log",
    "outbox",
    "uncertainty_events",
    "rejected_updates",
    "failed_events",
    "replay_runs",
    "cost_ledger",
    "property_files",
    "building_files",
    "liegenschaft_files",
)


def _connect() -> psycopg2.extensions.connection:
    return psycopg2.connect(get_settings().database_url_sync, connect_timeout=3)


def _row_counts() -> dict[str, int]:
    """Row count per guarded table; tables that do not exist yet count as 0."""
    counts: dict[str, int] = {}
    with _connect() as conn, conn.cursor() as cur:
        for table in _GUARDED_TABLES:
            cur.execute("SELECT to_regclass(%s) IS NOT NULL", (f"public.{table}",))
            row = cur.fetchone()
            if row and row[0]:
                cur.execute(f"SELECT count(*) FROM {table}")  # noqa: S608 - fixed allow-list
                counts[table] = int(cur.fetchone()[0])
            else:
                counts[table] = 0
    conn.close()
    return counts


@pytest.fixture(scope="session")
def _db_error() -> str | None:
    """``None`` when Postgres accepts connections, otherwise the error text."""
    try:
        _connect().close()
    except psycopg2.OperationalError as exc:
        return str(exc).strip()
    return None


@pytest.fixture(scope="session")
def db(_db_error: str | None) -> None:
    """Require a reachable Postgres with the current schema applied."""
    if _db_error is not None:
        pytest.fail(
            f"Postgres is not reachable at {get_settings().database_url_sync}: {_db_error}\n"
            "Start it with `docker compose up -d postgres` (or point DATABASE_URL and "
            "DATABASE_URL_SYNC at another instance).",
            pytrace=False,
        )
    from connectors.migrations import apply_all  # noqa: PLC0415
    from seed.seed import _apply_schema, _schema_applied  # noqa: PLC0415

    with _connect() as conn, conn.cursor() as cur:
        if not _schema_applied(cur):
            _apply_schema(cur)
    conn.close()
    apply_all()


@pytest.fixture(autouse=True)
async def _fresh_engine() -> AsyncIterator[None]:
    db_session.get_engine.cache_clear()
    db_session.get_sessionmaker.cache_clear()
    yield
    if db_session.get_engine.cache_info().currsize:
        await db_session.get_engine().dispose()
    db_session.get_engine.cache_clear()
    db_session.get_sessionmaker.cache_clear()


@pytest.fixture(scope="session", autouse=True)
def _no_leaked_rows(_db_error: str | None) -> Iterator[None]:
    if _db_error is not None:
        yield
        return
    before = _row_counts()
    yield
    after = _row_counts()
    leaked = {t: (before[t], after[t]) for t in _GUARDED_TABLES if before[t] != after[t]}
    if leaked:
        detail = ", ".join(f"{t}: {b} -> {a}" for t, (b, a) in leaked.items())
        pytest.fail(f"test suite left rows behind: {detail}", pytrace=False)
