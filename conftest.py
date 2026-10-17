"""Shared pytest fixtures for the backend, connectors and eval suites.

Database tests run against the Postgres at ``DATABASE_URL`` /
``DATABASE_URL_SYNC`` (``docker compose up -d postgres`` provides one).

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
"""

from __future__ import annotations

from collections.abc import AsyncIterator

import psycopg2
import pytest

from backend.config import get_settings
from backend.db import session as db_session


def _connect() -> psycopg2.extensions.connection:
    return psycopg2.connect(get_settings().database_url_sync, connect_timeout=3)


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

