"""Async SQLAlchemy engine + session factory.

A single engine is created lazily on first use. FastAPI routes should obtain
sessions via the :func:`get_session` dependency rather than touching the
factory directly — this keeps transactional lifecycles tied to request scope.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from functools import lru_cache

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from backend.config import get_settings


@lru_cache(maxsize=1)
def get_engine() -> AsyncEngine:
    """Return a cached async engine built from the configured database URL."""
    settings = get_settings()
    return create_async_engine(
        settings.database_url,
        echo=False,
        pool_pre_ping=True,
        future=True,
    )


@lru_cache(maxsize=1)
def get_sessionmaker() -> async_sessionmaker[AsyncSession]:
    """Return a cached async session factory bound to the engine."""
    return async_sessionmaker(
        bind=get_engine(),
        expire_on_commit=False,
        autoflush=False,
        class_=AsyncSession,
    )


async def get_session() -> AsyncIterator[AsyncSession]:
    """FastAPI dependency yielding a per-request :class:`AsyncSession`."""
    factory = get_sessionmaker()
    async with factory() as session:
        yield session
