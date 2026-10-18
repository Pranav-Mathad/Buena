"""Backfill of the materialized property, building and Liegenschaft files."""

from __future__ import annotations

import structlog
from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from backend.db.session import get_session
from backend.pipeline.materializer import (
    materialize_all,
)

router = APIRouter()
log = structlog.get_logger(__name__)


class MaterializeAllResponse(BaseModel):
    """Per-tier write counts returned by ``POST /admin/materialize-all``."""

    properties: int
    buildings: int
    liegenschaften: int


@router.post("/materialize-all", response_model=MaterializeAllResponse)
async def materialize_all_endpoint(
    session: AsyncSession = Depends(get_session),
) -> MaterializeAllResponse:
    """Bootstrap the materialized file cache for every tier.

    Mirrors ``python -m connectors.cli materialize-all`` but reachable
    over HTTP, which is needed on Cloud Run where the CLI can't connect to
    Cloud SQL directly. Idempotent: re-running just refreshes
    ``last_rendered_at`` and ``content_md`` for every row.

    The applier hook propagates updates on every fact write going
    forward, so this endpoint only needs calling once: after the
    ``*_files`` cache tables are created on an existing database, or
    after a bulk load that bypassed the applier.
    """
    counts = await materialize_all(session)
    await session.commit()
    log.info("admin.materialize_all", **counts)
    return MaterializeAllResponse(**counts)
