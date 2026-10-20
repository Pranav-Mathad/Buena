"""Admin API, assembled from one module per resource under ``/admin``."""

from fastapi import APIRouter

from backend.api.admin import (
    feed,
    materialize,
    onboarding,
    rejected,
    replay,
    uncertainties,
    unrouted,
)

router = APIRouter(prefix="/admin", tags=["admin"])
for module in (unrouted, feed, materialize, rejected, replay, onboarding, uncertainties):
    router.include_router(module.router)

__all__ = ["router"]
