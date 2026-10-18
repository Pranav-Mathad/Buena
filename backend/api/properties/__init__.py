"""Properties API, assembled from one module per resource under ``/properties``."""

from fastapi import APIRouter

from backend.api.properties import activity, ask, facts, file, graph, inbox, listing

router = APIRouter(tags=["properties"])
# The prefix goes on each include rather than on ``router``: FastAPI refuses
# to include the listing route (path "") under an empty prefix.
# Listing and search first: their static paths are matched before the
# /{property_id}/... routes, as in the original single module.
for module in (listing, graph, file, facts, activity, ask, inbox):
    router.include_router(module.router, prefix="/properties")

__all__ = ["router"]
