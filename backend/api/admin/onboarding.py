"""Onboarding view: a briefing for someone opening a property for the first time."""

from __future__ import annotations

from uuid import UUID

import structlog
from fastapi import APIRouter, Depends
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from backend.db.session import get_session

router = APIRouter()
log = structlog.get_logger(__name__)


class OnboardingResponse(BaseModel):
    """``GET /admin/properties/{id}/onboarding`` envelope."""

    property_id: UUID
    markdown: str
    cached: bool = Field(
        ...,
        description=(
            "True when the briefing came from the metadata.onboarding cache. "
            "False means Gemini Pro was invoked this request."
        ),
    )


@router.get(
    "/properties/{property_id}/onboarding",
    response_model=OnboardingResponse,
)
async def get_property_onboarding(
    property_id: UUID,
    session: AsyncSession = Depends(get_session),
) -> OnboardingResponse:
    """Render the first-time-read onboarding view for a property.

    Five sections: stammdaten + counts, open issues, Gemini Pro 12-month
    briefing (the only LLM call), recurring patterns, pointer index.
    The briefing is cached on ``properties.metadata.onboarding`` keyed by
    a hash of the latest fact/uncertainty/rejection timestamp, so a
    re-read is free, but any mutation invalidates.
    """
    from backend.services import onboarding  # noqa: PLC0415

    last_fact, last_unc, last_rej = await onboarding._last_mutation_timestamps(session, property_id)
    cache_key = onboarding._cache_key(last_fact, last_unc, last_rej)
    cached_briefing = await onboarding._read_cache(session, property_id, cache_key)

    try:
        markdown = await onboarding.render_onboarding(session, property_id)
    except ValueError as exc:
        from fastapi import HTTPException  # noqa: PLC0415

        raise HTTPException(status_code=404, detail=str(exc)) from exc

    log.info(
        "admin.onboarding.render",
        property_id=str(property_id),
        markdown_chars=len(markdown),
        cached=cached_briefing is not None,
    )
    return OnboardingResponse(
        property_id=property_id,
        markdown=markdown,
        cached=cached_briefing is not None,
    )
