"""Onboarding view: a briefing for someone opening a property for the first time."""

from __future__ import annotations

from uuid import UUID

import structlog
from fastapi import APIRouter, Depends
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from backend.db.session import get_session
from backend.services.onboarding import BriefingSource

router = APIRouter()
log = structlog.get_logger(__name__)


class OnboardingResponse(BaseModel):
    """``GET /admin/properties/{id}/onboarding`` envelope."""

    property_id: UUID
    markdown: str
    briefing_source: BriefingSource = Field(
        ...,
        description=(
            "Where the Key context briefing came from: 'llm' (written by the "
            "configured provider on this request), 'cache' (metadata.onboarding) "
            "or 'unavailable' (no provider configured or reachable; the section "
            "is a one-line note and every other section is complete)."
        ),
    )
    cached: bool = Field(..., description="Same as briefing_source == 'cache'.")


@router.get(
    "/properties/{property_id}/onboarding",
    response_model=OnboardingResponse,
)
async def get_property_onboarding(
    property_id: UUID,
    session: AsyncSession = Depends(get_session),
) -> OnboardingResponse:
    """Render the first-time-read onboarding view for a property.

    Five sections: stammdaten + counts, open issues, an LLM-written
    12-month briefing (the only LLM call), recurring patterns, pointer index.
    The briefing is cached on ``properties.metadata.onboarding`` keyed by
    a hash of the latest fact/uncertainty/rejection timestamp, so a
    re-read is free, but any mutation invalidates.
    """
    from backend.services import onboarding  # noqa: PLC0415

    try:
        document = await onboarding.render_onboarding_document(session, property_id)
    except ValueError as exc:
        from fastapi import HTTPException  # noqa: PLC0415

        raise HTTPException(status_code=404, detail=str(exc)) from exc

    log.info(
        "admin.onboarding.render",
        property_id=str(property_id),
        markdown_chars=len(document.markdown),
        briefing_source=document.briefing_source,
    )
    return OnboardingResponse(
        property_id=property_id,
        markdown=document.markdown,
        briefing_source=document.briefing_source,
        cached=document.briefing_source == "cache",
    )
