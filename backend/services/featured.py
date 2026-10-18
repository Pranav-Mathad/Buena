"""Resolve the featured property from configuration.

``FEATURED_PROPERTY`` names the property the UI opens first and the one-click
replay targets. It accepts either a property UUID or any of the property's
aliases. Property UUIDs are generated per database, so the default is an alias
(the unit ID ``EH-029``) that resolves on every install of the sample data.
"""

from __future__ import annotations

from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from backend.config import get_settings


async def resolve_featured_property_id(session: AsyncSession) -> UUID | None:
    """Return the featured property's id, or ``None`` if the reference matches nothing."""
    ref = get_settings().featured_property.strip()
    if not ref:
        return None
    try:
        query, params = "SELECT id FROM properties WHERE id = :ref", {"ref": UUID(ref)}
    except ValueError:
        query = (
            "SELECT id FROM properties WHERE :ref = ANY(aliases) ORDER BY created_at LIMIT 1"
        )
        params = {"ref": ref}
    row = (await session.execute(text(query), params)).first()
    return UUID(str(row.id)) if row else None
