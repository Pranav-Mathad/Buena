"""Idempotent seeder for the Keystone demo dataset.

Run as ``python -m seed.seed``. Safe to re-run: existing records are matched on
natural keys (email, address, (source, source_ref), …) and left untouched.

The seeder applies :mod:`backend.db.schema` on a blank database, then inserts
owners, buildings, contractors, properties, tenants, relationships, events and
facts from :mod:`seed.realistic_data`. Events carry their original 6-month
timestamps; facts reference events via ``source_event_id`` so the living
markdown renders with real source links.
"""

from __future__ import annotations

import json
import sys
from collections.abc import Iterable
from pathlib import Path
from typing import Any, cast

import psycopg2
import psycopg2.extras
import structlog

from backend.config import get_settings
from backend.logging import configure_logging
from seed.realistic_data import (
    BUILDINGS,
    CONTRACTORS,
    OWNERS,
    PROPERTIES,
    BuildingSeed,
    ContractorSeed,
    EventSeed,
    FactSeed,
    OwnerSeed,
    PropertySeed,
    TenantSeed,
)

log = structlog.get_logger("seed")

SCHEMA_PATH = Path(__file__).resolve().parent.parent / "backend" / "db" / "schema.sql"

Cursor = psycopg2.extensions.cursor


def _schema_applied(cur: Cursor) -> bool:
    """Return True if the canonical ``properties`` table already exists."""
    cur.execute(
        "SELECT to_regclass('public.properties') IS NOT NULL AS exists;"
    )
    row = cur.fetchone()
    return bool(row and row[0])


def _apply_schema(cur: Cursor) -> None:
    """Run :file:`backend/db/schema.sql` against the database."""
    sql = SCHEMA_PATH.read_text(encoding="utf-8")
    log.info("seed.schema.apply", path=str(SCHEMA_PATH), bytes=len(sql))
    cur.execute(sql)


def _upsert_owner(cur: Cursor, owner: OwnerSeed) -> str:
    """Insert an owner if absent; return the UUID as a string."""
    cur.execute("SELECT id FROM owners WHERE email = %s", (owner.email,))
    row = cur.fetchone()
    if row:
        return cast(str, row[0])
    cur.execute(
        """
        INSERT INTO owners (name, email, phone, preferences)
        VALUES (%s, %s, %s, %s::jsonb)
        RETURNING id
        """,
        (owner.name, owner.email, owner.phone, json.dumps(owner.preferences)),
    )
    return cast(str, cur.fetchone()[0])


def _upsert_building(cur: Cursor, building: BuildingSeed) -> str:
    """Insert a building if absent; return the UUID as a string."""
    cur.execute("SELECT id FROM buildings WHERE address = %s", (building.address,))
    row = cur.fetchone()
    if row:
        return cast(str, row[0])
    cur.execute(
        """
        INSERT INTO buildings (address, year_built, metadata)
        VALUES (%s, %s, %s::jsonb)
        RETURNING id
        """,
        (building.address, building.year_built, json.dumps(building.metadata)),
    )
    return cast(str, cur.fetchone()[0])


def _upsert_contractor(cur: Cursor, contractor: ContractorSeed) -> str:
    """Insert a contractor if absent; return the UUID as a string."""
    cur.execute("SELECT id FROM contractors WHERE name = %s", (contractor.name,))
    row = cur.fetchone()
    if row:
        return cast(str, row[0])
    cur.execute(
        """
        INSERT INTO contractors (name, specialty, rating, contact)
        VALUES (%s, %s, %s, %s::jsonb)
        RETURNING id
        """,
        (contractor.name, contractor.specialty, contractor.rating, json.dumps(contractor.contact)),
    )
    return cast(str, cur.fetchone()[0])


def _upsert_property(
    cur: Cursor,
    prop: PropertySeed,
    owner_id: str,
    building_id: str,
) -> str:
    """Insert a property if absent; return the UUID as a string."""
    cur.execute("SELECT id FROM properties WHERE name = %s", (prop.name,))
    row = cur.fetchone()
    if row:
        return cast(str, row[0])
    cur.execute(
        """
        INSERT INTO properties (name, address, aliases, owner_id, building_id, metadata)
        VALUES (%s, %s, %s, %s, %s, %s::jsonb)
        RETURNING id
        """,
        (
            prop.name,
            prop.address,
            prop.aliases,
            owner_id,
            building_id,
            json.dumps({"seed_key": prop.key}),
        ),
    )
    return cast(str, cur.fetchone()[0])


def _upsert_tenant(cur: Cursor, property_id: str, tenant: TenantSeed) -> str:
    """Insert a tenant if absent; return the UUID as a string."""
    cur.execute(
        "SELECT id FROM tenants WHERE property_id = %s AND email = %s",
        (property_id, tenant.email),
    )
    row = cur.fetchone()
    if row:
        return cast(str, row[0])
    cur.execute(
        """
        INSERT INTO tenants (property_id, name, email, phone, move_in_date)
        VALUES (%s, %s, %s, %s, %s)
        RETURNING id
        """,
        (property_id, tenant.name, tenant.email, tenant.phone, tenant.move_in_date),
    )
    return cast(str, cur.fetchone()[0])


def _upsert_relationship(
    cur: Cursor,
    *,
    from_type: str,
    from_id: str,
    to_type: str,
    to_id: str,
    relationship_type: str,
) -> None:
    """Create a relationship edge if it does not already exist."""
    cur.execute(
        """
        SELECT 1 FROM relationships
        WHERE from_type = %s AND from_id = %s
          AND to_type = %s AND to_id = %s
          AND relationship_type = %s
        """,
        (from_type, from_id, to_type, to_id, relationship_type),
    )
    if cur.fetchone():
        return
    cur.execute(
        """
        INSERT INTO relationships (from_type, from_id, to_type, to_id, relationship_type)
        VALUES (%s, %s, %s, %s, %s)
        """,
        (from_type, from_id, to_type, to_id, relationship_type),
    )


def _upsert_event(
    cur: Cursor,
    property_id: str,
    event: EventSeed,
) -> str:
    """Insert an event keyed by (source, source_ref); return UUID as string.

    Seed events are stamped ``processed_at = received_at`` because their facts
    were authored by hand — we don't want the live pipeline to re-extract them
    on startup and produce noisy rule-based duplicates.
    """
    cur.execute(
        "SELECT id FROM events WHERE source = %s AND source_ref = %s",
        (event.source, event.key),
    )
    row = cur.fetchone()
    if row:
        return cast(str, row[0])
    cur.execute(
        """
        INSERT INTO events (source, source_ref, property_id, raw_content,
                            metadata, received_at, processed_at)
        VALUES (%s, %s, %s, %s, %s::jsonb, %s, %s)
        RETURNING id
        """,
        (
            event.source,
            event.key,
            property_id,
            event.raw_content,
            json.dumps(event.metadata),
            event.received_at,
            event.received_at,
        ),
    )
    return cast(str, cur.fetchone()[0])


def _upsert_fact(
    cur: Cursor,
    property_id: str,
    event_ids: dict[str, str],
    fact: FactSeed,
) -> None:
    """Insert a fact if an identical (property, section, field, source) tuple is absent."""
    event_id = event_ids[fact.event_key]
    cur.execute(
        """
        SELECT 1 FROM facts
        WHERE property_id = %s
          AND section = %s
          AND field = %s
          AND source_event_id = %s
        """,
        (property_id, fact.section, fact.field, event_id),
    )
    if cur.fetchone():
        return
    cur.execute(
        """
        INSERT INTO facts (property_id, section, field, value, source_event_id,
                           confidence, valid_from)
        VALUES (%s, %s, %s, %s, %s, %s, now())
        """,
        (property_id, fact.section, fact.field, fact.value, event_id, fact.confidence),
    )


def _contractor_id_for(
    prop: PropertySeed,
    contractor_ids: dict[str, str],
) -> Iterable[tuple[str, str]]:
    """Yield ``(contractor_key, contractor_id)`` tuples for the property."""
    for key in prop.contractor_keys:
        yield key, contractor_ids[key]


def seed(connection_url: str) -> dict[str, Any]:
    """Apply schema + seed data against the given database URL.

    Returns a summary dict usable for smoke assertions in tests or CLIs.
    """
    log.info("seed.connect", url=connection_url.split("@")[-1])
    with psycopg2.connect(connection_url) as conn:
        conn.autocommit = False
        with conn.cursor() as cur:
            if not _schema_applied(cur):
                _apply_schema(cur)
            else:
                log.info("seed.schema.skip", reason="already_applied")

            owner_ids = {owner.key: _upsert_owner(cur, owner) for owner in OWNERS}
            building_ids = {bld.key: _upsert_building(cur, bld) for bld in BUILDINGS}
            contractor_ids = {c.key: _upsert_contractor(cur, c) for c in CONTRACTORS}

            summary: dict[str, Any] = {
                "owners": len(owner_ids),
                "buildings": len(building_ids),
                "contractors": len(contractor_ids),
                "properties": 0,
                "tenants": 0,
                "events": 0,
                "facts": 0,
            }

            for prop in PROPERTIES:
                property_id = _upsert_property(
                    cur,
                    prop,
                    owner_ids[prop.owner_key],
                    building_ids[prop.building_key],
                )
                summary["properties"] += 1

                _upsert_relationship(
                    cur,
                    from_type="property",
                    from_id=property_id,
                    to_type="owner",
                    to_id=owner_ids[prop.owner_key],
                    relationship_type="owned_by",
                )
                _upsert_relationship(
                    cur,
                    from_type="property",
                    from_id=property_id,
                    to_type="building",
                    to_id=building_ids[prop.building_key],
                    relationship_type="in_building",
                )

                for tenant in prop.tenants:
                    tenant_id = _upsert_tenant(cur, property_id, tenant)
                    summary["tenants"] += 1
                    _upsert_relationship(
                        cur,
                        from_type="property",
                        from_id=property_id,
                        to_type="tenant",
                        to_id=tenant_id,
                        relationship_type="occupied_by",
                    )

                for _, contractor_id in _contractor_id_for(prop, contractor_ids):
                    _upsert_relationship(
                        cur,
                        from_type="property",
                        from_id=property_id,
                        to_type="contractor",
                        to_id=contractor_id,
                        relationship_type="serviced_by",
                    )

                event_ids: dict[str, str] = {}
                for event in prop.events:
                    event_ids[event.key] = _upsert_event(cur, property_id, event)
                    summary["events"] += 1

                for fact in prop.facts:
                    _upsert_fact(cur, property_id, event_ids, fact)
                    summary["facts"] += 1

                log.info(
                    "seed.property.done",
                    key=prop.key,
                    events=len(prop.events),
                    facts=len(prop.facts),
                )

        conn.commit()

    log.info("seed.done", **summary)
    return summary


def main() -> int:
    """CLI entry point — returns a POSIX exit code."""
    configure_logging()
    settings = get_settings()
    try:
        seed(settings.database_url_sync)
    except Exception:  # noqa: BLE001 — surface any failure clearly
        log.exception("seed.failed")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
