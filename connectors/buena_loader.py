"""Adapt Buena Stammdaten dicts onto Phase 0's seed dataclasses + upserts.

This module is the bridge between :mod:`connectors.buena_archive` (which
produces redacted dicts) and :mod:`seed.seed` (which owns the canonical
``_upsert_*`` helpers). Reusing the existing helpers keeps a single
ingestion path for **all** customer master data — Berliner 4B and the
Buena dataset travel through the same SQL.

Public entry point: :func:`load_stammdaten` runs the full upsert
sequence (owners → buildings → contractors → properties → tenants →
relationships) inside one transaction and returns a summary dict.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any, cast

import psycopg2
import structlog

from backend.config import get_settings
from connectors import buena_archive
from connectors.buena_archive import BuenaStammdaten
from connectors.migrations import apply_all as ensure_migrations
from seed.realistic_data import (
    BuildingSeed,
    ContractorSeed,
    OwnerSeed,
    PropertySeed,
    TenantSeed,
)
from seed.seed import (
    _apply_schema,
    _schema_applied,
    _set_building_liegenschaft,
    _upsert_building,
    _upsert_contractor,
    _upsert_liegenschaft,
    _upsert_owner,
    _upsert_property,
    _upsert_relationship,
    _upsert_tenant,
)

log = structlog.get_logger(__name__)


@dataclass
class LoadSummary:
    """What :func:`load_stammdaten` reports back to the CLI."""

    liegenschaften_total: int = 0
    liegenschaften_inserted_now: int = 0
    owners_total: int = 0
    owners_inserted_now: int = 0
    buildings_total: int = 0
    buildings_inserted_now: int = 0
    contractors_total: int = 0
    contractors_inserted_now: int = 0
    properties_total: int = 0
    properties_inserted_now: int = 0
    tenants_total: int = 0
    tenants_inserted_now: int = 0
    tenants_skipped_inactive: int = 0
    relationships_total: int = 0

    def as_json(self) -> dict[str, int]:
        """Serializable view used by the CLI's ``--json`` output mode."""
        return {
            "liegenschaften_total": self.liegenschaften_total,
            "liegenschaften_inserted_now": self.liegenschaften_inserted_now,
            "owners_total": self.owners_total,
            "owners_inserted_now": self.owners_inserted_now,
            "buildings_total": self.buildings_total,
            "buildings_inserted_now": self.buildings_inserted_now,
            "contractors_total": self.contractors_total,
            "contractors_inserted_now": self.contractors_inserted_now,
            "properties_total": self.properties_total,
            "properties_inserted_now": self.properties_inserted_now,
            "tenants_total": self.tenants_total,
            "tenants_inserted_now": self.tenants_inserted_now,
            "tenants_skipped_inactive": self.tenants_skipped_inactive,
            "relationships_total": self.relationships_total,
        }


def _owner_seed(payload: dict[str, Any]) -> OwnerSeed:
    """Build an :class:`OwnerSeed` from a redacted Buena owner dict."""
    eig_id = str(payload.get("buena_eig_id") or payload.get("name", ""))
    email = str(payload.get("email") or f"{eig_id.lower()}@example.com")
    return OwnerSeed(
        key=eig_id,
        name=str(payload.get("name") or eig_id),
        email=email,
        phone=str(payload.get("phone") or ""),
        preferences=dict(payload.get("preferences") or {}),
    )


def _building_seed(payload: dict[str, Any]) -> BuildingSeed:
    """Build a :class:`BuildingSeed` from a redacted Buena building dict."""
    return BuildingSeed(
        key=str(payload.get("buena_haus_id") or payload.get("address", "")),
        address=str(payload.get("address") or ""),
        year_built=int(payload.get("year_built") or 0) or 0,
        metadata=dict(payload.get("metadata") or {}),
    )


def _contractor_seed(payload: dict[str, Any]) -> ContractorSeed:
    """Build a :class:`ContractorSeed` from a redacted Buena contractor dict."""
    return ContractorSeed(
        key=str(payload.get("buena_dl_id") or payload.get("name", "")),
        name=str(payload.get("name") or ""),
        specialty=str(payload.get("specialty") or ""),
        rating=float(payload.get("rating") or 0.0),
        contact=dict(payload.get("contact") or {}),
    )


def _property_seed(
    payload: dict[str, Any],
    owner_key: str,
    building_key: str,
) -> PropertySeed:
    """Build a :class:`PropertySeed`. ``owner_key`` / ``building_key``
    are stable customer IDs we use only for downstream lookups; they
    are not stored on the row (the actual FK is the resolved UUID
    passed separately to ``_upsert_property``).
    """
    return PropertySeed(
        key=str(payload.get("buena_eh_id") or payload.get("name", "")),
        name=str(payload.get("name") or ""),
        address=str(payload.get("address") or ""),
        aliases=list(payload.get("aliases") or []),
        owner_key=owner_key,
        building_key=building_key,
        contractor_keys=[],
        tenants=[],
        events=[],
        facts=[],
        metadata=dict(payload.get("metadata") or {}),
    )


def _tenant_seed(payload: dict[str, Any]) -> TenantSeed:
    """Build a :class:`TenantSeed`. ``move_in_date`` is parsed
    upstream by :mod:`connectors.buena_archive` (already a ``date``)."""
    move_in = payload.get("move_in_date")
    if isinstance(move_in, str):
        try:
            move_in = date.fromisoformat(move_in)
        except ValueError:
            move_in = None
    if move_in is None:
        move_in = date(1970, 1, 1)
    return TenantSeed(
        name=str(payload.get("name") or ""),
        email=str(payload.get("email") or ""),
        phone=str(payload.get("phone") or ""),
        move_in_date=cast(date, move_in),
        metadata=dict(payload.get("metadata") or {}),
    )


def load_stammdaten(
    payload: BuenaStammdaten,
    *,
    connection_url: str | None = None,
) -> LoadSummary:
    """Apply the Stammdaten payload to Postgres in one transaction.

    Args:
        payload: Output of :func:`connectors.buena_archive.load_stammdaten`.
        connection_url: Optional override; otherwise reads
            ``settings.database_url_sync``.

    Returns:
        :class:`LoadSummary` with totals + how many rows were *new*
        on this invocation (idempotency reports zeros on re-runs).
    """
    url = connection_url or get_settings().database_url_sync
    log.info("buena.load_stammdaten.start", url=url.split("@")[-1])

    ensure_migrations(url)

    owner_uuid_by_key: dict[str, str] = {}
    building_uuid_by_key: dict[str, str] = {}
    liegenschaft_uuid_by_key: dict[str, str] = {}

    summary = LoadSummary(
        liegenschaften_total=len(payload.liegenschaften),
        owners_total=len(payload.owners),
        buildings_total=len(payload.buildings),
        contractors_total=len(payload.contractors),
        properties_total=len(payload.properties),
        tenants_total=len(payload.tenants),
    )

    with psycopg2.connect(url) as conn:
        conn.autocommit = False
        with conn.cursor() as cur:
            # Phase 0 schema is the contract; the seed module already knows
            # how to apply it idempotently. Re-using that here ensures the
            # canonical tables exist on a fresh DB before our additive
            # migrations + Buena rows land.
            if not _schema_applied(cur):
                _apply_schema(cur)

            # ---- Liegenschaften (WEG) — must come before buildings so we
            # can stamp building.liegenschaft_id during the building upsert.
            for raw in payload.liegenschaften:
                lie_key = str(raw.get("buena_liegenschaft_id") or "")
                before = (
                    _count(cur, "liegenschaften", "buena_liegenschaft_id", lie_key)
                    if lie_key
                    else 0
                )
                lie_uuid = _upsert_liegenschaft(
                    cur,
                    name=str(raw.get("name") or "WEG"),
                    buena_liegenschaft_id=lie_key or None,
                    metadata=dict(raw.get("metadata") or {}),
                )
                if before == 0:
                    summary.liegenschaften_inserted_now += 1
                liegenschaft_uuid_by_key[lie_key] = lie_uuid

            # ---- Owners ----
            for raw in payload.owners:
                seed = _owner_seed(raw)
                before = _count(cur, "owners", "email", seed.email)
                uid = _upsert_owner(cur, seed)
                if before == 0:
                    summary.owners_inserted_now += 1
                owner_uuid_by_key[seed.key] = uid

            # ---- Buildings + Haus → WEG link ----
            for raw in payload.buildings:
                seed = _building_seed(raw)
                before = _count(cur, "buildings", "address", seed.address)
                uid = _upsert_building(cur, seed)
                if before == 0:
                    summary.buildings_inserted_now += 1
                building_uuid_by_key[seed.key] = uid

                lie_key = str(raw.get("buena_liegenschaft_id") or "")
                lie_uuid = liegenschaft_uuid_by_key.get(lie_key)
                if lie_uuid is not None:
                    _set_building_liegenschaft(cur, uid, lie_uuid)

            # ---- Contractors ----
            for raw in payload.contractors:
                seed = _contractor_seed(raw)
                before = _count(cur, "contractors", "name", seed.name)
                _upsert_contractor(cur, seed)
                if before == 0:
                    summary.contractors_inserted_now += 1

            # ---- Properties + owner/building edges ----
            property_uuid_by_eh: dict[str, str] = {}
            for raw in payload.properties:
                owner_key = str(raw.get("buena_eig_id") or "")
                building_key = str(raw.get("buena_haus_id") or "")
                owner_uuid = owner_uuid_by_key.get(owner_key)
                building_uuid = building_uuid_by_key.get(building_key)
                if owner_uuid is None or building_uuid is None:
                    log.warning(
                        "buena.load.property.unresolved",
                        eh=raw.get("buena_eh_id"),
                        owner_key=owner_key,
                        building_key=building_key,
                    )
                    continue
                seed = _property_seed(raw, owner_key, building_key)
                before = _count(cur, "properties", "name", seed.name)
                uid = _upsert_property(cur, seed, owner_uuid, building_uuid)
                if before == 0:
                    summary.properties_inserted_now += 1
                property_uuid_by_eh[seed.key] = uid

                # Edges that are always true once the property exists.
                _add_edge(
                    cur, summary,
                    from_type="property", from_id=uid,
                    to_type="owner", to_id=owner_uuid,
                    rel="owned_by",
                )
                _add_edge(
                    cur, summary,
                    from_type="property", from_id=uid,
                    to_type="building", to_id=building_uuid,
                    rel="in_building",
                )

            # ---- Tenants (active only) + occupied_by edges ----
            for raw in payload.tenants:
                eh_id = str(raw.get("buena_eh_id") or "")
                property_uuid = property_uuid_by_eh.get(eh_id)
                if property_uuid is None:
                    log.warning(
                        "buena.load.tenant.unresolved", mie=raw.get("buena_mie_id"), eh=eh_id
                    )
                    continue
                metadata = raw.get("metadata") or {}
                if metadata.get("active") is False:
                    summary.tenants_skipped_inactive += 1
                    continue
                seed = _tenant_seed(raw)
                if not seed.email:
                    # _upsert_tenant uses email as natural key; skip rows
                    # without one rather than collapse them all together.
                    summary.tenants_skipped_inactive += 1
                    continue
                before = _count_tenant(cur, property_uuid, seed.email)
                uid = _upsert_tenant(cur, property_uuid, seed)
                if before == 0:
                    summary.tenants_inserted_now += 1
                _add_edge(
                    cur, summary,
                    from_type="property", from_id=property_uuid,
                    to_type="tenant", to_id=uid,
                    rel="occupied_by",
                )

        conn.commit()

    log.info("buena.load_stammdaten.done", **summary.as_json())
    return summary


def _count(cur: psycopg2.extensions.cursor, table: str, column: str, value: Any) -> int:
    """Pre-insert existence probe so the summary reports new vs existing."""
    cur.execute(
        f"SELECT COUNT(*) FROM {table} WHERE {column} = %s",
        (value,),
    )
    return int(cur.fetchone()[0] or 0)


def _count_tenant(cur: psycopg2.extensions.cursor, property_id: str, email: str) -> int:
    """Pre-insert tenant existence probe matching ``_upsert_tenant``'s key."""
    cur.execute(
        "SELECT COUNT(*) FROM tenants WHERE property_id = %s AND email = %s",
        (property_id, email),
    )
    return int(cur.fetchone()[0] or 0)


def _add_edge(
    cur: psycopg2.extensions.cursor,
    summary: LoadSummary,
    *,
    from_type: str,
    from_id: str,
    to_type: str,
    to_id: str,
    rel: str,
) -> None:
    """Idempotently add a relationship edge and bump the summary counter."""
    _upsert_relationship(
        cur,
        from_type=from_type,
        from_id=from_id,
        to_type=to_type,
        to_id=to_id,
        relationship_type=rel,
    )
    summary.relationships_total += 1


def load_from_disk(
    *,
    extracted_root: str | None = None,
    connection_url: str | None = None,
) -> LoadSummary:
    """Convenience wrapper — read from disk + load. Used by the CLI."""
    root = buena_archive.require_root(extracted_root)
    payload = buena_archive.load_stammdaten(root)
    return load_stammdaten(payload, connection_url=connection_url)
