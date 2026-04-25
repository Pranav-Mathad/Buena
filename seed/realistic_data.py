"""Hand-crafted seed data for the Keystone demo.

Data is keyed by stable string identifiers (``owner_key``, ``building_key``,
``event_key``, …) so :mod:`seed.seed` can resolve cross-references in a single
pass without relying on generated UUIDs. Every event carries a source and a
timestamp; every fact references an event by key so that :func:`render_markdown`
can emit the required ``[source: <event_id>]`` links.

Dates are anchored on ``DEMO_NOW`` (2026-04-24) so the seeded history always
spans the trailing six months relative to the demo.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Any

UTC = timezone.utc
DEMO_NOW: datetime = datetime(2026, 4, 24, 12, 0, 0, tzinfo=UTC)


def days_ago(days: int, hour: int = 9, minute: int = 0) -> datetime:
    """Return a timestamp ``days`` before :data:`DEMO_NOW` at ``hour:minute`` UTC."""
    base = DEMO_NOW - timedelta(days=days)
    return base.replace(hour=hour, minute=minute, second=0, microsecond=0)


@dataclass(frozen=True)
class OwnerSeed:
    """Owner record — matched on ``email`` for idempotent upserts."""

    key: str
    name: str
    email: str
    phone: str
    preferences: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class BuildingSeed:
    """Building record — matched on ``address`` for idempotent upserts."""

    key: str
    address: str
    year_built: int
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ContractorSeed:
    """Contractor record — matched on ``name`` for idempotent upserts."""

    key: str
    name: str
    specialty: str
    rating: float
    contact: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class TenantSeed:
    """Tenant record — matched on ``(property, email)`` for idempotency."""

    name: str
    email: str
    phone: str
    move_in_date: date
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class EventSeed:
    """Historical event. ``key`` maps to ``source_ref`` and must be unique per source."""

    key: str
    source: str
    raw_content: str
    received_at: datetime
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class FactSeed:
    """Current fact about a property, tied back to the event that produced it."""

    section: str
    field: str
    value: str
    confidence: float
    event_key: str


@dataclass(frozen=True)
class PropertySeed:
    """Full demo fixture for a single property."""

    key: str
    name: str
    address: str
    aliases: list[str]
    owner_key: str
    building_key: str
    contractor_keys: list[str]
    tenants: list[TenantSeed]
    events: list[EventSeed]
    facts: list[FactSeed]
    metadata: dict[str, Any] = field(default_factory=dict)


# -----------------------------------------------------------------------------
# Owners, buildings, contractors
# -----------------------------------------------------------------------------

OWNERS: list[OwnerSeed] = [
    OwnerSeed(
        key="maria_schmidt",
        name="Maria Schmidt",
        email="maria.schmidt@keystone.demo",
        phone="+49 30 1234 5678",
        preferences={"notify_channel": "email", "summary_cadence": "weekly"},
    ),
    OwnerSeed(
        key="klaus_hoffmann",
        name="Klaus Hoffmann",
        email="klaus.hoffmann@keystone.demo",
        phone="+49 40 9876 5432",
        preferences={"notify_channel": "email"},
    ),
    OwnerSeed(
        key="ingrid_mueller",
        name="Ingrid Müller",
        email="ingrid.mueller@keystone.demo",
        phone="+49 89 5544 3322",
        preferences={"notify_channel": "email", "summary_cadence": "monthly"},
    ),
]

BUILDINGS: list[BuildingSeed] = [
    BuildingSeed(
        key="berliner_12",
        address="Berliner Strasse 12, 10713 Berlin",
        year_built=1978,
        metadata={
            "units": 8,
            "floors": 5,
            "heating_system": "central gas boiler (Viessmann Vitoplex, 1987)",
            "notes": "pre-1990 stock; shared central boiler serves all units",
        },
    ),
    BuildingSeed(
        key="elbchaussee_88",
        address="Elbchaussee 88, 22763 Hamburg",
        year_built=2005,
        metadata={"units": 4, "floors": 3, "heating_system": "district heating"},
    ),
    BuildingSeed(
        key="leopoldstrasse_45",
        address="Leopoldstrasse 45, 80802 Munich",
        year_built=2015,
        metadata={"units": 12, "floors": 6, "heating_system": "geothermal heat pump"},
    ),
]

CONTRACTORS: list[ContractorSeed] = [
    ContractorSeed(
        key="bobs_plumbing",
        name="Bob's Plumbing",
        specialty="Heating & plumbing, pre-war Berlin stock",
        rating=4.6,
        contact={"email": "service@bobsplumbing.de", "phone": "+49 30 444 5566"},
    ),
    ContractorSeed(
        key="hanseatic_repair",
        name="Hanseatic Repair Co.",
        specialty="General building maintenance, Hamburg area",
        rating=4.3,
        contact={"email": "info@hanseatic-repair.de", "phone": "+49 40 222 3344"},
    ),
    ContractorSeed(
        key="alpenservice",
        name="Alpenservice GmbH",
        specialty="Modern building systems, Munich",
        rating=4.8,
        contact={"email": "team@alpenservice.de", "phone": "+49 89 333 2211"},
    ),
]


# -----------------------------------------------------------------------------
# Property 1 — Berliner Strasse 12, Apt 4B (demo hero)
# -----------------------------------------------------------------------------

_BERLIN_4B_EVENTS: list[EventSeed] = [
    EventSeed(
        key="berlin_4b_lease_2024",
        source="pdf",
        raw_content=(
            "Mietvertrag / Lease renewal\n"
            "Property: Berliner Strasse 12, Apt 4B, Berlin\n"
            "Tenants: Lukas Weber, Anna Weber\n"
            "Term: 2024-09-01 through 2026-08-31\n"
            "Monthly rent: EUR 1,200 (cold). Deposit: EUR 3,600.\n"
            "Utilities: EUR 180/month advance (heat, water, waste)."
        ),
        received_at=days_ago(178),
    ),
    EventSeed(
        key="berlin_4b_elevator_202510",
        source="email",
        raw_content=(
            "From: lukas.weber@tenant.demo\n"
            "Subject: Elevator noise — Apt 4B\n\n"
            "Hi, the elevator is making a grinding noise when it reaches the "
            "top floor. Nothing urgent but worth a look. — Lukas"
        ),
        received_at=days_ago(178),
    ),
    EventSeed(
        key="berlin_4b_heat_nov",
        source="email",
        raw_content=(
            "From: lukas.weber@tenant.demo\n"
            "Subject: Heating not working — Apt 4B\n\n"
            "The radiators are cold since last night. The boiler in the "
            "basement is making a loud rattling noise. Can someone come by? "
            "— Lukas"
        ),
        received_at=days_ago(161),
    ),
    EventSeed(
        key="berlin_4b_contractor_nov",
        source="slack",
        raw_content=(
            "[#ops-berliner12] Bob's Plumbing: visited 4B, bled radiators, "
            "pressure was low in the main loop. Reset boiler. Recommend full "
            "service on Vitoplex unit before winter peak."
        ),
        received_at=days_ago(160),
    ),
    EventSeed(
        key="berlin_4b_heat_jan",
        source="email",
        raw_content=(
            "From: anna.weber@tenant.demo\n"
            "Subject: Heating stopped again\n\n"
            "Heat cut out again overnight, no hot water this morning. Baby in "
            "the apartment — can we get this fixed today? — Anna"
        ),
        received_at=days_ago(104),
    ),
    EventSeed(
        key="berlin_4b_rent_q1",
        source="erp",
        raw_content=(
            '{"account": "BER-4B", "period": "2026-Q1", "rent_received_eur": 3600, '
            '"late_payments": 0, "balance_eur": 0}'
        ),
        received_at=days_ago(54),
    ),
    EventSeed(
        key="berlin_4b_heat_mar",
        source="email",
        raw_content=(
            "From: lukas.weber@tenant.demo\n"
            "Subject: Hot water intermittent\n\n"
            "Third time this winter — hot water keeps shutting off, boiler "
            "seems to kick off and on. Something is clearly not right with "
            "the heating system. — Lukas"
        ),
        received_at=days_ago(50),
    ),
    EventSeed(
        key="berlin_4b_tavily_neighborhood",
        source="web",
        raw_content=(
            "Tavily snapshot (Wilmersdorf): median 2BR cold rent EUR 1,280; "
            "Berlin Mietspiegel 2025 caps rent adjustments at CPI+1.5% for "
            "pre-1990 stock."
        ),
        received_at=days_ago(30),
    ),
]

_BERLIN_4B_FACTS: list[FactSeed] = [
    FactSeed("overview", "unit_type", "2-bedroom apartment, 78 sqm, 4th floor",
             0.98, "berlin_4b_lease_2024"),
    FactSeed("overview", "building_notes",
             "Part of Berliner Strasse 12 — pre-1990 building, shared central gas boiler.",
             0.95, "berlin_4b_lease_2024"),
    FactSeed("overview", "neighborhood",
             "Wilmersdorf, Berlin. Median 2BR cold rent EUR 1,280 (Mietspiegel 2025).",
             0.8, "berlin_4b_tavily_neighborhood"),
    FactSeed("tenants", "primary_tenants", "Lukas Weber and Anna Weber (2 adults, 1 infant)",
             0.97, "berlin_4b_lease_2024"),
    FactSeed("tenants", "tenant_contact", "lukas.weber@tenant.demo / anna.weber@tenant.demo",
             0.95, "berlin_4b_heat_jan"),
    FactSeed("tenants", "move_in_date", "2024-09-01", 0.99, "berlin_4b_lease_2024"),
    FactSeed("lease", "start_date", "2024-09-01", 0.99, "berlin_4b_lease_2024"),
    FactSeed("lease", "end_date", "2026-08-31", 0.99, "berlin_4b_lease_2024"),
    FactSeed("lease", "monthly_rent", "EUR 1,200 cold + EUR 180 utilities advance",
             0.99, "berlin_4b_lease_2024"),
    FactSeed("lease", "deposit", "EUR 3,600 held", 0.99, "berlin_4b_lease_2024"),
    FactSeed("maintenance", "elevator_noise_2025_10",
             "Elevator grinding noise reported on the top floor (non-urgent).",
             0.85, "berlin_4b_elevator_202510"),
    FactSeed("maintenance", "heating_issue_2025_11",
             "Cold radiators + rattling boiler — Bob's Plumbing bled radiators and reset unit.",
             0.92, "berlin_4b_heat_nov"),
    FactSeed("maintenance", "heating_issue_2026_01",
             "Overnight heat + hot water cutout; tenant has infant — escalated same day.",
             0.94, "berlin_4b_heat_jan"),
    FactSeed("maintenance", "heating_issue_2026_03",
             "Hot water intermittent, boiler cycling off; third heating incident in 5 months.",
             0.93, "berlin_4b_heat_mar"),
    FactSeed("maintenance", "preferred_contractor",
             "Bob's Plumbing — boiler specialist familiar with the Vitoplex unit.",
             0.9, "berlin_4b_contractor_nov"),
    FactSeed("financials", "rent_status_ytd",
             "2026-Q1 rent EUR 3,600 received in full; 0 late payments.",
             0.99, "berlin_4b_rent_q1"),
]

BERLIN_4B = PropertySeed(
    key="berlin_4b",
    name="Berliner Strasse 12, Apt 4B",
    address="Berliner Strasse 12, 10713 Berlin",
    aliases=["4B", "Apt 4B", "Berliner 4B", "Berliner Strasse 12 4B"],
    owner_key="maria_schmidt",
    building_key="berliner_12",
    contractor_keys=["bobs_plumbing"],
    tenants=[
        TenantSeed(
            name="Lukas Weber",
            email="lukas.weber@tenant.demo",
            phone="+49 151 1122 3344",
            move_in_date=date(2024, 9, 1),
        ),
        TenantSeed(
            name="Anna Weber",
            email="anna.weber@tenant.demo",
            phone="+49 151 2233 4455",
            move_in_date=date(2024, 9, 1),
        ),
    ],
    events=_BERLIN_4B_EVENTS,
    facts=_BERLIN_4B_FACTS,
)


# -----------------------------------------------------------------------------
# Property 2 — Berliner Strasse 12, Apt 2A (pair for cross-property signal)
# -----------------------------------------------------------------------------

_BERLIN_2A_EVENTS: list[EventSeed] = [
    EventSeed(
        key="berlin_2a_lease_2025",
        source="pdf",
        raw_content=(
            "Mietvertrag\n"
            "Property: Berliner Strasse 12, Apt 2A, Berlin\n"
            "Tenant: Felix Fischer\n"
            "Term: 2025-11-01 through 2027-10-31\n"
            "Monthly rent: EUR 1,050 (cold). Deposit: EUR 3,150."
        ),
        received_at=days_ago(175),
    ),
    EventSeed(
        key="berlin_2a_heat_dec",
        source="email",
        raw_content=(
            "From: felix.fischer@tenant.demo\n"
            "Subject: No heat overnight\n\n"
            "Woke up freezing this morning — radiators were stone cold from "
            "around 2am. Came back on by 8am. — Felix"
        ),
        received_at=days_ago(127),
    ),
    EventSeed(
        key="berlin_2a_rent_jan",
        source="erp",
        raw_content='{"account": "BER-2A", "period": "2026-01", "rent_received_eur": 1050}',
        received_at=days_ago(93),
    ),
    EventSeed(
        key="berlin_2a_heat_feb",
        source="email",
        raw_content=(
            "From: felix.fischer@tenant.demo\n"
            "Subject: Boiler making loud noise\n\n"
            "Every night around midnight the boiler starts banging — it's "
            "loud enough that I can hear it through the floor. — Felix"
        ),
        received_at=days_ago(81),
    ),
    EventSeed(
        key="berlin_2a_contractor_feb",
        source="slack",
        raw_content=(
            "[#ops-berliner12] Bob's Plumbing: second visit to 2A. "
            "Expansion vessel pressure dropping. Recommend full boiler "
            "service before next heating cycle."
        ),
        received_at=days_ago(63),
    ),
    EventSeed(
        key="berlin_2a_heat_mar",
        source="email",
        raw_content=(
            "From: felix.fischer@tenant.demo\n"
            "Subject: Heating inconsistent\n\n"
            "Warm in the morning, cold at night. Been happening all week. "
            "— Felix"
        ),
        received_at=days_ago(35),
    ),
    EventSeed(
        key="berlin_2a_compliance_apr",
        source="pdf",
        raw_content=(
            "Gebäudeversicherung certificate — policy no. HDI-BER-2024-884. "
            "Property: Berliner Strasse 12. Valid through 2026-12-31."
        ),
        received_at=days_ago(19),
    ),
]

_BERLIN_2A_FACTS: list[FactSeed] = [
    FactSeed("overview", "unit_type", "1-bedroom apartment, 52 sqm, 2nd floor",
             0.97, "berlin_2a_lease_2025"),
    FactSeed("overview", "building_notes",
             "Part of Berliner Strasse 12 — shared central boiler with Apt 4B.",
             0.95, "berlin_2a_lease_2025"),
    FactSeed("tenants", "primary_tenants", "Felix Fischer (single occupant)",
             0.98, "berlin_2a_lease_2025"),
    FactSeed("tenants", "tenant_contact", "felix.fischer@tenant.demo",
             0.95, "berlin_2a_heat_dec"),
    FactSeed("tenants", "move_in_date", "2025-11-01", 0.99, "berlin_2a_lease_2025"),
    FactSeed("lease", "start_date", "2025-11-01", 0.99, "berlin_2a_lease_2025"),
    FactSeed("lease", "end_date", "2027-10-31", 0.99, "berlin_2a_lease_2025"),
    FactSeed("lease", "monthly_rent", "EUR 1,050 cold",
             0.99, "berlin_2a_lease_2025"),
    FactSeed("lease", "deposit", "EUR 3,150 held", 0.99, "berlin_2a_lease_2025"),
    FactSeed("maintenance", "heating_issue_2025_12",
             "Overnight no-heat incident — radiators cold 2am–8am.",
             0.93, "berlin_2a_heat_dec"),
    FactSeed("maintenance", "heating_issue_2026_02",
             "Boiler banging audibly at night; expansion vessel losing pressure.",
             0.93, "berlin_2a_heat_feb"),
    FactSeed("maintenance", "heating_issue_2026_03",
             "Heating output inconsistent throughout the day.",
             0.9, "berlin_2a_heat_mar"),
    FactSeed("maintenance", "contractor_recommendation",
             "Bob's Plumbing recommends full service on Vitoplex boiler.",
             0.92, "berlin_2a_contractor_feb"),
    FactSeed("financials", "last_rent_payment",
             "EUR 1,050 received for 2026-01 via ERP.",
             0.99, "berlin_2a_rent_jan"),
    FactSeed("compliance", "building_insurance",
             "HDI policy HDI-BER-2024-884 valid through 2026-12-31.",
             0.97, "berlin_2a_compliance_apr"),
]

BERLIN_2A = PropertySeed(
    key="berlin_2a",
    name="Berliner Strasse 12, Apt 2A",
    address="Berliner Strasse 12, 10713 Berlin",
    aliases=["2A", "Apt 2A", "Berliner 2A", "Berliner Strasse 12 2A"],
    owner_key="maria_schmidt",
    building_key="berliner_12",
    contractor_keys=["bobs_plumbing"],
    tenants=[
        TenantSeed(
            name="Felix Fischer",
            email="felix.fischer@tenant.demo",
            phone="+49 151 3344 5566",
            move_in_date=date(2025, 11, 1),
        ),
    ],
    events=_BERLIN_2A_EVENTS,
    facts=_BERLIN_2A_FACTS,
)


# -----------------------------------------------------------------------------
# Property 3 — Hamburg, Elbchaussee 88
# -----------------------------------------------------------------------------

_HAMBURG_EVENTS: list[EventSeed] = [
    EventSeed(
        key="hamburg_lease_2024",
        source="pdf",
        raw_content=(
            "Mietvertrag\n"
            "Property: Elbchaussee 88, 22763 Hamburg\n"
            "Tenant: Sophie Becker\n"
            "Term: 2024-11-01 through 2026-10-31\n"
            "Monthly rent: EUR 1,650 cold. Deposit: EUR 4,950."
        ),
        received_at=days_ago(170),
    ),
    EventSeed(
        key="hamburg_plumbing_dec",
        source="email",
        raw_content=(
            "From: sophie.becker@tenant.demo\n"
            "Subject: Kitchen sink leaking\n\n"
            "The kitchen sink has been dripping under the cabinet. Not urgent "
            "but starting to warp the wood. — Sophie"
        ),
        received_at=days_ago(141),
    ),
    EventSeed(
        key="hamburg_contractor_dec",
        source="slack",
        raw_content=(
            "[#ops-hamburg] Hanseatic Repair: replaced kitchen P-trap at "
            "Elbchaussee 88. Cabinet base dried, no further damage."
        ),
        received_at=days_ago(139),
    ),
    EventSeed(
        key="hamburg_rent_jan",
        source="erp",
        raw_content='{"account": "HAM-88", "period": "2026-01", "rent_received_eur": 1650}',
        received_at=days_ago(100),
    ),
    EventSeed(
        key="hamburg_permit_feb",
        source="email",
        raw_content=(
            "From: klaus.hoffmann@keystone.demo\n"
            "Subject: Balcony permit question\n\n"
            "Sophie asked about adding a small planter on the balcony rail. "
            "Anything we need to check with the Bezirksamt? — Klaus"
        ),
        received_at=days_ago(73),
    ),
    EventSeed(
        key="hamburg_compliance_mar",
        source="pdf",
        raw_content=(
            "Hamburg Bezirksamt letter: revised facade maintenance guidance "
            "for Altona waterfront properties. Inspection recommended every "
            "24 months."
        ),
        received_at=days_ago(43),
    ),
    EventSeed(
        key="hamburg_keys_apr",
        source="email",
        raw_content=(
            "From: sophie.becker@tenant.demo\n"
            "Subject: Key replacement request\n\n"
            "Lost my front door key on the ferry yesterday. Can we get a "
            "replacement set? — Sophie"
        ),
        received_at=days_ago(14),
    ),
]

_HAMBURG_FACTS: list[FactSeed] = [
    FactSeed("overview", "unit_type", "3-bedroom waterfront apartment, 118 sqm",
             0.97, "hamburg_lease_2024"),
    FactSeed("overview", "building_notes",
             "Elbchaussee 88 — 2005 build, Altona waterfront, district heating.",
             0.95, "hamburg_lease_2024"),
    FactSeed("tenants", "primary_tenants", "Sophie Becker (single occupant)",
             0.97, "hamburg_lease_2024"),
    FactSeed("tenants", "tenant_contact", "sophie.becker@tenant.demo",
             0.95, "hamburg_plumbing_dec"),
    FactSeed("tenants", "move_in_date", "2024-11-01", 0.99, "hamburg_lease_2024"),
    FactSeed("lease", "start_date", "2024-11-01", 0.99, "hamburg_lease_2024"),
    FactSeed("lease", "end_date", "2026-10-31", 0.99, "hamburg_lease_2024"),
    FactSeed("lease", "monthly_rent", "EUR 1,650 cold", 0.99, "hamburg_lease_2024"),
    FactSeed("lease", "deposit", "EUR 4,950 held", 0.99, "hamburg_lease_2024"),
    FactSeed("maintenance", "kitchen_leak_2025_12",
             "Kitchen P-trap leak resolved by Hanseatic Repair; no structural damage.",
             0.94, "hamburg_contractor_dec"),
    FactSeed("maintenance", "preferred_contractor",
             "Hanseatic Repair Co. — general maintenance, Hamburg.",
             0.9, "hamburg_contractor_dec"),
    FactSeed("maintenance", "key_replacement_2026_04",
             "Front door key replacement requested by tenant.",
             0.88, "hamburg_keys_apr"),
    FactSeed("financials", "last_rent_payment",
             "EUR 1,650 received for 2026-01 via ERP.",
             0.99, "hamburg_rent_jan"),
    FactSeed("compliance", "facade_inspection_guidance",
             "Altona Bezirksamt recommends 24-month facade inspection cadence.",
             0.9, "hamburg_compliance_mar"),
    FactSeed("compliance", "balcony_permit_note",
             "Owner investigating balcony planter permitting with Bezirksamt.",
             0.75, "hamburg_permit_feb"),
]

HAMBURG = PropertySeed(
    key="hamburg_88",
    name="Elbchaussee 88",
    address="Elbchaussee 88, 22763 Hamburg",
    aliases=["Elbchaussee 88", "Hamburg 88", "Altona 88"],
    owner_key="klaus_hoffmann",
    building_key="elbchaussee_88",
    contractor_keys=["hanseatic_repair"],
    tenants=[
        TenantSeed(
            name="Sophie Becker",
            email="sophie.becker@tenant.demo",
            phone="+49 151 4455 6677",
            move_in_date=date(2024, 11, 1),
        ),
    ],
    events=_HAMBURG_EVENTS,
    facts=_HAMBURG_FACTS,
)


# -----------------------------------------------------------------------------
# Property 4 — Munich, Leopoldstrasse 45
# -----------------------------------------------------------------------------

_MUNICH_EVENTS: list[EventSeed] = [
    EventSeed(
        key="munich_lease_2025",
        source="pdf",
        raw_content=(
            "Mietvertrag\n"
            "Property: Leopoldstrasse 45, 80802 Munich\n"
            "Tenants: Maximilian Bauer, Julia Bauer\n"
            "Term: 2025-11-01 through 2027-10-31\n"
            "Monthly rent: EUR 1,850 cold. Deposit: EUR 5,550."
        ),
        received_at=days_ago(173),
    ),
    EventSeed(
        key="munich_washer_dec",
        source="email",
        raw_content=(
            "From: julia.bauer@tenant.demo\n"
            "Subject: Washing machine not draining\n\n"
            "The built-in washing machine is stopping mid-cycle with an E18 "
            "code. Water just sits in the drum. — Julia"
        ),
        received_at=days_ago(125),
    ),
    EventSeed(
        key="munich_contractor_dec",
        source="slack",
        raw_content=(
            "[#ops-munich] Alpenservice: cleared washing machine drain pump "
            "at Leo 45. Back in service same day."
        ),
        received_at=days_ago(124),
    ),
    EventSeed(
        key="munich_rent_jan",
        source="erp",
        raw_content='{"account": "MUC-45", "period": "2026-01", "rent_received_eur": 1850}',
        received_at=days_ago(89),
    ),
    EventSeed(
        key="munich_internet_feb",
        source="email",
        raw_content=(
            "From: maximilian.bauer@tenant.demo\n"
            "Subject: Fibre installation\n\n"
            "Deutsche Glasfaser will be running lines in the building next "
            "month. Do we need approval to route the cable to our apartment? "
            "— Max"
        ),
        received_at=days_ago(65),
    ),
    EventSeed(
        key="munich_tavily_mar",
        source="web",
        raw_content=(
            "Tavily snapshot (Schwabing, Munich): median 2BR cold rent "
            "EUR 1,920. Munich Mietpreisbremse extended through 2029."
        ),
        received_at=days_ago(40),
    ),
    EventSeed(
        key="munich_parking_apr",
        source="email",
        raw_content=(
            "From: julia.bauer@tenant.demo\n"
            "Subject: Parking spot transfer\n\n"
            "We'd like to switch our underground spot with apartment 3C's, "
            "they have the wider one. Both parties agree. — Julia"
        ),
        received_at=days_ago(19),
    ),
]

_MUNICH_FACTS: list[FactSeed] = [
    FactSeed("overview", "unit_type", "2-bedroom apartment, 88 sqm, 5th floor",
             0.97, "munich_lease_2025"),
    FactSeed("overview", "building_notes",
             "Leopoldstrasse 45 — 2015 build, Schwabing, geothermal heat pump.",
             0.95, "munich_lease_2025"),
    FactSeed("overview", "neighborhood",
             "Schwabing. Median 2BR cold rent EUR 1,920 (Tavily / Mietspiegel).",
             0.8, "munich_tavily_mar"),
    FactSeed("tenants", "primary_tenants", "Maximilian Bauer and Julia Bauer (couple)",
             0.97, "munich_lease_2025"),
    FactSeed("tenants", "tenant_contact",
             "maximilian.bauer@tenant.demo / julia.bauer@tenant.demo",
             0.95, "munich_washer_dec"),
    FactSeed("tenants", "move_in_date", "2025-11-01", 0.99, "munich_lease_2025"),
    FactSeed("lease", "start_date", "2025-11-01", 0.99, "munich_lease_2025"),
    FactSeed("lease", "end_date", "2027-10-31", 0.99, "munich_lease_2025"),
    FactSeed("lease", "monthly_rent", "EUR 1,850 cold", 0.99, "munich_lease_2025"),
    FactSeed("lease", "deposit", "EUR 5,550 held", 0.99, "munich_lease_2025"),
    FactSeed("maintenance", "washing_machine_2025_12",
             "Washing machine E18 drain fault cleared by Alpenservice same day.",
             0.93, "munich_contractor_dec"),
    FactSeed("maintenance", "preferred_contractor",
             "Alpenservice GmbH — building systems, Munich.",
             0.9, "munich_contractor_dec"),
    FactSeed("maintenance", "fibre_install_note",
             "Tenant inquiring about Deutsche Glasfaser cable routing permission.",
             0.82, "munich_internet_feb"),
    FactSeed("financials", "last_rent_payment",
             "EUR 1,850 received for 2026-01 via ERP.",
             0.99, "munich_rent_jan"),
    FactSeed("compliance", "mietpreisbremse_note",
             "Munich Mietpreisbremse confirmed extended through 2029.",
             0.88, "munich_tavily_mar"),
    FactSeed("activity", "parking_transfer_request_2026_04",
             "Tenant requests underground parking spot swap with Apt 3C — mutual agreement.",
             0.82, "munich_parking_apr"),
]

MUNICH = PropertySeed(
    key="munich_45",
    name="Leopoldstrasse 45",
    address="Leopoldstrasse 45, 80802 Munich",
    aliases=["Leopoldstrasse 45", "Leo 45", "Munich 45", "Schwabing 45"],
    owner_key="ingrid_mueller",
    building_key="leopoldstrasse_45",
    contractor_keys=["alpenservice"],
    tenants=[
        TenantSeed(
            name="Maximilian Bauer",
            email="maximilian.bauer@tenant.demo",
            phone="+49 151 6677 8899",
            move_in_date=date(2025, 11, 1),
        ),
        TenantSeed(
            name="Julia Bauer",
            email="julia.bauer@tenant.demo",
            phone="+49 151 7788 9900",
            move_in_date=date(2025, 11, 1),
        ),
    ],
    events=_MUNICH_EVENTS,
    facts=_MUNICH_FACTS,
)


PROPERTIES: list[PropertySeed] = [BERLIN_4B, BERLIN_2A, HAMBURG, MUNICH]
