"""Phase 11 Step 1 — single-event vocab-fix verification.

Re-runs the extractor on the canonical Ruhestörung event for the
hero property (WE 29). Charges the new ``phase11_vocab_verify``
cost-ledger label with a ``$0.10`` cap. Does NOT write the new
facts to the database — Step 2 will sweep the corpus.

Output: before-fact (current ``open_water_damage`` for hero), the
new ``ExtractionResult.facts_to_update`` list, and an explicit
comparison line stating whether Gemini picked ``noise_complaint``
this time.
"""

from __future__ import annotations

import asyncio
import json
from decimal import Decimal
from typing import Any
from uuid import UUID

import psycopg2
import structlog

from backend.config import get_settings
from backend.services.gemini import extract_facts
from backend.services.lang import detect_language
from connectors import cost_ledger

log = structlog.get_logger(__name__)


HERO_PROPERTY_ID = UUID("509393da-6806-49ef-9e59-3da0213008cd")
LEDGER_LABEL = "phase11_vocab_verify"
LEDGER_CAP_USD = Decimal("0.10")

# Gemini Pro pricing (matches eval/runner.py constants).
PRO_PROMPT_USD_PER_M = Decimal("1.25")
PRO_COMPLETION_USD_PER_M = Decimal("10.0")


def _gemini_call_cost(prompt_tokens: int, completion_tokens: int) -> Decimal:
    """Estimate USD cost of one Pro call from observed token counts."""
    cost = (
        Decimal(prompt_tokens) * PRO_PROMPT_USD_PER_M / Decimal(1_000_000)
        + Decimal(completion_tokens) * PRO_COMPLETION_USD_PER_M / Decimal(1_000_000)
    )
    return cost.quantize(Decimal("0.000001"))


def _fetch_canonical_event() -> dict[str, Any]:
    """Pull the hero's current open_water_damage event from Postgres.

    Filters to the noise-complaint pattern that surfaced the
    misclassification (per the analyst diagnostic).
    """
    settings = get_settings()
    sql = """
        SELECT e.id, e.source_ref, e.raw_content,
               e.metadata->>'subject' AS subj,
               f.id AS fact_id, f.value AS current_fact_value
        FROM events e
        JOIN facts f ON f.source_event_id = e.id
        WHERE f.property_id = %s
          AND f.section = 'maintenance'
          AND f.field = 'open_water_damage'
          AND f.superseded_by IS NULL
          AND (
            f.value ILIKE '%%lärm%%'
            OR f.value ILIKE '%%ruhe%%'
            OR f.value ILIKE '%%nachbarn%%'
          )
        LIMIT 1
    """
    with psycopg2.connect(settings.database_url_sync) as conn, conn.cursor() as cur:
        cur.execute(sql, (str(HERO_PROPERTY_ID),))
        row = cur.fetchone()
    if row is None:
        raise SystemExit("no canonical Ruhestörung event found for hero")
    return {
        "event_id": row[0],
        "source_ref": row[1],
        "raw_content": row[2],
        "subject": row[3],
        "fact_id": row[4],
        "current_fact_value": row[5],
    }


async def _amain() -> None:
    cost_ledger.ensure_label(LEDGER_LABEL, LEDGER_CAP_USD)
    state_before = cost_ledger.get_state(LEDGER_LABEL)

    event = _fetch_canonical_event()
    body_preview = (event["raw_content"] or "").strip()[:200]
    print("=" * 72)
    print("CANONICAL EVENT")
    print("=" * 72)
    print(f"event_id           : {event['event_id']}")
    print(f"source_ref         : {event['source_ref']}")
    print(f"subject            : {event['subject']}")
    print(f"body (first 200)   : {body_preview}")
    print()
    print("=" * 72)
    print("BEFORE — current fact in DB (will NOT be overwritten by this run)")
    print("=" * 72)
    print(f"section.field      : maintenance.open_water_damage")
    print(f"current value      : {event['current_fact_value']}")
    print()

    print("=" * 72)
    print("RE-EXTRACT — single Pro call, no internal retry, no rule fallback")
    print("=" * 72)
    # Bypass the wrapper in backend.pipeline.extractor so:
    #   - max_attempts=1 → exactly one HTTP call per script invocation
    #     (the user's spec is human-paced retries between invocations).
    #   - GeminiUnavailable (after the 1 attempt) propagates instead of
    #     silently falling through to the rule-based path, which has
    #     nothing to do with the JSON vocabulary and would make a 429
    #     look like a vocab regression.
    raw = event["raw_content"]
    lang = detect_language(raw)
    result = await extract_facts(
        property_name="Immanuelkirchstraße 26 WE 29",
        current_context_excerpt="(none — vocab-fix verification run)",
        source="email",
        raw_content=raw,
        lang=lang,
        max_attempts=1,
    )

    if (
        result.source == "gemini"
        and result.prompt_tokens is not None
        and result.completion_tokens is not None
    ):
        cost = _gemini_call_cost(int(result.prompt_tokens), int(result.completion_tokens))
        cost_ledger.charge(LEDGER_LABEL, cost)
        print(f"prompt_tokens      : {result.prompt_tokens}")
        print(f"completion_tokens  : {result.completion_tokens}")
        print(f"latency_ms         : {result.latency_ms:.0f}")
        print(f"call cost (USD)    : ${cost:.6f}")
    else:
        print(f"extractor source   : {result.source}  (rule-based, no Gemini call)")

    print()
    print(f"category           : {result.category}")
    print(f"priority           : {result.priority}")
    print()
    print("facts_to_update:")
    print(json.dumps(result.facts_to_update, indent=2, ensure_ascii=False))
    print()
    if result.uncertain:
        print("uncertain[]:")
        print(json.dumps(result.uncertain, indent=2, ensure_ascii=False))
        print()

    print("=" * 72)
    print("VERDICT")
    print("=" * 72)
    fields_picked = sorted({f.get("field", "") for f in result.facts_to_update})
    print(f"fields picked      : {fields_picked}")
    if "noise_complaint" in fields_picked:
        print(
            "OUTCOME            : ✓ vocab fix shifted Gemini to noise_complaint — "
            "proceed to Step 2"
        )
    elif "open_water_damage" in fields_picked:
        print(
            "OUTCOME            : ✗ Gemini still picked open_water_damage despite the "
            "noise_complaint field being available — needs anti_examples escalation"
        )
    else:
        print(
            f"OUTCOME            : ? Gemini picked something else: {fields_picked} — "
            "examine before deciding next step"
        )
    print()

    state_after = cost_ledger.get_state(LEDGER_LABEL)
    print("=" * 72)
    print("COST LEDGER (label=phase11_vocab_verify)")
    print("=" * 72)
    print(f"before             : ${state_before.cumulative_usd:.6f} / ${state_before.cap_usd}")
    print(f"after              : ${state_after.cumulative_usd:.6f} / ${state_after.cap_usd}")
    print(
        f"delta              : ${state_after.cumulative_usd - state_before.cumulative_usd:.6f}"
    )
    print()
    print("(facts NOT written to DB — Step 2 sweeps the corpus.)")


if __name__ == "__main__":
    asyncio.run(_amain())
