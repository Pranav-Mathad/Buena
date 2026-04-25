"""Runner: load ground truth, drive ``backend.pipeline.extractor.extract``, score.

```
python -m eval.runner --set emails_v1
python -m eval.runner --set emails_v1 --json
python -m eval.runner --set emails_v1 --out eval/runs/2026-04-25-step4.md
```

The runner is intentionally honest about its inputs:

- It calls ``backend.pipeline.extractor.extract`` directly. Whatever
  the extractor's current behaviour is (Gemini Flash if keyed, rule
  fallback otherwise) is what gets measured.
- Routing is evaluated using the same router the live worker would
  use for that source: ``route(raw_content)`` for emails / Slack /
  free-text events; ``route_structured(metadata, event_source=…)``
  for bank / invoice / letter / erp.
- All work runs in one event loop; tests are scored sequentially so
  the order in the markdown matches the JSONL order.

Ground-truth file format is documented in ``eval/README.md``.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from dataclasses import asdict
from pathlib import Path
from typing import Any

import structlog

from backend.db.session import get_sessionmaker
from backend.logging import configure_logging
from backend.pipeline.extractor import extract as run_extractor
from backend.pipeline.router import (
    route_structured,
    route_text_event,
)
from connectors.migrations import apply_all as ensure_migrations
from eval.metrics import Report, score_row

#: Sources that travel through the *text-based* router (route).
TEXT_ROUTER_SOURCES: frozenset[str] = frozenset({"email", "slack", "debug"})

#: Sources that travel through ``route_structured``.
STRUCTURED_ROUTER_SOURCES: frozenset[str] = frozenset(
    {"bank", "invoice", "letter", "erp", "web"}
)

log = structlog.get_logger("eval.runner")

REPO_ROOT = Path(__file__).resolve().parents[1]
GROUND_TRUTH_DIR = REPO_ROOT / "eval" / "ground_truth"


def _load_ground_truth(set_name: str) -> list[dict[str, Any]]:
    """Read ``eval/ground_truth/<set_name>.jsonl`` into memory."""
    path = GROUND_TRUTH_DIR / f"{set_name}.jsonl"
    if not path.is_file():
        raise FileNotFoundError(
            f"ground truth set {set_name!r} not found at {path}; "
            "see eval/README.md for the schema"
        )
    rows: list[dict[str, Any]] = []
    with path.open(encoding="utf-8") as handle:
        for line_no, line in enumerate(handle, 1):
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError as exc:
                raise ValueError(
                    f"{path}:{line_no} is not valid JSON — {exc}"
                ) from exc
    log.info("eval.ground_truth.loaded", set_name=set_name, rows=len(rows))
    return rows


async def _scope_for_event(
    *,
    source: str,
    raw_content: str,
    metadata: dict[str, Any],
) -> str:
    """Resolve the routed scope label using the same router the live worker uses.

    Returns one of ``property | building | liegenschaft | unrouted``.
    Best-effort: needs a working DB so the alias lookups + WEG fallback
    fire. When the DB is unreachable we degrade to the row's metadata
    (``eh_id`` → property; otherwise unrouted) so calibration metrics
    still complete.
    """
    try:
        ensure_migrations()
        factory = get_sessionmaker()
        async with factory() as session:
            if source in TEXT_ROUTER_SOURCES:
                text_route = await route_text_event(
                    session, raw_content, metadata=metadata
                )
                if text_route.property_id is not None:
                    return "property"
                if text_route.building_id is not None:
                    return "building"
                if text_route.liegenschaft_id is not None:
                    return "liegenschaft"
                return "unrouted"
            structured = await route_structured(
                session, metadata, event_source=source
            )
            if structured.property_id is not None:
                return "property"
            if structured.building_id is not None:
                return "building"
            if structured.liegenschaft_id is not None:
                return "liegenschaft"
            return "unrouted"
    except Exception as exc:  # noqa: BLE001 — degrade gracefully, log once
        log.warning("eval.routing.unavailable", error=str(exc))
        if metadata.get("eh_id") or "EH-" in raw_content or "WE " in raw_content:
            return "property"
        return "unrouted"


async def _score_one(row: dict[str, Any]) -> tuple[Any, dict[str, Any]]:
    """Run the extractor + router on one ground-truth row.

    Returns ``(scored_row, raw_extractor_payload)``.
    """
    event_id = str(row.get("event_id") or "?")
    metadata = dict(row.get("metadata") or {})
    raw_content = str(row.get("raw_content") or "")
    expected = dict(row.get("ground_truth") or {})

    expected_property_alias = expected.get("expected_property_alias", "")
    property_name = (
        f"property aliased {expected_property_alias}"
        if expected_property_alias
        else "(unspecified)"
    )

    result = await run_extractor(
        property_name=property_name,
        current_context_excerpt="(none — eval run)",
        source=str(row.get("source") or "email"),
        raw_content=raw_content,
    )

    extracted_scope = await _scope_for_event(
        source=str(row.get("source") or "email"),
        raw_content=raw_content,
        metadata=metadata,
    )

    scored = score_row(
        event_id=event_id,
        expected=expected,
        extracted_category=str(getattr(result, "category", "")),
        extracted_priority=str(getattr(result, "priority", "")),
        extracted_facts=list(getattr(result, "facts_to_update", [])),
        extractor_source=str(getattr(result, "source", "")),
        latency_ms=float(getattr(result, "latency_ms", 0.0) or 0.0),
        prompt_tokens=None,  # Phase 8 doesn't surface token counts in ExtractionResult
        completion_tokens=None,
        extracted_scope=extracted_scope,
    )

    raw_payload = {
        "event_id": event_id,
        "extracted_category": scored.extracted_category,
        "extracted_priority": scored.extracted_priority,
        "extracted_facts": scored.extracted_facts,
        "extracted_scope": extracted_scope,
        "category_correct": scored.category_correct,
        "routing_correct": scored.routing_correct,
        "fact_matches": scored.fact_matches,
        "spurious_facts": scored.spurious_facts,
    }
    return scored, raw_payload


async def run(set_name: str) -> tuple[Report, list[dict[str, Any]]]:
    """Run the eval set end-to-end. Returns (report, raw payloads)."""
    rows = _load_ground_truth(set_name)
    report = Report(set_name=set_name)
    raw: list[dict[str, Any]] = []
    for gt_row in rows:
        scored, payload = await _score_one(gt_row)
        report.rows.append(scored)
        raw.append(payload)
    return report, raw


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="eval.runner")
    p.add_argument("--set", required=True, help="Ground-truth set name (no .jsonl)")
    p.add_argument(
        "--json",
        action="store_true",
        help="Emit a single JSON dump of the report instead of markdown.",
    )
    p.add_argument(
        "--out",
        default=None,
        help="Optional markdown destination (e.g. eval/runs/2026-04-25-step4.md).",
    )
    return p


def main(argv: list[str] | None = None) -> int:
    """CLI entry point — returns a POSIX exit code."""
    configure_logging()
    args = _build_parser().parse_args(argv)
    try:
        report, raw_payloads = asyncio.run(run(args.set))
    except FileNotFoundError as exc:
        print(str(exc), file=sys.stderr)
        return 2

    if args.json:
        payload = {
            "set": report.set_name,
            "n_rows": report.n_rows,
            "category_accuracy": report.category_accuracy,
            "routing_accuracy": report.routing_accuracy,
            "category_stats": [asdict(s) for s in report.category_stats()],
            "calibration": [asdict(b) for b in report.calibration()],
            "tokens": report.total_tokens(),
            "rows": raw_payloads,
        }
        print(json.dumps(payload, indent=2, default=str))
    else:
        markdown = report.render_markdown()
        if args.out:
            out_path = Path(args.out)
            out_path.parent.mkdir(parents=True, exist_ok=True)
            out_path.write_text(markdown, encoding="utf-8")
            log.info("eval.report.written", path=str(out_path))
        print(markdown)
    return 0


if __name__ == "__main__":
    sys.exit(main())
