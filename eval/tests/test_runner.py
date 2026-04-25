"""Smoke tests for the eval harness on a tiny fixture file."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from eval import runner
from eval.metrics import score_row


pytestmark = pytest.mark.asyncio


def _write_fixture(tmp_path: Path) -> Path:
    """Two-row JSONL fixture covering a positive + a negative case."""
    fixture = {
        "rows": [
            {
                "event_id": "FX-001",
                "source": "email",
                "raw_content": (
                    "From: lukas.weber@example.com\n"
                    "Subject: Heizung kalt seit gestern\n\n"
                    "Die Heizung in EH-005 funktioniert seit gestern nicht."
                ),
                "metadata": {"eh_id": "EH-005"},
                "ground_truth": {
                    "category": "maintenance",
                    "priority": "high",
                    "facts_to_update": [
                        {
                            "section": "maintenance",
                            "field": "latest_heating_issue",
                            "value": "Heizung kalt seit gestern",
                            "confidence_min": 0.7,
                        }
                    ],
                    "expected_property_alias": "EH-005",
                    "expected_scope": "property",
                    "notes": "fixture: heating",
                },
            },
            {
                "event_id": "FX-002",
                "source": "email",
                "raw_content": "Hi team, just checking in. Best, Klaus",
                "metadata": {},
                "ground_truth": {
                    "category": "other",
                    "priority": "low",
                    "facts_to_update": [],
                    "expected_property_alias": "",
                    "expected_scope": "unrouted",
                    "notes": "fixture: chitchat — should produce no facts",
                },
            },
        ]
    }
    out = tmp_path / "fixture.jsonl"
    out.write_text(
        "\n".join(json.dumps(r) for r in fixture["rows"]) + "\n",
        encoding="utf-8",
    )
    return out


def test_score_row_matches_keys_and_values_softly() -> None:
    expected = {
        "category": "maintenance",
        "priority": "high",
        "facts_to_update": [
            {
                "section": "maintenance",
                "field": "latest_heating_issue",
                "value": "Heizung kalt seit gestern",
                "confidence_min": 0.7,
            }
        ],
        "expected_scope": "property",
    }
    extracted = [
        {
            "section": "maintenance",
            "field": "latest_heating_issue",
            "value": "Heizung kalt seit gestern (gemeldet via email).",
            "confidence": 0.85,
        }
    ]
    row = score_row(
        event_id="FX-001",
        expected=expected,
        extracted_category="maintenance",
        extracted_priority="high",
        extracted_facts=extracted,
        extractor_source="rule",
        latency_ms=1.0,
        prompt_tokens=None,
        completion_tokens=None,
        extracted_scope="property",
    )
    assert row.category_correct
    assert row.routing_correct
    assert len(row.fact_matches) == 1
    fm = row.fact_matches[0]
    assert fm["key_matched"]
    assert fm["value_matched"]


def test_score_row_flags_missing_fact() -> None:
    expected = {
        "category": "maintenance",
        "priority": "high",
        "facts_to_update": [
            {
                "section": "maintenance",
                "field": "latest_water_issue",
                "value": "Leck",
                "confidence_min": 0.7,
            }
        ],
        "expected_scope": "property",
    }
    row = score_row(
        event_id="FX-X",
        expected=expected,
        extracted_category="maintenance",
        extracted_priority="medium",
        extracted_facts=[],  # extractor missed it
        extractor_source="rule",
        latency_ms=1.0,
        prompt_tokens=None,
        completion_tokens=None,
        extracted_scope="property",
    )
    assert row.fact_matches[0]["key_matched"] is False
    assert row.fact_matches[0]["value_matched"] is False


def test_load_ground_truth_raises_on_missing(tmp_path: Path) -> None:
    runner.GROUND_TRUTH_DIR = tmp_path  # type: ignore[assignment]
    with pytest.raises(FileNotFoundError):
        runner._load_ground_truth("does_not_exist")


async def test_runner_end_to_end_on_fixture(tmp_path, monkeypatch) -> None:
    """Full end-to-end against a tmpdir fixture: extractor + scorer + report."""
    fx = _write_fixture(tmp_path)
    # Point the runner at our tmp ground-truth file
    monkeypatch.setattr(runner, "GROUND_TRUTH_DIR", tmp_path)
    fx.rename(tmp_path / "fixture.jsonl")

    report, raw = await runner.run("fixture")
    assert report.n_rows == 2
    md = report.render_markdown()
    assert "Eval report — fixture" in md
    assert "Per-section P / R / F1" in md
    assert "Calibration curve" in md
    # Both rows scored, no exceptions
    assert len(raw) == 2
