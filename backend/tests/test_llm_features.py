"""Ask, reply drafting and the semantic check work with Gemini alone, and degrade cleanly.

Before the provider interface these three features called Pioneer directly:
with only GEMINI_API_KEY set, Ask returned 503 "PIONEER_API_KEY not set" and
the worker logged a skipped semantic check for every event.
"""

from __future__ import annotations

import uuid
from collections.abc import AsyncIterator
from typing import Any

import httpx
import pytest
from sqlalchemy import text
from structlog.testing import capture_logs

from backend.config import get_settings
from backend.db.session import get_sessionmaker
from backend.pipeline import semantic_validator
from backend.pipeline.events import insert_event
from backend.pipeline.worker import process_specific
from backend.services import llm
from backend.services.llm.base import JsonCompletion, LLMNotConfiguredError
from backend.services.llm.gemini import GeminiProvider


def _client() -> httpx.AsyncClient:
    from backend.main import app  # noqa: PLC0415

    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test")


@pytest.fixture
def gemini_only(monkeypatch: pytest.MonkeyPatch) -> list[dict[str, Any]]:
    """Configure Gemini (and nothing else) and record its JSON calls."""
    monkeypatch.setenv("GEMINI_API_KEY", "test-key")
    get_settings.cache_clear()
    calls: list[dict[str, Any]] = []
    replies: dict[str, dict[str, Any]] = {
        "ask": {
            "status": "answered",
            "answer": "The heating was reported broken.",
            "confidence": "high",
            "reasoning": "One email reports it.",
            "cited_event_ids": [],
            "partial_context_event_ids": [],
        },
        "draft": {"subject": "Re: Heating", "body": "We are on it.", "knows_about_incident": False},
        "audit": {
            "verdict": "has_contradictions",
            "contradictions": [
                {
                    "section": "overview",
                    "field": "floor_count",
                    "claim": "tenant lives on the 7th floor",
                    "conflict_with": "building has 4 floors",
                    "severity": "hard",
                    "reason": "Building has 4 floors.",
                }
            ],
        },
    }

    async def fake_complete_json(self: GeminiProvider, **kwargs: Any) -> JsonCompletion:
        calls.append(kwargs)
        system = kwargs["system"].lower()
        key = "audit" if "audit" in system else "draft" if "repl" in system else "ask"
        return JsonCompletion(data=replies[key], model=self.model, latency_ms=1.0)

    monkeypatch.setattr(GeminiProvider, "complete_json", fake_complete_json)
    return calls


@pytest.fixture
async def property_id(db: None) -> AsyncIterator[uuid.UUID]:
    suffix = uuid.uuid4().hex[:6]
    factory = get_sessionmaker()
    async with factory() as session:
        bid = (
            await session.execute(
                text("INSERT INTO buildings (address) VALUES (:a) RETURNING id"),
                {"a": f"LLM test {suffix}"},
            )
        ).scalar_one()
        pid = (
            await session.execute(
                text(
                    "INSERT INTO properties (name, address, building_id, aliases) "
                    "VALUES (:n, :a, :b, ARRAY[:n]) RETURNING id"
                ),
                {"n": f"Testweg {suffix} Apt 3C", "a": f"Testweg {suffix}", "b": bid},
            )
        ).scalar_one()
        await session.commit()
    yield pid
    async with factory() as session:
        await session.execute(text("DELETE FROM facts WHERE property_id = :p"), {"p": pid})
        await session.execute(text("DELETE FROM events WHERE property_id = :p"), {"p": pid})
        await session.execute(text("DELETE FROM properties WHERE id = :p"), {"p": pid})
        await session.execute(text("DELETE FROM buildings WHERE id = :b"), {"b": bid})
        await session.commit()


async def test_ask_answers_with_gemini_only(
    property_id: uuid.UUID, gemini_only: list[dict[str, Any]]
) -> None:
    async with _client() as client:
        response = await client.post(
            f"/properties/{property_id}/ask", json={"question": "Is the heating fixed?"}
        )

    assert response.status_code == 200, response.text
    assert response.json()["status"] == "answered"
    assert len(gemini_only) == 1


async def test_ask_without_provider_returns_503_naming_the_key(property_id: uuid.UUID) -> None:
    async with _client() as client:
        response = await client.post(
            f"/properties/{property_id}/ask", json={"question": "Is the heating fixed?"}
        )

    assert response.status_code == 503
    assert "GEMINI_API_KEY" in response.json()["detail"]


async def test_draft_reply_with_gemini_only(
    property_id: uuid.UUID, gemini_only: list[dict[str, Any]]
) -> None:
    async with _client() as client:
        response = await client.post(
            "/draft-reply",
            json={"property_id": str(property_id), "body": "The heating is broken."},
        )

    assert response.status_code == 200, response.text
    assert response.json()["body"] == "We are on it."


async def test_draft_reply_without_provider_returns_503(property_id: uuid.UUID) -> None:
    async with _client() as client:
        response = await client.post(
            "/draft-reply",
            json={"property_id": str(property_id), "body": "The heating is broken."},
        )

    assert response.status_code == 503
    assert "GEMINI_API_KEY" in response.json()["detail"]


async def test_semantic_check_runs_on_gemini_only(gemini_only: list[dict[str, Any]]) -> None:
    verdict = await semantic_validator.semantic_validate(
        property_file_markdown="Building: 4 floors",
        event_body="I live on the 7th floor.",
        event_source="email",
        proposed_facts=[],
    )

    assert verdict.has_contradictions
    assert verdict.rejections[0].field == "floor_count"


async def test_semantic_check_without_provider_raises_not_configured() -> None:
    with pytest.raises(LLMNotConfiguredError):
        await semantic_validator.semantic_validate(
            property_file_markdown="", event_body="x", event_source="email", proposed_facts=[]
        )


async def test_worker_logs_nothing_per_event_without_provider(property_id: uuid.UUID) -> None:
    factory = get_sessionmaker()
    async with factory() as session:
        name = (
            await session.execute(
                text("SELECT name FROM properties WHERE id = :p"), {"p": property_id}
            )
        ).scalar_one()
        event_id, _ = await insert_event(
            session,
            source="email",
            source_ref=f"test-llm-{uuid.uuid4().hex[:8]}",
            raw_content=f"Subject: Heizung\n\nDie Heizung in {name} ist kalt.",
        )
        await session.commit()

    with capture_logs() as logs:
        assert await process_specific(event_id) is True

    assert not [e for e in logs if "semantic" in e["event"]]


def test_startup_logs_missing_provider_once() -> None:
    with capture_logs() as logs:
        llm.log_configuration()

    assert [e["event"] for e in logs] == ["llm.provider.not_configured"]
    assert logs[0]["missing"] == "GEMINI_API_KEY"


def test_startup_logs_the_configured_provider(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GEMINI_API_KEY", "test-key")
    get_settings.cache_clear()

    with capture_logs() as logs:
        llm.log_configuration()

    assert logs == [
        {
            "event": "llm.provider",
            "log_level": "info",
            "provider": "gemini",
            "model": get_settings().gemini_pro_model,
        }
    ]
