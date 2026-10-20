"""ERP poller is opt-in: no URL means no scheduled job and no HTTP calls."""

from __future__ import annotations

import pytest

from backend import scheduler
from backend.config import Settings
from backend.services import erp_poller


def _settings(url: str) -> Settings:
    return Settings(mock_erp_url=url)


async def test_poll_once_is_noop_without_url(monkeypatch: pytest.MonkeyPatch) -> None:
    async def _fail(_: str) -> list[dict[str, object]]:
        raise AssertionError("poller must not call the ERP when MOCK_ERP_URL is unset")

    monkeypatch.setattr(erp_poller, "get_settings", lambda: _settings(""))
    monkeypatch.setattr(erp_poller, "_fetch_payments", _fail)

    assert await erp_poller.poll_once() == 0


def test_scheduler_omits_erp_job_without_url(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(scheduler, "get_settings", lambda: _settings(""))

    job_ids = {job.id for job in scheduler.build_scheduler().get_jobs()}

    assert "erp_poll" not in job_ids
    assert "worker_tick" in job_ids


def test_scheduler_registers_erp_job_with_url(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(scheduler, "get_settings", lambda: _settings("http://erp.test"))

    job_ids = {job.id for job in scheduler.build_scheduler().get_jobs()}

    assert "erp_poll" in job_ids


def test_format_payment_carries_routing_hint() -> None:
    raw = erp_poller._format_payment(
        {"account": "ACC-1", "property_hint": "EH-029", "amount_eur": 950, "late_days": 3}
    )

    assert "Property: EH-029" in raw
    assert "Amount: 950 EUR" in raw
    assert "Late by: 3 days" in raw
