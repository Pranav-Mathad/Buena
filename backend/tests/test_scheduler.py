"""Optional pollers are only scheduled when configured."""

from __future__ import annotations

import pytest

from backend import scheduler
from backend.config import get_settings


def _job_ids() -> set[str]:
    return {job.id for job in scheduler.build_scheduler().get_jobs()}


def test_default_install_schedules_only_core_jobs(monkeypatch: pytest.MonkeyPatch) -> None:
    for key in ("IMAP_USER", "IMAP_PASSWORD", "MOCK_ERP_URL"):
        monkeypatch.setenv(key, "")
    get_settings.cache_clear()

    assert _job_ids() == {"worker_tick", "signal_eval"}


def test_imap_job_registered_when_credentials_are_set(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("IMAP_USER", "inbox@example.com")
    monkeypatch.setenv("IMAP_PASSWORD", "app-password")
    get_settings.cache_clear()

    assert "imap_poll" in _job_ids()
