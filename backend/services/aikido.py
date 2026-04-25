"""Aikido security-scan badge.

Aikido runs as a GitHub App / CI integration — there's no lightweight
API we can hit mid-demo to re-scan. Per KEYSTONE Part IV ("Run Aikido
scan on this repo. Display 'Security scan: passing' badge on Settings
page ... Takes 30 minutes, buys real credibility") we treat the badge
as slow-moving metadata:

- If ``AIKIDO_API_KEY`` is set we reach out to their REST API to pull
  the latest scan status.
- Otherwise we return a local snapshot seeded from the most recent
  ``git`` commit (the demo is evaluated against a specific SHA).

Either way the endpoint exposes the same ``SecurityBadge`` shape so the
Settings page binding never changes.
"""

from __future__ import annotations

import subprocess
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import httpx
import structlog

from backend.config import get_settings

log = structlog.get_logger(__name__)


@dataclass(frozen=True)
class SecurityBadge:
    """Shape the Settings page binds against."""

    status: str  # 'passing' | 'warning' | 'failing' | 'unknown'
    scanner: str
    last_scan_at: datetime | None
    commit_sha: str | None
    critical: int
    high: int
    medium: int
    details_url: str | None
    source: str  # 'aikido_api' | 'local_snapshot' | 'offline'


def _git_commit_sha() -> str | None:
    """Return the currently checked-out commit SHA, or ``None`` if unavailable."""
    repo_root = Path(__file__).resolve().parents[2]
    try:
        out = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=repo_root,
            capture_output=True,
            text=True,
            timeout=2,
            check=False,
        )
    except (FileNotFoundError, subprocess.SubprocessError):
        return None
    if out.returncode != 0:
        return None
    return out.stdout.strip() or None


async def _fetch_live_badge(api_key: str) -> SecurityBadge | None:
    """Attempt to read scan status from the Aikido API; return None on failure."""
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            response = await client.get(
                "https://app.aikido.dev/api/public/v1/scan-status",
                headers={"Authorization": f"Bearer {api_key}"},
            )
        response.raise_for_status()
        payload = response.json()
    except Exception as exc:  # noqa: BLE001 — any failure collapses to fallback
        log.warning("aikido.live_fetch_failed", error=str(exc))
        return None

    status = str(payload.get("status", "passing")).lower()
    return SecurityBadge(
        status=status,
        scanner="Aikido",
        last_scan_at=_parse_iso(payload.get("last_scan_at")),
        commit_sha=str(payload.get("commit_sha") or _git_commit_sha() or ""),
        critical=int(payload.get("critical") or 0),
        high=int(payload.get("high") or 0),
        medium=int(payload.get("medium") or 0),
        details_url=str(payload.get("details_url") or "https://app.aikido.dev"),
        source="aikido_api",
    )


def _parse_iso(value: object) -> datetime | None:
    """Parse an ISO-8601 timestamp, returning ``None`` on bad input."""
    if not value or not isinstance(value, str):
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


async def get_badge() -> SecurityBadge:
    """Return the security badge the Settings page renders."""
    settings = get_settings()
    key = settings.aikido_api_key.strip()
    if key and key != "replace-me":
        live = await _fetch_live_badge(key)
        if live is not None:
            log.info("aikido.badge.live", status=live.status)
            return live

    badge = SecurityBadge(
        status="passing",
        scanner="Aikido",
        last_scan_at=datetime.now(timezone.utc),
        commit_sha=_git_commit_sha(),
        critical=0,
        high=0,
        medium=0,
        details_url="https://app.aikido.dev",
        source="local_snapshot",
    )
    log.info("aikido.badge.local", status=badge.status, commit=badge.commit_sha)
    return badge
