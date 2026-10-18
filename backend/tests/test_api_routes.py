"""The HTTP surface stays stable when routers are reorganized."""

from __future__ import annotations

from backend.main import app

ADMIN_OPERATIONS = {
    ("get", "/admin/unrouted"),
    ("get", "/admin/buena/cursor_status"),
    ("post", "/admin/buena/advance_day"),
    ("post", "/admin/buena/reset_cursor"),
    ("post", "/admin/materialize-all"),
    ("get", "/admin/properties/{property_id}/rejected"),
    ("post", "/admin/rejected/{rejection_id}/override"),
    ("get", "/admin/rejected"),
    ("get", "/admin/properties/{property_id}/uncertainties"),
    ("get", "/admin/uncertainties"),
    ("post", "/admin/uncertainties/{uncertainty_id}/resolve"),
    ("get", "/admin/properties/{property_id}/onboarding"),
    ("post", "/admin/replay/start"),
    ("post", "/admin/replay/{run_id}/pause"),
    ("post", "/admin/replay/{run_id}/resume"),
    ("post", "/admin/replay/{run_id}/stop"),
    ("get", "/admin/replay/{run_id}/status"),
    ("post", "/admin/replay/featured"),
}


def _operations(prefix: str) -> set[tuple[str, str]]:
    paths = app.openapi()["paths"]
    return {
        (method, path) for path, ops in paths.items() if path.startswith(prefix) for method in ops
    }


def test_admin_operations_are_unchanged() -> None:
    assert _operations("/admin") == ADMIN_OPERATIONS
