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

PROPERTIES_OPERATIONS = {
    ("get", "/properties"),
    ("post", "/properties"),
    ("get", "/properties/search"),
    ("get", "/properties/{property_id}/activity"),
    ("post", "/properties/{property_id}/ask"),
    ("get", "/properties/{property_id}/events"),
    ("patch", "/properties/{property_id}/facts/{fact_id}"),
    ("get", "/properties/{property_id}/file"),
    ("get", "/properties/{property_id}/graph"),
    ("get", "/properties/{property_id}/inbox"),
    ("get", "/properties/{property_id}/markdown"),
}


def _operations(prefix: str) -> set[tuple[str, str]]:
    paths = app.openapi()["paths"]
    return {
        (method, path) for path, ops in paths.items() if path.startswith(prefix) for method in ops
    }


def test_admin_operations_are_unchanged() -> None:
    assert _operations("/admin") == ADMIN_OPERATIONS


def test_properties_operations_are_unchanged() -> None:
    assert _operations("/properties") == PROPERTIES_OPERATIONS
