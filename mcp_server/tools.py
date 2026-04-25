"""Implementations of the five canonical MCP tools.

Every tool is a thin adapter over the Keystone REST backend (Part V:
"MCP server is a thin adapter. All tools call the backend REST API.").
The ``KeystoneClient`` wraps ``httpx.AsyncClient`` so the server can be
pointed at any deployment (dev, staging, Railway) via
``KEYSTONE_BASE_URL``.
"""

from __future__ import annotations

import os
from collections.abc import Awaitable, Callable
from typing import Any

import httpx


class KeystoneClient:
    """Convenience wrapper for the Keystone REST backend.

    Attributes:
        base_url: Backend origin. Defaults to ``KEYSTONE_BASE_URL`` env or
            ``http://localhost:8000``.
        timeout: Per-request timeout in seconds.
    """

    def __init__(
        self,
        base_url: str | None = None,
        *,
        timeout: float = 10.0,
    ) -> None:
        self.base_url = (
            base_url or os.environ.get("KEYSTONE_BASE_URL", "http://localhost:8000")
        ).rstrip("/")
        self.timeout = timeout

    async def _request(
        self,
        method: str,
        path: str,
        **kwargs: Any,
    ) -> httpx.Response:
        """Execute an HTTP request against the backend and raise on errors."""
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.request(method, self.base_url + path, **kwargs)
        response.raise_for_status()
        return response

    async def get_property_markdown(self, property_id: str) -> str:
        """GET /properties/{id}/markdown."""
        response = await self._request("GET", f"/properties/{property_id}/markdown")
        return response.text

    async def search(self, query: str, limit: int = 5) -> list[dict[str, Any]]:
        """GET /properties/search."""
        response = await self._request(
            "GET", "/properties/search", params={"q": query, "limit": limit}
        )
        return list(response.json())

    async def list_signals(
        self,
        property_id: str | None = None,
        severity: str | None = None,
    ) -> list[dict[str, Any]]:
        """GET /signals?status=pending."""
        params: dict[str, Any] = {"status": "pending"}
        if property_id:
            params["property_id"] = property_id
        if severity:
            params["severity"] = severity
        response = await self._request("GET", "/signals", params=params)
        return list(response.json())

    async def get_activity(
        self,
        property_id: str,
        since: str | None = None,
    ) -> list[dict[str, Any]]:
        """GET /properties/{id}/activity.

        The backend doesn't currently filter by ``since`` server-side, so we
        do it client-side here — the activity feed is small enough that this
        is effectively free.
        """
        response = await self._request(
            "GET", f"/properties/{property_id}/activity", params={"limit": 100}
        )
        items = list(response.json())
        if since:
            items = [i for i in items if i.get("received_at", "") >= since]
        return items

    async def propose_action(
        self,
        *,
        property_id: str | None,
        action: dict[str, Any],
    ) -> dict[str, Any]:
        """POST /signals/propose (MCP entry point for external AI proposals)."""
        payload = {
            "property_id": property_id,
            "type": str(action.get("type") or "external_proposal"),
            "severity": str(action.get("severity") or "medium"),
            "message": str(action.get("message") or "External AI proposal"),
            "action": action.get("proposed_action", {}),
            "evidence": action.get("evidence", []),
        }
        response = await self._request("POST", "/signals/propose", json=payload)
        return dict(response.json())


# Tool registration helpers — used by mcp_server/main.py.
ToolFn = Callable[..., Awaitable[Any]]


def build_tools(client: KeystoneClient) -> dict[str, ToolFn]:
    """Return the five canonical tools bound to ``client``."""

    async def get_property_context(property_id: str) -> str:
        """Return the living markdown document for a Keystone property."""
        return await client.get_property_markdown(property_id)

    async def search_properties(query: str, limit: int = 5) -> list[dict[str, Any]]:
        """Search properties by free text; returns ``[{id, name, address, snippet, score}]``."""
        return await client.search(query, limit=limit)

    async def list_signals(
        property_id: str | None = None,
        severity: str | None = None,
    ) -> list[dict[str, Any]]:
        """Return pending signals, optionally filtered by property or severity."""
        return await client.list_signals(property_id=property_id, severity=severity)

    async def get_activity(
        property_id: str, since: str | None = None
    ) -> list[dict[str, Any]]:
        """Return recent events + extraction summaries for a property."""
        return await client.get_activity(property_id, since=since)

    async def propose_action(
        property_id: str | None, action: dict[str, Any]
    ) -> dict[str, Any]:
        """Create a pending signal on behalf of an external AI. Human approves in the inbox."""
        return await client.propose_action(property_id=property_id, action=action)

    return {
        "get_property_context": get_property_context,
        "search_properties": search_properties,
        "list_signals": list_signals,
        "get_activity": get_activity,
        "propose_action": propose_action,
    }
