"""Keystone MCP server — stdio transport.

Registers the five canonical tools from KEYSTONE Part VIII against a
``FastMCP`` instance. The tools are thin adapters over
:mod:`mcp_server.tools`, which in turn hit the Keystone REST backend.

Run directly for Claude Desktop (``python -m mcp_server.main``) — the
script speaks the MCP stdio protocol. Override the backend origin via
``KEYSTONE_BASE_URL`` if the server is on another host.
"""

from __future__ import annotations

from typing import Any

from mcp.server.fastmcp import FastMCP

from mcp_server.tools import KeystoneClient

_client = KeystoneClient()
mcp = FastMCP(
    "keystone",
    instructions=(
        "Keystone is the operational brain for property management. Use these "
        "tools to read each property's living context, search across the "
        "portfolio, inspect open signals, review activity, and propose "
        "actions that a human will approve."
    ),
)


@mcp.tool()
async def get_property_context(property_id: str) -> str:
    """Return the rendered markdown document for a Keystone property.

    Args:
        property_id: UUID of the property.
    """
    return await _client.get_property_markdown(property_id)


@mcp.tool()
async def search_properties(query: str, limit: int = 5) -> list[dict[str, Any]]:
    """Search the portfolio by free text (name, address, aliases, or facts).

    Args:
        query: Free-text search query (e.g. ``"heating issues Berlin"``).
        limit: Max hits (1-25). Default 5.
    """
    return await _client.search(query, limit=limit)


@mcp.tool()
async def list_signals(
    property_id: str | None = None,
    severity: str | None = None,
) -> list[dict[str, Any]]:
    """List pending signals, optionally filtered by property or severity.

    Args:
        property_id: Optional UUID filter.
        severity: Optional severity filter (``low|medium|high|urgent``).
    """
    return await _client.list_signals(property_id=property_id, severity=severity)


@mcp.tool()
async def get_activity(
    property_id: str, since: str | None = None
) -> list[dict[str, Any]]:
    """Return recent events + extraction summaries for a property.

    Args:
        property_id: UUID of the property.
        since: Optional ISO-8601 timestamp; items with ``received_at < since``
            are filtered out.
    """
    return await _client.get_activity(property_id, since=since)


@mcp.tool()
async def propose_action(
    property_id: str | None, action: dict[str, Any]
) -> dict[str, Any]:
    """Propose an action for a property. Creates a pending signal a human approves.

    Args:
        property_id: UUID of the property (or ``None`` for portfolio-level).
        action: Free-form action payload. Recognised keys:
            - ``type``: signal type string
            - ``severity``: one of ``low|medium|high|urgent``
            - ``message``: human-readable summary (required)
            - ``proposed_action``: optional full action dict
            - ``evidence``: list of ``{event_id, fact_id}`` dicts
    """
    return await _client.propose_action(property_id=property_id, action=action)


def main() -> None:
    """Entry point for ``python -m mcp_server.main``."""
    mcp.run()


if __name__ == "__main__":
    main()
