"""The MCP server registers its five tools and maps them onto the REST API."""

from __future__ import annotations

import json
from typing import Any

import httpx
import pytest

from mcp_server import tools
from mcp_server.main import mcp


async def test_server_registers_the_documented_tools() -> None:
    names = {tool.name for tool in await mcp.list_tools()}

    assert names == {
        "get_property_context",
        "search_properties",
        "list_signals",
        "get_activity",
        "propose_action",
    }


@pytest.fixture
def requests(monkeypatch: pytest.MonkeyPatch) -> list[httpx.Request]:
    """Capture the client's HTTP requests instead of sending them."""
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        if request.url.path.endswith("/markdown"):
            return httpx.Response(200, text="# WE 29")
        return httpx.Response(200, json={"signal_id": "s1", "status": "pending"})

    real_client = httpx.AsyncClient

    def client_factory(**kwargs: Any) -> httpx.AsyncClient:
        return real_client(transport=httpx.MockTransport(handler), **kwargs)

    monkeypatch.setattr(tools.httpx, "AsyncClient", client_factory)
    return seen


async def test_property_context_reads_rendered_markdown(requests: list[httpx.Request]) -> None:
    client = tools.KeystoneClient("http://api.test")

    assert await client.get_property_markdown("p1") == "# WE 29"
    assert str(requests[0].url) == "http://api.test/properties/p1/markdown"


async def test_propose_action_files_a_signal(requests: list[httpx.Request]) -> None:
    client = tools.KeystoneClient("http://api.test")

    result = await client.propose_action(
        property_id="p1", action={"type": "dispatch_contractor", "message": "Check boiler"}
    )

    assert result["status"] == "pending"
    assert requests[0].method == "POST"
    assert requests[0].url.path == "/signals/propose"
    body = json.loads(requests[0].content)
    assert body["property_id"] == "p1"
    assert body["type"] == "dispatch_contractor"
