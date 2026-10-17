# Keystone MCP Server

A thin MCP adapter over the Keystone REST API. Its five tools let any
MCP-capable client (for example Claude Desktop) read a property's current
file, search the portfolio, inspect signals, review activity, and propose
actions that a human then approves in the Keystone inbox. Every tool is an
HTTP call; the server has no database access of its own.

## Tools

| Tool | Signature | What it does |
|---|---|---|
| `get_property_context` | `(property_id)` | Rendered markdown for a property. |
| `search_properties` | `(query, limit=5)` | Keyword search → `[{id, name, address, snippet, score}]`. |
| `list_signals` | `(property_id?, severity?)` | Pending signals, optionally filtered. |
| `get_activity` | `(property_id, since?)` | Recent events + one-line summaries. |
| `propose_action` | `(property_id, action)` | Files a new pending signal for human approval. |

## Install and run the backend first

```bash
pip install -e ".[mcp]"              # the MCP SDK is an optional extra
docker compose up -d postgres
python -m seed.seed                  # schema plus 4 sample properties
uvicorn backend.main:app --port 8000 # the REST API this server adapts
```

## Wire up Claude Desktop

1. Quit Claude Desktop.
2. Open `~/Library/Application Support/Claude/claude_desktop_config.json`
   (create it if absent).
3. Add the `keystone` server under `mcpServers`:

```json
{
  "mcpServers": {
    "keystone": {
      "command": "/path/to/keystone/.venv/bin/python",
      "args": ["-m", "mcp_server.main"],
      "cwd": "/path/to/keystone",
      "env": {
        "KEYSTONE_BASE_URL": "http://localhost:8000",
        "PYTHONPATH": "/path/to/keystone"
      }
    }
  }
}
```

Replace the paths with your checkout. `PYTHONPATH` lets Python import the
`mcp_server` package from the repository root.

4. Relaunch Claude Desktop. The hammer icon in the chat input should list
   five Keystone tools.

## Try it

Ask Claude Desktop:

> *What's going on with Berliner Strasse 12, Apt 4B?*

Claude will call `search_properties` → `get_property_context` → `list_signals`
→ `get_activity` and answer from the markdown + signals, not its training data.

## Pointing at a deployed backend

```bash
export KEYSTONE_BASE_URL=https://keystone.example.com
python -m mcp_server.main
```

Or update the `env.KEYSTONE_BASE_URL` line in the Claude Desktop config.

## Smoke test without Claude Desktop

```bash
python - <<'PY'
import asyncio
from mcp_server.tools import KeystoneClient, build_tools

client = KeystoneClient()
tools = build_tools(client)

async def main():
    hits = await tools["search_properties"]("heating Berlin")
    print(hits)
    if hits:
        md = await tools["get_property_context"](hits[0]["id"])
        print(md[:400])

asyncio.run(main())
PY
```
