# Keystone MCP Server

Thin MCP adapter over the Keystone REST backend. Exposes the five canonical tools
from KEYSTONE Part VIII so any MCP-capable client (e.g. Claude Desktop) can
read a property's living context, search the portfolio, inspect signals,
review activity, and propose actions that a human approves in the Keystone
inbox.

## Tools

| Tool | Signature | What it does |
|---|---|---|
| `get_property_context` | `(property_id)` | Rendered markdown for a property. |
| `search_properties` | `(query, limit=5)` | Keyword search → `[{id, name, address, snippet, score}]`. |
| `list_signals` | `(property_id?, severity?)` | Pending signals, optionally filtered. |
| `get_activity` | `(property_id, since?)` | Recent events + one-line summaries. |
| `propose_action` | `(property_id, action)` | Files a new pending signal for human approval. |

## Run the backend first

```bash
docker compose up -d                 # Postgres + pgvector
python -m seed.seed                  # 4 properties + 6mo history
uvicorn backend.main:app --port 8000 # REST API the MCP server adapts
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
      "command": "/Users/YOU/Desktop/hackathon/.venv/bin/python",
      "args": ["-m", "mcp_server.main"],
      "cwd": "/Users/YOU/Desktop/hackathon",
      "env": {
        "KEYSTONE_BASE_URL": "http://localhost:8000",
        "PYTHONPATH": "/Users/YOU/Desktop/hackathon"
      }
    }
  }
}
```

Replace the two absolute paths to match your checkout. The `PYTHONPATH`
entry lets the `mcp_server` package find `backend/` neighbours if you ever
share state locally.

4. Relaunch Claude Desktop. The hammer icon in the chat input should list
   five Keystone tools.

## Try it

Ask Claude Desktop:

> *What's going on with apartment 4B?*

Claude will call `search_properties` → `get_property_context` → `list_signals`
→ `get_activity` and answer from the markdown + signals, not its training data.

## Pointing at a deployed backend

```bash
export KEYSTONE_BASE_URL=https://keystone.onrender.com
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
