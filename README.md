# Keystone — Pioneer Edition

> The operational brain for property management. Transitioned to a **LangGraph-based "Ralph Loop"** for mathematically verifiable, hallucination-free decision making, powered by **Fastino Pioneer**.

Keystone turns fragmented communication (emails, Slack, PDFs, ERP data) into a living, trusted context layer per property. This version ("Pioneer Edition") utilizes a state-machine architecture to ensure all updates are grounded in physical property facts.

---

## The "Ralph Loop" (State Machine)
Keystone now uses **LangGraph** to orchestrate the decision process:
1. **VALIDATE_INPUT**: Uses a local **GLiNER2** model to check for physical constraints (e.g., "6th floor ticket" on a "5 floor building").
2. **RETRIEVE_CONTEXT**: Fetches grounded data from **pgvector**.
3. **GENERATE_RESPONSE**: **Fastino Pioneer** (OpenAI-compatible) reasons over the context.
4. **UPDATE_MARKDOWN**: Programmatically updates the `Property_Context.md` if a maintenance/lease event is detected.

---

## Quick start (5 steps)

1. **Clone + env**
   ```bash
   git clone <repo-url> keystone && cd keystone
   cp .env.example .env
   # Add PIONEER_API_KEY to .env
   ```

2. **Python deps** (Python 3.11+ required)
   ```bash
   python -m venv .venv && source .venv/bin/activate
   pip install -e ".[dev]"
   ```

3. **Start Postgres + pgvector**
   ```bash
   docker compose up -d
   ```

4. **Ingest and Validate**
   ```bash
   # Ingest historical data and seed pgvector
   python -m seed.seed
   # Verify the Ralph Loop with a trap ticket
   python scripts/phase11_step1_verify.py
   ```

5. **Run the Orchestrator**
   ```bash
   # Run the Manager's Brain Dashboard (FastAPI + LangGraph)
   uvicorn backend.main:app --reload
   ```

## Verify & Tools

* **Main Interface**: `http://localhost:8000/` (Interactive Dashboard)
* **API Health**: `http://localhost:8000/health`
* **MCP Server**: `http://localhost:8000/mcp/` (For Claude Desktop integration)

---

## Documents
* [KEYSTONE.md](KEYSTONE.md) — The Project Constitution.
* [DECISIONS.md](DECISIONS.md) — Non-obvious architectural calls and phase logs.
* [DEMO_QA.md](DEMO_QA.md) — Frequently asked technical questions.
