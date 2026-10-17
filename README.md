# Keystone

Keystone turns a property manager's scattered inbox, bank statements,
invoices and master data into one current, sourced file per property. Every
fact links to the email or document it came from, carries a confidence score
and keeps its history; anything the system is unsure about goes to a review
inbox instead of into the file. On top of the files, rules raise signals
(recurring heating failures, expiring leases, patterns across buildings) that
a human approves before anything is sent. It runs without any API key, using a
rule-based extractor.

Built solo in a hackathon window, then cleaned up and hardened.

<!-- Screenshots: add these files to docs/img/ -->
| Portfolio | Property file | Ask |
|---|---|---|
| ![Portfolio](docs/img/portfolio.png) | ![Property file](docs/img/property-file.png) | ![Ask](docs/img/ask-panel.png) |
| **Approval inbox** | **MCP in Claude Desktop** | |
| ![Approval inbox](docs/img/approval-inbox.png) | ![MCP](docs/img/mcp-claude-desktop.png) | |

## How it works

```mermaid
flowchart LR
    S[Email, bank CSV, PDFs, Slack, API] --> C[Connectors / API]
    C -->|idempotent insert| Q[(events in Postgres)]
    Q -->|SKIP LOCKED| W[Worker: route, extract, diff, validate, apply]
    W --> F[(facts + materialized files)]
    W --> R[(review inbox)]
    F --> API[REST + SSE]
    API --> UI[React frontend]
    API --> M[MCP server]
    F --> E[Signal evaluator] --> I[Approval inbox] --> O[(outbox)]
```

Design details: [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md). Why it is built
this way, including the mistakes: [docs/DECISIONS.md](docs/DECISIONS.md).

## Features

- **Idempotent ingestion** of `.eml` archives, bank CSV, invoice and letter
  PDFs, master data, IMAP, Slack and PDF upload, keyed on
  `(source, source_ref)`: `connectors/`, `backend/api/webhooks.py`,
  `backend/api/uploads.py`
- **Postgres as the job queue** with `FOR UPDATE SKIP LOCKED`, one transaction
  per event: `backend/pipeline/worker.py`
- **Three-tier routing** (owners' association, building, unit) that leaves an
  event unrouted rather than guessing: `backend/pipeline/router.py`
- **LLM extraction with structured output**, timeouts, retries and a
  rule-based fallback: `backend/pipeline/extractor.py`,
  `backend/services/gemini.py`
- **Facts with provenance and supersession**, where operator edits are never
  overwritten: `backend/pipeline/differ.py`, `backend/pipeline/applier.py`
- **Trust layer**: 8 rule constraints, an LLM contradiction check and an
  uncertainty inbox: `backend/pipeline/validator.py`,
  `backend/pipeline/constraints/`, `backend/pipeline/semantic_validator.py`
- **Per-property files re-rendered in the same transaction** as each fact
  write: `backend/pipeline/materializer.py`
- **Signals with human approval**; a decision holds for a cooldown period:
  `backend/signals/`, `backend/api/signals.py`
- **Ask a property** with cited answers or an explicit "I don't know":
  `backend/services/ask.py`
- **MCP server** exposing property context to MCP clients: `mcp_server/`
- **PII redaction at ingestion** and a **durable LLM spend cap**:
  `connectors/redact.py`, `connectors/cost_ledger.py`
- **Evaluation harness** with committed run reports: `eval/`, `docs/eval-runs/`

## Quickstart

Requires Docker, Python 3.11 and Node 18 or later.

```bash
cp .env.example .env
docker compose up -d postgres                      # Postgres on localhost:5433
python3.11 -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
python -m seed.seed                                # schema + 4 sample properties
python -c "from connectors.migrations import apply_all; apply_all()"
EXTRACTED_ROOT=Extracted_demo python -m connectors.cli load-stammdaten --source buena   # 52 synthetic units
uvicorn backend.main:app --reload                  # http://localhost:8000/docs

cd frontend && cp .env.example .env.local && npm ci && npm run dev   # http://localhost:8080
```

- API and interactive docs: <http://localhost:8000/docs>
- Frontend: <http://localhost:8080>

No API key is needed: extraction falls back to the rule-based extractor and
signal drafts to templates. Ask, reply drafting and the LLM contradiction check
currently need `PIONEER_API_KEY`. Try the pipeline with:

```bash
curl -X POST localhost:8000/debug/trigger_event -H 'content-type: application/json' \
  -d '{"raw_content": "From: tenant@example.com\nSubject: Heizung\n\nDie Heizung in WE 29 ist ausgefallen."}'
```

Tests: `pytest backend/tests connectors/tests eval/tests mcp_server/tests` (needs the
Postgres above; nothing else).

## Evaluation

30 hand-labelled emails, scored by `python -m eval.runner --set emails_v1`.
Full methodology and weaknesses: [docs/EVALUATION.md](docs/EVALUATION.md).

| Metric | LLM (Gemini) | Rule-based, no key |
|---|---:|---:|
| Category accuracy | 73.3% | 63.3% |
| Routing accuracy | 93.3% | 93.3% |
| Fact extraction, micro F1 | 0.88 | 0.42 |
| Lease facts, F1 | 0.62 | 0.67 |
| Extracted values matching the source wording | 3 of 26 | 0 of 11 |

Every number here is traced to its command or report in
[docs/PROJECT_FACTS.md](docs/PROJECT_FACTS.md).

## Tech stack

Python 3.11, FastAPI, SQLAlchemy 2 (async, asyncpg), PostgreSQL, APScheduler,
pydantic, structlog; Google Gemini and an OpenAI-compatible gateway for LLM
calls; MCP Python SDK. Frontend: React 18, TypeScript, Vite, TanStack Query,
Tailwind CSS, Radix UI. Tests: pytest, pytest-asyncio.

## Project layout

```
backend/        FastAPI app: api/, pipeline/, signals/, services/, db/
connectors/     archive importers, PII redaction, cost ledger, CLI
eval/           ground truth, runner, metrics
mcp_server/     MCP adapter over the REST API
frontend/       React client
seed/           sample data and schema bootstrap
scripts/        dev runner, single-property backfill, demo-data synthesis
tools/mock_erp/ optional stand-in ERP feed
docs/           architecture, decisions, evaluation, facts, eval run reports
Extracted_demo/ synthetic master data (committed)
```

## Limitations

No authentication; approved actions are written to an outbox, never sent;
scheduled jobs are not safe to run in several instances; some LLM features
require one specific provider; without a web-search key the regulation watcher
inserts fixed illustrative headlines; extraction quality is measured on only 30
emails. The full list is in
[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md#known-limitations).

## License

MIT, see [LICENSE](LICENSE).
