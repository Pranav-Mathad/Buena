# Keystone

Keystone keeps one file per property for a property manager. It reads what
the manager already has (tenant email, bank statements, contractor invoices,
letters, the master-data spreadsheet), pulls the facts out, and writes each
one into the right property's file with a link to the message or document
it came from, a confidence score and its history. When it is not sure, the
item goes to a review inbox instead of the file. A few rules watch the files
for things worth acting on, such as a heating system that keeps failing or a
lease about to expire, and draft a message that a person has to approve
before it goes anywhere.

I wrote the first version in a hackathon and have been cleaning it up since.
It runs without any API key: extraction falls back to a rule-based extractor
and signal drafts to templates. Give it a key and an LLM does the extraction,
answers questions about a property with citations, and drafts replies.

| Portfolio | Property file |
|---|---|
| ![Portfolio](docs/img/portfolio.png) | ![Property file](docs/img/property-file.png) |
| **Approval inbox** | **Ask** |
| ![Approval inbox](docs/img/approval-inbox.png) | ![Ask](docs/img/ask-panel.png) |

The screenshots are from a fresh `docker compose up` with no API key: the
sample data as it loads on first start, nothing added by hand.

## How it works

```
   email, bank CSV, PDFs, Slack, HTTP
                  |
                  v
             connectors         redact, then insert keyed on
                  |             (source, source_ref); a re-run is a no-op
                  v
          events (Postgres)     Postgres is the queue
                  |
                  v
               worker           claims one row with SKIP LOCKED, then in
                  |             one transaction: route, extract, diff,
                  |             validate, apply, re-render the file
         +--------+--------+
         |                 |
         v                 v
   facts and files    review inbox
         |
         +----> REST API, SSE ----> React frontend
         |                   \----> MCP server
         |
         +----> signal rules -----> approval inbox ----> outbox
```

A connector redacts IBANs, phone numbers and email domains before anything
is stored, and inserts the event keyed on `(source, source_ref)`, so
importing the same archive twice changes nothing. The worker claims events
with `FOR UPDATE SKIP LOCKED`; several workers can share one database and
there is no separate queue to run.

For each event the worker looks for a property named in the text and marks
the event unrouted rather than guess. (The bulk email import also routes to
a building or an owners' association.) It extracts facts, diffs them
against what the file already says, runs eight rule constraints (a
building's address and year built never change, a rent change needs an
addendum, and so on) and, with a key, an LLM check for contradictions with
the current file. Anything under the confidence floor or flagged by a
constraint lands in the review inbox. A fact an operator edited by hand is
never overwritten. Old facts are kept and point at their replacement, so
the history of any field can be read back. The property's file is
re-rendered in the same transaction as the fact write, so nobody reads a
file that disagrees with its facts.

Signals run on their own schedule over the current facts, so a slow rule
never holds up ingestion. A decision on a signal holds for a cooldown
period, so the same finding does not come back the next morning.

The long version, with the data model and the concurrency story, is
[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md). Each non-obvious choice, the
alternatives that were rejected and the mistakes along the way are in
[docs/DECISIONS.md](docs/DECISIONS.md).

## Running it

You need Docker.

    cp .env.example .env
    docker compose up

Then:

    frontend    http://localhost:8080
    API docs    http://localhost:8000/docs

The first start builds both images, applies the schema and loads the sample
data: 56 properties, and six tenant emails for the featured property (WE 29)
that go through the pipeline so its file has a history. All of it is
synthetic. Starting again is safe; every step is idempotent.

To push an event through yourself:

    curl -X POST localhost:8000/debug/trigger_event \
      -H 'content-type: application/json' \
      -d '{"raw_content": "From: tenant@example.com\nSubject: Heizung\n\nDie Heizung in WE 29 ist ausgefallen."}'

The property page picks the change up over the server-sent event stream.

Without a key, extraction uses the rule-based path, signal drafts use
templates, and Ask and reply drafting return 503. To turn the LLM on, set
`GEMINI_API_KEY` in `.env`, or `LLM_PROVIDER=pioneer` and `PIONEER_API_KEY`
for an OpenAI-compatible gateway. Nothing else changes.

## Running without Docker

Python 3.11, Node 20 and Postgres 16. The compose file can provide just the
database, published on host port 5433 so it does not collide with one you
already run; the DSNs in `.env.example` point at it.

    docker compose up -d postgres
    python3.11 -m venv .venv && source .venv/bin/activate
    pip install -e ".[dev]"
    (cd frontend && npm ci)

Apply the schema and load the sample data, which is what the container
entrypoint does on start:

    python -m seed.seed
    python -c "from connectors.migrations import apply_all; apply_all()"
    EXTRACTED_ROOT=Extracted_demo python -m connectors.cli load-stammdaten --source buena
    python -m connectors.cli materialize-all
    python -m connectors.cli load-sample-events

Then `./scripts/dev.sh` starts the API with reload on port 8000 and the Vite
dev server on 8080, and stops both on Ctrl-C.

## Configuration

Everything is read from the environment or `.env`. The example file lists
every variable with a comment, and the defaults are enough for a local run.
The ones worth knowing:

    DATABASE_URL            asyncpg DSN; DATABASE_URL_SYNC is the psycopg2 one
    LLM_PROVIDER            gemini (default) or pioneer, and that provider's key
    FEATURED_PROPERTY       the property the UI opens first (default EH-029)
    KEYSTONE_CORS_ORIGINS   browser origins allowed to call the API
    IMAP_HOST, IMAP_USER,   mailbox to poll; the poller stays off until a
    IMAP_PASSWORD           real password is set
    SLACK_SIGNING_SECRET    the Slack webhook checks signatures once this is set
    MOCK_ERP_URL            payments feed to poll; empty turns the poller off

## Tests and checks

The Python tests need the Postgres from `docker compose up -d postgres` and
nothing else. API keys are blanked for every test, so a run never makes a
paid call, and a test that needs the private dataset skips and says so.

    pytest                      # backend, connectors, eval, mcp_server, scripts
    cd frontend && npm test     # Vitest

`pre-commit install` adds ruff, prettier and a check that no real address,
IBAN or deployment URL gets committed. CI runs those plus mypy in strict
mode, ESLint, the TypeScript check, both test suites and the frontend
build, against an empty Postgres 16. See `.github/workflows/ci.yml`.

## Evaluation

The extractor is scored against 30 hand-labelled emails by
`python -m eval.runner --set emails_v1`. The LLM run and the keyless run
on the same set:

| | Gemini | Rule-based, no key |
|---|---:|---:|
| Category accuracy | 73.3% | 63.3% |
| Routing accuracy | 93.3% | 93.3% |
| Fact extraction, micro F1 | 0.88 | 0.42 |
| Lease facts, F1 | 0.62 | 0.67 |
| Values matching the source wording | 3 of 26 | 0 of 11 |

These are not numbers to leave unattended, and the set is too small to tell
two strong models apart. Lease precision is low because the model reads an
owner selling up as a tenant giving notice. Values come back paraphrased
rather than quoted. Nearly every LLM fact claims a confidence of 0.9 or
more while about one in five is in the wrong slot. The reports are
committed under `docs/eval-runs/` and discussed in
[docs/EVALUATION.md](docs/EVALUATION.md). Every number in this file is
traced to the command that produced it in
[docs/PROJECT_FACTS.md](docs/PROJECT_FACTS.md).

## Command-line tools

`python -m connectors.cli` loads master data and walks the bank, invoice and
email archives into events. The email backfill goes through the live
extraction pipeline, is resumable, and stops at a spend cap kept in a ledger
table (default $20), so a restart cannot double the bill. The archive
readers expect the private dataset under `Extracted/`, which is not in the
repository; `Extracted_demo/` holds synthetic master data and six sample
emails in the same shape.

`python -m mcp_server.main` exposes five tools (property context, search,
pending signals, activity, propose an action) over MCP as a thin client of
the REST API. Install with `pip install -e ".[mcp]"` first. Wiring it into
Claude Desktop is described in [mcp_server/README.md](mcp_server/README.md).

`uvicorn tools.mock_erp.main:app --port 8001` serves a stand-in payments
feed. Set `MOCK_ERP_URL=http://localhost:8001` to have the API poll it.

## Layout

```
backend/        FastAPI app: api/, pipeline/, signals/, services/, db/
connectors/     archive importers, redaction, cost ledger, migrations, CLI
eval/           ground truth, runner, metrics
mcp_server/     MCP adapter over the REST API
frontend/       React client; has its own README
seed/           schema bootstrap and hand-written sample data
scripts/        dev runner, container entrypoint, sensitive-data check,
                demo-data synthesis, release helpers
tools/mock_erp/ stand-in ERP feed
docs/           architecture, decisions, evaluation, facts, eval runs
Extracted_demo/ synthetic master data and sample emails
```

Python side: FastAPI, SQLAlchemy 2 with asyncpg, pydantic, APScheduler,
structlog; pytest, ruff, mypy. Frontend: React 18, TypeScript, Vite,
TanStack Query, Tailwind, a few Radix primitives; Vitest.

## Known problems

There is no authentication. Do not put it on the internet as it is.

Nothing is ever sent. An approved signal is written to an outbox table and
stops there.

Scheduled jobs run in every API process, so with N instances the signal
evaluator and the pollers do their work N times. The worker itself is safe
to run in parallel. A leader lock would fix the rest.

Redaction replaces the domain of an email address but keeps the local part,
so a name in an address survives.

Bulk-imported emails skip the rule constraints and the contradiction check.
Live events get the full pipeline.

Property search is keyword matching. The IMAP code is tested through
parsing real `.eml` files; the fetch loop against a live server is not.

Extraction quality is measured on 30 emails, as above.

The full list, with detail, is in
[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md#known-limitations).

## License

MIT. See [LICENSE](LICENSE).
