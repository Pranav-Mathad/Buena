# KEYSTONE — Project Constitution

## Git Hygiene

- **Commit atomically.** One logical change per commit. Don't bundle unrelated fixes.
- **Commit format:** conventional commits — `feat:`, `fix:`, `chore:`, `refactor:`, `test:`, `docs:`.
- **Good messages:** `feat(pipeline): add Gemini extractor with JSON schema validation` — not `stuff` or `wip`.
- **Commit at natural breakpoints:** after a test passes, after a feature is demo-able, before switching context. Not on a timer.
- **Never commit secrets.** `.env` is gitignored. Double-check before every commit.
- **Tag at phase boundaries:** `git tag phase-1-complete` after each phase exit criterion is met.
- **Branch naming:** `phase-N/short-description` if you branch. Main is always demo-ready.

## 

You are **Claude Code**, building **Keystone**: the winning submission for the **Buena track** at the hackathon.

**Read this entire document before writing any code.**
**Re-read "The North Star" and "Scope Discipline" at the start of every session.**
**Update "Current Phase" at the end of every session.**

---

# PART I — THE NORTH STAR

## The Product in One Sentence

**Keystone is the operational brain for property management.**
It turns fragmented communication (email, Slack, PDFs, ERPs, web) into a **living, trusted context layer per property**, and proactively suggests actions with human approval.

---

## The Winning Insight

Other teams will build:

> Aggregate and summarize data.

We build:

> Detect patterns, raise signals, and propose decisions.

**Pitch line (use verbatim):**

> "Other teams built a better filing cabinet. We built the brain that runs the operation."

---

## What Judges Must Feel

By the end of the demo, each judge must think:

1. **"It noticed something I wouldn't have."**
2. **"I trust where this came from."**
3. **"I would actually use this."**

If any of these three is unclear, we lose. Every feature is evaluated against these three.

---

## Four Core Principles

### 1. Context = Source of Truth
Every fact has a source. Every fact has confidence. Every fact has history. No black-box outputs.

### 2. Signals > Summaries
We don't just describe — we detect, prioritize, and recommend.

### 3. Human-in-the-Loop
System proposes. Human approves. System learns patterns from the decisions.

### 4. Intelligence Across Properties
Insights are portfolio-aware — not isolated, not reactive.

---

## The Signal Quality Bar

**Every signal must sound like an expert speaking, not a database emitting rows.**

❌ **Bad:** *"Recommend inspection."*
✅ **Good:** *"3 heating complaints across units sharing the same boiler model in 6 weeks. High likelihood of imminent boiler failure. Schedule contractor visit within 48h. Notify owner Maria."*

The difference between winning and losing is whether signals feel *smart*.

---

# PART II — THE 2-MINUTE DEMO (BUILD ONLY FOR THIS)

Build backwards from this demo. Every line of code must serve one of these seven beats.

### (0:00) Portfolio View
Open Keystone: 4 properties, 3 active signals, 2 pending approvals.
**Line:** *"Keystone is the operational brain for property management."*

### (0:15) Property Deep Dive
Zoom into **Berliner Strasse 12, Apt 4B**. Show:
- Living markdown with clickable sources
- Context panel: Owner → Building → Tenants → Contractor

### (0:45) Live Event Trigger
Send email from phone: *"Heating not working in 4B"*.
Within ~8 seconds: activity feed updates, markdown gains a maintenance entry with a clickable source.

### (1:00) The WOW Signal
A signal fires:
> *"3 heating complaints across units sharing the same boiler model. High risk of system failure. Schedule contractor inspection within 48h. Notify owner Maria."*

### (1:15) Human Approval
Click **Approve**. Signal resolves. Outbox shows the "sent" message to the owner.

### (1:30) MCP Integration
Open Claude Desktop on second screen. Ask: *"What's going on with apartment 4B?"*
Claude answers using Keystone context via MCP.
**Line:** *"Keystone becomes the memory layer any AI can use."*

### (1:45) Portfolio Intelligence
Zoom to portfolio. Banner:
> *"Keystone detected: 3 pre-1990 boiler properties had heating issues this winter. Schedule portfolio-wide inspection?"*

### (2:00) Close
> *"Other teams built a filing system. We built the system that runs operations."*

---

# PART III — SCOPE DISCIPLINE (NON-NEGOTIABLE)

Read this at the start of every session.

### The Golden Question
Before adding anything: **"Does this show up in the 2-minute demo?"** If no, cut it.

### Build Order Rule
**End-to-end first, polish later.** A working ugly pipeline beats a beautiful broken one.

### What We Are NOT Building
- ❌ No authentication. One hardcoded demo user.
- ❌ No multi-tenancy.
- ❌ No real OAuth. IMAP app password + Slack webhook signing secret only.
- ❌ No message queue. **Postgres IS the queue** (`SELECT FOR UPDATE SKIP LOCKED`).
- ❌ No real email sending. Write to `outbox` table and show a toast.
- ❌ No real ERP. Mock JSON endpoint.
- ❌ No ML training. "Learning" is approval-rate statistics.
- ❌ No mobile app.
- ❌ No Kubernetes, Redis, Kafka, or microservice sprawl.

### Reliability > Sophistication
A signal that fires reliably on cue beats a clever signal that might not fire.

### If Unsure
Make the choice that **maximizes demo impact and minimizes risk**. Log the decision in `DECISIONS.md`.

---

# PART IV — PARTNER TECHNOLOGIES

Every partner must have a real, visible role. Bolt-ons are obvious to judges.

## Google DeepMind (Gemini) — CORE
- `gemini-2.0-flash` for event extraction (speed-critical path)
- `gemini-2.0-pro` for signal action drafting (quality-critical path)
- `text-embedding-004` for embeddings
- **Use structured output (JSON schema) for all extraction calls. Non-negotiable.**
- All calls go through `backend/services/gemini.py` with retries + schema validation.

## Lovable — CORE (entire UI)
- Frontend built in Lovable editor. Not in this repo.
- Backend exposes clean REST endpoints that Lovable consumes.

## Tavily — HIGH (must be visible in UI)
Two uses:
1. **Property enrichment on creation** — neighborhood info, local regulations, recent news. Stored as facts with `source: 'web (tavily)'`.
2. **Regulation watcher cron** — hourly for keywords like "Berlin rent cap", "Mietpreisbremse".

**UI requirement:** property detail page shows a "Updated from web sources" badge next to Tavily-sourced facts. This must be visible in the demo.

## Entire — HIGH (approval layer)
- Powers the approval inbox. Every approved action brokers through Entire's SDK.
- If their SDK isn't available in time, build the approval inbox natively with an `EntireBroker` interface that's easy to swap. In the pitch, say *"Entire-compatible approval layer"* — never overclaim.

## Pioneer (Fastino) — MEDIUM (learning layer)
- Track approval vs rejection rate per signal type.
- Display on dashboard: *"Keystone is prioritizing heating issues based on your behavior"*.
- If Pioneer exposes a self-training endpoint usable in hackathon time, feed the approval log to it for ranking. Otherwise implement locally with weighted thresholds adjusted from `approval_log` stats.

## Aikido — LOW but visible (trust badge)
- Run Aikido scan on this repo.
- Display "Security scan: passing" badge on Settings page and property detail footer.
- Takes 30 minutes, buys real credibility.

## Gradium — OPTIONAL STRETCH (voice briefing)
- Only if core is rock-solid by hour 40.
- Feature: "Morning briefing" — manager asks via mic, system responds via Gradium TTS with top signals.
- High-risk demo beat. Skip if anything else is unstable.

---

# PART V — ARCHITECTURE

## Services

```
/
├── KEYSTONE.md                 # this file
├── DECISIONS.md                # running log of judgment calls
├── README.md                   # setup + run instructions
├── docker-compose.yml          # Postgres + pgvector + backend + mcp_server + mock_erp
├── .env.example                # all required keys listed
├── pyproject.toml              # ruff, mypy, pytest config
├── backend/
│   ├── main.py                 # FastAPI entry
│   ├── config.py               # pydantic Settings
│   ├── api/
│   │   ├── properties.py
│   │   ├── events.py
│   │   ├── signals.py
│   │   ├── chat.py
│   │   ├── uploads.py
│   │   ├── webhooks.py         # slack, debug/trigger_event
│   │   └── sse.py              # server-sent events for live UI updates
│   ├── services/
│   │   ├── gemini.py           # ALL Gemini calls go through here
│   │   ├── tavily.py
│   │   ├── entire.py
│   │   ├── pioneer.py
│   │   ├── aikido.py
│   │   ├── imap_poller.py
│   │   ├── slack_webhook.py
│   │   └── pdf_extractor.py
│   ├── pipeline/
│   │   ├── worker.py           # async event processor
│   │   ├── router.py           # event → property routing
│   │   ├── extractor.py        # Gemini extraction
│   │   ├── differ.py           # fact diff + resolution rules
│   │   ├── applier.py          # write facts, supersede old ones
│   │   └── renderer.py         # facts → markdown
│   ├── signals/
│   │   ├── evaluator.py        # runs all rules
│   │   ├── drafter.py          # Gemini writes proposed_action messages
│   │   └── rules/
│   │       ├── recurring_maintenance.py
│   │       ├── lease_expiring.py
│   │       ├── late_payment.py
│   │       ├── regulation_change.py
│   │       └── cross_property_pattern.py
│   ├── db/
│   │   ├── schema.sql          # canonical schema (see Part VI)
│   │   ├── session.py
│   │   └── models.py           # SQLAlchemy models
│   ├── scheduler.py            # APScheduler jobs: IMAP poll, signal eval, Tavily cron
│   └── tests/
│       ├── test_pipeline_happy_path.py   # MANDATORY
│       ├── test_router.py
│       ├── test_extractor.py
│       ├── test_differ.py
│       └── test_signals.py
├── mcp_server/
│   ├── main.py
│   ├── tools.py
│   └── README.md               # Claude Desktop install instructions
├── mock_erp/
│   ├── main.py
│   └── data.json               # edited by hand during demo
└── seed/
    ├── seed.py                 # populates 4 properties with history
    └── realistic_data.py       # pre-generated realistic content
```

## Processing Pipeline (Critical Path)

Every event flows through a single async worker:

1. **Route** — match property by regex/fuzzy over name + aliases. Unmatched → unrouted inbox.
2. **Extract** — Gemini Flash with JSON schema → `{category, priority, facts_to_update[], summary}`.
3. **Diff** — for each proposed fact, compare with current. Resolution: PDF > ERP > email > Slack; higher confidence wins; newer wins.
4. **Apply** — insert fact with `source_event_id`, supersede old ones.
5. **Render** — regenerate markdown (cached per property).
6. **Signal** — run rule set against updated property + portfolio.
7. **Embed** — event + new facts into pgvector.

**Target latency: under 10 seconds end-to-end.** Achieve this by keeping prompts tight and avoiding more than one Gemini call per event on the hot path.

## Idempotency Rule
Processing the same event twice **must not** double-insert facts. Use `source_event_id` deduplication.

---

# PART VI — DATABASE SCHEMA (canonical)

```sql
-- Enable pgvector
CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE buildings (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  address TEXT NOT NULL,
  year_built INT,
  metadata JSONB DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE owners (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  name TEXT NOT NULL,
  email TEXT,
  phone TEXT,
  preferences JSONB DEFAULT '{}'::jsonb
);

CREATE TABLE properties (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  name TEXT NOT NULL,
  address TEXT NOT NULL,
  aliases TEXT[] DEFAULT '{}',          -- for routing: ["4B", "Apt 4B", "Berliner 4B"]
  owner_id UUID REFERENCES owners(id),
  building_id UUID REFERENCES buildings(id),
  metadata JSONB DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE tenants (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  property_id UUID REFERENCES properties(id) ON DELETE CASCADE,
  name TEXT NOT NULL,
  email TEXT,
  phone TEXT,
  move_in_date DATE,
  metadata JSONB DEFAULT '{}'::jsonb
);

CREATE TABLE contractors (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  name TEXT NOT NULL,
  specialty TEXT,
  rating FLOAT,
  contact JSONB DEFAULT '{}'::jsonb
);

CREATE TABLE relationships (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  from_type TEXT NOT NULL,              -- 'property' | 'owner' | 'building' | 'tenant' | 'contractor'
  from_id UUID NOT NULL,
  to_type TEXT NOT NULL,
  to_id UUID NOT NULL,
  relationship_type TEXT NOT NULL,      -- 'owned_by' | 'in_building' | 'serviced_by' | 'occupied_by'
  metadata JSONB DEFAULT '{}'::jsonb
);
CREATE INDEX idx_rel_from ON relationships(from_type, from_id);
CREATE INDEX idx_rel_to ON relationships(to_type, to_id);

CREATE TABLE events (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  source TEXT NOT NULL,                 -- 'email' | 'slack' | 'pdf' | 'erp' | 'web' | 'debug'
  source_ref TEXT,                      -- message-id, filename, etc. (used for idempotency)
  property_id UUID REFERENCES properties(id),
  raw_content TEXT NOT NULL,
  metadata JSONB DEFAULT '{}'::jsonb,
  received_at TIMESTAMPTZ DEFAULT now(),
  processed_at TIMESTAMPTZ,
  processing_error TEXT,
  embedding vector(768),
  UNIQUE (source, source_ref)
);
CREATE INDEX idx_events_unprocessed ON events (received_at) WHERE processed_at IS NULL;
CREATE INDEX idx_events_property ON events (property_id, received_at DESC);

CREATE TABLE facts (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  property_id UUID REFERENCES properties(id) ON DELETE CASCADE,
  section TEXT NOT NULL,                -- 'overview' | 'tenants' | 'lease' | 'maintenance' | 'financials' | 'compliance' | 'activity' | 'patterns'
  field TEXT NOT NULL,
  value TEXT NOT NULL,
  source_event_id UUID REFERENCES events(id),
  confidence FLOAT NOT NULL,
  valid_from TIMESTAMPTZ DEFAULT now(),
  valid_to TIMESTAMPTZ,
  superseded_by UUID REFERENCES facts(id),
  created_at TIMESTAMPTZ DEFAULT now(),
  embedding vector(768)
);
CREATE INDEX idx_facts_current ON facts (property_id, section, field) WHERE superseded_by IS NULL;

CREATE TABLE signals (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  property_id UUID REFERENCES properties(id),
  type TEXT NOT NULL,                   -- rule name
  severity TEXT NOT NULL,               -- 'low' | 'medium' | 'high' | 'urgent'
  message TEXT NOT NULL,
  evidence JSONB DEFAULT '[]'::jsonb,   -- list of event_ids/fact_ids supporting this signal
  proposed_action JSONB,                -- {type, payload, drafted_message}
  status TEXT DEFAULT 'pending',        -- 'pending' | 'approved' | 'rejected' | 'resolved'
  created_at TIMESTAMPTZ DEFAULT now(),
  resolved_at TIMESTAMPTZ
);
CREATE INDEX idx_signals_pending ON signals (created_at DESC) WHERE status = 'pending';

CREATE TABLE approval_log (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  signal_id UUID REFERENCES signals(id),
  user_id TEXT DEFAULT 'demo_user',
  decision TEXT NOT NULL,               -- 'approved' | 'rejected' | 'edited'
  edits JSONB,
  created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE outbox (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  signal_id UUID REFERENCES signals(id),
  channel TEXT NOT NULL,                -- 'email' | 'slack'
  recipient TEXT NOT NULL,
  subject TEXT,
  body TEXT NOT NULL,
  sent_at TIMESTAMPTZ DEFAULT now()
);
```

---

# PART VII — GEMINI EXTRACTION (canonical)

All extraction goes through `backend/services/gemini.py` with this schema + prompt.

```python
EXTRACTION_SCHEMA = {
  "type": "object",
  "properties": {
    "category": {
      "type": "string",
      "enum": ["maintenance", "lease", "payment", "complaint",
               "compliance", "tenant_change", "owner_communication", "other"]
    },
    "priority": {"type": "string", "enum": ["low", "medium", "high", "urgent"]},
    "facts_to_update": {
      "type": "array",
      "items": {
        "type": "object",
        "properties": {
          "section": {
            "type": "string",
            "enum": ["overview", "tenants", "lease", "maintenance",
                     "financials", "compliance", "activity", "patterns"]
          },
          "field": {"type": "string"},
          "value": {"type": "string"},
          "confidence": {"type": "number", "minimum": 0, "maximum": 1}
        },
        "required": ["section", "field", "value", "confidence"]
      }
    },
    "summary": {"type": "string"}
  },
  "required": ["category", "priority", "facts_to_update", "summary"]
}

EXTRACTION_PROMPT = """You are processing an event for a property management context system.

Property: {property_name}
Current relevant context:
{current_context_excerpt}

New event from {source}:
---
{raw_content}
---

Extract structured facts. Rules:
- `section` + `field` must be specific and stable (e.g. `section=lease, field=end_date`, not `section=info, field=stuff`)
- `value` should be self-contained and human-readable
- `confidence` reflects certainty given the source
- If the event doesn't warrant updates (e.g. chit-chat), return empty `facts_to_update`
- Always return a one-line `summary` for the activity feed

Return ONLY valid JSON matching the schema."""
```

---

# PART VIII — MCP SERVER (canonical tool set)

The MCP server is a thin adapter. All tools call the backend REST API.

```
get_property_context(property_id: str) -> str
    Returns the rendered markdown for the property.

search_properties(query: str, limit: int = 5) -> list[dict]
    Semantic search across all properties. Returns [{id, name, snippet, score}].

list_signals(property_id: str | None = None, severity: str | None = None) -> list[dict]
    Returns pending signals, optionally filtered.

get_activity(property_id: str, since: str | None = None) -> list[dict]
    Returns recent events on a property.

propose_action(property_id: str, action: dict) -> dict
    Creates a pending signal from an external AI. Returns {signal_id, status}.
```

`mcp_server/README.md` must include the exact Claude Desktop JSON config block so anyone (including judges) can install it in under a minute.

---

# PART IX — SIGNAL RULES

Start with these three for the demo, add two more if time permits.

## Rule 1: `recurring_maintenance` (the demo hero)
SQL: for a given property, count maintenance facts with similar keywords (e.g. "heat%") in the past 120 days. If ≥3, fire.
Action draft: Gemini Pro composes an owner notification + contractor dispatch recommendation with specific timeline.

## Rule 2: `lease_expiring`
SQL: lease `end_date` facts with value < `now() + 60 days` and no active renewal signal.
Action draft: renewal proposal to owner with rent adjustment suggestion based on market facts from Tavily enrichment.

## Rule 3: `cross_property_pattern` (the portfolio-intelligence closer)
SQL across portfolio: bucket properties by building year (pre-1990, 1990-2010, post-2010) and count properties with same issue category in past 90 days. If ≥3 in a bucket, fire a portfolio signal.
Action draft: portfolio-wide inspection recommendation.

## Rules 4-5 (stretch): `late_payment_anomaly`, `regulation_change` (powered by Tavily cron).

---

# PART X — PHASED BUILD PLAN

Each phase has an **explicit exit criterion**. Do not start phase N+1 until phase N is verified.

## Phase 0 — Setup (Hours 0–3)
**Build:**
- `docker-compose.yml` with Postgres + pgvector
- FastAPI skeleton (`backend/`)
- MCP server skeleton (`mcp_server/`)
- Mock ERP (`mock_erp/`)
- `.env.example` with every key
- `db/schema.sql` (from Part VI, runnable)
- `seed/seed.py` — 4 properties with rich realistic history (use Gemini to pre-generate emails, lease content, maintenance history)
- Deploy to Railway: Postgres + backend + MCP. Public URL.

**Exit criterion:** `GET /properties` on the public URL returns 4 seeded properties with rendered markdown that looks like a real product.

## Phase 1 — Demo Spine (Hours 3–12) — MUST WORK
**Build:**
- IMAP poller (APScheduler, every 10s) with app password auth
- `POST /debug/trigger_event` as IMAP backup (critical for demo resilience)
- Event insertion with idempotency via `(source, source_ref)` unique constraint
- Worker: route → extract (Gemini Flash) → diff → apply → re-render
- `GET /properties/{id}/markdown` + SSE `/properties/{id}/events` for live updates
- Activity feed endpoint

**Exit criterion:** send an email to the demo inbox → within 10s the UI (or `curl`) shows the updated markdown with a new fact sourced from that event. **DO NOT PROCEED UNTIL STABLE.**

## Phase 2 — Sources + Enrichment (Hours 12–20)
**Build:**
- Slack webhook with signature verification
- PDF upload → `pdfplumber` extract → event pipeline
- Mock ERP poller (every 30s)
- Tavily enrichment on property creation (runs once)
- "Updated from web sources" badge on Tavily-sourced facts

**Exit criterion:** all four sources flow through the pipeline. PDF upload visibly updates markdown. At least one fact on each property has a visible Tavily badge.

## Phase 3 — Intelligence + Approval (Hours 20–30)
**Build:**
- Signal rules 1, 2, 3
- Gemini Pro `drafter.py` that writes expert-quality `proposed_action` messages (see Signal Quality Bar)
- Approval inbox endpoints: list, approve, reject, edit
- Entire integration (or `EntireBroker` interface with local impl)
- Outbox writes on approve
- Approval log updates

**Exit criterion:** trigger a signal → proposed action appears in approval inbox → approve → outbox row + signal resolved. All visible in UI.

## Phase 4 — MCP + Portfolio Intelligence (Hours 30–38)
**Build:**
- MCP server with all 5 tools wired to backend
- `mcp_server/README.md` with Claude Desktop config block, tested live
- Cross-property signal rule
- Context graph endpoint: `GET /properties/{id}/graph` returns nodes + edges
- Portfolio banner endpoint

**Exit criterion:** open Claude Desktop, ask "What's going on with apartment 4B?", get a correct answer sourced from Keystone. Portfolio view shows at least one cross-property signal.

## Phase 5 — Polish + Sponsor Visibility (Hours 38–44)
**Build:**
- Regulation watcher (Tavily cron) + signal rule 4
- Aikido scan + badge
- Pioneer learning dashboard (approval-rate stats)
- Polish UI in Lovable
- Final seed data pass — ensure portfolio view is vivid
- Gradium voice briefing **only if everything above is solid**

**Exit criterion:** every partner tool is visible somewhere in the demo flow. Settings page shows security + learning dashboards.

## Phase 6 — Demo Lock (Hours 44–48)
**Build nothing new.**
- Rehearse 2-minute pitch 5+ times
- Test full demo flow 10x with real email from phone
- Record backup video
- Seed "starter signals" so portfolio is visibly alive at demo start
- Prepare 5 Q&A answers (how does it scale, what about privacy, what's the business model, how accurate is extraction, what happens when it's wrong)
- Sleep 4 hours minimum

**Exit criterion:** 10 consecutive successful demo runs. Backup video saved. Laptop on charger. You are calm.

---

# PART XI — CODE STANDARDS (enforce from line 1)

- **Python 3.11+**, type hints everywhere.
- **Pydantic** for all schemas including Gemini structured outputs.
- **`ruff` + `mypy --strict`** pass before every commit. Enforce in `pyproject.toml`.
- **No global state.** Dependencies injected via FastAPI `Depends`.
- **Docstrings** on every service function: what it does, inputs, outputs, side effects.
- **Structured logging** with `structlog`. Every LLM call logs prompt hash + latency + token count. Judges notice production-grade logs.
- **Idempotent event processing.** Unique constraint on `(events.source, events.source_ref)`.
- **One integration test is mandatory:** `test_pipeline_happy_path.py` — insert email event → assert fact created with correct source linkage → assert markdown contains new fact. This test protects the demo.
- **Secrets only via env.** Never commit keys. `.env.example` lists all required keys with dummy values.
- **No dead code.** If a file isn't imported, delete it. Clean repo is a winning repo.

---

# PART XII — RISK MITIGATION

## Demo-day risks and counters

| Risk | Counter |
|---|---|
| IMAP polling fails on venue Wi-Fi | `POST /debug/trigger_event` to inject the email payload directly |
| Gemini API slow/down | Cache last successful extraction per event type; fallback to rule-based extraction for demo emails |
| Claude Desktop config broken | Pre-recorded 20-second MCP demo in backup video |
| Lovable UI crashes | Keep a static HTML fallback of the property page |
| Live demo freezes | Backup video with voiceover, ready in <5 seconds |
| Aikido scan fails | Screenshot of passing scan from earlier run |

## Demo stability checklist (hour 46)
- [ ] 10 consecutive `trigger_event` runs succeed in <10s each
- [ ] 10 consecutive real-email runs succeed in <15s each
- [ ] MCP works from Claude Desktop three times in a row
- [ ] Signal fires on cue with the pre-seeded "heating" events
- [ ] Backup video recorded and saved to two locations (laptop + cloud)
- [ ] Phone fully charged for email-sending
- [ ] Demo laptop on power, external display tested

---

# PART XIII — `DECISIONS.md` PROTOCOL

Every time you make a judgment call (picked one tech over another, mocked something, deferred a feature, chose a threshold), append to `DECISIONS.md`:

```
## [timestamp] - [short title]
Context: <1 sentence>
Decision: <what you chose>
Reason: <why — especially demo impact>
Revisit if: <condition to reconsider>
```

This document becomes invaluable during Q&A. It also prevents oscillation across sessions.

---

# PART XIV — CURRENT PHASE

**[UPDATE THIS AT THE END OF EVERY SESSION]**

**Current phase:** Phase 6 — Demo Lock (**automatable rows of the Part XII checklist green**). Remaining: Lovable UI rehearsal, backup video, sleep.
**Next deliverable:** Demo. No further backend changes.
**Blockers:** None on this repo. Pitch rehearsal + recorded backup video are non-Claude tasks.
**Last session notes (Phase 1):**
- `backend/services/gemini.py` is the single choke-point. Uses structured output (the Part VII JSON schema), 3× retries with backoff, and logs prompt hash + latency + token counts on every call. Raises `GeminiUnavailable` when `GEMINI_API_KEY` is unset or requests fail.
- `backend/pipeline/extractor.py` calls Gemini Flash when available, otherwise a deterministic keyword-based fallback (heating/leak/payment/lease/compliance) so the demo survives wifi/quota loss (Part XII mitigation).
- `backend/pipeline/router.py` resolves events to properties via alias substring match first (longest wins) and token-overlap fallback. Unrouted events are parked with `processing_error='unrouted'`.
- `backend/pipeline/differ.py` enforces source precedence (pdf > erp > email > slack > web > debug), confidence, and recency before returning a `DiffPlan`.
- `backend/pipeline/applier.py` writes new fact rows and stamps `superseded_by` on displaced predecessors.
- `backend/pipeline/events.py` owns idempotent event insertion (ON CONFLICT on `(source, source_ref)`) plus an in-process `EventBus` for SSE fan-out.
- `backend/pipeline/worker.py` uses `SELECT ... FOR UPDATE SKIP LOCKED LIMIT 1` per tick, then routes → extracts → diffs → applies → stamps `processed_at` → publishes to the event bus. Errors are isolated in a fresh session so the lock is released cleanly.
- `backend/scheduler.py` runs the worker every 2s and IMAP poll every 10s via APScheduler, started in the FastAPI lifespan.
- `backend/services/imap_poller.py` polls unread messages and ingests each as an event (idempotent via RFC822 Message-ID). No-ops cleanly when `IMAP_PASSWORD` is the placeholder.
- `backend/api/events.py` exposes `POST /debug/trigger_event` as the demo-resilience path — inserts the event and drains the worker inline so `curl` POSTs return with updated markdown visible. Idempotent on `source_ref`.
- `backend/api/sse.py` exposes `GET /properties/{id}/events` with `text/event-stream` and a 15s heartbeat; `backend/api/properties.py` adds `GET /properties/{id}/activity`.
- `backend/tests/test_pipeline_happy_path.py` ingests an email event, runs the worker, asserts a sourced fact was written and shows up in the rendered markdown. Protects the demo.
- `seed/seed.py` now stamps `processed_at = received_at` on seeded events so the worker doesn't re-extract the hand-crafted dataset on boot.
- Local self-verify: `/health` → `/debug/trigger_event` → markdown update in **~25ms** (target 10 s). 10 consecutive trigger runs all under 25ms. SSE stream delivers a `fact_update` payload when a new event is processed. Integration test passes (`pytest backend/tests/test_pipeline_happy_path.py`).

**Phase 2 session notes:**
- `mock_erp/main.py` is a one-file FastAPI service on `:8001` that re-reads `data.json` every request — edit the JSON live during the demo to fake a new payment. `GET /payments` supports `account` + `since` filters.
- `backend/services/erp_poller.py` drains that endpoint every 30s via APScheduler, inserting each payment row as an `erp` event keyed on `payment_id` (UNIQUE constraint enforces idempotency).
- `backend/services/pdf_extractor.py` wraps `pdfplumber` with a max-pages cap; `backend/api/uploads.py` exposes `POST /uploads/pdf` (multipart) with `source_ref = {filename}:{sha256[:16]}` for dedupe and an inline worker drain.
- `backend/services/slack_webhook.py` implements the canonical `v0:{ts}:{body}` HMAC-SHA256 verification with a 5-minute replay window; `backend/api/webhooks.py` mounts `POST /webhooks/slack` and answers the `url_verification` challenge. Bad signature → 401.
- `backend/services/tavily.py` wraps the `tavily-python` client and, on enrichment, writes the web event + two sourced facts (`overview.market_snapshot`, `compliance.regulation_watch`) in one transaction so the 🌐 badge is always present. Offline fallback kicks in when `TAVILY_API_KEY` is missing or errors, with a clearly-labelled "offline snapshot" value — the demo badge stays up on flaky wifi (Part XII mitigation).
- `backend/api/properties.py` now exposes `POST /properties` (creates + enriches) and `POST /properties/{id}/enrich` (idempotent admin utility for existing seeded rows).
- `backend/pipeline/renderer.py` joins facts with events to surface `source` and appends a 🌐 _Updated from web sources_ badge next to every web-sourced fact.
- `backend/scheduler.py` now runs three jobs: worker (2s), IMAP poll (10s), ERP poll (30s).
- Phase-2 self-verify: all four sources (email, Slack, PDF, ERP) land in the pipeline and show up in the activity feed for Apt 4B; every property has ≥2 web-badged facts after enrichment; Slack webhook 401s on bad signature; PDF upload turns into a fact within ~100ms; Phase 1 regression test still passes.

**Phase 3 session notes:**
- `backend/signals/types.py` defines `SignalCandidate` (rule output) and `ProposedAction` (persisted JSONB on `signals.proposed_action`).
- `backend/signals/rules/recurring_maintenance.py` fires `urgent` per-property signals when ≥3 maintenance facts match `heat|boiler|water|leak|electric` keywords within 120 days.
- `backend/signals/rules/lease_expiring.py` parses `lease.end_date` values and fires `medium/high` signals when the end date is within 60 days (threshold constant so demo can widen it).
- `backend/signals/rules/cross_property_pattern.py` implements both demo beats: shared-building boiler signal (beat 1:00 "wow") and building-year-cohort portfolio signal (beat 1:45). Portfolio signals use `property_id=NULL`.
- `backend/signals/evaluator.py` runs every rule, dedupes against already-pending signals keyed on `(property_id, type, payload.hint.{topic|subtype})`, asks the drafter for a `ProposedAction`, and inserts into `signals`. Scheduler runs it every 30s; `POST /signals/evaluate` fires it on demand for demo determinism.
- `backend/services/gemini.py` gained a Pro `draft_action_message` path with 2x retries; `backend/signals/drafter.py` prefers it and falls back to a four-part template (observation → risk → concrete next step → deadline) so the Signal Quality Bar holds without Gemini.
- `backend/services/entire.py` ships a `runtime_checkable Protocol` + `LocalEntireBroker` that writes outbox rows. `get_broker/set_broker` let the real Entire SDK drop in without touching the signals API.
- `backend/api/signals.py` exposes `GET /signals`, `GET /signals/{id}`, `POST /signals/evaluate`, `POST /signals/{id}/approve|reject|edit`. Approve routes through `get_broker()`, writes `outbox` + `approval_log` (decision=`approved`), and flips status to `resolved`. Edit persists the subject/body diff into `approval_log` (decision=`edited`) for Pioneer's learning stats later.
- Phase-3 self-verify: evaluator fires 3 signals (recurring_maintenance × 2 + shared-boiler cross), repeat calls create 0 (dedupe OK), approve → outbox row + `approval_log` entry + `signals.status='resolved'`, reject writes log + status='rejected', edit writes `approval_log(decision='edited')` with before/after diff. Phase 1 integration test still green.

**Phase 4 session notes:**
- Backend gained three new endpoints: `GET /properties/search?q=…` (multi-term weighted keyword search), `GET /properties/{id}/graph` (nodes + edges — owner, building, tenants, contractors), `GET /portfolio/banner` + `GET /portfolio/summary` (dashboard header + top-priority portfolio signal), and `POST /signals/propose` (external AI entry point that always writes `status='pending'` — humans approve in the inbox).
- `mcp_server/tools.py` wraps `httpx.AsyncClient` and pokes the REST backend (overridable via `KEYSTONE_BASE_URL`). Zero DB imports — Part V's "thin adapter" rule upheld.
- `mcp_server/main.py` registers the five canonical Part VIII tools (`get_property_context`, `search_properties`, `list_signals`, `get_activity`, `propose_action`) on a `FastMCP` instance. `python -m mcp_server.main` speaks the MCP stdio protocol.
- `mcp_server/README.md` ships a copy-pasteable Claude Desktop `claude_desktop_config.json` snippet + a smoke-test recipe.
- Phase 4 self-verify: all five MCP tools succeed end-to-end against a running backend, including a real stdio `initialize` → `list_tools` → `call_tool` handshake. Portfolio banner returns the shared-boiler cross-property signal. Search + graph endpoints return expected shapes. `POST /signals/propose` appears in the inbox tagged as AI-authored.

**Phase 5 session notes:**
- `backend/services/tavily.py` gained `watch_regulations()` — polls Tavily for five rent-regulation queries when `TAVILY_API_KEY` is live, otherwise seeds three clearly-labelled offline headlines. Every event stamped `processed_at` so the extractor worker skips it; idempotent via `source_ref = tavily-reg:{query}:{hour}`.
- `backend/signals/rules/regulation_change.py` is rule #4. Portfolio-level (`property_id=NULL`), `medium` severity, reads `web` events tagged `metadata.regulation=true` inside a 14-day window.
- Evaluator + drafter + subject-line picker updated to recognize the new type; dedupe keys on `payload.hint.subtype = source_ref` so each distinct headline fires at most one pending signal.
- `backend/services/aikido.py` ships `get_badge()` — live Aikido REST when keyed, otherwise a `local_snapshot` badge carrying the current git SHA from `git rev-parse --short HEAD`. Response discloses `source` so the UI can honestly distinguish live vs fallback.
- `backend/services/pioneer.py` ships `compute_learning()` — reads `approval_log`, derives per-signal-type approval rates (edits = 0.8 × approved), maps to `[0.5, 1.5]` priority weights with sample-size shrinkage, emits a trend-line sentence suitable for the Settings > Learning panel.
- `backend/api/settings.py` exposes `GET /settings/security`, `GET /settings/learning`, and `POST /settings/regulation_watch` (manual trigger that runs the watcher + evaluator inline so fresh signals show up in the inbox immediately).
- `backend/scheduler.py` adds the `regulation_watch` job at 60-minute cadence; `backend/main.py` mounts the settings router.
- Phase 5 self-verify: `POST /settings/regulation_watch` → 3 events ingested + 3 regulation_change signals created; repeat calls → 0/0 (dedupe OK); `/settings/security` returns `passing` with commit SHA; `/settings/learning` produces a rich snapshot with trend line "Keystone is prioritizing **cross property pattern** based on your approval behavior (100% approved over 2 proposals)" after a mix of approve/edit/reject decisions. Phase 1 regression test still green.

**Phase 6 session notes:**
- Added `seed/demo_bootstrap.py` — one-shot script that wipes a fresh DB to a visibly-alive demo state: schema → seed → Tavily-enrich every property → run regulation watcher → fire evaluator. Idempotent; safe to re-run between rehearsals.
- Added `DEMO_QA.md` — written answers to the five Part XII judge questions (scale, privacy, business model, extraction accuracy, what happens when it's wrong), each grounded in repo paths so we can show the file behind the answer.
- Phase XII stability checklist (automatable rows):
  - **10× `/debug/trigger_event`** — 10/10 under 10 s; avg **24 ms**, max **64 ms**.
  - **3× MCP stdio handshake** — 3/3 clean, all under **250 ms**, every run lists 5 tools and a `search_properties` call returns live JSON.
  - **Signal fires on cue** — bootstrap → 6 pending signals (4 portfolio-level), portfolio banner returns the urgent shared-boiler signal.
  - **Sponsor visibility** — every property has 2-4 🌐 web-badged facts, Aikido `passing` with current commit SHA, Pioneer trend line goes from neutral → "prioritizing cross_property_pattern" after one approval.
  - **Phase 1 integration test** — passes in 0.17 s.
- Remaining checklist rows are human work: backup video, real-email run from a phone, calm pitch rehearsal, laptop on charger.

---

# PART XV — NON-NEGOTIABLES

Before submitting:

1. ✅ 2-minute demo works end-to-end on real Wi-Fi.
2. ✅ Backup video exists.
3. ✅ At least 5 partner tools visible in the demo.
4. ✅ MCP works with Claude Desktop, live.
5. ✅ Every fact in the markdown has a clickable source.
6. ✅ No feature outside demo scope shipped.
7. ✅ Repo is clean: `ruff` + `mypy --strict` pass, no dead code, README explains setup in 5 steps.
8. ✅ `DECISIONS.md` reflects every non-obvious choice.

---

# PART XVI — CONNECTOR ARCHITECTURE (Phase 8)

Phase 8 introduces `connectors/` as the customer-agnostic ingestion
layer. **Buena is the first composer, not a special case.** Every new
customer integration is a new composite under `connectors/`; the
shape-handling primitives below are shared.

## Contract

Every connector satisfies one of these Protocols (`connectors/base.py`):

```python
@runtime_checkable
class Connector(Protocol):
    name: str
    def pull(self) -> Iterator[ConnectorEvent]: ...
    def stream(self) -> Iterator[ConnectorEvent]: ...
```

`ConnectorEvent` mirrors the keyword arguments of
`backend.pipeline.events.insert_event` so the call site is a one-liner.
Every event carries `source`, `source_ref`, `raw_content`, `metadata`,
`received_at`, and an optional pre-routed `property_id`. PDF-derived
events additionally carry `metadata.document_type` (Phase 9 constraints
read this).

## Idempotency

`(events.source, events.source_ref)` is `UNIQUE` (Part VI). Connectors
must produce the **same** `source_ref` on every re-run for the same
input. Concrete formulas:

| Source | `source_ref` |
|---|---|
| Email (`.eml`)        | `Message-ID` (fallback `sha256(From+Date+Subject)[:16]`) |
| Bank txn              | `sha256(datum + betrag + verwendungszweck + buena_id)[:12]` |
| Invoice / letter PDF  | `<filename>:<sha256(file_bytes)[:16]>` |
| Master data row       | natural key (email / id / address) |

## PII redaction at ingestion (not at render)

`connectors/redact.py` is the **only** module that touches raw IBANs,
full phone numbers, or full email domains on the way *into* the
database. Every connector calls `redact.iban_last4`, `phone_last4`,
`email_redact`, or `scrub_text` before yielding a `ConnectorEvent`.
Tests assert no `r"DE\d{20}"` or full E.164 phone survives.

PII contract:

- IBAN  → `****<last4>`  (e.g. `****1349`)
- Phone → `+CC *** **<last4>` (country code preserved when present)
- Email → `<local-part>@example.com` (local part kept; domain replaced)

## Cost ledger

Every LLM call charges `connectors/cost_ledger.py`. The ledger is a
durable Postgres table (`cost_ledger`, additive migration in
`connectors/migrations.py`) keyed on `source_label` so a single
`--max-total-cost-usd` cap governs the entire backfill across CLI
invocations. Subsequent runs read the persisted state and abort
immediately if the cap was already hit. Reset is friction-gated:
`--reset-cost-ledger` prompts `y/N`.

Sub-labels share the same table — e.g. the PDF document-type
classifier (`pdf_doctype`) gets a `$2` sub-cap inside the broader
`buena_email` budget.

## Composite layout

`connectors/buena_archive.py` is the only module that knows Buena's
directory shape. It exposes `load_stammdaten(root)`, `iter_emails`,
`iter_bank`, `iter_invoices`, `iter_letters`, and
`iter_incremental_day(day)`. The next customer ships a sibling file
(`connectors/<customer>_archive.py`) and reuses the primitives.

## Extending

Adding a new customer:

1. Verify their data shape — what files / formats / cardinalities.
2. Reuse existing primitives where possible
   (`csv_stammdaten`, `eml_archive`, `camt_bank`,
   `pdf_invoice_archive`, `pdf_letter_archive`).
3. Write a new composite `connectors/<customer>_archive.py`.
4. Add a `<customer>_archive` smoke test (skip when the dataset is
   absent, like `connectors/tests/test_buena_archive.py`).
5. Wire a CLI subcommand under `connectors/cli.py` (Step 2+).

---

# FINAL REMINDER

Do not build the most complete system. Build the system that makes judges say:

> **"This already feels like a real product."**

Then stop. Then rehearse. Then win.