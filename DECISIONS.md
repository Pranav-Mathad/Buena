# DECISIONS

Running log of non-obvious judgment calls. Format per KEYSTONE.md Part XIII.

## 2026-04-24 — pgvector image: `pgvector/pgvector:pg15` (not `ankane/pgvector:pg15`)
Context: Part V specified `ankane/pgvector:pg15`, but that tag does not exist on Docker Hub — `ankane/pgvector` uses v-prefixed tags and the modern pg15 flavor has moved to the official `pgvector/pgvector` repo.
Decision: Use `pgvector/pgvector:pg15` in `docker-compose.yml`.
Reason: Unblocks Phase 0 without any behavior change; `pgvector/pgvector` is the canonical upstream image and pulls cleanly. Pinning the requested tag would have failed on `docker compose up`.
Revisit if: We standardize on a newer Postgres (pg16) or move to Railway-managed Postgres.

## 2026-04-24 — Host port mapping `5433:5432`
Context: The dev box already runs a Homebrew `postgresql@15` on 127.0.0.1:5432, so publishing the container on 5432 leaves `localhost:5432` pointed at the host Postgres (where role `keystone` doesn't exist) and the seed script fails.
Decision: Expose Postgres on host port 5433, container 5432. `.env.example` DSNs updated in lockstep so no downstream code has to know.
Reason: Stopping the user's Brew service would be destructive to their environment. The port number isn't load-bearing — all application connections read `DATABASE_URL(_SYNC)` from `.env`.
Revisit if: We ship to Railway (ports irrelevant there) or the team wants to standardize on 5432 locally.

## 2026-04-24 — Seed uses psycopg2 (sync), not SQLAlchemy async
Context: The seed needs to run schema DDL and ~100 upserts from a CLI. The production code uses SQLAlchemy async, but bringing up an event loop for a one-shot script adds complexity.
Decision: `seed/seed.py` connects with `psycopg2` directly (sync). Production request paths still use the async engine in `backend/db/session.py`.
Reason: Faster, simpler, identical semantics for a one-shot. `psycopg2-binary` is already a required dep for parity with APScheduler-backed jobs later.
Revisit if: We want to share upsert helpers between the seed and runtime (unlikely — seed data stays here).

## 2026-04-24 — Idempotent seed via natural-key SELECT-then-INSERT
Context: The user asked for idempotent seed. `events` already has `UNIQUE (source, source_ref)`, but owners/buildings/contractors/properties/facts have no natural unique constraints in the canonical schema.
Decision: For each of those tables the seeder does `SELECT id WHERE <natural key>` first and only inserts if absent. Facts are matched on `(property_id, section, field, source_event_id)`.
Reason: Lets re-runs be safe without modifying the canonical schema (Part VI is verbatim). Keeps the seed understandable.
Revisit if: We start seeing drift between seeded and runtime-produced facts — in that case the differ/applier will own supersession and the seed should stop trying to be an authority.

## 2026-04-24 — Fact confidence > 0.8 default for seed, lower for web/inferential
Context: Phase 0 seeds facts directly instead of going through Gemini extraction; we still want the `confidence` column to be meaningful for the demo.
Decision: Lease/tenant/financial facts sourced from a PDF carry 0.95–0.99; maintenance-from-email carry 0.85–0.94; Tavily/neighborhood facts carry 0.75–0.85.
Reason: Signals downstream are meant to key off confidence, and the demo markdown visibly prints the value. Gradient matches how a real extractor would score these sources.
Revisit if: Gemini extractor lands and its scores calibrate differently — re-seed and align.

## 2026-04-24 — Phase 1: dual-path extractor (Gemini Flash + rule-based fallback)
Context: Part IV designates Gemini Flash as the extraction engine. The demo venue's wifi + Gemini's quota are both failure modes; Part XII explicitly lists "fallback to rule-based extraction for demo emails" as the mitigation.
Decision: `backend/pipeline/extractor.py` calls `backend.services.gemini.extract_facts` when `GEMINI_API_KEY` is set, otherwise runs a deterministic keyword-based extractor covering heating/leak/payment/lease/compliance shapes. Both paths return the same `ExtractionResult`.
Reason: Demo can't brick on a network issue, and the fallback also makes local dev / CI possible without burning Gemini quota. Gemini remains the production path — the fallback does **not** write lower-quality facts when Gemini is available.
Revisit if: Gemini throughput becomes reliable enough to drop the fallback, or we want to log calibration deltas between the two paths.

## 2026-04-24 — Phase 1: seed events stamped `processed_at = received_at`
Context: Dropping seed events into the queue with `processed_at IS NULL` caused the Phase 1 worker to rerun the rule-based extractor over the hand-crafted dataset on first boot, producing spurious "Latest heating issue: Mietvertrag" facts.
Decision: `seed/seed.py` now writes `processed_at = received_at` for every seeded event, signalling to the worker that the hand-authored facts are authoritative.
Reason: Keeps the seeded markdown clean; the live pipeline only touches events that actually arrive post-boot. No schema change.
Revisit if: We start wanting the pipeline to *re-extract* over the seed (e.g. to verify Gemini output against ground truth) — in which case add a `--reprocess-seed` switch instead of flipping the default.

## 2026-04-24 — Phase 5: Regulation watcher runs hourly, offline mode seeds canned headlines
Context: Part IV says "Regulation watcher cron — hourly for keywords like 'Berlin rent cap', 'Mietpreisbremse'." Demo must still show a `regulation_change` signal on a fresh DB even when `TAVILY_API_KEY` is absent (flaky wifi, missing key, etc.).
Decision: `backend/services/tavily.watch_regulations` polls Tavily for five canonical queries when the key is set; otherwise seeds three clearly-labelled "offline snapshot" headlines as `web` events tagged `metadata.regulation=true`. Every event is stamped `processed_at` so the extractor worker doesn't touch it — the `regulation_change` rule reads the events directly. Idempotent via `source_ref = tavily-reg:{query}:{hour}`.
Reason: Preserves the partner-visibility bar ("every partner tool visible somewhere in the demo") without lying about live data when offline, and the hourly cadence is what Part IV specifies.
Revisit if: We want streaming / diff-based regulation change detection — at that point store a `content_hash` per headline and only fire when it changes.

## 2026-04-24 — Phase 5: Aikido badge reads live when keyed, otherwise local snapshot with git SHA
Context: Aikido is a scheduled scanner, not a mid-request API. The demo renders a "Security scan: passing" badge on Settings; we need it to be honest whether or not the CI scan has been hooked up.
Decision: `backend/services/aikido.get_badge` attempts a live GET when `AIKIDO_API_KEY` is present, falls back to a `local_snapshot` badge that surfaces the git SHA the demo is running on (via `git rev-parse --short HEAD`). The response payload tags `source` so the UI / judges know which mode.
Reason: "Passing" without context is a hand-wave. Showing the commit SHA + explicit source mode makes the fallback defensible and obvious.
Revisit if: CI starts publishing scan results somewhere (e.g., GitHub Actions artifact) — point the fetcher at that instead of Aikido's REST API.

## 2026-04-24 — Phase 5: Pioneer approval-rate weighting computed locally
Context: Pioneer / Fastino may not expose a usable ranking endpoint in hackathon time. The Settings > Learning panel still needs to show Keystone is adapting to the human.
Decision: `backend/services/pioneer.compute_learning` reads `approval_log`, derives per-signal-type approval rates (edits count as 0.8 × approved), and maps them to priority weights in `[0.5, 1.5]` with sample-size shrinkage (< 5 proposals anchored to 1.0). The trend-line sentence highlights the top-weighted signal type ("Keystone is prioritizing cross_property_pattern based on your approval behavior (100% approved over 2 proposals)"). Interface is a plain dataclass so swapping in a real Pioneer call later is a one-function change.
Reason: Keeps the learning story true to what the system has observed, backs up the "Pioneer learning layer" pitch with verifiable numbers, and avoids overclaiming ML we haven't trained.
Revisit if: We start feeding live approval logs to Pioneer's training endpoint — replace `compute_learning` with a call that merges remote weights into the local snapshot.

## 2026-04-24 — Phase 4: MCP server is a thin REST adapter, not a database client
Context: Part V says "MCP server is a thin adapter. All tools call the backend REST API." An alternative would be sharing SQLAlchemy sessions / pgvector access across both surfaces for speed.
Decision: `mcp_server/tools.py` wraps `httpx.AsyncClient` and hits the same REST endpoints the UI uses. Zero DB imports. Every schema change lands in exactly one place (the REST layer), and the MCP server can be pointed at a remote Keystone by flipping `KEYSTONE_BASE_URL`.
Reason: Matches the constitution, keeps the MCP surface under one pane of review, and means judges can reason about the MCP tools the same way they reason about the UI.
Revisit if: We later need streaming tool results (e.g., large markdown) where an HTTP round-trip is the bottleneck — then add an in-process shortcut, keeping the REST adapter as the fallback.

## 2026-04-24 — Phase 4: Multi-term keyword search (no embeddings yet)
Context: Part VIII calls `search_properties` "semantic search". The schema has `vector(768)` columns on facts + events, but embedding generation hasn't been wired into the worker yet.
Decision: Ship a per-term LIKE search with weighted scoring (name/alias > address > fact-value, hit-count boost) and split the query on whitespace so `"heating Berlin"` still finds useful hits. Label the tool `search_properties` — not `semantic_search` — to stay honest.
Reason: Keeps the MCP surface working today without a Gemini round-trip per query, and aligns with the "reliability > sophistication" rule. Embedding-based search can slot in behind the same endpoint in Phase 5.
Revisit if: We ship the Gemini embedding pipeline — swap the SQL out for `ORDER BY embedding <=> :query_vec LIMIT :k` without changing the endpoint shape.

## 2026-04-24 — Phase 4: `propose_action` writes a pending signal, never dispatches
Context: MCP tools can run without human approval. Part I Principle 3 ("System proposes. Human approves.") forbids letting an external AI close the loop end-to-end.
Decision: `POST /signals/propose` inserts a `status='pending'` signal tagged `payload.proposed_by='external_ai'`; approval still flows through `/signals/{id}/approve` and the Entire-compatible broker.
Reason: Keeps the human-in-the-loop invariant intact for MCP-originated actions and gives us an audit trail (the inbox shows "proposed by Claude Desktop").
Revisit if: We later want first-class AI-co-signed actions — add a separate `ai_authorized` pathway rather than loosening this.

## 2026-04-24 — Phase 3: portfolio-level signals use `property_id = NULL`
Context: `cross_property_pattern` fires across a building or an entire portfolio cohort; there's no single property to attach it to.
Decision: Persist those signals with `property_id = NULL`. The inbox filter `?property_id=X` still works for per-property signals; portfolio-level ones appear in the unfiltered listing and (Phase 4) will surface on the portfolio dashboard banner.
Reason: Keeps the schema as-is (Part VI). A synthetic "portfolio" property would distort every per-property query.
Revisit if: Portfolio UI needs more than one dimension of grouping — then add a `scope` column instead of overloading NULL.

## 2026-04-24 — Phase 3: dedupe signals on `(property_id, type, payload.hint.{topic|subtype})`
Context: The evaluator runs every 30s; without dedupe it would insert a fresh pending signal for every pattern on every tick, flooding the inbox.
Decision: `_already_open` in `backend/signals/evaluator.py` checks for an existing `status='pending'` signal with the same `(property_id, type)` **and** matching `proposed_action.payload.hint.topic|subtype` — so `recurring_maintenance:heating` and `recurring_maintenance:water` on the same property remain distinct.
Reason: Matches the way rules author their output (each rule sets `action_hint.topic` or `subtype`), preserves re-fire on the next tick if a human approves/rejects/lets the signal resolve, and avoids a separate dedupe table.
Revisit if: A rule starts producing signals with the same `(property, type, topic)` but legitimately different evidence (e.g. a second-level failure after the first is resolved) — promote topic to a fact-level timestamp tiebreaker.

## 2026-04-24 — Phase 3: Gemini Pro drafter with a four-part template fallback
Context: KEYSTONE's Signal Quality Bar ("expert speaking, not database emitting rows") is load-bearing for the demo. Gemini Pro produces that quality when available; a generic fallback must not embarrass us when it isn't.
Decision: `backend/signals/drafter.py` calls Gemini Pro when `GEMINI_API_KEY` is set, otherwise picks a deterministic template keyed on `candidate.type` and fills in action-hint values. Every template follows the **observation → risk → concrete next step → deadline** structure.
Reason: Keeps the demo honest without a Gemini dependency, and the structure is what the bar actually measures.
Revisit if: Template copy drifts out of date for a new rule — add a test asserting each template mentions a deadline and a concrete action.

## 2026-04-24 — Phase 3: Entire-compatible broker via Protocol + local impl
Context: Part IV says "if their SDK isn't available in time, build the approval inbox natively with an EntireBroker interface that's easy to swap."
Decision: `backend/services/entire.py` defines a `runtime_checkable Protocol` and ships `LocalEntireBroker` that writes outbox rows. A `set_broker()` hook lets tests / the real SDK drop in without touching callers.
Reason: Honors the pitch line ("Entire-compatible approval layer") without overclaiming, and keeps the swap to one file.
Revisit if: Entire SDK lands — replace `LocalEntireBroker` with an adapter that delegates dispatch while still writing the outbox row for auditability.

## 2026-04-24 — Phase 2: Tavily enrichment writes facts directly (not via worker)
Context: Phase 2 exit criterion requires "At least one fact on each property has a visible Tavily badge." Letting the worker extract facts from a generic "web enrichment" event would produce unreliable output (the rule-based extractor might route it to `compliance.note` or nothing, and real Gemini quality varies).
Decision: `enrich_property` in `backend/services/tavily.py` inserts the event **and** two seed facts (`overview.market_snapshot`, `compliance.regulation_watch`) in the same transaction, then stamps `processed_at` so the worker skips re-extraction. Idempotent — a second call returns `None` if any Tavily event already exists for the property.
Reason: Guarantees the demo badge appears, without precluding the worker from handling more nuanced web events later. Keeps the source-of-truth audit trail (event row + sourced facts) intact.
Revisit if: Tavily hits become rich enough that parsing them with Gemini Pro would outperform the canned summary — at that point route through the worker and drop the direct insert.

## 2026-04-24 — Phase 2: Tavily offline fallback snapshot
Context: `TAVILY_API_KEY` won't be set on every dev box / CI run, and venue wifi could throttle. Part XII lists "flaky network" as a demo risk.
Decision: When the key is missing or `tavily.search` errors out, `enrich_property` falls back to a single canned "offline snapshot" fact set clearly labelled as such, rather than no enrichment at all.
Reason: The badge is a visible UI contract the demo depends on. Offline mode keeps the product surface honest (facts are labelled "offline snapshot (2026 Q1)") without lying to judges.
Revisit if: The canned copy drifts out of date or starts looking too generic — swap the wording seasonally or tie it to the property's region.

## 2026-04-24 — Phase 2: Mock ERP reads data.json on every request
Context: The demo needs an ERP data source that's easy to edit live (Part II beat 1:00). A database or fake auth scheme would add friction.
Decision: `mock_erp/main.py` re-reads `data.json` on every GET. Editing the JSON file is the canonical "ERP got a new payment" gesture during the demo.
Reason: Minimum moving parts, max demo legibility. No state syncing, no restart needed.
Revisit if: We start needing filtering/aggregation that's too slow for the naive scan — unlikely under a few hundred rows.

## 2026-04-24 — Phase 2: PDF source_ref = `{filename}:{sha256[:16]}`
Context: PDFs don't come with a Message-ID; we still need idempotency (`events.(source, source_ref)` is UNIQUE).
Decision: The `/uploads/pdf` endpoint hashes the file bytes (SHA-256) and concatenates with the filename to form `source_ref`. Reuploading the exact same PDF is a no-op; a renamed PDF is considered distinct.
Reason: Conservative — we'd rather accept a duplicate upload than miss a genuinely-new PDF with the same content. The filename prefix preserves human context in the events table.
Revisit if: We start ingesting thousands of PDFs where byte-identical duplicates should collapse even under different filenames.

## 2026-04-24 — Phase 1: Postgres as the queue, not Redis/Kafka (reinforced)
Context: Phase 1 needed a queue for events. KEYSTONE Part III already forbids Redis/Kafka.
Decision: `backend/pipeline/worker.py` uses `SELECT ... FOR UPDATE SKIP LOCKED LIMIT 1` per tick; `backend/scheduler.py` runs the worker every 2s via APScheduler. `/debug/trigger_event` also kicks a drain inline so `curl` POSTs see fresh markdown before they return.
Reason: Honors the spirit of the constitution (one system, visible SQL). SKIP LOCKED is safe under concurrency when we scale to multiple worker processes later. Inline drain on debug POSTs cuts perceived latency to ~20ms without altering the scheduled path.
Revisit if: We add a second backend instance and notice drift between the APScheduler ticks and the debug inline call (add LISTEN/NOTIFY then).

## 2026-04-24 — Skip heavy deps (google-generativeai, pdfplumber, tavily-python, imapclient) for Phase 0 local boot
Context: `pyproject.toml` lists the full dep matrix, but Phase 0 only touches FastAPI + SQLAlchemy + pydantic + structlog + psycopg2 + asyncpg.
Decision: Installed only Phase 0 runtime deps into `.venv` locally to keep the first boot fast; `pip install -e ".[dev]"` still pulls the full tree for CI / Railway / Phase 1+.
Reason: Hackathon time discipline. The pyproject is the contract; the local venv is an optimization.
Revisit if: We add tests that import the partner services, or start Phase 1 — at that point run `pip install -e ".[dev]"`.
