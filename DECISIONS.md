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
