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

## 2026-04-24 — Skip heavy deps (google-generativeai, pdfplumber, tavily-python, imapclient) for Phase 0 local boot
Context: `pyproject.toml` lists the full dep matrix, but Phase 0 only touches FastAPI + SQLAlchemy + pydantic + structlog + psycopg2 + asyncpg.
Decision: Installed only Phase 0 runtime deps into `.venv` locally to keep the first boot fast; `pip install -e ".[dev]"` still pulls the full tree for CI / Railway / Phase 1+.
Reason: Hackathon time discipline. The pyproject is the contract; the local venv is an optimization.
Revisit if: We add tests that import the partner services, or start Phase 1 — at that point run `pip install -e ".[dev]"`.
