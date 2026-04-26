"""Additive migrations for Phase 8+ tables.

Phase 0's ``backend/db/schema.sql`` is the canonical schema for the
demo data model (Part VI of KEYSTONE). Everything Phase 8 introduces
is **additive** — new tables created with ``CREATE TABLE IF NOT
EXISTS`` so re-running is idempotent and existing demo databases
upgrade in place without a destructive reset.

Run :func:`apply_all` (sync) at CLI startup. Tests do the same against
the dev Postgres on :5433.
"""

from __future__ import annotations

import structlog

import psycopg2

from backend.config import get_settings

log = structlog.get_logger(__name__)


# Each migration is (name, sql). Names are descriptive only — the
# CREATE TABLE / ALTER TABLE / CREATE INDEX IF NOT EXISTS shape is what
# makes them safe to replay. New migrations must be append-only;
# never edit a prior tuple in place.
_MIGRATIONS: list[tuple[str, str]] = [
    (
        "0001_cost_ledger",
        """
        CREATE TABLE IF NOT EXISTS cost_ledger (
          id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
          source_label TEXT NOT NULL,
          cumulative_usd NUMERIC(12, 4) NOT NULL DEFAULT 0,
          cap_usd NUMERIC(12, 4) NOT NULL,
          hit_at TIMESTAMPTZ,
          updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          UNIQUE (source_label)
        );
        CREATE INDEX IF NOT EXISTS idx_cost_ledger_label ON cost_ledger(source_label);
        """,
    ),
    (
        "0002_liegenschaft_hierarchy",
        """
        -- WEG / Liegenschaft is the legal owner-association entity above
        -- buildings. One Liegenschaft can own multiple Häuser; events
        -- billed at WEG level (Hausgeld / Verwaltergebühr / shared
        -- contractor fees) attach here, not to a specific Haus.
        CREATE TABLE IF NOT EXISTS liegenschaften (
          id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
          name TEXT NOT NULL,
          buena_liegenschaft_id TEXT UNIQUE,
          metadata JSONB DEFAULT '{}'::jsonb,
          created_at TIMESTAMPTZ DEFAULT now()
        );

        ALTER TABLE buildings
          ADD COLUMN IF NOT EXISTS liegenschaft_id UUID
          REFERENCES liegenschaften(id);

        ALTER TABLE events
          ADD COLUMN IF NOT EXISTS building_id UUID
          REFERENCES buildings(id);
        ALTER TABLE events
          ADD COLUMN IF NOT EXISTS liegenschaft_id UUID
          REFERENCES liegenschaften(id);

        ALTER TABLE facts
          ADD COLUMN IF NOT EXISTS building_id UUID
          REFERENCES buildings(id);
        ALTER TABLE facts
          ADD COLUMN IF NOT EXISTS liegenschaft_id UUID
          REFERENCES liegenschaften(id);

        CREATE INDEX IF NOT EXISTS idx_events_building
          ON events (building_id, received_at DESC);
        CREATE INDEX IF NOT EXISTS idx_events_liegenschaft
          ON events (liegenschaft_id, received_at DESC);
        CREATE INDEX IF NOT EXISTS idx_facts_building_current
          ON facts (building_id, section, field) WHERE superseded_by IS NULL;
        CREATE INDEX IF NOT EXISTS idx_facts_liegenschaft_current
          ON facts (liegenschaft_id, section, field) WHERE superseded_by IS NULL;
        """,
    ),
    (
        "0003_failed_events",
        """
        -- Backfill dead-letter table. ``retry_count`` increments on each
        -- failed extraction attempt; once it hits the configured ceiling
        -- (default 3) the event lands here for human triage via
        -- /admin/failed in Step 9. ``last_error`` keeps the most recent
        -- traceback excerpt so the operator can scan without joining.
        CREATE TABLE IF NOT EXISTS failed_events (
          id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
          event_id UUID NOT NULL REFERENCES events(id) ON DELETE CASCADE,
          retry_count INTEGER NOT NULL DEFAULT 0,
          last_error TEXT,
          last_attempted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          UNIQUE (event_id)
        );
        CREATE INDEX IF NOT EXISTS idx_failed_events_retry
          ON failed_events (retry_count, last_attempted_at DESC);
        """,
    ),
    (
        "0004_system_state",
        """
        -- Generic per-process / per-feature key-value store. Phase 8 Step 7
        -- uses ``system_state['buena_day_cursor']`` to persist the
        -- incremental-feed cursor across server restarts; future features
        -- (e.g. Phase 10 replay state) can reuse the same primitive without
        -- a new migration.
        CREATE TABLE IF NOT EXISTS system_state (
          key TEXT PRIMARY KEY,
          value JSONB NOT NULL DEFAULT '{}'::jsonb,
          updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        """,
    ),
    (
        "0005_rejected_updates",
        """
        -- Phase 9 Step 9.2 — constraint validator drop site.
        -- Each row records a fact-update proposal that the validator
        -- rejected (or marked needs_review) so the admin /rejected
        -- inbox can surface it for human triage. ``constraint_name``
        -- is the class name of the firing constraint; ``reason`` is
        -- the operator-facing message; ``required_source_type``
        -- captures what document_type / source the constraint
        -- expected (e.g. ``kaufvertrag``, ``lease_addendum``).
        --
        -- ``reviewed_status``:
        --   pending      — default, not yet looked at
        --   needs_review — constraint marked the proposal as soft-
        --                  reject (Phase 9.1 will promote these to
        --                  uncertainty_events when that step lands)
        --   overridden   — operator explicitly approved + an
        --                  approval_log row was written
        --   dismissed    — operator explicitly rejected
        CREATE TABLE IF NOT EXISTS rejected_updates (
          id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
          event_id UUID NOT NULL REFERENCES events(id) ON DELETE CASCADE,
          property_id UUID REFERENCES properties(id) ON DELETE SET NULL,
          building_id UUID REFERENCES buildings(id) ON DELETE SET NULL,
          liegenschaft_id UUID REFERENCES liegenschaften(id) ON DELETE SET NULL,
          proposed_section TEXT NOT NULL,
          proposed_field TEXT NOT NULL,
          proposed_value TEXT NOT NULL,
          proposed_confidence NUMERIC(5, 4),
          constraint_name TEXT NOT NULL,
          reason TEXT NOT NULL,
          required_source_type TEXT,
          reviewed_status TEXT NOT NULL DEFAULT 'pending',
          created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          reviewed_at TIMESTAMPTZ,
          reviewed_by TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_rejected_updates_property
          ON rejected_updates (property_id, created_at DESC)
          WHERE reviewed_status = 'pending';
        CREATE INDEX IF NOT EXISTS idx_rejected_updates_pending_status
          ON rejected_updates (reviewed_status, created_at DESC);
        """,
    ),
    (
        "0006_uncertainty_events",
        """
        -- Phase 9 Step 9.1 — uncertainty as first-class output.
        -- The extractor and the validator both produce things the
        -- system "noticed but didn't want to commit to":
        --   * Gemini emits items in its uncertain[] schema field.
        --   * Confidence floor < 0.7 demotes a fact to uncertainty.
        --   * Validator needs_review verdicts (e.g. rent change with
        --     a lease_addendum PDF) — soft-rejects that ask for
        --     human confirmation rather than blocking outright.
        -- All three land here. ``status`` tracks the review lifecycle.
        --
        -- ``observation`` — what the system saw (the verbatim or
        --     near-verbatim phrasing that triggered the uncertainty).
        -- ``hypothesis``  — the candidate value if there is one
        --     (NULL when the model just flagged something noticed).
        -- ``reason_uncertain`` — operator-facing why-this-is-flagged.
        -- ``source`` — where the uncertainty came from:
        --     gemini | confidence_floor | validator_needs_review
        CREATE TABLE IF NOT EXISTS uncertainty_events (
          id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
          event_id UUID NOT NULL REFERENCES events(id) ON DELETE CASCADE,
          property_id UUID REFERENCES properties(id) ON DELETE SET NULL,
          building_id UUID REFERENCES buildings(id) ON DELETE SET NULL,
          liegenschaft_id UUID REFERENCES liegenschaften(id) ON DELETE SET NULL,
          relevant_section TEXT,
          relevant_field TEXT,
          observation TEXT NOT NULL,
          hypothesis TEXT,
          reason_uncertain TEXT NOT NULL,
          source TEXT NOT NULL,
          status TEXT NOT NULL DEFAULT 'open',
          resolved_to_fact_id UUID REFERENCES facts(id) ON DELETE SET NULL,
          created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          resolved_at TIMESTAMPTZ
        );
        CREATE INDEX IF NOT EXISTS idx_uncertainty_events_property_open
          ON uncertainty_events (property_id, created_at DESC)
          WHERE status = 'open';
        CREATE INDEX IF NOT EXISTS idx_uncertainty_events_section
          ON uncertainty_events (property_id, relevant_section, created_at DESC)
          WHERE status = 'open';
        """,
    ),
    (
        "0007_replay_runs",
        """
        -- Phase 10 Step 10.2 — sequential replay state.
        -- One row per ``POST /admin/replay/start`` invocation. Tracks
        -- progress + control state so the SSE client can render the
        -- "n / total events streamed" UI without re-querying events,
        -- and so pause / resume / stop are durable across reconnects.
        --
        -- ``status`` lifecycle:
        --   running   — actively streaming
        --   paused    — stopped on a scheduled_pauses checkpoint or
        --               an operator pause
        --   completed — drained the configured event window
        --   stopped   — operator-cancelled mid-flight
        --   failed    — engine raised; ``last_error`` is populated
        CREATE TABLE IF NOT EXISTS replay_runs (
          id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
          property_id UUID NOT NULL REFERENCES properties(id) ON DELETE CASCADE,
          speed_multiplier INTEGER NOT NULL DEFAULT 10,
          source_filter TEXT[] NOT NULL DEFAULT ARRAY['email','invoice','bank']::TEXT[],
          start_date TIMESTAMPTZ,
          end_date TIMESTAMPTZ,
          scheduled_pauses JSONB NOT NULL DEFAULT '[]'::jsonb,
          reset_property BOOLEAN NOT NULL DEFAULT FALSE,
          status TEXT NOT NULL DEFAULT 'running',
          total_events INTEGER NOT NULL DEFAULT 0,
          processed_events INTEGER NOT NULL DEFAULT 0,
          last_event_id UUID REFERENCES events(id) ON DELETE SET NULL,
          last_error TEXT,
          started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          paused_at TIMESTAMPTZ,
          completed_at TIMESTAMPTZ
        );
        CREATE INDEX IF NOT EXISTS idx_replay_runs_property_status
          ON replay_runs (property_id, started_at DESC);
        """,
    ),
    (
        "0008_approval_log_generic",
        """
        -- Phase 10 Step 10.6 — extend approval_log so trust-layer
        -- actions (uncertainty resolve, rejection override) can write
        -- audit rows alongside the original signal-centric format.
        --
        -- The existing columns (signal_id, user_id, decision, edits)
        -- stay untouched so Phase 5 signal approvals keep working.
        -- New columns are NULL-by-default so old INSERTs don't break.
        ALTER TABLE approval_log
          ADD COLUMN IF NOT EXISTS actor TEXT,
          ADD COLUMN IF NOT EXISTS action TEXT,
          ADD COLUMN IF NOT EXISTS target_type TEXT,
          ADD COLUMN IF NOT EXISTS target_id TEXT,
          ADD COLUMN IF NOT EXISTS payload JSONB;
        -- Old schema required ``decision`` for signal approvals; trust-
        -- layer actions track ``action`` instead, so we drop the NOT NULL
        -- constraint. Existing signal-approval INSERTs still write it.
        ALTER TABLE approval_log
          ALTER COLUMN decision DROP NOT NULL;
        CREATE INDEX IF NOT EXISTS idx_approval_log_target
          ON approval_log (target_type, target_id, created_at DESC)
          WHERE target_id IS NOT NULL;
        """,
    ),
    (
        "0009_facts_human_edited",
        """
        -- Phase 11 — preserve human edits to facts. The brief calls
        -- this out: "surgically updated without destroying human
        -- edits." Re-extraction respects ``human_edited=TRUE`` and
        -- skips writing a new value over the operator's correction.
        -- The override path (``POST /facts/{id}/edit?force=true``) can
        -- still flip the flag back when needed.
        ALTER TABLE facts
          ADD COLUMN IF NOT EXISTS human_edited BOOLEAN NOT NULL DEFAULT FALSE,
          ADD COLUMN IF NOT EXISTS edited_by TEXT,
          ADD COLUMN IF NOT EXISTS edited_at TIMESTAMPTZ;
        CREATE INDEX IF NOT EXISTS idx_facts_human_edited
          ON facts (property_id, section, field)
          WHERE human_edited = TRUE AND superseded_by IS NULL;
        """,
    ),
    (
        "0010_property_markdown_overrides",
        """
        -- Phase 11 — operator-edited property markdown. The renderer
        -- still composes the canonical view on every request, but if
        -- a row exists here for a property the API serves the
        -- override verbatim. Switching back to auto-generated is a
        -- single DELETE on this table — no destructive ops.
        CREATE TABLE IF NOT EXISTS property_markdown_overrides (
          property_id UUID PRIMARY KEY
            REFERENCES properties(id) ON DELETE CASCADE,
          markdown_text TEXT NOT NULL,
          edited_by TEXT,
          edited_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        """,
    ),
]


def apply_all(connection_url: str | None = None) -> int:
    """Run every additive migration on the configured Postgres.

    Returns:
        The number of migrations actually executed (in our scheme that's
        always ``len(_MIGRATIONS)`` — Postgres absorbs the no-ops via
        ``IF NOT EXISTS``).
    """
    url = connection_url or get_settings().database_url_sync
    log.debug("connectors.migrations.apply", count=len(_MIGRATIONS))
    with psycopg2.connect(url) as conn:
        conn.autocommit = False
        with conn.cursor() as cur:
            for name, sql in _MIGRATIONS:
                cur.execute(sql)
                log.info("connectors.migrations.applied", name=name)
        conn.commit()
    return len(_MIGRATIONS)
