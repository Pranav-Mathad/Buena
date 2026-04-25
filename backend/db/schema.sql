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

-- Phase 8 — durable LLM-spend ledger.
-- Persists across CLI invocations so a single budget cap (e.g. $20)
-- governs the entire Buena backfill, not just one run. Sub-labels
-- (e.g. 'pdf_doctype' at $2 sub-cap) share the same row family.
CREATE TABLE IF NOT EXISTS cost_ledger (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  source_label TEXT NOT NULL,           -- 'buena_email' | 'pdf_doctype' | etc.
  cumulative_usd NUMERIC(12, 4) NOT NULL DEFAULT 0,
  cap_usd NUMERIC(12, 4) NOT NULL,
  hit_at TIMESTAMPTZ,                   -- set the first time the cap is reached
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (source_label)
);
CREATE INDEX IF NOT EXISTS idx_cost_ledger_label ON cost_ledger(source_label);
