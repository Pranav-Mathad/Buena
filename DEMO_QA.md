# Keystone — Demo Q&A

Answers to the five questions KEYSTONE Part XII tells us to be ready for. Each
answer leans on what's actually in the repo so you can show the file/path if a
judge presses.

---

## 1. How does it scale?

**Short answer:** Postgres is the queue, the worker is stateless, and the
critical path is one Gemini call per event.

**Where it lives in the repo:**
- `backend/pipeline/worker.py` claims work with `SELECT ... FOR UPDATE SKIP
  LOCKED LIMIT 1`. Two backend instances on the same DB are already safe —
  no Redis, no Kafka.
- `events.UNIQUE (source, source_ref)` (`backend/db/schema.sql`) keeps re-
  delivered messages idempotent across IMAP, Slack, PDF, ERP, and Tavily.
- `backend/scheduler.py` runs everything inside one APScheduler with
  `coalesce=True, max_instances=1`, so a slow tick can never pile up.

**Sketch for going wider:**
- One write replica + N read replicas of Postgres covers the dashboard / MCP
  read path; pgvector indexes scale to millions of rows.
- More workers = more uvicorn pods pointed at the same DB. SKIP LOCKED keeps
  them honest.
- LLM cost grows linearly with events; Gemini Flash on the hot path keeps the
  per-event $/latency tight, and Gemini Pro only fires when a human is about
  to read the draft (signal approval), so cost tracks engagement.

**Throughput we measured locally (Phase 6):** 10× `/debug/trigger_event` runs
average **24 ms**, max **64 ms** end-to-end against a single Postgres on
:5433. The 10-second budget in KEYSTONE is comfortable headroom.

---

## 2. What about privacy?

**Short answer:** Every fact has a source, every signal has evidence, and no
action leaves the system without human approval.

**Where it lives in the repo:**
- `facts` table tracks `source_event_id`, `confidence`, `valid_from/valid_to`,
  `superseded_by` — the full provenance chain for any displayed claim.
  See `backend/db/schema.sql` lines 79-93.
- `backend/pipeline/renderer.py` emits `[source: <event_id>]` next to every
  fact line; the UI links them. Nothing is unsourced.
- The approval inbox (`backend/api/signals.py`) is the **only** path to the
  `outbox` table. `LocalEntireBroker.dispatch` (`backend/services/entire.py`)
  writes the outbox row on approve, never on signal creation.
- MCP `propose_action` (`backend/api/signals.py::propose_signal`) tags every
  AI-authored signal with `payload.proposed_by='external_ai'` — the inbox
  shows who suggested what, and a human still has to approve.
- `.env` ships with placeholder secrets. Nothing committed.
- Aikido scan badge (`/settings/security`) makes the security posture visible
  on the Settings page.

**What we don't ship:** Auth (one demo user — Part III), real email send
(outbox table only — Part III). Those are the next-level concerns; the
audit-trail design is in place to support them.

---

## 3. What's the business model?

**Short answer:** Per-property monthly subscription with usage-based add-ons
for the partner integrations.

**Pricing shape we'd take into market:**
- **Core seat per property** — say €15/property/month — covers the living
  context layer, the pipeline (email + Slack + PDF + ERP), and the approval
  inbox.
- **Intelligence pack** — €5/property/month for the signals + portfolio
  intelligence layer. Usage-priced because Gemini Pro is what makes the
  Signal Quality Bar quality possible.
- **Enterprise / portfolio** — flat tier for portfolios over ~200 doors that
  bundles Tavily regulation watch, Aikido security badge, Pioneer learning
  layer, MCP, and SSO.

**Why this works:**
- The unit of value is "operational decisions per property per month" — easy
  to ROI against a property manager's labor cost (an hour a week per
  property at €40/h = €170/month, so €15-20 for a system that prevents one
  unnecessary truck-roll pays for itself).
- Partner integrations layer on revenue (and lock-in) without re-pricing the
  core seat.

---

## 4. How accurate is extraction?

**Short answer:** Every fact carries a confidence score, comes from a
specific source, and competes against existing facts via a documented
resolution rule. We don't claim 100%; we claim "always sourced, always
contestable".

**Where it lives in the repo:**
- `backend/services/gemini.py::EXTRACTION_SCHEMA` — Gemini Flash returns a
  structured object including a per-fact `confidence` in [0, 1]. We never
  free-text our way into facts.
- `backend/pipeline/differ.py::SOURCE_RANK` — when two sources disagree, the
  rule is `pdf > erp > email > slack > web > debug`, with a confidence-delta
  override for weaker sources (a high-confidence email can still beat a
  stale PDF if the delta is ≥0.1).
- `backend/pipeline/applier.py` writes the new fact and stamps
  `superseded_by` on the displaced one. The history is queryable; the UI
  can show "this fact replaced this one on date X".
- Phase 1 mitigation (`backend/pipeline/extractor.py`): if Gemini errors out,
  we fall back to a deterministic keyword extractor for the demo email
  shapes. Worse extraction, same provenance contract.

**Numbers we'd publish at GA:** precision/recall on a held-out set per
extraction category (`maintenance`, `lease`, `payment`, …). We have the
schema in place; producing the numbers is post-hackathon.

---

## 5. What happens when it's wrong?

**Short answer:** Three layers of containment — the human approval gate, the
fact supersession chain, and the per-rule re-fire on dedup.

**Where it lives in the repo:**
- **Approval gate.** No external action happens without `POST
  /signals/{id}/approve` (`backend/api/signals.py`). A bad signal can be
  rejected (`/reject`) or edited (`/edit`); both paths write
  `approval_log(decision)` rows so we can measure rule quality over time.
- **Fact supersession.** `backend/pipeline/applier.py` never destroys a fact;
  it stamps `superseded_by` so the wrong-but-old version is still queryable
  and replaceable. Re-process the original event after a fix and the chain
  extends cleanly.
- **Pioneer learning loop.** `backend/services/pioneer.py::compute_learning`
  reads `approval_log` and adjusts per-signal-type priority weights inside
  a `[0.5, 1.5]` band, with sample-size shrinkage so a single wrong signal
  doesn't flip a rule off. The trend line surfaces the change ("Keystone is
  deprioritizing X based on your approval behavior").
- **Idempotency.** Every event source uses a stable `source_ref`
  (`Message-ID`, `payment_id`, `filename:sha256[:16]`, Slack
  `team:channel:ts`). Replaying the world reproduces the same event ids;
  there's no double-action risk after a bug fix.
- **Demo-day fallback.** Per Part XII: Gemini outage → rule-based extractor
  fallback (`backend/pipeline/extractor.py::_rule_based`); Tavily outage →
  offline snapshot fact (`backend/services/tavily.py::_offline_snapshot`);
  IMAP outage → `POST /debug/trigger_event` (`backend/api/events.py`).

**The framing we want to leave with judges:** wrong is observable, wrong is
reversible, and wrong gets the system smarter — because the approval log is
the training signal, not the model's opinion of itself.
