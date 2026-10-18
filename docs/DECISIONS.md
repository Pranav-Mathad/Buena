# Decisions

This is Keystone's architecture decision record (ADR) log: one entry per
non-obvious choice, newest first. Each entry gives the context that forced a
decision, what was chosen, why, and the condition under which it should be
revisited. Entries are never deleted; when a later decision replaces an
earlier one, the earlier entry gets a **Status** line pointing forward. Some
of the most useful entries record mistakes: a provider call that hung without
erroring, an audit table whose inserts had been failing silently, and a test
that nearly wiped a live spend ledger. They are kept because they explain why
the code looks the way it does.

Format: **Context**, **Decision**, **Reason**, **Revisit if**, and optionally
**Status**.

## 2026-10-17: committed data is synthetic, including eval ground truth

**Context.** The source dataset (`Extracted/`) is gitignored, but derived
material had leaked into committed files: the real street address of the
sample building, three real IBANs in connector tests and docstrings, two real
contractor names and two real email addresses in tests, a real tenant name in
frontend fixtures, and person and company names in the 30 eval emails.

**Decision.** Every committed value derived from the dataset is synthetic. The
building address is a fictional one (`Musterstraße 26, 10115 Berlin`), and
`scripts/synthesize_demo_stammdaten.py` writes it rather than copying the
source. Names in the eval set are replaced deterministically: entities that
exist in the master data get the same synthetic name `Extracted_demo` already
uses for that row (seeded by `sha256(row_id)`), everything else is seeded by
`sha256(original name)`. Test IBANs keep their last four digits so redaction
assertions are unchanged, and are assembled at runtime so no IBAN-shaped
literal is committed. Entity IDs (`EH-`, `HAUS-`, `LIE-`, `MIE-`, `EIG-`,
`DL-`) are unchanged because routing depends on them.

**Reason.** A public repository should not depend on anyone's judgement about
whether a third party's dataset is safe to publish. Deterministic replacement
keeps the eval set and `Extracted_demo` consistent with each other and makes the
substitution reproducible. The eval run reports were produced before
pseudonymisation and are kept unchanged.

**Revisit if.** New fixtures are derived from real data. A pre-commit check
for the old address and for `DE` + 20 digits outside `Extracted_demo/` guards
against regressions.

## 2026-10-17: plain PostgreSQL, no pgvector

**Context.** The schema required the `vector` extension and declared
`vector(768)` embedding columns on `events` and `facts`, the project depended
on the `pgvector` Python package, and an embedding-model setting existed.
Nothing generated, stored or queried embeddings. The dependency forced a
special Postgres image and made the schema fail on a stock server.

**Decision.** Remove the extension requirement and the columns from the base
schema, drop the columns from existing databases (migration
`0014_drop_unused_embeddings`), and remove the Python dependency and the
`GEMINI_EMBED_MODEL` setting. Compose uses `postgres:16` with a volume named
for the major version, since a data directory from one major version does not
start under another. The extension itself is not dropped from existing
databases, because that can need superuser rights and costs nothing to leave.

**Reason.** Declared-but-unused infrastructure misleads readers about what the
system does and adds operational requirements for no benefit. Semantic search
is listed as future work instead.

**Revisit if.** Semantic search over events is built; add the extension,
column and index in the same change as the code that fills and queries them.

## 2026-10-17: Ask, reply drafting and the semantic check use the configured provider

**Context.** These three features built their own HTTP requests to the Pioneer
gateway. With only a Gemini key, `POST /properties/{id}/ask` and
`POST /draft-reply` returned 503 "PIONEER_API_KEY not set", and the worker
logged a skipped semantic check for every single event.

**Decision.** All three call the configured provider's `complete_json`. Without
a provider, Ask and reply drafting return 503 with a message naming the missing
variable, and the semantic check is skipped without a log line; the LLM setup
is logged once at startup instead (`llm.provider` or
`llm.provider.not_configured`). A configured provider that fails still logs a
warning per event, because that is a real failure.

**Reason.** A feature should work with whichever provider is configured, and a
known, static condition should be reported once rather than on every event,
where it buries real warnings.

**Revisit if.** Different features need different models; give the provider
interface a per-call model override rather than reintroducing vendor-specific
code paths.

## 2026-10-17: LLM calls go through one provider interface, Gemini by default

**Context.** Two vendor clients had grown side by side. Extraction tried
Pioneer, then Gemini, then rules; onboarding briefings tried Pioneer, then
Gemini; signal drafting used only Gemini; Ask, reply drafting and the semantic
validator each built their own Pioneer HTTP requests. Which model answered
depended on which keys happened to be set, prompts and schemas were duplicated,
and every new feature had to choose a vendor.

**Decision.** `backend/services/llm/` defines an `LLMProvider` protocol
(`extract_facts`, `complete_json`, `complete_text`) with `GeminiProvider` and
`PioneerProvider`. `LLM_PROVIDER` selects one, default `gemini`; without that
provider's key, `get_provider()` returns `None` and features use their
deterministic fallback. There is no automatic fallback to the other provider.
The extraction schema, prompts and vocabulary live in one shared module;
features own their own prompts. Every call goes through one retry helper with
a per-attempt timeout. The PDF document-type classifier stays a direct,
synchronous Gemini Flash call.

**Reason.** One explicit setting makes behaviour predictable and testable: the
configured provider is the one that runs, and eval results name the backend
they measured. A shared retry helper keeps the hang protection from the
backfill incident in one place instead of six.

**Revisit if.** Cross-provider failover becomes a requirement; it should be an
explicit, logged policy in `get_provider()`, not an accident of key presence.

## 2026-10-17: web enrichment and the regulation watcher are removed

**Context.** Two features used a web-search API: enrichment of newly created
properties (a market snapshot and a regulation note written as facts) and an
hourly regulation watcher whose results raised `regulation_change` signals.
Neither had a test. Without an API key, which is the default, both wrote fixed
text: enrichment an "offline snapshot" fact on every new property, the watcher
three illustrative headlines that never appeared anywhere, which then showed up
as signals in the approval inbox.

**Decision.** Remove `backend/services/tavily.py`, the `regulation_change`
rule, the hourly job, `POST /settings/regulation_watch`,
`POST /properties/{id}/enrich`, enrichment on `POST /properties`, the
`TAVILY_API_KEY` setting, the `tavily-python` dependency, and
`seed/demo_bootstrap.py`, which existed mainly to run those steps. The renderer
keeps its generic badge for web-sourced facts, which the hand-written seed data
still contains.

**Reason.** An untested integration whose default behaviour is to present
invented content as live signals costs more trust than it adds features.
Regulation monitoring is a reasonable product idea; it should come back with a
real source, tests, and no offline fabrication.

**Revisit if.** Regulation monitoring is wanted again: ingest real feeds as
`web` events through the normal pipeline and raise signals from extracted
facts rather than from raw headlines.

## 2026-10-17: the ERP poller is opt-in

**Context.** The scheduler polled `MOCK_ERP_URL` every 30 seconds with a
default of `http://localhost:8001`. A default install has no ERP, so it logged
a connection error every 30 seconds.

**Decision.** `MOCK_ERP_URL` defaults to empty, and the `erp_poll` job is only
registered when it is set. The mock ERP service moved to `tools/mock_erp/`,
outside the packaged backend.

**Reason.** An optional integration should cost nothing when it is not
configured: no job, no outbound calls, no log noise.

**Revisit if.** A real ERP integration replaces the mock.

## 2026-10-17: a human decision on a signal holds for a cooldown period

**Context.** The evaluator deduplicated candidates only against *pending*
signals with the same `(property_id, type, subtype)`. Rules are stateless and
keep matching the same facts, so once a signal was approved or rejected, the
next 30-second run raised an identical pending signal (observed: approved at
T, duplicate pending at T+60s). The original dedupe entry (2026-04-24)
described re-raising after a decision as intended; in practice it undid every
operator decision.

**Decision.** Signals carry a `natural_key` column,
`type|<property id or 'portfolio'>|<subtype or topic>`, written by the
evaluator and backfilled for existing rows by migration
`0013_signal_natural_key`. A candidate is skipped while a signal with the same
key is pending, or was resolved (approved or rejected) within
`SIGNAL_RESOLUTION_COOLDOWN_DAYS` (default 7). After the cooldown the finding
can be raised again if the rule still matches. Signals proposed through the
API or MCP keep a NULL key and are not deduplicated by the evaluator.

**Reason.** An explicit key column makes the dedupe rule one indexed lookup and
keeps it out of JSONB path expressions. A cooldown, rather than suppressing
forever, matches how operators work: a rejected boiler warning should not
reappear tomorrow, but if the complaints continue for weeks it deserves another
look.

**Revisit if.** Operators want per-type cooldowns (lease reminders and
maintenance patterns likely want different windows) or an explicit "snooze
until" on reject. Both fit on the same key.

## 2026-10-17: the manual trigger endpoint accepts `raw_content`

**Context.** `POST /debug/trigger_event` required `body` (plus optional `from`
and `subject`) and composed `raw_content` from them. Everywhere else the event
payload is called `raw_content`: the `events` table, `ConnectorEvent`, the
eval ground truth. Posting `{"raw_content": ...}` returned a 422 about a
missing `body`, and posting both fields silently ignored `raw_content`.

**Decision.** The endpoint accepts exactly one of `raw_content` (stored
verbatim, as a connector would) or `body` (composed with `from` and `subject`
into the IMAP layout). Both or neither is a 422 that names the rule, as is
`from` or `subject` alongside `raw_content`. Documentation uses the
`raw_content` form.

**Reason.** Matching the event model removes a translation step for anyone
injecting test events, and refusing ambiguous payloads replaces a silent drop
with an error. Keeping `body` costs nothing and keeps existing scripts working.

**Revisit if.** The endpoint needs structured sources (bank rows, invoices);
it should then take a `ConnectorEvent`-shaped body directly.

## 2026-10-17: CORS origins come only from configuration

**Context.** `backend/main.py` appended five hardcoded hosts from the original
frontend builder to the configured origins, and set `allow_origin_regex` to
accept any `*.lovable.app` or `*.lovableproject.com` origin. With
`allow_credentials=True`, any site hosted on that platform could make
credentialed browser requests to the API.

**Decision.** `KEYSTONE_CORS_ORIGINS` is the only source of allowed origins,
parsed by `Settings.cors_origin_list()`. No hardcoded hosts and no regex. The
default still covers local frontends; a deployment names its frontend origin
explicitly.

**Reason.** An allow-list belongs in configuration, where a deployment can see
and change it. A wildcard combined with credentials extends trust to every
tenant of a third-party hosting platform, which is not a decision the code
should make on an operator's behalf.

**Revisit if.** The API gains token-based authentication; `allow_credentials`
could then be turned off.

## 2026-10-17: the test suite must leave its database unchanged

**Context.** After a suite run, `GET /properties` on a developer database
showed a "Test Cap Apt 1" property. The email-backfill tests inserted a
property and building and never removed them, and the eval runner test created
a real `step5_eval` cost-ledger row. Tests run against the same Postgres
developers use for the app, so every leak shows up in the product.

**Decision.** Tests that create rows own their cleanup in yield fixtures or
`finally` blocks, so cleanup also runs when an assertion fails. The backfill
tests get a fixture that creates a uniquely named property, building and
ledger label and deletes them plus every event they produced. The eval runner
test stubs the ledger, since it tests scoring, not spend. A session-scoped
fixture in `conftest.py` counts rows in 17 core tables before and after the
run and errors if any count changed.

**Reason.** A rolled-back outer transaction per test was the alternative. It
does not fit this code: the worker, loaders and cost ledger open their own
sessions or psycopg2 connections and commit, so they would escape it. Explicit
cleanup plus a guard that fails the run makes a new leak visible on the first
run that introduces it.

**Revisit if.** The application gains a single injectable session factory used
everywhere, including the synchronous psycopg2 paths; a per-test transaction
with savepoints would then make isolation automatic.

## 2026-10-17: onboarding test asserts the current degraded-briefing notice

**Context.** `test_render_onboarding_empty_property` asserted "Briefing
skipped", the notice from when Gemini was the only briefing backend. The
notice was reworded to "Briefing unavailable" when a second backend was added,
and the test was not updated, so the suite carried one known failure.

**Decision.** The test asserts the text the code emits when no LLM backend is
reachable.

**Reason.** The wording change was intentional; the test was stale, not the
code. A suite with a standing red test teaches people to ignore red.

**Revisit if.** The notice changes again when the LLM backends move behind one
provider interface.

## 2026-10-17: database tests fail loudly and get a fresh engine per test

**Context.** The end-to-end pipeline test passed on its own but skipped in a
full run. The application caches one async engine per process
(`backend.db.session.get_engine`), and its asyncpg pool binds to the event
loop that first used it. pytest-asyncio gives each test a new loop, so a later
test reused a pool attached to a closed loop and failed with "attached to a
different loop". Each DB test guarded itself with
`try: SELECT 1 / except Exception: skip`, which turned that failure into a
skip. Nine test files carried the same guard; eight happened to clear the
engine cache first and the pipeline test did not.

**Decision.** A root `conftest.py` gives every test a fresh engine (cache
cleared before, engine disposed after) and provides a `db` fixture that checks
connectivity once per session, applies the schema and migrations, and fails
with instructions when Postgres is unreachable. All nine guards are removed.
The pipeline test builds its own property, processes only its own event with
`process_specific` instead of draining the queue, and needs no seed data, so it
runs on an empty CI database.

**Reason.** A skipped integration test reads as green in a one-line summary,
which is how this one went unnoticed. Tests that need only the database must
run whenever the suite runs; tests that need the private dataset or API keys
still skip, with the reason in the message. The fixture lives at the
repository root because the backend, connectors and eval suites all use it.

**Revisit if.** The suite moves to a session-scoped event loop; one shared
engine would then be safe and cheaper than one per test.

## 2026-04-26: materialize rendered files into tables instead of rendering on every read

**Context.** The property markdown was rendered live on every
`GET /properties/{id}/markdown`. That kept it fresh but blocked two
requirements: a portfolio-wide "what changed since" feed, which needs to know
when each file last changed, and propagation, since a building-level event
must refresh every property file under that building even if nobody reads it.

**Decision.** Three additive tables, `property_files`, `building_files` and
`liegenschaft_files`, keyed on the scope id. Every fact write updates the
affected rows atomically with the write (see the same-transaction entry
below). The `/markdown` endpoints read the table first and render live only on
a cache miss. `GET /property_files/{id}` returns the full row including
`trigger_event_id` and `trigger_summary`; `GET /property_files/changes?since=`
is the polling feed.

**Reason.** Both requirements are met with three tables and one fan-out
helper. The existing render functions are reused unchanged, so materializing
stays deterministic and LLM-free: no spend, no matter how often it runs. The
render cost moves from every read to each write.

**Revisit if.** A renderer change produces materially different output and
needs a re-render sweep (bump `GENERATION_VERSION` in
`backend/pipeline/materializer.py` and re-render rows below it), or row-write
latency from cascading propagation becomes a bottleneck (batch the upsert per
tier).

## 2026-04-26: three file tables, not one polymorphic table

**Context.** A single `files` table with `(scope_type, scope_id, content_md,
...)` is the obvious alternative: one schema, one index set, one materialize
function.

**Decision.** Three tables. Each primary key is a typed foreign key to its
parent (`properties.id`, `buildings.id`, `liegenschaften.id`) with
`ON DELETE CASCADE`.

**Reason.** Cascading deletes need a typed foreign key; a polymorphic
`scope_id` would need application-level cleanup on every delete, the kind of
deferred chore that fails at the worst time. The propagation queries ("every
property under this building") read naturally against typed tables. A fourth
tier would be one new table rather than a new enum value threaded through
constraints.

**Revisit if.** The number of tiers grows past four or five, where the
duplication outweighs the cleanup code.

## 2026-04-26: fact writes and file updates share one transaction

**Context.** Propagation could run synchronously in the writer's session (a
renderer error rolls back the fact write) or asynchronously after commit
(cheaper writes, but a window where the file disagrees with the facts).

**Decision.** Synchronous. `propagate_after_fact_write(session, ...)` takes the
caller's `AsyncSession`, never opens or commits its own, and raises on renderer
failure.

**Reason.** The file is the product's source of truth; a window in which
`GET /property_files/{id}` contradicts `SELECT ... FROM facts` is exactly what
it cannot have. The costs are write latency (one render per affected scope)
and renderer bugs becoming write failures. The second proved useful: a
renderer bug that dropped `occurred_at` for building and Liegenschaft scopes
surfaced as a clean rollback the first time all three render paths ran,
instead of as a silently stale cache.

**Revisit if.** Per-scope render time exceeds roughly 500 ms. Make the
renderer faster first (it reads indexed tables); only if that fails, introduce
asynchronous propagation with a documented staleness bound.

## 2026-04-26: Liegenschaft-wide fan-out is the documented scaling boundary

**Context.** The sample dataset is one Liegenschaft, three buildings and 52
units. A Liegenschaft-level event propagates to every property under it: 52
renders in one transaction.

**Decision.** Accept the cost at current volume. Rendering one property reads
already-indexed facts and events, and the 52 render calls per Liegenschaft
fact are visible in the materializer's logs.

**Reason.** Liegenschaft-level events are a few per day against a hundred or
more unit-level events. Batching or deferring renders would add code paths
whose benefit cannot be measured at this volume.

**Revisit if.** A Liegenschaft has hundreds of units. Then batch the upsert
into one `INSERT ... ON CONFLICT` over `UNNEST(...)` and render concurrently
with `asyncio.gather`.

## 2026-04-26: the frontend change feed is a separate change

**Context.** `/property_files/changes` is meant to drive a "what changed"
notification in the UI, and the materialized `content_md` could replace the
empty-state shown for properties without extracted facts.

**Decision.** The backend shipped the data layer only: schema, materializer,
propagation and read endpoints, all verifiable with `curl`. Frontend wiring is
separate.

**Reason.** Keeping the two apart let the data layer be verified on its own,
and lets the polling cadence and UI be designed without backend deployment
constraints.

**Revisit if.** The feed stays unused. The minimal frontend change is one
query polling `/property_files/changes` and one branch rendering `content_md`
when the structured file view is empty.

**Status.** Still open; the frontend does not call the changes feed yet.

## 2026-04-25: the trust-layer override path had been failing; `approval_log` generalized

**Context.** Verifying the audit trail of `POST /admin/rejected/{id}/override`
end to end failed on the first insert. The override code wrote
`approval_log (actor, action, target_type, target_id, payload)`, but the table
had only `(signal_id, user_id, decision, edits)`. The columns it referenced did
not exist, so the override path had been broken since it shipped. No test
caught it because the tests for that change covered the validator side, not
the override side.

**Decision.** Migration `0008_approval_log_generic` adds `actor`, `action`,
`target_type`, `target_id` and `payload` as nullable columns next to the
original ones, and drops `NOT NULL` on `decision`, so signal approvals (which
still use `decision`) and trust-layer actions share the table. The override
path and the new uncertainty-resolve endpoint both write rows that round-trip.
Existing signal-approval inserts are unchanged.

**Reason.** One audit table for every human decision matches how operators
look for "who decided what, and when". Splitting into two tables, or packing
trust actions into `edits`, would make that question harder to answer.

**Revisit if.** Per-decision-type indexes or typed foreign keys on `target_id`
are needed; then use a discriminated union with `CHECK` constraints.

## 2026-04-25: source links go through a redirect, not direct file URLs

**Context.** Every `[source: <id>]` link in the rendered markdown must open the
originating document. Rendering `/files/<path>` for PDFs and
`/events/<id>/raw` for emails directly would require the renderer to know each
fact's source type, and every new source type would mean a renderer change.

**Decision.** All fact-source links point to `/events/<id>/source`, which looks
up the event and redirects (307) to the right view: `/files/<path>` for PDFs,
`/events/<id>/raw` for emails, `/events/<id>/detail` for bank rows.

**Reason.** One stable link format in rendered markdown, regardless of how
source handling evolves. The path-traversal protection stays in `/files`, so a
malicious stored path is still refused. The cost is one extra HTTP hop per
click.

**Revisit if.** Rendered markdown is served where redirects cannot be followed
(a static export); then resolve URLs at render time.

## 2026-04-25: onboarding briefings regenerate on read, keyed by mutation timestamps

**Context.** The LLM-written onboarding briefing is cached on
`properties.metadata.onboarding`. It could be regenerated on every fact write,
or lazily on the next read after something changed.

**Decision.** Lazy. The cache key is a hash of the latest fact, uncertainty and
rejection timestamps. Any mutation changes the hash; the next
`GET /admin/properties/{id}/onboarding` regenerates; later reads are free.

**Reason.** A briefing call takes tens of seconds; putting one on the write path
would add that to every event processed. Read-time regeneration pays only when
someone opens the view after something material changed, and the invalidation
is complete because every relevant mutation moves one of the three timestamps.

**Revisit if.** Stale briefings need push notification; then add a
`LISTEN/NOTIFY` channel on the cache key rather than synchronous regeneration.

## 2026-04-25: briefing language follows the property's dominant source language

**Context.** The first briefing for a German property came back in English
while the rest of its file was German, because the prompt was English-only.

**Decision.** Two prompt templates, German and English; `render_onboarding`
passes the property's detected language. The German prompt uses `Lücke:` as the
gap marker, parallel to `Gap:` in English.

**Reason.** A German property manager reading their own property's file in
English undermines the claim that the file reflects their sources. The cost is
one more template to maintain.

**Revisit if.** A third language appears, or a property's sources are evenly
bilingual (ties fall back to English).

**Status.** Superseded in practice: a later change made the briefing always
English, because the operator who reads it works in English, while the
deterministic onboarding sections still follow the property's language. The
provider refactor kept that behaviour and removed the unused German prompt.

## 2026-04-25: the IMAP transport loop stays untested offline

**Context.** The IMAP poller's ingestion path (parsing real German `.eml`,
idempotent inserts on `Message-ID`, a no-op without credentials) has four tests
against real archive files. The transport (`imapclient` connect, login,
select, search and fetch in `poll_once`) needs a live server.

**Decision.** Ship without an offline test of the transport and document the
gap.

**Reason.** `imapclient` is mature, the wrapper is about ten lines of standard
usage, and the payload it hands to `_ingest` is tested on real content. Mocking
the library's internals would add little confidence for the effort.

**Revisit if.** A real IMAP problem appears (charset edge cases, OAuth, IDLE
instead of polling). Then add a containerized IMAP server for integration
tests rather than mocking `imapclient`.

## 2026-04-25: uncertainties ship read-only first

**Context.** Uncertainty became first-class output: the extractor emits
`uncertain[]`, validator review verdicts and sub-0.7-confidence facts are
routed there, the renderer shows a "Needs review" subsection, and
`GET /admin/properties/{id}/uncertainties` lists them. The plan also included
`POST /uncertainties/{id}/resolve` (promote to fact or dismiss).

**Decision.** Ship the read path first and defer the resolve endpoint, since
the rejected-update override already demonstrated the write pattern.

**Reason.** Making uncertainty visible in the rendered file was the part that
changes what a reader trusts; the resolve endpoint was a smaller, mechanical
follow-up under a fixed time budget.

**Revisit if.** Operators start working the uncertainty inbox.

**Status.** Superseded the same day: `POST /admin/uncertainties/{id}/resolve`
shipped, writing the promoted fact and an `approval_log` row.

## 2026-04-25: full email backfill stopped after a silent hang; targeted single-property backfill instead

**Context.** The full backfill of 6,546 emails, running four concurrent
extraction workers, stopped making progress 5 minutes 24 seconds in, after
inserting 251 events and 85 facts from 100 successful LLM calls costing
$0.4868. All four workers were blocked on provider calls that never returned:
no rate-limit response, no exception, no log line. Database and ledger stayed
frozen for over ten minutes before the silence watchdog fired. Forensics are in
`docs/eval-runs/2026-04-25-step6-partial.md`.

**Decision.** Do not restart the full run sequentially. Keep what was ingested,
add a per-call timeout (next entry), and run a targeted backfill of one
property (now `python -m scripts.backfill --property <id>`) so that property's
file is complete. Volume beyond that is deferred.

**Reason.** At the observed sequential rate of about one event per 12 seconds,
the full archive would take more than six hours, displacing the validation and
onboarding work. One property with complete coverage exercises every downstream
feature; the remaining volume adds cost without new behaviour.

**Revisit if.** The full archive is needed. The run resumes from the persisted
ledger, and `(source, source_ref)` idempotency skips events already ingested.

## 2026-04-25: every extraction call has a 60-second timeout

**Context.** The hang above was not really about concurrency. The provider SDK's
blocking `generate_content` ran inside `asyncio.to_thread`; if the response
never arrived, the thread waited forever. Four workers only made it four times
more likely to happen early.

**Decision.** Wrap the call in `asyncio.wait_for(timeout=60.0)`. The observed
p99 latency in that run was 37 seconds. A timeout is treated like any other
transient error: retry, and after three attempts fall back to the rule-based
extractor. The value is `GEMINI_EXTRACT_TIMEOUT_S`.

**Reason.** Without a timeout, any deployment, sequential or concurrent, can
freeze permanently on a single bad call. With it, the worst case is a
one-minute pause and a warning log for one event.

**Revisit if.** Legitimate calls need more than 60 seconds, or timeouts should
surface as `failed_events` rows instead of falling back.

## 2026-04-25: the default property for single-property workflows

**Context.** The onboarding view and the replay engine needed one property with
rich extracted facts. Among the 99 property-routed events from the partial
backfill, the strongest candidates were GE 37 (9 events, 6 facts), WE 29 (8
events, 6 facts) and WE 50 (6 events, 7 facts).

**Decision.** `KEYSTONE_DEMO_HERO_PROPERTY = 509393da-6806-49ef-9e59-3da0213008cd`
(`Musterstraße 26 WE 29`). The targeted backfill filters on its unit-specific
aliases (`EH-029`, `WE 29`, `4. OG mitte`) and skips building-wide ones
(`HAUS-14`, the bare street), which would pull in other units' mail.

**Reason.** WE 29 had the most even spread of sections (two maintenance, one
lease, three financial facts). GE 37 is a commercial unit, which reads less
naturally as "an apartment"; WE 50 had fewer events to replay.

**Revisit if.** The targeted backfill yields fewer than 30 events, or a richer
property emerges after a full backfill.

## 2026-04-25: `cost_ledger.reset_label` cannot tell test labels from production labels

**Context.** While a live backfill was charging the label
`step6_email_backfill`, a pytest run of the connector tests called
`cost_ledger.reset_label('step6_email_backfill')` to set up its starting state.
That would have deleted the live ledger row mid-run, and the backfill's next
charge would have crashed with thousands of emails left. The test run was
killed before it reached that point.

**Decision.** Immediate fix: tests use a unique label per test. The structural
fix, making `reset_label` refuse any label that does not carry a test prefix
unless called with an explicit `allow_production=True` (behind the CLI's
existing confirmation prompt), was deferred.

**Reason.** The per-test label closed the immediate risk. The guard is a real
fix for a real class of mistake, but was not blocking at the time.

**Revisit if.** Any test touches `reset_label` with a shared label again.

**Status.** Still open; `reset_label` has no guard yet.

## 2026-04-25: concurrent backfill with a bounded cost-cap overshoot

**Context.** Sequential extraction of 6,546 emails at about three minutes per 50
would take roughly 6.5 hours. `cost_ledger.charge` is a read-modify-write, so
naive parallelism could let two workers read the same cumulative value and both
pass the cap check.

**Decision.** Four parallel workers. `cost_ledger.charge` locks the row with
`SELECT ... FOR UPDATE`, and the loader additionally serializes charges behind
one `asyncio.Lock`, which makes the contract obvious in code. When the cap is
crossed mid-flight, the overshoot is bounded by
`concurrency × max_call_cost`: with four workers and about $0.005 per call,
$0.02 against a $20 cap.

**Reason.** Roughly four times faster for a worst-case overshoot under 0.1% of
the cap. Eight workers would halve the remaining time again but double the
bound and approach provider rate limits.

**Revisit if.** Provider rate limits rise, a tighter cap is needed, or a
concurrency bug appears in the ledger or the `failed_events` upsert.

**Status.** This run is the one that hit the silent hang described above.

## 2026-04-25: lease F1 of 0.62, caused by over-firing `lease.termination_notice`

**Context.** On the German-first prompt, lease F1 was 0.62, driven by five
spurious `lease.termination_notice` facts on emails whose true category was
owner communication (sale intent, modernization consent, owners' association
governance). The model conflates "the relationship is changing" on the owner
side with "the tenancy is ending".

**Decision.** Accept 0.62, above the acceptance floor of 0.50 set for that
evaluation, and record it as a known weakness.

**Reason.** Tightening the prompt without a larger eval set risks regressing
other categories without being able to see it.

**Revisit if.** Signal rules start relying on termination notices, or
production data shows owner emails triggering tenant-change actions. The fix
is a prompt clause: emit `lease.termination_notice` only when the *tenant*
signals the end of the tenancy.

## 2026-04-25: compliance emails bleed into owner communication

**Context.** Two of the compliance emails in the eval set (fire-safety
certification, rent-cap topics) were classified as owner communication because
the prompt pulls topics touching the owners' association toward that category.
Section F1 for compliance stayed at 1.00; the miss is in category accuracy.

**Decision.** Not patched; logged.

**Reason.** Same as the lease weakness: the error is consistent (always toward
owner communication), so it is tractable once there is a larger eval set to
check a prompt change against.

**Revisit if.** A compliance-triggered signal rule is added. The fix is to list
compliance triggers (fire safety, building authority, rent cap, survey
reports) above the governance triggers in the prompt.

## 2026-04-25: extracted values are paraphrases, not source quotes

**Context.** After the calibration metric was corrected to match on
`(section, field)`, key placement in the 0.90 to 1.00 confidence bucket was 79%
accurate, but the per-section value-match rate was mostly 0.00 (highest 0.40).
The model paraphrases: "Heizung defekt, kein Warmwasser seit gestern" becomes
"Heating system malfunction reported; tenant lacks hot water since yesterday".
Right slot, different words.

**Decision.** Accept for now. Key placement is what the trust layer builds on.

**Reason.** Forcing verbatim quotes would either over-constrain the model and
cost recall, or need a post-hoc span-matching step that did not exist.

**Revisit if.** Source fidelity becomes a requirement for auditing. Add a prompt
clause asking for a verbatim substring of the source where possible, and
re-run the eval watching value-match rise (target 0.6 or better in the dense
sections).

## 2026-04-25: PDF events without extracted text say "awaiting extraction"

**Context.** Connectors wrote `[INVOICE: filename.pdf] (text not extracted)` for
PDFs whose text was deliberately not parsed during the bulk import (329 PDFs,
skipped to save cost). To a reader it looked like an error.

**Decision.** Connectors now write `Invoice <filename>, awaiting extraction`
(and the same for letters), and the renderer rewrites existing rows the same
way when `metadata.head_chars == 0`.

**Reason.** The system should state its epistemic status accurately: the
content exists and has not been processed yet. That is not a failure.

**Revisit if.** PDF text is extracted during import; the renderer then shows
the snippet automatically.

## 2026-04-25: 13 bank rows for ended tenancies stay unrouted on purpose

**Context.** After re-routing, 13 events remain unrouted with
`refs (none) kat=miete unresolved`. They are bank rows referencing `MIE-NNN`
for tenants whose lease has ended. The master-data loader deliberately skips
ended tenancies when building `occupied_by` edges, so the router cannot resolve
those tenants to a current unit.

**Decision.** Expected behaviour, not a routing bug. Documented so nobody
"fixes" it by loosening the active-tenancy rule.

**Reason.** The active-only rule protects every reader's answer to "who lives
here today". Leaving 13 historical rows unrouted is the cheapest honest
representation.

**Revisit if.** Historical attribution matters (for example, reconstructing
late payments across tenancies). Then add a separate historical-tenancy edge
the router consults after the active path misses.

## 2026-04-25: a three-tier ownership hierarchy

**Context.** With routing at the property level only, all invoices and 12.5% of
bank rows in the sample data landed unrouted. The data spans three tiers (one
Liegenschaft, three buildings, 52 units), and owners'-association-level events
had a known correct attribution that the schema could not express.

**Decision.** Add `liegenschaften`, `buildings.liegenschaft_id`,
`events.{building_id, liegenschaft_id}` and `facts.{building_id,
liegenschaft_id}` (migration `0002_liegenschaft_hierarchy`). The structured
router gains building-ID and Liegenschaft keyword and category rules. Property
markdown ends with building and Liegenschaft context sections, and buildings
and Liegenschaften get their own markdown endpoints.

**Reason.** Routing an owners'-association event to one building, or to one
unit, is the false attribution already rejected at the unit level. A junction
table would over-engineer a one-to-many relationship. The three-tier model
mirrors how German property management is actually organized and is present
in the master data directly.

**Revisit if.** A customer has several Liegenschaften; `_default_liegenschaft`
assumes one and would need a routing key.

## 2026-04-25: owners'-association keywords match case-insensitively on word boundaries

**Context.** Bank purpose text mixes `Hausgeld`, `HAUSGELD` and `hausgeld`. A
substring match over-matches inside longer words; a case-sensitive one misses
uppercase rows.

**Decision.** `re.compile(rf"\b(?:{keywords})\b", re.IGNORECASE)` over an
explicit list drawn from the data and the domain vocabulary (Hausgeld,
Verwaltergebühr, Gemeinschaftskosten, Hausverwaltung, WEG, Sonderumlage,
Instandhaltungsrücklage, Kontoführungsgebühr, plus ASCII spellings).

**Reason.** Predictable matching across casing, an auditable list of triggers,
and no NLP dependency.

**Revisit if.** A token like `WEG-` appears with an unrelated meaning (a model
number, say); then move to per-customer keyword sets.

## 2026-04-25: invoices without a unit reference route to the Liegenschaft

**Context.** Invoice files are named `YYYYMMDD_DL-NNN_INV-NNN.pdf`: contractor
and invoice number, never a unit. Filename heuristics and keyword matching
both fail, but in this dataset every invoice is an owners'-association bill.

**Decision.** The router takes an `event_source`; for `invoice` with a single
Liegenschaft present, it routes there with `method='weg_invoice'`.

**Reason.** This recognizes the data as it is. Invoices that name a building or
a unit hit the more specific rules first.

**Revisit if.** A customer has per-unit invoices without a unit reference; the
rule then needs a per-customer override. This rule is dataset-specific and is
listed as such in the architecture limitations.

## 2026-04-25: re-routing existing events is a one-time CLI step

**Context.** After the hierarchy migration, already-ingested events needed
`building_id` and `liegenschaft_id` filled in. Re-running the import skips them
(idempotent on `(source, source_ref)`).

**Decision.** `python -m connectors.cli re-route` walks events without a scope
and applies the current rules. It is idempotent, documented as one-time, and
not scheduled. New events are routed at ingestion anyway.

**Reason.** A data migration belongs in a command an operator runs on purpose,
not in the live pipeline.

**Revisit if.** Re-routing becomes recurring (for example after keyword
changes); a SQL backfill migration is then the better tool.

## 2026-04-25: the structured router does not invent property attribution

**Context.** Invoices and contractor bank payments are billed to the owners'
association, not to a unit, and carry no unit identifier.

**Decision.** `route_structured` accepts a property only when an identifier in
the event's metadata (unit ID, tenant ID, invoice reference) resolves to one.
No token-overlap fallback for structured events.

**Reason.** Spreading shared costs onto the first matching property would
corrupt every per-property financial figure. The unrouted inbox makes the gap
visible instead.

**Revisit if.** Events gain somewhere honest to land above the property level.

**Status.** Resolved by the three-tier hierarchy above; such events now route
to their building or Liegenschaft.

## 2026-04-25: each monthly rent payment supersedes the previous one

**Context.** Two years of bank data hold 12 to 24 rent payments per tenant.
Writing each as its own current fact would clutter the file and hide the
question "is this unit paid up?".

**Decision.** `extract_bank_facts` writes `financials.last_rent_payment`,
superseding the previous fact for the same `(property_id, section, field)`.
The supersession chain is the payment history. Identical values short-circuit,
so re-runs add nothing.

**Reason.** Facts represent current truth; history lives in supersession. 611
raw rent rows produced 26 current facts, one per tenant who has ever paid.

**Revisit if.** The UI needs a payment timeline; the history is already in the
chain and needs only a different read path.

## 2026-04-25: `connectors/` is a customer-agnostic ingestion layer

**Context.** The first real dataset arrived in one customer's shape; the next
will arrive in another. Treating it as a special case inside `seed/` would bake
a customer into generic code.

**Decision.** `connectors/base.py` defines the `Connector` protocol and
`ConnectorEvent`. Reusable primitives (`csv_stammdaten`, `eml_archive`,
`camt_bank`, `pdf_invoice_archive`, `pdf_letter_archive`) handle formats; one
composite per customer (`buena_archive.py`) knows the directory layout.
Redaction is centralized in `redact.py`; spend is tracked in a durable ledger.

**Reason.** A new customer adds one composite and its tests; the primitives,
redaction and ledger do not change.

**Revisit if.** A customer's format does not fit the primitives; extend a
primitive rather than fork one.

## 2026-04-25: redact at ingestion, not at render

**Context.** The renderer, API responses, MCP tools and SSE stream all read rows
back out. Redacting at render time would require every reader to remember.

**Decision.** `connectors/redact.py` replaces IBANs (`****<last4>`), full phone
numbers (country code plus last four digits) and email domains
(`<local>@example.com`) before a connector yields an event. Tests assert that
no `DE` + 20-digit IBAN and no full E.164 number survives a connector.

**Reason.** One chokepoint to audit, no burden on readers, and readers can
still scrub further without changing the contract.

**Revisit if.** A use case needs full values; keep redaction on by default and
put any exception behind an explicit flag. Note that the local part of an
email address is kept, so names embedded in addresses survive.

## 2026-04-25: the cost ledger persists across runs

**Context.** A $20 cap should govern a whole backfill, not one CLI invocation.
A per-run counter would let an operator press Ctrl-C, restart, and spend
another $20.

**Decision.** `cost_ledger(source_label, cumulative_usd, cap_usd, hit_at)` in
Postgres. Every extraction charges it; reaching the cap stamps `hit_at` and
raises `CostCapExceeded`; later runs read the row and stop immediately. Reset
requires `--reset-cost-ledger` and an interactive confirmation.

**Reason.** Spend across runs is the only definition that makes the cap real,
and the default (remember spend) is the safe one.

**Revisit if.** Multi-tenant deployments need per-customer caps; namespace
`source_label` by customer.

## 2026-04-25: property upserts keep master-data metadata

**Context.** The original upsert hardcoded `metadata = {"seed_key": ...}`,
dropping fields needed for real units (rent, floor position, floor area).

**Decision.** When `PropertySeed.metadata` is non-empty it is merged in next to
`seed_key`; tenants work the same way. Both default to `{}`, so the
hand-written seed data is unchanged.

**Reason.** One ingestion path for all master data.

**Revisit if.** Fields need their own columns; migrate them out of JSONB rather
than forking the upsert.

## 2026-04-25: tenants with an ended lease are not linked as occupants

**Context.** The tenant file has 26 rows, at least one with an end date set.
Linking ended tenancies as `occupied_by` would misstate current occupancy.

**Decision.** The loader skips tenants whose lease has ended and reports the
count (`tenants_skipped_inactive`); the 25 active tenants get `occupied_by`
edges.

**Reason.** "Who lives here today" is a load-bearing question for the file and
for signals. Ended tenancies should reappear only through lease history.

**Revisit if.** Lease history needs those tenant rows; insert them flagged as
inactive and keep them out of `occupied_by`.

## 2026-04-24: Postgres image `pgvector/pgvector:pg15`

**Context.** The planned image tag `ankane/pgvector:pg15` does not exist; that
repository uses `v`-prefixed tags, and the maintained images moved to
`pgvector/pgvector`.

**Decision.** Use `pgvector/pgvector:pg15` in `docker-compose.yml`.

**Reason.** It is the maintained upstream image and pulls cleanly; the planned
tag would have failed on `docker compose up`.

**Revisit if.** The project moves to a newer Postgres or a managed database.
Nothing reads or writes the embedding columns this extension was needed for,
so dropping the extension is the likely next step.

**Status.** Superseded on 2026-10-17 by "plain PostgreSQL, no pgvector".

## 2026-04-24: Postgres published on host port 5433

**Context.** Development machines often run a local Postgres on 5432, so
publishing the container there leaves `localhost:5432` pointing at the wrong
server and the seed fails.

**Decision.** Publish the container's 5432 on host port 5433; the DSNs in
`.env.example` match.

**Reason.** Stopping a developer's local Postgres is not the project's call. The
port is not load-bearing: every connection reads `DATABASE_URL` and
`DATABASE_URL_SYNC`.

**Revisit if.** The team standardizes on 5432 locally.

## 2026-04-24: the seed script uses psycopg2, not the async engine

**Context.** Seeding runs the schema DDL and about a hundred upserts from a
CLI. Starting an event loop for a one-shot script adds complexity for nothing.

**Decision.** `seed/seed.py` connects with `psycopg2` directly. Request paths
keep using the async engine in `backend/db/session.py`.

**Reason.** Simpler, faster, and identical semantics for a one-shot script.
`psycopg2-binary` is already a dependency.

**Revisit if.** Upsert helpers need to be shared between seed and runtime code.

## 2026-04-24: idempotent seeding by natural key

**Context.** `events` has `UNIQUE (source, source_ref)`, but owners,
buildings, contractors, properties and facts have no natural unique
constraints, and re-running the seed must be safe.

**Decision.** For those tables the seed looks up the natural key first and
inserts only if absent. Facts match on
`(property_id, section, field, source_event_id)`.

**Reason.** Safe re-runs without changing the base schema, and the seed stays
easy to read.

**Revisit if.** Seeded and pipeline-produced facts drift; the differ and
applier should then own supersession and the seed should stop acting as an
authority.

## 2026-04-24: seeded fact confidence by source

**Context.** The seed writes facts directly instead of through extraction, but
`confidence` should still mean something, since rules and the rendered file
use it.

**Decision.** Lease, tenant and financial facts from a PDF get 0.95 to 0.99;
maintenance facts from email 0.85 to 0.94; neighbourhood facts from the web
0.75 to 0.85.

**Reason.** The gradient mirrors how an extractor would score these sources.

**Revisit if.** Extractor scores calibrate differently; re-seed to match.

## 2026-04-24: two extraction paths, LLM and rule-based

**Context.** LLM extraction can be unavailable: no key, quota exhausted, no
network. The system must still process events, and local development and CI
should not need a key.

**Decision.** `backend/pipeline/extractor.py` calls the LLM when a key is
configured, and otherwise runs a deterministic keyword extractor covering
heating, water, payment, lease and compliance shapes. Both return the same
`ExtractionResult`.

**Reason.** Processing never stops on a provider problem, and the fallback makes
keyless development and testing possible. The fallback does not run when an
LLM is available, so it never replaces higher-quality output.

**Revisit if.** Calibration differences between the two paths need measuring;
the eval runner can score both.

## 2026-04-24: seeded events are stamped as already processed

**Context.** Seed events entered the queue with `processed_at IS NULL`, so the
worker re-extracted the hand-written dataset on first boot and produced
nonsense facts such as "Latest heating issue: Mietvertrag".

**Decision.** `seed/seed.py` sets `processed_at = received_at` on every seeded
event.

**Reason.** The hand-written facts are authoritative; only events arriving
after boot should be extracted. No schema change needed.

**Revisit if.** Re-extracting the seed is wanted (for example to compare against
ground truth); add an explicit flag rather than changing the default.

## 2026-04-24: the regulation watcher runs hourly and seeds fixed headlines offline

**Context.** The regulation watcher polls a web-search API for rent-regulation
news. Without an API key it has nothing to read.

**Decision.** `backend/services/tavily.watch_regulations` queries five fixed
searches hourly when `TAVILY_API_KEY` is set. Without a key it inserts three
fixed, illustrative headlines as `web` events marked `offline` in their
metadata. Events are stamped processed so the extractor skips them;
`regulation_change` reads them directly. Idempotent via
`source_ref = tavily-reg:{query}:{hour}`.

**Reason.** At the time, the goal was for a fresh install to show a working
regulation signal without a key.

**Revisit if.** This is now a liability: on a default install the inbox shows
signals about headlines that were never published. The offline mode should go,
or the job should not run without a key.

**Status.** Superseded on 2026-10-17 by "web enrichment and the regulation watcher are removed".

## 2026-04-24: the security-scan badge reports its source

**Context.** A settings page showed a "security scan: passing" badge from a
third-party scanner, which is a scheduled scanner rather than a request-time
API.

**Decision.** `backend/services/aikido.get_badge` calls the scanner's API when
keyed, and otherwise returns a `local_snapshot` badge with the current git SHA.
The response states which mode produced it.

**Reason.** "Passing" without context is unverifiable; showing the commit and
the mode at least makes the fallback explicit.

**Revisit if.** CI publishes scan results; read those instead.

**Status.** Superseded on 2026-10-17: the badge, `/settings/security`, the `/aikido.txt` verification route and the scanner key were removed. A badge that reports "passing" from a local git SHA when the scanner is not configured asserts something nothing verified; security scanning belongs in CI.

## 2026-04-24: approval-rate weighting is computed locally

**Context.** The settings page shows how operator decisions change signal
priority. An external learning service was considered but not available.

**Decision.** `backend/services/approval_stats.compute_learning` (originally
`pioneer.py`) reads `approval_log`,
computes approval rates per signal type (an edit counts as 0.8 of an approval),
and maps them to priority weights in `[0.5, 1.5]`; with fewer than five
proposals the weight is pulled toward 1.0 in proportion to the sample size. The output is a plain dataclass.

**Reason.** The numbers come from observed decisions and nothing more; no
trained model is implied.

**Revisit if.** A real ranking model is trained on the approval log.

## 2026-04-24: the MCP server is a REST adapter, not a database client

**Context.** The MCP server could share database sessions with the backend for
speed, or call the same REST API as the UI.

**Decision.** `mcp_server/tools.py` uses `httpx.AsyncClient` against the REST
API. No database imports. `KEYSTONE_BASE_URL` points it at any backend.

**Reason.** Every data rule is enforced in exactly one place, and the MCP tools
can be reasoned about like the UI.

**Revisit if.** Large results (long markdown) make the HTTP round trip a
bottleneck; add an in-process shortcut and keep REST as the default.

## 2026-04-24: property search is keyword search

**Context.** The schema has embedding columns on facts and events, but no
embedding generation was wired up.

**Decision.** `GET /properties/search` splits the query on whitespace and runs
per-term `LIKE` matching with weighted scores (name or alias above address
above fact values, boosted by hit count). The MCP tool is named
`search_properties`, not `semantic_search`.

**Reason.** Works without an LLM call per query, and the name does not promise
more than it does.

**Revisit if.** An embedding pipeline exists; the endpoint shape can stay while
the query changes to a vector search.

**Status.** The unused embedding columns were dropped on 2026-10-17; semantic
search would add them back together with the pipeline that fills them.

## 2026-04-24: externally proposed actions are always pending

**Context.** MCP clients can call tools without a human in the loop. The system
rule is that it proposes and a human approves.

**Decision.** `POST /signals/propose` inserts `status='pending'` with
`payload.proposed_by='external_ai'`. Approval still goes through
`/signals/{id}/approve` and the approval broker.

**Reason.** Keeps the human-approval invariant for AI-originated actions and
leaves an audit trail of who proposed what.

**Revisit if.** AI co-signed actions are wanted; add a separate, explicit path
instead of loosening this one.

## 2026-04-24: portfolio-level signals use `property_id = NULL`

**Context.** Cross-property patterns span a building or a cohort of buildings;
there is no single property to attach them to.

**Decision.** Store them with `property_id = NULL`. Filtering by property still
works for per-property signals; portfolio signals appear in the unfiltered
list.

**Reason.** No schema change, and no synthetic "portfolio" property distorting
per-property queries.

**Revisit if.** Signals need more than one grouping dimension; add a `scope`
column.

## 2026-04-24: deduplicate signals on `(property_id, type, payload.hint.{topic|subtype})`

**Context.** The evaluator runs every 30 seconds; without deduplication it
would insert a new pending signal for every pattern on every run.

**Decision.** Skip a candidate when a *pending* signal exists with the same
`(property_id, type)` and the same `proposed_action.payload.hint.topic` or
`subtype`, so `recurring_maintenance:heating` and
`recurring_maintenance:water` on one property stay distinct. After approval or
rejection, the next run could raise the signal again.

**Reason.** Matches how rules describe their output and avoids a separate
dedupe table.

**Revisit if.** A rule produces the same key with legitimately different
evidence.

**Status.** Superseded on 2026-10-17 by "a human decision on a signal holds for
a cooldown period": re-raising after approval or rejection undid operator
decisions on the next run.

## 2026-04-24: LLM-drafted signal messages with a structured template fallback

**Context.** A signal is only useful if its message reads like an expert's
recommendation rather than a database row. An LLM produces that when
available; the fallback must not be useless.

**Decision.** `backend/signals/drafter.py` uses the LLM when a key is
configured, and otherwise a template per signal type. Every template follows
observation, risk, concrete next step, deadline.

**Reason.** The structure is what makes a recommendation actionable, and it
holds without a key.

**Revisit if.** A new rule's template drifts; add a test that each template
names a deadline and a concrete action.

## 2026-04-24: approvals go through a broker protocol with a local implementation

**Context.** Approved actions need dispatching somewhere. An external approval
service was considered; it was not available.

**Decision.** `backend/services/entire.py` (now `approval.py`) defines a `runtime_checkable`
broker protocol and ships a local implementation that writes the outbox row.
`set_broker()` lets tests or another implementation replace it without
touching callers.

**Reason.** Callers depend on the protocol, so a real dispatcher is a one-file
change, and nothing claims an integration that does not exist.

**Revisit if.** A real dispatcher (email, Slack) is added; it should still write
the outbox row for auditability.

**Status.** 2026-10-17: renamed from `entire.py`/`EntireBroker` to
`approval.py`/`ApprovalBroker` with `LocalApprovalBroker`, and the unused
`ENTIRE_*` settings removed. The protocol and behaviour are unchanged.

## 2026-04-24: web enrichment writes its facts directly

**Context.** Enriching a new property with web context could go through the
worker as an ordinary event, but the rule-based extractor would map a generic
web summary to an unreliable field, if any.

**Decision.** `enrich_property` in `backend/services/tavily.py` inserts the
event and two facts (`overview.market_snapshot`,
`compliance.regulation_watch`) in one transaction and stamps the event
processed. A second call returns `None` if the property already has a web
event.

**Reason.** Predictable output with an intact audit trail (event row plus
sourced facts), while leaving room to route richer web content through the
worker later.

**Revisit if.** Web results are rich enough that LLM extraction would beat the
fixed summary.

**Status.** Superseded on 2026-10-17 by "web enrichment and the regulation watcher are removed".

## 2026-04-24: web enrichment falls back to a labelled offline snapshot

**Context.** `TAVILY_API_KEY` is absent on most development machines and in CI.

**Decision.** Without a key, or when the search fails, `enrich_property`
writes one fixed fact set labelled "offline snapshot".

**Reason.** At the time, a visible enrichment fact on every property was the
goal, and labelling avoided presenting it as live data.

**Revisit if.** The fixed text is mistaken for real data; it is listed as a
known limitation in the architecture document.

**Status.** Superseded on 2026-10-17 by "web enrichment and the regulation watcher are removed".

## 2026-04-24: the mock ERP re-reads its JSON file on every request

**Context.** A stand-in ERP data source should be easy to change by hand. A
database or fake authentication would add friction.

**Decision.** `tools/mock_erp/main.py` re-reads `data.json` on every request;
editing the file is how a new payment "arrives".

**Reason.** Minimum moving parts, no state to sync, no restart needed.

**Revisit if.** Filtering becomes too slow for a naive scan, which is unlikely
below a few hundred rows.

## 2026-04-24: uploaded PDFs are keyed by filename and content hash

**Context.** PDFs have no Message-ID, but `events (source, source_ref)` must
still be unique.

**Decision.** `POST /uploads/pdf` uses `{filename}:{sha256(bytes)[:16]}` as
`source_ref`. Re-uploading the same file is a no-op; the same content under a
new name is a distinct event.

**Reason.** Accepting an occasional duplicate is better than dropping a new
document that happens to match. The filename keeps the events table readable.

**Revisit if.** Thousands of PDFs arrive and byte-identical duplicates under
different names should collapse.

## 2026-04-24: Postgres is the queue

**Context.** Events need a work queue.

**Decision.** `backend/pipeline/worker.py` claims work with
`SELECT ... FOR UPDATE SKIP LOCKED LIMIT 1`; the scheduler runs the worker
every 2 seconds. `/debug/trigger_event` processes its event inline so a caller
sees the result in the response.

**Reason.** One system to operate and inspect, with plain SQL visibility into
the queue. `SKIP LOCKED` stays correct with several worker processes.

**Revisit if.** Several backend instances run; the worker is safe, but the
scheduled jobs are not (see the architecture limitations), and
`LISTEN/NOTIFY` would cut idle polling.

## 2026-04-24: install only the core dependencies for the first local boot

**Context.** `pyproject.toml` lists the full dependency set, but the first
milestone needed only FastAPI, SQLAlchemy, pydantic, structlog, psycopg2 and
asyncpg.

**Decision.** Install just those into the first local virtualenv;
`pip install -e ".[dev]"` remains the full install.

**Reason.** A faster first boot. `pyproject.toml` is the contract; the local
environment is an optimization.

**Revisit if.** Tests import the optional integrations; install the full set.
