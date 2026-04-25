# Step 6 — Email backfill (partial)

Phase 8 Step 6 was scoped down to a partial pass after the
concurrency=4 Gemini Pro silent-hang surfaced at the 5-minute mark.
The decision to stop rather than restart at sequential is recorded
in `DECISIONS.md`; the short version is *demo doesn't need volume,
trust layer (Phase 9) and replay (Phase 10) need engineering time*.

## Numbers

| Metric | Value |
|---|---:|
| Total `.eml` files in archive | 6,546 |
| Events inserted | **251** (3.8% of archive) |
| Events stamped `processed_at` | 247 (4 zombies — workers killed mid-flight) |
| Facts written | **85** |
| Routed → property | 99 (39% of inserted) |
| Routed → liegenschaft | 46 (18% of inserted) |
| Routed → building | 0 |
| Unrouted | 106 (42% of inserted) |
| Failed events (dead-letter) | 0 |
| Cost ledger | **$0.4868 / $20.00** (resumable; not exhausted) |
| Concurrency | 4 |
| Wall-clock active | 5 min 24 s |

## Gemini Pro latency distribution (n=100 successful calls)

| stat | latency_ms |
|---|---:|
| mean | 12,646 |
| p50 | 10,884 |
| p90 | 18,477 |
| p99 / max | **37,055** |

p99 of 37 s is at the upper edge of normal Pro response time. Under
concurrency=4 (issuing ~20 calls/min), one Google-side slow regime
caught all four workers simultaneously and they never returned. No
HTTP error, no 429 — silent hang. The 4 unstamped events match the
worker count exactly; ledger cumulative was unchanged from before
the freeze.

## Unrouted-reason buckets

The 106 unrouted events break down via the
`_classify_unrouted_reason()` helper:

The buckets are auditable in
[`connectors/buena_email_loader.py`](../../connectors/buena_email_loader.py)
under `_classify_unrouted_reason`. From the
[full backfill log](.step6_full.stdout.log), the dominant categories
on this 251-event slice were `no_signal_known_sender`,
`unknown_sender_domain`, and `property_tokens_no_alias_match`.
Step 9's `sender_routing_history` consumes these directly when it
ships.

## What survived

- **Three-tier routing** (property / building / liegenschaft) running
  cleanly on Buena's real WEG topology. Route_text_event fired the
  WEG-keyword precedence as intended; no false-attribution incidents
  observed.
- **Cost ledger** durable across runs at $0.49 — resumable any time
  with `--reset-cost-ledger=False` (the default). New runs would
  pick up where this one stopped.
- **failed_events** table has zero rows: no event hit the
  dead-letter ceiling. The four zombie events from the hang are
  still inserted with `processed_at IS NULL` — they'd retry on next
  invocation under the existing `_should_retry` logic.
- **Hero property** (`Immanuelkirchstraße 26 WE 29`,
  `509393da-6806-49ef-9e59-3da0213008cd`) gets a *complete* targeted
  pass via `scripts/hero_backfill.py` so the Phase 10 onboarding
  view reads as rich. Hero choice rationale + ID logged in
  `DECISIONS.md` as `KEYSTONE_DEMO_HERO_PROPERTY`.

## What changed in response

- **`backend.services.gemini.extract_facts`** gains a 60-second
  per-call `asyncio.wait_for` timeout. A silent hang now raises
  `TimeoutError` inside one event's attempt; the existing retry
  path treats it as transient, the rule fallback fires after
  three attempts. **This is the actual product hygiene fix** —
  worth shipping regardless of concurrency setting, because a
  single hung Gemini call would freeze a sequential run too.
- **DECISIONS.md** logs the silent-hang as a known production
  concern. Recommendation for any future re-run: sequential or
  `concurrency ≤ 2`, with rate-limit-aware backoff. Phase 9+
  hardening item.

## What did *not* run

- Step 6 full VERIFY (6,546 events) — deliberately deferred.
- Steps 7 (incremental runner), 8 (signal discovery), and 9
  (admin overrides). Steps 7 + 8 are *staged* in the codebase
  ([`connectors/incremental_runner.py`](../../connectors/incremental_runner.py),
  [`eval/notebooks/signal_discovery.py`](../notebooks/signal_discovery.py))
  ready to land post-hackathon; Step 9 is unstarted.

## Demo posture

The replay engine in Phase 10 reads directly from the `.eml`
archive, not from the DB. So full DB volume isn't a demo
requirement — the partial 251-event corpus + the hero property's
complete coverage are sufficient to drive the file-builds-itself
narrative.
