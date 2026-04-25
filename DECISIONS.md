# DECISIONS

Running log of non-obvious judgment calls. Format per KEYSTONE.md Part XIII.

## 2026-04-25 — Phase 10 Step 10.6: latent ``approval_log`` schema mismatch
Context: Verifying the existing ``POST /admin/rejected/{id}/override`` audit trail end-to-end (per the user's "you have this — verify it works" review note) failed at the first INSERT. Phase 9.2's override endpoint writes ``approval_log (actor, action, target_type, target_id, payload)`` — but Phase 5's original schema is ``(signal_id, user_id, decision, edits)``. The columns the Phase 9.2 INSERT references simply don't exist; the override path has been broken since 9.2 shipped, and no test exercised it before now because Phase 9.2's tests focused on the validator side, not the override side.
Decision: Migration 0008_approval_log_generic adds (actor, action, target_type, target_id, payload) as nullable columns alongside the original ones, plus drops NOT NULL on ``decision`` so signal-approvals (which still use it) and trust-layer actions can share the table cleanly. New endpoint (``POST /uncertainties/{id}/resolve``) and the existing override path now both write rows that round-trip correctly. Backward compatible — existing signal-approval INSERTs are untouched.
Reason: The shared-table approach beats either splitting tables (``trust_audit_log`` vs ``approval_log``) or rewriting the existing inserts to cram everything into ``edits``. One audit table for all human decisions matches the operator mental model — they're going to look in *one place* to find "who decided what and when."
Revisit if: We start needing per-decision-type indexes that don't fit the generic shape, or want decision-type FK constraints (e.g., ``target_id`` strictly typed as UUID per ``target_type``). At that point, switch to a discriminated union with ``CHECK`` constraints.

## 2026-04-25 — Phase 10 Step 10.4: source links route through a redirect, not direct file URLs
Context: Source links in the rendered markdown (every ``[source: <id>]`` bullet) need to actually open the originating document. Naive choice: render ``/files/<original_path>`` directly for PDFs and ``/events/<id>/raw`` for emails. Problem: the renderer doesn't know the source type per fact unless we widen ``FactRow`` to carry it everywhere, and even then, future source types (Slack pulls, ERP detail pages, web archives) would force renderer changes.
Decision: All fact-source links go to ``/events/<id>/source``, a redirect dispatcher that looks up the event and 307s to the right surface (``/files/<path>`` for PDFs, ``/events/<id>/raw`` for emails, ``/events/<id>/detail`` for bank). The renderer never has to know which is which.
Reason: One stable link string in rendered markdown across source-type evolutions. The redirect indirection is also where path-traversal protection lives — if a future source captured a malicious ``original_path``, the ``/files`` route still refuses it. Cost: one extra HTTP hop per click. Benefit: the markdown stays valid forever even as source-type handling changes.
Revisit if: We start serving rendered markdown to a context where the redirect can't follow (e.g., a static export). At that point, render the resolved URL at write time and accept the renderer-knows-source coupling.

## 2026-04-25 — Phase 10 Step 10.3: read-time briefing regen, no write-side hooks
Context: Plan called for caching the Gemini Pro briefing on ``properties.metadata.onboarding`` and regenerating on fact insertion or uncertainty resolution. Two implementation choices: hook into the worker's apply-time write path (regen runs synchronously after every fact write), or hash the latest mutation timestamps + regen lazily on the next read.
Decision: Hash-based read-time invalidation. Cache key = sha1(last_fact_at, last_uncertainty_at, last_rejection_at). Any mutation flips the hash; the next ``GET /admin/properties/{id}/onboarding`` regenerates; subsequent reads are free until the next mutation.
Reason: Write-path Gemini calls would push 24-second p50 onboarding latency onto every event ingestion — that's a ten-thousand-times worse trade than the alternative. Read-time regen pays the cost only when an operator actually opens the view, and only after something material changed. The cache invalidation is provably correct (any of the three mutation kinds bumps the hash).
Revisit if: We start sending push-style notifications when an onboarding briefing has gone stale (then we'd want to know *at write time* that an open view needs a refresh). At that point, add a ``LISTEN/NOTIFY`` channel on the cache key, not a synchronous regen.

## 2026-04-25 — Phase 10 Step 10.3: briefing language matches property's source-event majority
Context: First live render against hero (WE 29 — German property) emitted a perfect 7-bullet briefing in *English* while the rest of the rendered markdown was German. The Gemini Pro prompt was English-only.
Decision: Two prompt templates (``ONBOARDING_PROMPT_DE`` and ``ONBOARDING_PROMPT_EN``); ``backend.services.onboarding.render_onboarding`` passes the property's detected language through. The German prompt uses ``Lücke:`` as the explicit gap-marker prefix (parallel to the English ``Gap:``).
Reason: A German property manager reading their own property's file in English breaks the demo's whole "this is your file" trust narrative. Cost: one extra prompt template to maintain. Benefit: native-language rendering for Buena's primary cohort, which is most of the demo's substance.
Revisit if: We add a third locale or someone reports a property whose source-event mix is bilingual. The detector falls back to English on ties — that's a deliberate "least surprise" choice but worth re-checking against real usage.

## 2026-04-25 — Phase 10 IMAP: live transport remains untested offline
Context: User asked to verify live IMAP polling on real Buena-style emails before Phase 10 closes. The poller's ingestion path (parsing real German .eml, writing events idempotently on Message-ID, no-op safety when creds are missing) is fully tested with 4 tests against real ``Extracted/emails/`` files. The actual IMAP transport (``imapclient.IMAPClient`` connect/login/select/search/fetch loop in ``poll_once``) requires a live IMAP server.
Decision: Ship Phase 10 without an offline test of the transport. Document the gap.
Reason: ``imapclient`` is a mature library; the wrapper is ~10 lines of standard usage; the parsed payload it hands to ``_ingest`` *is* tested on real Buena .eml content. Spending the time to mock-IMAP-server everything would push other higher-impact work (operator action endpoints, document linking) without changing risk meaningfully.
Revisit if: We hit a real IMAP issue in production (charset edge case, OAuth instead of password, IDLE-mode push instead of polling). At that point, set up a containerised dovecot for integration tests rather than mocking imapclient internals.

## 2026-04-25 — Phase 9 Step 9.1: defer ``POST /uncertainties/{id}/resolve`` to post-hackathon
Context: Step 9.1 ships uncertainty as first-class output — extractor emits ``uncertain[]``, validator's ``needs_review`` verdicts route here, sub-floor confidence (< 0.7) facts demote here, renderer surfaces a "Needs Review" / "Zu prüfen" subsection per section, and ``GET /admin/properties/{id}/uncertainties`` lists everything. The original plan also called for ``POST /uncertainties/{id}/resolve`` (promote to fact, dismiss with reason).
Decision: Ship the read-only path only. ``POST /resolve`` is **deferred** to post-hackathon. The validator's hard-reject path already has ``POST /admin/rejected/{id}/override`` from Step 9.2, so the override surface is demonstrable; the uncertainty surface is purely read-only for now.
Reason: Hackathon time discipline. The demo's value is in *seeing* the system render an honest "I noticed something I can't commit to" subsection — that's the trust-layer narrative. Judges won't click through to resolve uncertainties; they'll read the rendered markdown and move on. Building the resolution surface costs ~1 hour (write fact + audit log + state transitions + test) for zero demo lift. Phase 10 work has higher marginal return on the same hour.
Revisit if: Real users start consuming the uncertainty inbox in production, or the demo script grows a beat where a judge does click "resolve" on an uncertainty. The pattern from ``POST /rejected/{id}/override`` is directly transferable — the resolution endpoint is essentially the same shape.

## 2026-04-25 — Phase 8 Step 6: stop, pivot to hero-property backfill, defer Steps 7-9
Context: Step 6's full 6,546-email backfill silent-hung at minute 5:24 with `concurrency=4` after 251 events / 85 facts / 100 successful Gemini Pro calls / $0.4868 spent. All 4 worker tasks ended up blocked on simultaneous Pro calls that never returned (no 429, no exception, no log). DB state and cost ledger were both frozen at the moment of hang for >10 minutes before the silence-watchdog tripped and the kill ladder fired. Forensics in `eval/runs/2026-04-25-step6-partial.md`.
Decision: Do *not* restart at sequential. Take what we have — 251 events, 85 facts, three-tier routing working on real WEG topology, eval framework with measured 73 % category accuracy — as Phase 8 substance. Run a *targeted* hero-property backfill (`scripts/hero_backfill.py`) so the demo's onboarding view reads as rich. Skip Phase 8 Steps 7, 8, 9 entirely (Step 7+8 stayed staged on disk for post-hackathon, Step 9 is unstarted). Tag `phase-8-complete` and immediately start `phase-9/trust-layer` Step 9.2 (constraint validator).
Reason: Hackathon time discipline. Spending the next 6+ hours getting the rest of the 6,546 emails through (sequential rate is ~1 event / 12 s) displaces Phase 9 (trust layer) and Phase 10 (replay + onboarding) — both PM-prioritised. Volume isn't a demo requirement; the replay engine in Phase 10 reads directly from the `.eml` archive, not the DB. The hero property's *complete* coverage is what the demo actually needs.
Revisit if: Post-hackathon. The full backfill resumes cleanly from the existing $0.4868 / $20 ledger (or with `--reset-cost-ledger`); source-ref idempotency means already-processed events are skipped. Sequential re-run: ~6.5 h, ~$9.30 additional. Or address the silent-hang properly first — see the next DECISIONS entry.

## 2026-04-25 — Phase 8 Step 6: 60-second timeout on `extract_facts` (silent-hang mitigation)
Context: The concurrency=4 hang above wasn't really *about* concurrency. A single Gemini Pro call can silent-hang too — the Google SDK's `generate_content` is a blocking call wrapped in `asyncio.to_thread`, and if the server-side response never arrives, that thread waits forever. Concurrency=4 just made the failure surface 4× faster.
Decision: Wrap the call site in `asyncio.wait_for(timeout=60.0)`. p99 latency observed in Step 6 was 37 s, so 60 s is comfortable headroom. On timeout the existing retry/exception path kicks in, and after `max_attempts=3` the rule fallback fires. Default is exposed as `GEMINI_EXTRACT_TIMEOUT_S` so future tuning is one-line.
Reason: Genuine product hygiene worth shipping regardless of phase. Without the timeout, *any* future production rollout — sequential, concurrent, scheduled, on-demand — has a "permanent freeze on a single bad call" failure mode. With it, the worst case is a 60 s pause on one event's attempt; the retry handles it; the operator sees one warning log and the pipeline keeps moving.
Revisit if: We see legitimate Gemini calls that need > 60 s (raise the ceiling per-call) or want to surface timeouts as `failed_events` rows for the admin inbox in Step 9 (currently the rule fallback runs and the event leaves the queue cleanly).

## 2026-04-25 — Phase 8 Step 6: hero property = WE 29 (`509393da-6806-49ef-9e59-3da0213008cd`)
Context: Demo's onboarding view (Phase 10 Step 10.3) needs one property with rich extracted facts so the file-builds-itself beat reads as substantive. Among the 99 property-routed events from the partial Step 6 pass, top three candidates were GE 37 (9 events / 6 facts), WE 29 (8 events / 6 facts), and WE 50 (6 events / 7 facts).
Decision: **`KEYSTONE_DEMO_HERO_PROPERTY = 509393da-6806-49ef-9e59-3da0213008cd`** (`Immanuelkirchstraße 26 WE 29`). Most balanced cross-section of facts: maint=2 / lease=1 / fin=3, all sections populated. `scripts/hero_backfill.py` runs the targeted re-extraction filtered by the property's specific aliases (`EH-029`, `WE 29`, `4. OG mitte` — drops `HAUS-14` and the bare street prefix because they're building-wide, not unit-specific).
Reason: WE 29 has the cleanest narrative arc. GE 37 has higher event count but it's a `Gewerbe` (commercial unit) — most demo viewers will read "apartment" and connect more directly to a `WE` (Wohneinheit). WE 50 has highest fact density but only 6 events — less to visibly stream during the replay beat. WE 29 is the median story.
Revisit if: The hero backfill produces < 30 events (cut-off where the onboarding "60-second briefing" looks thin), in which case fall back to GE 37 with a one-line "kommerziell" framing in the narrative. Or if a richer property surfaces post-hackathon with the full backfill complete.

## 2026-04-25 — Phase 8 Step 6: cost-ledger reset_label has no test-vs-prod separation (Phase 9+ hardening)
Context: While the live Buena backfill was running with label `step6_email_backfill`, I started `pytest connectors/tests/` to sanity-check Step 7+8 drafts. The cost-cap test calls `cost_ledger.reset_label('step6_email_backfill')` to set up its starting state — which would have **deleted the live ledger row** mid-run, causing the backfill's next `cost_ledger.charge` to raise `RuntimeError("cost_ledger has no row for label=…")` and crash with several thousand more emails to process. Caught and killed pytest before damage. Today's quick fix: tests monkeypatch the label to a per-test unique `step6_test_<uuid>`, so concurrent runs are now isolated.
Decision: Quick fix landed today. Defer the structural fix — making `cost_ledger.reset_label()` refuse any label whose name doesn't start with a recognised test prefix (`test_*`, `_test_*`, etc.) at runtime — to Phase 9 hardening. Production labels (`step5_eval`, `step6_email_backfill`, `step7_incremental`, `pdf_doctype`) would be opt-in via `reset_label(..., allow_production=True)` with the friction-gated `y/N` prompt that already lives in the CLI.
Reason: Today's test isolation patch closes the immediate landmine. The structural guard is a real bug (any future test author can repeat the same near-miss) but isn't blocking Phase 8. Putting the guard in Phase 9 lets us land it next to the trust-layer / admin-overrides surface where the same "production data needs friction" theme is being designed.
Revisit if: Anyone — me, future contributors, or CI — touches `reset_label()` from a test file again. Or if Phase 9 ships before this guard does (move it forward).

## 2026-04-25 — Phase 8 Step 6: backfill concurrency=4 with bounded cost-cap overshoot
Context: The Buena email archive is 6,546 .eml files. Sequential extraction at ~3 minutes per 50 emails meant ~6.5 hours for the full pass. Concurrent extraction is the obvious speedup, but `cost_ledger.charge` is a read-modify-write — naive parallelism could double-spend by reading the same `cumulative_usd` from two workers and both passing the cap check before either UPDATE lands.
Decision: Run 4 parallel extraction workers. Serialize `cost_ledger.charge` behind a single `asyncio.Lock` so the read-modify-write is atomic at the application level (Postgres `FOR UPDATE` already serializes at the DB level — the asyncio lock is belt-and-suspenders, and makes the contract obvious in the loader). Worst-case overshoot when the cap is breached mid-flight is bounded by `concurrency × max_call_cost`. With `concurrency=4` and `max_call_cost ≈ $0.005` (a Pro call on a long German email at ~3k prompt + 200 completion tokens), the bound is **$0.02**. The cap is $20.00, so the overshoot is < 0.1% — auditable.
Reason: 4× wall-clock improvement (≈1.5 h vs 6.5 h) for a $0.02 worst-case cost ceiling that's well within tolerance. Going higher (`concurrency=8`) would shave another ~30 min but doubles the overshoot bound and runs into Gemini per-minute rate-limit territory; we'll revisit if signal-discovery (Step 8) needs a faster-moving extraction pass.
Revisit if: Gemini publishes higher per-minute rate limits, OR the backfill needs to repeat under a tighter cost cap, OR a real concurrency bug surfaces (cost-ledger deadlock, double-charge in logs, race on the `failed_events` UPSERT that we missed). Per the launch directive, any concurrency-related surprise in the first 5 minutes triggers a kill-and-revert-to-sequential.

## 2026-04-25 — Phase 8 Step 5: lease F1 = 0.62 driven by `lease.termination_notice` over-firing on owner-communication emails
Context: After Gemini Pro shipped on the German-first prompt, lease F1 was 0.62 — driven entirely by 5 spurious `lease.termination_notice` emissions on emails whose ground-truth category was `owner_communication` (sale intent, modernization consent, WEG governance). The model conflates "the relationship is changing" (owner side) with "the tenancy is ending" (lease side).
Decision: Ship as-is at 0.62, above the VERIFY 5 acceptance floor of 0.50. No prompt patch in Step 5; the residual is logged as a known limitation.
Reason: Step 5's quality bar is a floor, not a target — meeting it is the gate, exceeding it is gravy. Tightening the prompt today risks regressing other categories without a corresponding eval expansion to detect the regression. Better to leave the eval set as the witness and revisit when there's signal-discovery (Step 8) or production usage data showing this creates false signals.
Revisit if: Step 8 signal-discovery surfaces `lease.termination_notice` as a high-recall but low-precision rule, OR production usage shows owner-communication emails routinely triggering tenant-change downstream actions. Fix by adding to the German prompt: "Only emit `lease.termination_notice` when the *tenant* is signaling end of tenancy. Owner-side intent (sale, modernization, WEG governance) is `owner_communication`, not lease."

## 2026-04-25 — Phase 8 Step 5: compliance ↔ owner_communication category bleed on WEG-related emails
Context: 2 of 30 ground-truth `compliance` emails (Brandschutznachweis, Mietpreisbremse-adjacent topics) classified as `owner_communication` because the prompt's section-mapping pulls WEG-touching topics toward owner_communication. Within the F1 = 1.00 band on `compliance` (the other 2 hit), but the category-accuracy miss is real.
Decision: Within the 70% category-accuracy band; not patched in Step 5. Logged as a Step 8 lookback item.
Reason: Same reasoning as the lease residual — the band is met, the failure mode is identifiable, and prompt-tuning without an expanded eval set risks unmeasured collateral. The category bleed is consistent (always pulls toward owner_communication, never the reverse), so it's tractable when we get to it.
Revisit if: Step 8 signal-discovery includes a compliance-class signal (Brandschutz, Bauamt deadlines, Mietpreisbremse review) AND owner-communication emails are bleeding into the trigger set. Fix in the German prompt by explicitly listing compliance triggers (Brandschutznachweis, Bauamt, Mietpreisbremse, Vermessungsprotokoll) above the WEG-governance triggers in the section-routing list.

## 2026-04-25 — Phase 8 Step 5: value-string fidelity ~3 % (paraphrase, not source-quote)
Context: After the calibration metric was corrected to (section, field) match, the eval re-run showed 79 % observed accuracy in the 0.90–1.00 confidence bucket on key placement — but the per-section `value_match_rate` column is mostly 0.00 (max 0.40 on liegenschaft_financials). The model paraphrases the value: "Heizung defekt, kein Warmwasser seit gestern" becomes "Heating system malfunction reported; tenant lacks hot water since yesterday." Right slot, different words.
Decision: Accept for Phase 8. The key-placement accuracy is what the trust layer is built on; paraphrase is a Phase 9 concern when we surface facts in markdown to humans who'll want to see source-language verbatim quotes.
Reason: Phase 8 is product-quality on extraction; Phase 9 is the trust layer where source-fidelity becomes load-bearing (a fact rendered as paraphrase loses the auditability that the trust layer is selling). Forcing verbatim quotes today would either over-constrain the model (regressing recall) or require a post-hoc string-snap pass we don't have a primitive for yet.
Revisit if: Phase 9 trust-layer prompt-tuning: add a "value MUST be a verbatim substring of the source event when possible; only paraphrase when the source spans multiple sentences or contains markup that would corrupt the value field" clause. Validate by re-running the eval and watching `value_match_rate` rise (target ≥ 0.6 on the dense buckets).

## 2026-04-25 — Phase 8.1 Step 3b: forward-looking PDF placeholder text ("awaiting extraction")
Context: The connectors emitted `[INVOICE: filename.pdf] (text not extracted)` for PDF events whose body wasn't parsed at backfill time (we deliberately skip pdfplumber on the 329 PDFs in Step 3 to keep the cost ledger free for Steps 4-6). The phrasing reads like an error to a human glancing at the WEG Context block.
Decision: The connectors now emit `Invoice {filename} — awaiting extraction` (or `Letter {filename} — …`) for the no-body case, and the renderer's `_context_body` reformats existing rows to the same wording when `metadata.head_chars == 0`. New ingest writes the new text directly; existing 194 invoice rows display via the renderer fix without a re-write.
Reason: Phase 9's trust-layer ethos says the system should be honest about its epistemic state. "Awaiting extraction" is forward-looking and accurate — we know there's content there, we haven't processed it yet. It's not a failure mode to apologise for.
Revisit if: We start extracting PDF text on backfill (Steps 4-6 may opt-in) — at that point the renderer will fall through to the snippet path automatically once `head_chars > 0`.

## 2026-04-25 — Phase 8.1 Step 3b: 13 closed-lease MIE rows are *known acceptable* unrouted, not a routing bug
Context: After re-route, 13 events remain unrouted with the diagnostic `refs (none) kat=miete unresolved`. They are bank rows referencing `MIE-NNN` where the matching tenant has `mietende` set (lease ended). Step 2's loader correctly skipped the closed-lease tenant from `relationships(occupied_by)` because the active-only invariant — so the router can't resolve the `MIE-NNN` to a current property.
Decision: This is *expected* behaviour, not a routing miss. Logged here so a future operator reading the unrouted inbox doesn't waste cycles trying to fix it. If a property manager genuinely needs closed-lease historical routing, the right move is a Phase 9+ feature (e.g. an `historical_tenancies` table or an opt-in `include_closed=True` flag on the loader) — not loosening Step 2's invariant.
Reason: The active-only constraint protects the property markdown's "who lives here today" answer. Including closed-lease tenants in `occupied_by` would corrupt that for every per-property reader. Keeping 13 events unrouted is the cheapest, most honest representation.
Revisit if: A customer ships a meaningful number of historical-tenant events that need attribution to a unit (e.g. for late-payment reconstruction across tenancies) — at that point design a parallel `historical_occupied_by` edge that the router can consult after the active-only path misses.

## 2026-04-25 — Phase 8.1 Step 3b: three-tier ownership hierarchy (Liegenschaft → Building → Property)
Context: After Step 3a shipped, the live verify revealed 100% of Buena invoices and 12.5% of bank rows landing unrouted. Investigation showed Buena's events span three ownership tiers (1 WEG → 3 Häuser → 52 units), but the schema only modelled per-property attribution. WEG-billed events have a *known* correct attribution; routing them to "unrouted" was hiding truth, not surfacing a gap.
Decision: Add `liegenschaften` table + `buildings.liegenschaft_id` FK + `events.{building_id, liegenschaft_id}` + `facts.{building_id, liegenschaft_id}` (additive migration `0002_liegenschaft_hierarchy`). Router gains precedence rules for HAUS-N alias → building and WEG keyword/kategorie/invoice → liegenschaft. Renderer ends property markdown with **Building Context** and **WEG Context** subsections; new `/buildings/{id}/markdown` and `/liegenschaften/{id}/markdown` endpoints.
Reason: Routing a WEG event to one Haus (Option 1: HAUS-12 as primary) is the same false-attribution mistake we rejected at the unit level. A junction table (Option 3) over-engineers a 1:N relationship and burdens every reader. Liegenschaft alongside building reflects German Hausverwaltung reality and is supported by `stammdaten.json::liegenschaft` directly.
Revisit if: Customer data shows a multi-Liegenschaft tenant — at that point `_default_liegenschaft` (which assumes a single WEG) needs a routing key like `metadata.liegenschaft_id` or a verwendungszweck-based heuristic.

## 2026-04-25 — Phase 8.1 Step 3b: case-insensitive word-boundary WEG keyword matching
Context: Buena's `verwendungszweck` mixes capitalization freely — `Hausgeld`, `HAUSGELD`, `hausgeld` all appear in real rows. A naive substring or case-sensitive regex would either over-match (`hausgeldverordnung` shouldn't trigger if it ever appeared) or under-match (uppercase rows missed).
Decision: `_WEG_KEYWORD_RE = re.compile(rf"\b(?:{kws})\b", re.IGNORECASE)` over the keyword list. Word boundaries prevent substring traps; `re.IGNORECASE` covers casing variance. Keyword list is auditable: drawn from real Buena data + the German Hausverwaltung lexicon (Hausgeld, Verwaltergebühr/Verwaltergebuehr, Gemeinschaftskosten, Hausverwaltung, WEG, Sonderumlage, Instandhaltungsrücklage/Instandhaltungsruecklage, Kontoführungsgebühr).
Reason: Predictable matching across casing variants; auditable list of triggers; no third-party NLP dependency.
Revisit if: New customer data shows a token-overlap edge case (e.g. `WEG-` as a model number unrelated to Wohnungseigentümergemeinschaft) — at that point promote the heuristic to per-customer keyword sets.

## 2026-04-25 — Phase 8.1 Step 3b: invoice → liegenschaft fallback (Buena invariant)
Context: Invoice PDFs in Buena's archive carry filename pattern `YYYYMMDD_DL-NNN_INV-NNN.pdf` — contractor + invoice number, never an EH-NNN or HAUS-NN. Filename heuristics + WEG-keyword matching both fail, but the events are unambiguously WEG bills.
Decision: Router accepts an `event_source` argument; when `event_source == "invoice"` and a single Liegenschaft exists, route there with `method='weg_invoice'`, `reason='invoice without per-unit attribution'`. Loaders pass `event_source` from the `ConnectorEvent.source` field.
Reason: Honest data-shape recognition: every invoice in Buena's archive IS a WEG bill. Customers with per-Haus invoice patterns (filenames containing `HAUS-NN`) hit precedence 4 first. Customers with per-unit invoices carry `EH-NNN` or `MIE-NNN` and hit precedence 1-3.
Revisit if: A customer ships per-property invoices that lack EH-NNN — at that point the rule needs a per-customer override.

## 2026-04-25 — Phase 8.1 Step 3b: one-time re-route migration is documented, not permanent
Context: After Migration 0002 lands, already-ingested events need their `building_id` / `liegenschaft_id` populated via UPDATE. Running the original backfill skips them (idempotent on `(source, source_ref)`).
Decision: Ship `connectors.cli re-route` as a one-time migration step. Walks events with no scope set and re-evaluates routing using the current rules. Idempotent (re-running on already-routed events scans nothing). Documented as one-time in CLI help text and module docstring; not added to the scheduler or any auto-run path. Per-event live ingestion in `_ingest_one` already calls `route_structured` on each new row, so the rule changes apply automatically to new events.
Reason: Migration semantics belong in a dedicated CLI subcommand operators run intentionally, not in the live pipeline. The lack of automation makes it auditable: ledger-style record of "we ran this on date X" lives in DECISIONS.md.
Revisit if: We start needing recurring re-routes (e.g. when adding new keywords) — at that point the right tool is a SQL migration that backfills, not a Python loop.

## 2026-04-25 — Phase 8 Step 3: structured router refuses to invent property attribution
Context: Invoices in Buena's archive (filename pattern `YYYYMMDD_DL-NNN_INV-NNN.pdf`) are billed to the WEG (building owners' association), not to a specific unit. The filename carries contractor (DL-NNN) and invoice number (INV-NNN), never an `EH-NNN`. Bank rows of `kategorie=dienstleister` are the same shape — payments to a contractor for shared services, not for one unit.
Decision: `backend.pipeline.router.route_structured` only accepts a property when `metadata.eh_id`, `metadata.mie_id`, or `metadata.invoice_ref` *resolves* to one. No token-overlap fallback for structured events; no inventing attribution for shared services. The result: invoice events land 100% unrouted, bank events land 12.5% unrouted (the contractor-payment subset).
Reason: Honest — the schema currently models attribution at the property level only. Sprinkling shared-service costs onto the first matching property would corrupt every per-property financial metric. The unrouted inbox (`GET /admin/unrouted`) makes the gap observable instead of hidden.
Revisit if: We add a `building_id` column to `events` (one-line additive migration) so building-level events have somewhere honest to land. Step 8 / 9 work could prompt this; documented as a follow-up rather than a Step 3 blocker.

## 2026-04-25 — Phase 8 Step 3: per-month rent payments supersede the prior `last_rent_payment`
Context: A two-year bank archive holds 12-24 rent payments per active tenant. Writing each as its own immutable fact would clutter the renderer and obscure the "is this property current?" question.
Decision: `extract_bank_facts` writes `financials.last_rent_payment` superseding any prior fact for the same `(property_id, section, field)`. Each new payment becomes the current fact; the chain of superseded predecessors is the audit history (still queryable via `superseded_by`). Identical-value writes short-circuit so re-runs add nothing.
Reason: Matches KEYSTONE's "facts = current truth" rule (Part VI). 611 raw rent rows produced 26 current `last_rent_payment` facts after supersession — one per active tenant who has ever paid.
Revisit if: We add explicit financial activity timeline UI that wants every monthly row visible — the supersession history is already the data, just needs a different read path.

## 2026-04-25 — Phase 8 Step 1: connectors/ as customer-agnostic ingestion layer
Context: Buena dropped 29 MB of partner data; the next customer will drop a different shape. Earlier plans treated Buena as a special path inside `seed/`; that bakes a customer name into a generic primitive.
Decision: `connectors/` is the new customer-agnostic module. `connectors/base.py` defines the `Connector` Protocol + `ConnectorEvent` dataclass; primitives (`csv_stammdaten`, `eml_archive`, `camt_bank`, `pdf_invoice_archive`, `pdf_letter_archive`) are reusable. Each customer ships a single composite (`connectors/<customer>_archive.py`) that knows their directory shape. PII redaction is centralized in `connectors/redact.py` and applied at ingestion. Cost ledger (`connectors/cost_ledger.py` + `cost_ledger` table via `connectors/migrations.py`) is durable across CLI invocations.
Reason: Treats Buena as the first customer of a real architecture, not an exception. Future customers add one composite + tests; the primitives + redaction + ledger don't change.
Revisit if: A customer's shape doesn't fit the primitives — at that point we extend a primitive (e.g. add a Slack-archive walker) rather than fork.

## 2026-04-25 — Phase 8 Step 1: redact at ingestion, not at render
Context: Several places in the pipeline (renderer, MCP responses, SSE stream) read DB rows back out. Redacting at render means every reader has to know to scrub.
Decision: `connectors/redact.py` scrubs IBANs (→ `****<last4>`), full phones (→ `+CC *** **<last4>`), and email domains (→ `<local>@example.com`) before yielding any `ConnectorEvent`. Tests assert no `r"DE\d{20}"` and no full E.164 phone survives a round-trip through any connector. Buena is treated as PII until anonymisation status is confirmed (TODO note in `redact.py`).
Reason: Single chokepoint; one module to audit; zero burden on readers; defence-in-depth (renderer can still scrub additionally without changing semantics).
Revisit if: Buena confirms the dataset is fully synthetic — even then, keep redaction on by default; flip a feature flag if specific demos need fuller display.

## 2026-04-25 — Phase 8 Step 1: durable cost ledger persists across invocations
Context: A single `--max-total-cost-usd $20` cap should govern the entire Buena backfill, not just one CLI run. A per-run-only counter would let an operator hit Ctrl-C, restart, and silently spend another $20.
Decision: New `cost_ledger(source_label, cumulative_usd, cap_usd, hit_at)` table (additive `CREATE TABLE IF NOT EXISTS`, applied via `connectors/migrations.py`). Every Gemini call charges via `connectors.cost_ledger.charge`; on `>= cap` the row's `hit_at` is stamped and `CostCapExceeded` raised. Subsequent invocations read the row and abort immediately. Reset is friction-gated (`--reset-cost-ledger` + interactive `y/N`).
Reason: Spend-across-runs is the only definition that makes the cap real; defaults to "remember spend" so the operator never accidentally over-spends.
Revisit if: We add per-customer multi-tenancy and need per-customer caps — at that point use `source_label` to namespace per customer, which the schema already supports.

## 2026-04-25 — Phase 8 Step 2: `_upsert_property` honours `prop.metadata`
Context: The Phase 0 `_upsert_property` hardcoded `metadata = {"seed_key": prop.key}`, which kept the Berliner seed simple but lost richer payload (`kaltmiete`, `lage`, `wohnflaeche_qm`, …) needed for Buena units.
Decision: When `prop.metadata` is non-empty, the upsert merges it under `seed_key` so both worlds coexist (`{"seed_key": "EH-014", "lage": "5. OG mitte", "wohnflaeche_qm": 85.0, …}`). Same pattern for `_upsert_tenant`. `PropertySeed.metadata` and `TenantSeed.metadata` default to `{}` so the hand-crafted Berliner seed continues bit-identically (no change to existing data).
Reason: Single ingestion path for *all* customer master data — Buena rows and Berliner rows travel through the same SQL.
Revisit if: We start needing schema-level richer fields (e.g. dedicated columns for kaltmiete) — at that point migrate columns out of JSONB rather than fork the upsert.

## 2026-04-25 — Phase 8 Step 2: tenants with `mietende` set are skipped (active-only)
Context: Buena's `mieter.csv` includes 26 rows total but at least one has a populated `mietende` (lease ended). Including those as `occupied_by` edges would misrepresent current occupancy.
Decision: The Buena loader skips any tenant whose `metadata.active` is `False` (mietende set). Skipped count is reported in the summary (`tenants_skipped_inactive`). The 25 active tenants land with `occupied_by` edges; the 1 closed lease stays out of the relationship graph.
Reason: "Who lives here today" is a load-bearing question for the markdown / signals; including ex-tenants would confuse it. Closed leases should resurface only via lease history facts (Step 3+).
Revisit if: Lease-history reconstruction needs the closed-lease tenant rows in `tenants` — at that point insert them with a metadata flag and keep them out of `relationships(occupied_by)`.

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
