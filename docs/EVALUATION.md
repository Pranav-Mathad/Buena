# Evaluation

How Keystone's extraction is measured, what the latest numbers are, and where
it is weak. Every number below is copied from a report in
[`docs/eval-runs/`](eval-runs/); the file is named next to each figure. The
harness itself (ground-truth format, runner, metrics) is documented in
[`eval/README.md`](../eval/README.md).

## What is measured

For each email in the ground-truth set, the runner calls the production
extractor (`backend.pipeline.extractor.extract`) and the production router,
then compares the output with hand-written expectations:

- **Category accuracy**: share of emails whose predicted category
  (`maintenance`, `payment`, `lease`, `compliance`, `owner_communication`, ...)
  matches the label.
- **Routing accuracy**: share of emails routed to the expected scope
  (`property`, `building`, `liegenschaft` or `unrouted`). The router uses the
  master data in the database, so this needs the units loaded.
- **Per-section precision, recall and F1** over extracted facts. A fact
  counts as a true positive when its `(section, field)` key matches an
  expected fact. Whether the value text also matches is reported separately as
  **value-match**, the share of true positives whose value matches the
  expected value.
- **Calibration**: extracted facts bucketed by the model's stated confidence,
  with the share in each bucket whose key was correct.
- **Token spend**, charged to a cost-ledger label per run.

## Dataset

`eval/ground_truth/emails_v1.jsonl` holds 30 German and English emails from
the sample property-management archive, hand-annotated with category,
priority, expected facts, expected scope and notes.

- 12 drawn uniformly from ten days of incoming mail (`random.seed(42)`).
- 18 drawn from the archive, three from each of six keyword buckets: heating,
  water, payment, lease, compliance, other.

The script that drew the sample was not committed; the JSONL file is the
sample. Bodies were PII-redacted at ingestion, and person and company names
were pseudonymised on 2026-10-17 (see [DECISIONS.md](DECISIONS.md)). No
inter-annotator agreement was measured.

## How to run

```bash
python -m eval.runner --set emails_v1                                   # markdown to stdout
python -m eval.runner --set emails_v1 --out docs/eval-runs/<date>-<name>.md
python -m eval.runner --set emails_v1 --via pioneer                     # use the Pioneer backend
```

The runner needs Postgres with master data loaded for the routing metric:
`python -m seed.seed`, then
`EXTRACTED_ROOT=Extracted_demo python -m connectors.cli load-stammdaten --source buena`.
Without an API key the extractor falls back to its rule-based path, which is
what the 2026-10-17 run measures.

## Results

Three runs on the same 30 emails:

| | Gemini | Pioneer | Rule-based (no key) |
|---|---:|---:|---:|
| Report | `2026-04-25-step5.md` | `2026-04-26-emails_v1-pioneer.md` | `2026-10-17-emails_v1-rule-based.md` |
| Model | `gemini-2.5-pro` (configured default; not recorded in the report) | Pioneer gateway (default `claude-sonnet-4-6`; not recorded in the report) | keyword lexicon |
| Category accuracy | 73.3% | 73.3% | 63.3% |
| Routing accuracy | 93.3% | 93.3% | 93.3% |
| Facts expected / extracted / key-correct | 26 / 33 / 26 | 26 / 33 / 26 | 26 / 26 / 11 |
| Micro precision / recall / F1 | 0.79 / 1.00 / 0.88 | 0.79 / 1.00 / 0.88 | 0.42 / 0.42 / 0.42 |
| Value-match (of key-correct facts) | 3 of 26 | 2 of 26 | 0 of 11 |
| Tokens (prompt / completion) | 51,650 / 4,982 | 106,773 / 10,771 | 0 / 0 |

Micro-averaged figures are computed from the per-section tables in each
report: precision = key-correct / extracted, recall = key-correct / expected.

Per-section F1:

| Section | Expected | Gemini | Pioneer | Rule-based |
|---|---:|---:|---:|---:|
| `maintenance` | 7 | 1.00 | 1.00 | 0.67 |
| `liegenschaft_financials` | 5 | 0.91 | 0.91 | 0.00 |
| `financials` | 4 | 0.89 | 0.89 | 0.00 |
| `lease` | 4 | 0.62 | 0.62 | 0.67 |
| `compliance` | 2 | 1.00 | 1.00 | 0.00 |
| `overview` | 2 | 1.00 | 1.00 | 0.44 |
| `liegenschaft_compliance` | 1 | 1.00 | 1.00 | 0.00 |
| `liegenschaft_maintenance` | 1 | 1.00 | 1.00 | 0.00 |

Calibration (key correctness by stated confidence):

| Run | Bucket | Facts | Key-correct | Observed |
|---|---|---:|---:|---:|
| Gemini | 0.90 to 1.00 | 33 | 26 | 0.79 |
| Pioneer | 0.90 to 1.00 | 32 | 26 | 0.81 |
| Pioneer | 0.70 to 0.80 | 1 | 0 | 0.00 |
| Rule-based | 0.70 to 0.80 | 26 | 11 | 0.42 |

The Gemini and Pioneer runs produced identical counts in every section; they
differ only in value-match, calibration and token use. With 26 expected facts,
that is plausible for two strong models on the same prompt, but it also means
this set cannot tell them apart.

## Known weaknesses

- **Lease precision is low.** Lease F1 is 0.62 for both LLM runs: precision
  0.44 (4 correct of 9 extracted). The extra five are
  `lease.termination_notice` facts on emails about the owner side (sale
  intent, modernization consent, owners'-association governance). The model
  reads "the relationship is changing" as "the tenancy is ending". The fix is
  a prompt rule that only a tenant can give notice; it has not been applied
  because the set is too small to show whether it regresses other sections.
- **Values are paraphrased, not quoted.** Only 3 of 26 (Gemini) and 2 of 26
  (Pioneer) correctly placed facts have value text matching the expected
  value. The model rewrites "Heizung defekt, kein Warmwasser seit gestern" as
  an English summary. Key placement is reliable; verbatim source fidelity is
  not.
- **Confidence is inflated.** Almost every LLM fact is reported at 0.90 or
  above, and about one in five of those is in the wrong slot (0.79 and 0.81
  observed). Confidence separates nothing at this sample size, which weakens
  the 0.7 confidence floor as a filter.
- **Category accuracy is 73.3%.** Eight of 30 emails get the wrong category.
  In the Pioneer report, five go to `tenant_change` (the termination-notice
  confusion above), one to `payment`, and two compliance emails to
  `owner_communication`.
- **Routing over-attributes to the Liegenschaft.** Routing accuracy is 93.3%
  (28 of 30) in all three runs; it does not depend on the extractor. Both
  misses are emails labelled `unrouted` (no property or owners'-association
  relevance) that the router sends to the Liegenschaft.
- **The rule-based fallback is a safety net, not a substitute.** It reaches
  63.3% category accuracy but extracts no Liegenschaft-level facts at all, and
  42% of its facts are in the right slot.
- **The set is small and partly stale.** 30 emails and 26 expected facts give
  wide error bars; one fact moves a section's F1 noticeably. Both LLM runs
  predate later extractor changes (the Gemini run predates the confidence
  floor and `uncertain[]` output; both predate an extractor revision on
  2026-04-26) and the name pseudonymisation. They have not been re-run.

## Run history

| Report | What it measured |
|---|---|
| `2026-04-25-step4-baseline.md` | First run, rule-based extractor at the time: 16.7% category accuracy, 76.7% routing, no key-correct facts |
| `2026-04-25-step4-routing-fix.md` | Same extractor after a routing fix: routing 76.7% to 93.3% |
| `2026-04-25-step5.md` | Gemini on the German-first prompt |
| `2026-04-25-step6-partial.md` | Not an extraction eval: the partial email backfill (251 events, 85 facts, $0.4868, LLM latency p50 10.9 s and p99 37.1 s over 100 calls) and the silent hang that stopped it |
| `2026-04-26-emails_v1-pioneer.md` | Pioneer backend on the same set |
| `2026-10-17-emails_v1-rule-based.md` | Current rule-based fallback, no API key, pseudonymised set |

The rule-based extractor's lexicon grew between the first run and the
2026-10-17 run, which is why the two rule-based numbers differ (16.7% and
63.3% category accuracy).
