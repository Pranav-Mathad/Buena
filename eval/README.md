# Keystone Extraction Evaluation

Phase 8 Step 4 framework. **We do not improve what we cannot
measure.** This directory holds the ground truth, runner, metrics,
and dated reports that gate every change to
`backend/pipeline/extractor.py` from Step 5 onward.

## Layout

```
eval/
├── README.md
├── ground_truth/
│   └── emails_v1.jsonl          # one JSON object per line — see schema below
├── runner.py                    # python -m eval.runner --set emails_v1
├── metrics.py                   # P/R/F1, calibration curve, routing accuracy
├── runs/                        # YYYY-MM-DD-stepN.md reports
└── tests/                       # smoke + unit on a fixture file
```

## Sampling methodology (`emails_v1`)

30 emails total, drawn from `Extracted/emails/` and
`Extracted/incremental/day-*/emails/`:

- **12 incremental** — uniformly sampled across day-01..day-10 with
  `random.seed(42)`. These drive the Phase 10 live-feed demo so
  extraction quality on them gates the demo.
- **18 archive** — stratified into six buckets of three emails each:
  - `heating`     — keyword `heiz|heizung|kalt|warmwasser|thermostat`
  - `water`       — `wasser|leck|tropf|feucht|schimmel|rohr`
  - `payment`     — `miete|zahlung|mahnung|überweisung|lastschrift|säumig`
  - `lease`       — `mietvertrag|kündigung|verlängerung|nachmieter`
  - `compliance`  — `mietpreisbremse|verordnung|prüfung|abnahme|brandschutz|weg|hausverwaltung`
  - `other`       — none of the above

The bucketing primary-keyword is preserved in the row's
`ground_truth.notes` field for traceability.

The script that produced the sample list is reproducible — re-running
`scripts/sample_eval_emails.py` with the same seed picks the same
files.

### Optional expansion to 100

Step 4 ships **30 hand-annotated** rows. Step 5 metrics decide whether
to expand: if per-category P/R/F1 confidence intervals are too wide
to distinguish the Step-5 changes from noise, expand to 100 with
Gemini Pro drafting + manual audit. Default plan: stop at 30, save
the time.

The Gemini Pro drafting prompt lives at
`backend/services/prompts/eval_groundtruth_draft.md` (added when we
trigger the expansion); audit is gated on a manual diff before
commit.

## Ground-truth row schema

One JSON object per line. Every field is required unless marked
optional.

```jsonl
{
  "event_id": "EMAIL-04031",
  "source": "email",
  "raw_content": "<full body, redacted via connectors.redact.scrub_text>",
  "metadata": {
    "from": "<redacted>",
    "subject": "<original subject>",
    "date": "2025-03-16T16:10:00+00:00",
    "filename_hint": "incremental/day-01/.../EMAIL-XXXX.eml"
  },
  "ground_truth": {
    "category": "maintenance",
    "priority": "high",
    "facts_to_update": [
      {
        "section": "maintenance",
        "field": "open_water_issue",
        "value": "Wasserleitung im Bad tropft seit 14.03.",
        "confidence_min": 0.8
      }
    ],
    "expected_property_alias": "EH-007",
    "expected_scope": "property",
    "notes": "annotator commentary; sampling bucket; edge cases"
  }
}
```

Field semantics:

- `event_id` — stable identifier; matches `events.source_ref` for the
  email when ingested.
- `source` / `metadata` — what the connector produced after
  redaction. `raw_content` is what gets fed to the extractor; it
  must be PII-redacted (no raw IBAN, full E.164 phone, or full email
  domain).
- `ground_truth.category` / `priority` — single canonical value per
  the Part VII enum.
- `ground_truth.facts_to_update[]` — every fact the email *should*
  produce. Annotator is conservative: omit anything they're not sure
  the model could pick up.
- `ground_truth.confidence_min` — the floor below which we'd consider
  the extraction "weak"; the runner uses this for calibration
  scoring.
- `ground_truth.expected_property_alias` / `expected_scope` —
  routing oracle (Phase 8.1: `property | building | liegenschaft |
  unrouted`).
- `ground_truth.notes` — keep the bucket label + the annotator's
  reasoning for the hard calls. Future-self thanks them.

## Runner

```
python -m eval.runner --set emails_v1
python -m eval.runner --set emails_v1 --json --out eval/runs/2026-04-25-step4.md
```

Reads ground-truth file, runs each row through
`backend.pipeline.extractor.extract`, builds a
:class:`eval.metrics.Report`. The runner prints a markdown summary
and (with `--out`) writes a dated report into `runs/`.

## Metrics

`eval.metrics.Report` carries:

- Per-category **precision / recall / F1**. Match key is
  `(section, field)` — value-equality is reported separately.
- **Routing accuracy** — fraction of rows whose extracted scope
  matches `ground_truth.expected_scope`.
- **Calibration curve** — extractions bucketed by reported
  `confidence` (0.5–0.6, 0.6–0.7, …, 0.9–1.0); for each bucket the
  fraction of extractions where the *value matches* the ground-truth
  value. A well-calibrated extractor's bucket midpoint ≈ accuracy in
  that bucket.
- **Top-20 failures** — the rows where the extractor most diverged
  from ground truth, with a one-line diff.
- **Token spend** — sum of Gemini token counters across the run, for
  cost reasoning.

## Operating principle

Honest numbers, no smoothing. If a metric drops between Step 5 and
Step 6, that's the finding; we discuss it before merging anything.
The first eval run in `runs/` becomes the baseline.
