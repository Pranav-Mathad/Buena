"""Metrics + reporting for the extraction eval.

The runner builds one :class:`Row` per ground-truth example, and
:class:`Report` aggregates them. All scores are computed honestly:

- **Precision / Recall / F1 per category** — match key is
  ``(section, field)``. Value-equality is reported as a separate
  ``value_match_rate`` so we can see when the extractor finds the
  right slot but a different (still-defensible) phrasing.
- **Routing accuracy** — fraction whose extracted scope matches
  ``ground_truth.expected_scope`` (Phase 8.1: ``property | building |
  liegenschaft | unrouted``).
- **Calibration curve** — confidence-bucketed accuracy where
  "correct" means the extractor placed the fact in the right
  ``(section, field)`` slot. Value-equality lives in the per-section
  ``value_match_rate`` column so paraphrase doesn't pollute the
  calibration signal. The bucket midpoint should approximate accuracy
  if the extractor is honest about its uncertainty.
- **Top-N failures** — rows where the extractor diverged most.

No smoothing, no rounding-to-zero, no "but the model meant well".
"""

from __future__ import annotations

import textwrap
from dataclasses import dataclass, field
from typing import Any


# Confidence buckets for the calibration curve.
CALIBRATION_BUCKETS: tuple[tuple[float, float], ...] = (
    (0.0, 0.5),
    (0.5, 0.6),
    (0.6, 0.7),
    (0.7, 0.8),
    (0.8, 0.9),
    (0.9, 1.001),
)


@dataclass
class Row:
    """One eval row's evaluation outcome."""

    event_id: str
    expected_category: str
    extracted_category: str
    category_correct: bool
    expected_priority: str
    extracted_priority: str

    # ground_truth.facts_to_update[]
    expected_facts: list[dict[str, Any]]
    # extractor's facts_to_update[]
    extracted_facts: list[dict[str, Any]]

    # Per-fact match details — len(matches) == len(expected_facts).
    # Each entry: {section, field, expected_value, extracted_value,
    # extracted_confidence, key_matched, value_matched}
    fact_matches: list[dict[str, Any]] = field(default_factory=list)

    # Extra extracted facts that have no expected counterpart.
    spurious_facts: list[dict[str, Any]] = field(default_factory=list)

    expected_scope: str = "property"
    extracted_scope: str = "property"
    routing_correct: bool = False

    extractor_source: str = "rule"  # 'gemini' | 'rule'
    latency_ms: float = 0.0
    prompt_tokens: int | None = None
    completion_tokens: int | None = None

    failure_summary: str = ""


@dataclass
class CategoryStats:
    """Per-category P / R / F1."""

    category: str
    expected_count: int = 0
    extracted_count: int = 0
    true_positive: int = 0  # key matched
    value_correct: int = 0  # key matched AND value matched

    @property
    def precision(self) -> float:
        return self.true_positive / self.extracted_count if self.extracted_count else 0.0

    @property
    def recall(self) -> float:
        return self.true_positive / self.expected_count if self.expected_count else 0.0

    @property
    def f1(self) -> float:
        p, r = self.precision, self.recall
        return (2 * p * r / (p + r)) if (p + r) > 0 else 0.0

    @property
    def value_match_rate(self) -> float:
        return self.value_correct / self.true_positive if self.true_positive else 0.0


@dataclass
class CalibrationBucket:
    """One row of the calibration curve."""

    low: float
    high: float
    count: int = 0
    correct: int = 0

    @property
    def midpoint(self) -> float:
        return (self.low + self.high) / 2.0

    @property
    def accuracy(self) -> float:
        return self.correct / self.count if self.count else 0.0


@dataclass
class Report:
    """Full evaluation report."""

    set_name: str
    rows: list[Row] = field(default_factory=list)

    @property
    def n_rows(self) -> int:
        return len(self.rows)

    @property
    def routing_accuracy(self) -> float:
        if not self.rows:
            return 0.0
        return sum(1 for r in self.rows if r.routing_correct) / len(self.rows)

    @property
    def category_accuracy(self) -> float:
        if not self.rows:
            return 0.0
        return sum(1 for r in self.rows if r.category_correct) / len(self.rows)

    def category_stats(self) -> list[CategoryStats]:
        """Per-category P/R/F1 over the section.field match keys."""
        stats: dict[str, CategoryStats] = {}
        for row in self.rows:
            for ef in row.expected_facts:
                sec = str(ef["section"])
                stats.setdefault(sec, CategoryStats(category=sec)).expected_count += 1
            for xf in row.extracted_facts:
                sec = str(xf.get("section", ""))
                stats.setdefault(sec, CategoryStats(category=sec)).extracted_count += 1
            for fm in row.fact_matches:
                if not fm.get("key_matched"):
                    continue
                sec = str(fm["section"])
                stats.setdefault(sec, CategoryStats(category=sec)).true_positive += 1
                if fm.get("value_matched"):
                    stats[sec].value_correct += 1
        return sorted(stats.values(), key=lambda s: s.category)

    def calibration(self) -> list[CalibrationBucket]:
        """Calibration curve over extractor confidence.

        Iterates *every fact the extractor produced* (matched or
        spurious). A fact counts as ``correct`` when its
        ``(section, field)`` key matches a ground-truth fact for the
        same row — i.e. the extractor put the observation in the right
        slot. Value-equality is reported separately via
        :attr:`CategoryStats.value_match_rate`; conflating the two here
        punishes paraphrase even when the model nailed the slot.

        A well-calibrated extractor's bucket midpoint should approximate
        accuracy in that bucket.
        """
        buckets = [CalibrationBucket(low=lo, high=hi) for lo, hi in CALIBRATION_BUCKETS]
        for row in self.rows:
            expected_keys: set[tuple[str, str]] = {
                (str(ef.get("section", "")), str(ef.get("field", "")))
                for ef in row.expected_facts
            }

            for xf in row.extracted_facts:
                conf_raw = xf.get("confidence")
                if conf_raw is None:
                    continue
                try:
                    conf = float(conf_raw)
                except (TypeError, ValueError):
                    continue
                key = (str(xf.get("section", "")), str(xf.get("field", "")))
                key_correct = key in expected_keys
                for b in buckets:
                    if b.low <= conf < b.high:
                        b.count += 1
                        if key_correct:
                            b.correct += 1
                        break
        return buckets

    def top_failures(self, limit: int = 20) -> list[Row]:
        """Rows where the extractor diverged most from ground truth.

        Heuristic ranking: missed facts (false negatives) plus spurious
        facts (false positives) plus category mismatch counts as 0.5.
        Routing miss adds 1.
        """
        scored: list[tuple[float, Row]] = []
        for row in self.rows:
            missed = sum(1 for fm in row.fact_matches if not fm.get("key_matched"))
            spurious = len(row.spurious_facts)
            score = (
                float(missed) + float(spurious)
                + (0.5 if not row.category_correct else 0.0)
                + (1.0 if not row.routing_correct else 0.0)
            )
            if score > 0:
                scored.append((score, row))
        scored.sort(key=lambda p: p[0], reverse=True)
        return [r for _, r in scored[:limit]]

    def total_tokens(self) -> dict[str, int]:
        """Sum prompt + completion tokens across rows where Gemini fired."""
        prompt = sum(r.prompt_tokens or 0 for r in self.rows)
        completion = sum(r.completion_tokens or 0 for r in self.rows)
        return {"prompt_tokens": prompt, "completion_tokens": completion}

    def render_markdown(self) -> str:
        """Pretty markdown summary suitable for committing to ``eval/runs/``."""
        lines: list[str] = [
            f"# Eval report — {self.set_name}",
            "",
            f"- rows scored: **{self.n_rows}**",
            f"- category accuracy: **{self.category_accuracy:.1%}**",
            f"- routing accuracy: **{self.routing_accuracy:.1%}**",
            f"- token spend: prompt={self.total_tokens()['prompt_tokens']}, "
            f"completion={self.total_tokens()['completion_tokens']}",
            "",
            "## Per-section P / R / F1",
            "",
            "| section | expected | extracted | TP | P | R | F1 | value-match |",
            "|---|---:|---:|---:|---:|---:|---:|---:|",
        ]
        for cs in self.category_stats():
            lines.append(
                f"| `{cs.category}` | {cs.expected_count} | {cs.extracted_count} | "
                f"{cs.true_positive} | {cs.precision:.2f} | {cs.recall:.2f} | "
                f"{cs.f1:.2f} | {cs.value_match_rate:.2f} |"
            )

        lines += ["", "## Calibration curve", "", "| bucket | n | correct | observed accuracy |", "|---|---:|---:|---:|"]
        for b in self.calibration():
            lines.append(
                f"| {b.low:.2f}–{b.high:.2f} | {b.count} | {b.correct} | {b.accuracy:.2f} |"
            )

        failures = self.top_failures(limit=20)
        if failures:
            lines += [
                "",
                f"## Top {len(failures)} failures",
                "",
            ]
            for r in failures:
                summary = r.failure_summary or _row_diff(r)
                lines.append(f"- `{r.event_id}` — {textwrap.shorten(summary, width=160)}")

        return "\n".join(lines).rstrip() + "\n"


def _row_diff(row: Row) -> str:
    """One-line diff between expected and extracted for the failure list."""
    parts: list[str] = []
    if not row.category_correct:
        parts.append(
            f"category {row.expected_category!r}→{row.extracted_category!r}"
        )
    if not row.routing_correct:
        parts.append(
            f"scope {row.expected_scope!r}→{row.extracted_scope!r}"
        )
    missed = [
        fm for fm in row.fact_matches if not fm.get("key_matched")
    ]
    if missed:
        parts.append(
            "missed "
            + ", ".join(f"{m['section']}.{m['field']}" for m in missed[:3])
        )
    if row.spurious_facts:
        parts.append(
            "spurious "
            + ", ".join(
                f"{f.get('section', '?')}.{f.get('field', '?')}"
                for f in row.spurious_facts[:3]
            )
        )
    return "; ".join(parts) or "(no measurable diff)"


def score_row(
    *,
    event_id: str,
    expected: dict[str, Any],
    extracted_category: str,
    extracted_priority: str,
    extracted_facts: list[dict[str, Any]],
    extractor_source: str,
    latency_ms: float,
    prompt_tokens: int | None,
    completion_tokens: int | None,
    extracted_scope: str,
) -> Row:
    """Compare one extraction against its ground truth and produce a :class:`Row`."""
    expected_category = str(expected.get("category", ""))
    expected_priority = str(expected.get("priority", ""))
    expected_facts = list(expected.get("facts_to_update") or [])
    expected_scope = str(expected.get("expected_scope") or "property")

    # Map (section, field) → extracted fact for quick lookup
    extracted_by_key: dict[tuple[str, str], dict[str, Any]] = {}
    for f in extracted_facts:
        key = (str(f.get("section", "")), str(f.get("field", "")))
        # Keep the highest-confidence one if duplicates appear.
        existing = extracted_by_key.get(key)
        if existing is None or float(f.get("confidence") or 0) > float(
            existing.get("confidence") or 0
        ):
            extracted_by_key[key] = f

    fact_matches: list[dict[str, Any]] = []
    matched_keys: set[tuple[str, str]] = set()
    for ef in expected_facts:
        key = (str(ef.get("section", "")), str(ef.get("field", "")))
        xf = extracted_by_key.get(key)
        if xf is None:
            fact_matches.append(
                {
                    "section": key[0],
                    "field": key[1],
                    "expected_value": str(ef.get("value", "")),
                    "extracted_value": None,
                    "extracted_confidence": None,
                    "key_matched": False,
                    "value_matched": False,
                }
            )
            continue
        matched_keys.add(key)
        expected_value = str(ef.get("value", "")).strip().lower()
        extracted_value = str(xf.get("value", "")).strip().lower()
        # Soft value match: substring containment in either direction
        # so paraphrased-but-equivalent extractions count.
        value_matched = bool(
            expected_value
            and (
                expected_value in extracted_value
                or extracted_value in expected_value
            )
        )
        fact_matches.append(
            {
                "section": key[0],
                "field": key[1],
                "expected_value": ef.get("value"),
                "extracted_value": xf.get("value"),
                "extracted_confidence": xf.get("confidence"),
                "key_matched": True,
                "value_matched": value_matched,
            }
        )

    spurious_facts = [
        f
        for f in extracted_facts
        if (str(f.get("section", "")), str(f.get("field", ""))) not in matched_keys
    ]

    return Row(
        event_id=event_id,
        expected_category=expected_category,
        extracted_category=extracted_category,
        category_correct=expected_category == extracted_category,
        expected_priority=expected_priority,
        extracted_priority=extracted_priority,
        expected_facts=expected_facts,
        extracted_facts=extracted_facts,
        fact_matches=fact_matches,
        spurious_facts=spurious_facts,
        expected_scope=expected_scope,
        extracted_scope=extracted_scope,
        routing_correct=expected_scope == extracted_scope,
        extractor_source=extractor_source,
        latency_ms=latency_ms,
        prompt_tokens=prompt_tokens,
        completion_tokens=completion_tokens,
    )
