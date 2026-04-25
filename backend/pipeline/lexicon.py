"""Per-category keyword lexicon for the rule-based extractor fallback.

Step 5 promotes the previously-inlined regex set to a single source of
truth. Each ``Topic`` carries a German + English keyword pattern; both
match case-insensitively with word boundaries.

The lexicon is also surfaced to Step 8's signal_discovery notebook so
keyword analysis runs against the same vocabulary the extractor uses.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Final

# A keyword family — one pattern per language. The patterns use Python
# alternation (``a|b|c``); ``compile`` builds the case-insensitive,
# word-boundary regex.
_TopicPattern = tuple[str, str]  # (de_pattern, en_pattern)


@dataclass(frozen=True)
class Topic:
    """Named topic with language-specific keyword patterns."""

    name: str
    de: re.Pattern[str]
    en: re.Pattern[str]

    def matches(self, text: str, *, lang: str | None = None) -> str | None:
        """Return the matched substring, or ``None`` if neither pattern fires."""
        for code, regex in self._dispatch(lang):
            m = regex.search(text)
            if m:
                return m.group(0)
        return None

    def _dispatch(self, lang: str | None) -> list[tuple[str, re.Pattern[str]]]:
        if lang == "de":
            return [("de", self.de), ("en", self.en)]
        if lang == "en":
            return [("en", self.en), ("de", self.de)]
        return [("de", self.de), ("en", self.en)]


def _compile(pat: str) -> re.Pattern[str]:
    """Compile a keyword alternation as a case-insensitive word-boundary regex."""
    return re.compile(rf"\b(?:{pat})\b", re.IGNORECASE)


# Topic patterns drawn from real Buena bodies + ground truth + Phase 1's
# regex set. Each pattern is intentionally narrow — over-broad keywords
# (``mahnung`` for example) are out of this lexicon and live in
# ``router.WEG_KEYWORDS`` instead, where the precedence rules can keep
# them from over-matching.
HEATING: Final = Topic(
    name="heating",
    de=_compile("heiz|heizung|kalt|warmwasser|frost|thermostat|heizungsausfall"),
    en=_compile("heat|heater|heating|hot[- ]water|cold[- ]radiator|thermostat"),
)
WATER: Final = Topic(
    name="water",
    de=_compile("wasser|leck|tropf|feucht|schimmel|rohr|wasserschaden"),
    en=_compile("water|leak|drip|moisture|mould|mold|pipe|water[- ]damage"),
)
PAYMENT: Final = Topic(
    name="payment",
    de=_compile(
        "miete|kaltmiete|nebenkosten|nebenkostenabrechnung|zahlung|"
        "lastschrift|säumig|kaution|hausgeld"
    ),
    en=_compile("rent|payment|deposit|overdue|invoice|wire[- ]transfer|sepa"),
)
LEASE: Final = Topic(
    name="lease",
    de=_compile(
        "mietvertrag|kündigung|kuendigung|verlängerung|verlaengerung|"
        "nachmieter|untermiete|mieterwechsel|mietende|wohnungsuebergabe"
    ),
    en=_compile(
        "lease|tenancy|termination|renewal|sub[- ]let|tenant[- ]change|"
        "move[- ]out|hand[- ]over"
    ),
)
COMPLIANCE: Final = Topic(
    name="compliance",
    de=_compile(
        "mietpreisbremse|verordnung|prüfung|abnahme|brandschutz|"
        "modernisierung|baugenehmigung|brandschutznachweis"
    ),
    en=_compile(
        "regulation|inspection|approval|fire[- ]safety|modernization|"
        "building[- ]permit"
    ),
)
COMPLAINT: Final = Topic(
    name="complaint",
    de=_compile("beschwerde|reklamation|störung|stoerung|defekt|mängel|maengel"),
    en=_compile("complaint|defect|malfunction|disturbance"),
)
KEY_LOSS: Final = Topic(
    name="key_loss",
    de=_compile("schluesselverlust|schlüsselverlust|schließanlage|schliessanlage"),
    en=_compile("lost[- ]key|key[- ]replacement|locksmith|lock[- ]change"),
)
WINDOW_DOOR: Final = Topic(
    name="window_door",
    de=_compile("fenster|tür|tuer|griff|dichtung|jalousie|rolladen"),
    en=_compile("window|door|handle|seal|blind|shutter"),
)
TENANT_CHANGE: Final = Topic(
    name="tenant_change",
    de=_compile(
        "kuendigung|kündigung|mieterwechsel|nachmieter|"
        "wohnungsuebergabe|wohnungsübergabe|kaution"
    ),
    en=_compile(
        "lease[- ]termination|tenant[- ]change|new[- ]tenant|"
        "hand[- ]over|deposit[- ]return"
    ),
)
OWNER_COMM: Final = Topic(
    name="owner_communication",
    de=_compile(
        "verkaufsabsicht|sonderumlage|hausverwaltung|eigentuemer|"
        "eigentümer|etv|beirat|bescheinigung"
    ),
    en=_compile(
        "sale[- ]intent|owner[- ]assembly|board[- ]member|certificate"
    ),
)


# Ordered list — categorize() returns the first match.
TOPICS: tuple[Topic, ...] = (
    OWNER_COMM,
    TENANT_CHANGE,
    LEASE,
    HEATING,
    WATER,
    KEY_LOSS,
    WINDOW_DOOR,
    COMPLIANCE,
    COMPLAINT,
    PAYMENT,
)


def categorize(text: str, *, lang: str | None = None) -> tuple[Topic, str] | None:
    """Return the first :class:`Topic` whose pattern fires + the matched span."""
    for topic in TOPICS:
        match = topic.matches(text, lang=lang)
        if match:
            return topic, match
    return None
