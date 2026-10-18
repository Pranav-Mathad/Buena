"""Fail when a file contains data that must not be committed.

Run by pre-commit on staged files, or with no arguments on every tracked
file. Three checks:

- **Blocked street addresses.** Every "street + house number" in a file is
  normalised (lowercase, ``ß`` and ``str.`` spelled ``strasse``) and its
  SHA-256 compared with :data:`BLOCKED_ADDRESS_HASHES`. The list holds
  hashes, not addresses, so the check does not put the address it guards
  against back into the repository.
- **IBAN-shaped numbers** (``DE`` followed by 20 digits). The synthetic
  dataset in ``Extracted_demo/`` is exempt.
- **Cloud Run URLs** (``*.run.app``). Deployment URLs identify a project and
  do not belong in the source.

Usage::

    python scripts/check_sensitive_data.py [FILE ...]
"""

from __future__ import annotations

import hashlib
import re
import subprocess
import sys
from collections.abc import Iterable, Iterator
from pathlib import Path

# SHA-256 of the normalised form produced by normalise_address().
BLOCKED_ADDRESS_HASHES = frozenset(
    {
        # The building in the original (non-synthetic) dataset.
        "44926ef2e0fc0bde63c520e071d3573cb291e4adf8bf5e6b61884957eeaaddea",
    }
)

EXEMPT_PREFIXES = ("Extracted_demo/",)

_ADDRESS = re.compile(r"([a-zäöüß][a-zäöüß-]*?)(?:straße|strasse|str\.)\s*(\d+)", re.IGNORECASE)
_IBAN = re.compile(r"\bDE\d{20}\b")
_RUN_APP = re.compile(r"[\w.-]+\.run\.app\b")


def normalise_address(street: str, number: str) -> str:
    """Return the form that is hashed: ``<street>strasse <number>``, lowercase."""
    return f"{street.lower().replace('ß', 'ss')}strasse {number}"


def _line_of(text: str, offset: int) -> int:
    return text.count("\n", 0, offset) + 1


def find_problems(path: str, text: str) -> Iterator[str]:
    """Yield one ``path:line: message`` per finding in ``text``."""
    for match in _ADDRESS.finditer(text):
        key = normalise_address(match.group(1), match.group(2))
        if hashlib.sha256(key.encode()).hexdigest() in BLOCKED_ADDRESS_HASHES:
            yield f"{path}:{_line_of(text, match.start())}: blocked street address"
    if not path.startswith(EXEMPT_PREFIXES):
        for match in _IBAN.finditer(text):
            yield f"{path}:{_line_of(text, match.start())}: IBAN-shaped number"
    for match in _RUN_APP.finditer(text):
        yield f"{path}:{_line_of(text, match.start())}: Cloud Run URL"


def _tracked_files() -> list[str]:
    result = subprocess.run(["git", "ls-files"], capture_output=True, text=True, check=True)
    return result.stdout.splitlines()


def check(paths: Iterable[str]) -> list[str]:
    """Return every finding in ``paths``; binary and missing files are skipped."""
    problems: list[str] = []
    for path in paths:
        try:
            text = Path(path).read_text(encoding="utf-8")
        except (UnicodeDecodeError, FileNotFoundError, IsADirectoryError):
            continue
        problems.extend(find_problems(path, text))
    return problems


def main(argv: list[str]) -> int:
    problems = check(argv or _tracked_files())
    for problem in problems:
        print(problem)
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
