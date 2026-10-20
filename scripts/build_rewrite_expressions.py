"""Build and verify the ``git filter-repo`` expressions for the public history.

The committed tree is clean, but older commits still contain the original
dataset's street address, a Cloud Run URL, real IBANs and real names. This
script derives the replacements from the history itself, so neither it nor
the release checklist has to spell any of them out.

``build`` (run in the original repository) writes two files to ``--out``,
which must be outside the repository:

- ``expressions.txt`` for ``--replace-text``: name replacements, then the
  address, Cloud Run URL and IBAN rules.
- ``messages.txt`` for ``--replace-message``: the address and URL rules.

Name replacements are the changed runs in the two commits that scrubbed
names (see ``SCRUB_COMMITS``), keeping only runs that no longer occur
anywhere in the current tree. Anything still in the tree is not removed
personal data, and replacing it would change current files. Review
``expressions.txt`` before using it.

``verify`` (run after the rewrite) checks that the rewritten branch has the
same tree as the original and that no address, Cloud Run URL, real IBAN or
replaced name is left in contents or commit messages.

Usage::

    python scripts/build_rewrite_expressions.py build --out ../keystone-rewrite
    python scripts/build_rewrite_expressions.py verify --original . \\
        --rewritten ../keystone-public --expressions ../keystone-rewrite/expressions.txt
"""

from __future__ import annotations

import argparse
import hashlib
import re
import subprocess
import sys
from collections.abc import Iterable
from pathlib import Path

from scripts.check_sensitive_data import _ADDRESS, BLOCKED_ADDRESS_HASHES, normalise_address

# The commits that replaced real names with synthetic ones.
SCRUB_COMMITS = ("01691ed", "f29e7b9")

FICTIONAL_STREET = "Muster"
FICTIONAL_POSTCODE = "10115"
URL_REPLACEMENT = "https://api.example.com"
# Real IBANs become the synthetic form the code uses: zeros plus the last four
# digits. Synthetic IBANs already have that form and map to themselves.
IBAN_RULE = r"regex:\bDE\d{16}(\d{4})\b==>DE0000000000000000\1"

_POSTCODE_SUFFIX = re.compile(r",\s*(\d{5})\s+Berlin")
_RUN_APP_URL = re.compile(r"https?://[\w.-]+\.run\.app")
_IBAN = re.compile(r"DE\d{20}")
_SYNTHETIC_IBAN = re.compile(r"DE0{16}\d{4}")


def _git(repo: Path, *args: str) -> str:
    result = subprocess.run(
        ["git", "-C", str(repo), *args], capture_output=True, text=True, check=True
    )
    return result.stdout


def address_rules(text: str) -> list[str]:
    """Replacement lines for every spelling of a blocked address in ``text``.

    Full addresses with a postcode come first so the postcode is replaced
    too; then each casing of the street name on its own, which also covers
    abbreviations such as ``<street>str.`` without a house number.
    """
    full: set[str] = set()
    stems: set[str] = set()
    for match in _ADDRESS.finditer(text):
        key = normalise_address(match.group(1), match.group(2))
        if hashlib.sha256(key.encode()).hexdigest() not in BLOCKED_ADDRESS_HASHES:
            continue
        stem = match.group(1)
        stems.add(stem)
        suffix = _POSTCODE_SUFFIX.match(text, match.end())
        if suffix:
            full.add(match.group(0) + suffix.group(0))

    def fictional(stem: str) -> str:
        if stem.isupper():
            return FICTIONAL_STREET.upper()
        return FICTIONAL_STREET if stem[0].isupper() else FICTIONAL_STREET.lower()

    rules: list[str] = []
    for variant in sorted(full, key=lambda v: (-len(v), v)):
        replaced = variant
        for stem in stems:
            replaced = replaced.replace(stem, fictional(stem))
        replaced = _POSTCODE_SUFFIX.sub(f", {FICTIONAL_POSTCODE} Berlin", replaced)
        rules.append(f"{variant}==>{replaced}")
    rules.extend(
        f"{stem}==>{fictional(stem)}" for stem in sorted(stems, key=lambda s: (-len(s), s))
    )
    return rules


def url_rules(text: str) -> list[str]:
    """Replace each Cloud Run URL in ``text`` with :data:`URL_REPLACEMENT`."""
    return [f"{url}==>{URL_REPLACEMENT}" for url in sorted(set(_RUN_APP_URL.findall(text)))]


def changed_runs(word_diff: str) -> list[tuple[str, str]]:
    """``(old, new)`` pairs from ``git show --word-diff=porcelain`` output."""
    lines = word_diff.split("\n")
    pairs: list[tuple[str, str]] = []
    for removed, added in zip(lines, lines[1:], strict=False):
        if (
            removed.startswith("-")
            and not removed.startswith("---")
            and added.startswith("+")
            and not added.startswith("+++")
        ):
            old, new = removed[1:].strip(), added[1:].strip()
            if old and new and old != new:
                pairs.append((old, new))
    return pairs


def name_rules(pairs: Iterable[tuple[str, str]], current_tree: str) -> list[str]:
    """Literal rules for runs that are gone from the current tree, longest first.

    Runs containing an IBAN are left to :data:`IBAN_RULE`; runs without a
    letter are not names.
    """
    kept: dict[str, str] = {}
    for old, new in pairs:
        if _IBAN.search(old) or not re.search(r"[^\W\d_]", old) or "==>" in old:
            continue
        if old in current_tree:
            continue
        kept.setdefault(old, new)
    return [f"{old}==>{kept[old]}" for old in sorted(kept, key=lambda o: (-len(o), o))]


def _current_tree_text(repo: Path) -> str:
    parts = []
    for path in _git(repo, "ls-files").splitlines():
        try:
            parts.append((repo / path).read_text(encoding="utf-8"))
        except (UnicodeDecodeError, FileNotFoundError, IsADirectoryError):
            continue
    return "\n".join(parts)


def build(repo: Path, out: Path) -> None:
    if out.resolve().is_relative_to(repo.resolve()):
        raise SystemExit("--out must be outside the repository")
    history = _git(repo, "log", "--all", "-p", "--no-color")
    messages = _git(repo, "log", "--all", "--format=%B")
    pairs = []
    for commit in SCRUB_COMMITS:
        pairs.extend(changed_runs(_git(repo, "show", "--word-diff=porcelain", "--format=", commit)))

    names = name_rules(pairs, _current_tree_text(repo))
    address = address_rules(history + messages)
    urls = url_rules(history + messages)

    out.mkdir(parents=True, exist_ok=True)
    (out / "expressions.txt").write_text(
        "\n".join([*names, *address, *urls, IBAN_RULE]) + "\n", encoding="utf-8"
    )
    (out / "messages.txt").write_text("\n".join([*address, *urls]) + "\n", encoding="utf-8")
    print(
        f"wrote {out / 'expressions.txt'}: {len(names)} name, {len(address)} address, "
        f"{len(urls)} URL and 1 IBAN rule(s); {out / 'messages.txt'}"
    )


def verify(original: Path, rewritten: Path, expressions: Path, branch: str) -> int:
    problems: list[str] = []
    if _git(original, "rev-parse", f"{branch}^{{tree}}") != _git(
        rewritten, "rev-parse", f"{branch}^{{tree}}"
    ):
        problems.append(f"{branch}: tree differs from the original")

    text = _git(rewritten, "log", "--all", "-p", "--no-color") + _git(
        rewritten, "log", "--all", "--format=%B"
    )
    if address_rules(text):
        problems.append("a blocked address is still in the history")
    if _RUN_APP_URL.search(text):
        problems.append("a Cloud Run URL is still in the history")
    real_ibans = [m for m in _IBAN.findall(text) if not _SYNTHETIC_IBAN.fullmatch(m)]
    if real_ibans:
        problems.append(f"{len(real_ibans)} non-synthetic IBAN(s) still in the history")
    olds = [
        line.split("==>", 1)[0]
        for line in expressions.read_text(encoding="utf-8").splitlines()
        if "==>" in line and not line.startswith("regex:")
    ]
    left = sum(1 for old in olds if old in text)
    if left:
        problems.append(f"{left} replaced string(s) still in the history")

    for problem in problems:
        print(problem)
    if not problems:
        print(f"ok: {branch} tree unchanged; no address, URL, real IBAN or replaced string left")
    return 1 if problems else 0


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = parser.add_subparsers(dest="command", required=True)
    b = sub.add_parser("build")
    b.add_argument("--repo", type=Path, default=Path("."))
    b.add_argument("--out", type=Path, required=True)
    v = sub.add_parser("verify")
    v.add_argument("--original", type=Path, required=True)
    v.add_argument("--rewritten", type=Path, required=True)
    v.add_argument("--expressions", type=Path, required=True)
    v.add_argument("--branch", default="main")
    args = parser.parse_args(argv)
    if args.command == "build":
        build(args.repo, args.out)
        return 0
    return verify(args.original, args.rewritten, args.expressions, args.branch)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
