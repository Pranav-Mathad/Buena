# Release checklist: publishing the public repository

The public repository is a **new** GitHub repository holding a rewritten copy
of this history. This repository and its existing remote are not changed by
any step below.

Why a rewrite: the current tree is clean, but older commits still contain the
original dataset's street address, a Cloud Run URL, real IBANs and real
names, from before the commits that replaced them (`b0aa256`, `01691ed`,
`f29e7b9`). The rewrite replaces them in every commit, in file contents and
commit messages, and leaves the final tree byte-identical.

None of these strings is written in this file or anywhere else in the
repository. `scripts/build_rewrite_expressions.py` derives them from the
history, matching the address by the same SHA-256 as the pre-commit check.

Status: prepared, not run. The full procedure was rehearsed on a throwaway
clone on 2026-10-18 (git-filter-repo `a40bce548d2c`): `verify` passed after
the rewrite and failed, as it should, on an unrewritten clone.

## 1. Before the rewrite

Run from the repository root on `cleanup/resume-ready`, with Postgres up
(`docker compose up -d postgres`) and the test DSNs from `.env.example`
exported.

- [ ] The working tree is clean (`git status` shows nothing).
- [ ] Backend checks pass: `ruff check . && ruff format --check . && mypy && pytest`.
- [ ] Frontend checks pass: `cd frontend && npm ci && npm run format:check && npm run lint && npm run typecheck && npm test && npm run build`.
- [ ] `python scripts/check_sensitive_data.py` prints nothing.
- [ ] `git log --all --name-only --pretty=format: | grep "^Extracted/"` prints
      nothing. It printed nothing on 2026-10-18, so the command below has no
      `--path Extracted/ --invert-paths`. If it prints anything now, add
      those two flags to the `git filter-repo` command in step 3.
- [ ] `pip install git-filter-repo`.

## 2. Build the expressions

The output directory must be outside the repository, and its files must not
be committed or pasted anywhere public: they contain the strings being
removed.

```bash
python -m scripts.build_rewrite_expressions build --out ../keystone-rewrite
```

This writes:

- `../keystone-rewrite/expressions.txt` for `--replace-text`: name rules
  first, then the address rules, the Cloud Run URL and one IBAN rule. The
  rehearsal produced 96 name, 3 address, 1 URL and 1 IBAN rule.
- `../keystone-rewrite/messages.txt` for `--replace-message`: the address and
  URL rules.

- [ ] **Review the name rules by hand.** Each line is `old==>new`, taken from
      the changes in the two name-scrubbing commits, and keeps only text that
      no longer occurs in the current tree. Delete any line whose left side
      is not personal data. The order matters: name rules run before the
      address rule, so keep them first.

What the rules cannot cover: a real name that appears in history but was
never part of those two scrub commits. The sweep before `f29e7b9` found no
others, but the list is only as complete as that sweep.

## 3. Rewrite a fresh clone

```bash
git clone --no-local . ../keystone-public
cd ../keystone-public
git filter-repo \
  --replace-text ../keystone-rewrite/expressions.txt \
  --replace-message ../keystone-rewrite/messages.txt
```

`git filter-repo` refuses to run on anything but a fresh clone, rewrites every
branch and tag in it, and removes the `origin` remote so nothing can be pushed
back to this repository by accident.

## 4. Verify

From the original repository:

```bash
python -m scripts.build_rewrite_expressions verify \
  --original . \
  --rewritten ../keystone-public \
  --expressions ../keystone-rewrite/expressions.txt \
  --branch cleanup/resume-ready
```

- [ ] It prints `ok: cleanup/resume-ready tree unchanged; ...`. It fails if
      the rewritten tree differs from the original, or if any address, Cloud
      Run URL, non-synthetic IBAN or replaced string is left in file contents
      or commit messages.

## 5. Point the docs at the new commit hashes

`docs/PROJECT_FACTS.md` and `docs/eval-runs/2026-10-17-emails_v1-rule-based.md`
cite commits by short hash, and every hash changes in the rewrite. In
`../keystone-public`:

```bash
git switch cleanup/resume-ready
python3 - <<'EOF'
import pathlib, re
pairs = [l.split() for l in pathlib.Path(".git/filter-repo/commit-map").read_text().splitlines()[1:]]
short = {old[:7]: new[:7] for old, new in pairs if old != new}
for name in ["docs/PROJECT_FACTS.md", "docs/eval-runs/2026-10-17-emails_v1-rule-based.md"]:
    path = pathlib.Path(name)
    text = path.read_text(encoding="utf-8")
    path.write_text(re.sub(r"\b[0-9a-f]{7}\b", lambda m: short.get(m.group(0), m.group(0)), text), encoding="utf-8")
EOF
git commit -am "docs: point cited commit hashes at the rewritten history"
```

- [ ] Every hash in the diff resolves: `git cat-file -e <hash>^{commit}`.

Run this after step 4: `verify` compares trees, and this commit changes the
tree on purpose.

## 6. Publish

- [ ] Delete the tags in the public clone. They are milestone markers from
      the original timeline (`phase-N-complete`), and 10 of the 11 point at
      commits that are not on the published branch:
      `git tag -l | xargs git tag -d`.
- [ ] Create an empty repository on GitHub (no README, licence or
      `.gitignore`, so the first push is not rejected).
- [ ] Push only the cleaned branch, as `main`:

      ```bash
      git remote add origin git@github.com:<owner>/<new-repo>.git
      git push -u origin cleanup/resume-ready:main
      ```

- [ ] CI passes on the first push (both jobs in `.github/workflows/ci.yml`).
- [ ] Never push the rewritten history to the existing remote, and never
      force-push it there.

## 7. After publishing

- [ ] Decide what happens to the existing public repository. Its history,
      and its remote branches such as `backup-before-rewrite` and
      `phase-*`, still contain the strings removed above. Making it private
      or archiving it removes public access. Deleting it is the only way to
      remove the data from GitHub, and forks or cached copies are outside
      this repository's control.
- [ ] Contributors install the hooks once: `pip install -e ".[dev]" && pre-commit install`.
