# GAP-28.0 — CI Foundation & Bundle Sync Gate

| Field | Value |
| :--- | :--- |
| **Spec ID** | GAP-28.0 |
| **Epic** | [GAP-28 — Enterprise Field Readiness](README.md) |
| **Status** | `SHIPPED` |
| **Module** | General UI / UX |
| **Target Release** | v1.4.4 |
| **Depends On** | — |
| **Blocks** | [GAP-28.3](GAP-28.3-dual-theme.md), [GAP-28.5](GAP-28.5-windows-launchers.md) |
| **Estimated Effort** | M — 2-3 days |
| **Owner** | _unassigned_ |
| **Last Updated** | September 16, 2026 |

---

## 1. Problem

The repository has **29 test modules and no continuous integration**. Nothing runs them on push.

Two consequences block the rest of this epic:

1. **We cannot claim Windows support.** GAP-28.5 promises first-class Windows launchers, but with no
   `windows-latest` runner, the first contributor change silently breaks them and nobody notices until a
   customer reports it.
2. **We cannot verify a 753-site theme sweep.** GAP-28.3 rewrites color literals across two duplicated
   frontend bundles. Manual visual QA cannot cover views × themes × bundles.

Separately, the frontend is shipped **twice** and nothing enforces that the copies agree.

## 2. Evidence

| Claim | Evidence |
| :--- | :--- |
| No CI exists | `.github/` contains only `ISSUE_TEMPLATE/` (5 files) — no `workflows/` directory |
| 29 test modules exist and are unrun by automation | `tests/` — `pytest.ini` present, `requirements-dev.txt` = `pytest>=7.0`, `pytest-cov>=4.0` |
| `docs/static/*` are byte-identical copies | `diff static/style.css docs/static/style.css` → identical; `app.js` identical (417,155 bytes each) |
| The Pages simulator consumes the copy | `docs/simulator.html:371` → `<link rel="stylesheet" href="./static/style.css">` |
| Sync-enforcement precedent already exists | `tests/test_batch_candidates.py:139` → `BUNDLES = ["static/app.js", "docs/static/app.js"]` |

## 3. Goals / Non-Goals

**Goals**
* Run the existing suite on every push and PR, on Linux **and** Windows.
* Make `static/` ↔ `docs/static/` divergence a hard CI failure.
* Provide a one-command sync script so the gate is trivial to satisfy.
* Stand up the Playwright + contrast jobs that GAP-28.3 depends on (wired but skipped until the light theme exists).

**Non-Goals**
* Release automation, publishing, or container builds.
* Coverage thresholds or lint gates (worth doing, separate spec).
* Rewriting any existing test.

## 4. Design

### 4.1 Workflow

`.github/workflows/ci.yml`, triggered on `push` and `pull_request`:

| Job | Runner | Purpose |
| :--- | :--- | :--- |
| `test` | `ubuntu-latest` | `pytest` + coverage report |
| `bundle-sync` | `ubuntu-latest` | `static/` vs `docs/static/` byte identity |
| `windows` | `windows-latest` | `pytest` + `run.bat` smoke test (skipped until GAP-28.5 lands) |
| `visual` | `ubuntu-latest` | Playwright dual-theme snapshots (skipped until GAP-28.3 lands) |
| `contrast` | `ubuntu-latest` | WCAG AA assertion over computed styles (skipped until GAP-28.3 lands) |

`visual`, `contrast` and `windows` ship in this spec as **defined but conditionally skipped** jobs so the
dependent specs only have to flip a flag rather than author CI from scratch.

> [!NOTE]
> **Superseded.** This spec originally required every CI job to export `AUTH_ENFORCED_UPSTREAM=true`,
> because `src/main.py` raised `RuntimeError` and refused to boot without it. That startup guard has
> been removed: the app now starts with no environment variable at all, and `_warn_if_publicly_bound()`
> (`src/main.py:204-248`) emits a warning when bound off-loopback instead of blocking. **CI needs no
> auth-related configuration.**

### 4.2 Bundle sync

`scripts/sync_docs_bundle.sh`:

```bash
#!/usr/bin/env bash
# Mirrors the canonical frontend bundle into the GitHub Pages copy.
set -euo pipefail
cd "$(dirname "$0")/.."

FILES=("app.js" "style.css" "report.css")
for f in "${FILES[@]}"; do
  cp "static/$f" "docs/static/$f"
  echo "synced static/$f → docs/static/$f"
done
```

`tests/test_bundle_sync.py` asserts SHA-256 equality for each file and names the script in the failure
message. This generalizes the per-feature `BUNDLES` check already in `tests/test_batch_candidates.py`.

### 4.3 New dependencies

Playwright and `pytest-playwright` are **new dependencies**.

> [!CAUTION]
> The dependency-scan gate must be run and its result recorded in the PR description **before** either
> package is added to `requirements-dev.txt`. Do not add the import first.

Browser binaries are cached by CI key (`~/.cache/ms-playwright`) to keep the `visual` job under ~2 minutes.

## 5. Implementation Tasks

- [ ] Run the dependency-scan gate for `playwright` and `pytest-playwright`; paste the result into the PR.
- [ ] Add both to `requirements-dev.txt` with pinned floors, matching the existing `>=x,<y` style of `requirements.txt`.
- [ ] Create `scripts/sync_docs_bundle.sh` (executable) and document it in `README.md` under contributing.
- [ ] Create `tests/test_bundle_sync.py` (SHA-256 equality for `app.js`, `style.css`, `report.css`).
- [ ] Create `.github/workflows/ci.yml` with the five jobs; gate `visual` / `contrast` / `windows` behind a skip condition.
- [ ] Cache pip and Playwright browsers.
- [ ] Add a CI status badge to `README.md`.
- [ ] Verify the suite is actually green on `windows-latest` (expect path/encoding surprises — record any failures as input to GAP-28.5).

## 6. Acceptance Criteria

1. A push with a modified `static/app.js` and an unmodified `docs/static/app.js` **fails** `bundle-sync`, and the failure message names `scripts/sync_docs_bundle.sh`.
2. Running `scripts/sync_docs_bundle.sh` makes that same push pass.
3. `pytest` passes on `ubuntu-latest` and on `windows-latest` (or known Windows failures are documented in GAP-28.5 rather than silently skipped).
4. The `visual`, `contrast` and `windows` jobs are present in the workflow file and skip cleanly with an explicit reason.
5. Total CI wall-clock for a typical PR is under 5 minutes.
6. The dependency-scan result for Playwright is recorded in the PR description.

## 7. Risks & Mitigations

| Risk | Mitigation |
| :--- | :--- |
| The existing suite is red on Windows (untested there, ever) | Run it locally on `windows-latest` first; if red, land the Linux jobs and open a blocker task against GAP-28.5 rather than disabling the job |
| Playwright browser download makes CI slow/flaky | Cache by OS + Playwright version; pin the version |
| `bundle-sync` becomes an annoying gate contributors work around | Ship the one-command script *with* the gate and mention it in the failure output |

## 8. Rollback

Delete `.github/workflows/ci.yml`. No runtime code is touched, so there is no product impact — worst case
we are back to today's state of no CI.

## 9. Open Questions

| # | Question | Default |
| :--- | :--- | :--- |
| 1 | Should `bundle-sync` auto-commit the sync instead of failing? | No — auto-commits from CI surprise contributors; fail loudly. |
| 2 | Add a coverage floor now? | No — out of scope; propose separately once a baseline is measured. |
