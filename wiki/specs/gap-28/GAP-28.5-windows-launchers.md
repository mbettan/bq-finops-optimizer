# GAP-28.5 — Native Windows Launchers & Cross-Platform Hardening

| Field | Value |
| :--- | :--- |
| **Spec ID** | GAP-28.5 |
| **Epic** | [GAP-28 — Enterprise Field Readiness](README.md) |
| **Status** | `SHIPPED` |
| **Module** | General UI / UX |
| **Target Release** | v1.4.4 |
| **Depends On** | [GAP-28.0 — CI Foundation](GAP-28.0-ci-foundation.md) *(hard: the `windows-latest` job)* |
| **Blocks** | — |
| **Estimated Effort** | S — 1-2 days |
| **Owner** | _unassigned_ |
| **Last Updated** | September 16, 2026 |

---

## 1. Problem

Windows evaluators hit friction before the product does anything: no Python on `PATH`, no venv, PowerShell
execution policy blocks, and the app refuses to start without an environment variable they have not read
about yet. The evaluation ends at minute two.

> [!NOTE]
> **This is the convenience path, not the critical path.** Google Cloud Shell shipped in v1.4.4 (deployment
> Option 2) already solves Windows onboarding with zero code — standard Linux, one-click badge, native
> HTTPS Web Preview on port 8080. This spec is sequenced **last** in the epic for that reason.

## 2. Evidence

| Claim | Evidence |
| :--- | :--- |
| The app starts with no env var required | (Superseded) Previously raised `RuntimeError` without `AUTH_ENFORCED_UPSTREAM`; that guard has been removed. `_warn_if_publicly_bound()` (`src/main.py:204-248`) now warns instead of blocking. |
| A startup check is an attestation, not a control | `src/main.py:159-173` — the real boundary is Cloud Run IAM or IAP, never anything this process can assert about itself |
| Cross-platform file handling risk is **small and measurable** | `src/` contains **4** `open()` calls without `encoding=` and **6** `os.path` usages |
| No launcher exists for any platform | Repo root has no `run.*` script |
| Windows has never been tested | No CI at all prior to GAP-28.0 |

## 3. Goals / Non-Goals

**Goals**
* Double-click-to-run on Windows: detect Python, build the venv, install deps, start the server, open the browser.
* Fail with human instructions, never a stack trace.
* Keep the security posture honest and bounded.
* Eliminate the measurable Windows encoding/path hazards.

**Non-Goals**
* Supporting shared/server Windows deployment (Cloud Run is the answer — see §4.3).
* An installer, packaged binary, or Windows service.
* Supporting environments where GPO enforces `AllSigned` (documented as Cloud Shell / `run.bat` territory).

## 4. Design

### 4.1 `run.bat` — Command Prompt launcher

Behavior: detect `python` → fall back to `py -3` → build `venv` if absent → install deps **only when
changed** → set the guardrail → poll for readiness → open the browser → run uvicorn on loopback.

Bugs to avoid — each of these was present in an earlier draft:

| Bug | Fix |
| :--- | :--- |
| `%errorlevel%` inside a parenthesised `if/else` under `setlocal enabledelayedexpansion` is expanded at **parse time**, so the `py` fallback branch tests a stale value | Use `!errorlevel!` |
| `start http://127.0.0.1:8080` fires before uvicorn is listening — first impression is a browser error page during a 30-60 s cold venv build | Poll `/` up to 30 × 1 s, then open |
| `pip install -r requirements.txt` on every launch costs 5-20 s and hard-fails behind corporate proxies / offline | Gate on a `venv\.deps.sha256` sentinel; if install fails but `uvicorn` already imports, warn and continue |

```bat
@echo off
setlocal enabledelayedexpansion

:: 1. Locate Python
where python >nul 2>nul
if !errorlevel! equ 0 (
    set "PY_CMD=python"
) else (
    where py >nul 2>nul
    if !errorlevel! equ 0 (
        set "PY_CMD=py -3"
    ) else (
        echo [ERROR] Python 3 was not found on your PATH.
        echo Install Python 3.10+ from https://www.python.org/downloads/
        echo IMPORTANT: tick "Add Python to PATH" during installation.
        echo.
        echo No install rights? Use Google Cloud Shell instead - see README Option 2.
        pause
        exit /b 1
    )
)
:: 2-6. venv, hashed deps install, readiness poll, browser, uvicorn on 127.0.0.1
```

### 4.2 `run.ps1` — PowerShell launcher

| Bug | Fix |
| :--- | :--- |
| `& ".\venv\Scripts\Activate.ps1"` runs in a **child scope**, so the venv is never actually activated | Skip activation entirely — call `.\venv\Scripts\python.exe -m uvicorn …` directly. Immune to scope *and* policy issues |
| `Set-ExecutionPolicy -Scope Process` **inside** the script is chicken-and-egg: if policy blocks scripts, the line never executes | Remove it. Document `powershell -ExecutionPolicy Bypass -File .\run.ps1` in the README |
| `Set-StrictMode` + `$ErrorActionPreference = "Stop"` abort on benign `pip` stderr warnings | Do not wrap `pip` in strict mode |

The README must state plainly: in GPO-enforced `AllSigned` environments, `run.ps1` will not run — use
`run.bat` or Cloud Shell.

### 4.3 Security guardrail with bounded blast radius

(Superseded) Both launchers used to set `AUTH_ENFORCED_UPSTREAM=true` in order to start. That flag has
been removed and the app now boots with no environment variable at all, so the launchers no longer set
anything. The guardrail is therefore **entirely** the launcher's responsibility: make the boundary real
and the limitation loud.

* **Hard-pin** `--host 127.0.0.1`. Never `0.0.0.0`. This is now the only enforced control — binding
  elsewhere merely produces a warning from `_warn_if_publicly_bound()`.
* Print, every launch:

```
[NOTICE] LOCAL LOOPBACK USE ONLY (127.0.0.1).
         This app has no built-in authentication. Do NOT run these launchers on a
         shared host, VM, or server. For multi-user access, deploy to Cloud Run
         with --no-allow-unauthenticated.
```

* The README Windows section repeats the same warning.

### 4.4 Cross-platform hardening (right-sized)

Measured, not assumed: **4** `open()` calls without `encoding=` and **6** `os.path` usages in `src/`. This
is a one-commit fix — explicit `encoding="utf-8"` (prevents CP-1252 crashes on logs, SQL and JSON
snapshots) and `pathlib.Path` for the log path. It is **not** a refactor phase.

## 5. Implementation Tasks

- [ ] **Prereq:** GAP-28.0 merged; the `windows-latest` job exists and its result on the current suite is known.
- [ ] Write `run.bat` (`!errorlevel!`, deps sentinel, readiness poll, loopback pin, notice banner).
- [ ] Write `run.ps1` (direct interpreter invocation, no in-script policy change, no strict mode around pip).
- [ ] Add `encoding="utf-8"` to the 4 bare `open()` calls; convert the 6 `os.path` usages to `pathlib.Path`.
- [ ] Enable the `windows-latest` CI job: cold clone → `run.bat` → HTTP 200 on `/` → clean shutdown.
- [ ] Add a README Windows section: both launchers, the `-ExecutionPolicy Bypass -File` invocation, the `AllSigned` caveat, the loopback warning, and a pointer to Cloud Shell as the zero-setup alternative.
- [ ] Add `venv/.deps.sha256` to `.gitignore` if not already covered by `venv/`.
- [ ] `RELEASE_NOTES.md` entry.

## 6. Acceptance Criteria

1. On a clean Windows machine with Python on `PATH`, double-clicking `run.bat` reaches a working UI with no manual steps.
2. With Python absent, `run.bat` prints install instructions **and** the Cloud Shell alternative, then exits non-zero. No stack trace.
3. Neither launcher opens the browser before the server answers on `/`.
4. A second launch with unchanged `requirements.txt` skips the pip step (measurably faster).
5. With no network, a launch on an already-provisioned venv still starts the server.
6. `run.ps1` starts the server on a machine with `RemoteSigned` policy using the documented invocation.
7. Both launchers bind `127.0.0.1` only — verified by asserting a connection from another host fails.
8. The loopback notice is printed on every launch by both launchers.
9. `windows-latest` CI job is green: cold clone → `run.bat` → HTTP 200 → shutdown.
10. `pytest` passes on `windows-latest`.

## 7. Risks & Mitigations

| Risk | Mitigation |
| :--- | :--- |
| Someone copies `run.bat` onto a shared VM and exposes an unauthenticated app | Hard-pinned loopback bind + per-launch notice + README warning (§4.3) |
| Corporate proxy blocks PyPI, so the venv never provisions | Non-fatal install failure when the venv already works; error text points to Cloud Shell |
| GPO `AllSigned` blocks `run.ps1` entirely | Documented; `run.bat` and Cloud Shell are the supported paths |
| The existing test suite is red on Windows | Discovered in GAP-28.0; fixes land here with the encoding/path work |
| Launchers rot without maintenance | The `windows-latest` smoke test is the contract |

## 8. Rollback

Delete `run.bat` / `run.ps1` and disable the `windows` CI job. The UTF-8 and `pathlib` fixes should be
kept regardless — they are correctness improvements on every platform.

## 9. Open Questions

| # | Question | Default |
| :--- | :--- | :--- |
| 1 | Add a matching `run.sh` for macOS/Linux parity? | Yes if trivial — same readiness poll and notice, ~30 lines. |
| 2 | Auto-open the browser at all, or just print the URL? | Auto-open after the readiness poll; print the URL as well. |
| 3 | Should the launchers offer to install Python via `winget`? | No — silent toolchain installs are exactly what corporate policy forbids. |
