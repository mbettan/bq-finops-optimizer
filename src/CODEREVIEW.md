# Senior Code Review — `bigquery-finops`

**Reviewer perspective:** Senior Software Engineer (Python / SQL / BigQuery)
**Review date:** 2026-07-25
**Scope reviewed:** `src/main.py` (3,946 lines), `src/utils.py` (374), `src/fluid_scaling.py` (575), `src/hbo.py` (436), `src/cost_attribution.py` (280), `static/app.js`, `static/index.html`, `Dockerfile`, `tests/`
**Review weighting:** SQL / BigQuery — query construction, cost controls, partitioning & clustering, `INFORMATION_SCHEMA` usage, billing accuracy.

---

## Table of Contents

- [0. Executive Summary](#0-executive-summary)
- [1. What This Codebase Gets Right](#1-what-this-codebase-gets-right)
- [2. Findings Index](#2-findings-index)
- [3. Critical Findings](#3-critical-findings)
  - [F1 — Unbounded `JOBS_BY_ORGANIZATION` scans](#f1--unbounded-jobs_by_organization-scans-on-two-endpoints)
  - [F2 — `total_bytes_billed` summed across per-second rows](#f2--total_bytes_billed-summed-across-per-second-timeline-rows)
  - [F3 — Cost attribution silently drops unconfigured reservations](#f3--cost-attribution-silently-drops-unconfigured-reservations)
- [4. High Findings](#4-high-findings)
  - [F4 — Governance expiration audit is silently project-scoped](#f4--governance-expiration-audit-is-silently-project-scoped)
  - [F5 — `PARTITIONS` misread; unpartitioned tables flagged](#f5--partitions-misread-unpartitioned-tables-flagged-as-violations)
  - [F6 — Permanent `BadRequest` retried 5× behind a misleading message](#f6--permanent-badrequest-retried-5-behind-a-misleading-error-message)
  - [F7 — Chargeback config written to the container filesystem](#f7--chargeback-config-written-to-the-container-filesystem)
- [5. Medium Findings](#5-medium-findings)
  - [F8 — Tiered recommendations omit job-type filters](#f8--tiered-recommendations-omit-the-job-type-filters-every-sibling-query-has)
  - [F9 — Fluid-scaling fallback hardcodes `borrowed_slots = 0`](#f9--fluid-scaling-capacity-fallback-hardcodes-borrowed_slots--0)
  - [F10 — `fairness_enabled` is last-writer-wins](#f10--fairness_enabled-is-last-writer-wins-across-admin-projects)
  - [F11 — HBO hardcodes $0.06/slot-hour](#f11--hbo-hardcodes-006slot-hour)
  - [F12 — HBO analyze and summary report on different bases](#f12--hbo-analyze-and-summary-report-on-different-bases-and-top-n-is-biased)
  - [F13 — `HBOAnalyzeParams.limit` is unbounded](#f13--hboanalyzeparamslimit-is-unbounded)
  - [F14 — `audit_type` silently ignored; both audits run per click](#f14--audit_type-is-silently-ignored-so-both-governance-audits-run-on-every-click)
- [6. Low Findings & Hygiene](#6-low-findings--hygiene)
  - [F15 — Sanitization by mutation-at-a-distance](#f15--sanitization-by-mutation-at-a-distance)
  - [F16 — `_run_and_log` duplicated four times](#f16--_run_and_log-duplicated-four-times)
  - [F17 — Dead code](#f17--dead-code)
  - [F18 — Stale comment in `fluid_scaling.py`](#f18--stale-comment-misdescribes-the-unified-query)
  - [F19 — Unguarded `FULL OUTER JOIN` fan-out](#f19--unguarded-full-outer-join-fan-out-verify-first)
  - [F20 — Cost attribution window semantics](#f20--cost-attribution-window-semantics-are-undocumented)
  - [F21 — `get_anomalies()` returns fabricated data](#f21--get_anomalies-returns-fabricated-data-to-a-production-ui)
- [7. Test Coverage Gap](#7-test-coverage-gap)
- [8. Recommended Remediation Order](#8-recommended-remediation-order)
- [9. Cross-Cutting Constraints](#9-cross-cutting-constraints-read-before-editing)
- [10. Revision Log](#10-revision-log)

---

## 0. Executive Summary

This is a well-built tool. The SQL is more disciplined than most FinOps tooling — parameterized `IN UNNEST(@focus_projects)`, an identifier allow-list, `maximum_bytes_billed` on every job, and `retry=None` to defeat the SDK's 10-minute retry are all correct instincts that most codebases in this space get wrong.

The problems cluster in two places, both of which matter disproportionately for this particular product:

1. **Cost control on its own queries.** A tool whose purpose is telling you what BigQuery costs must not itself be the expensive query. Two endpoints scan an organization's full 180-day job history on every click (**F1**).

2. **Billing accuracy.** Several numbers the UI presents do not mean what the label says: bytes inflated by job duration (**F2**), chargeback totals that silently fail to reconcile against the invoice (**F3**), an "org-wide" audit that is project-scoped (**F4**), savings figures priced at list rate while every sibling module honours committed-use discounts (**F11**).

Nothing here is architectural rot. Every finding below is a contained fix. The single highest-value structural change is **F16** (deduplicate `_run_and_log`), because four copies of the query-execution path is what allowed **F1** and **F8** to drift from house standards unnoticed.

**Security note:** the SQL injection surface is genuinely closed. I verified that all 26 `@app.post` handlers call `_validate_safe_params`, and `tests/test_security.py` parameterizes 23 endpoints against a backtick-injection payload. **F15** reclassifies the remaining concern from *vulnerability* to *architectural fragility* — the guard is correct but structurally skippable.

---

## 1. What This Codebase Gets Right

Worth stating explicitly, because these should not be regressed while fixing the findings below.

| Practice | Where | Why it matters |
|---|---|---|
| Focus filter is parameterized, never interpolated | [`build_project_filter`](src/utils.py#L130) | `ArrayQueryParameter` + `IN UNNEST(@focus_projects)` is the correct BigQuery idiom. String-building this list is the #1 injection vector in FinOps tools. |
| Identifier allow-list on every interpolated identifier | [`_safe_ident`](src/utils.py#L164), `_IDENT_RE` | Project/dataset/region names cannot be backticked into SQL. Enforced by regex, not escaping. |
| `maximum_bytes_billed` on every single job | [`get_max_bytes_billed`](src/utils.py#L352) | Clamped to [1 GiB, 10 TiB]. This is the difference between a bug and an incident. |
| SDK retry defeated deliberately | [`run_query_with_retry_limit`](src/utils.py#L299) | `retry=None, job_retry=None` — the default 10-minute `job_retry` is a classic source of runaway BigQuery spend. |
| Scope map derived from the type system | [`get_scope_map`](src/main.py#L226) | `/api/meta/scope-map` reflects over `model_fields` rather than a hand-maintained JS map. Cannot drift. |
| `OrgParams` uses `extra="forbid"` | [`utils.py:78`](src/utils.py#L78) | Contract enforced by the type system rather than imperative checks. |
| Request-ID correlation | `request_id_var` contextvar + logging filter | Error messages reference an ID instead of leaking BigQuery internals. Propagated into worker threads in `check_hbo_status`. |
| Clickable BQ Console URL per query | all four `_run_and_log` copies | Every query is traceable to a job in the console with bytes processed/billed logged. Excellent operational hygiene. |
| `GROUPING SETS` to avoid a second scan | [`analyze_slots`](src/main.py#L2318) | Computes per-reservation *and* org-wide per-second totals in one base-table pass. |
| Physical-byte decomposition tested | `tests/test_physical_bytes_decomposition.py` | Time-travel/fail-safe byte accounting is subtle and correctly handled. |

---

## 2. Findings Index

| ID | Severity | Title | Primary file |
|---|---|---|---|
| [F1](#f1--unbounded-jobs_by_organization-scans-on-two-endpoints) | 🔴 Critical | Unbounded `JOBS_BY_ORGANIZATION` scans on two endpoints | `src/main.py:2216`, `:2261` |
| [F2](#f2--total_bytes_billed-summed-across-per-second-timeline-rows) | 🔴 Critical | `total_bytes_billed` summed across per-second rows | `src/main.py:2606` |
| [F3](#f3--cost-attribution-silently-drops-unconfigured-reservations) | 🔴 Critical | Cost attribution silently drops unconfigured reservations | `src/cost_attribution.py:199` |
| [F4](#f4--governance-expiration-audit-is-silently-project-scoped) | 🟠 High | Governance expiration audit is silently project-scoped | `src/main.py:2110` |
| [F5](#f5--partitions-misread-unpartitioned-tables-flagged-as-violations) | 🟠 High | `PARTITIONS` misread; unpartitioned tables flagged | `src/main.py:2162` |
| [F6](#f6--permanent-badrequest-retried-5-behind-a-misleading-error-message) | 🟠 High | Permanent `BadRequest` retried 5× behind a misleading message | `src/utils.py:329` |
| [F7](#f7--chargeback-config-written-to-the-container-filesystem) | 🟠 High | Chargeback config written to the container filesystem | `src/cost_attribution.py:19` |
| [F8](#f8--tiered-recommendations-omit-the-job-type-filters-every-sibling-query-has) | 🟡 Medium | Tiered recommendations omit job-type filters | `src/main.py:2510` |
| [F9](#f9--fluid-scaling-capacity-fallback-hardcodes-borrowed_slots--0) | 🟡 Medium | Fluid-scaling fallback hardcodes `borrowed_slots = 0` | `src/fluid_scaling.py:287` |
| [F10](#f10--fairness_enabled-is-last-writer-wins-across-admin-projects) | 🟡 Medium | `fairness_enabled` is last-writer-wins | `src/main.py:2406` |
| [F11](#f11--hbo-hardcodes-006slot-hour) | 🟡 Medium | HBO hardcodes $0.06/slot-hour | `src/hbo.py:129`, `:205` |
| [F12](#f12--hbo-analyze-and-summary-report-on-different-bases-and-top-n-is-biased) | 🟡 Medium | HBO analyze/summary basis mismatch; biased top-N | `src/hbo.py:111` |
| [F13](#f13--hboanalyzeparamslimit-is-unbounded) | 🟡 Medium | `HBOAnalyzeParams.limit` is unbounded | `src/hbo.py:52` |
| [F14](#f14--audit_type-is-silently-ignored-so-both-governance-audits-run-on-every-click) | 🟡 Medium | `audit_type` ignored; both audits run per click | `src/main.py:2078` |
| [F15](#f15--sanitization-by-mutation-at-a-distance) | 🟢 Low | Sanitization by mutation-at-a-distance | `src/main.py:2795` |
| [F16](#f16--_run_and_log-duplicated-four-times) | 🟢 Low | `_run_and_log` duplicated four times | 4 files |
| [F17](#f17--dead-code) | 🟢 Low | Dead code | `src/main.py:2365`, `cost_attribution.py:143` |
| [F18](#f18--stale-comment-misdescribes-the-unified-query) | 🟢 Low | Stale comment misdescribes the unified query | `src/fluid_scaling.py:260` |
| [F19](#f19--unguarded-full-outer-join-fan-out-verify-first) | 🟢 Low | Unguarded `FULL OUTER JOIN` fan-out | `src/fluid_scaling.py:329` |
| [F20](#f20--cost-attribution-window-semantics-are-undocumented) | 🟢 Low | Cost attribution window semantics undocumented | `src/cost_attribution.py:159` |
| [F21](#f21--get_anomalies-returns-fabricated-data-to-a-production-ui) | 🟢 Low | `get_anomalies()` returns fabricated data | `src/main.py:3912` |

---

## 3. Critical Findings

### F1 — Unbounded `JOBS_BY_ORGANIZATION` scans on two endpoints

**Severity:** 🔴 Critical — direct, repeatable cost exposure
**Files:** [`src/main.py:2216-2253`](src/main.py#L2216), [`src/main.py:2261-2295`](src/main.py#L2261)
**UI surface:** *Anti-Patterns* tab → "Materialized View Rejections" and "Proactive Resource Warnings" panels

#### The problem

Both endpoints query `INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION` with **no predicate on `creation_time`**.

`/api/mv/analyze`:

```sql
SELECT job_id, user_email, mv.table_reference.table_id AS mv_name,
       mv.chosen, mv.rejected_reason
FROM `{target_project}`.`{params.region}`.INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION,
UNNEST(materialized_view_statistics.materialized_view) AS mv
WHERE mv.chosen = false
  {focus_clause}
LIMIT 50
```

`/api/resource_warnings/analyze`:

```sql
SELECT job_id, user_email, query_info.resource_warning
FROM `{target_project}`.`{params.region}`.INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION
WHERE query_info.resource_warning IS NOT NULL
  {focus_clause}
ORDER BY creation_time DESC
LIMIT 50
```

#### Why this is wrong

`JOBS_BY_ORGANIZATION` is a **partitioned view over `creation_time` with 180-day retention**. BigQuery prunes partitions only when the query carries a `creation_time` predicate. Without one, every partition is read — the organization's complete 180-day job history — on every button click.

Three specific consequences:

**(a) `LIMIT` does not bound cost.** `LIMIT 50` is applied *after* the scan, during result materialization. It caps rows returned, not bytes read. `ORDER BY` is worse: `ORDER BY creation_time DESC` *guarantees* a full scan plus a global sort, because the engine must see every row to know which 50 are newest. The presence of `LIMIT 50` makes these queries look bounded when they are not — this is almost certainly why the bug survived review.

**(b) The 200 GiB safety cap converts this into a hard failure.** Every query runs under `maximum_bytes_billed` (default 200 GiB, [`utils.py:351`](src/utils.py#L351)). On any organization large enough to exceed that in 180 days of job metadata, these two panels **never work** — they raise `BadRequest`, which [`handle_endpoint_exception`](src/utils.py#L262) masks as a generic *"BigQuery rejected the request (invalid parameters or malformed query)"*. The user sees a misleading error with no mention of the cap. Below the cap, they get a bill instead. See **F6**, which compounds this.

**(c) `/api/mv/analyze` is additionally non-deterministic.** It has `LIMIT 50` with **no `ORDER BY`**. BigQuery guarantees no ordering, so the panel shows an arbitrary 50 of however many thousand MV rejections exist across 180 days — a *different* arbitrary 50 on each run, potentially all five months old. The panel is not just expensive; it is not answering the question the UI claims it answers.

#### Evidence this is unintentional

Every other `JOBS_*` query in this codebase carries a lookback window. `grep -n "lookback_days" src/main.py` returns **20+ call sites**, all identical in shape:

```python
lookback_days: int = Field(default=7, ge=1, le=90)
...
WHERE creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {params.lookback_days} DAY)
```

These two endpoints are the only `JOBS_BY_ORGANIZATION` readers in the file without one. The cause is visible in the type signature: both take `GovernanceParams` ([`main.py:2078`](src/main.py#L2078)), a model written for the *dataset-metadata* auditor, which queries `SCHEMATA` / `TABLE_OPTIONS` / `PARTITIONS` where no time dimension exists. Two job-history endpoints were later attached to that model and inherited its lack of a time window.

**The misuse of a shared model is the root cause. The fix must address that, not just patch two SQL strings.**

#### The fix

##### Step 1 — Add a dedicated params model

**File:** `src/main.py`, immediately after the `GovernanceParams` class body (currently ends line 2081).

```python
class GovernanceParams(FocusMixin):
    org_project_id: Optional[str] = None
    region: str = "region-us"
    max_bytes_billed_gb: Optional[int] = None


class JobGovernanceParams(GovernanceParams):
    """Governance checks that read INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION.

    That view is partitioned on creation_time with 180-day retention. Without a
    creation_time predicate BigQuery cannot prune partitions and every run scans
    the org's full job history. LIMIT is applied after the scan and does not
    bound bytes read, so the window is mandatory rather than a convenience.

    extra='forbid' is safe here (unlike on GovernanceParams, whose callers send
    an ignored audit_type field — see F14) and makes frontend/backend version
    skew fail loudly instead of silently reverting to an unbounded scan.
    """
    model_config = ConfigDict(extra="forbid")

    lookback_days: int = Field(default=30, ge=1, le=90)
```

**Import check:** `Field` is already imported at [`main.py:5`](src/main.py#L5). `ConfigDict` is **not** — add it:

```python
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
```

**On the chosen bounds:** `default=30` rather than the house default of 7, because MV rejections and resource warnings are rare events — a 7-day window will frequently render an empty table and read as "broken". `le=90` matches every other model in the file. The view retains 180 days, so raising the ceiling later is safe.

##### Step 2 — Fix `/api/mv/analyze`

Change the signature at [`main.py:2217`](src/main.py#L2217):

```python
def analyze_mv_rejections(params: JobGovernanceParams):
```

**Before:**

```
        WHERE mv.chosen = false
          {focus_clause}
        LIMIT 50
```

**After:**

```
        WHERE creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {params.lookback_days} DAY)
          AND mv.chosen = false
          {focus_clause}
        ORDER BY creation_time DESC
        LIMIT 50
```

Both changes are required. The `creation_time` predicate leads the `WHERE` clause to match house style; the added `ORDER BY` fixes the non-determinism from (c) and aligns this endpoint with its sibling.

##### Step 3 — Fix `/api/resource_warnings/analyze`

Change the signature at [`main.py:2262`](src/main.py#L2262):

```python
def analyze_resource_warnings(params: JobGovernanceParams):
```

**Before:**

```
        WHERE query_info.resource_warning IS NOT NULL
          {focus_clause}
        ORDER BY creation_time DESC
        LIMIT 50
```

**After:**

```
        WHERE creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {params.lookback_days} DAY)
          AND query_info.resource_warning IS NOT NULL
          {focus_clause}
        ORDER BY creation_time DESC
        LIMIT 50
```

##### Step 4 — Send the parameter from the frontend

**Files:** `static/app.js` **and** `docs/static/app.js`.

> ⚠️ `docs/static/app.js` is **byte-identical** to `static/app.js` (verified with `diff -q`). It backs the GitHub Pages demo. Apply identical edits to both or they drift.

In the `btnAnalyzeMvRejections` handler ([`app.js:3830`](static/app.js#L3830)) and the `btnAnalyzeWarnings` handler ([`app.js:3864`](static/app.js#L3864)):

```javascript
            const params = {
                org_project_id: state.orgProject,
                max_bytes_billed_gb: state.maxBytesBilledGb,
                region: state.region,
                focus_projects: state.focusProjects,
                lookback_days: 30
            };
```

This matches the established pattern for this tab — the sibling DML panel hardcodes `lookback_days: 1` at [`app.js:3609`](static/app.js#L3609) with no UI control. **Do not add a lookback input**; that is a separate UX decision.

##### Step 5 — Update user-facing copy

**File:** `static/index.html`. Without this, the row-count drop on `/api/mv/analyze` will be reported as a regression.

[`index.html:2219`](static/index.html#L2219) — **Before:**
> This table lists queries where a Materialized View was considered but rejected by the optimizer.

**After:**
> This table lists queries from the last 30 days where a Materialized View was considered but rejected by the optimizer. Showing the 50 most recent.

[`index.html:2244`](static/index.html#L2244) — **Before:**
> This table lists queries that triggered resource warnings (e.g., approaching limits).

**After:**
> This table lists queries from the last 30 days that triggered resource warnings (e.g., approaching limits). Showing the 50 most recent.

#### Critical constraints

> **⛔ Do NOT add `lookback_days` to `GovernanceParams` itself.** It is shared with `/api/governance/analyze`, which queries only dataset metadata. A time window there is meaningless and would pollute a public API contract.

> **⛔ Do NOT add `extra="forbid"` to `GovernanceParams`.** This looks like a tempting hardening step and **would break the app**. The frontend sends `audit_type: 'expiration'` / `'filter'` to `/api/governance/analyze` ([`app.js:3751`](static/app.js#L3751), [`app.js:3793`](static/app.js#L3793)) but the model has no such field. It is currently accepted and discarded because `FocusMixin` ([`utils.py:72`](src/utils.py#L72)) does not forbid extras. Strict extras would 422 both governance buttons. See **F14**. Applying `extra="forbid"` to the **new subclass only** is safe and recommended.

> **⚠️ Interpolate the integer; do not parameterize it.** Use `INTERVAL {params.lookback_days} DAY`, matching the 20+ existing sites — not `INTERVAL @lookback_days DAY`. Two reasons: consistency (only [`main.py:3029`](src/main.py#L3029) uses the parameterized form), and more importantly, partition pruning on `INFORMATION_SCHEMA` views is most reliable with constant expressions; a query parameter risks degrading the pruning that is the entire point of this fix. Since the value is a Pydantic-validated `int` with `ge`/`le` bounds, interpolation carries no injection risk — Pydantic rejects non-integers with a 422 before the f-string is built.

> **⚠️ Deploy backend first.** Because extras are currently permitted, a frontend that ships `lookback_days` before the backend understands it has the field **silently discarded**, and the unbounded scan continues with no error. `extra="forbid"` on the new subclass removes this failure mode permanently.

> **ℹ️ `params.region` is already sanitized** by `_validate_safe_params` at the top of both handlers. Do not add escaping around the existing `{params.region}` interpolation.

#### Tests

Create `tests/test_governance_lookback.py`. The `mock_bq_all` fixture ([`conftest.py:70`](tests/conftest.py#L70)) returns a `MagicMock` client, so generated SQL is recoverable via `mock_client.query.call_args`.

```python
"""Regression tests for F1 — JOBS_BY_ORGANIZATION scans must be time-bounded.

INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION is partitioned on creation_time with
180-day retention. A query without a creation_time predicate cannot prune
partitions and reads the org's entire job history; LIMIT does not bound bytes
scanned. These tests fail if that predicate is ever dropped again.
"""
import pytest

_BASE = {"org_project_id": "test-project", "region": "region-us"}

JOB_GOVERNANCE_ENDPOINTS = [
    "/api/mv/analyze",
    "/api/resource_warnings/analyze",
]


def _captured_sql(mock_client):
    assert mock_client.query.call_args is not None, "no query was submitted"
    return mock_client.query.call_args[0][0]


@pytest.mark.parametrize("endpoint", JOB_GOVERNANCE_ENDPOINTS)
def test_query_is_time_bounded(test_client, mock_bq_all, endpoint):
    resp = test_client.post(endpoint, json={**_BASE})
    assert resp.status_code == 200
    sql = _captured_sql(mock_bq_all)
    assert "creation_time >" in sql
    assert "TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL" in sql


@pytest.mark.parametrize("endpoint", JOB_GOVERNANCE_ENDPOINTS)
def test_default_lookback_is_30_days(test_client, mock_bq_all, endpoint):
    resp = test_client.post(endpoint, json={**_BASE})
    assert resp.status_code == 200
    assert "INTERVAL 30 DAY" in _captured_sql(mock_bq_all)


@pytest.mark.parametrize("endpoint", JOB_GOVERNANCE_ENDPOINTS)
def test_explicit_lookback_reaches_sql(test_client, mock_bq_all, endpoint):
    resp = test_client.post(endpoint, json={**_BASE, "lookback_days": 7})
    assert resp.status_code == 200
    assert "INTERVAL 7 DAY" in _captured_sql(mock_bq_all)


@pytest.mark.parametrize("endpoint", JOB_GOVERNANCE_ENDPOINTS)
@pytest.mark.parametrize("bad", [0, -1, 91, 999])
def test_out_of_range_lookback_rejected(test_client, mock_bq_all, endpoint, bad):
    """Bounds are what make f-string interpolation of this value safe."""
    resp = test_client.post(endpoint, json={**_BASE, "lookback_days": bad})
    assert resp.status_code == 422


@pytest.mark.parametrize("endpoint", JOB_GOVERNANCE_ENDPOINTS)
def test_non_integer_lookback_rejected(test_client, mock_bq_all, endpoint):
    resp = test_client.post(endpoint, json={**_BASE, "lookback_days": "30 DAY) OR TRUE--"})
    assert resp.status_code == 422


@pytest.mark.parametrize("endpoint", JOB_GOVERNANCE_ENDPOINTS)
def test_unknown_field_rejected(test_client, mock_bq_all, endpoint):
    """extra='forbid' turns version skew into a loud 422 rather than a silent
    revert to an unbounded scan."""
    resp = test_client.post(endpoint, json={**_BASE, "lookbackDays": 30})
    assert resp.status_code == 422


def test_mv_query_is_deterministically_ordered(test_client, mock_bq_all):
    resp = test_client.post("/api/mv/analyze", json={**_BASE})
    assert resp.status_code == 200
    assert "ORDER BY creation_time DESC" in _captured_sql(mock_bq_all)


def test_governance_endpoint_still_accepts_audit_type(test_client, mock_bq_all):
    """Regression guard against adding extra='forbid' to the shared base."""
    resp = test_client.post("/api/governance/analyze",
                            json={**_BASE, "audit_type": "expiration"})
    assert resp.status_code == 200
```

**Existing tests must pass unchanged** — six cases post payloads without `lookback_days`: [`test_smoke_endpoints.py:140-150`](tests/test_smoke_endpoints.py#L140), [`test_focus_guard.py:174-175`](tests/test_focus_guard.py#L174), [`test_security.py:22-23`](tests/test_security.py#L22). If any fail, the field was made required — revert. Do **not** fix them by adding the field; their passing is the backward-compatibility proof.

#### Expected user-visible outcome

| Panel | Change |
|---|---|
| Proactive Resource Warnings | **No visible change** when ≥50 warnings exist in 30 days — already ordered `creation_time DESC LIMIT 50`, so the same rows render. Bytes scanned drop. |
| Materialized View Rejections | Rows **will** change and may shrink; previous output was an arbitrary unordered sample. This is the fix, not a regression. |
| Datasets Missing Expiration / Partitioned Tables Missing Filter | **Untouched.** Still `GovernanceParams`. |

---

### F2 — `total_bytes_billed` summed across per-second timeline rows

**Severity:** 🔴 Critical — silently wrong billing metric
**File:** [`src/main.py:2606-2634`](src/main.py#L2606) (`/api/slots/utilization`)

#### The problem

```sql
WITH per_second AS (
  SELECT
    period_start,
    SUM(period_slot_ms)        AS total_slot_ms,
    SUM(total_bytes_billed)    AS total_bytes_billed,     -- ← job-level column
    SUM(total_bytes_processed) AS total_bytes_processed   -- ← job-level column
  FROM `...`.INFORMATION_SCHEMA.JOBS_TIMELINE_BY_ORGANIZATION
  WHERE period_start > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {params.lookback_days} DAY)
    AND job_type = 'QUERY'
    AND (statement_type != 'SCRIPT' OR statement_type IS NULL)
  GROUP BY period_start
)
SELECT
  ...
  SUM(total_bytes_billed)    / COUNT(*) AS bytes_billed_avg,
  SUM(total_bytes_processed) / COUNT(*) AS bytes_processed_avg
FROM per_second
GROUP BY period_min
```

#### Why this is wrong

`JOBS_TIMELINE_*` emits **one row per job per second of execution**. Two different kinds of column live on that row:

| Column | Grain | Safe to `SUM` across seconds? |
|---|---|---|
| `period_slot_ms` | **Per-second** — slot-ms consumed in *this* second | ✅ Yes |
| `total_bytes_billed` | **Per-job total** — repeated verbatim on every one of the job's rows | ❌ No |
| `total_bytes_processed` | **Per-job total** — same | ❌ No |

Summing a job-level total across per-second rows multiplies each job's byte count by its duration in seconds. A 10-minute query contributes its full byte count **600 times**.

The inflation factor is approximately the mean job duration in seconds — commonly 100×, and unbounded for long-running queries. The slot columns in the same CTE are correct, which makes the wrong ones far harder to spot in review: the query looks internally consistent.

#### Impact assessment — and why the fix is deletion

I grepped the entire repository for consumers:

```bash
grep -rn "bytes_billed_avg\|bytes_processed_avg" --include=*.js --include=*.html --include=*.py --include=*.md .
# → only src/main.py
```

**Nothing consumes these fields.** Not `static/app.js`, not `docs/static/app.js`, not `index.html`, not any test, not the docs. They are computed, shipped over the wire in every `/api/slots/utilization` response, rounded in Python at [`main.py:2662-2663`](src/main.py#L2662), and discarded by the client.

So the correct fix is **deletion**, not repair. Repairing a metric nobody reads means writing and maintaining a job-level dedup CTE for no user benefit. Deleting it removes a wrong number from a public API response and simplifies the query.

#### The fix (recommended: delete)

**File:** `src/main.py`

**Step 1** — Remove the two columns from the `per_second` CTE:

**Before:**
```sql
    WITH per_second AS (
      SELECT
        period_start,
        SUM(period_slot_ms) AS total_slot_ms,
        SUM(total_bytes_billed) AS total_bytes_billed,
        SUM(total_bytes_processed) AS total_bytes_processed
      FROM
```

**After:**
```sql
    -- NOTE: total_bytes_billed / total_bytes_processed are intentionally absent.
    -- JOBS_TIMELINE_* repeats those JOB-LEVEL totals on every per-second row, so
    -- SUM()-ing them here multiplies each job's bytes by its duration in seconds.
    -- Byte metrics must come from JOBS_BY_ORGANIZATION keyed on job_id, never
    -- from the timeline view. Only period_slot_ms is genuinely per-second.
    WITH per_second AS (
      SELECT
        period_start,
        SUM(period_slot_ms) AS total_slot_ms
      FROM
```

**Step 2** — Remove the two derived columns from the outer `SELECT`:

**Before:**
```sql
      APPROX_QUANTILES(total_slot_ms / 1000, 100)[OFFSET(99)] AS p99_slots,
      SUM(total_bytes_billed) / COUNT(*) AS bytes_billed_avg,
      SUM(total_bytes_processed) / COUNT(*) AS bytes_processed_avg
    FROM per_second
```

**After:**
```sql
      APPROX_QUANTILES(total_slot_ms / 1000, 100)[OFFSET(99)] AS p99_slots
    FROM per_second
```

**Step 3** — Remove from the Python row projection at [`main.py:2655-2664`](src/main.py#L2655):

**Before:**
```python
            processed_results.append({
                "timestamp": ts_tz.isoformat(),
                "max_slots": round(row['max_slots'] or 0, 2),
                "median_slots": round(row['p50_slots'] or 0, 3),
                "p90_slots": round(row['p90_slots'] or 0, 3),
                "p99_slots": round(row['p99_slots'] or 0, 3),
                "time_average": round(row['time_average'] or 0, 4),
                "bytes_billed_avg": round(row['bytes_billed_avg'] or 0, 2),
                "bytes_processed_avg": round(row['bytes_processed_avg'] or 0, 4)
            })
```

**After:**
```python
            processed_results.append({
                "timestamp": ts_tz.isoformat(),
                "max_slots": round(row['max_slots'] or 0, 2),
                "median_slots": round(row['p50_slots'] or 0, 3),
                "p90_slots": round(row['p90_slots'] or 0, 3),
                "p99_slots": round(row['p99_slots'] or 0, 3),
                "time_average": round(row['time_average'] or 0, 4)
            })
```

#### Alternative fix (only if the metric is genuinely wanted)

If product wants a bytes metric on this chart, it must be built on a job-level basis. Add a separate CTE that deduplicates to one row per job before aggregating, and attribute each job to the bucket of its first observed second:

```sql
    job_bytes AS (
      SELECT
        job_id,
        MIN(period_start) AS first_seen,
        -- ANY_VALUE is correct here: these columns are constant across a job's rows.
        ANY_VALUE(total_bytes_billed)    AS job_bytes_billed,
        ANY_VALUE(total_bytes_processed) AS job_bytes_processed
      FROM
        `{resolved_project}`.`{params.region}`.INFORMATION_SCHEMA.JOBS_TIMELINE_BY_ORGANIZATION
      WHERE
        period_start > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {params.lookback_days} DAY)
        AND job_type = 'QUERY'
        AND (statement_type != 'SCRIPT' OR statement_type IS NULL)
      GROUP BY job_id
    ),
    bytes_per_bucket AS (
      SELECT
        TIMESTAMP_TRUNC(first_seen, {resolution}, @tz) AS period_min,
        SUM(job_bytes_billed)    AS bytes_billed_total,
        SUM(job_bytes_processed) AS bytes_processed_total,
        COUNT(*)                 AS job_count
      FROM job_bytes
      GROUP BY period_min
    )
```

Then `LEFT JOIN bytes_per_bucket USING (period_min)` in the final `SELECT`. **Rename the response fields** to `bytes_billed_total` / `bytes_processed_total` — the old `_avg` names described a per-second average that never made sense.

⚠️ This costs an extra aggregation over the same scan. Given zero current consumers, prefer deletion.

#### Tests

```python
def test_utilization_does_not_sum_job_level_bytes(test_client, mock_bq_all):
    """F2: JOBS_TIMELINE_* repeats job-level byte totals on every per-second row.
    SUM()-ing them multiplies each job's bytes by its duration in seconds."""
    resp = test_client.post("/api/slots/utilization",
                            json={"org_project_id": "p", "region": "region-us"})
    assert resp.status_code == 200
    sql = mock_bq_all.query.call_args[0][0]
    assert "SUM(total_bytes_billed)" not in sql
    assert "SUM(total_bytes_processed)" not in sql
```

---

### F3 — Cost attribution silently drops unconfigured reservations

**Severity:** 🔴 Critical — chargeback numbers that do not reconcile, with no signal
**File:** [`src/cost_attribution.py:197-202`](src/cost_attribution.py#L197)

#### The problem

```python
            short_res_id = res_id.split('.')[-1] if '.' in res_id else (res_id.split(':')[-1] if ':' in res_id else res_id)
            res_config = config.reservations.get(short_res_id) or config.reservations.get(res_id)
            if not res_config:
                logger.warning(f"No configuration found for reservation {res_id} (short: {short_res_id}). Skipping.")
                continue
```

The same pattern repeats in the waste-rule-B branch at [`cost_attribution.py:245-247`](src/cost_attribution.py#L245).

#### Why this is wrong

The shipped default config is:

```json
{"waste_rule": "A", "central_cost_center_project": null,
 "borrowing_rule": "lender_pays", "reservations": {}}
```

`reservations` is **empty**. So out of the box, *every* reservation is skipped and `/api/cost-attribution/calculate` returns **HTTP 200 with an empty `attributions` list** — indistinguishable from "this org has no reservation spend".

Once partially configured, the failure is worse because it is partial: any reservation an admin forgot to configure vanishes from the output. The response still looks complete, still returns 200, and still renders a plausible-looking table. The only trace is a `logger.warning` in the server log, which the finance user reconciling against a GCP invoice will never see.

For a chargeback tool this is the worst possible failure mode. **A number that is silently incomplete is more dangerous than an error**, because it gets forwarded to business units as fact. A missing reservation means the sum of all attributions is less than the admin bill, and nothing in the API surface explains the gap.

#### The fix

Track skipped reservations and return them as a first-class part of the response.

**File:** `src/cost_attribution.py`

**Step 1** — Collect skips instead of discarding them. In `calculate_cost_attribution`, before the main loop (after line 190, `final_attributions = []`):

```python
        final_attributions = []
        unconfigured: Dict[str, float] = defaultdict(float)  # res_id -> unattributed slot_hours
```

**Step 2** — Record on skip. **Before** ([`cost_attribution.py:200-202`](src/cost_attribution.py#L200)):

```python
            if not res_config:
                logger.warning(f"No configuration found for reservation {res_id} (short: {short_res_id}). Skipping.")
                continue
```

**After:**

```python
            if not res_config:
                # Do NOT silently drop. An unconfigured reservation means the
                # attributed total will not reconcile against the admin bill,
                # and the caller has no way to detect that from a 200 + list.
                logger.warning(
                    "No configuration found for reservation %s (short: %s). "
                    "Its %.2f slot-hours will be reported as unattributed.",
                    res_id, short_res_id, slot_hours
                )
                unconfigured[res_id] += slot_hours
                continue
```

**Step 3** — Apply the same treatment in the rule-B branch. **Before** ([`cost_attribution.py:245-247`](src/cost_attribution.py#L245)):

```python
                res_config = config.reservations.get(short_res_id) or config.reservations.get(res_id)
                if not res_config:
                    continue
```

**After:**

```python
                res_config = config.reservations.get(short_res_id) or config.reservations.get(res_id)
                if not res_config:
                    # Already recorded in `unconfigured` by the main loop above.
                    continue
```

**Step 4** — Surface it in the response. **Before** ([`cost_attribution.py:275-277`](src/cost_attribution.py#L275)):

```python
        logger.info("Returning %d attribution records (scope: %s).", len(final_attributions), scope.mode)
        log_endpoint_end("Cost Attribution", t0, _logger=logger)
        return {"scope": scope.model_dump(), "attributions": final_attributions}
```

**After:**

```python
        unattributed = [
            {"reservation_id": rid, "slot_hours": round(hours, 2)}
            for rid, hours in sorted(unconfigured.items(), key=lambda kv: -kv[1])
        ]
        total_unattributed_slot_hours = round(sum(unconfigured.values()), 2)
        if unattributed:
            logger.warning(
                "%d reservation(s) totalling %.2f slot-hours are unconfigured and "
                "excluded from attribution — the result will NOT reconcile with the bill.",
                len(unattributed), total_unattributed_slot_hours
            )

        logger.info("Returning %d attribution records (scope: %s).", len(final_attributions), scope.mode)
        log_endpoint_end("Cost Attribution", t0, _logger=logger)
        return {
            "scope": scope.model_dump(),
            "attributions": final_attributions,
            # Reconciliation contract: a non-empty list here means `attributions`
            # is incomplete by construction. Callers MUST surface this.
            "unattributed_reservations": unattributed,
            "total_unattributed_slot_hours": total_unattributed_slot_hours,
            "is_complete": not unattributed,
        }
```

**Step 5** — Render it in the UI. In the cost-attribution view handler in `static/app.js` (and `docs/static/app.js`), after rendering the attributions table, add a warning banner when `is_complete === false`:

```javascript
                if (data.is_complete === false) {
                    showNotification(
                        `${data.unattributed_reservations.length} reservation(s) ` +
                        `(${data.total_unattributed_slot_hours.toLocaleString()} slot-hours) ` +
                        `have no cost configuration and are excluded. Totals will not ` +
                        `reconcile with your GCP invoice until they are configured.`,
                        'warning'
                    );
                }
```

#### Why not just raise an error?

Considered and rejected. Partial attribution is a legitimate intermediate state while an admin is configuring reservations one at a time. Failing the whole request would make the tool unusable during onboarding. The right contract is: **return the partial result, and make the incompleteness impossible to miss.**

#### Tests

```python
def test_attribution_reports_unconfigured_reservations(test_client, mock_bq_all,
                                                       mock_bq_row_factory):
    """F3: an unconfigured reservation must appear in unattributed_reservations,
    not vanish from a 200 response."""
    rows = [mock_bq_row_factory(project_id="proj-a", reservation_id="res-unknown",
                                total_slot_ms=7_200_000)]  # 2 slot-hours
    result = mock_bq_all.query.return_value.result.return_value
    result.__iter__ = lambda self: iter(rows)

    resp = test_client.post("/api/cost-attribution/calculate", json={
        "billing_month_start": "2026-06-01",
        "billing_month_end": "2026-06-30",
        "org_project_id": "test-project", "region": "region-us",
    })
    assert resp.status_code == 200
    body = resp.json()
    assert body["is_complete"] is False
    assert body["attributions"] == []
    assert body["unattributed_reservations"] == [
        {"reservation_id": "res-unknown", "slot_hours": 2.0}
    ]
    assert body["total_unattributed_slot_hours"] == 2.0


def test_empty_config_does_not_look_like_zero_spend(test_client, mock_bq_all,
                                                    mock_bq_row_factory):
    """The shipped default config has reservations={}. That must not render as
    'no reservation spend'."""
    rows = [mock_bq_row_factory(project_id="p", reservation_id="r",
                                total_slot_ms=3_600_000)]
    mock_bq_all.query.return_value.result.return_value.__iter__ = lambda self: iter(rows)
    resp = test_client.post("/api/cost-attribution/calculate", json={
        "billing_month_start": "2026-06-01", "billing_month_end": "2026-06-30",
        "org_project_id": "test-project", "region": "region-us",
    })
    assert resp.json()["is_complete"] is False
```

---

## 4. High Findings

### F4 — Governance expiration audit is silently project-scoped

**Severity:** 🟠 High — returns empty results that read as "no issues found"
**File:** [`src/main.py:2104-2122`](src/main.py#L2104)

#### The problem

```python
    exp_focus_clause, exp_focus_params = build_project_filter(
        params.focus_projects, column="catalog_name", table_alias="s"
    )
```

```sql
        SELECT
          s.catalog_name AS project_id,
          s.schema_name AS dataset_id,
          CAST(NULL AS STRING) AS default_table_expiration
        FROM `{target_project}`.`{params.region}`.INFORMATION_SCHEMA.SCHEMATA s
        LEFT JOIN `{target_project}`.`{params.region}`.INFORMATION_SCHEMA.SCHEMATA_OPTIONS o
          ON s.catalog_name = o.catalog_name
          AND s.schema_name = o.schema_name
          AND o.option_name = 'default_table_expiration_days'
        WHERE o.option_name IS NULL
          {exp_focus_clause}          -- AND s.catalog_name IN UNNEST(@focus_projects)
```

#### Why this is wrong

`INFORMATION_SCHEMA.SCHEMATA` has **no `_BY_ORGANIZATION` variant**. It only ever lists datasets within `target_project`. Therefore `s.catalog_name` is effectively a constant equal to `target_project`.

Consequence: if a user sets focus projects to anything that does not include the target project, `AND s.catalog_name IN UNNEST(@focus_projects)` matches nothing and the query returns **zero rows** — rendered by the UI as an empty "Datasets Missing Expiration Policy" table, indistinguishable from a clean bill of health.

This is particularly deceptive because the `build_project_filter` plumbing was *carefully* built for this call site: `column="catalog_name"` and `table_alias="s"` are both non-default arguments that exist in `_ALLOWED_FILTER_COLUMNS` and `_ALIAS_RE` specifically to serve this query. The care taken makes it read as intentionally org-scoped when it structurally cannot be.

Note the second half of the same endpoint (`TABLE_STORAGE_BY_ORGANIZATION`, line 2144) **is** genuinely org-wide. So one endpoint mixes an org-scoped audit with a project-scoped one and presents both as equally scoped.

#### The fix — option A (recommended): one query per project, run in parallel

> **⚠️ Do not use `UNION ALL` here.** An earlier draft of this review proposed a single `UNION ALL` over one `SCHEMATA` branch per focused project. That is the wrong shape for a **cross-project** query, because a BigQuery job is **atomic with respect to permissions**: if the caller lacks `bigquery.datasets.get` (or the region's `INFORMATION_SCHEMA` read) on **any one** branch, the entire job fails with `403 Forbidden` and **all** projects return nothing — including the ones the caller can read. A `try/except Forbidden` around the job can only degrade all-or-nothing back to the target project; it cannot recover the per-project results, so a single unreadable project blanks the whole panel. That is the same "empty table reads as clean bill of health" failure this finding exists to remove.
>
> `UNION ALL` remains correct for the partition-filter audit in the same function and for `get_physical_datasets` at [`main.py:864`](src/main.py#L864), because those iterate **datasets inside already-authorised projects** — the permission boundary has already been cleared.
>
> The correct shape is **one job per project, executed concurrently, merged in Python**, so a 403 on one project degrades only that project. This also gives an explicit list of what was skipped, which the panel can surface.

**Concurrency model.** These handlers are sync `def`, so FastAPI already runs them in the AnyIO worker thread pool (limiter raised to 100 at lifespan startup). `asyncio.gather` is therefore not available. Use `ThreadPoolExecutor`, exactly as `check_hbo_status` already does at [`hbo.py:380-391`](src/hbo.py#L380) — including its `request_id_var` propagation, without which every log line emitted inside a worker thread loses request correlation and prints the `"--------"` default.

**Before** ([`main.py:2110-2124`](src/main.py#L2110)):

```python
        # 1. Audit Dataset Expiration
        exp_sql = f"""
        SELECT
          s.catalog_name AS project_id,
          s.schema_name AS dataset_id,
          CAST(NULL AS STRING) AS default_table_expiration
        FROM `{target_project}`.`{params.region}`.INFORMATION_SCHEMA.SCHEMATA s
        LEFT JOIN `{target_project}`.`{params.region}`.INFORMATION_SCHEMA.SCHEMATA_OPTIONS o
          ON s.catalog_name = o.catalog_name
          AND s.schema_name = o.schema_name
          AND o.option_name = 'default_table_expiration_days'
        WHERE o.option_name IS NULL
          {exp_focus_clause}
        """

        exp_results = run_query_and_log(scoped_client, exp_sql, "Expiration Audit", params=params, query_parameters=exp_focus_params)
```

**After:**

```python
        # 1. Audit Dataset Expiration
        #
        # INFORMATION_SCHEMA.SCHEMATA has NO _BY_ORGANIZATION variant — it only
        # ever lists datasets inside the project that owns the view. A WHERE on
        # catalog_name is therefore a no-op at best and silently returns zero
        # rows at worst (when focus_projects excludes target_project), which the
        # UI renders as "no issues found".
        #
        # We therefore run ONE JOB PER PROJECT rather than a single UNION ALL.
        # A BigQuery job is atomic with respect to permissions: a 403 on any one
        # UNION ALL branch fails the entire job, so a single unreadable project
        # would blank the whole panel — reintroducing the exact silent-empty
        # failure this fix exists to remove. Per-project jobs degrade to
        # per-project gaps, and we report which projects were skipped.
        #
        # Each project ID is re-validated through _safe_ident before
        # interpolation. focus_projects has already passed
        # validate_focus_projects() in _validate_safe_params, but these values
        # land in a backtick-quoted identifier position, so validate again at
        # the point of use (defence in depth, consistent with the derived
        # admin_project_id handling in analyze_slots).
        expiration_projects = params.focus_projects or [target_project]
        if len(expiration_projects) > MAX_FOCUS_PROJECTS:
            raise HTTPException(
                400,
                f"Expiration audit supports at most {MAX_FOCUS_PROJECTS} projects; "
                f"got {len(expiration_projects)}."
            )

        def _expiration_for_project(proj: str):
            """Returns (project_id, rows, error_or_None). Never raises."""
            safe_proj = _safe_ident(proj, "focus project (expiration audit)")
            sql = f"""
            SELECT
              s.catalog_name AS project_id,
              s.schema_name AS dataset_id,
              CAST(NULL AS STRING) AS default_table_expiration
            FROM `{safe_proj}`.`{params.region}`.INFORMATION_SCHEMA.SCHEMATA s
            LEFT JOIN `{safe_proj}`.`{params.region}`.INFORMATION_SCHEMA.SCHEMATA_OPTIONS o
              ON s.catalog_name = o.catalog_name
              AND s.schema_name = o.schema_name
              AND o.option_name = 'default_table_expiration_days'
            WHERE o.option_name IS NULL
            """
            try:
                rows = list(run_query_and_log(
                    scoped_client, sql, f"Expiration Audit [{proj}]", params=params
                ))
                return proj, rows, None
            except (gax_exc.Forbidden, gax_exc.NotFound) as e:
                # Forbidden  -> caller lacks INFORMATION_SCHEMA read on this project.
                # NotFound   -> project does not exist, or has no data in this region.
                # Neither is fatal to the other projects; record and continue.
                logger.warning(
                    f"Expiration audit skipped for project '{proj}': "
                    f"{type(e).__name__}: {e}"
                )
                return proj, [], type(e).__name__
            except Exception as e:
                logger.warning(f"Expiration audit failed for project '{proj}': {e}")
                return proj, [], type(e).__name__

        # Sync def route -> already on an AnyIO worker thread; asyncio.gather is
        # not available. Use ThreadPoolExecutor with request_id propagation,
        # matching check_hbo_status at hbo.py:380-391. Without the contextvar
        # copy, every log line from a worker thread prints the "--------"
        # default instead of the request ID.
        req_id = request_id_var.get()

        def _with_ctx(proj: str):
            token = request_id_var.set(req_id)
            try:
                return _expiration_for_project(proj)
            finally:
                request_id_var.reset(token)

        # Bounded: MAX_FOCUS_PROJECTS is 50, and each worker holds a BQ job.
        # 10 matches the existing pool size in check_hbo_status.
        with ThreadPoolExecutor(max_workers=min(10, len(expiration_projects))) as ex:
            per_project = list(ex.map(_with_ctx, expiration_projects))

        exp_results = [row for _, rows, _ in per_project for row in rows]
        inaccessible_projects = [proj for proj, _, err in per_project if err]
```

Downstream, `exp_results` is now a plain `list` of rows rather than a `RowIterator`. The existing consumption loop iterates it, so no change is needed there — but do **not** call `.total_rows` or re-iterate it as if it were lazy.

Surface the partial-coverage signal so an incomplete audit cannot masquerade as a clean one. Add to `GovernanceResponse`:

```python
class GovernanceResponse(BaseModel):
    expiration_issues: List[ExpirationResult]
    filter_issues: List[PartitionFilterResult]
    # Projects the expiration audit could not read (403/404). Empty list means
    # full coverage. The panel must render a warning when this is non-empty —
    # otherwise a permissions gap is indistinguishable from "no issues found".
    inaccessible_projects: List[str] = Field(default_factory=list)
```

This field is **additive**, so `test_governance_has_expiration_and_filter_issues` ([`test_smoke_endpoints.py:348`](tests/test_smoke_endpoints.py#L348)) keeps passing, and the frontend ignores unknown keys until the panel is updated.

Then **delete** the now-unused builder at [`main.py:2104-2106`](src/main.py#L2104):

```python
    exp_focus_clause, exp_focus_params = build_project_filter(
        params.focus_projects, column="catalog_name", table_alias="s"
    )
```

**Import check:**
- `MAX_FOCUS_PROJECTS` and `request_id_var` are in `src/utils.py`; `request_id_var` is already imported at [`main.py:44`](src/main.py#L44). Verify `MAX_FOCUS_PROJECTS` is in the `from .utils import (...)` block; add it if not.
- `ThreadPoolExecutor` is already imported at [`main.py:7`](src/main.py#L7).
- `gax_exc` (`google.api_core.exceptions`) and `Field` must both be in scope in `main.py`.

#### The fix — option B (minimal): make the scope honest

If per-project iteration is not wanted, delete the focus clause and rename the field so the contract is not misleading:

```python
        # SCHEMATA is project-scoped with no _BY_ORGANIZATION variant, so this
        # audit covers target_project only. focus_projects is deliberately NOT
        # applied — a catalog_name filter here would silently return zero rows.
        exp_sql = f"""
        SELECT ...
        WHERE o.option_name IS NULL
        """
```

and add to `GovernanceResponse`:

```python
class GovernanceResponse(BaseModel):
    expiration_issues: List[ExpirationResult]
    expiration_scope_project: str          # SCHEMATA is project-scoped; this is the only project covered
    filter_issues: List[PartitionFilterResult]
```

Display it in the panel header so the user knows what they are looking at.

#### Tests

```python
def test_expiration_audit_issues_one_job_per_focus_project(test_client, mock_bq_all):
    """F4: SCHEMATA is project-scoped. Focus must expand to one job PER PROJECT
    (not one UNION ALL job, and not a catalog_name predicate that matches
    nothing), so a 403 on one project cannot blank the whole panel."""
    test_client.post("/api/governance/analyze", json={
        "org_project_id": "target-proj", "region": "region-us",
        "focus_projects": ["proj-a", "proj-b"],
    })
    exp_sqls = [c[0][0] for c in mock_bq_all.query.call_args_list
                if "SCHEMATA_OPTIONS" in c[0][0]]

    # Two separate jobs, not one UNION ALL job.
    assert len(exp_sqls) == 2, f"expected one job per project, got {len(exp_sqls)}"
    assert not any("UNION ALL" in s for s in exp_sqls)

    joined = "\n".join(exp_sqls)
    assert "`proj-a`.`region-us`.INFORMATION_SCHEMA.SCHEMATA" in joined
    assert "`proj-b`.`region-us`.INFORMATION_SCHEMA.SCHEMATA" in joined
    assert "catalog_name IN UNNEST" not in joined


def test_expiration_audit_survives_forbidden_on_one_project(test_client, mock_bq_all):
    """F4: a permissions gap on one project must degrade to a reported gap, not
    an empty panel. This is the regression that UNION ALL would reintroduce."""
    from google.api_core import exceptions as gax_exc

    def _query(sql, *a, **kw):
        if "SCHEMATA_OPTIONS" in sql and "`proj-b`" in sql:
            raise gax_exc.Forbidden("no bigquery.datasets.get on proj-b")
        return mock_bq_all.query.return_value
    mock_bq_all.query.side_effect = _query

    resp = test_client.post("/api/governance/analyze", json={
        "org_project_id": "target-proj", "region": "region-us",
        "focus_projects": ["proj-a", "proj-b"],
    })
    assert resp.status_code == 200          # proj-a's results are NOT lost
    assert resp.json()["inaccessible_projects"] == ["proj-b"]
```

---

### F5 — `PARTITIONS` misread; unpartitioned tables flagged as violations

**Severity:** 🟠 High — false positives that make the panel untrustworthy
**File:** [`src/main.py:2160-2166`](src/main.py#L2160)

#### The problem

```python
                partitioned_tables_clauses.append(
                    f"SELECT DISTINCT '{p}' AS p, '{ds}' AS d, table_name AS t FROM `{p}`.`{ds}`.INFORMATION_SCHEMA.PARTITIONS"
                )
```

feeding:

```sql
                WITH partitioned_tables AS (
                  {pt_sql}
                ),
                table_options AS (
                  {opt_sql}
                )
                SELECT pt.p, pt.d, pt.t, o.option_value
                FROM partitioned_tables pt
                LEFT JOIN table_options o ON pt.p = o.p AND pt.d = o.d AND pt.t = o.t
                WHERE o.option_value IS NULL OR o.option_value = 'false'
```

#### Why this is wrong

The CTE is named `partitioned_tables`, but `INFORMATION_SCHEMA.PARTITIONS` emits a row for **every** table in the dataset, not only partitioned ones. An unpartitioned table appears as a single row with `partition_id IS NULL`.

So the anti-join reports **every unpartitioned table** in the top-5 heaviest datasets as "Partitioned Table Missing Filter Guardrail". That advice is meaningless — `require_partition_filter` cannot be set on a table with no partitions.

Ingestion-time partitioned tables also expose the sentinel partition IDs `__NULL__` (rows with a NULL partitioning column) and `__UNPARTITIONED__` (rows still in streaming buffer). Those are legitimately partitioned tables and must **not** be excluded.

There are two further problems with using `PARTITIONS` as the *primary* detector, which the fix below addresses:

- **Cost / latency.** `PARTITIONS` emits **one row per partition**. A dataset holding a few daily-partitioned tables with multi-year retention produces tens of thousands of rows, and the view is materialised per query. `SELECT DISTINCT table_name` over it is an expensive way to answer a question about table *metadata*.
- **Empty tables are invisible.** A partitioned table with zero partitions (freshly created, or fully expired) produces **no rows at all** in `PARTITIONS`, so it is silently omitted from the audit. A brand-new partitioned table missing `require_partition_filter` is exactly the case worth catching early, and it is the one case this view cannot see.

A secondary correctness issue on the same lines: `'{p}'` and `'{ds}'` are interpolated into **string literal** positions without validation. They originate from a BigQuery result set (`TABLE_STORAGE_BY_ORGANIZATION`), not user input, so this is not currently exploitable — but it is the only place in the file where an interpolated value skips `_safe_ident`, and it is inconsistent with the deliberate defence-in-depth applied to derived `admin_project_id` values at [`main.py:2400`](src/main.py#L2400).

Third: `partition_type` is hardcoded to `"UNKNOWN"` at [`main.py:2190`](src/main.py#L2190) with the comment *"Exact type not strictly needed for UI presentation"* — but the UI renders a "Partition Type" column ([`index.html:2205`](static/index.html#L2205)), so every row shows `UNKNOWN`.

#### The fix

**File:** `src/main.py`

> **⚠️ `COLUMNS` alone is not a drop-in replacement for `PARTITIONS`.** The obvious improvement is to detect partitioning with `INFORMATION_SCHEMA.COLUMNS WHERE is_partitioning_column = 'YES'` — one row per column instead of one row per partition, includes empty tables, and hands you the partitioning column name for free (making the old Step 2 redundant). All true, and it is the right primary detector.
>
> But it has a **systematic blind spot**: `_PARTITIONTIME` and `_PARTITIONDATE` are **pseudocolumns, not schema fields**. They do not appear in `INFORMATION_SCHEMA.COLUMNS` at all, so an **ingestion-time partitioned table has no row with `is_partitioning_column = 'YES'`**. A pure-`COLUMNS` audit would therefore drop *every ingestion-time partitioned table* from the results — silently, and permanently.
>
> That is the worst possible class of table to lose here. Ingestion-time partitioning is the legacy default, so those tables skew old, large, and widely queried — precisely the population where a missing `require_partition_filter` costs the most. Trading one silent-omission bug (empty tables) for another (ingestion-time tables) is not a fix.
>
> **Use both**: `COLUMNS` as the primary detector, and a narrow `PARTITIONS` probe for the ingestion-time residue.

**Step 1** — Validate the derived identifiers.

**Before** ([`main.py:2155-2169`](src/main.py#L2155)):

```python
            for row in top_datasets_results:
                p = row.project_id
                ds = row.dataset_id
                partitioned_tables_clauses.append(
                    f"SELECT DISTINCT '{p}' AS p, '{ds}' AS d, table_name AS t FROM `{p}`.`{ds}`.INFORMATION_SCHEMA.PARTITIONS"
                )
                table_options_clauses.append(
                    f"SELECT '{p}' AS p, '{ds}' AS d, table_name AS t, option_value FROM `{p}`.`{ds}`.INFORMATION_SCHEMA.TABLE_OPTIONS WHERE option_name = 'require_partition_filter'"
                )
```

**After:**

```python
            for row in top_datasets_results:
                # These come from a BigQuery result set, not the request, but they
                # land in both identifier and string-literal positions below.
                # Validate anyway — consistent with the derived admin_project_id
                # handling in analyze_slots.
                p = _safe_ident(row.project_id, "project_id (derived)")
                ds = _safe_ident(row.dataset_id, "dataset_id (derived)")

                # PRIMARY detector: COLUMNS is one row per column (cheap), covers
                # tables with zero partitions, and yields the partitioning column
                # name — which removes the hardcoded partition_type="UNKNOWN".
                partitioned_tables_clauses.append(
                    f"SELECT '{p}' AS p, '{ds}' AS d, table_name AS t, "
                    f"column_name AS partition_col "
                    f"FROM `{p}`.`{ds}`.INFORMATION_SCHEMA.COLUMNS "
                    f"WHERE is_partitioning_column = 'YES'"
                )

                # SUPPLEMENT: _PARTITIONTIME / _PARTITIONDATE are PSEUDOCOLUMNS and
                # are absent from COLUMNS entirely, so ingestion-time partitioned
                # tables produce NO is_partitioning_column='YES' row. Without this
                # branch the audit would silently skip every one of them — the
                # legacy-default, largest, most-queried tables in the estate.
                #
                # PARTITIONS also emits one row per table for UNPARTITIONED tables,
                # with partition_id = NULL; that predicate excludes them. The
                # ingestion-time sentinels '__NULL__' and '__UNPARTITIONED__' are
                # deliberately KEPT — those tables ARE partitioned.
                #
                # The NOT EXISTS de-dupes against the COLUMNS branch so a
                # column-partitioned table is not emitted twice.
                ingestion_time_clauses.append(
                    f"SELECT DISTINCT '{p}' AS p, '{ds}' AS d, part.table_name AS t, "
                    f"CAST(NULL AS STRING) AS partition_col "
                    f"FROM `{p}`.`{ds}`.INFORMATION_SCHEMA.PARTITIONS part "
                    f"WHERE part.partition_id IS NOT NULL "
                    f"AND NOT EXISTS ("
                    f"  SELECT 1 FROM `{p}`.`{ds}`.INFORMATION_SCHEMA.COLUMNS col "
                    f"  WHERE col.table_name = part.table_name "
                    f"    AND col.is_partitioning_column = 'YES'"
                    f")"
                )

                table_options_clauses.append(
                    f"SELECT '{p}' AS p, '{ds}' AS d, table_name AS t, option_value "
                    f"FROM `{p}`.`{ds}`.INFORMATION_SCHEMA.TABLE_OPTIONS "
                    f"WHERE option_name = 'require_partition_filter'"
                )
```

Initialise `ingestion_time_clauses = []` alongside the two existing lists, and fold it into the CTE:

```python
                pt_sql = "\nUNION ALL\n".join(
                    partitioned_tables_clauses + ingestion_time_clauses
                )
```

The `UNION ALL` across datasets is safe here — unlike F4, these datasets came from `TABLE_STORAGE_BY_ORGANIZATION` results the caller has already demonstrably read, so the permission boundary is cleared before the query is built.

**Step 2** — Carry `partition_col` through the audit query and retire `"UNKNOWN"`.

The CTE now has a fourth column, so widen the projection:

```sql
                SELECT pt.p, pt.d, pt.t, pt.partition_col, o.option_value
                FROM partitioned_tables pt
                LEFT JOIN table_options o ON pt.p = o.p AND pt.d = o.d AND pt.t = o.t
                WHERE o.option_value IS NULL OR o.option_value = 'false'
```

and replace `partition_type="UNKNOWN"` at [`main.py:2190`](src/main.py#L2190) with:

```python
                            # NULL partition_col == came from the ingestion-time
                            # branch, where the partitioning key is a pseudocolumn
                            # with no COLUMNS row.
                            partition_type=row.partition_col or "_PARTITIONTIME",
```

This is what the UI's "Partition Type" column at [`index.html:2205`](static/index.html#L2205) has been showing as `UNKNOWN`.

**If you deliberately choose `COLUMNS`-only** (to avoid the `PARTITIONS` scan entirely), that is a defensible cost trade — but then say so in the panel. Drop `ingestion_time_clauses`, and add a footnote to the Governance tab: *"Ingestion-time partitioned tables (`_PARTITIONTIME`) are not covered by this audit."* An uncovered class that is documented is a limitation; an uncovered class that is not is the same silent-empty bug as F4.

#### Tests

```python
def _governance_audit_sql(test_client, mock_bq_all, mock_bq_row_factory):
    top = [mock_bq_row_factory(project_id="proj-a", dataset_id="ds1",
                               total_bytes=10**12)]
    mock_bq_all.query.return_value.result.return_value.__iter__ = lambda self: iter(top)
    test_client.post("/api/governance/analyze",
                     json={"org_project_id": "p", "region": "region-us"})
    return next(c[0][0] for c in mock_bq_all.query.call_args_list
                if "partitioned_tables" in c[0][0])


def test_partition_audit_uses_columns_as_primary_detector(
        test_client, mock_bq_all, mock_bq_row_factory):
    """F5: COLUMNS is one row per column and covers zero-partition tables;
    PARTITIONS is one row per partition and does not."""
    sql = _governance_audit_sql(test_client, mock_bq_all, mock_bq_row_factory)
    assert "is_partitioning_column = 'YES'" in sql
    assert "column_name AS partition_col" in sql


def test_partition_audit_still_detects_ingestion_time_tables(
        test_client, mock_bq_all, mock_bq_row_factory):
    """F5: _PARTITIONTIME is a PSEUDOCOLUMN — ingestion-time partitioned tables
    have NO is_partitioning_column='YES' row in COLUMNS. Dropping the PARTITIONS
    branch would silently remove them all from the audit."""
    sql = _governance_audit_sql(test_client, mock_bq_all, mock_bq_row_factory)
    assert "INFORMATION_SCHEMA.PARTITIONS" in sql
    assert "partition_id IS NOT NULL" in sql          # excludes unpartitioned tables
    assert "NOT EXISTS" in sql                        # de-dupes vs the COLUMNS branch
    # Ingestion-time sentinels are legitimately partitioned — must NOT be excluded.
    assert "__UNPARTITIONED__" not in sql
    assert "__NULL__" not in sql


def test_partition_audit_reports_real_partition_type(
        test_client, mock_bq_all, mock_bq_row_factory):
    """F5: the UI renders a 'Partition Type' column; it must not be all UNKNOWN."""
    sql = _governance_audit_sql(test_client, mock_bq_all, mock_bq_row_factory)
    assert "pt.partition_col" in sql
```

---

### F6 — Permanent `BadRequest` retried 5× behind a misleading error message

**Severity:** 🟠 High — wasted latency plus an actively wrong diagnostic
**Files:** [`src/utils.py:299-351`](src/utils.py#L299), [`src/utils.py:262-296`](src/utils.py#L262)

#### The problem

```python
        except (gax_exc.GoogleAPIError, Exception) as e:
            err_msg = str(e)
            # Permanent errors: no amount of retrying will fix a missing
            # IAM permission or a non-existent resource.
            if isinstance(e, (gax_exc.Forbidden, gax_exc.NotFound)):
                logger.error(...)
                raise
            if attempt >= max_attempts:
                logger.error(...)
                raise

            logger.warning(...)
            time.sleep(delay)
            delay = min(delay * 2.0, max_delay)
```

Only `Forbidden` and `NotFound` are treated as permanent. Everything else gets 5 attempts with exponential backoff (1s → 2s → 4s → 8s, capped at 10s).

#### Why this is wrong

`gax_exc.BadRequest` (HTTP 400) is what BigQuery returns for:

- **SQL syntax errors** — deterministic; will fail identically on every retry
- **`maximum_bytes_billed` exceeded** — deterministic; the cap does not move between attempts
- Invalid query parameters, type mismatches, unresolved column names

None are transient. Each such failure now burns **~15 seconds of wall clock and 5 job submissions** before surfacing. In a synchronous FastAPI handler this also holds an AnyIO thread-pool slot for 15s — the pool was raised to 100 at lifespan startup precisely because handlers are sync `def`, so this consumes real concurrency headroom.

The `except (gax_exc.GoogleAPIError, Exception)` clause is itself redundant — `Exception` subsumes `GoogleAPIError`, so the tuple is equivalent to `except Exception`. Harmless, but it signals uncertainty about the exception hierarchy at exactly the point where that hierarchy is the control flow.

**The compounding problem** is in the error surface. [`handle_endpoint_exception`](src/utils.py#L262) maps `BadRequest` to:

> *"BigQuery rejected the request (invalid parameters or malformed query). Reference: {req_id}"*

So a user who exceeds the 200 GiB cap — the single most likely `BadRequest` in this application, and the exact failure mode **F1** produces — waits 15 seconds and is then told their query is malformed. The message never mentions the cap, never mentions the `max_bytes_billed_gb` knob that would fix it, and actively misdirects toward a nonexistent syntax problem.

#### The fix

**File:** `src/utils.py`

**Step 1** — Treat `BadRequest` as permanent.

**Before** ([`utils.py:328-336`](src/utils.py#L328)):

```python
        except (gax_exc.GoogleAPIError, Exception) as e:
            err_msg = str(e)
            # Permanent errors: no amount of retrying will fix a missing
            # IAM permission or a non-existent resource.
            if isinstance(e, (gax_exc.Forbidden, gax_exc.NotFound)):
                logger.error(
                    "❌ %s failed with permanent error (no retry): %s",
                    description, err_msg.splitlines()[0]
                )
                raise
```

**After:**

```python
        except Exception as e:
            err_msg = str(e)
            # Permanent errors: no amount of retrying will fix a missing IAM
            # permission, a non-existent resource, a syntax error, or a query
            # that exceeds maximum_bytes_billed. BadRequest in particular is
            # THE most common failure in this app (the bytes-billed cap), and
            # retrying it burned ~15s of wall clock plus an AnyIO thread-pool
            # slot before surfacing an error that was deterministic from the
            # first attempt.
            if isinstance(e, (gax_exc.Forbidden, gax_exc.NotFound, gax_exc.BadRequest)):
                logger.error(
                    "❌ %s failed with permanent error (no retry): %s",
                    description, err_msg.splitlines()[0]
                )
                raise
```

**Step 2** — Make the bytes-billed error actionable. In `handle_endpoint_exception`, add a branch **before** the existing generic `BadRequest` handling:

```python
    if isinstance(e, gax_exc.BadRequest):
        err_str = str(e)
        low = err_str.lower()
        # Surface the safety cap explicitly. This is the single most common
        # BadRequest in this application and the generic "malformed query"
        # message actively misdirects the user.
        if "maximum_bytes_billed" in low or "bytes billed" in low:
            cap_gib = DEFAULT_MAX_BYTES_BILLED // (1024**3)
            raise HTTPException(
                status_code=400,
                detail=(
                    f"This query would scan more data than the configured safety cap "
                    f"(default {cap_gib} GiB). Narrow the lookback window, apply a "
                    f"project focus, or raise 'max_bytes_billed_gb' in Settings. "
                    f"No data was billed."
                ),
            )
        # ... existing generic handling unchanged
```

The *"No data was billed"* clause is worth keeping — `maximum_bytes_billed` causes BigQuery to fail the job **before** billing, and users will not know that.

#### Tests

`tests/test_retry_limit.py` already exists and covers the attempt cap. Add:

```python
def test_bad_request_is_not_retried():
    """F6: BadRequest is deterministic (syntax error / bytes-billed cap).
    Retrying it wastes ~15s and an AnyIO thread-pool slot."""
    from google.api_core import exceptions as gax_exc
    from src.utils import run_query_with_retry_limit

    client = MagicMock()
    client.query.side_effect = gax_exc.BadRequest("Query exceeded limit for bytes billed")
    with pytest.raises(gax_exc.BadRequest):
        run_query_with_retry_limit(client, "SELECT 1", MagicMock(), max_attempts=5)
    assert client.query.call_count == 1, "BadRequest must not be retried"


def test_bytes_billed_error_names_the_cap(test_client, mock_bq_all):
    """The generic 'malformed query' message misdirects users who hit the cap."""
    from google.api_core import exceptions as gax_exc
    mock_bq_all.query.side_effect = gax_exc.BadRequest(
        "Query exceeded limit for bytes billed: 214748364800."
    )
    resp = test_client.post("/api/mv/analyze",
                            json={"org_project_id": "p", "region": "region-us"})
    assert resp.status_code == 400
    detail = resp.json()["detail"]
    assert "max_bytes_billed_gb" in detail
    assert "malformed" not in detail.lower()
```

---

### F7 — Chargeback config written to the container filesystem

**Severity:** 🟠 High — data loss and cross-instance divergence in production
**File:** [`src/cost_attribution.py:19`](src/cost_attribution.py#L19), [`:104-123`](src/cost_attribution.py#L104)

#### The problem

```python
CONFIG_FILE = Path(__file__).parent / "cost_attribution_config.json"
```

```python
def save_config(config: CostAttributionConfig):
    try:
        with open(CONFIG_FILE, "w") as f:
            json.dump(config.model_dump(), f, indent=2)
    except Exception as e:
        logger.error(f"Failed to save config: {e}")
        raise HTTPException(status_code=500, detail="Failed to save configuration")
```

`Path(__file__).parent` is `/app/src` inside the container. The `Dockerfile` confirms the deployment target:

```dockerfile
FROM python:3.11-slim
COPY src/ src/
EXPOSE 8080
CMD uvicorn src.main:app --host 0.0.0.0 --port 8080
```

`0.0.0.0:8080` + `EXPOSE 8080` is the Cloud Run contract.

#### Why this is wrong

On Cloud Run the container filesystem is **in-memory (tmpfs), per-instance, and ephemeral**. Consequences for the data that drives every chargeback number:

1. **Lost on every cold start / revision deploy / instance recycle.** SKU rates and admin bill totals silently revert to the image defaults (`reservations: {}`), which — per **F3** — makes `/calculate` return an empty attribution list with HTTP 200.
2. **Divergence under autoscaling.** With `min-instances > 1` or during a scale-out, instance A has the saved config and instance B has the image default. Which one answers a request is arbitrary. `GET /config` immediately after `POST /config` can legitimately return the old values.
3. **Counts against the memory limit.** tmpfs writes consume the instance's RAM allocation.
4. **The file is committed to the repo and baked into the image.** `src/cost_attribution_config.json` is not in `.gitignore`, so whatever a developer last saved locally ships to production as the default.
5. **Read errors are correctly not masked** — the docstring on `load_config` explicitly reasons about this and it is right. But that careful handling is undermined by a storage layer that loses the file routinely, making "file missing → defaults" the common path rather than the initial-state path it was designed for.

#### The fix — recommended: GCS with a generation precondition

The data is one small JSON object with infrequent writes and a strong need for read-after-write consistency across instances. GCS is the smallest correct change; `google-cloud-storage` is a transitive dependency of `google-cloud-bigquery` and needs no new requirement.

**File:** `src/cost_attribution.py`

**Before** (lines 19, 89-110):

```python
CONFIG_FILE = Path(__file__).parent / "cost_attribution_config.json"
```

**After:**

```python
# Config persistence.
#
# Cloud Run's filesystem is in-memory, per-instance, and ephemeral, so a
# local file loses SKU rates and admin bill totals on every cold start and
# diverges across instances during autoscaling. Both failure modes are
# silent: a lost config makes /calculate return an empty attribution list
# with HTTP 200 (see F3).
#
# COST_ATTRIBUTION_CONFIG_URI accepts:
#   gs://bucket/path/config.json   -> GCS (required in production)
#   unset                          -> local file (development only)
_CONFIG_URI = os.getenv("COST_ATTRIBUTION_CONFIG_URI", "").strip()
_LOCAL_CONFIG_FILE = Path(__file__).parent / "cost_attribution_config.json"


def _gcs_blob(uri: str):
    """Resolve a gs:// URI to a Blob. Imported lazily so dev runs without GCS."""
    from google.cloud import storage
    if not uri.startswith("gs://"):
        raise ValueError(f"Unsupported config URI scheme: {uri!r}")
    bucket_name, _, blob_path = uri[len("gs://"):].partition("/")
    if not bucket_name or not blob_path:
        raise ValueError(f"Malformed GCS URI: {uri!r}")
    return storage.Client().bucket(bucket_name).blob(blob_path)


def load_config() -> CostAttributionConfig:
    """Load the saved config, or defaults if none has been saved yet.

    A missing object/file is a legitimate initial state (returns defaults). One
    that exists but fails to parse/validate is a real problem — callers must
    handle that explicitly rather than have it silently masked as defaults,
    which would make every reservation appear "unconfigured" with no indication
    that the stored config was actually lost or corrupted.
    """
    if _CONFIG_URI:
        from google.api_core import exceptions as gax_exc
        blob = _gcs_blob(_CONFIG_URI)
        try:
            data = json.loads(blob.download_as_bytes())
        except gax_exc.NotFound:
            return CostAttributionConfig()
        return CostAttributionConfig(**data)

    if not os.path.exists(_LOCAL_CONFIG_FILE):
        return CostAttributionConfig()
    with open(_LOCAL_CONFIG_FILE, "r") as f:
        data = json.load(f)
    return CostAttributionConfig(**data)


def save_config(config: CostAttributionConfig):
    payload = json.dumps(config.model_dump(), indent=2)
    try:
        if _CONFIG_URI:
            _gcs_save(payload)
        else:
            with open(_LOCAL_CONFIG_FILE, "w") as f:
                f.write(payload)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to save config: {e}")
        raise HTTPException(status_code=500, detail="Failed to save configuration")


def _gcs_save(payload: str, _attempt: int = 0):
    """Write the config object, refusing to clobber a concurrent write.

    if_generation_match=0 means "only if this object does not exist yet".
    Supplying a non-zero generation means "only if the object is still at
    exactly that generation". Either way GCS returns 412 PreconditionFailed
    if another instance wrote in between, so a lost update becomes a visible
    error instead of silent data loss.

    Note the deliberate absence of blob.exists() / blob.reload() before the
    upload: that pattern costs two extra round trips AND is still racy, since
    the object can change between the check and the write. Attempt the create
    first and let the 412 tell us the truth.
    """
    from google.api_core import exceptions as gax_exc
    blob = _gcs_blob(_CONFIG_URI)
    try:
        blob.upload_from_string(
            payload, content_type="application/json", if_generation_match=0
        )
        return
    except gax_exc.PreconditionFailed:
        pass  # Object already exists — fall through to a generation-matched update.

    if _attempt >= 1:
        raise HTTPException(
            status_code=409,
            detail="Configuration was modified concurrently. Reload and retry.",
        )
    blob.reload()  # Populates blob.generation with the current value.
    try:
        blob.upload_from_string(
            payload,
            content_type="application/json",
            if_generation_match=blob.generation,
        )
    except gax_exc.PreconditionFailed:
        # Raced again between reload() and upload — retry once, then give up.
        _gcs_save(payload, _attempt + 1)
```

##### ⚠️ Deployment prerequisite: IAM (do this **before** shipping the code)

This change makes the service account's GCS permissions load-bearing. If it is deployed without them, `/api/cost-attribution/config` starts returning `500` for both reads and writes — the settings page breaks completely, and the failure looks like an application bug rather than a missing role.

| Item | Value |
|---|---|
| Identity | The Cloud Run service's runtime service account (**not** the default compute SA unless that is what the service actually uses — check `gcloud run services describe <svc> --format='value(spec.template.spec.serviceAccountName)'`) |
| Permissions needed | `storage.objects.get`, `storage.objects.create`, **and `storage.objects.delete`** — GCS models overwrite as delete+create, so a role without `delete` fails every save after the first |
| Recommended role | `roles/storage.objectUser` — narrower than `objectAdmin` (no ACL/IAM management), and sufficient for get/create/delete/list |
| Acceptable alternative | `roles/storage.objectAdmin` — works, but grants object-ACL control this service does not need |
| Insufficient | `roles/storage.objectViewer` (read-only), `roles/storage.objectCreator` (**no overwrite** — the first save succeeds and every later one 412s/403s, which is a particularly confusing failure) |
| Scope | Grant on the **bucket**, not the project |

```bash
SA="$(gcloud run services describe bq-finops --region=us-central1 \
        --format='value(spec.template.spec.serviceAccountName)')"

gcloud storage buckets add-iam-policy-binding gs://my-finops-config \
  --member="serviceAccount:${SA}" \
  --role="roles/storage.objectUser"

gcloud run services update bq-finops --region=us-central1 \
  --set-env-vars=COST_ATTRIBUTION_CONFIG_URI=gs://my-finops-config/cost_attribution.json
```

Also confirm the bucket exists, is in the same region as the service (latency), and has **versioning enabled** — versioning turns an accidental bad save into a recoverable one, which matters for data that drives chargeback numbers.

**Startup guard** — fail loudly rather than losing data silently. Add near the `AUTH_ENFORCED_UPSTREAM` check at [`main.py:126`](src/main.py#L126):

```python
if os.getenv("K_SERVICE"):
    _uri = os.getenv("COST_ATTRIBUTION_CONFIG_URI", "").strip()
    if not _uri:
        raise RuntimeError(
            "COST_ATTRIBUTION_CONFIG_URI must be set when running on Cloud Run "
            "(K_SERVICE is present). Without it, cost attribution config is "
            "written to an ephemeral per-instance filesystem and is lost on "
            "every cold start."
        )
    # Env-var presence proves configuration, not access. Probe the actual
    # permission at startup so an IAM misconfiguration surfaces as a failed
    # revision rollout — which Cloud Run rolls back automatically — instead of
    # as 500s on the settings page hours later.
    try:
        from .cost_attribution import load_config
        load_config()
    except Exception as e:
        raise RuntimeError(
            f"Cannot read cost attribution config at {_uri}: {type(e).__name__}: {e}. "
            "Grant the Cloud Run service account roles/storage.objectUser on the "
            "bucket (needs objects.get, objects.create AND objects.delete)."
        ) from e
```

`K_SERVICE` is injected automatically by Cloud Run, so this cannot be forgotten. Note the probe only exercises the **read** path; a create-only role such as `objectCreator` still passes it. The role table above is the authoritative check.

**Also:** add to `.gitignore` so a developer's local config never ships in the image:

```gitignore
src/cost_attribution_config.json
```

and remove the tracked copy:

```bash
git rm --cached src/cost_attribution_config.json
```

#### Alternatives considered

| Option | Verdict |
|---|---|
| **Firestore** | Better for concurrent writes and audit history, but adds a dependency and IAM surface for one small document. Overkill at this scale. |
| **Secret Manager** | Versioned and access-controlled, but rates and bill totals are not secrets, and the version-per-write model is a poor fit for a settings page. |
| **BigQuery table** | Consistent with the rest of the stack, but a full query round-trip to read a settings blob on every `/calculate` is slow and costs money. |
| **GCS object** | ✅ Smallest correct change, no new dependency, strong read-after-write consistency, generation preconditions handle concurrency. Costs one IAM grant — see the deployment prerequisite above; ship the role before the code. |

---

## 5. Medium Findings

### F8 — Tiered recommendations omit the job-type filters every sibling query has

**Severity:** 🟡 Medium — recommendations that do not reconcile with adjacent panels
**File:** [`src/main.py:2510-2521`](src/main.py#L2510)

#### The problem

```sql
        WITH per_second_usage AS (
          SELECT
            period_start,
            reservation_id,
            SUM(period_slot_ms) / 1000 AS concurrent_slots
          FROM
            `{resolved_project}`.`{params.region}`.INFORMATION_SCHEMA.{table_name}
          WHERE
            period_start > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {params.lookback_days} DAY)
          GROUP BY
            period_start, reservation_id
        ),
```

Compare its three siblings, all reading the same view:

| Endpoint | Line | `job_type = 'QUERY'` | `statement_type != 'SCRIPT'` |
|---|---|---|---|
| `/api/slots/analyze` | [2325](src/main.py#L2325) | — | ✅ |
| `/api/slots/utilization` | [2617](src/main.py#L2617) | ✅ | ✅ |
| `/api/slots/peak` | [3432](src/main.py#L3432) | ✅ | ✅ |
| **`/api/slots/tiered_recommendations`** | **2518** | ❌ | ❌ |

#### Why this is wrong

Two independent inflations:

1. **Non-query jobs counted.** `LOAD`, `COPY`, and `EXTRACT` jobs appear in `JOBS_TIMELINE_*` and consume slots, but they are not the workload that autoscaler baselines are sized for.

2. **Scripts double-counted.** This is the more serious one. A multi-statement script emits a **parent** job row *and* one row per child statement, with the parent's `period_slot_ms` overlapping its children's. Summing both inflates concurrent slots for that second by roughly 2×. The sibling queries all filter `(statement_type != 'SCRIPT' OR statement_type IS NULL)` for exactly this reason — the `OR ... IS NULL` half matters because non-script jobs have a NULL `statement_type` and a bare `!=` would drop them.

Since this query then takes `MAX(concurrent_slots)` per minute and feeds percentiles over per-minute peaks, the inflation lands directly on the p80/p95/max baseline recommendations. Those are the numbers a user acts on when resizing a reservation — and they will be visibly higher than the peak-slots figure rendered next to them from `/api/slots/peak`, which does filter correctly.

#### The fix

**File:** `src/main.py`, inside `get_sql()`.

**Before:**

```sql
          FROM
            `{resolved_project}`.`{params.region}`.INFORMATION_SCHEMA.{table_name}
          WHERE
            period_start > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {params.lookback_days} DAY)
          GROUP BY
            period_start, reservation_id
```

**After:**

```sql
          FROM
            `{resolved_project}`.`{params.region}`.INFORMATION_SCHEMA.{table_name}
          WHERE
            period_start > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {params.lookback_days} DAY)
            -- Match the filters used by /api/slots/analyze, /api/slots/utilization
            -- and /api/slots/peak. Without them: (1) LOAD/COPY/EXTRACT slot usage
            -- inflates the baseline, and (2) multi-statement scripts are counted
            -- twice, because the parent job row and its child statement rows both
            -- report period_slot_ms for the same second.
            -- The `OR statement_type IS NULL` half is required: non-script jobs
            -- have a NULL statement_type and a bare != would drop them.
            AND job_type = 'QUERY'
            AND (statement_type != 'SCRIPT' OR statement_type IS NULL)
          GROUP BY
            period_start, reservation_id
```

⚠️ Expect recommended baselines to **drop** after this change. That is the correction, but note it in release notes — a user who screenshotted last week's recommendation will otherwise report it as a bug.

#### Tests

```python
def test_tiered_recommendations_filter_scripts_and_non_queries(test_client, mock_bq_all):
    """F8: script parent + child rows double-count slots for the same second."""
    test_client.post("/api/slots/tiered_recommendations",
                     json={"org_project_id": "p", "region": "region-us"})
    sql = mock_bq_all.query.call_args[0][0]
    assert "job_type = 'QUERY'" in sql
    assert "statement_type != 'SCRIPT' OR statement_type IS NULL" in sql


@pytest.mark.parametrize("endpoint", [
    "/api/slots/analyze", "/api/slots/utilization",
    "/api/slots/peak", "/api/slots/tiered_recommendations",
])
def test_all_timeline_readers_exclude_scripts(test_client, mock_bq_all, endpoint):
    """Invariant: every JOBS_TIMELINE_* consumer must exclude script parents."""
    test_client.post(endpoint, json={"org_project_id": "p", "region": "region-us"})
    sqls = " ".join(c[0][0] for c in mock_bq_all.query.call_args_list)
    assert "SCRIPT" in sqls
```

---

### F9 — Fluid-scaling capacity fallback hardcodes `borrowed_slots = 0`

**Severity:** 🟡 Medium — inconsistently understates the savings estimate
**File:** [`src/fluid_scaling.py:281-292`](src/fluid_scaling.py#L281)

#### The problem

```sql
  UNNEST(
    IF(ARRAY_LENGTH(per_second_details) > 0,
       ARRAY(
         SELECT AS STRUCT d.start_time, d.autoscale_current_slots, d.slots_assigned, d.borrowed_slots
         FROM UNNEST(per_second_details) AS d
       ),
       ARRAY(
         SELECT AS STRUCT
           ts AS start_time,
           autoscale.current_slots AS autoscale_current_slots,
           slots_assigned AS slots_assigned,
           0 AS borrowed_slots          -- ← fabricated
         FROM UNNEST(GENERATE_TIMESTAMP_ARRAY(
           rt.period_start,
           TIMESTAMP_ADD(rt.period_start, INTERVAL 59 SECOND),
           INTERVAL 1 SECOND
         )) AS ts
       )
    )
  ) AS s
```

#### Why this is wrong

The primary branch reads the real per-second `d.borrowed_slots`; the synthetic fallback branch — used whenever `per_second_details` is empty for a minute — fabricates `0`.

That value feeds directly into the core formula at [`fluid_scaling.py:344`](src/fluid_scaling.py#L344):

```sql
SUM(LEAST(GREATEST(IFNULL(used_slots, 0) - IFNULL(borrowed_slots, 0) - IFNULL(baseline_slots, 0), 0), IFNULL(current_slots, 0))) AS fluid_slot_seconds
```

With `borrowed_slots = 0`, slots that were actually *borrowed* from another reservation are misattributed to the fluid autoscaler. `fluid_slot_seconds` inflates. Since savings is computed as `legacy − fluid` ([`fluid_scaling.py:400`](src/fluid_scaling.py#L400)), an inflated `fluid_slot_seconds` **reduces** the reported savings.

The estimate therefore **understates** savings — and does so *inconsistently*, depending on which minutes happened to carry `per_second_details`. Two runs over overlapping windows can disagree. Understating is the safer direction for a savings claim, but non-determinism in a financial projection undermines the whole number.

There is a related nit two lines below in the same block: `INTERVAL @lookback_days + 7 DAY` at [`fluid_scaling.py:305`](src/fluid_scaling.py#L305). BigQuery parses this as `INTERVAL (@lookback_days + 7) DAY`, which is the intent — but the precedence is non-obvious to readers and one edit away from being wrong. Parenthesize it.

#### The fix

**File:** `src/fluid_scaling.py`

**Option A (recommended) — make the fabrication explicit and flag affected rows.** The minute-grain `rt` row has no borrowed-slots column to fall back to, so the value genuinely cannot be recovered. Make the uncertainty visible instead of silently baking it in:

**Before:**

```sql
       ARRAY(
         SELECT AS STRUCT
           ts AS start_time,
           autoscale.current_slots AS autoscale_current_slots,
           slots_assigned AS slots_assigned,
           0 AS borrowed_slots
         FROM UNNEST(GENERATE_TIMESTAMP_ARRAY(
```

**After:**

```sql
       ARRAY(
         SELECT AS STRUCT
           ts AS start_time,
           autoscale.current_slots AS autoscale_current_slots,
           slots_assigned AS slots_assigned,
           -- RESERVATION_TIMELINE_BY_PROJECT exposes borrowed_slots only inside
           -- per_second_details. When that array is empty we fabricate a
           -- per-second series from the minute row, and borrowed slots are
           -- genuinely unrecoverable at this grain.
           --
           -- 0 is the conservative choice: it means borrowed capacity is
           -- attributed to the fluid autoscaler, which INFLATES
           -- fluid_slot_seconds and therefore UNDERSTATES slot_hours_saved.
           -- We prefer understating a savings claim to overstating one.
           -- `is_synthetic` propagates so the caller can report coverage.
           0 AS borrowed_slots,
           TRUE AS is_synthetic
         FROM UNNEST(GENERATE_TIMESTAMP_ARRAY(
```

Add `FALSE AS is_synthetic` to the primary branch (struct field lists must match), carry it through `capacity_per_sec` and `joined_per_sec`, and expose coverage in the final `SELECT`:

```sql
  COUNTIF(is_synthetic) AS synthetic_capacity_seconds,
  COUNT(*)              AS total_capacity_seconds,
```

Then surface `synthetic_pct` on `FluidEstimateMetric` and render it as a confidence indicator. A reservation whose estimate is 90% synthetic deserves a different level of trust than one at 2%.

**Option B (minimum viable)** — if plumbing `is_synthetic` is too invasive, at minimum add the explanatory comment above and log the coverage ratio in `_process_unified_results` so it appears in the server log.

**Also fix the precedence nit** at [`fluid_scaling.py:305`](src/fluid_scaling.py#L305):

```sql
  WHERE job_creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL (@lookback_days + 7) DAY)
```

#### Tests

`tests/test_design_invariants.py` already asserts §7.3 *"both capacity paths produce equivalent slot-seconds"*. Extend it to cover the borrowed-slots divergence, which is precisely where the two paths are **not** equivalent:

```python
def test_synthetic_capacity_path_diverges_only_on_borrowed_slots():
    """F9: the fabricated per-second series cannot recover borrowed_slots.
    Document the known divergence so it is a choice, not an accident."""
    real = _compute_fluid_seconds(used=100, borrowed=30, baseline=50, current=200)
    synth = _compute_fluid_seconds(used=100, borrowed=0, baseline=50, current=200)
    assert synth > real, "synthetic path over-attributes to the autoscaler"
    assert synth - real == 30, "divergence must equal exactly the borrowed slots"
```

---

### F10 — `fairness_enabled` is last-writer-wins across admin projects

**Severity:** 🟡 Medium — non-deterministic response field
**File:** [`src/main.py:2406`](src/main.py#L2406), [`:2431`](src/main.py#L2431)

#### The problem

```python
        fairness_enabled = False
        for admin_proj in admin_projects:
            # Query Project Options for Fluid Scaling and Fairness
            fluid_enabled_reservations = set()      # ← re-scoped per iteration ✅
            ...
                    elif name == 'enable_reservation_based_fairness' and val:
                        fairness_enabled = (val.lower() == 'true')   # ← overwritten ❌
```

#### Why this is wrong

`fairness_enabled` is initialized **once outside** the loop and **reassigned inside** it. With two admin projects that disagree, the returned value is whichever project was visited last.

`admin_projects` is a **`set`** ([`main.py:2394`](src/main.py#L2394)), built from a set comprehension over reservation IDs. Python set iteration order for strings depends on hash randomization, which varies per process unless `PYTHONHASHSEED` is fixed. So the same request can return `true` on one Cloud Run instance and `false` on another, and flip between cold starts on the same instance.

The contrast with `fluid_enabled_reservations` directly above is telling: that variable **is** correctly re-scoped inside the loop and its results are attached per-reservation. The bug is specific to this one variable, which suggests it was an oversight rather than a design decision.

There is a secondary issue: the response field name `fairness_enabled` implies a single org-wide fact, but reservation-based fairness is a **per-administration-project** setting. Even fixed, a single boolean cannot represent a mixed configuration honestly.

#### The fix

**File:** `src/main.py`

**Step 1** — Collect per project.

**Before** ([`main.py:2406`](src/main.py#L2406)):

```python
        fairness_enabled = False
        for admin_proj in admin_projects:
```

**After:**

```python
        # Reservation-based fairness is a PER-ADMIN-PROJECT setting. Collapsing it
        # into one boolean inside the loop made the result last-writer-wins, and
        # because admin_projects is a set, iteration order (and therefore the
        # answer) varied between processes.
        fairness_by_project: dict[str, bool] = {}
        for admin_proj in admin_projects:
```

**Step 2** — Record instead of overwrite.

**Before** ([`main.py:2431`](src/main.py#L2431)):

```python
                    elif name == 'enable_reservation_based_fairness' and val:
                        fairness_enabled = (val.lower() == 'true')
```

**After:**

```python
                    elif name == 'enable_reservation_based_fairness' and val:
                        fairness_by_project[admin_proj] = (val.lower() == 'true')
```

**Step 3** — Return both the map and an explicit aggregate.

**Before** ([`main.py:2470-2474`](src/main.py#L2470)):

```python
        return {
            "recommendations": recommendations_data,
            "current_reservations": current_reservations_data,
            "fairness_enabled": fairness_enabled
        }
```

**After:**

```python
        # `fairness_enabled` is retained for backward compatibility with the
        # existing UI, now defined explicitly as "enabled in ANY admin project"
        # rather than "whichever project the set happened to yield last".
        return {
            "recommendations": recommendations_data,
            "current_reservations": current_reservations_data,
            "fairness_enabled": any(fairness_by_project.values()),
            "fairness_by_project": fairness_by_project,
            "fairness_is_mixed": len(set(fairness_by_project.values())) > 1,
        }
```

**Step 4** — In the UI, when `fairness_is_mixed` is true, render "Mixed" rather than a single on/off state, with the per-project breakdown in a tooltip.

#### Tests

```python
def test_fairness_is_deterministic_across_admin_projects(test_client, mock_bq_all):
    """F10: admin_projects is a set; last-writer-wins made this vary by
    hash-randomized iteration order."""
    resp = test_client.post("/api/slots/analyze",
                            json={"org_project_id": "p", "region": "region-us"})
    body = resp.json()
    assert "fairness_by_project" in body
    assert body["fairness_enabled"] == any(body["fairness_by_project"].values())
```

---

### F11 — HBO hardcodes $0.06/slot-hour

**Severity:** 🟡 Medium — savings overstated by 20–40% for committed-use customers
**File:** [`src/hbo.py:129`](src/hbo.py#L129), [`src/hbo.py:205`](src/hbo.py#L205)

#### The problem

```python
                estimated_savings = saved_slot_hours * 0.06
```

```python
            daily_usd_avg = (total_saved_slot_hours * 0.06) / lookback
```

#### Why this is wrong

$0.06/slot-hour is the BigQuery Enterprise Edition **pay-as-you-go list rate**. Every other module in this codebase parameterizes the price:

| Module | Parameter | Default |
|---|---|---|
| `FluidEstimateParams` | `price_per_slot_hr` | 0.06 |
| `JobAnalysisParams` | `edition_slot_hr_rate` | 0.06 |
| `SlotSimulationParams` | `payg_price` / `commit_1yr_price` / `commit_3yr_price` | 0.06 / 0.048 / 0.036 |
| `UserProfilerParams` | `ed_price` | 0.06 |
| **`HBOCommonParams`** | **— none —** | **hardcoded 0.06** |

The codebase's *own* defaults establish that 1-year commitments cost $0.048 and 3-year $0.036. A customer on a 3-year commitment sees HBO savings overstated by **67%** relative to the true rate, and inconsistent with the Fluid Scaling tile rendered on the same dashboard using the same underlying slot-hours.

This is exactly the class of error that erodes trust in a FinOps tool: two tiles, same input, different money.

#### The fix

**File:** `src/hbo.py`

**Step 1** — Add the parameter to the shared base so both endpoints inherit it.

**Before** ([`hbo.py:44-52`](src/hbo.py#L44)):

```python
class HBOCommonParams(FocusMixin):
    org_project_id: Optional[str] = None
    region: str = "region-us"
    lookback_days: int = Field(default=7, ge=1, le=MAX_LOOKBACK_DAYS)
    max_bytes_billed_gb: Optional[int] = None

class HBOAnalyzeParams(HBOCommonParams):
    limit: int = 10
```

**After:**

```python
class HBOCommonParams(FocusMixin):
    org_project_id: Optional[str] = None
    region: str = "region-us"
    lookback_days: int = Field(default=7, ge=1, le=MAX_LOOKBACK_DAYS)
    max_bytes_billed_gb: Optional[int] = None
    # Slot-hour price. Default is the Enterprise Edition PAYG list rate; callers
    # on a committed-use plan should pass their effective rate (the codebase's
    # own simulator defaults are 0.048 for 1-year and 0.036 for 3-year).
    # Previously hardcoded to 0.06 in two places, which overstated savings by up
    # to 67% for 3-year-commit customers and disagreed with the Fluid Scaling
    # tile computed from the same slot-hours.
    price_per_slot_hr: float = Field(default=0.06, gt=0, le=100)

class HBOAnalyzeParams(HBOCommonParams):
    limit: int = Field(default=10, ge=1, le=1000)   # see F13
```

**Step 2** — Use it in `analyze_hbo`.

**Before** ([`hbo.py:129`](src/hbo.py#L129)):
```python
                estimated_savings = saved_slot_hours * 0.06
```
**After:**
```python
                estimated_savings = saved_slot_hours * params.price_per_slot_hr
```

**Step 3** — Use it in `get_hbo_summary`.

**Before** ([`hbo.py:205`](src/hbo.py#L205)):
```python
            daily_usd_avg = (total_saved_slot_hours * 0.06) / lookback
```
**After:**
```python
            daily_usd_avg = (total_saved_slot_hours * params.price_per_slot_hr) / lookback
```

**Step 4** — Send the user's configured rate from the frontend. Wherever `/api/hbo/analyze` and `/api/hbo/summary` are called in `static/app.js` (and `docs/static/app.js`), add `price_per_slot_hr` sourced from the same state field the Fluid Scaling and Simulator views already use. This is the point of the change — without it, the tiles still disagree.

**Step 5** — Audit for other bare `0.06` literals:

```bash
grep -rn "\* 0\.06\|0\.06 \*\|= 0\.06" src/ | grep -v "default"
```

#### Tests

```python
def test_hbo_honours_committed_use_rate(test_client, mock_bq_all, mock_bq_row_factory):
    """F11: a 3-year-commit customer at 0.036 must not be quoted list rate."""
    rows = [mock_bq_row_factory(job_id="j1", user_email="u@x.com", query_hash="h",
                                start_time=None, end_time=None, duration_ms=1000,
                                total_slot_ms=3_600_000, prev_exec_ms=2000)]
    mock_bq_all.query.return_value.result.return_value.__iter__ = lambda self: iter(rows)

    resp = test_client.post("/api/hbo/analyze", json={
        "org_project_id": "p", "region": "region-us", "price_per_slot_hr": 0.036,
    })
    assert resp.status_code == 200
    # 50% saved × 1 slot-hour × $0.036
    assert resp.json()[0]["estimated_savings_usd"] == pytest.approx(0.018, abs=1e-6)


def test_hbo_price_defaults_to_list_rate(test_client, mock_bq_all):
    resp = test_client.post("/api/hbo/analyze",
                            json={"org_project_id": "p", "region": "region-us"})
    assert resp.status_code == 200   # omitting the field must not 422
```

---

### F12 — HBO analyze and summary report on different bases, and top-N is biased

**Severity:** 🟡 Medium — two numbers on one screen that cannot be reconciled
**File:** [`src/hbo.py:111-145`](src/hbo.py#L111), [`src/hbo.py:196-216`](src/hbo.py#L196)

#### Problem 12a — mismatched time bases

`/summary` projects to a month ([`hbo.py:196-216`](src/hbo.py#L196)):

```python
            daily_slot_avg = total_saved_slot_hours / lookback
            daily_usd_avg = (total_saved_slot_hours * 0.06) / lookback
            monthly_saved_slot_hours = daily_slot_avg * DAYS_PER_MONTH
            monthly_estimated_savings_usd = daily_usd_avg * DAYS_PER_MONTH
```

`/analyze` returns raw window values per job ([`hbo.py:127-129`](src/hbo.py#L127)):

```python
                saved_slot_hours = (percent_saved / 100) * ((row.total_slot_ms or 0) / 3600000.0)
                estimated_savings = saved_slot_hours * 0.06
```

Both feed the same HBO screen. Summing the `analyze` table will **never** match the `summary` tile — off by a factor of `DAYS_PER_MONTH / lookback_days` (≈4.3× at the default 7-day lookback), *plus* the effect of 12b below.

The only signal is `time_base: str = "monthly_projected"` buried in the `HBOSummary` model ([`hbo.py:69`](src/hbo.py#L69)). That is a good instinct — someone knew — but a field the UI does not render is not a disclosure.

#### Problem 12b — biased top-N

```sql
        ORDER BY
          total_slot_ms DESC
        LIMIT 1000
```

then in Python:

```python
        output.sort(key=lambda x: x.percent_execution_time_saved, reverse=True)
        return output[:params.limit]
```

SQL ranks by `total_slot_ms`, takes 1000, then Python re-ranks the survivors by `percent_execution_time_saved`. The result is not "top 10 by % saved" — it is **"top 10 by % saved among the 1000 largest-slot jobs."** A job with a 95% improvement that consumed modest slots can never appear, no matter how instructive it is.

The two stages also disagree about what "top" means, so the answer changes discontinuously as the 1000-row cut moves.

#### The fix

**Step 1 — rank in SQL on the actual ranking key.**

**Before** ([`hbo.py:110-114`](src/hbo.py#L110)):

```sql
          {focus_clause}
        ORDER BY 
          total_slot_ms DESC
        LIMIT 1000
        """
```

**After:**

```sql
          {focus_clause}
          -- Restrict to rows the Python loop will actually keep. Without this,
          -- jobs with no performance-insights baseline
          -- (avg_previous_execution_ms NULL or 0 -> SAFE_DIVIDE returns NULL)
          -- consume slots inside the LIMIT and are then discarded by the
          -- `if prev_exec_ms > 0` guard, so the endpoint silently returns
          -- FEWER than `limit` rows.
          AND IFNULL(query_info.performance_insights.avg_previous_execution_ms, 0) > 0
        -- Rank on the SAME key the caller ranks on. Previously this ordered by
        -- total_slot_ms, took 1000, then re-sorted in Python by percent saved —
        -- so the "top N by % saved" could never include a high-percentage job
        -- that fell outside the 1000 largest-slot jobs.
        --
        -- NULLS LAST is redundant given the WHERE filter above (and given
        -- BigQuery's default DESC ordering — see §9.8), but it is stated
        -- explicitly so the intent survives a future edit to that filter.
        ORDER BY
          SAFE_DIVIDE(
            query_info.performance_insights.avg_previous_execution_ms
              - TIMESTAMP_DIFF(end_time, start_time, MILLISECOND),
            query_info.performance_insights.avg_previous_execution_ms
          ) DESC NULLS LAST
        LIMIT @row_limit
        """
```

> **⚠️ NULL-ordering note — BigQuery is the opposite of PostgreSQL here.** GoogleSQL treats NULL as the **minimum** possible value, so NULLs appear *first* in `ASC` sorts and **last** in `DESC` sorts. PostgreSQL and Oracle treat NULL as the maximum, which is why a `DESC` sort there leads with NULLs. So unfiltered NULL-ratio rows would **not** have flooded the top of this panel on BigQuery.
>
> The real hazard is the under-fill described in the comment above: the Python `prev_exec_ms > 0` guard drops rows *after* the SQL `LIMIT` has already counted them, so `limit=10` can return 3. Fix it in `WHERE` — do not rely on either engine's null-ordering convention.

Pass the limit as a parameter — this one is a `LIMIT` clause, not a partition predicate, so parameterizing is safe and does not affect pruning (contrast with the reasoning in **F1**):

```python
        results = _run_and_log(
            bq_client, sql, "HBO Raw Data", params=params,
            query_parameters=focus_params + [
                bigquery.ScalarQueryParameter("row_limit", "INT64", params.limit),
            ],
        )
```

**Step 2 — drop the redundant Python re-sort.**

**Before** ([`hbo.py:137-140`](src/hbo.py#L137)):

```python
        # Sort output by percent_saved descending
        output.sort(key=lambda x: x.percent_execution_time_saved, reverse=True)
        log_endpoint_end("HBO Analyze", t0, _logger=logger)
        return output[:params.limit]
```

**After:**

```python
        # Ordering and limiting are now done in SQL on the same key, so the
        # result is a true global top-N rather than a re-rank of a slot-biased
        # subset. The Python guard `if prev_exec_ms > 0` can still drop rows,
        # so slice defensively.
        log_endpoint_end("HBO Analyze", t0, _logger=logger)
        return output[:params.limit]
```

**Step 3 — make the time basis explicit on both payloads.**

Add to `HBOResult` ([`hbo.py:55`](src/hbo.py#L55)):

```python
class HBOResult(BaseModel):
    job_id: str
    percent_execution_time_saved: float
    new_elapsed_ms: int
    original_elapsed_ms: int
    saved_slot_hours: float
    estimated_savings_usd: float
    # Per-job figures are raw observations over the lookback window. The /summary
    # endpoint returns MONTHLY-PROJECTED totals, so these will not sum to it.
    time_base: str = "lookback_window"
```

**Step 4 — render the basis in the UI.** Label the summary tile "Projected monthly savings" and the table header "Savings observed in the last N days". Without this the API-level disclosure changes nothing for the user.

#### Tests

```python
def test_hbo_analyze_ranks_by_percent_in_sql(test_client, mock_bq_all):
    """F12b: ranking by total_slot_ms then re-sorting in Python yields
    'top N among the 1000 biggest', not 'top N'."""
    test_client.post("/api/hbo/analyze",
                     json={"org_project_id": "p", "region": "region-us", "limit": 5})
    sql = mock_bq_all.query.call_args[0][0]
    assert "ORDER BY\n          SAFE_DIVIDE" in sql or "SAFE_DIVIDE" in sql.split("ORDER BY")[1]
    assert "ORDER BY total_slot_ms DESC" not in sql


def test_hbo_result_declares_time_base(test_client, mock_bq_all, mock_bq_row_factory):
    """F12a: /analyze is window-bound, /summary is monthly-projected."""
    rows = [mock_bq_row_factory(job_id="j", user_email="u", query_hash="h",
                                start_time=None, end_time=None, duration_ms=100,
                                total_slot_ms=3_600_000, prev_exec_ms=200)]
    mock_bq_all.query.return_value.result.return_value.__iter__ = lambda self: iter(rows)
    resp = test_client.post("/api/hbo/analyze",
                            json={"org_project_id": "p", "region": "region-us"})
    assert resp.json()[0]["time_base"] == "lookback_window"
```

---

### F13 — `HBOAnalyzeParams.limit` is unbounded

**Severity:** 🟡 Medium — inconsistent with every comparable parameter; silently wrong on negatives
**File:** [`src/hbo.py:52`](src/hbo.py#L52)

#### The problem

```python
class HBOAnalyzeParams(HBOCommonParams):
    limit: int = 10
```

No `Field` constraints — unlike every comparable parameter in the codebase (`AIParams.limit`, `BIParams.limit`, `HygieneParams.limit`), all of which carry `ge`/`le` bounds and are explicitly covered by `tests/test_input_validation.py`.

#### Why this is wrong

Two concrete failures:

1. **Negative values silently truncate the wrong end.** `output[:params.limit]` with `limit=-5` is `output[:-5]` — Python drops the **last 5** results instead of returning 5. No error; a plausible-looking short list. This is the kind of bug that survives for years.
2. **Unbounded positives.** `limit=1000000` forces the full result set through Pydantic model construction and JSON serialization. After the **F12** fix it also becomes the SQL `LIMIT`, at which point it directly controls result-set size from BigQuery.

Because `limit` becomes a query parameter under **F12**, bounding it stops being cosmetic and becomes part of the query-cost story.

#### The fix

**Before:**

```python
class HBOAnalyzeParams(HBOCommonParams):
    limit: int = 10
```

**After:**

```python
class HBOAnalyzeParams(HBOCommonParams):
    # Bounded like every other *.limit in this codebase (AIParams, BIParams,
    # HygieneParams). Unbounded, a negative value silently reversed the slice
    # semantics: output[:-5] drops the LAST five rows instead of returning five.
    # This value is also the SQL LIMIT (see F12), so the ceiling is a real
    # result-size control, not just input hygiene.
    limit: int = Field(default=10, ge=1, le=1000)
```

#### Tests

Extend `tests/test_input_validation.py` alongside the existing limit-bound cases:

```python
@pytest.mark.parametrize("bad", [0, -1, -5, 1001, 10**9])
def test_hbo_analyze_rejects_out_of_range_limit(test_client, mock_bq_all, bad):
    """F13: limit=-5 previously became output[:-5], dropping the last 5 rows."""
    resp = test_client.post("/api/hbo/analyze", json={
        "org_project_id": "p", "region": "region-us", "limit": bad,
    })
    assert resp.status_code == 422
```

---

### F14 — `audit_type` is silently ignored, so both governance audits run on every click

**Severity:** 🟡 Medium — ~2× the necessary query cost on a governance scan
**Files:** [`src/main.py:2078`](src/main.py#L2078), [`static/app.js:3751`](static/app.js#L3751), [`static/app.js:3793`](static/app.js#L3793)

#### The problem

Both governance buttons send a discriminator:

```javascript
            const params = {
                org_project_id: state.orgProject,
                max_bytes_billed_gb: state.maxBytesBilledGb,
                region: state.region,
                focus_projects: state.focusProjects,
                audit_type: 'expiration'     // and 'filter' on the other button
            };
```

`GovernanceParams` has no `audit_type` field. `FocusMixin` does not set `extra="forbid"`. Pydantic therefore **discards it silently**, and `analyze_governance` unconditionally runs:

1. the expiration audit (`SCHEMATA` + `SCHEMATA_OPTIONS`)
2. the top-datasets query (`TABLE_STORAGE_BY_ORGANIZATION`)
3. the bulk partition-filter audit (`PARTITIONS` + `TABLE_OPTIONS` across 5 datasets)

and returns both result sets. Each button then renders one and discards the other:

```javascript
                cachedGov.filter_issues = govData.filter_issues || [];
```

#### Why this is wrong

Every click on either button pays for **both** audits — three queries when one was intended. The partition-filter audit is the expensive one (a `UNION ALL` across up to 10 `INFORMATION_SCHEMA` views plus a preceding `TABLE_STORAGE_BY_ORGANIZATION` aggregation), and the "Expiration" button pays for it in full while showing none of it.

The deeper issue is that the frontend and backend have **silently disagreed** about this contract for as long as `audit_type` has been sent. Nothing failed, nothing logged, so nobody noticed. This is the same class of failure as the **F1** deploy-order trap, from the same root cause: permissive extras on `FocusMixin`.

#### The fix

**File:** `src/main.py`

**Step 1** — Make the discriminator real.

**Before** ([`main.py:2078-2081`](src/main.py#L2078)):

```python
class GovernanceParams(FocusMixin):
    org_project_id: Optional[str] = None
    region: str = "region-us"
    max_bytes_billed_gb: Optional[int] = None
```

**After:**

```python
class GovernanceParams(FocusMixin):
    org_project_id: Optional[str] = None
    region: str = "region-us"
    max_bytes_billed_gb: Optional[int] = None
    # The UI has been sending this since the panels were split, but the field
    # did not exist, so Pydantic discarded it and BOTH audits ran on every
    # click — three queries where one was wanted. Literal-typed so an unknown
    # value is a 422 rather than another silent full run.
    audit_type: Literal["all", "expiration", "filter"] = "all"
```

**Import check:** add `Literal` to the `typing` import at [`main.py:6`](src/main.py#L6):

```python
from typing import Literal, Optional, List, Set, Tuple
```

**Step 2** — Honour it. Wrap each audit block:

```python
        expiration_issues = []
        if params.audit_type in ("all", "expiration"):
            # ... existing expiration audit, unchanged ...

        filter_issues = []
        if params.audit_type in ("all", "filter"):
            # ... existing top-datasets query + bulk partition audit, unchanged ...
```

`"all"` as the default preserves behaviour for any caller that omits the field, so this is backward compatible.

**Step 3** — Note the interaction with **F1**. Once `audit_type` exists on the model, adding `extra="forbid"` to `GovernanceParams` becomes safe. Do it in a **separate commit, after** this one lands and the frontend is confirmed to send only known fields. The `test_governance_endpoint_still_accepts_audit_type` guard from **F1** should then be updated to assert the field is *honoured*, not merely *tolerated*.

#### Tests

```python
@pytest.mark.parametrize("audit_type,expect_expiration,expect_filter", [
    ("all",        True,  True),
    ("expiration", True,  False),
    ("filter",     False, True),
])
def test_audit_type_limits_queries(test_client, mock_bq_all, audit_type,
                                   expect_expiration, expect_filter):
    """F14: audit_type was discarded, so both audits ran on every click."""
    test_client.post("/api/governance/analyze", json={
        "org_project_id": "p", "region": "region-us", "audit_type": audit_type,
    })
    sqls = " ".join(c[0][0] for c in mock_bq_all.query.call_args_list)
    assert ("SCHEMATA_OPTIONS" in sqls) is expect_expiration
    assert ("TABLE_STORAGE_BY_ORGANIZATION" in sqls) is expect_filter


def test_unknown_audit_type_rejected(test_client, mock_bq_all):
    resp = test_client.post("/api/governance/analyze", json={
        "org_project_id": "p", "region": "region-us", "audit_type": "everything",
    })
    assert resp.status_code == 422
```

---

## 6. Low Findings & Hygiene

### F15 — Sanitization by mutation-at-a-distance

**Severity:** 🟢 Low — currently correct; structurally fragile
**File:** [`src/main.py:2795-2815`](src/main.py#L2795)

```python
def _validate_safe_params(params):
    if hasattr(params, "region") and params.region:
        params.region = _safe_ident(_normalize_region(params.region), "region")
```

The function mutates `params` in place. Twenty-plus downstream f-strings then interpolate `{params.region}` on the assumption that it ran.

**This is not currently a vulnerability.** I verified all 26 `@app.post` handlers call `_validate_safe_params`, and `tests/test_security.py` covers 23 endpoints plus a dedicated `test_region_injection_blocked`. The concern is structural: the failure mode for endpoint #27 is a **silent injection**, not a test failure — the new endpoint simply will not appear in the hand-maintained `SECURITY_TEST_CASES` list.

**Fix direction** — move normalization into the type system so it cannot be skipped:

```python
class SafeParamsBase(BaseModel):
    """Base for every endpoint that interpolates identifiers into SQL.

    Validation runs in a field_validator, so it is impossible to construct an
    instance with an unsanitized region. Contrast _validate_safe_params(), which
    is correct only as long as every handler remembers to call it.
    """
    region: str = "region-us"

    @field_validator("region")
    @classmethod
    def _sanitize_region(cls, v: str) -> str:
        return _safe_ident(_normalize_region(v), "region")

    @field_validator("org_project_id", "admin_project_id", check_fields=False)
    @classmethod
    def _sanitize_project(cls, v: Optional[str], info) -> Optional[str]:
        if not v or not v.strip():
            return v
        ident = _safe_ident(v.strip(), info.field_name)
        reject_dummy_project(ident)
        return ident
```

⚠️ This changes error codes from **400 → 422** (Pydantic validation vs. `HTTPException`), which would break the 23 assertions in `test_security.py` expecting `400`. Not a reason to avoid the change — but it must be a deliberate, separately-tested migration, not a drive-by. Add a `RequestValidationError` handler mapping identifier failures back to 400 if the existing contract must hold.

Until then, a cheap structural guard:

```python
def test_every_post_endpoint_validates_params():
    """Fails when a new POST endpoint forgets _validate_safe_params."""
    import inspect
    from fastapi.routing import APIRoute
    from src.main import app

    missing = []
    for route in app.routes:
        if not isinstance(route, APIRoute) or "POST" not in (route.methods or ()):
            continue
        src = inspect.getsource(route.endpoint)
        if "_validate_safe_params" not in src and "validate_focus_projects" not in src:
            missing.append(route.path)
    assert not missing, f"POST endpoints missing param validation: {missing}"
```

---

### F16 — `_run_and_log` duplicated four times

**Severity:** 🟢 Low — but it is the root cause of F1 and F8
**Files:** [`main.py:704`](src/main.py#L704), [`cost_attribution.py:24`](src/cost_attribution.py#L24), [`hbo.py:14`](src/hbo.py#L14), [`fluid_scaling.py:424`](src/fluid_scaling.py#L424)

Four near-verbatim copies of the same 30-line function: `get_max_bytes_billed` → `QueryJobConfig` → `run_query_with_retry_limit` → timing → BQ Console URL → structured log. They differ only cosmetically (`main.py` splits into `run_query_and_log` / `run_query_to_df`; `fluid_scaling` folds in a `lookback_days` parameter).

This is the finding to fix *first structurally*, because **four copies of the query path is precisely what let F1 and F8 drift from house standards without anyone noticing.** There is no single place where "every query gets a time bound" could have been asserted.

**Fix** — one implementation in `src/utils.py`:

```python
def run_query_logged(
    client: bigquery.Client,
    sql: str,
    description: str = "Query",
    params=None,
    query_parameters=None,
    fetch_df: bool = False,
):
    """Single query-execution path: safety cap, bounded retry, timing,
    bytes accounting, and a clickable BQ Console URL.

    Consolidates four near-identical copies (main, hbo, cost_attribution,
    fluid_scaling). Having one path means invariants like "every
    JOBS_BY_ORGANIZATION read is time-bounded" can be asserted in one place.

    fetch_df=True returns a pandas DataFrame via the BigQuery Storage Read API
    (Arrow, streamed). fetch_df=False returns the RowIterator unmaterialised.
    """
    job = run_query_with_retry_limit(client, sql, job_config)
    result = job.result()
    ...
    if fetch_df:
        # MUST stay create_bqstorage_client=True. This is not a performance
        # nicety — it is the memory contract. The REST download path buffers
        # the whole result set as Python objects before building the frame,
        # which OOMs a memory-capped Cloud Run instance on the large
        # JOBS_TIMELINE reads in fluid_scaling. The Storage Read API streams
        # Arrow record batches instead, with a far smaller peak footprint.
        return result.to_dataframe(create_bqstorage_client=True)
    return result
```

⚠️ **Do not let the consolidation quietly change the DataFrame path.** The four copies being merged are not identical in this respect: `main.run_query_to_df` and `fluid_scaling`'s copy pass `create_bqstorage_client=True`, and a naive unification that writes plain `result.to_dataframe()` would compile, pass every test (the mocks in [`conftest.py:50`](tests/conftest.py#L50) return an empty `pd.DataFrame` regardless of arguments), and then OOM in production on the first large lookback. **Add an explicit assertion** so the mock cannot hide it:

```python
def test_fetch_df_uses_storage_read_api(mock_bq_all):
    """F16: the unified path must preserve the streamed-Arrow DataFrame
    contract. The conftest mock accepts any kwargs, so only an explicit
    assertion catches a regression here."""
    from src.utils import run_query_logged
    run_query_logged(mock_bq_all, "SELECT 1", "t", fetch_df=True)
    to_df = mock_bq_all.query.return_value.result.return_value.to_dataframe
    to_df.assert_called_once_with(create_bqstorage_client=True)
```

Also confirm `google-cloud-bigquery-storage` is a pinned requirement rather than an incidental transitive dependency — `create_bqstorage_client=True` **falls back silently to the REST path** with only a debug-level log if the package is missing, which reproduces the exact OOM this flag exists to prevent.

Then in each module: `from .utils import run_query_logged as _run_and_log` and delete the local copy. Keep `run_query_to_df` in `main.py` as a thin `fetch_df=True` wrapper so its ~15 call sites are unaffected.

Once consolidated, a single defensive check becomes possible:

```python
    if "JOBS_BY_ORGANIZATION" in sql or "JOBS_TIMELINE" in sql:
        if "creation_time" not in sql and "period_start" not in sql:
            logger.error(
                "%s reads a partitioned JOBS view with no time predicate — "
                "this scans the full 180-day retention. SQL:\n%s", description, sql
            )
```

Log-only in production; assert in tests.

---

### F17 — Dead code

**Severity:** 🟢 Low — misleading to readers

**(a) `reservations_sql` built and discarded.** [`main.py:2365-2376`](src/main.py#L2365) constructs a `RESERVATIONS` query against `resolved_project`, which is unconditionally overwritten at [`main.py:2435`](src/main.py#L2435) by the per-`admin_proj` version inside the loop. The first block never executes. **Delete lines 2365-2376.**

**(b) Unreachable `else` in cost attribution.** [`cost_attribution.py:141-145`](src/cost_attribution.py#L141):

```python
        if target_project:
            table_name = f"`{target_project}`.`{region}`.INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION"
        else:
            # Fallback to region-scoped view as in example
            table_name = f"`{region}`.INFORMATION_SCHEMA.JOBS"
```

`target_project` comes from `_safe_ident(...)` on the line above, which raises `HTTPException(400)` on empty input ([`utils.py:166`](src/utils.py#L166)). It is therefore always truthy and the `else` is unreachable. It is also *wrong* if it ever ran — `INFORMATION_SCHEMA.JOBS` is project-scoped, which would silently narrow an org-wide chargeback calculation. **Delete the branch:**

```python
        # _safe_ident raises on empty, so target_project is always set.
        table_name = f"`{target_project}`.`{region}`.INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION"
```

---

### F18 — Stale comment misdescribes the unified query

**Severity:** 🟢 Low
**File:** [`src/fluid_scaling.py:260-264`](src/fluid_scaling.py#L260)

```python
# Aggregates to the minute-grain inside BigQuery instead of returning every
# per-second row to the client. A 90-day lookback would otherwise materialize
# up to ~60x more rows per reservation (one per second) into a pandas
# DataFrame — enough to OOM a memory-capped Cloud Run instance. Summing here
# is mathematically identical to summing the same per-second rows client-side.
```

The query does not aggregate to minute grain — it aggregates to **reservation grain** (`GROUP BY reservation_id`, [`fluid_scaling.py:348`](src/fluid_scaling.py#L348)), returning one row per reservation.

The *reasoning* is correct and worth keeping; only the description is wrong, and it understates how good the optimization is.

**Fix:**

```python
# Aggregates all the way down to ONE ROW PER RESERVATION inside BigQuery rather
# than returning per-second rows to the client. A 90-day lookback would
# otherwise materialize ~7.8M rows per reservation into a pandas DataFrame —
# enough to OOM a memory-capped Cloud Run instance. Summing bounded per-second
# values here is mathematically identical to summing the same rows client-side.
```

---

### F19 — Unguarded `FULL OUTER JOIN` fan-out (verify first)

**Severity:** 🟢 Low — likely a non-issue, but the failure mode is silent
**File:** [`src/fluid_scaling.py:326-332`](src/fluid_scaling.py#L326)

```sql
  FROM capacity_per_sec c
  FULL OUTER JOIN usage_per_sec u
    ON c.reservation_id = u.reservation_id
   AND c.period_start = u.period_start
```

`usage_per_sec` is explicitly `GROUP BY reservation_id, period_start`, so it is unique on the join key. `capacity_per_sec` has **no such guarantee** — it is `RESERVATION_TIMELINE_BY_PROJECT` cross-joined with an unnested per-second array.

This is *probably* fine: `RESERVATION_TIMELINE_BY_PROJECT` should emit one row per reservation per minute for the administration project, so the unnested seconds should be unique. **I have not verified this against a live dataset and am not asserting a bug.**

But the failure mode if it is ever untrue — a reservation appearing under two `project_id` values, an overlapping timeline row at a boundary — is that `used_slots` fans out and `total_pure_used_seconds` silently multiplies. No error, no warning, just a wrong savings number. The guard costs one line:

```sql
),
capacity_dedup AS (
  SELECT * FROM capacity_per_sec
  -- One row per (reservation, second). RESERVATION_TIMELINE_BY_PROJECT should
  -- already guarantee this for a single admin project, but a duplicate here
  -- fans out usage_per_sec in the FULL OUTER JOIN below and silently
  -- multiplies total_pure_used_seconds with no error surfaced.
  QUALIFY ROW_NUMBER() OVER (PARTITION BY reservation_id, period_start) = 1
),
```

then join against `capacity_dedup`. Cheap insurance against an invisible failure.

---

### F20 — Cost attribution window semantics are undocumented

**Severity:** 🟢 Low — defensible behaviour, undisclosed
**File:** [`src/cost_attribution.py:158-160`](src/cost_attribution.py#L158)

```sql
            WHERE
              creation_time >= TIMESTAMP(@start_date)
              AND creation_time < TIMESTAMP(@end_date)
```

Two conventions are baked in and neither is stated in the API response:

1. **Attribution by `creation_time`, not by slot consumption.** A job created 31 Jan 23:50 that runs for an hour has *all* of its slot-hours attributed to January. Correct for most purposes and much cheaper than pro-rating across the timeline view — but it means a month boundary with long-running jobs will not match a per-second reconciliation.
2. **UTC boundaries.** `TIMESTAMP('2026-06-01')` is midnight **UTC**. A billing account reconciling on US/Pacific is offset by 7–8 hours at each end of the month.

The exclusive-end handling at [`cost_attribution.py:147-149`](src/cost_attribution.py#L147) is done correctly (`end_date + 1 day`, then `<`), which shows the boundary was thought about. The convention just is not disclosed.

**Fix** — add to the response payload:

```python
            "window": {
                "start": params.billing_month_start,
                "end_exclusive": exclusive_end_str,
                "timezone": "UTC",
                "attribution_basis": "job creation_time",
                "note": (
                    "Jobs are attributed wholly to the month they were CREATED in, "
                    "not pro-rated across their runtime. Boundaries are UTC."
                ),
            },
```

If a local-timezone billing month is genuinely required, add an optional `billing_timezone` param and use `TIMESTAMP(@start_date, @billing_tz)` — BigQuery's `TIMESTAMP()` accepts a timezone as its second argument.

---

### F21 — `get_anomalies()` returns fabricated data to a production UI

**Severity:** 🟢 Low — but a trust hazard
**File:** [`src/main.py:3912-3944`](src/main.py#L3912)

```python
    return [
        Anomaly(
            severity="critical",
            message="Project data-warehouse-prod spend +340% on Nov 14",
            deepLink="#cost-attribution?project=data-warehouse-prod"
        ),
        ...
    ]
```

The three sibling dashboard stubs are handled correctly — `get_kpis` returns `KpiResponse(stub=True)`, `get_opportunities` and `get_top_projects` return `[]`. `get_anomalies` is the outlier: it returns **three fabricated critical/warning anomalies naming plausible-looking projects, reservations, and a service-account email**, with working deep links.

A user landing on the dashboard sees "Project data-warehouse-prod spend +340% on Nov 14" rendered identically to a real finding. The `stub=True` flag that protects the KPI tile has no equivalent here.

**Fix** — match the sibling stubs:

```python
@app.get("/api/dashboard/anomalies", response_model=List[Anomaly])
def get_anomalies():
    """
    TODO: Real anomaly detection requires historical baseline.

    For v1, use this simple rule:
      For each project: compare last 7 days spend vs prior 7 days spend.
      Flag if change > 50% in either direction.
      Critical = >100% change. Warning = 50-100% change.

    `message` is plain text — never pre-built HTML. Once wired to real
    project/reservation/user data, those values must not be embedded into a
    trusted-HTML string; the frontend escapes `message` before display.

    Returns [] until implemented. This previously returned three fabricated
    anomalies that the UI rendered indistinguishably from real findings.
    """
    return []
```

If the sample data is wanted for demos, gate it behind the same mechanism `docs/simulator.html` already uses, or an explicit `DEMO_MODE` env var — never as the default production response.

---

## 7. Test Coverage Gap

The suite has **~87 tests across 17 files**. Input validation and pattern extraction are genuinely well covered:

- `test_security.py` — 23 endpoints × backtick injection, plus edition/resolution/region cases
- `test_input_validation.py` — limit bounds, focus-project count and format
- `test_max_bytes_billed.py` — clamping at both ends
- `test_retry_limit.py` — attempt cap enforced
- `test_focus_guard.py` / `test_focus_filter.py` — org-vs-focus contract, parameterization
- `test_patterns.py` — job-ID masking
- `test_physical_bytes_decomposition.py` — time-travel/fail-safe byte math
- `test_smoke_endpoints.py` — every endpoint returns 200 against an empty mock

**The gap:** of the 21 findings above, exactly **one** (`test_design_invariants.py::test_fluid_scaling_mode_no_60s_floor`, touching the `LEAST/GREATEST` clamp) is in the SQL-or-FinOps-math category. Everything else tests that endpoints do not crash and that bad inputs are rejected.

Nothing asserts that a known set of input rows produces a known slot-hour or dollar figure.

**F2, F3, F9, F11, and F12 would all have been caught by table-driven tests over the Python post-processing** — no BigQuery required, since most aggregation already happens in Python after the fetch. The `mock_bq_row_factory` fixture in `conftest.py` already exists for exactly this and is barely used.

**Recommended additions, in priority order:**

1. **`tests/test_billing_math.py`** — fixture rows → asserted dollar figures, for cost attribution (including the reconciliation identity `sum(attributions) + unattributed == total_admin_bill`), HBO, and fluid scaling.
2. **`tests/test_sql_invariants.py`** — structural assertions over generated SQL:
   - every `JOBS_BY_ORGANIZATION` / `JOBS_TIMELINE_*` reader has a time predicate (**F1**)
   - every `JOBS_TIMELINE_*` reader excludes script parents (**F8**)
   - no query `SUM`s a job-level byte column from a timeline view (**F2**)
   - every POST endpoint validates its params (**F15**)
3. **Golden-file tests** for the three largest SQL templates, so a diff in generated SQL is a reviewable event rather than an invisible one.

Category 2 is the highest-leverage: those are cheap string assertions that would have caught four of the seven most severe findings, and they keep working as new endpoints are added.

---

## 8. Recommended Remediation Order

| # | Finding | Why this order | Effort |
|---|---|---|---|
| 1 | **F1** — unbounded scans | Smallest diff, protects the user's bill immediately, blocks nothing | ~1h |
| 2 | **F6** — retry + error message | Makes every other failure legible; without it F1's cap failures stay misdiagnosed | ~1h |
| 3 | **F3** + **F7** — attribution completeness + config durability | Ship together: these two decide whether chargeback numbers can be trusted at all. **F7 needs an IAM grant landed first** — see its deployment prerequisite | ~4h + IAM lead time |
| 4 | **F2**, **F5**, **F8** — query correctness | Self-contained SQL fixes with clear tests. F5 is the largest of the three: it swaps the primary detector to `COLUMNS` *and* keeps a `PARTITIONS` branch for ingestion-time tables | ~4h |
| 5 | **F11**, **F12**, **F13** — HBO pricing and basis | Cheap; makes the dashboard internally self-consistent | ~2h |
| 6 | **F4**, **F14** — governance scope and dispatch | Related; both touch `GovernanceParams`. Land F14 before adding `extra="forbid"`. F4 adds a thread pool and a new response field, so it is no longer a pure SQL change | ~4h |
| 7 | **F16** — consolidate `_run_and_log` | Do *after* the SQL fixes so there is one path to add the invariant check to | ~2h |
| 8 | **F9**, **F10**, **F19** — estimate robustness | Lower user impact; F9 needs a product call on surfacing confidence | ~3h |
| 9 | **F15** — Pydantic-native sanitization | Largest blast radius (400→422 migration); do last, with its own test plan | ~4h |
| 10 | **F17**, **F18**, **F20**, **F21** — hygiene | Batch into one cleanup PR | ~1h |

**Sequencing constraints:**

- **F14 before F1's `extra="forbid"` extension** — adding strict extras to `GovernanceParams` requires `audit_type` to exist first.
- **F13 before F12** — `limit` becomes a SQL parameter in F12, so bound it first.
- **F16 after F1/F2/F5/F8** — consolidating the query path is more valuable once the SQL is correct, and the invariant check added during F16 should encode the F1/F8 rules.
- **F3 before F7 is fine either way**, but shipping F7 alone means durable storage for a config that still produces silently-incomplete output.
- **F7's IAM grant strictly before F7's code.** The startup guard turns a missing `roles/storage.objectUser` binding into a failed revision. That is the intended behaviour — but it means the bucket and the binding must exist before the revision deploys, or the rollout fails.
- **F4 and F5 both need §9.7 read first.** They fan out in opposite ways for a reason: F4 crosses a permission boundary (one job per project), F5 does not (`UNION ALL` is fine). Copying either pattern to the other endpoint reintroduces a bug.
- **F16 must preserve `create_bqstorage_client=True`.** The test mocks accept any kwargs, so the regression is invisible without the explicit assertion in F16.

---

## 9. Cross-Cutting Constraints (read before editing)

These apply to every change above. Each is a real trap in this repository.

### 9.1 `docs/static/app.js` is a byte-identical mirror

```bash
diff -q static/app.js docs/static/app.js   # → identical, verified
```

It backs the GitHub Pages demo. **Every** JS edit must be applied to both files. Add this to CI:

```bash
diff -q static/app.js docs/static/app.js || {
  echo "static/app.js and docs/static/app.js have drifted"; exit 1; }
```

### 9.2 `FocusMixin` does not forbid extra fields

[`utils.py:72`](src/utils.py#L72) — unlike `OrgParams` ([`utils.py:78`](src/utils.py#L78)), which does. Consequences:

- Unknown fields sent by the frontend are **silently discarded** (this is F14, and the F1 deploy-order trap).
- **Deploy backend before frontend** for any new request field, or the field vanishes with no error.
- Adding `extra="forbid"` to an existing model requires first auditing every payload the frontend sends to it.

### 9.3 Interpolated integers are safe; interpolated strings are not

The house pattern `INTERVAL {params.lookback_days} DAY` is safe **only** because `Field(ge=..., le=...)` guarantees an `int`. Never extend this pattern to a string field. String identifiers must go through `_safe_ident`; string values must be `ScalarQueryParameter`.

### 9.4 Prefer constant expressions in partition predicates

Partition pruning on `INFORMATION_SCHEMA` views is most reliable with constant expressions. Use f-string interpolation of a bounded int for `creation_time` / `period_start` predicates; reserve query parameters for `LIMIT`, `IN UNNEST`, and value comparisons where pruning is not at stake.

### 9.5 Know which timeline columns are per-second

| Column in `JOBS_TIMELINE_*` | Grain | `SUM` across seconds? |
|---|---|---|
| `period_slot_ms` | per-second | ✅ |
| `total_bytes_billed` | per-job, repeated | ❌ (**F2**) |
| `total_bytes_processed` | per-job, repeated | ❌ (**F2**) |
| `total_slot_ms` | per-job, repeated | ❌ |
| `job_creation_time`, `job_type`, `statement_type` | per-job, repeated | n/a — filter only |

Anything that is not per-second must be deduplicated by `job_id` (`ANY_VALUE` after `GROUP BY job_id`) before aggregation.

### 9.6 `_BY_ORGANIZATION` variants do not exist for every view

| View | Org-wide variant? |
|---|---|
| `JOBS` | ✅ `JOBS_BY_ORGANIZATION` |
| `JOBS_TIMELINE` | ✅ `JOBS_TIMELINE_BY_ORGANIZATION` |
| `TABLE_STORAGE` | ✅ `TABLE_STORAGE_BY_ORGANIZATION` |
| `RECOMMENDATIONS` | ✅ `RECOMMENDATIONS_BY_ORGANIZATION` |
| **`SCHEMATA`** | ❌ **project-scoped only** (**F4**) |
| **`SCHEMATA_OPTIONS`** | ❌ project-scoped only |
| **`TABLE_OPTIONS`** | ❌ dataset-scoped only |
| **`PARTITIONS`** | ❌ dataset-scoped only |
| **`COLUMNS`** | ❌ dataset-scoped only |
| **`RESERVATIONS`** | ❌ admin-project-scoped |
| **`PROJECT_OPTIONS`** | ❌ project-scoped |

For the project-scoped views, `focus_projects` must expand to **one query per project**, **not** a `WHERE` predicate on the catalog column — and not a single `UNION ALL` either, for the reason in §9.7.

### 9.7 A BigQuery job is atomic with respect to permissions

A `UNION ALL` across projects is **one job**. If the caller lacks read access on **any** branch, the whole job fails `403 Forbidden` and every project returns nothing — including the readable ones. There is no partial-success mode, and no `try/except` around the job can recover the branches that would have succeeded.

| Situation | Correct shape |
|---|---|
| Fan out across **projects** (permission boundary not yet cleared) | One job per project, `ThreadPoolExecutor`, merge in Python, report failures (**F4**) |
| Fan out across **datasets inside an authorised project** | `UNION ALL` is fine (**F5**, `get_physical_datasets`) |

When fanning out in threads, copy `request_id_var` into each worker — see [`hbo.py:380-391`](src/hbo.py#L380). Without it, worker-thread log lines print the `"--------"` default and lose request correlation. Handlers here are sync `def` and already run on an AnyIO worker thread, so `ThreadPoolExecutor` is the tool; `asyncio.gather` is not available.

And whenever coverage is partial, **return the gap in the payload**. An audit that silently skips a project is indistinguishable from an audit that found nothing wrong — the root failure pattern behind F3, F4, and F5.

### 9.8 BigQuery sorts NULLs **last** in `DESC` — the opposite of PostgreSQL

GoogleSQL treats NULL as the **minimum** possible value:

| | `ORDER BY x ASC` | `ORDER BY x DESC` |
|---|---|---|
| **BigQuery** | NULLs **first** | NULLs **last** |
| PostgreSQL / Oracle | NULLs last | NULLs **first** |

So a `DESC` ranking on a `SAFE_DIVIDE` result does *not* get flooded with NULLs on BigQuery. `NULLS FIRST` / `NULLS LAST` is supported and worth writing explicitly for readers arriving from other engines, but it is documentation, not a fix.

The real hazard in this codebase is different and worse: **a SQL `LIMIT` counts rows that a later Python guard then discards**, so the endpoint under-fills below the requested limit (**F12**). Push the guard into `WHERE`; do not rely on null ordering to do it.

### 9.9 `_PARTITIONTIME` / `_PARTITIONDATE` are pseudocolumns and are absent from `COLUMNS`

`INFORMATION_SCHEMA.COLUMNS` is the cheap, complete way to find **column**-partitioned tables — one row per column, includes tables with zero partitions, and gives you the partitioning column name. But ingestion-time partitioned tables have **no** `is_partitioning_column = 'YES'` row, because their partitioning key is a pseudocolumn rather than a schema field.

| Detector | Column-partitioned | Ingestion-time | Zero-partition tables | Cost |
|---|---|---|---|---|
| `COLUMNS WHERE is_partitioning_column='YES'` | ✅ | ❌ **misses entirely** | ✅ | 1 row/column |
| `PARTITIONS WHERE partition_id IS NOT NULL` | ✅ | ✅ | ❌ **misses entirely** | 1 row/**partition** |

Neither is sufficient alone. **F5** uses both. Note also that `PARTITIONS` emits one `partition_id IS NULL` row for each *unpartitioned* table — that predicate is what excludes them — while the ingestion-time sentinels `__NULL__` and `__UNPARTITIONED__` are non-NULL and must be **kept**, since those tables are genuinely partitioned.

### 9.10 `LIMIT` never reduces bytes scanned

Applied after the scan. `ORDER BY ... LIMIT n` is worse — it forces a full scan plus a global sort. Only a predicate on the partitioning column reduces cost.

### 9.11 Cloud Run is the deployment target

`Dockerfile` → `uvicorn --host 0.0.0.0 --port 8080`, `EXPOSE 8080`. Therefore: no local-filesystem persistence (**F7**), no in-process state that must survive a request, multiple concurrent instances, and `K_SERVICE` present in the environment as a reliable "am I in production" signal.

---

## 10. Revision Log

**Rev 2** — revised after architecture review. Five remediation steps changed; no finding was withdrawn and no severity moved.

| Section | Change | Reason |
|---|---|---|
| **F4** | `UNION ALL` across projects → **one job per project** in a `ThreadPoolExecutor`, merged in Python, with a new `inaccessible_projects` response field | A BigQuery job is atomic w.r.t. permissions: one 403 fails the whole `UNION ALL`, so a single unreadable project would blank the panel — the exact silent-empty bug F4 exists to remove (§9.7) |
| **F5** | Primary detector moved from `PARTITIONS` to `COLUMNS`; `PARTITIONS` retained as a narrow, de-duplicated branch; old "Step 2" folded in | `COLUMNS` is one row per column instead of one per partition, and it sees zero-partition tables. But `_PARTITIONTIME` is a **pseudocolumn**, so `COLUMNS`-only would silently drop every ingestion-time partitioned table (§9.9) |
| **F7** | Added the **IAM deployment prerequisite** (`roles/storage.objectUser`, needs `objects.delete` for overwrite); rewrote `save_config` to drop `blob.exists()` / `blob.reload()` pre-checks; startup guard now probes access, not just env-var presence | Without the grant the settings page returns 500 on every request and reads as an app bug. The old pre-check pattern cost three round trips and was still racy |
| **F12** | Added an explicit `WHERE avg_previous_execution_ms > 0`; `NULLS LAST` stated but documented as redundant | The original concern raised was NULLs flooding a `DESC` sort — that is PostgreSQL behaviour; BigQuery sorts NULLs **last** in `DESC` (§9.8). The real defect is that the Python `prev_exec_ms > 0` guard drops rows *after* `LIMIT` counts them, under-filling the response |
| **F16** | Made `create_bqstorage_client=True` an explicit, asserted part of the unified `fetch_df` contract | The conftest mock accepts any kwargs, so losing the Storage Read API during consolidation would pass every test and OOM in production |
| **§9** | Added §9.7 (job/permission atomicity), §9.8 (NULL ordering), §9.9 (pseudocolumns vs. `COLUMNS`); renumbered old §9.7–9.8 → §9.10–9.11 | Each is a reusable trap, not a one-off |

**Reference sources for the two verified SQL-semantics claims:**

- NULL ordering — [Data types: ordering](https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types) and [Query syntax: `ORDER BY`](https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax): *"NULLs are the minimum possible value; that is, NULLs appear first in ASC sorts and last in DESC sorts."*
- Partitioning-column visibility — [`INFORMATION_SCHEMA.COLUMNS`](https://cloud.google.com/bigquery/docs/information-schema-columns), [Introduction to partitioned tables](https://cloud.google.com/bigquery/docs/partitioned-tables), [Querying partitioned tables](https://cloud.google.com/bigquery/docs/querying-partitioned-tables).

---

*End of review. 21 findings — 3 critical, 4 high, 7 medium, 7 low. Every finding includes a concrete diff and a regression test.*
