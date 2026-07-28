# Code Review — `bq-optimizer-client-test`

**Date:** 2026-07-27
**Reviewer:** Claude Opus 5 (senior code review pass)
**Scope:** 20 `src/` modules, `static/app.js` (6,221 lines), `static/index.html`, `Dockerfile`, packaging, docs, and the 526-test suite (executed, not just read).
**Baseline:** Findings F1–F21 from `src/CODEREVIEW.md` (2026-07-25) were re-verified against current source — 14 fixed, 2 partial, 5 still open. **Nothing in this document duplicates them**, with one correction noted in §7.

---

## Table of contents

1. [Headline: the test suite does not test the money](#1-headline-the-test-suite-does-not-test-the-money)
2. [Critical — wrong numbers presented as authoritative](#2-critical--wrong-numbers-presented-as-authoritative)
3. [High](#3-high)
4. [Medium — backend](#4-medium--backend)
5. [Medium — frontend](#5-medium--frontend)
6. [Medium / Low — packaging, deployment, docs](#6-medium--low--packaging-deployment-docs)
7. [Corrections and unverified claims](#7-corrections-and-unverified-claims)
8. [Verified as _not_ bugs](#8-verified-as-not-bugs)
9. [Suggested fix order](#9-suggested-fix-order)

---

## 1. Headline: the test suite does not test the money

Five deliberate financial-math bugs were injected into `src/` and the full suite re-run:

| #   | File:line                     | Mutation                                                                                   |
| --- | ----------------------------- | ------------------------------------------------------------------------------------------ |
| 1   | `src/hbo.py:109`              | `/ 3600000.0` → `/ 360000.0` (**10×** inflated HBO savings)                                |
| 2   | `src/cost_attribution.py:154` | `/ 3600000.0` → `/ 1000.0` (**3600×** inflated slot-hours → all dollar attributions)       |
| 3   | `src/fluid_scaling.py:390`    | `usd_annual = usd_window * (DAYS_PER_YEAR/lookback_days)` → `usd_annual = 0.0`             |
| 4   | `src/main.py:1032`            | `editions_cost = …` → `editions_cost = 0.0`                                                |
| 5   | `src/cost_attribution.py:202` | `allocated_waste = waste_cost * project_share_percentage` → `allocated_waste = waste_cost` |

```
Clean run:   2 failed, 517 passed, 7 skipped, 7 warnings in 84.36s
Mutated run: 2 failed, 517 passed, 7 skipped, 7 warnings in 84.69s   ← byte-identical
```

**Zero of 526 tests detected any of the five.**

Control, to prove the method works: re-introducing a known-bad `- fail_safe_physical_gib` term at `src/main.py:781` produced **32 failures** in `tests/test_physical_bytes_decomposition.py`. The suite _can_ catch this class of bug where real tests exist — it simply has none anywhere the money is computed. All sources were restored and verified clean after the experiment.

Read every finding below against that backdrop: **nothing in the cost math has a safety net.**

### 1.1 Two tests fail on a clean tree and have evidently never passed

```
FAILED tests/test_migration_optimizer.py::test_synthesize_optimizer_yaml
FAILED tests/test_migration_optimizer.py::test_synthesize_yaml_anti_join_and_merge
```

- `test_synthesize_optimizer_yaml` asserts `"experimental_optimizer" in yaml_str`; `src/migration_optimizer.py:475` emits `type: optimizer`. The key the test looks for is never produced.
- `test_synthesize_yaml_anti_join_and_merge` asserts `ANTI_JOIN_EXPLICIT_NOT_NULL` and `MERGE_PRECOMPUTE_PRUNING_BOUNDARIES` are auto-detected. `synthesize_optimizer_yaml` (`src/migration_optimizer.py:409-483`) implements 7 transformations, neither of which is among them, so it returns `None` at line 473.

Both were written against a spec, not the code.

### 1.2 Root cause of the coverage illusion

`tests/conftest.py:41-57` — `_make_mock_job()` returns `result_obj.__iter__ → iter([])` and `to_dataframe() → pd.DataFrame()`. All 26 endpoints in `tests/test_smoke_endpoints.py:80-215` therefore execute with **zero rows**, so every `for row in results:` body is skipped and the tests assert HTTP 200 on the zero-row early-return path.

Coverage confirms it exactly:

```
src\bqrecommender.py           412    412     0%   55-751
src\cost_attribution.py        142     52    63%
src\fluid_scaling.py           218     42    81%
src\hbo.py                     189     58    69%
src\main.py                   1825    586    68%
src\migration_optimizer.py     425    169    60%
src\utils.py                   194     24    88%
TOTAL                         3405   1343    61%
```

The missed ranges are precisely the arithmetic: `cost_attribution.py:171-209` (the whole attribution loop), `hbo.py:102-112` (savings math), `main.py:999-1057` (editions vs on-demand cost).

Where tests _do_ supply numeric rows, the assertions are pass-throughs. `tests/test_ai_doctor.py:521` asserts `annualized_cost_usd == 162.5` against a mock row constructed with `annualized_cost_usd=162.5` — the cost is computed in the BigQuery SQL, which is mocked away, so the assertion reduces to `162.5 == 162.5`.

_(Environment note: no Python on PATH; the run used a provisioned `.venv-test/` with CPython 3.12.10, `requirements.txt`, `requirements-dev.txt`, `pytest-timeout`, `httpx`. `AUTH_ENFORCED_UPSTREAM` was not needed — `tests/conftest.py:15` does `os.environ.setdefault("AUTH_ENFORCED_UPSTREAM", "true")` before importing `src.main`, which also means the startup guard is never tested.)_

---

## 2. Critical — wrong numbers presented as authoritative

### C1 — Chargeback misstates the bill three different ways, and `is_complete` stays `true` for all three

**`src/cost_attribution.py:195`, `:201`, `:31`**

**(a) Over-recovery is silently discarded.**

```python
total_res_direct_cost = reservation_totals[res_id] * sku_rate_per_slot_hour
waste_cost = max(0, total_billed_to_admin - total_res_direct_cost)   # :195
```

The clamp means the allocation sums to `direct + max(0, bill − direct)`, which equals the bill only when `direct ≤ bill`. When measured usage exceeds the invoice — the _normal_ outcome if you enter the list `sku_rate` but your bill reflects a CUD — the overage is dropped and the chargeback total exceeds the invoice.

> Reservation `prod-us`, `total_admin_bill = $10,000`, `sku_rate = 0.06` (list), actual effective rate $0.036 after a 40% CUD. Measured usage 200,000 slot-hours.
> `direct = 200,000 × 0.06 = $12,000` → `waste = max(0, 10,000 − 12,000) = 0` → **$12,000 attributed against a $10,000 invoice. $2,000 (20%) of phantom charges**, `is_complete: true`.

**(b) Zero measured slot-hours makes the entire reservation bill vanish.**

```python
project_share_percentage = slot_hours / reservation_totals[res_id] if reservation_totals[res_id] > 0 else 0   # :201
allocated_waste = waste_cost * project_share_percentage
```

The zero-denominator guard correctly avoids `ZeroDivisionError` but sets _every_ project's share to `0` while `waste_cost` is simultaneously the **full bill**. Nothing catches the remainder.

> `total_admin_bill = $50,000`; every job in the window was a cache hit or metadata-only, so `SUM(total_slot_ms) = 0` per group.
> `waste_cost = max(0, 50,000 − 0) = 50,000`, share `= 0` for every project → every row `$0.00`, `total_unattributed_slot_hours: 0.0`, **`is_complete: true`**. $50,000 disappears.

The clincher: rule B on identical input **correctly** dumps the full $50,000 on the central cost center (`:236`, `if waste_cost > 0`). Rules A and B disagree by 100% of the bill on the same data — proof this is a bug, not a policy difference.

**(c) A typo in `waste_rule` drops all waste.**

```python
waste_rule: str = "A"   # :31 — no Literal, no validator
...
if   config.waste_rule == "A": ...     # :199
elif config.waste_rule == "B": pass    # :203
                                       # no else
```

`POST /api/cost-attribution/config` with `{"waste_rule": "a"}` — or `"Proportional"`, or any future UI typo — returns **200 "Configuration updated successfully"**. Then neither branch matches, `allocated_waste` stays `0.0`, and the rule-B block at `:219` is skipped too.

> Bill $10,000, direct usage $4,000 → **$6,000 (60% of the bill) silently evaporates**, `is_complete: true`.

**Fix:** compute the residual explicitly (`bill − Σ(direct + waste)`) and surface it in the payload; when `reservation_totals[res_id] == 0 and waste_cost > 0`, emit an explicit unallocated record and set `is_complete = False`; declare `waste_rule: Literal["A","B"]` and make the dispatch exhaustive with `else: raise`. Add one property test asserting `Σ total_cost_attribution_usd == Σ total_admin_bill` for configured reservations — it catches all three.

**Related:** the rule-B precondition check at `:219-225` runs _after_ the BigQuery scan at `:147`, so a misconfigured request burns a full org-wide `JOBS_BY_ORGANIZATION` scan (up to the 200 GiB cap, ≈$1.25) before returning 400. Hoist it.

---

### C2 — One pending job 500s the whole month's chargeback

**`src/cost_attribution.py:132-141`, `:154`**

```sql
WHERE creation_time >= TIMESTAMP(@start_date)
  AND creation_time <  TIMESTAMP(@end_date)
  AND job_type = 'QUERY'
  AND (statement_type IS NULL OR statement_type <> 'SCRIPT')
  AND reservation_id IS NOT NULL
```

```python
slot_hours = row.total_slot_ms / 3600000.0   # :154
```

No `state = 'DONE'` filter and no NULL guard. Every sibling query in `hbo.py` has both (`hbo.py:155`, and `hbo.py:109` uses `(row.total_slot_ms or 0)`). This — the query that produces the money numbers — has neither.

Two consequences:

1. Jobs `RUNNING`/`PENDING` at execution time contribute partial slot-ms to a closed billing month, so re-running the same month later yields a different answer.
2. `SUM(total_slot_ms)` returns **NULL** for any `(project_id, reservation_id)` group whose rows all have NULL slot_ms — exactly the in-flight case. Line 154 then raises `TypeError: unsupported operand type(s) for /: 'NoneType' and 'float'`, which becomes a generic **HTTP 500, "Cost attribution failed; check server logs"** with no hint of the cause.

**Fix:** add `AND state = 'DONE'`; change to `SUM(IFNULL(total_slot_ms, 0))`; belt-and-braces `(row.total_slot_ms or 0) / 3600000.0`.

---

### C3 — The migration optimizer can return your own query as the "optimized" one, with `success=True`

**`src/migration_optimizer.py:287-290`, `:508`, consumed at `src/main.py:2178`**

Source and target configs share the same `relativePath`, so `walk_literals(final_payload)` at `:508` seeds from the **echoed input** literal under key `input.sql`. Subtask output is meant to overwrite it — but if `client.subtasks()` 403s or yields no `.sql`, `literals.get("input.sql")` is the original query. It is truthy, so `success=True`, and `main.py:2178` hands it to the user as the compiler rewrite.

The author documented the correct invariant at `:360` — literals come "strictly from subtask outputs." Line 508 defeats it.

**Fix:** give source and target distinct `relativePath`s, or drop the `input.sql` key from the seed set and require a subtask-derived literal before setting `success=True`.

---

### C4 — Phantom −100% savings on any CTE query

**`src/migration_optimizer.py:645`, `:431`**

`delta_pct` is computed with no check on `totalBytesProcessedAccuracy`. `REWRITE_CTE_TO_TEMP_TABLE` auto-enables for anything containing `WITH ` + ` AS (` (`:431`), which produces a **multi-statement script** — and BigQuery dry-runs scripts as `0` bytes.

> A 5 TiB query rewritten to a temp-table script reports a **100% byte reduction**.

**Fix:** refuse to report a delta unless `totalBytesProcessedAccuracy == "PRECISE"`; for scripts, either sum per-statement dry runs or report "not measurable."

---

### C5 — AI Doctor prices in decimal TB while the rest of the codebase uses TiB

**`src/main.py:1696-1697`**

```sql
SUM(effective_bytes) / POW(10, 12) * 6.25 AS window_cost_usd,
(SUM(effective_bytes) / POW(10, 12) * 6.25) / {params.lookback_days} * 365 AS annualized_cost_usd,
```

BigQuery on-demand is billed per **TiB** (2^40), which the rest of the codebase gets right with `POW(1024, 4)`. Every AI Doctor dollar figure is **~10% overstated**. It also hardcodes `6.25` instead of reading `ON_DEMAND_USD_PER_TB` (`main.py:61`), the documented single source of truth, so a customer-specific rate set via `BQ_ON_DEMAND_USD_PER_TB` is silently ignored on this endpoint only.

**Fix:** `POW(1024, 4)` and parameterize the rate from `ON_DEMAND_USD_PER_TB`.

---

### C6 — Storage analysis fabricates savings when permissions are missing

**`src/main.py:838-851`, `:861-863`, consumed at `:896`**

```python
except Exception as e:
    logger.warning(f"Fast UNION ALL failed: {e}. Falling back to loop.")
physical_datasets = set()
for p in projects:
    try: ...
    except Exception as e:
        logger.warning(f"Failed to query SCHEMATA_OPTIONS for project {p}: {e}")
return physical_datasets
...
currently_on = "physical" if (project, dataset) in physical_datasets else "logical"   # :896
```

Both the fast path and the per-project fallback swallow exceptions and return an empty set — **indistinguishable from "nothing is on physical billing."** Line 896 then labels every dataset `logical` and the endpoint emits `ALTER SCHEMA … SET OPTIONS (storage_billing_model='physical')` with a dollar figure attached, for datasets already on physical.

**Fix:** track which projects failed; return them in the response as an explicit `undetermined` set and exclude them from the savings total rather than defaulting them to `logical`.

---

### C7 — The Dashboard is broken on load, and worse on reload

**`static/app.js:5132`, `:5127`, `:5034`, `:5010`; backend `src/main.py:4213-4260`**

```js
value: kpis.anomalyCount.toString(),   // :5132
```

`GET /api/dashboard/kpis` is a **stub** — `KpiResponse` declares all seven metrics `Optional[...] = None` and the handler returns `KpiResponse(stub=True)`. The `if (!kpis)` guard passes because the object itself is truthy.

First visit: `render()` throws `TypeError: Cannot read properties of null (reading 'toString')`, caught by a bare `console.error` at `:5038` — so `UIState.renderError` is never reached and four skeleton placeholders stay on screen forever with **no error UI**.

Second visit is worse, because of the ordering at `:5034-5035`:

```js
writeCache(data); // :5034 — caches a payload that has not rendered successfully
render(data); // :5035
```

`readCache()` now returns the poisoned payload and `:5010` executes it **outside the try/catch**:

```js
if (cached) {
  render(cached.data); // :5010 — unprotected
  updateFreshness(cached.fetchedAt);
  return;
}
```

→ unhandled promise rejection out of `Dashboard.init()`, and `setRefreshSpinning(false)` never runs because this path has no `finally`. The refresh button spins for the full 1-hour `CACHE_TTL_MS`.

`:5127` has the same root cause with a softer symptom: `deltaLabel: \`${kpis.opportunityCount} opportunities\`` renders the literal string **"null opportunities"**.

**Fix:** `(kpis.anomalyCount ?? 0).toString()`; treat `kpis.stub === true` as unavailable and call `UIState.renderError`; move `render(cached.data)` inside a try; don't `writeCache` a payload you haven't successfully rendered.

---

### C8 — Copy SQL and Dry Run are broken for essentially every AI row

**`static/app.js:4283`, `:4322`; sanitizer at `:1-82`**

A global `window.fetch` monkey-patch wraps every `/api/` response in a Proxy whose `.json()` recursively HTML-escapes **every string in the payload**, exempting only the key `ddl` (`:66`). So `optimized_query` arrives entity-encoded — and two consumers treat it as raw SQL:

```js
const sql = rowData?.optimized_query || "";
await navigator.clipboard.writeText(sql); // :4283 / :4287
```

```js
body: JSON.stringify(
  buildPayload("/api/ai/dry_run", {
    org_project_id: state.orgProject,
    query: rowData.optimized_query, // :4322
  }),
);
```

> Gemini returns `SELECT * FROM t WHERE dt > '2026-01-01'`.
> Copy SQL → paste into the BigQuery console → `SELECT * FROM t WHERE dt &gt; &#39;2026-01-01&#39;` → syntax error.
> Dry Run sends the same mangled string → `valid: false` with a parser error.

Since string literals and comparison operators appear in virtually every non-trivial statement, **both features fail for practically every row** — and the failure presents as _"the AI produced invalid SQL,"_ which is the wrong diagnosis.

**Fix:** exempt `optimized_query` (and `query`) from `sanitizeData` the way `ddl` already is, and escape at the render sink instead.

---

## 3. High

### H1 — Stored XSS in `showNotification`, reachable via snapshot import

**Sink `static/app.js:2509-2512`; sources `:656`, `:664`, `:690`; trigger `:703`**

```js
notification.innerHTML = `
    <i class="fa-solid ${icon}"></i>
    <div class="notif-content">${message}</div>
`;
```

`message` is interpolated raw. Nearly every caller passes a literal or an already-sanitized `error.message` — but three build the string from **DOM input values, which never pass through `fetch` and are therefore never escaped**:

```js
validationErrors.push(`Invalid Organization Project ID "${state.orgProject}". …`);   // :656
validationErrors.push(`Invalid Admin Project ID "${state.adminProject}". …`);        // :664
validationErrors.push(`Invalid Focus Project ID(s): ${invalidProjects.map(p => `"${p}"`).join(', ')}. …`);  // :690
...
if (validationErrors.length > 0) { showNotification(validationErrors.join('\n'), 'error'); return; }   // :703
```

The regex is `/^[a-z][a-z0-9\-]{5,29}$/`, so a payload is **guaranteed to fail validation and guaranteed to reach the sink**. Paste `<img src=x onerror=alert(document.domain)>` into Focus Projects → Save → executes in the app origin with access to `localStorage` (org project, admin project, focus projects, every cached analysis result) and to the authenticated same-origin `/api/*` surface.

Delivery is not limited to self-XSS: the snapshot import feature (`:370-450`) restores `bq_focus_projects` from a shared `.json` file, and the settings inputs are repopulated from localStorage on boot. A crafted snapshot plus "now hit Save" plants the payload. See also L4 below — the import path's escaper is dead code.

**Fix:** build the node with `document.createElement` + `textContent`, or run `message` through the existing `escapeHtml`.

---

### H2 — Fluid scaling computes `edition` and throws it away

**`src/fluid_scaling.py:243`, `:306`, `:319`**

`MAX(edition) AS edition` is selected, but `_ReservationSummary` has no `edition` field. Every reservation is priced at one flat `price_per_slot_hr` (default `0.06`, the Enterprise rate) — Standard is ~$0.04, Enterprise Plus ~$0.10.

A mixed-edition org gets both a wrong total **and** a wrong priority ordering, since reservations are ranked by a dollar figure computed at the wrong rate.

**Fix:** add `edition` to `_ReservationSummary` and map it to a per-edition rate table; keep `price_per_slot_hr` as the override for unknown editions.

---

### H3 — Green "fully enabled" badge when zero reservations were found

**`src/fluid_scaling.py:449-451`, detection at `:490`**

```python
is_fully_enabled = True
if missing_res:
    is_fully_enabled = False
```

An empty result set produces an empty `missing_res`, so the default `True` survives. The empty case **is** detected and logged at `:490` — it just never reaches the response.

> Typo the `admin_project_id`, or point at a region with no reservations, and the panel reports fluid scaling fully enabled across the estate.

**Fix:** propagate the empty-dataframe condition into the response as an explicit `no_data` / `is_fully_enabled: null` state.

---

### H4 — Narrowing scope _increases_ reported Active Assist savings

**`src/main.py:576-587` (focus branch) vs `:596-608` (org branch)**

```sql
FROM `{p}`.`{region_val}`.INFORMATION_SCHEMA.RECOMMENDATIONS
WHERE recommender = 'google.bigquery.table.PartitionClusterRecommender'
-- org branch additionally has:  AND state = 'ACTIVE'
```

The focus branch omits `AND state = 'ACTIVE'`, so recommendations already actioned or dismissed reappear at their original dollar values. Drilling into a single project therefore _raises_ its reported savings relative to the org view.

**Fix:** add the state filter to the focus branch.

---

### H5 — `None < 5` blanks the profiler panel

**`src/main.py:4117-4119` vs `:4131`**

```python
avg_bytes = row['avg_bytes_processed'] or 0.0
recommendation = "N/A"
if avg_bytes < 100 * 1024 * 1024 and row['avg_duration_seconds'] < 5:   # :4119 — unguarded
...
"avg_duration_seconds": round(row['avg_duration_seconds'] or 0.0, 2),   # :4131 — guarded, same column
```

`AVG(TIMESTAMP_DIFF(end_time, …))` is NULL when every job in a hash group was cancelled, and the query has no `state = 'DONE'` filter. One cancelled scheduled query → `TypeError` → HTTP 500 for the whole panel. The line above guards `avg_bytes` and line 4131 guards the _same column_ — this is an oversight, not a design choice.

**Fix:** `(row['avg_duration_seconds'] or 0.0) < 5`, and add `state = 'DONE'` to the query.

---

### H6 — BigQuery quota errors are hard-classed as permanent IAM failures

**`src/utils.py:352-357`, message at `:250-255`**

```python
if isinstance(e, (gax_exc.Forbidden, gax_exc.NotFound, gax_exc.BadRequest)):
    logger.error("❌ %s failed with permanent error (no retry): %s", …)
    raise
```

BigQuery returns **HTTP 403** for `rateLimitExceeded` and `quotaExceeded`, not just for missing IAM, and `google.api_core` raises `Forbidden` for every 403 regardless of `reason`. So the single most retry-worthy BigQuery error class is the one hard-coded as non-retryable — and `handle_endpoint_exception` then reports it as _"Access Denied: the service account or user lacks required BigQuery permissions."_

> `check_hbo_status` on 300 projects with `max_workers=10`. The API rate limit trips on ~40. Those 40 return `enabled=None` → 40 rows of "permission or query error," in a UI that just told the operator their service account is misconfigured. A 1-second backoff would have succeeded.

This is the mirror image of F6 (permanent `BadRequest` retried 5×) — same function, opposite direction, not covered by it.

**Fix:**

```python
def _is_retryable_403(e):
    return any(err.get("reason") in ("rateLimitExceeded", "quotaExceeded")
               for err in getattr(e, "errors", []) or [])
```

Exclude those from the permanent set, and map them to **429** with `Retry-After` instead of the IAM message.

---

### H7 — The one guard rail talks itself into standing down

**`src/utils.py:274`, `:298`**

```python
if "bytesBilledLimitExceeded" in err_str or "exceeds the maximum" in err_str.lower():
    raise HTTPException(400, f"Query exceeded the bytes-billed safety cap. Raise max_bytes_billed_gb …")
```

The second clause is a bare substring test against the full error text. Unrelated BigQuery 400s contain that phrase — e.g. `Number of partitions is 5000, which exceeds the maximum allowed number of partitions 4000`, and column-count limit errors.

> The user trips a partition-count limit and is told, authoritatively, to **raise** `max_bytes_billed_gb`. They set it to the 10240 GiB maximum and retry. The query still fails for the original reason — but now with the safety cap effectively disabled, so up to **10 TiB billed ≈ $62.50 per attempt** on a query that was never going to succeed.

**Fix:** match the reason code — `any(err.get("reason") == "bytesBilledLimitExceeded" for err in getattr(e, "errors", []))` — or at minimum tighten to `"exceeds the maximum bytes billed"`.

---

### H8 — Chargeback config save is fire-and-forget

**`static/app.js:2650-2654`**

```js
await fetch('/api/cost-attribution/config', { method: 'POST', … });   // no .ok, no .catch
// then calculate
const response = await fetch('/api/cost-attribution/calculate', { … });
```

`POST /config` returns **500** on any save failure (`src/cost_attribution.py:94-98`), and `/calculate` reads the config **server-side**, not from this request body.

> Config file read-only or disk full → save 500s silently → calculate runs against the _previous_ config. The user changed `waste_rule` and the per-reservation SKU rates; the table renders a full attribution breakdown computed with the **old rates**, with a success toast and no indication anything went wrong.

For a financial-reporting tool this is the worst available failure mode.

**Fix:** check `.ok` on the config POST and abort the calculate with an error toast.

---

### H9 — One client-construction failure kills a 300-project HBO check

**`src/hbo.py:333-334`**

```python
def _check_project_status(prj):
    local_client = bigquery.Client(project=prj)   # <-- outside the try
    try:
        ...
    except Exception as e:
        return prj, None
    finally:
        local_client.close()
```

The constructor sits outside the `try`, so its exceptions bypass the `except` at `:356`, pass through `check_with_ctx` (which has `try/finally` but no `except`), re-raise the moment `list(executor.map(...))` materializes at `:373`, and are swallowed by the outer handler at `:417`.

> 300 projects; project #7 has a malformed ID or hits a transient credential-refresh error. Instead of one `enabled=None` row among 300 results, the whole endpoint returns a single 500.

The `enabled=None` design — added specifically so a failed check would be distinguishable — is defeated by exactly the failure class it was built for. `finally: local_client.close()` also never runs for the partially-constructed client.

**Fix:**

```python
def _check_project_status(prj):
    try:
        with bigquery.Client(project=prj) as local_client:
            ...
    except Exception as e:
        logger.warning(...); return prj, None
```

---

## 4. Medium — backend

### M1 — HBO saved-slot-hours understated by a factor of `prev/duration`

**`src/hbo.py:109`, mirrored in SQL at `:165-168`**

```python
saved_slot_hours = (percent_saved / 100) * ((row.total_slot_ms or 0) / 3600000.0)
```

This multiplies the _fraction_ of time saved by the **post**-optimization slot consumption. Since `percent_saved < 100` by construction, the reported saving can never exceed the slots the _optimized_ run used — a mathematical ceiling unrelated to the actual saving.

> 100,000 ms → 10,000 ms, consuming 3,600,000 slot-ms (1.0 slot-hour).
> Reported: `0.9` slot-hours ≈ **$0.054**. Actual, under the proportionality the code already assumes: ~9 slot-hours ≈ **$0.54**. **10× understatement**; a 20× speedup is understated 20×.

`analyze` and `summary` share the formula, so they agree with each other while both being wrong — which is why F12's basis analysis didn't surface it.

**Fix:** `saved_slot_hours = (total_slot_ms / 3.6e6) * (prev_exec_ms - duration_ms) / max(duration_ms, 1)`; mirror in SQL; document the proportionality assumption.

### M2 — All BigQuery 5xx are returned to the client as HTTP 400

**`src/utils.py:286-308`** — `InternalServerError` (500), `ServiceUnavailable` (503), `TooManyRequests` (429) and `DeadlineExceeded` all fall into the `GoogleAPIError` branch and are relabeled as client errors.

> BigQuery is degraded and returns 503 on all 5 attempts. The browser, Cloud Run's request log, and every SLO dashboard record **400**. 4xx is conventionally neither retried nor paged on, so a real outage is invisible in error-rate alerting and surfaces to the user as a validation-flavoured message about something they cannot fix.

**Fix:** `TooManyRequests` → 429 with `Retry-After`; `ServerError`/`ServiceUnavailable`/`DeadlineExceeded` → 503; reserve 400 for genuine client errors.

### M3 — `borrowing_rule` is a UI control wired to nothing

**`src/cost_attribution.py:33`** — the field appears in exactly four places: this declaration, two `<option>` tags in `static/index.html:1502-1503`, and a read/write in `static/app.js:2615`/`:2646`. **Zero references in any calculation.** Slot borrowing is never modelled in `/calculate`; the math only uses the `reservation_id` recorded on the job row, so borrowed capacity is implicitly charged to the borrower regardless of this setting.

> A FinOps owner deliberately switches from Lender Pays to Borrower Pays, sees the success toast, re-runs, and not one cent moves. They either conclude the tool is broken, or publish a chargeback report under a policy the tool never applied.

**Fix:** implement it, or remove the field and the dropdown. If it is a roadmap placeholder, reject a non-default value with 501 rather than accepting it silently.

### M4 — `check_hbo_status` silently audits an arbitrary 500 projects

**`src/hbo.py:318-326`** — `LIMIT 500` with no `ORDER BY` and no truncation signal in the response.

> An org with 1,200 active projects. Monday's run reports 3 disabled projects; the team fixes them. Tuesday's run reports 5 _different_ ones that were simply outside Monday's arbitrary subset. Nothing tells the operator they have only ever seen ~42% of the estate.

`LIMIT` does not reduce bytes scanned on a partitioned `JOBS_*` view, so there is no cost argument for the cap.

**Fix:** `ORDER BY project_id`, request `LIMIT 501`, and return `truncated: true` plus the discovered count when 501 rows come back — or page through.

### M5 — HBO focus mode can emit DDL for a project you never asked about

**`src/hbo.py:375-412`** — the result loop only appends rows for `enabled is False` and `enabled is None`. A healthy project produces no output, so with `focus_projects=["prod-a","prod-b"]` both healthy, `output` is empty, the `if not output:` fallback at `:395` fires, and the endpoint returns a single row for **`target_project`** — the admin project, which need not be in `focus_projects`.

> Best case the operator misreads it as confirmation about prod-a/prod-b. Worst case `finops-admin` itself has `adaptive=off`, so `:411` hands them a ready-to-paste `ALTER PROJECT \`finops-admin\` SET OPTIONS (…)` on a screen otherwise about prod-a and prod-b.

**Fix:** always emit one `HBOStatus` per checked project including `enabled=True`, and restrict the fallback to the requested scope.

### M6 — Chargeback rows don't add up

**`src/cost_attribution.py:212-215`** — three independent `round(…, 2)` calls per row. With `direct_cost = 1.004` and `allocated_waste = 1.004`, the row renders as **`1.00 + 1.00 = 2.01`**. Across a 400-project reservation the column totals drift from the bill by a couple of dollars with no residual row to absorb it. For a document people reconcile line-by-line against a GCP invoice, a row that visibly doesn't add is a credibility problem out of proportion to the cent involved.

**Fix:** compute in `Decimal` or integer cents, round once, assign the residual to the largest-share project.

### M7 — `sku_rate` / `total_admin_bill` accept negative and infinite values

**`src/cost_attribution.py:26-28`** — no `ge=0`, and Pydantic v2 permits inf/nan by default.

> `{"sku_rate": 1e400}` is valid JSON, parses to `float('inf')`, validates, and persists. `/calculate` then produces `inf` costs; Starlette's `JSONResponse.render` uses `allow_nan=False` and raises **after** the handler's `try/except` has returned — an unhandled 500 with no `Ref:` correlation ID, invisible to the request_id machinery the rest of the module maintains.

Negative `sku_rate` is quieter and worse: it makes `total_res_direct_cost` negative, so `waste_cost = bill − (negative)` _inflates_ the distributed waste above the actual bill, compounding C1(a).

**Fix:** `Field(ge=0, allow_inf_nan=False)` on both.

### M8 — `save_config` truncates in place

**`src/cost_attribution.py:78-84`** — `open(CONFIG_FILE, "w")` truncates before writing, with no atomic rename and no locking. Two concurrent `POST /config` calls, or a Cloud Run `SIGTERM` between truncate and flush, leaves a partial or empty file. `load_config` deliberately does _not_ swallow parse errors (correct, per its docstring), so **every** subsequent `GET /config` and `POST /calculate` returns 500 until someone repairs the file by hand — and on Cloud Run there is no shell in which to do that.

Distinct from F7, which addresses the storage _location_. Whichever backend F7 lands on, the write itself must be atomic: temp file + `os.replace`, or a GCS generation precondition.

### M9 — `max_bytes_billed_gb` has no field constraint; the clamp silently rewrites bad input

**`src/utils.py:396-402`; field declared at `utils.py:87`, `hbo.py:29`, `cost_attribution.py:42`**

The clamp arithmetic is correct. The problem is that it is silent and happens deep in the call stack rather than at the API boundary.

> A user means to type `500` and types `5`. They get a 5 GiB cap. Every org-wide query fails with `bytesBilledLimitExceeded`, and the handler tells them to _"raise `max_bytes_billed_gb`"_ — a cap they believe they already raised. No log line records that the requested value was rewritten. Same for a negative value, which becomes 1 GiB.

**Fix:** `Optional[int] = Field(default=None, ge=1, le=10240)` so Pydantic 422s at the edge, and `logger.warning` whenever the clamp actually changes the value.

### M10 — `_to_metric` divides by `lookback_days` with no guard

**`src/fluid_scaling.py:390`** — `FluidEstimateParams.lookback_days` has `ge=1` (`:68`) so the HTTP path is safe, but any direct or internal caller passing `0` raises `ZeroDivisionError`. No test covers it either way.

---

## 5. Medium — frontend

### F-M1 — "Abort on validation errors" doesn't abort

**`static/app.js:653-705`** — the handler mutates `state` and writes `localStorage` **before** checking `validationErrors`:

```js
state.orgProject = elements.cfgOrgProject.value.trim();                        // :653
state.adminProject = elements.cfgAdminProject.value.trim();                    // :661
localStorage.setItem('bq_admin_project', state.adminProject);                  // :666
state.maxBytesBilledGb = …; localStorage.setItem('bq_max_bytes_billed_gb', …); // :675-677
state.focusProjects = raw.split(',')…;                                         // :686
safeSetLocalStorage('bq_focus_projects', JSON.stringify(state.focusProjects));  // :695
…
if (validationErrors.length > 0) { showNotification(…); return; }              // :702-704
```

Only `bq_org_project`, `bq_region`, the cache flush, and the header labels are skipped by the early return.

> Type `PROD` (uppercase — fails the regex) into Focus Projects and save. Toast: "Invalid Focus Project ID(s)." But `state.focusProjects` is now `['PROD']` and persisted; the header still shows the _old_ scope; and **the cache flush never ran**. Every subsequent Analyze sends the invalid scope, is rejected, and the module falls back to rendering the previous scope's cached results. Reloading restores `['PROD']`.

**Fix:** validate into local variables first; `return` before touching `state` or `localStorage`.

### F-M2 — Six scope-dependent caches survive a scope change

**`static/app.js:710-717`** — the flush uses a `_results` suffix filter plus a hand-written list. Six scope-dependent keys match neither:

| Key                              | Written | Read back and rendered |
| -------------------------------- | ------- | ---------------------- |
| `bq_slots_utilization`           | 1595    | 4625                   |
| `bq_slots_actual_provisioning`   | 1609    | 4626                   |
| `bq_slots_provisioning_timeline` | 1611    | 4644                   |
| `bq_slots_tiered`                | 1651    | 4606                   |
| `bq_fluid_estimate_data`         | 5527    | 5396                   |
| `bq_fluid_simulation_data`       | 5536    | 5414                   |

All six are per-`org_project_id` **and per-`region`**.

> Analyze slots in `region-us`, switch Region to `region-eu`, save. On the next load the Slots view repopulates the utilization chart, the provisioning timeline, and the tiered-recommendations table from the **US** cache, directly beneath a header reading `region-eu`. Fluid Scaling shows US savings estimates. Nothing marks the data stale, and the user's next action is to act on baseline / autoscale-max recommendations for the wrong region.

Two entries in the hand-written list — `bq_cost_attr_results` and `bq_cost_attr_config` — are **dead**; no such keys are ever written (the real one is `bq_cost_attribution_results`, caught incidentally by the suffix filter). The explicit list gives a false sense of completeness.

**Fix:** invert it to an allow-list of scope-_independent_ keys and drop everything else.

### F-M3 — HBO KPI tiles show two different numbers for the same data

**`static/app.js:2938-2939`** (cached path) vs **`:3061-3062`** (live path)

`renderHboResults` sums whatever rows it is handed and writes the tiles. The live handler calls it with `analyzeData.slice(0, 10)` and then **overwrites** the tiles with org-wide totals from `/api/hbo/summary` — but only the top-10 slice is cached at `:3064`; `summaryData` never is. On reload, `:3190` recomputes the tiles from 10 rows.

> Run HBO analysis on an org with 4,000 optimized jobs. Tiles read `$18,400 saved`. Press F5 — same view, same scope, no new data — and they read `$62.10`. **~300× difference** depending on whether the user reloaded before screenshotting for a stakeholder.

`HBOAnalyzeParams.limit` defaults to 10 (`src/hbo.py:61`) while `/api/hbo/summary` is genuinely org-wide, so the two numbers are structurally incomparable.

**Fix:** cache `summaryData` under `bq_hbo_summary`, flush it with the others, and move tile-writing out of `renderHboResults` into a function driven exclusively by the summary payload.

### F-M4 — `err.detail` renders as `[object Object]`

**~20 sites, e.g. `static/app.js:1706`, `:3779`, `:3821`, `:4453`** — FastAPI's 422 body is `{"detail": [ {...} ]}`. `detail` is an **array**, which is truthy, so `err.detail || 'fallback'` never fires and `new Error([...])` stringifies to `[object Object]`.

422 is reachable from the UI: `StorageParams` requires `time_travel_hours` whenever `time_travel_rescale < 1.0` and constrains it to `{48,72,96,120,144,168}`; `AIParams.limit` is `le=100`; `lookback_days` is `le=90`; `discovery_strategy` is a strict `Literal`.

> Set Time Travel Rescale to 0.5 without picking an hours value, click Analyze, get a red toast reading `[object Object]`. The backend said exactly what was wrong and the frontend threw it away.

**Fix:** a shared `detailToMessage(detail, fallback)` helper that handles string, array-of-`{loc,msg}`, and absent.

### F-M5 — Dry Run has no `response.ok` check, then `.toFixed()` on the error body

**`static/app.js:4317-4341`** — the only `fetch` of 40 with no status check. `/api/ai/dry_run` returns a hard 400 (`src/main.py:2280`) for anything not `SELECT`/`WITH`.

> The AI rewrite starts with `CREATE OR REPLACE TABLE` or `MERGE` — common for the partition/cluster recommendations this tool emits. Backend returns 400 with a useful `detail`. `dryData.valid` is `undefined` → falsy → the code prints **"Dry-run failed: Unknown error"** and discards the message. On a 403 the same thing happens, so the user cannot distinguish a permissions problem from a bad rewrite.

If `valid` were ever truthy with absent numerics, `:4341` throws inside the click handler and the button stays disabled with a spinner, because the re-enable is downstream of the throw.

### F-M6 — Scope-map load failure fails **open**

**`static/app.js:155-185`** — on a non-OK or thrown `/api/meta/scope-map`, `FOCUS_SCOPE_MAP` stays `{}` and `buildPayload` passes `focus_projects` through unchanged. That is exactly wrong: `OrgParams` sets `model_config = ConfigDict(extra="forbid")` (`src/utils.py:78`), which Pydantic v2 propagates to all subclasses. Ten endpoints inherit it — `/api/slots/analyze`, `/simulate`, `/utilization`, `/tiered_recommendations`, `/fluid_simulation`, `/actual_provisioning`, `/peak`, plus `/api/fluid-scaling/status` and `/estimate`. Sending them `focus_projects` is a hard **422**.

> A transient 502 or a cold-start timeout on the boot fetch at `:6110`. For any user with focus projects configured, Slots, Slots Simulator, and Fluid Scaling now 422 on every button for the whole session. The only signal is a `console.warn`; the toast shows `[object Object]` (F-M4).

**Fix:** fail closed — default unmapped endpoints to `'org'` (strip), or block with a retry banner. A hardcoded fallback map is defensible since the backend derives it deterministically.

### F-M7 — Double-escaping corrupts AI advice, migration YAML, and dry-run errors

**`static/app.js:4075-4080`, call sites `:4210`, `:4246`, `:4332`, `:4363`** — a local `escHtml` is applied to data the global proxy has _already_ escaped, so `&` → `&amp;` twice.

> `migration_applied_yaml` contains `flags: --dry-run "true"`. Proxy → `--dry-run &quot;true&quot;`. `escHtml` → `--dry-run &amp;quot;true&amp;quot;`. The user literally sees `--dry-run &quot;true&quot;` in the `<pre>`. Same for the optimized-query preview and BigQuery error strings, which are full of quoted identifiers.

The security comment on this helper is misleading: it adds nothing (the data is already safe) while breaking display — and its _absence_ on the raw-SQL paths is what causes C8.

### F-M8 — Shared `bq_gov_results` key: each governance scan wipes the other's cache

**`static/app.js:3763` + `:3783-3789`, and `:3805` + `:3825-3831`** — two buttons write disjoint sub-keys of one entry, and each begins with `clearModuleCache(['bq_gov_results'], …)`. The read-back-and-merge is clearly _intended_ to preserve the sibling's data, but the key was already removed two dozen lines earlier, so it always reads `{}`.

> Run the Partitioned Tables Filter Scan (slow, org-wide), then the Dataset Expiration Scan. Reload. The Filter Issues table is empty; minutes of scan results are gone with no error.

### F-M9 — `reduce` with no initial value on a possibly-empty response

**`static/app.js:1730-1732`** — `/api/slots/simulate` returns a bare list (`src/main.py:3135`) that is empty whenever the utilization query finds no rows, and there is no length guard before `renderSimulationResults(data)`.

> A brand-new reservation, or a window with no jobs → `TypeError: Reduce of empty array with no initial value`, surfaced verbatim as an error toast. A raw JavaScript internal error presented as if it were a BigQuery problem.

### F-L1 — Unguarded local formatters in `renderStaticAuditResults`

**`static/app.js:1318-1328`** — `bytes === 0` is strict, so `null` slips past: `Math.log(null)` → `-Infinity` → `sizes[-Infinity]` → the cell renders **`NaN undefined`**. `formatNumber(null)` throws and kills the row loop. `StaticAuditResult` declares both fields required, so the live API path is safe; the reachable vector is snapshot import (`:370-450`), which hydrates from a user-supplied file with no shape validation.

### F-L2 — `formatDataSize` switches units at 1000 but divides by 1024

**`static/app.js:3369-3374`** — for `1000 ≤ gb < 1024` the branch fires but the division yields < 1, so a 1,010 GB scan renders as **"0.99 TB"** — reading as _smaller_ than the 999 GB scan listed above it in the same sorted table. Fix: `if (gb >= 1024)`.

### F-L3 — GiB/TiB values labeled GB/TB

Backend computes `billed_gb` as `total_bytes_billed / POW(1024, 3)` (`src/main.py:1377`, `:2353-2354`); frontend does the same at `app.js:4128` and `:1335`. These are **GiB**, rendered with the label `GB` — a 7.4% understatement versus the decimal GB shown in the BigQuery console and GCP billing exports. The dollar math is _not_ affected (the per-TiB rate correctly pairs with `1024**4`). Labels only.

### F-L4 — Snapshot import: the real sanitizer is dead code

**`static/app.js:370-379`, `:419-442`** — `sanitizeImportedValue` returns non-JSON values **raw** at `:377`. Every `*_results` key is JSON so the main payload is covered, but the scalar settings keys (`bq_org_project`, `bq_admin_project`, `bq_region`) are bare strings that fail `JSON.parse` and pass through untouched — the delivery half of H1.

Separately, `importSnapshot` defines a complete, correct recursive escaper at `:422-437` under a three-line comment explaining precisely why imports need it — and **never calls it**; `:442` calls `sanitizeImportedValue` instead. Dead code that looks like the mitigation.

### F-L5 — Four `AIResult` fields the backend never populates

`src/main.py:1570` declares `dry_run_validated`, `bytes_scanned_optimized`, `estimated_savings_pct`, `is_external_table_query`; the handler at `:2184-2202` never assigns them. The frontend reads none of the four today, so nothing renders `0%` — but they are in the public response contract, and the obvious next UI change (a "% savings" column) would silently render `0.0%` for every row.

---

## 6. Medium / Low — packaging, deployment, docs

### P1 — Container runs as root

No `USER` directive anywhere in the 18-line `Dockerfile`. Sharper than usual here because the process **writes to its own source tree at runtime**: `cost_attribution.py:19-20` resolves `CONFIG_FILE` to `/app/src/`, and `main.py:147` writes `/app/app.log`. As uid 0, any path-influence bug is a root-owned write next to `src/main.py` — and a dropped `.py` there is imported on the next cold start. There is no application authentication in front of any of it (see P5).

**Fix:** `RUN useradd -r -u 10001 appuser && chown -R appuser /app` + `USER 10001`, and move mutable state off the source tree.

### P2 — Mutable base tag, zero pinned dependencies

`FROM python:3.11-slim` re-resolves every build. All nine requirements are open ranges installed with plain `pip install -r`, no lockfile, no `--require-hashes`; `requirements-dev.txt` is looser still (`pytest>=7.0`, no upper bound).

> Two builds of the identical SHA a week apart produce different images. A breaking `pandas` minor, or a compromised release of any transitive dep of `google-cloud-bigquery-storage`, reaches production with nothing in git to bisect against. When a CVE lands there is no record of which version the running revision shipped.

No _currently_ known-vulnerable pin exists — precisely because nothing is pinned; the floors are all above their respective advisories. Correct floor policy, but not a substitute for a lock.

**Fix:** digest-pin the base image; `pip-compile --generate-hashes`; install with `--require-hashes`.

### P3 — No `.dockerignore`

`COPY src/ src/` bakes into the production image:

- **`src/CODEREVIEW.md` — 154 KB**, enumerating every unfixed finding with exact file:line and reproduction detail
- `src/__pycache__/`
- `src/cost_attribution_config.json`

Not web-reachable (only `static/` is mounted), so not direct disclosure — but anyone who can `docker pull` from Artifact Registry, or who gets any foothold in the container, receives a prioritized list of the application's own unpatched weaknesses.

More practically: shipping `cost_attribution_config.json` in the image means **a rebuild silently reverts** whatever chargeback config an operator saved at runtime — a second, independent path to the data loss F7 describes.

### P4 — `CMD` hardcodes port 8080 instead of honouring `$PORT`

```dockerfile
CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8080"]
```

Exec form, so no shell expansion is available even if `$PORT` were written. Cloud Run injects `PORT` and it is not always 8080.

> Deploy with `--port=8081` to avoid a sidecar conflict. Uvicorn still binds 8080, the startup probe never connects, and the revision fails with _"the user-provided container failed to start and listen on the port defined by the PORT environment variable"_ — a message that sends the operator hunting through startup code rather than to this line.

**Fix:** `CMD exec uvicorn src.main:app --host 0.0.0.0 --port ${PORT:-8080}`.

### P5 — `AUTH_ENFORCED_UPSTREAM` is honour-system only

**`src/main.py:114-121`, `:135-142`; `README.md:162`**

The check cannot be bypassed per-request — it is at import time and the process genuinely refuses to start. But it asserts nothing about reality:

1. `.env` is loaded at `:114-121`, **before** the check at `:135`. The README instructs `echo 'AUTH_ENFORCED_UPSTREAM=true' >> .env`, and `.env` is gitignored. A developer sets it once for a local run and it stays satisfied forever, invisibly, in that tree. Today `COPY src/ static/` doesn't copy `.env` — but a `docker run -v $PWD:/app`, or anyone widening the COPY to `COPY . .`, carries the attestation into a deployed container.
2. Setting the flag and deploying with `--allow-unauthenticated` are independent actions. Nothing detects the combination, which is exactly the scenario the guard exists to prevent.

**Fix:** keep the startup flag and add a runtime assertion in `inject_request_id` — when `K_SERVICE` is set, reject any request lacking `X-Goog-Authenticated-User-Email` / `X-Goog-IAP-JWT-Assertion`. That converts an attestation into an enforced control.

### P6 — Up to 60 MiB of duplicate logs in Cloud Run's tmpfs

`src/main.py:147` — `RotatingFileHandler(maxBytes=10MiB, backupCount=5)` writing `/app/app.log`. On Cloud Run the container filesystem is **tmpfs and counts against the memory limit**. On a 512 MiB instance that is ~12% of RAM duplicating logs Cloud Run already captures from stdout via the `StreamHandler` on the line above. Combined with a pandas DataFrame from a 90-day org-wide scan, a plausible contributor to an OOM kill — which surfaces as a bare 503 with no application log line, since the log file dies with the instance.

The Dockerfile also sets no `PYTHONUNBUFFERED=1`, so stdout to a pipe is block-buffered at 8 KB and the last lines before a `SIGKILL` are lost precisely when needed.

**Fix:** skip the file handler when `K_SERVICE` is set; add `ENV PYTHONUNBUFFERED=1`.

### P7 — Dependency and dead-code hygiene

- **`requests` is imported in two `src/` modules and absent from `requirements.txt`.** It currently resolves transitively; a future pin change breaks the image at import time.
- **`pyOpenSSL` is pinned and imported nowhere** in `src/`. If it is a transitive-pin workaround it belongs in a constraints file with a comment; otherwise it is needless attack surface and build time.
- **`src/bqrecommender.py` — 751 lines, imported by nothing**, not by `src/main.py` and not by any test. 0% coverage. Module-level `TOKEN_PROVIDER = ThreadSafeTokenProvider()` at `:166` performs **import-time authentication**, so merely importing it reaches out for credentials.

### P8 — Docs contradict the code and each other

- `RELEASE_NOTES.md` documents **v1.3.0 dated 2026-07-27** while `src/main.py:58` says `__version__ = "1.2.3"`.
- `README.md` contradicts itself on AI Doctor parameters: the module table claims `max_output_tokens: 1024` / `thinking_level: MINIMAL`, while the "AI Doctor Cost Control" section at line 311 claims `thinking_budget: 0` / `max_output_tokens: 300`. **The code (`src/main.py:2013`) actually uses `max_output_tokens: 8192`, `thinking_level: MEDIUM`, `temperature: 0.1`, and all four safety categories set to `OFF`** — so both documented figures understate token spend by 8× and 27× respectively, and neither mentions the safety configuration.
- `docs/static/app.js` is **stale, not forked** — 12 hunks, all one-way additions in `static/` (6,221 lines vs 5,979). The docs bundle is internally self-consistent (older JS + older 6-column `<thead>` at `docs/simulator.html:2721` + older fixture), so it renders — but the public GitHub Pages demo shows none of Severity, Original Cost, Optimized Query, Copy SQL, Dry Run, or the discovery-strategy selector. There is no build step, copy script, or CI check keeping them in sync.
  **One detail matters beyond the drift:** `docs/` still carries the comment _"already HTML-escaped by the global sanitizeData() fetch wrapper — do not escape again here."_ That comment was **deleted** from `static/` in the same changeset that added the redundant `escHtml` helper — and the invariant it documented was then violated in both directions (F-M7 double-escapes, C8 fails to un-escape). Anyone diffing the two files to understand the escaping contract gets the correct explanation only from the stale copy.

### P9 — Published demo data uses a real domain

`docs/data/mock_data.json` (21 KB) and `docs/finops-snapshot_dummy.json` (2.9 MB) contain synthetic user emails on the author's **real** registered domain — `amanda@mbettan.com`, `andrew@mbettan.com`, an alphabetical generated first-name list — plus `bi-dashboard@reporting.iam.gserviceaccount.com`, `data-sci@analytics-prod.iam.gserviceaccount.com`, and `analyst.smith@company.com`. Minor hygiene; RFC 2606 reserves `example.com` for exactly this.

### P10 — `_ALIAS_RE` uses `$` where `_IDENT_RE` correctly uses `\Z`

**`src/utils.py:64-65`**

```python
_IDENT_RE  = re.compile(r"^[a-zA-Z0-9_\-\.\:]+\Z")   # correct
_ALIAS_RE  = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$") # $ matches before a trailing \n
```

`_ALIAS_RE.match("j\n")` succeeds, yielding `"AND j\n.project_id IN UNNEST(@focus_projects)"`. **Not currently exploitable** — all 21 call sites of `build_project_filter` were checked; exactly one passes `table_alias` and it is the literal `"j"` (`src/main.py:3844`). Reported because it is a one-character fix and the inconsistency with the sibling regex two lines above invites a future caller to plumb an alias from request data.

### P11 — `_safe_ident` accepts multi-part paths

**`src/utils.py:64`, `:164-168`** — `.` and `:` are in the character class, so `"a.b.c"`, `"..."`, `":::"` and `"-"` all pass a function whose docstring says "Validates that a string is a safe GCP identifier." Inside backticks, a quoted string containing dots parses as a **path**, not a single name, so the invariant callers rely on does not hold.

**No working exploit was constructed**, and that should be explicit: every interpolation site has the shape `` `{proj}`.`{region}`.INFORMATION_SCHEMA.<VIEW> ``, and INFORMATION_SCHEMA requires exactly that 4-part form, so extra components yield a parse error rather than a redirect. Filed as defence-in-depth on the security core, not as a live vulnerability.

### P12 — `err_msg.splitlines()[0]` raises `IndexError` on an empty exception string

**`src/utils.py:355`, `:364`, `:376`** — `"".splitlines()` returns `[]`, not `[""]`. Any exception whose `str()` is empty — a bare `concurrent.futures.TimeoutError()`, `ConnectionResetError()`, `Exception()` — converts a logged-and-retried transient error into an `IndexError` raised _from inside the except block_. The original error is destroyed, the retry loop never runs, and the user gets a generic 500 logged as `Unexpected error in <service>` pointing at the logging statement rather than the fault.

**Fix:** `first_line = (err_msg.splitlines() or [repr(e)])[0]` once at the top.

---

## 7. Corrections and unverified claims

### F3 is half-landed, not fixed

The backend does return `is_complete`, `unattributed_reservations`, and `total_unattributed_slot_hours` (`src/cost_attribution.py:270-275`). But grepping all three JS/HTML files for `is_complete|unattributed` returns **zero hits** — no frontend consumer exists, so the reconciliation warning is invisible to the user it was added for. An earlier verification pass in this review marked F3 FIXED; that verdict was wrong and is corrected here.

### The lowercase-DDL claim is dropped

The generated DDL at `src/main.py:913` and `:915` emits `storage_billing_model='physical'` / `'logical'` in lowercase (from `better_on`), while the codebase reads uppercase `'PHYSICAL'` / `'LOGICAL'` everywhere else (`:830`, `:844`, `:863`, `:876-877`). Whether BigQuery rejects the lowercase option value was **not confirmed**, so this is recorded as an inconsistency worth a 30-second check against a scratch dataset — **not** asserted as a defect.

### Test-run artifact

The empirical run provisioned `.venv-test/` (CPython 3.12.10 + dependencies, several hundred MB) in the project root. All five source mutations and the control were restored and re-verified byte-correct. The venv itself was left in place and can be deleted.

---

## 8. Verified as _not_ bugs

Recorded so nobody "fixes" them:

- **`_IDENT_RE` trailing-newline bypass does not exist.** `utils.py:64` uses `\Z` (absolute end) _and_ `\n` is not in the character class — `"abc\n"` fails on both counts.
- **`get_max_bytes_billed` clamp math is arithmetically correct**, including negatives: `gb=-5` → `min(-5,10240)=-5` → `max(1,-5)=1` → 1 GiB; `gb=0` short-circuits to the 200 GiB default. The problem is only that it is _silent_ (M9).
- **`build_project_filter` cannot be bypassed.** Values go through `ArrayQueryParameter`; `column` is checked against a closed set before any interpolation.
- **`handle_endpoint_exception` correctly re-raises `HTTPException`** (`src/utils.py:245-246`), so deliberate `raise HTTPException(400, …)` inside a `try` is not converted to a generic 500.
- **`request_id` contextvar propagation into the HBO thread pool is correct** (`hbo.py:364-370`) — explicit capture before spawn, `set`/`reset` per worker, per-request executor. This is the right pattern.
- **Rule B does not double-count waste** — the per-usage loop contributes only `direct_cost` when the rule is B.
- **Rule A's proportional shares genuinely sum to 1.0** within a reservation. The reconciliation failures in C1 are the `max(0,…)` clamp and the zero-denominator branch, not the share arithmetic.
- **`response.ok` coverage is good.** All 40 `fetch` call sites were enumerated; only two lack a status check (H8, F-M5). The rest guard correctly, including the `Promise.allSettled` fan-outs at `app.js:1593-1648`.
- **DataTables column counts match.** All eight cross-checked tables agree between `<thead>` and the JS row builders (`#ai-results-table` 9/9, `#hbo-results-table` 4/4).
- **Dashboard field names match exactly** — `mtdSpend`, `opportunityCount`, `anomalyCount` are camelCase on both sides. The dashboard problem is null _values_ (C7), not wrong names.
- **`org_status.error_message`** — the conditional 4th key on the storage region-not-enabled path (`main.py:944`) is correctly handled by the guard at `app.js:1456`.
- **`/api/hbo/performance_insights` heterogeneous keys** — the three lists carry different 4th keys (`stage_id` vs `diff_pct`); `app.js:3140/3160/3180` reads the right one from each.
- **`median_slots` vs `p50_slots`** — `/api/slots/utilization` returns `median_slots`; the frontend references neither name, so there is no mismatch to fix.
- **Extra fields on `FocusMixin` endpoints** — 15 of 25 POST models allow extras, so stray fields like `max_bytes_billed_gb` are silently ignored rather than 422'ing. Intentional.
- **`buildPayload` strips `focus_projects` correctly** for all ten org-scoped routes — _provided the scope map loaded_ (F-M6). `get_scope_map` (`main.py:236`) iterates `app.routes` at request time and classifies all 36 handlers correctly.

---

## 9. Suggested fix order

By ratio of damage to effort:

1. **C1 + C2** — the chargeback reconciliation cluster. One pass over `cost_attribution.py`, plus a single property test asserting `Σ attributions == Σ total_admin_bill`, which catches all four sub-findings at once.
2. **H6 + H7 + M2** — the `utils.py` error taxonomy. One pass over `handle_endpoint_exception` and the retry predicate. H7 actively costs money.
3. **C8 + F-M7** — the frontend escaping contract. Exempt `optimized_query` alongside `ddl`, delete the redundant `escHtml`, restore the two deleted invariant comments.
4. **C3** — the migration success signal, the finding most likely to make someone deploy a "rewrite" that isn't one.
5. **C5, C7, H4, H5** — small, isolated, each one a visibly wrong number on a user-facing panel.
6. **H1 + F-L4** — the XSS sink and its delivery path.
7. **P1, P4** — two-line Dockerfile changes, no behavioural risk.
8. **H2, H3, H8, H9**, then the Mediums, then the Lows.

### On tests

The highest-leverage change is **not** writing new tests. Three files re-implement the formula locally and assert against the copy:

- `tests/test_design_invariants.py:32-56` — `_compute_editions_cost()`, docstring _"Replicate the editions cost calculation from analyze_jobs (main.py L850-872)"_. The real code is at `src/main.py:1009-1032` — the reference has **already drifted ~160 lines** — and the copy hardcodes `slot_step_size=50` where the source reads `params.slot_step_size`. This is why mutation #4 went undetected while `test_small_job_matches_identity` kept passing.
- `tests/test_fluid_scaling.py:63-83` — `fluid_slot_seconds()`, docstring _"Pure-Python oracle of the BigQuery Fluid Scaling formula"_, with 6 parametrized cases. Nothing ever compares the oracle to the actual SQL. The SQL could drop its `GREATEST(...)` clamp and all 6 still pass.
- `tests/test_fluid_scaling.py:330-340` and `:390-398` — build `active` and `enabled` sets _in the test_, subtract them, assert empty. Tautology; `_build_config_status` is never called. (`:320-325` is similar — asserting properties of a list literal defined 15 lines above, in a class whose docstring claims to guard the HTML `<thead>` and the DataTables sort index, while reading neither.)

Re-pointing those at `src` converts ~15 currently-vacuous tests into real ones for free.

Then add numeric suites, modeled on `tests/test_physical_bytes_decomposition.py` (the one file in the repo that does this right — it pins real dollars against real `src` code and caught 32 control mutations):

- **`_to_metric`** (`src/fluid_scaling.py:375-405`) — computes _all_ fluid-scaling dollars, is a pure function with no I/O, and is **imported by `tests/test_fluid_scaling.py:20` and never called**. Alongside `DAYS_PER_MONTH`, `DAYS_PER_YEAR`, and `SECONDS_PER_HOUR`, all four imported and never referenced in the 398-line file. Highest value, lowest effort in the repo.
- **`calculate_cost_attribution`** — no test file exists at all. Untested: slot-hour conversion, Rule A allocation, the Rule B central dump and its 400, the negative-waste clamp, reservation-ID normalization (`res-a` vs `proj:loc.res-a` vs `proj.loc.res-a` — a mismatch silently routes every slot-hour into `unconfigured`), and the `total_unattributed_slot_hours` accounting.
- **`get_hbo_summary` / `analyze_hbo`** — no test file. `src/hbo.py:102-112` and `:180-194` are dark; `test_hbo_summary_has_totals` asserts only that three keys exist, and with zero rows it passes on the all-zeros fallback at `:198`.

Other structural gaps worth a line each:

- **`tests/conftest.py`** defines `mock_bq_row_factory` (`:31`) and `mock_bq_job` (`:60`) — used by **zero** tests. `mock_bq_row_factory` is precisely the fixture that would make the empty-mock problem fixable. It exists, it is correct, nobody calls it.
- **SQL assertions are substring checks.** `tests/test_governance_lookback.py:29-30,38,45,76` assert `"INTERVAL 30 DAY" in sql` etc. — which pass if the query selects the wrong table, filters the wrong column, uses `JOBS_BY_PROJECT` instead of `JOBS_BY_ORGANIZATION`, or drops `job_type = 'QUERY'`. `_captured_sql()` (`:18-20`) also reads only the **last** `call_args`, so in multi-query endpoints it silently validates the wrong statement.
- **Security tests probe exactly one character.** `tests/test_security.py:10-33` hits 23 endpoints with the identical payload ``"proj`ect"``. Untested against `_safe_ident`: newline, `--`, `;`, `/*`, `${}`, unicode homoglyph, `..`, whitespace padding. One `@pytest.mark.parametrize` over hostile strings × two endpoints beats 23 endpoints × one character.
- **The `AUTH_ENFORCED_UPSTREAM` startup guard is never tested** — `conftest.py:15` sets it before import, so no test verifies the app refuses to start without it.
- **`tests/test_integration_client.py`** (201 lines, 6 tests) is skipped wholesale at module level because `BIGQUERY_PROJECT_ID` is the placeholder `mbettan-project`. Correct to skip, but the file is inert in CI and its `@pytest.mark.integration` marks are unregistered (7 `PytestUnknownMarkWarning`s). **`tests/test_sibling_tracing.py`** is 92 lines testing a feature the skip reason itself says "are not implemented in the backend."
