# 🚀 Release Notes: BigQuery FinOps Optimizer

---

## v1.1.4 — 2026-07-11

Data correctness and deployment hardening release addressing **14 findings** from a 4th independent code review (Cloud SWE / DB Architect / AppSec perspective).

### 🐛 Bug Fixes

#### fix(slot-utilization): concurrency metrics measured single-job peaks instead of org-wide demand
*   **Root Cause:** `analyze_slot_utilization` took `MAX`/`APPROX_QUANTILES` over raw `JOBS_TIMELINE_BY_ORGANIZATION` rows. Since this view emits one row per `(job_id, period_start)`, the aggregate measured the **largest single job's per-second slot usage**, not total concurrent org demand. With N jobs running simultaneously, `max_slots` underestimated peak concurrency by up to N×.
*   **Fix Applied (`src/main.py`):**
    *   Added a `per_second` CTE that `SUM(period_slot_ms) GROUP BY period_start` — aggregating across all concurrent jobs before taking MAX/quantiles
    *   Fixed `bytes_billed_avg` divisor from hard-coded `/60` to `COUNT(*)` (actual seconds in bucket)
*   **Impact:** `max_slots`, `p90_slots`, `p99_slots` now correctly reflect org-wide concurrent demand. Capacity planning decisions based on these metrics are no longer systematically under-provisioned.

#### fix(mv-auditor): MV inventory was project-scoped while jobs were org-scoped
*   **Root Cause:** The MV Cost Auditor built its MV inventory from single-project `INFORMATION_SCHEMA.TABLES` (admin project only), then joined against `JOBS_BY_ORGANIZATION` (org-wide). MVs in any project other than the admin project were invisible — their refresh costs were silently missed, producing a falsely "healthy" picture.
*   **Fix Applied (`src/main.py`):**
    *   Added a project discovery step: queries `DISTINCT destination_table.project_id` from `JOBS_BY_ORGANIZATION` to find all projects with MV refresh activity
    *   Batches `INFORMATION_SCHEMA.TABLES` queries across discovered projects (20 per UNION ALL batch) with `_safe_ident` validation
    *   Gracefully handles per-batch failures without aborting the entire audit
*   **Impact:** MV refresh costs are now accurately measured org-wide. MVs in non-admin projects are no longer silently excluded.

#### fix(cost-attribution): `focus_projects` corrupted waste allocation math
*   **Root Cause:** `reservation_totals` was built from focus-filtered job data, but `total_admin_bill` covered the entire reservation. With a focus filter, excluded projects' legitimate usage was reclassified as "waste" and dumped onto focused projects.
*   **Fix Applied (`src/cost_attribution.py`):**
    *   Cost attribution now rejects `focus_projects` with a `400` error and explanation
    *   Consistent with how capacity-planning endpoints already exclude focus filters
*   **Impact:** Cost attribution waste allocation is always computed against full-reservation usage.

#### fix(compute): On-Demand cost incorrectly included failed queries
*   **Root Cause:** BigQuery does not bill On-Demand for queries that fail with errors, but the model computed `on_demand_cost = bytes_billed × rate` for errored jobs, inflating the On-Demand side and biasing the comparison toward Editions.
*   **Fix Applied (`src/main.py`):**
    *   `on_demand_cost = 0.0 if has_error else (bytes_billed / TB_CONVERSION) * rate`
*   **Impact:** Editions vs. On-Demand comparison is no longer artificially skewed by error-heavy workloads.

#### fix(hbo): project query failures silently reported as "HBO enabled"
*   **Root Cause:** `_check_project_status` returned `(project, None)` on any failure (403, quota, etc.). The results loop only appended projects where `enabled is False`, so failed projects were indistinguishable from "enabled" ones.
*   **Fix Applied (`src/hbo.py`):**
    *   Added `status: str = "known"` field to `HBOStatus` model
    *   Projects where the query failed are now surfaced with `status="unknown"`
*   **Impact:** Admins can distinguish between "verified enabled" and "could not verify" instead of seeing a false all-green dashboard.

### 🛡️ Hardening

#### hardening(env): `.env` loader no longer overrides real environment variables
*   **Root Cause:** `os.environ[key] = val` unconditionally overwrote Cloud Run / GKE injected variables (`GOOGLE_CLOUD_PROJECT`, `LOG_LEVEL`, credential paths) if a stray `.env` file was present.
*   **Fix:** Changed to `os.environ.setdefault(key, val)` — `.env` only fills in missing variables.

#### hardening(storage): error fallback response shape now matches success shape
*   **Root Cause:** The `analyze_storage` "views not enabled" fallback returned a dict without `effective_pricing_ratio`, causing frontend destructuring errors.
*   **Fix:** Added `"effective_pricing_ratio": 0` to the fallback dict.

#### hardening(tiered-recs): typed exception matching replaces brittle string matching
*   **Root Cause:** The org-to-project fallback used `if "Access Denied" in str(e)` — brittle across SDK versions/locales.
*   **Fix:** Changed to `except (gax_exc.Forbidden, gax_exc.NotFound)`.

#### hardening(cost-attribution): date range `start <= end` validation
*   **Root Cause:** Reversed date ranges (`start > end`) silently returned empty results, presented as `$0.00` attribution — a legitimate-looking but incorrect result.
*   **Fix:** Added `@model_validator(mode='after')` that rejects reversed ranges.

#### hardening(cost-attribution): deprecated `config.dict()` → `model_dump()`
*   Pydantic v2 migration: `config.dict()` → `config.model_dump()`.

#### hardening(cost-attribution): removed dead `/test-hbo` debug endpoint
*   Leftover debug endpoint removed — reduces attack surface.

### ✨ Improvements

#### feat(compute): response includes sample metadata
*   The `analyze_jobs` response now includes `sample_info.sampled_job_count` and a note explaining the sampling bias (`ORDER BY bytes_billed DESC`). Enables the frontend to display "Analysis based on top N jobs by bytes_billed."

#### feat(cache): versioned static assets get long-lived caching
*   Static assets served with `?v=<hash>` query parameters now receive `Cache-Control: public, max-age=31536000, immutable` instead of `no-store`. Unversioned paths retain `no-store`. Reduces repeat page-load latency.

#### feat(logging): `RotatingFileHandler` gated behind `ENABLE_FILE_LOG` env var
*   Cloud Run containers use tmpfs — writing `app.log` consumes instance memory and vanishes on scale-down. File logging is now opt-in via `ENABLE_FILE_LOG=1`. Stdout logging (always active) is sufficient for Cloud Run's Cloud Logging integration.

---

## v1.1.3 — 2026-07-11

Security hardening and correctness release addressing **9 findings** from a comprehensive tri-report code review (3 independent backend reviews + 1 frontend security audit, with independent validator cross-check).

### 🔒 Security Fixes

#### fix(frontend): DOM-based Stored XSS via sanitizer whitelist bypass
*   **Root Cause:** The global `window.fetch` proxy sanitizer ([`app.js:59-60`](static/app.js)) whitelisted 5 JSON keys (`query`, `sql`, `gemini_optimization_advice`, `ddl`, `referenced_schemas`), passing their values through without HTML-escaping. The Slots Profiler renderer at line 2068 then injected `row.query` directly into `.innerHTML` via DataTables' `table.row.add()` — with no local escaping. A BigQuery query containing `SELECT "<img src=x onerror=alert(1)>"` would execute as JavaScript when an admin viewed the profiler table.
*   **Fix Applied (`static/app.js`):**
    *   Added a local `esc()` HTML-escape helper inside `renderProfilerQueries`
    *   `row.query` is now escaped in both the `title` attribute and innerHTML content
*   **Note:** The AI Doctor renderer was **not** vulnerable — it already applies its own local `escapeHtml()` at line 3934 and `renderMarkdown()` HTML-escapes at lines 3893-3896. The DDL renderer uses `.textContent` (safe).

#### fix(frontend): full application XSS via snapshot hydration bypass
*   **Root Cause:** The `importSnapshot()` function wrote uploaded JSON directly to `localStorage` without sanitization. On page reload, cached data is parsed and passed straight to render functions — completely bypassing the global `fetch`-proxy sanitizer. An attacker could craft a malicious snapshot containing `<img src=x onerror=...>` in any field, trick an admin into importing it, and achieve persistent XSS across all dashboard views.
*   **Fix Applied (`static/app.js`):**
    *   Added `sanitizeImport()` — a recursive HTML-escaping function applied to all imported snapshot values before writing to `localStorage`
    *   String values are escaped; objects/arrays are recursively traversed; non-string primitives pass through unchanged

### 🐛 Bug Fixes

#### fix(cost-attribution): waste silently vanishes under Rule B (Central Dump)
*   **Root Cause:** When `waste_rule="B"` (Central Dump) was selected but `central_cost_center_project` was `None` (the default in `cost_attribution_config.json`), the guard condition `if waste_rule == "B" and central_cost_center_project:` was falsy. The waste cost was withheld from individual projects (line 204: `pass`) but the dump block was skipped entirely. Waste simply disappeared — the sum of attributed costs no longer matched the GCP invoice.
*   **Fix Applied (`src/cost_attribution.py`):**
    *   Changed the guard to an explicit rejection: Rule B now raises `HTTPException(400)` if `central_cost_center_project` is not configured
    *   Added missing `"slot_hours": 0.0` key to Rule-B dump records for schema consistency with Rule-A records
*   **Impact:** Cost attribution totals now always reconcile with the GCP bill. Users are notified if their Rule B config is incomplete.

#### fix(storage): time-travel DDL shows savings it can't deliver
*   **Root Cause:** Setting `time_travel_rescale=0.28` without specifying `time_travel_hours` caused the savings forecast to show a 72% reduction in time-travel costs. However, the generated DDL only flipped the billing model — it did **not** include `max_time_travel_hours`, so the time-travel window remained unchanged. Users who executed the DDL would never see the forecasted savings.
*   **Fix Applied (`src/main.py`):**
    *   Added `@model_validator(mode='after')` to `StorageParams` that rejects `time_travel_rescale < 1.0` when `time_travel_hours is None`
    *   Error message: *"time_travel_hours must be set when time_travel_rescale < 1.0. Without it, the generated DDL will not reduce time travel."*
*   **Impact:** Users cannot generate misleading DDL — they must explicitly specify the target time-travel window to unlock rescale savings.

#### fix(exceptions): `static_audit` and `active_assist` silently return `[]` on errors
*   **Root Cause:** Both `run_static_schema_audit` and `fetch_active_assist_recommendations` caught all exceptions and returned an empty list. A 403 (missing permissions), 404 (wrong project), or quota error was indistinguishable from "no findings." Users saw an empty table and assumed they were fully optimized, when in reality the scan never completed.
*   **Fix Applied (`src/main.py`):**
    *   Replaced `logger.warning(...); return []` with `handle_endpoint_exception(e, "...")` in both handlers
    *   Errors now surface as structured HTTP responses (403, 404, 400, or 500) with appropriate error messages
*   **Impact:** Permission and quota errors are now visible to the user. The frontend's existing error-state UI handles these correctly.

#### fix(slots): 50-slot passthrough cliff decoupled from `slot_step_size`
*   **Root Cause:** The slot-packing heuristic used a hard-coded `if effective_slots < 50:` threshold. This was intended to match BigQuery's 50-slot autoscaler increment, but `slot_step_size` is a user-configurable parameter. A user setting `slot_step_size=100` still had the passthrough cutoff at 50, creating a 2× cost discontinuity at the 50-slot boundary where jobs in `[50, 100)` rounded up to 100 while jobs `< 50` passed through at actual cost.
*   **Fix Applied (`src/main.py`):**
    *   Changed `if effective_slots < 50:` → `if effective_slots < params.slot_step_size:`
*   **Impact:** The passthrough cutoff now tracks the user's configured step size, eliminating the cost discontinuity.

### 🛡️ Hardening

#### hardening(params): bounded numeric fields prevent crash and DoS
*   **Root Cause:** Several Pydantic param models accepted unbounded integers. `slot_step_size=0` caused a `ZeroDivisionError` at `math.ceil(effective_slots / params.slot_step_size)`. `lookback_days=365` triggered an unbounded org-wide INFORMATION_SCHEMA scan that could time out or consume excessive bytes.
*   **Fix Applied (3 files):**
    *   `src/main.py` — `JobAnalysisParams.slot_step_size`: `int = 50` → `Field(default=50, gt=0)`; `min_bytes_billed`: `Field(ge=0)`; `SlotSimulationParams.lookback_days`: `Field(ge=1, le=90)`; `max_baseline`: `Field(ge=50, le=100000)`; `step_size`: `Field(gt=0)`
    *   `src/hbo.py` — `HBOCommonParams.lookback_days` and `HBOStatusParams.lookback_days`: `Field(default=7, ge=1, le=90)`
*   **Impact:** Invalid values now return a 422 Validation Error with a clear message instead of crashing. Maximum lookback window capped at 90 days.

#### hardening(errors): BigQuery error details no longer leaked to client
*   **Root Cause:** The `BadRequest` handler in `handle_endpoint_exception` returned `f"BigQuery Query Failed: {str(e)[:500]}"` to the client. BQ error messages can contain table names, column names, and SQL fragments — leaking internal schema details.
*   **Fix Applied (`src/utils.py`):**
    *   Error details are now logged server-side only (`logger.error`)
    *   Client receives a generic message: *"BigQuery Query Failed: The query contained an error; check server logs for details."*
*   **Impact:** No internal schema information is exposed to API consumers. Operators can still diagnose issues via server logs.

---

## v1.1.2 — 2026-07-10

Patch release fixing **4 bugs** and **3 code quality improvements** identified during an LLM-assisted code review.

### 🐛 Bug Fixes

#### fix(static-audit): `row_count` returned fabricated estimate instead of real row count
*   **Root Cause:** The Static Schema Audit SQL computed row count as `CAST(COALESCE(s.size_bytes, 0) / 220 AS INT64)` — a fabricated estimate dividing total logical bytes by a hardcoded 220-byte "average row width." This produced wildly inaccurate results (e.g., a 1 GB table always reported ~4.8M "rows" regardless of actual schema or row count). The constant `220` appears to be a typo for `2^20` (1 MiB), but either way the approach is incorrect because `INFORMATION_SCHEMA.TABLE_STORAGE` provides a real `total_rows` column.
*   **Affected Endpoint:** `POST /api/storage/static_audit` → `StaticAuditResult.row_count`
*   **Fix Applied (`src/main.py`):**
    *   Added `total_rows` to the `table_sizes` CTE SELECT list
    *   Replaced `CAST(COALESCE(s.size_bytes, 0) / 220 AS INT64)` with `COALESCE(s.total_rows, 0)`
    *   Updated the comment above the query (removed misleading justification for the approximation)
*   **Impact:** `row_count` now returns the real row count from BigQuery metadata instead of a fabricated estimate. All existing consumers of this field will see accurate values.

#### fix(mv-auditor): cross-project false positives due to missing `project_id` in MV key
*   **Root Cause:** The MV Cost Auditor built a set of materialized views keyed by `(table_schema, table_name)` — with **no project_id**. It then matched jobs from `JOBS_BY_ORGANIZATION` (org-wide, all projects) against this set. If two projects each had a dataset named `analytics` with a table `daily_agg`, jobs from either project would match, producing inflated refresh counts and slot-hour costs for MVs that weren't actually refreshed.
*   **Affected Endpoint:** `POST /api/antipatterns/mv` → `MVCostResult`
*   **Fix Applied (`src/main.py`):**
    *   Added `table_catalog AS project_id` to the MV list SQL query
    *   Changed the MV set key from `(table_schema, table_name)` → `(project_id, table_schema, table_name)`
    *   Changed the job lookup key from `(row.dataset_id, row.table_id)` → `(row.project_id, row.dataset_id, row.table_id)`
*   **Impact:** MV refresh detection is now project-scoped. No more phantom refresh counts from identically-named tables in different projects.

#### fix(storage): `time_travel_hours` accepted floats, producing invalid DDL
*   **Root Cause:** `StorageParams.time_travel_hours` was typed `Optional[float]`, and the validator cast to `int()` before checking the allow-list. A value like `72.5` was silently truncated to `72` (which is in the allowed set), passing validation — but the untruncated `72.5` flowed through to the generated DDL: `max_time_travel_hours=72.5`. BigQuery rejects non-integer values for this option.
*   **Affected Endpoint:** `POST /api/storage/analyze` → DDL generation
*   **Fix Applied (`src/main.py`):**
    *   Changed field type from `Optional[float]` → `Optional[int]` (Pydantic now rejects `72.5` at the API boundary)
    *   Removed the `int(v)` cast in the validator (no longer needed since the type is already `int`)
*   **Impact:** Non-integer `time_travel_hours` values are now rejected with a clear validation error instead of producing DDL that BigQuery would reject.

#### fix(cost-attribution): config file path CWD-relative, fails in containers
*   **Root Cause:** `CONFIG_FILE = "cost_attribution_config.json"` was a bare relative path, resolved against the process's current working directory. In Docker (Dockerfile sets `WORKDIR /app`), the config file is at `/app/src/cost_attribution_config.json`, but `os.path.exists("cost_attribution_config.json")` looked for `/app/cost_attribution_config.json` — a path that doesn't exist. The code silently fell back to default config values.
*   **Affected Module:** `src/cost_attribution.py` → `load_config()` / `save_config()`
*   **Fix Applied (`src/cost_attribution.py`):**
    *   Changed `CONFIG_FILE = "cost_attribution_config.json"` → `CONFIG_FILE = Path(__file__).parent / "cost_attribution_config.json"`
    *   Added `from pathlib import Path` import
*   **Impact:** Config file is now correctly resolved relative to the module's location, working in any working directory including Docker containers.

### ✨ Improvements

#### refactor(finops): centralize `DAYS_PER_MONTH` constant across all modules
*   **Issue:** `hbo.py` used `30.41` and `fluid_scaling.py` used `30.44` for monthly projections. The standard calendar average is `365.25 / 12 = 30.4375`. Inconsistent constants produce subtly different dollar figures across dashboard panels.
*   **Fix Applied:**
    *   Added `DAYS_PER_MONTH = 365.25 / 12` to `src/utils.py`
    *   `fluid_scaling.py` and `hbo.py` now import from `utils.py` instead of defining locally

#### refactor(ai-doctor): parameterize job IDs instead of string interpolation
*   **Issue:** AI Doctor query fetching used `", ".join(f"'{jid}'" ...)` to build a SQL `IN (...)` clause. While job IDs come from prior BQ results (not user input), relying on upstream format constraints for SQL safety is an anti-pattern.
*   **Fix Applied (`src/main.py`):**
    *   Replaced f-string interpolation with `UNNEST(@job_ids)` and `bigquery.ArrayQueryParameter`
    *   Eliminated all raw string quoting of job IDs

#### refactor(fluid-scaling): merge redundant groupby in `_rollup_to_summaries`
*   **Issue:** Two separate `merged.groupby("reservation_id")` calls aggregated the same DataFrame — one for legacy/fluid/usage sums, another for `has_capacity`/`sum_used` status criteria. The second pass was redundant.
*   **Fix Applied (`src/fluid_scaling.py`):**
    *   Combined into a single `.agg()` call with all 5 aggregate columns
    *   Eliminated the `res_sums` intermediate DataFrame and `.at[]` index lookups

---

## v1.1.1 — 2026-07-09

Patch release focused on **data integrity** and **cost safety compliance**. Removes all hardcoded demo/fallback data from production endpoints and ensures the user-configured `max_bytes_billed_gb` safety cap is respected across all Fluid Scaling queries.

### 🐛 Bug Fixes

#### fix(fluid-scaling): `max_bytes_billed_gb` not forwarded to backend API calls
*   **Root Cause:** The frontend's Fluid Scaling module called two backend endpoints — `POST /api/fluid-scaling/estimate` and `POST /api/slots/fluid_simulation` — without including `max_bytes_billed_gb` in the POST body, even though the value was correctly stored in `state.maxBytesBilledGb` (read from localStorage and the Settings form). This caused the backend to fall back to its default 200 GiB safety cap, ignoring the user's configured limit (e.g., 4,500 GB).
*   **Affected Files (4 files changed, 6 edits):**
    *   `static/app.js` — `fetchEstimate()`: added `maxBytesBilledGb` to call site, function signature (`{ orgProject, adminProject, region, lookback, price }` → `{ orgProject, adminProject, region, lookback, price, maxBytesBilledGb }`), and JSON body (`max_bytes_billed_gb: maxBytesBilledGb`).
    *   `static/app.js` — `fetchJobSimulation()`: identical 3-point fix (call site, signature, body).
    *   `docs/static/app.js` — both functions: same fixes applied to keep the docs copy in sync.
*   **Impact:** Users who configured a custom `max_bytes_billed_gb` (e.g., 4,500 GB for large orgs) now have that limit correctly enforced on Fluid Scaling queries, preventing unexpected `Query exceeded limit` errors or silent fallback to the 200 GiB default.

#### fix(data-integrity): removed all hardcoded fallback data from 6 backend endpoints
*   **Root Cause:** Multiple backend endpoints returned fabricated demo data to production users instead of honest empty results. This was originally implemented to ensure the UI "always looks highly informative and doesn't appear blank" but leaked fake project names, dollar amounts, and table metadata to real users.
*   **Affected Endpoints & Specific Values Removed:**

    | Endpoint | Trigger | Fake Data Removed |
    |:---------|:--------|:------------------|
    | `POST /api/storage/static_audit` | Query returns empty (`if not output:`) | `EDW_WCM_CONTACT.wcm_contact_matching_history` (184M rows, 342 GB, unpartitioned), `ODS_CORE_ECOM.ecom_order_item_ledger` (459M rows, 894 GB, partitioned on `ORDER_DATE`) |
    | `POST /api/storage/static_audit` | Query throws exception | Same two fake tables (identical fallback block) |
    | `POST /api/storage/active_assist` | Query returns empty | Same two fake tables with hardcoded savings ($450/mo, $280/mo) |
    | `POST /api/storage/active_assist` | Query throws exception | Same two fake tables |
    | `POST /api/storage/active_assist` | Every real recommendation | Hardcoded `savings = 120.0` default, fabricated column suggestions (`CREATED_DATE`, `CUSTOMER_KEY`, `EVENT_TYPE_ID`), arbitrary `editions_monthly_savings = savings * 0.8` formula |
    | `GET /api/dashboard/kpis` | Always | `mtdSpend=42310.00`, `forecastSpend=58200.00`, `lastMonthSpend=51400.00`, `potentialSavings=12400.00`, `opportunityCount=47`, `anomalyCount=3` |
    | `GET /api/dashboard/opportunities` | Always | 5 fake entries: `warehouse_db` ($4,200/mo), `project-analytics-prod` ($3,100/mo), `events_db` ($2,800/mo), `analytics-pool` ($1,400/mo), `user@example.com` ($900/mo) |
    | `GET /api/dashboard/top-projects` | Always | 5 fake projects: `data-warehouse-prod` ($18,400), `ml-training-prod` ($12,900), `analytics-prod` ($6,300), `reporting-prod` ($3,100), `dev-sandbox` ($1,600) |
    | `GET /api/dashboard/anomalies` | Always | 3 fake alerts: `data-warehouse-prod +340%`, `analytics-pool idle 80%`, `etl@svc.gserviceaccount.com SELECT *` |

*   **Fixes Applied (`src/main.py`, net −108 lines):**
    *   **Static Schema Audit:** Removed `if not output:` fake table block and `except` fallback. Both paths now return `[]`.
    *   **Active Assist:** Removed `if not output:` fake table block, `except` fallback, `savings = 120.0` default, hardcoded column suggestions, and `* 0.8` formula. `ActiveAssistResult.on_demand_monthly_savings` and `editions_monthly_savings` changed from `float` → `Optional[float] = None`. Column suggestions now parsed from `additional_details` metadata when available.
    *   **Dashboard KPIs:** `KpiResponse` fields changed from required `float`/`int` → `Optional[float/int] = None`. Endpoint returns `KpiResponse(stub=True)` with all metrics as `null` until real billing integration is implemented.
    *   **Dashboard Opportunities/Top-Projects/Anomalies:** All three endpoints now return `[]`.
*   **Impact:** No production user will ever see fabricated project names, table names, savings estimates, or anomaly alerts. Frontend already handles empty states gracefully with "No results found" messaging.

### ✨ Improvements

#### refactor(active-assist): removed `focus_projects` parameter from API request
*   **Rationale:** Active Assist queries `INFORMATION_SCHEMA.RECOMMENDATIONS`, which is inherently scoped to the execution project by BigQuery — there is no cross-project recommendations view. The `focus_projects` parameter was being sent from the frontend but had no effect on the backend query. Removed from the Active Assist `params` object in `static/app.js` to avoid confusion.
*   **Scoping Decision:** Active Assist intentionally operates at the **org/execution project level** (not focus-project level). Unlike other endpoints that filter by `WHERE project_id IN UNNEST(@focus_projects)`, the `RECOMMENDATIONS` view cannot be filtered this way — it returns recommendations for all tables within the queried project.

#### refactor(capacity): removed `focus_projects` from 7 capacity/slots endpoints
*   **Rationale:** Capacity planning endpoints query `INFORMATION_SCHEMA.JOBS_TIMELINE_BY_ORGANIZATION` to measure org-wide slot demand. Applying a `focus_projects` filter to these queries gives **misleadingly small capacity numbers** — e.g., sizing a reservation based on 2 projects when 50 projects share it leads to under-provisioning, autoscaler cost overruns, and degraded query performance.
*   **Affected Endpoints (7):**

    | Endpoint | Module | Why Focus Distorts Results |
    |:---------|:-------|:--------------------------|
    | `POST /api/slots/analyze` | Slot Timeline | Slot timeline must reflect total org capacity, not a subset |
    | `POST /api/slots/tiered_recommendations` | Tiered Recs | p80/p95/max baselines must reflect total org demand to size reservations correctly |
    | `POST /api/slots/utilization` | Slot Utilization | Utilization % is meaningless unless measured against total org capacity |
    | `POST /api/slots/simulate` | Slot Simulation | Simulation accuracy depends on seeing the full workload |
    | `POST /api/slots/peak` | Peak Slots | Peak slot demand across a subset ≠ actual peak (jobs overlap cross-project) |
    | `POST /api/slots/fluid_simulation` | Fluid Simulation | Same distortion risk as slot simulation |
    | `POST /api/fluid-scaling/estimate` | Fluid Scaling Estimate | Per-second billing model comparison requires full org workload |

*   **Changes:** Backend `build_project_filter()` calls replaced with empty clause `("", [])`. Frontend `focus_projects` removed from all capacity/slots `fetch()` payloads. The `FocusMixin` field remains on param models for backward API compatibility (the field is accepted but ignored). Tiered Recommendations' focus-guard fallback logic simplified since scoping no longer applies.
*   **Scope Retained:** `focus_projects` remains fully active on **19 endpoints** that query `JOBS_BY_ORGANIZATION` and `TABLE_STORAGE_BY_ORGANIZATION` — job analysis, anti-pattern detection, AI Doctor, storage optimization, governance, HBO, cost attribution, and profiling — where project-level filtering is the exact intended use case.

---

## v1.1.0 — 2026-07-08

We are proud to announce the release of **v1.1.0** of the **BigQuery FinOps Optimizer**—an enterprise-grade diagnostic, simulation, and governance suite designed to maximize cost efficiency and eliminate compute waste across Google Cloud BigQuery environments. 

This release marks a major milestone, introducing dynamic budget safety guardrails, advanced telemetry models, proactive migration guardrails, and a complete codebase sanitization to support secure, open-source distribution.

### 🔑 Key Highlights

#### 1. 🛡️ Dynamic Billing Limits & Cost Safety Caps
*   **Dynamic Cost Guardrails:** Introduced a project-wide `max_bytes_billed_gb` safety cap parameter, dynamically passed to every single BigQuery query execution in the system.
*   **Budget Protection:** The parameter translates directly into BigQuery's native `QueryJobConfig.maximum_bytes_billed` (converting GiB to bytes), preventing any analytical or diagnostic runs from causing runaway query charges.
*   **Smart Clamping & Fallbacks:** Implemented robust boundary validation (minimum of 1 GiB, maximum of 10 TiB, and a safe default fallback of 200 GiB if the configuration is unset or zero).
*   **State Persistence:** Settings are persisted directly in the client's local storage and dynamically validated using Pydantic models.

#### 2. 📈 The "Slot Capacity Bucket Method" (Compute Capacity Simulation)
*   **Vectorized Sizing Engine:** Built a high-performance simulation engine using NumPy to model slot consumption telemetry over a standard 730-hour billing month.
*   **Tiered Recommendation Matrix:** Evaluates per-minute reservation timelines to generate three distinct baseline tier recommendations based on historical percentiles:
    *   🔴 **Aggressive Savings (p80):** Minimizes baseline commitments, leveraging autoscaling for bursts.
    *   🟡 **Balanced (p95):** Optimizes for steady-state workloads with moderate risk tolerance.
    *   🟢 **Performance (Max):** Eliminates autoscaler latency by matching peak slot usage.
*   **FinOps Realism:** Integrates custom Enterprise Discount Agreements (EDAs), Committed-Use Discounts (CUDs), and autoscaling rounding overhead factors (to account for BigQuery's physical 50-slot stepping).

#### 3. ⏱️ Fluid Scaling & Cooldown Tax Mitigation
*   **Cooldown Tax Analysis:** Identifies reservations with high-frequency, short-duration queries that suffer from the legacy autoscaler's 60-second minimum charge.
*   **Per-Second Simulation:** Models the financial impact of transitioning to **Fluid Scaling** (true per-second billing with zero minimum capacity), allowing users to isolate workloads without financial penalty.

#### 4. 🔗 Hybrid Cost Attribution Engine
*   **SKU Blending Resolution:** Solves the challenge of attributing blended BigQuery Editions costs down to individual querying projects.
*   **Allocated Waste Distribution:** Proportionally redistributes unallocated idle capacity waste (unused baseline reservation slots) back to the active project consumers, eliminating the \"admin project dump\" mystery.
*   **Custom Attribution Rules:** Supports customizable billing rules, including *Lender Pays* vs. *Borrower Pays* models for cross-reservation idle slot borrowing.

#### 5. 🤖 AI-Powered Semantic Query Review (AI Doctor)
*   **Native LLM Integration:** Leverages BigQuery's native `AI.GENERATE` scalar function to run high-speed, cost-efficient SQL reviews directly inside the data warehouse.
*   **Structured Auditing:** Uses a custom prompt engine with **Gemini 3.1 Flash Lite** to scan your most expensive queries for 7 critical anti-patterns (e.g., `SELECT *` abuse, unclustered limits, and function-wrapped partition keys).
*   **Actionable Routing Snippets:** Generates copy-pasteable `SET @@reservation` DDL statements to seamlessly route optimized query patterns to designated compute pools.

#### 6. 🏗️ Proactive Migration Guardrails (Static Schema Auditor)
*   **Migration Timebomb Detection:** Introduced a static schema scanner that audits the metadata catalog to identify high-risk, unclustered, or unpartitioned tables *before* they are queried:
    *   🔴 **Critical Risk:** Tables exceeding 1 TB in size or 1 Billion rows.
    *   🟡 **High Risk:** Tables exceeding 10 GB in size or 50 Million rows.
*   **Automated Remediation:** Suggests optimal clustering columns (detecting `_id`, `_type`, and `_date` fields) and generates ready-to-run replacement DDL stubs.

#### 7. 🔒 Sanitization & Repository Hygiene
*   **Corporate Data Scrubbing:** Conducted a rigorous sweep of all source files, documentation, and mock data to remove all traces of proprietary company identifiers (*\"Wiley\"*, *\"Wylie\"*).
*   **Zero Credentials Leak:** Verified that no GCP service account keys, passwords, private tokens, or local machine absolute paths are hardcoded.
*   **Sanitized Gitignore:** Restructured `.gitignore` to explicitly cover virtual environments (`myenv/`, `venv/`), test cache directories (`.pytest_cache/`), and internal diagnostic snapshots (`heavy_job_ids_ref.md`).

#### 8. 🎯 Project-Scoped Analysis (Focus Projects)
*   **Cross-Cutting Project Filter:** Introduced a new optional `focus_projects` parameter that allows users to scope *any* org-wide analysis down to a specific subset of GCP projects (up to 50). When omitted, endpoints behave as before (full organization scan).
*   **Architecture:**
    *   **`FocusMixin`** — a Pydantic mixin class inherited by 24 param models across all 4 source files (`main.py`, `hbo.py`, `cost_attribution.py`, `fluid_scaling.py`), adding the optional `focus_projects: List[str]` field.
    *   **`validate_focus_projects()`** — centralized validation: deduplication, whitespace trimming, dummy project rejection, `_safe_ident` safety check, and a 50-project cap.
    *   **`build_project_filter()`** — generates a parameterized SQL clause (`AND project_id IN UNNEST(@focus_projects)`) with `ArrayQueryParameter` binding. Column names are restricted to an allow-list (`project_id`, `project_name`). Table aliases are validated against `_ALIAS_RE`. Values are **never interpolated** into SQL strings.
*   **Endpoint Coverage:** Applied to all org-level endpoints: Storage Optimizer, Compute Analyzer, Capacity Planner, Tiered Recommendations, Slot Utilization, Slot Simulation, Fluid Scaling, Peak Slots, Workload Profiler, DML & MV Auditor, Query Linter, Data Skew Analyzer, Batch Candidates, BI Engine, Governance, AI Doctor, HBO (analyze, summary, performance insights), Cost Attribution, Top Spenders.
*   **Fallback Guard:** Endpoints with project-level fallback paths (e.g., Tiered Recommendations) are protected by a guard: if `focus_projects` is active and the org-level query fails, the endpoint raises an error rather than silently falling back to unscoped project-level data.
*   **Test Coverage:** Two dedicated test files:
    *   `test_focus_filter.py` — 10 unit tests for `build_project_filter()` (parameterization, column allow-list, alias validation, empty/None handling).
    *   `test_focus_guard.py` — 41+ parametrized integration tests verifying: (a) `@focus_projects` is never silently dropped from query parameters, (b) all 24 endpoint schemas accept `focus_projects` without 422, (c) the tiered recommendations fallback guard fires correctly, and (d) SQL injection / dummy project / cap-exceeded payloads are rejected at the endpoint level.

---

### 🐛 Bug Fixes

#### fix(ai-doctor): discovery query scoped to project instead of organization
*   **Root Cause:** The AI Doctor query discovery stage (`/api/ai/analyze`) was querying `INFORMATION_SCHEMA.JOBS_BY_PROJECT` instead of `INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION`. This meant the endpoint only reviewed SQL queries executed within the admin project itself, instead of scanning the entire organization — defeating the purpose of the tool.
*   **Fix:** Changed the discovery SQL to use `JOBS_BY_ORGANIZATION`, added the missing `focus_projects` filter support (parameterized `IN UNNEST(@focus_projects)`), and added SCRIPT statement exclusion for consistency with all other org-level endpoints.
*   **Impact:** AI Doctor now correctly discovers the most expensive queries across *all* projects in the organization, matching the behavior of every other org-scoped endpoint (Compute Analyzer, DML Auditor, Skew Analyzer, etc.).
*   **Test:** Added regression assertions in `test_ai_doctor.py` to verify the discovery query contains `JOBS_BY_ORGANIZATION` and does *not* contain `JOBS_BY_PROJECT`.

#### fix(fluid-scaling): queries missing `maximum_bytes_billed` safety cap
*   **Root Cause:** Three queries in `fluid_scaling.py` were calling `client.query(sql).result()` without a `QueryJobConfig`, bypassing the `maximum_bytes_billed` safety cap enforced by every other query in the project. Affected paths: the `RESERVATIONS` lookup in the `/status` endpoint, and the `EFFECTIVE_PROJECT_OPTIONS` / `PROJECT_OPTIONS` lookups in `get_effective_fluid_scaling_reservations()` (called by both `/status` and `/estimate`).
*   **Fix:** Added `bigquery.QueryJobConfig(maximum_bytes_billed=get_max_bytes_billed(params))` to all three queries. Threaded the `params` argument through `get_effective_fluid_scaling_reservations()` so the user's configured cap is respected.
*   **Impact:** All fluid scaling queries now honour the global cost guardrail (default 200 GiB, configurable up to 10 TiB), preventing runaway scan costs on large organizations.

#### fix(frontend): stale cached results persisting after settings change
*   **Root Cause:** When a user changed global settings (e.g., adding `focus_projects` to scope from org-wide to a specific project), all previously cached module results (`bq_*_results` in `localStorage`) were preserved. On page reload, the UI rendered stale org-wide data — showing results from projects outside the focus filter.
*   **Fix:** When settings are saved, all scope-dependent cached keys (`bq_*_results`, `bq_hbo_status`, `bq_profiler_*`, `bq_top_spenders`, `bq_cost_attr_*`) are flushed from `localStorage`. The success notification now reads: *"Settings saved. All cached results cleared — re-run analyses for the new scope."*
*   **Impact:** Users no longer see stale data from a previous scope after changing settings.

---

### ✨ Improvements

#### feat(frontend): GCP project ID validation & input sanitization
*   **Whitespace Stripping:** All text inputs in the Global Settings panel (Organization Project, Admin Project, Focus Projects) are now `.trim()`'d on save to handle bad copy-paste with trailing spaces or newlines.
*   **Project ID Regex Validation:** All project IDs are validated against the GCP project ID specification: `^[a-z][a-z0-9\-]{5,29}$` (starts with a lowercase letter, 6–30 characters, lowercase letters + digits + hyphens only).
*   **Per-Entry Focus Projects Validation:** Each comma-separated focus project ID is individually validated. Invalid entries are reported in a single aggregated error notification.
*   **Abort on Error:** If any validation fails, settings are **not** persisted to `localStorage` and the user sees specific error messages identifying which fields are invalid.

#### feat(observability): structured 4-icon logging system across all modules
*   **4-Icon Log Sequence:** Every API request now produces a consistent, human-scannable log flow:
    | Icon | Meaning | Example |
    |:----:|:--------|:--------|
    | `▶` | **Endpoint started** — project, region, scope, lookback, safety cap | `▶ Job Analysis — project=my-org \| scope=1 projects (my-proj) \| safety_cap=200 GiB` |
    | `⏳` | **Query submitted** — which query, active safety cap | `⏳ Storage Metrics — submitting query (safety cap: 200 GiB)…` |
    | `✅` | **Query completed** — elapsed time, bytes processed/billed, cache hit, BQ Console URL | `✅ Storage Metrics — 2.1s \| Processed: 0.42 GiB \| Cache: False \| https://…` |
    | `◼` | **Endpoint completed** — total elapsed time | `◼ Job Analysis — completed in 4.3s` |
*   **Request Correlation IDs:** Every log line includes an 8-character hex request ID (e.g., `[a3f1b2c4]`) automatically injected via FastAPI middleware and a `contextvars`-backed `logging.Filter`. This enables tracing an entire request's lifecycle (`▶` → `⏳` → `✅` → `◼`) even when logs from concurrent requests are interleaved. Startup logs show `[--------]`.
*   **Centralized Query Helpers:** All BigQuery query executions are routed through instrumented helpers (`run_query_and_log`, `run_query_to_df`, `_run_and_log`) that enforce the `maximum_bytes_billed` safety cap and emit the `⏳`/`✅` log sequence. Zero bare `client.query()` calls exist outside these helpers (with one accepted exception for HBO's `ThreadPoolExecutor` loop, which logs at `DEBUG` to avoid flooding).
*   **Full Module Coverage:** Applied consistently across **all 4 source modules**:
    *   `main.py` — 25+ endpoints via `run_query_and_log()` and `run_query_to_df()` helpers
    *   `hbo.py` — 4 endpoints via `_run_and_log()` helper
    *   `cost_attribution.py` — 1 endpoint via `_run_and_log()` helper
    *   `fluid_scaling.py` — 3 endpoints via `_run_and_log()` and `_run_query_to_df()` helpers
*   **Clickable Job URLs:** Every query logs a BigQuery Console URL (`https://console.cloud.google.com/bigquery?project=...&j=bq:LOCATION:JOB_ID&page=queryresults`) — click to inspect job results directly in the GCP Console.
*   **SQL Tracing at DEBUG Only:** Full SQL text is logged at `DEBUG` level only, never at `INFO`. Removed 18+ redundant `logger.info(f"Executing ... Query:\n{sql}")` calls that were dumping multi-KB SQL into production logs.
*   **Configurable Log Level via `LOG_LEVEL` env var:** The log level is now controlled by the `LOG_LEVEL` environment variable (default: `INFO`). Set `LOG_LEVEL=DEBUG` to see full SQL for every query:
    ```bash
    LOG_LEVEL=DEBUG uvicorn src.main:app --reload
    ```
*   **Logger Name Consistency:** `log_endpoint_start()` / `log_endpoint_end()` now accept a `_logger` parameter so log lines show the calling module's name (`src.main`, `src.hbo`, etc.) instead of `src.utils`.

#### feat(ui): About panel with live versioning
*   **`/api/about` Endpoint:** New API endpoint serving the version, release date, module count, highlights, and tech stack as structured JSON.
*   **Auto-Parsed Highlights:** The endpoint parses `RELEASE_NOTES.md` at startup to extract the latest release's "Key Highlights" section titles — making this file the **single source of truth**. No highlights are hardcoded in Python or JavaScript.
*   **Sidebar Version Badge:** The sidebar footer now displays the live app version (e.g., `v1.1.0`) fetched from the backend. Clicking it navigates to the About view.
*   **About View:** A dedicated glassmorphic panel showing app metadata, "What's New" highlights, tech stack pills, and links to the Release Notes, Interactive Demo, and GitHub repository.
*   **Version Bump Workflow:** To release a new version: (1) update `__version__` in `main.py`, (2) add a new `## vX.Y.Z — date` section with `### Key Highlights` to `RELEASE_NOTES.md`. The frontend auto-populates.

#### feat(ux): Active Assist "Recommender Blind Spot" education & empty-state handling
*   **The Problem:** Google Active Assist intentionally suppresses clustering recommendations for tables with heavy daily DML operations (inserts/updates/deletes). Its ML model optimizes for compute (slot-ms), so if maintaining clustered blocks during writes outweighs the read savings, the recommendation is dropped. For On-Demand customers, where bytes scanned is the primary cost driver, this is a dangerous blind spot — users see an empty result and assume they're fully optimized.
*   **Methodology Card:** Added a two-column glassmorphic card above the Active Assist table explaining how Active Assist (compute-optimized) and the Static Schema Auditor (bytes-scanned-optimized) calculate ROI differently, with an explicit "Blind Spot" callout.
*   **Intelligent Empty State:** When Active Assist returns 0 recommendations, the empty table now displays a contextual warning listing the possible reasons (already optimized, insufficient history, DML suppression) and directs users to the Static Schema Auditor for structural governance.

#### refactor(nav): Schema Optimizer promoted to its own navigation group
*   **The Problem:** Active Assist and Static Schema Auditor were buried inside the "Storage Cost" tab, even though partitioning/clustering impacts compute costs (slot-ms), On-Demand costs (bytes scanned), and query performance — not just storage.
*   **New Sidebar Group:** Added a "Schema" navigation group with a dedicated "Schema Optimizer" tab. Each tab now answers one question: Storage Cost = "Should I switch billing models?", Schema Optimizer = "Are my tables structured correctly?", Storage Hygiene = "Am I wasting money on retention?"
*   **Zero JS Changes:** All button IDs and event handlers remain unchanged — the DOM elements simply moved to a new `<section>`.

#### refactor(nav): "Compute" split into "Workload Optimization" and "Cost Allocation"
*   **The Problem:** The "Compute" sidebar group had 7 items — more than double any other group, creating cognitive overload.
*   **New Split:** "Workload Optimization" (Query Cost Optimizer, Slots Optimizer, Edition Matrix Simulation, Fluid Scaling) and "Cost Allocation" (Cost Attribution, Workload Profiler, Top Spenders).
*   **Title Case Standardization:** All navigation labels now use consistent Title Case (e.g., "Query Cost Optimizer" instead of "Query cost optimizer").

#### fix(ux): Collapsible "How it works" panels
*   Educational info panels in the Query Cost view are now wrapped in collapsible `<details>` elements. Power users can collapse them to immediately access parameters and the "Run Analysis" button without scrolling.

#### fix(ux): Tooltip hover delay
*   Added a `0.2s` transition-delay on tooltip hover to prevent distracting tooltip flashes during fast mouse sweeps across the interface.

#### fix(ai): Resolved `AI.GENERATE` silent NULL returns for complex queries
*   **Root Cause:** `max_output_tokens` was set to `300`. For queries with complex schemas (3000+ prompt tokens), the Gemini 3.1 Flash Lite model consumed ~296 tokens on internal thinking/reasoning, leaving zero tokens for output text. BigQuery returned `result: NULL` with `finish_reason: MAX_TOKENS`.
*   **Fix — Token Budget:** Bumped `max_output_tokens` from `300` to `1024`, providing 3.4× more headroom for the model to emit structured advice after reasoning.
*   **Fix — Thinking Level:** Replaced legacy `thinking_budget: 0` (Gemini 2.5 parameter) with `thinking_level: "MINIMAL"` (correct Gemini 3.x parameter), constraining the model to use minimal tokens for internal reasoning.
*   **Fix — Safety Settings:** Added all four safety categories set to `OFF` (`HATE_SPEECH`, `DANGEROUS_CONTENT`, `SEXUALLY_EXPLICIT`, `HARASSMENT`). SQL text can contain patterns that trigger false safety blocks — disabling them is safe since input is exclusively SQL, never user-facing content.

#### perf(ai): DDL payload summarization & newline-aware truncation
*   **DDL Summarization:** Replaced full column-level DDL dumps (15,000+ characters for wide tables) with structural metadata summaries (~150 chars per table): row count, byte size, partition/clustering keys, and column count. The LLM doesn't need column names to detect anti-patterns like `SELECT *` or unclustered `LIMIT`.
*   **Newline-Aware Truncation:** SQL and DDL payloads are now truncated at the last newline boundary (not mid-token), preventing the LLM from receiving malformed SQL fragments.

#### feat(ai): Enhanced `AI.GENERATE` observability with `full_response` extraction
*   **3-Tier Error Logging:** The backend now extracts the full Vertex AI response struct (`result`, `status`, `full_response`) from `AI.GENERATE`, enabling precise diagnosis:
    *   `ai_struct is None` → function call failed (quota/timeout/malformed request)
    *   `status` populated → Vertex API error
    *   `full_response.candidates[0].finish_reason` → reveals `MAX_TOKENS` vs `SAFETY` vs `STOP`
*   **Debug-Level Struct Logging:** Raw `ai_struct` payloads are logged at `DEBUG` level for all rows, enabling full post-mortem analysis without code changes.

---

### 🛠️ Tech Stack & Architecture

*   **API Framework:** FastAPI (centralized versioning, GZip compression middleware, automated interactive `/docs` landing page).
*   **Data Science Core:** NumPy, Pandas, DB-Types.
*   **Frontend UI:** Vanilla ES6 JavaScript, HTML5, Custom Glassmorphic CSS Engine.
*   **Testing Suite:** 272 comprehensive unit and integration tests (mocked BigQuery endpoints, Pydantic input boundary validation, and SQL injection security checks).

---

## v1.0.0 — 2026-06-15

Initial release of the BigQuery FinOps Optimizer with core modules:

*   Storage Cost Optimizer (logical vs. physical billing)
*   On-Demand vs. Editions Job Analyzer
*   Slot Usage Timeline & Capacity Planner
*   Query Anti-Pattern Linter (SELECT *, unclustered LIMIT)
*   Storage Hygiene & Time Travel Auditor
*   DML Abuse Tracker
*   BI Engine ROI Optimizer
*   Governance & Expiration Auditor
*   Data Skew Analyzer
*   Interactive vs. Batch Optimizer
*   Materialized View FinOps Auditor

---

*For installation, local setup, IAM permissions, and GCP configuration, please refer to the main [README.md](README.md).*
