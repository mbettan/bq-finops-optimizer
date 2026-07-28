# Release Notes — BigQuery FinOps Optimizer

This page documents production updates to the BigQuery FinOps Optimizer. We recommend that users periodically check this list for any new announcements.

For architecture details and tech stack information, see the [README](README.md).

---


## July 28, 2026 — v1.3.0

**Feature (AI Doctor Multi-Strategy ROI Engine)**
Introduced an aggregated multi-metric query selection and FinOps ROI prioritization engine for AI Doctor. Rather than selecting single un-grouped expensive jobs, AI Doctor groups query executions across `JOBS_BY_ORGANIZATION` over 7–90 day lookback windows. Added an interactive **Discovery Priority** dropdown to the UI with 5 distinct strategy modes:
* ⚖️ **Balanced ROI Score (`composite`)**: Ranks workload templates using a multi-factor score: $0.40 \cdot \log_{10}(GB + 1) + 0.30 \cdot \log_{10}(Execs + 1) + 0.20 \cdot \log_{10}(Slots + 1) + 0.10 \cdot SpillFlag$.
* 💰 **Cumulative Cost (`cumulative_cost`)**: Ranks workloads by On-Demand cost or On-Demand Equivalent cost for BigQuery Editions (using scanned bytes when billed bytes is 0).
* 🔄 **High Frequency (`execution_frequency`)**: Targets dashboard micro-offenders executing repeatedly (`execution_count > 1`), rendering interactive execution run badges in the UI.
* 💾 **Memory RAM Spill (`memory_spill`)**: Surfaces memory-intensive queries that spill intermediate shuffle bytes to disk (`HAVING bytes_spilled > 0`).
* ⏱️ **Total Slot Time (`slot_ms`)**: Focuses on heavy slot-consuming query templates.

**Feature (BigQuery Editions Hybrid Cost Fallback)**
* **On-Demand Equivalent Cost (`effective_bytes`)**: Computed `effective_bytes = GREATEST(COALESCE(total_bytes_billed, 0), COALESCE(total_bytes_processed, 0))`. For queries running under BigQuery Editions (slot capacity reservations) where `total_bytes_billed = 0`, the discovery engine uses scanned data volume to rank workloads and calculate On-Demand Equivalent Costs instead of dropping them to $0.00.

**Feature (Multi-Stage Unnesting & Atomic Sampling)**
* **Stage Spill Aggregation**: Unnests `job_stages` via `(SELECT SUM(s.shuffle_output_bytes_spilled) FROM UNNEST(job_stages) s)` to capture intermediate RAM-to-disk spills across all execution stages.
* **Deterministic Worst-Job Sampling**: Uses `ARRAY_AGG(job_meta ORDER BY total_slot_ms DESC LIMIT 1)[OFFSET(0)]` to atomically isolate the single worst execution instance and its exact metadata payload.
* **Script Filtering**: Excludes procedural script headers (`statement_type != 'SCRIPT'`) to prevent double-counting slot time and processed bytes.

**Feature (Fluid Scaling Edition-Aware Pricing)**
* Per-edition slot pricing: Standard ($0.04/slot-hr), Enterprise ($0.06), and Enterprise Plus ($0.10) are now applied per-reservation instead of a flat $0.06 rate. The `edition` column from `RESERVATION_TIMELINE_BY_PROJECT` is captured and mapped to a rate table, with the user-supplied `price_per_slot_hr` as fallback for unknown editions.

**Feature (HBO Per-Job Optimization Type Badges)**
* **Per-Job Enrichment**: New `POST /api/hbo/optimizations` endpoint enriches org-wide HBO results with the exact optimization types BigQuery applied to each job (Semi-Join Reduction, Join Commutation, Join Pushdown, Parallelism Adjustment, Enhanced Vectorization, Short Query Optimization). Sources data from per-project `JOBS_BY_PROJECT` views since `optimization_details` is not available in `JOBS_BY_ORGANIZATION`.
* **Fault-Isolated Fan-Out**: Uses `ThreadPoolExecutor` with one BigQuery query per project — never `UNION ALL` across projects. A single 403 Forbidden on one project never blanks the results for other readable projects (§9.7).
* **Optimization Catalog**: 6 known optimization types mapped to `hbo` (history-based) or `engine` (runtime) categories with tooltip descriptions. Unknown future types render with an "unknown" badge — they never disappear.
* **listAll Redaction Awareness**: Distinguishes `None` (undetermined — row absent, permissions issue) from `[]` (checked, no optimizations applied). This prevents IAM `listAll` redaction from being misreported as "no optimizations."
* **Progressive Enhancement**: The enrichment call is non-blocking — the HBO table renders immediately with "Loading…" badges, which upgrade to real badges when the per-project fan-out resolves.
* **XSS-Safe Rendering**: Badge cells built with `document.createElement` + `textContent` (never `innerHTML`), preventing injection through server-returned optimization keys.

**Feature (AI Doctor Executive KPI Dashboard)**
* **KPI Summary Strip**: Three glassmorphism metric cards render above the results table immediately after analysis: **Audited Spend** (total cost + TiB scanned), **Findings by Severity** (HIGH/MED/LOW pill counts), and **Schema Coverage** (DDL retrieval percentage). Provides a FinOps lead with the 5-second answer before scanning any rows.

**Feature (AI Doctor Smart Filter Pills)**
* **Interactive Filter Toolbar**: Six clickable pill filters above the table — **All**, **High**, **Medium**, **Has Migration Config**, **Schema Gap**, **Repeat Offenders** — each with live counts. Filters integrate with DataTables custom search (`$.fn.dataTable.ext.search`) and reset cleanly on re-render via clone-to-reset pattern to prevent event listener stacking.

**Feature (AI Doctor UX Polish)**
* **Sort by Cost**: Default table sort changed from severity/slot-ms to **Original Cost descending** — the highest-spend queries surface first. Cost column now carries a numeric `data-order` attribute for correct DataTable sorting.
* **Severity Left-Stripe**: Replaced subtle background tinting with a **4px inset left border stripe** (red/amber/green `box-shadow`) for stronger visual scanning and improved accessibility for color-blind users.
* **Collapsible Migration API Config**: YAML badge refactored into an **accordion** (collapsed by default) with chevron rotation animation. Uses event delegation instead of inline `onclick` handlers (CSP-safe).
* **Advice Truncation**: AI Optimization Advice column capped at 150px with a gradient fade and **"▼ Show more" / "▲ Show less" toggle** for content exceeding 300 characters. Negative margin cleared on expand for clean layout.
* **Copy Original SQL**: Added a **"Copy SQL" button** to the original Query SQL column (rose-themed), matching the existing blue Copy SQL on the Optimized Query column.
* **Schema Note Consolidation**: Moved the visible inline footnote ("This recommendation used schema context…") into the **Schema Coverage badge tooltip** (hover), decluttering the Advice column.
* **Sticky Header Fix**: Added `z-index: 10` to the existing `position: sticky` thead rule so table content scrolls underneath headers correctly.
* **Query SQL Restyle**: Original SQL column redesigned from truncated text to a **scrollable `<pre>` code block** with rose/red syntax highlighting (`#fb7185`), consistent with the blue Optimized Query column.
* **`test_hbo_optimizations.py` (27 tests)**: Covers JSON parsing (4+ shapes), badge building, per-project fan-out isolation, UNION ALL prevention, partition pruning, listAll redaction semantics, and empty exception guard (P12).

**Security & Error Handling**
* **Strict IAM 403 Enforcement**: Requires `roles/bigquery.resourceViewer` or `roles/bigquery.admin` for organization discovery. Intercepts IAM 403 `Forbidden` exceptions and returns a clear `HTTP 403 Access Denied` response with role guidance, eliminating silent or degraded project-level fallbacks.
* **Pydantic Literal Validation**: Validates `discovery_strategy` parameters strictly via Pydantic `Literal` types (returning HTTP 422 for unrecognised options).
* **Retryable 403 Classification**: BigQuery quota/rate-limit 403s (`rateLimitExceeded`, `quotaExceeded`) are now distinguished from IAM denials by inspecting the structured error `reason` field. These map to HTTP 429 with `Retry-After` instead of the permanent "Access Denied" message, and are retried by `run_query_with_retry_limit`.
* **Precise Bytes-Billed Detection**: Replaced broad `"exceeds the maximum"` substring match with the structured `bytesBilledLimitExceeded` reason code, preventing unrelated partition-count errors from recommending users raise their safety cap.
* **Proper 5xx Status Codes**: BigQuery server errors (`InternalServerError`, `ServiceUnavailable`, `TooManyRequests`, `DeadlineExceeded`) now return HTTP 429/502 instead of being lumped into the generic HTTP 400 path.
* **Non-Root Container**: Dockerfile now runs as `USER 10001` (`appuser`) with `COPY --chown`, `PYTHONUNBUFFERED=1`, and dynamic port binding via `${PORT:-8080}`.
* **Reproducible Builds (P2)**: Base image digest-pinned (`python:3.11-slim@sha256:...`). Dependencies locked with `pip-compile --generate-hashes` into `requirements.lock`, installed with `--require-hashes` for supply chain integrity. Two builds of the same SHA now produce identical images.
* **`.dockerignore` (P3)**: Excludes `*.md` (including the 154 KB `CODEREVIEW.md` with exact file:line reproduction detail), `*.json` (prevents baking runtime config that a rebuild silently reverts), tests, caches, `.env`, and logs from the production image. `CODEREVIEW.md` also removed from the repository.

**Test Coverage (§1 Money-Math)**
* **`test_cost_attribution.py` (30 tests)**: Dedicated unit tests for chargeback financial math — slot-hour conversion (÷ 3,600,000), Rule A proportional waste allocation, Rule B central dump, negative-waste clamp (C1a), zero-denominator guard (C1b), rounding consistency (M6: `direct + waste == total` to the cent), reservation-ID normalization (short/dotted/colon formats), unconfigured reservation accounting, Pydantic field validation (M7), focus filter isolation, and the master invariant (`Σ attributions == Σ bill`). Includes explicit mutation detectors for §1 mutations #2 and #5.
* **`test_hbo.py` (19 tests)**: Dedicated unit tests for HBO savings formula — M1 proportionality (`saved_slots = post_slots × (prev − dur) / dur`), monthly projection via `DAYS_PER_MONTH`, edition-aware pricing, null/zero guards, sorting, and empty-result handling. Includes explicit mutation detector for §1 mutation #1 (the ÷ 3,600,000 → ÷ 360,000 10× inflation).

**Fixed**
* **AI Doctor TiB/TB Pricing**: Cost figures used `POW(10, 12)` (decimal TB) instead of `POW(1024, 4)` (TiB), overstating every dollar figure by ~10%. Also replaced hardcoded `6.25` with the `ON_DEMAND_USD_PER_TB` constant.
* **Dashboard Null Crash**: KPI panel crashed on `.toString()` of null metrics. Cached the broken payload, causing a permanent spinner. Now guards all values with `?? 0`, wraps cached render in try/catch, and only caches after successful render.
* **Copy SQL / Dry Run Broken**: The global fetch sanitizer HTML-escaped `optimized_query` and `query` fields, corrupting SQL operators. Exempted raw-SQL keys from the sanitizer proxy with `escHtml` retained at render sinks.
* **Active Assist Focus Filter**: Focus branch omitted `AND state = 'ACTIVE'`, causing actioned/dismissed recommendations to inflate savings in focus mode.
* **Profiler Null Guard**: `avg_duration_seconds` can be `NULL` for cancelled queries; `None < 5` raised `TypeError` and 500'd the profiler panel.
* **Fluid Scaling False "Fully Enabled"**: Empty result set (wrong admin project, wrong region) produced `is_fully_enabled: true`. Now returns `null` to distinguish "no data" from "genuinely enabled."
* **HBO Client Construction**: `bigquery.Client(project=prj)` outside the `try` block meant one bad project ID killed an entire 300-project HBO check. Moved inside `try` with context manager.
* **Chargeback Config Save**: `POST /config` response was never checked — a failed save silently ran `/calculate` against the previous config. Now checks `.ok` and aborts with the backend error detail.
* **Empty Exception Crash**: `splitlines()[0]` raised `IndexError` on exceptions with empty `str()`, destroying the original error and preventing retries.
* **High Frequency SQL HAVING Clause Fix**: Resolved BigQuery `400 Aggregate function COUNT(*) not allowed in WHERE clause` error by filtering on CTE column alias `execution_count > 1`.
* **UI Empty Notification & Tooltips**: Updated toast messages when candidate queries pass clean with zero anti-patterns, and updated UI tooltips for the `cumulative_cost` strategy.
* **`total_bytes_processed` Metric Correction**: Included `total_bytes_processed` in discovery SQL CTEs, ensuring `bytes_scanned_original` is accurately populated under BigQuery Editions.
* **Strategy-Aware Re-Sorting**: Ensured secondary project query fetches preserve exact strategy ordering (`optimization_potential_score`, `total_effective_bytes`, `execution_count`, `annualized_cost_usd`, or `total_slot_ms`).
* **Frontend Escaping Typo**: Fixed `escapeHtml` typo in `static/app.js` to ensure migration YAML badges render smoothly.
* **Chargeback Over-Recovery**: `max(0, bill − direct)` silently dropped over-recovery when measured usage exceeded the invoice (e.g. CUD discount). Attribution totals exceeded the actual bill by up to 20%.
* **Chargeback Zero Denominator**: When all jobs were cache hits (`SUM(slot_ms) = 0`), every project's share was `0` while `waste_cost` was the full bill — $50k vanished silently with `is_complete: true`.
* **Chargeback Waste Rule Validation**: `waste_rule` accepted any string (`"a"`, `"Proportional"`) — neither branch matched, silently dropping 100% of waste. Now uses `Literal["A", "B"]` with Pydantic 422.
* **Chargeback Pending Jobs**: Missing `state = 'DONE'` filter and no `IFNULL` guard on `total_slot_ms`. Running/pending jobs contributed partial slot-ms; all-NULL groups raised `TypeError` and 500'd the endpoint.
* **HBO Savings Understatement**: `saved_slot_hours` used `percent_saved × post_optimization_slots` — a ceiling unrelated to actual savings. Fixed to `post_slots × (prev_ms − dur_ms) / dur_ms` (proportionality assumption), matching the SQL summary formula.
* **HBO Project Discovery**: `LIMIT 500` with no `ORDER BY` returned an arbitrary subset. Added `ORDER BY project_id`, `LIMIT 501`, and truncation signal.
* **HBO Borrowing Rule**: `borrowing_rule` dropdown was wired to nothing — changing it had zero effect on calculations. Now rejected with HTTP 501 if non-default.
* **Chargeback Field Constraints**: `sku_rate` and `total_admin_bill` accepted negative and infinite values. Added `ge=0, allow_inf_nan=False`.
* **Chargeback Atomic Save**: `save_config` truncated the file in-place — concurrent writes or SIGTERM left a partial/empty file, permanently 500'ing all endpoints. Now uses temp file + `os.replace`.
* **XSS in Notifications**: `showNotification` used `innerHTML` with raw DOM input values. Replaced with `textContent` to prevent XSS via crafted project IDs or snapshot import.
* **Snapshot Import Sanitizer**: `sanitizeImportedValue` returned non-JSON strings raw — the XSS delivery path for scalar settings keys (`bq_org_project`, `bq_admin_project`). Now escapes bare strings.
* **Settings Validation Abort**: State and localStorage were mutated before validation — invalid input was persisted, cached, and survived reload. Now validates into local variables first and only commits after passing.
* **Scope-Dependent Cache Flush**: Six scope-dependent keys (`bq_slots_utilization`, `bq_fluid_estimate_data`, etc.) survived scope changes, rendering stale data under the new region/project header. Replaced deny-list with an allow-list of scope-independent keys.
* **Error Detail Rendering**: FastAPI 422 `detail` is an array of `{loc, msg}` objects. `err.detail || 'fallback'` rendered as `[object Object]`. Added `detailToMessage()` helper across all 11 error sites.
* **Dry Run Button Removed**: Removed the `/api/ai/dry_run` endpoint, its Pydantic models (`DryRunRequest`, `DryRunResponse`), the frontend button/event handler, and 2 dedicated test cases. The feature carried risk with minimal value — the underlying Migration API's internal `dry_run_compare` parameter is unaffected.
* **CSP-Blocked Inline Handlers**: All inline `onclick` handlers in the AI Doctor table (YAML accordion, advice toggle) replaced with **event delegation** via `tbody.addEventListener`, resolving click failures under Content Security Policy restrictions.
* **Event Listener Stacking**: Filter pill click handlers now use clone-to-reset (`cloneNode(true)` + `replaceChild`) to prevent duplicate handler accumulation across re-renders.
* **Governance Cache Wipe**: Both governance scan buttons called `clearModuleCache` on the shared `bq_gov_results` key before merging, wiping the sibling scan's cached data on every run.
* **Simulation Empty Array**: `Array.reduce()` on empty `/api/slots/simulate` response raised `TypeError`, surfaced verbatim as an error toast.
* **Format Data Size Threshold**: `formatDataSize` switched units at `1000 GB` but divided by `1024`, rendering 1,010 GB as "0.99 TB". Changed threshold to `1024`.
* **Max Bytes Billed Clamp Logging**: Silent clamp of `max_bytes_billed_gb` now logs a warning when the value is rewritten.
* **Fluid Scaling Zero Lookback Guard**: `_to_metric` division by `lookback_days` now guarded against zero for internal callers.
* **Alias Regex Fix**: `_ALIAS_RE` used `$` instead of `\Z`, allowing a trailing newline to bypass the check.
* **Migration Optimizer Echo Bug**: When the BigQuery Migration API subtask call failed (403, timeout), `walk_literals` returned the user's own input SQL as the "optimized" result with `success=True`. Now strips the source literal so only subtask-derived output qualifies.
* **Phantom −100% Savings on CTE Rewrites**: Multi-statement scripts (e.g. CTE-to-temp-table rewrites) dry-run as 0 bytes in BigQuery, producing phantom −100% byte deltas. Now detects zero-byte dry-runs and reports "not measurable" instead.
* **Storage Savings Fabrication**: When `SCHEMATA_OPTIONS` permissions were missing, both fast and fallback queries swallowed errors and returned an empty set — indistinguishable from "no datasets on physical billing." Every dataset was then labeled "logical" with fabricated savings. Now tracks failed projects and excludes them.
* **HBO Focus Mode Fallback**: When all focus projects were healthy (`enabled=True`), the empty-output fallback emitted a row for the admin project (not in the focus list). Now always emits one `HBOStatus` per checked project, including `enabled=True`.
* **Chargeback Rounding Drift**: Three independent `round(x, 2)` calls per row could produce `1.00 + 1.00 = 2.01`, breaking invoice reconciliation. Now rounds the total first and derives the waste as the residual.
* **HBO KPI Tiles 300× Discrepancy**: Live tiles showed org-wide summary, but cached tiles were computed from the top-10 slice (~300× difference on reload). Now caches `/api/hbo/summary` and restores tiles from it.
* **Scope Map Fails Open**: When `/api/meta/scope-map` failed to load (502, cold-start timeout), all endpoints were unmapped and received `focus_projects`, triggering Pydantic `extra=forbid` → 422 for the whole session. Unmapped endpoints now default to `org` scope.
* **Null-Safe formatNumber**: `formatNumber(null)` threw `TypeError`, killing the render loop. Now returns `'0'` for `null`, `undefined`, and `NaN`.
* **GiB/TiB Labels**: Binary computations (÷ 1024³, ÷ 1024⁴) were labeled "GB"/"TB" (decimal units) — a 7.4% visual understatement vs the BigQuery Console. Corrected to "GiB"/"TiB".
* **Unused AIResult Fields Removed**: `dry_run_validated`, `bytes_scanned_optimized`, `estimated_savings_pct`, and `is_external_table_query` were declared but never populated — zero defaults would silently render "0%" in a future UI column.
* **Identifier Regex Documented**: `_safe_ident` allows dotted paths (required for domain-scoped GCP projects like `example.com:legacy-project`). Added documentation confirming this is intentional and safe due to backtick quoting in SQL.

---

## July 25, 2026 — v1.2.4

**Fixed**
Removed inflated `bytes_billed_avg` and `bytes_processed_avg` columns from `/api/slots/utilization` — `JOBS_TIMELINE` rows are per-second slices, so summing job-level byte columns multiplied real values by the job's duration. Cost attribution now surfaces unconfigured reservations (`unattributed_reservations`, `total_unattributed_slot_hours`, `is_complete`) instead of silently skipping them.

**Fixed**
Governance partition audit now uses `INFORMATION_SCHEMA.COLUMNS` as primary detector (cheaper, covers empty tables, yields actual partition column name) with a supplementary `PARTITIONS` probe for ingestion-time tables. `BadRequest` errors (malformed SQL, bytes-billed cap exceeded) no longer retried; actionable error messages returned.

**Fixed**
Tiered slot recommendations now filter `job_type = 'QUERY'` and exclude parent `SCRIPT` jobs. `fairness_enabled` tracked per admin project instead of last-writer-wins. HBO slot-hour price parameterized (`price_per_slot_hr`), ranking fixed to sort by `saved_slot_hours`, and `limit` bounded (`1..1000`). Governance audit honours `audit_type` discriminator.

**Change**
Removed dead `reservations_sql` variable, unreachable cost attribution else branch, and three fabricated anomalies from `/api/dashboard/anomalies`. Fixed stale fluid-scaling comment and added `borrowed_slots = 0` rationale documentation.

---

## July 24, 2026 — v1.2.3

**Feature**
Updated the AI Doctor module to use `gemini-3.6-flash` as the default model in the dropdown and backend, with `gemini-3.5-flash-lite` as the secondary option for semantic SQL anti-pattern analysis and optimization recommendations.

---

## July 21, 2026 — v1.2.2

**Feature**
The Fluid Scaling tab now includes an interactive **Config Builder** with a checkbox table listing all reservations and their current status (Enabled / Not Enabled). Users can toggle individual reservations on or off, and the `ALTER PROJECT` DDL regenerates live. Already-enabled reservations are pre-checked to prevent accidental drops. Includes a "Select All" toggle for convenience.

**Fixed**
The `run_query_with_retry_limit()` utility now fails immediately on permanent errors (403 Forbidden, 404 Not Found) instead of retrying 5 times with exponential backoff. For org-scope scans with inaccessible projects, this eliminates ~15 seconds of wasted delay and 5 redundant BigQuery jobs per denied project.

**Fixed**
All "Copy DDL" and "Copy Job ID" buttons relied on `navigator.clipboard.writeText()`, which is only available in secure contexts (HTTPS or localhost). Added a `copyToClipboard()` helper with a `document.execCommand('copy')` fallback so clipboard operations work in all environments. Affects 9 copy buttons across Storage, Slots, Edition Matrix, HBO, Fluid Scaling, and Cost Attribution pages.

**Fixed**
The "Configuration Recommendations" table on the Slots Optimizer page generated a `preflight_fluid_autoscaling_reservations` DDL containing only the single missing reservation, silently dropping all already-enabled reservations from the property array. The DDL now correctly builds the full set (existing + new) before generating the `ALTER PROJECT` statement.

**Change**
Enhanced sidebar navigation hover effects with primary-color background highlights, subtle slide-right animation, icon glow, and pointer cursor. Removed visible focus rectangles on mouse clicks while preserving keyboard-only focus via `:focus-visible` for accessibility.

**Fixed**
The BigQuery Python SDK's hidden `job_retry` mechanism was silently resubmitting failed queries as brand-new jobs in an infinite loop. A single Active Assist request generated 154+ duplicate BigQuery jobs over several minutes. Disabled the SDK's `job_retry` and `retry` at both `client.query()` and `query_job.result()` layers, ensuring all retries are controlled exclusively by `run_query_with_retry_limit()` with a hard cap of 5 attempts.

**Fixed**
The HBO status-check query in `hbo.py` bypassed `run_query_with_retry_limit()` entirely, calling `client.query().result()` directly without retry caps or `job_retry=None` protection. Routed it through the same protected path as all other queries.

**Fixed**
The "Copy DDL" button remained visible on the Fluid Scaling panel even when all reservations were already enabled and no DDL was needed. The button is now hidden in the green "all enabled" state and re-shown when missing reservations are detected.

**Fixed**
Release note highlights in the About panel rendered markdown bold (`**Config Builder**`) and inline code (`` `ALTER PROJECT` ``) as raw text instead of formatted HTML. Added post-processing to convert `**text**` to `<strong>` and `` `text` `` to `<code>` elements.

**Fixed**
When Active Assist recommendations failed to load (e.g., BigQuery internal error), the error was silently swallowed to the browser console with no user-visible feedback. Now displays both a toast notification and an inline warning message in the results table.

**Change**
Suppressed noisy third-party DEBUG loggers (`urllib3`, `google.auth`, `google.api_core`, `httpcore`) that flooded `app.log` with hundreds of HTTP round-trip lines when `LOG_LEVEL=DEBUG` was active. Only application-level debug messages (SQL queries, endpoint timing) are now logged.

---

## July 17, 2026 — v1.2.1

**Change**
Replaced references to "Vertex AI" with "Agent Platform" in the AI Doctor frontend and linked to the official Gemini Enterprise Agent Platform documentation.

**Feature**
Improved empty-state handling in the AI Doctor UI when the backend filters out queries that solely interact with system tables or ML functions (like `INFORMATION_SCHEMA` or `AI.GENERATE_TEXT`) without physical DDL schemas.

**Fixed**
Radically improved the secondary SQL extraction query speed in AI Doctor (`JOBS_BY_PROJECT`) by enforcing `AND job_id IN UNNEST(@job_ids)` filtering, preventing full historical table scans across all project jobs.

**Fixed**
Fixed a bug in the AI Doctor schema extraction engine where 3-part system queries (`region-us.INFORMATION_SCHEMA...`) tricked the table parser into treating the region as a project ID, resulting in 403 errors. `INFORMATION_SCHEMA` paths are now properly excluded from physical DDL lookups.

---

## July 13, 2026 — v1.2.0

**Feature**
Organization-Level Schema Auditing: engineered a dynamic discovery and UNION ALL orchestration layer to seamlessly scan the entire GCP organization for missing partitions and clusters without querying empty projects or hitting API limits.

**Feature**
Introduced a global `focus_projects` parameter to scope analysis to specific GCP projects rather than the entire organization. Available on 19 org-level endpoints.

**Feature**
The `analyze_jobs` response now includes sample metadata (job count, sampling method, bytes-billed threshold), enabling accurate explanations of sampling bias in the frontend.

**Feature**
`RotatingFileHandler` logging is now gated behind an `ENABLE_FILE_LOG` environment variable to optimize memory in Cloud Run environments.

**Feature**
Versioned static assets served with version query parameters now receive long-lived caching headers (`Cache-Control: public, max-age=31536000, immutable`), reducing repeat page-load latency.

**Security**
Resolved a DOM-based Stored XSS in the Slots Profiler caused by a sanitizer whitelist bypass, and a full-application XSS vulnerability via snapshot hydration bypass. All imported snapshots and table rendering functions now properly HTML-escape user data.

**Security**
Removed all hardcoded fallback demo data (including mock project names, dummy schema names, and fabricated dollar amounts) from backend endpoints. Empty queries now return true empty states.

**Security**
Removed the `focus_projects` parameter from Capacity Planning and Active Assist endpoints where it previously distorted capacity totals or was ignored.

**Security**
Added `_safe_ident()` validation to BQ-sourced project IDs before SQL interpolation. Bounded numeric parameters (e.g., capping `lookback_days` to 90) to prevent unbounded org-wide scans or division-by-zero crashes.

**Fixed**
MV Cost Auditor: fixed a bug where MV refresh patterns were silently missed across projects. The auditor now discovers all projects with MV activity and keys the inventory correctly by project, preventing phantom counts and accurately measuring org-wide MV costs.

**Fixed**
Slot Utilization Concurrency: changed the aggregation method to sum across all concurrent jobs before taking quantiles. Capacity metrics now correctly reflect true org-wide concurrent demand rather than the single largest job's peak.

**Fixed**
Cost Attribution: rejected `focus_projects` filters in cost attribution (which corrupted waste allocation math), fixed waste vanishing under Rule B when the central project wasn't configured, and corrected the exclusion of `NULL` statement_type rows that artificially inflated wasted direct usage.

**Fixed**
BI Engine savings estimates are now only applied to on-demand jobs (not Editions). On-Demand cost simulations no longer erroneously include failed queries that aren't billed by BigQuery.

**Fixed**
Skew analysis now filters out SCRIPT parent/child jobs to avoid double-counting stages. DML auditor now requires `state = 'DONE'` to prevent in-flight jobs from inflating wasted slot hours.

**Fixed**
Verified that the user-configured `max_bytes_billed_gb` safety cap is consistently forwarded to and enforced by all fluid scaling BigQuery API calls.

**Fixed**
Projects failing HBO access verification now report as `unknown` status rather than false positives for being enabled.

**Fixed**
Invalid combinations of `time_travel_rescale` (without setting hours) or non-integer float hours are now strictly rejected to prevent generating unusable BigQuery DDL.

**Fixed**
Replaced a fabricated row count estimate (based on byte size) with the actual `total_rows` metric from `INFORMATION_SCHEMA.TABLE_STORAGE` in Static Schema Audit.

**Change**
Centralized standard date constants (`DAYS_PER_MONTH`), eliminated redundant `.groupby` operations in fluid scaling, and parameterized job IDs for AI Doctor instead of using string interpolation.

**Change**
Promoted the Schema Optimizer to its own navigation group and added collapsible educational panels for power users.

---

## July 8, 2026 — v1.1.0

**Feature**
Introduced a dynamic `max_bytes_billed_gb` safety cap parameter, dynamically passed to every BigQuery query execution. Includes smart clamping (1 GiB–10 TiB), fallback defaults, and local storage state persistence.

**Feature**
Built the **Slot Capacity Bucket Method** — a high-performance simulation engine using NumPy to model slot consumption telemetry. Evaluates per-minute timelines to generate Tiered Recommendations (Aggressive p80, Balanced p95, Performance Max).

**Feature**
Leverages BigQuery's native `AI.GENERATE` scalar function to run high-speed SQL reviews using Gemini 3.1 Flash Lite, generating actionable `SET @@reservation` routing snippets for expensive queries.

**Feature**
Hybrid Cost Attribution Engine: solves SKU blending resolution for BigQuery Editions, proportionally redistributes unallocated idle capacity waste, and supports custom attribution rules (Lender vs. Borrower Pays).

**Feature**
Fluid Scaling analysis: identifies reservations suffering from the legacy autoscaler's 60-second minimum charge and models the financial impact of transitioning to Fluid Scaling (true per-second billing).

**Feature**
Proactive Migration Guardrails (Static Schema Auditor): audits the metadata catalog to identify high-risk, unclustered, or unpartitioned tables before they are queried, suggesting optimal clustering columns and generating remediation DDL.

**Feature**
Added `focus_projects` parameter allowing users to scope org-wide analysis down to a specific subset of up to 50 GCP projects.

**Feature**
Introduced a 4-icon structured logging sequence (`▶`, `⏳`, `✅`, `◼`) with request correlation IDs, clickable BigQuery Console URLs, and strict environment-based `LOG_LEVEL` controls.

**Security**
Conducted a rigorous sweep of all source files, documentation, and mock data to remove all traces of proprietary company identifiers. Verified zero credentials leak and restructured `.gitignore` for strict privacy.

**Fixed**
AI Doctor Discovery: fixed a bug where the discovery query was incorrectly scoped to the admin project instead of the organization, failing to scan the most expensive queries org-wide.

**Fixed**
Implemented strict project ID regex validation on global settings. Fixed stale cached results persisting in the UI after scope changes.

**Fixed**
Increased `max_output_tokens` and reduced `thinking_level` to resolve silent `NULL` returns for complex queries in `AI.GENERATE`. Summarized DDL payloads to reduce token overhead.

**Change**
Added an "About" panel with live versioning, improved the Active Assist module with "Blind Spot" educational cards, grouped Schema optimization logically in the sidebar, and resolved tooltip flashing issues.

---

## Core Modules (Introduced in v1.0.0)

The following FinOps analysis modules are included since the initial release:

- Storage Cost Optimizer (logical vs. physical billing)
- On-Demand vs. Editions Job Analyzer
- Slot Usage Timeline & Capacity Planner
- Query Anti-Pattern Linter (SELECT *, unclustered LIMIT)
- Storage Hygiene & Time Travel Auditor
- DML Abuse Tracker
- BI Engine ROI Optimizer
- Governance & Expiration Auditor
- Data Skew Analyzer
- Interactive vs. Batch Optimizer
- Materialized View FinOps Auditor
