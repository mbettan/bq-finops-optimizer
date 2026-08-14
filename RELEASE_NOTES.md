# Release Notes — BigQuery FinOps Optimizer

This page documents production updates to the BigQuery FinOps Optimizer. We recommend that users periodically check this list for any new announcements.

For architecture details and tech stack information, see the [README](README.md).

---

## August 13, 2026 — v1.4.5

**Feature (Server-Side Shared Result Cache — GCS FUSE & Cross-User Hydration)**
Added a production-grade, zero-base-cost **Server-Side Shared Result Cache** (`src/cache.py`) that decouples diagnostic state from browser `localStorage` and shares analysis results across instances, cold starts, and team members:
- **Zero-Cost GCS FUSE Volume Mounts:** Leverages Cloud Run Gen 2 Cloud Storage volume mounts (`/cache`) backed by GCS FUSE, providing multi-instance result persistence with zero infrastructure cost at idle ($0/mo base vs. $35–50/mo for Redis/Memorystore).
- **Exact Parameter Hashing & Scoping:** Implements canonicalized SHA-256 parameter hashing (`v1/{org}/{region}/{module}/{hash}.json`) ensuring distinct lookback periods (e.g. 7d vs 90d), pricing rates, and focused project filters never cross-contaminate or serve invalid results. Domain-scoped project IDs (`example.com:proj`) are securely hashed (`h_<sha256>`).
- **In-Process Single-Flight Concurrency:** Mutex registry collapses concurrent identical requests on the same instance into a single BigQuery scan, preventing redundant compute during parallel report generation sweeps.
- **Cache Control & Freshness API:** Added `/api/cache/status` for instantaneous multi-module freshness inspection (accelerated by the 60s kernel stat cache), `/api/cache/{module}` for module-level eviction, and `/api/cache` for scope-wide invalidation.
- **Client-Side CacheClient & Instant Hydration:** Browser `localStorage` is converted into a lightweight first-paint render cache with non-destructive fallback (`safeSetLocalStorage`). Added `hydrateFromServerCache()` to populate views and assessment reports for new users or clean browsers with zero BigQuery cost (`X-Cache: HIT`).
- **Automatic Invalidation:** Updated Cost Attribution configuration updates to automatically drop stale attribution calculations across all scopes.
- **Standard HTTP Headers:** Emits `X-Cache` (`HIT`, `MISS`, `BYPASS`), `X-Cache-Age`, `X-Cache-Expires`, and `X-Cache-Key` while preserving unwrapped JSON payloads and `/api/meta/scope-map` endpoint introspection.

---

## August 13, 2026 — v1.4.3

**Fixed (Report Generator Normalization, Clean Scans & Universal CSV Export)**
- **Baseline Cost Normalization:** Standardized all report baseline and finding metrics (on-demand compute, fallback Editions pricing, slot-hours, average slots) to normalized 30-day monthly values regardless of custom `lookback_days` windows (7d, 14d, 90d).
- **Division Safety & Boundary Validation:** Added strict range validation (`1 <= lookback_days <= 365`) on `/api/report/prepare` and internal defensive sanitization to prevent zero/negative division.
- **Clean Scan Evaluation Matrix:** Updated Section 6 check matrix (`_is_module_evaluated()`) so diagnostic scans evaluated with 0 anti-patterns correctly render as **`✅ Passed`** rather than "Not run (no data)".
- **Universal Multi-Page CSV Export:** Upgraded `downloadTableAsCSV()` to export all filtered pages from DataTables internal cache while preserving user-selected sort order.
- **Snapshot Import Resilience:** Added 15 MB file size limit pre-check and automatic file input element reset on error/completion.
- **Defensive HBO Insights Rendering:** Added null-safe guards and fallback formatters across slot contention, shuffle quota, and data volume sub-tables.
- **AI Model Catalog Alignment:** Updated model pricing and documentation references to Gemini 3.6 Flash and Gemini 3.5 Flash-Lite across interactive calculators, documentation specs, and CI workflows.

---

## August 13, 2026 — v1.4.2

**Feature (Assessment Report Generator & Comprehensive Analysis Sweep)**
Added an end-to-end **Assessment Report Generator** (`src/report_generator.py`, `static/report.css`) with automated sequential sweep execution:
- **Comprehensive Analysis Sweep:** Automatically sweeps through BigQuery FinOps modules (Compute & Pricing, Slots, Simulation, Fluid Scaling, Top Spenders, Cost Attribution, Anti-Patterns, Storage, Hygiene, Governance, Active Assist, AI Doctor, BI Analyzer, HBO, Performance Warnings) with real-time stepper progress and error resilience.
- **Standalone Assessment Document:** Generates a self-contained, executive-ready assessment report with synthesized findings, executive KPI scorecard, workload optimization rankings, and print/PDF optimization.

**Documentation & Governance (IAM Least-Privilege Review)**
Updated documentation and error guidance to reflect least-privilege IAM configurations:
- Replaced `roles/bigquery.resourceAdmin` with read-only `roles/bigquery.resourceViewer` at the organization level.
- Removed overprivileged role references (`roles/bigquery.admin`, `roles/bigquery.dataOwner`) from documentation and runtime error messages.
- Downgraded org-level Data Viewer to optional `metadataViewer` with automated query fallback.
- Added explicit custom role permission guidance for `bigquery.jobs.create`.

---

## August 12, 2026 — v1.4.1

**Feature (Interactive Runtime Economics & FinOps ROI Calculator — #60)**
Added an interactive client-side **Diagnostic Cost & Savings Calculator** to GitHub Pages (`docs/static/calculator.js`, `docs/static/pricing.js`). FinOps practitioners and architects can simulate their exact monthly runtime economics before deployment:
- **3-Way Runtime Modeling:** Calculates BigQuery `INFORMATION_SCHEMA` metadata scans ($6.25/TiB on-demand vs. $0 under BigQuery Editions reservations), Serverless Cloud Run container compute (2 vCPU / 4 GiB), and Gemini 3.5 Flash / 3.1 Flash-Lite AI Doctor token usage.
- **Live Presets & Cadence Scaling:** Interactive sliders and quick-select presets (Small, Medium, Large, X-Large) with real-time monthly scaling across ad-hoc, weekly, daily, and continuous sweeps.
- **Full-Width Product Roadmap:** Redesigned the public roadmap toolbar into a full-width filter bar with inline GitHub live synchronization and touch-scrolling snapping on mobile.

---

## August 7, 2026 — v1.4.0

**Feature (Top Spenders: Actual Billing Mode, Waste & Potential Savings Engine)**
The **Top Spenders** profiler has been upgraded from hypothetical cost projections into an actionable, actual spend attribution and waste detection engine:
- **Billing Mode Classification:** Queries are partitioned by their true execution mode using `reservation_id IS NOT NULL` (assigned slot reservation) vs. `reservation_id IS NULL` (multi-tenant on-demand). Spenders receive color-coded badges: **Reservation** ($\ge 80\%$), **On-Demand** ($\le 20\%$), or **Mixed** (displaying exact percentage and query count breakdown).
- **Waste & Non-Productive Spend Detection:** Introduces exact dollarized waste tracking for capacity consumed with zero output — failed/cancelled query slot-hours & on-demand bytes plus the 10 MiB minimum-billing floor overage. Waste is bounded as a strict subset of actual spend ($W \le A$).
- **Frequency-Ranked Primary Reservations:** Utilizes `APPROX_TOP_COUNT` to extract and rank primary reservation names, surfaced via rich tooltips on the billing mode badge.
- **Actual Cost Attribution:** Computes true dollar spend by combining actual on-demand billed bytes (`total_bytes_billed` for OD jobs) and active slot duration (`total_slot_ms` for reservation jobs) without cross-contamination.
- **Actionable Potential Savings:** Evaluates `actual_cost - min(est_on_demand, est_editions)` to highlight high-ROI optimization candidates (such as heavy on-demand pipelines that belong on a reservation). When a user is already operating on the optimal pricing model, savings cleanly display as zero/dash (`—`).
- **Hypothetical Cost Accuracy (`hypothetical_od_bytes`):** Uses logical bytes scanned (`total_bytes_processed`) for reservation queries (which record `total_bytes_billed = 0`), preventing false-positive recommendations that previously suggested 100% reservation workloads could run for "$0" on-demand.
- **Multi-Statement Script Attribution:** Filters on `(statement_type != 'SCRIPT' OR statement_type IS NULL)` instead of `parent_job_id IS NULL`. This ensures child statements executed by orchestrators (Airflow/Cloud Composer, dbt, Stored Procedures) inherit their real `reservation_id` rather than being misattributed as on-demand by the parent script wrapper.
- **Data Transparency Tooltips:** When processed data exceeds billed data (due to reservation queries billing 0 bytes), hovering over the **Total Data Billed** cell explains the exact breakdown used for hypothetical on-demand cost calculations.
- **Executive KPIs & Sorting:** Refreshed KPI cards now track **Active Users Analyzed**, **Actual Total Spend**, **Wasted Spend**, **Potential Savings**, and **Total Slot Hours**, with default DataTables ordering pinned to **Actual Cost DESC**.

**Feature (Interactive vs. Batch Priority Engine)**
The Batch Candidates scan is now workload-centric rather than job-centric. Executions are aggregated into logical workloads using lineage labels (`dbt_model`, `airflow_dag_id`/`dag_id`, `dataform`, `looker`/`tableau`/`dashboard_id`, `requestor`), the `scheduled_query_` job-ID prefix, and service-account identity, then classified as **UNDER_BATCHED** (automated pipelines and heavy service-account DML burning the 100-query INTERACTIVE concurrency limit that live dashboards depend on) or **OVER_BATCHED** (human and BI connections stuck behind a >30s BATCH queue). BATCH and INTERACTIVE bill identically — same hardware, same slot-hour and per-byte pricing — so every finding is a pure concurrency win, not a cost trade-off.

Each row carries its detection reasons, a HIGH/LOW confidence grade derived from label provenance, and a copy-pasteable remediation snippet for the detected tooling: dbt `profiles.yml`, Airflow `BigQueryInsertJobOperator`, or the Python SDK / `bq` CLI. Dataform and Scheduled Queries expose no priority flag, so those rows show migration guidance instead of a snippet.

The scan probes `JOBS_BY_ORGANIZATION` with a dry run and transparently falls back to `JOBS_BY_PROJECT` when org-level IAM is unavailable. Cache hits, script child jobs, and failed jobs are excluded so a workload's run count reflects real executions, and the lookback window is bound as `@start_time_period` / `@end_time_period` query parameters instead of being interpolated into the SQL text.

**Feature (Universal CSV Export)**
Every results table across the application now carries a **Download CSV** button, injected automatically at page load and after any table that renders dynamically. Exports the full DataTables result set matching the current search and filter state — not just the visible page — with a UTF-8 BOM for Excel, HTML entity decoding, and RFC 4180 quote escaping. Cell text is read from the rendered DOM, so badges, formatted byte sizes, and currency cells export exactly as displayed.

**Feature (BigQuery Console Deep-Links)**
Job IDs render in monospace with hover- and focus-revealed actions to copy the ID or open the job directly in the BigQuery Console. Dataset and table names link to their Console explorer pages. `MVResult`, `WarningResult`, `BIResult`, and `AIResult` now carry `project_id` — `JOBS_BY_ORGANIZATION` spans the whole organization, so without it every cross-project deep-link resolved to the wrong project.

**Feature (Notification Center)**
A bell icon in the header archives every toast into a dropdown history with relative timestamps, an unread badge, and clear-all. Messages render via `textContent` so notification bodies carrying query text or user emails cannot inject markup.

**Feature (Anti-Pattern Linter: Filtering & Aggregation)**
New toolbar filters the SQL Wall of Shame by identity type (humans vs. service accounts), user, project, and anti-pattern type, with a one-click reset. A **Summary** view toggle swaps the per-job detail table for an aggregated roll-up grouped by (user, project, anti-pattern) showing query counts, cumulative data billed, and total estimated waste — turning a list of individual offences into a ranked list of owners.

**Feature (Storage Hygiene: Time Travel TTL)**
The hygiene auditor now surfaces each dataset's `default_time_travel_days` alongside its time-travel physical bytes, so the tables where lowering the window actually pays are visible without a second lookup. Values are read from `INFORMATION_SCHEMA.SCHEMATA_OPTIONS` in a single batched `UNION ALL` across up to 50 projects rather than one query per project.

**Feature (Settings & Cost Attribution Guidance)**
The Execution Project and Admin Project fields are now labelled by what they scope, with the exact IAM roles each one needs stated inline. Waste Rule, Central IT Project, Borrowing Rule, billing window, SKU rate, and Total Bill each gained tooltips explaining the financial trade-off rather than restating the field name. Cost attribution auto-populates its reservation list from the last Slots Optimizer run when the table is empty.

**Change (DML Abuse Detection: Per-Table Attribution)**
The DML abuse auditor now aggregates by destination table rather than by user alone, adding **Active Days** and **Avg Inserts / Day** columns so a steady pipeline is distinguishable from a one-day backfill. The default threshold drops from 1000 to 100: it applies per (destination table, user, project), and BigQuery caps a table at 1500 table-modifying operations per day, so 100/day against a single table already warrants the Storage Write API. Large counts render through a new `formatCompact()` helper with numeric `data-order` attributes preserved for sorting.

**Fixed (AI Doctor: Echo Suppression)**
When the optimizer returned SQL byte-identical to the input, the UI presented it as a rewrite. The backend now suppresses the echoed SQL and clears `migration_applied_yaml` with it — a config that demonstrably changed nothing should not advertise "Migration API Config Applied". The frontend repeats the test so snapshots captured before this fix behave identically, and the Migration filter pill and its KPI count both honour it.

**Fixed (Snapshot Redaction: value-level email scrubbing)**
Redacted snapshot exports previously decided what to scrub from the *key name* — anything matching `email`. The workload engine reports an operator under `workload_name` (the address is used as the workload identity when a query carries no lineage label), which that rule does not match, so those addresses would have survived a redacted export. Redaction now also scrubs any email-shaped value regardless of the key it sits under, in both object fields and string arrays. This strictly widens coverage; nothing previously redacted is now exported.

**Fixed (Anti-Pattern Filter State)**
Rebuilding the filter dropdowns after a new scan dropped stale values from the `<select>` without clearing them from the filter state, leaving a filter silently active — the table emptied while the dropdown read "All users". Selections are now retained only when still present in the new data, and the state is updated in lockstep.

**Fixed (Storage Hygiene TTL Robustness)**
`SAFE_CAST` replaces `CAST` on `option_value`, so one unparseable setting no longer aborts the union and blanks every other project's TTL. A single unreadable project triggers a per-project retry so partial access still yields partial data, and the 50-project cap logs exactly how many datasets fall back to the 7-day default instead of truncating silently.

**Fixed (Accessibility & Input)**
Job ID row actions used `opacity: 0`, which still accepts keyboard focus — tabbing through a results table landed on invisible buttons. They now toggle `visibility`, reveal on `:focus-within`, and stay pinned visible under `@media (hover: none)` so touch devices can reach them at all.

**Performance (BigQuery Client Pooling & ADC Caching)**
Replaced per-request client construction with a thread-safe, process-wide BigQuery client pool (`get_bq_client`) with LRU eviction and single-discovery Application Default Credentials (ADC) caching. Eliminates two metadata-server HTTP round-trips and TLS handshakes per API request on Cloud Run, and prevents ~1,000 metadata queries during HBO multi-project fan-outs. Pooled connections cleanly drain during application shutdown.

**Fixed (Cost Attribution: Idle Reservation Reconciliation)**
The cost attribution engine now iterates across all configured reservations as the primary loop. 100% idle reservations (zero query slot-hours) are accurately attributed as unallocated waste under Rule A/B instead of disappearing from reports, maintaining strict invoice reconciliation. Date range validation now parses explicit `datetime.date` objects to eliminate unpadded string comparison bugs.

**Fixed (Top Spenders: Minimum Billing Waste Isolation)**
Added `error_result.reason IS NULL` to minimum-billing overage calculations, ensuring failed queries are counted exclusively under `failed_cost` and never double-counted in minimum-billing floor waste.

**Fixed (Migration Optimizer: Diagnostic Guards & Timeout Resilience)**
Excluded SQL file literal nodes from diagnostic extraction and stripped comments prior to transformation matching, preventing false auto-opt-in rules. Constrained scalar subquery detection strictly to projection clauses and added Pass 1 timeout resilience with graceful fallback to Pass 2 translation.

**Fixed (Anti-Pattern Linter: Predicate Pushdown & Determinism)**
Pushed `REGEXP_CONTAINS(query, r'(?i)SELECT\s+\*\s+FROM')` and `ORDER BY total_bytes_billed DESC` directly into the BigQuery query, ensuring the top offending queries are returned rather than an arbitrary slice. Cleaned snippet truncation formatting.

**Fixed (Pricing Consistency & Unit Alignment)**
Centralized `ON_DEMAND_USD_PER_TB` and `EDITIONS_SLOT_HR_RATE` as process-wide sources of truth across all simulation modules and BI Engine queries. Aligned Active Assist byte conversions to decimal TB ($10^{12}$) matching the Google Cloud Recommender API format.

**Fixed (HBO Status: Dynamic Limits & Truncation Signaling)**
Parameterized the active project discovery query with configurable `limit` (up to 5,000) and surfaced a `truncated: bool` response indicator when active project count exceeds the configured ceiling. Removed unreachable fallback dead code.

---

## July 28, 2026 — v1.3.0

**Feature (AI Doctor: Multi-Strategy Discovery & ROI Engine)**
Re-engineered AI Doctor workload discovery from single-job lookups to an aggregated hash-grouped engine across `JOBS_BY_ORGANIZATION` with 5 strategy modes: **Balanced ROI** (multi-factor composite score), **Cumulative Cost** (scanned/billed bytes), **High Frequency** (repeat micro-offenders), **Memory Spill** (RAM-to-disk shuffle), and **Total Slot Time** (heavy compute). Includes Editions hybrid cost fallback (`GREATEST(billed, processed)` so $0-billed reservation queries rank by scanned volume), atomic worst-job sampling via `ARRAY_AGG(... ORDER BY slot_ms DESC LIMIT 1)[OFFSET(0)]`, and stage-level spill aggregation through `UNNEST(job_stages)`.

**Feature (AI Doctor: Executive Dashboard & UX)**
Added an executive KPI summary strip (Audited Spend, Severity Breakdown, Schema DDL Coverage) and a smart filter toolbar with 6 interactive pills (All, High, Medium, Migration Config, Schema Gap, Repeat) with live counts. Enhanced the results table with cost-descending default sort, severity left-stripe borders, collapsible YAML accordion (CSP-safe event delegation), advice truncation toggle, Copy SQL button for original queries, and scrollable `<pre>` code blocks.

**Feature (Fluid Scaling Edition-Aware Pricing)**
Per-edition slot pricing now applied per-reservation: Standard ($0.04/slot-hr), Enterprise ($0.06), Enterprise Plus ($0.10), with user-supplied `price_per_slot_hr` as fallback.

**Feature (HBO Per-Job Optimization Badges)**
New `POST /api/hbo/optimizations` endpoint enriches HBO results with exact optimization types (Semi-Join Reduction, Join Commutation, Parallelism Adjustment, etc.) via fault-isolated `ThreadPoolExecutor` fan-out across per-project `JOBS_BY_PROJECT` views. Progressive enhancement — table renders immediately, badges upgrade asynchronously. Distinguishes `None` (undetermined) from `[]` (checked, none applied) to prevent IAM redaction misreporting.

**Security (Hardened Validation & Error Handling)**
Strict IAM 403 enforcement with role guidance; Pydantic `Literal` validation on `discovery_strategy`; retryable 403 classification (quota vs IAM); structured `bytesBilledLimitExceeded` detection; proper 5xx status mapping (429/502); non-root Docker container (`USER 10001`); digest-pinned base image with `--require-hashes`; comprehensive `.dockerignore`.

**Test Coverage**
598 tests passing (7 skipped). Added `test_hbo_optimizations.py` (27 tests: badge building, fan-out isolation, redaction semantics), `test_cost_attribution.py` (30 tests: financial math, rounding invariants, mutation detectors), and `test_hbo.py` (19 tests: savings formula, edition pricing, null guards).

**Fixed (AI Doctor & Migration)**
Corrected TiB/TB pricing (10% overstatement); fixed `HAVING` clause alias error on high-frequency mode; removed dry-run endpoint; replaced CSP-blocked inline handlers with event delegation; fixed listener stacking via clone-to-reset; stripped echo bug in Migration API fallback; handled phantom −100% savings on CTE-to-temp-table rewrites.

**Fixed (Cost Attribution & Chargeback)**
Fixed invoice over-recovery (`max(0, bill − direct)`), zero-denominator guard when all jobs are cache hits, waste rule validation via `Literal["A","B"]`, pending job filtering (`state = 'DONE'`), rounding drift (total-first residual method), negative/infinite field constraints, atomic config save (`os.replace`), and config save error propagation.

**Fixed (HBO Module)**
Corrected savings formula (`post_slots × delta / dur`), added deterministic project ordering (`ORDER BY project_id` with `LIMIT 501` truncation), guarded borrowing rule with HTTP 501, fixed focus mode fallback emitting admin project, and resolved KPI tile 300× discrepancy between org-wide and top-10 cached values.

**Fixed (Security & Validation)**
XSS prevention in notifications and snapshot import via `textContent`; settings validation-before-commit; scope-dependent cache flush allow-list; `detailToMessage()` helper for FastAPI 422 arrays; scope map fail-open to `org` default; governance cache wipe prevention; `_ALIAS_RE` regex anchor fix.

**Fixed (Formatting & Data Quality)**
GiB/TiB binary label correction; `formatDataSize` 1024 GB threshold fix; `formatNumber(null)` safety guard; `total_bytes_processed` inclusion in discovery CTEs; strategy-aware re-sorting; max bytes billed clamp logging; fluid scaling zero lookback guard; storage savings fabrication prevention; unused `AIResult` fields removed.

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
