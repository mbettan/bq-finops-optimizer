# Release Notes — BigQuery FinOps Optimizer

This page documents production updates to the BigQuery FinOps Optimizer. We recommend that users periodically check this list for any new announcements.

For architecture details and tech stack information, see the [README](README.md).

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
