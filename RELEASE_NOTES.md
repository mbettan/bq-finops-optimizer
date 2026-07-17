# 🚀 Release Notes: BigQuery FinOps Optimizer

---

## v1.2.1 — 2026-07-17

This release focuses on optimizing AI Doctor performance and addressing edge cases related to system queries and missing DDL schemas.

### 🔑 Key Highlights

#### 1. AI Doctor UI Terminology Update
Replaced references to "Vertex AI" with "Agent Platform" in the frontend and linked to the official Gemini Enterprise Agent Platform documentation.

#### 2. AI Doctor Strict 0-DDL Frontend Handling
Improved empty-state clarity in the UI when the backend correctly filters out queries that solely interact with system tables or ML functions (like `INFORMATION_SCHEMA` or `AI.GENERATE_TEXT`) without physical DDL schemas.

#### 3. AI Doctor Project Scan Performance
Radically improved the secondary SQL extraction query speed (`JOBS_BY_PROJECT`) by enforcing `AND job_id IN UNNEST(@job_ids)` filtering, preventing it from performing full historical table scans across all project jobs.

#### 4. AI Doctor System View 403 Access Denied
Fixed a bug in the AI Doctor schema extraction engine where 3-part system queries (`region-us.INFORMATION_SCHEMA...`) tricked the table parser into treating the region as a project ID, resulting in 403 errors. `INFORMATION_SCHEMA` paths are now properly excluded from physical DDL lookups.


---

## v1.2.0 — 2026-07-13

This release consolidates multiple enhancements, security hardening updates, and critical bug fixes to ensure data correctness, cost safety compliance, and comprehensive org-wide auditing.

### 🔑 Key Highlights

#### 1. 🏗️ Organization-Level Schema Auditing
Engineered a dynamic discovery layer to seamlessly scan the entire GCP organization for missing partitions and clusters without hitting API limits.

#### 2. 🎯 Project Focus Scope Added
Introduced a global `focus_projects` parameter to scope analysis to specific GCP projects rather than the entire organization.

#### 3. 🛡️ Security & XSS Hardening
Resolved DOM-based XSS vulnerabilities and removed all hardcoded fallback demo data to ensure true empty states.

#### 4. 🐛 Org-Wide Bug Fixes
Fixed MV Cost Auditor phantom counts, Cost Attribution math, and corrected Slot Utilization concurrency to reflect true org-wide demand.

#### 5. 📊 Metadata in Job Analysis
The `analyze_jobs` response now includes sample metadata, enabling accurate explanations of sampling bias.

#### 6. 🔎 AI Doctor Enhancements
Parameterized AI Doctor job IDs to prevent string interpolation risks and improved the underlying prompt context.

#### 7. 🚀 Fluid Scaling Guardrails
Verified that user-configured safety caps (`max_bytes_billed_gb`) are strictly enforced across all fluid scaling backend API calls.

#### 8. 🧩 UI/UX Refinements
Promoted the Schema Optimizer to its own navigation group and added collapsible educational panels for power users.

### ✨ Features & Enhancements

*   **Organization-Level Schema Auditing:** Engineered a dynamic discovery and UNION ALL orchestration layer to seamlessly scan the entire GCP organization for missing partitions and clusters without querying empty projects or hitting API limits.
*   **Versioned Static Assets:** Static assets served with version query parameters now receive long-lived caching headers (`Cache-Control: public, max-age=31536000, immutable`), reducing repeat page-load latency.
*   **Metadata in Job Analysis:** The `analyze_jobs` response now includes sample metadata, enabling the frontend to accurately explain sampling bias (e.g., top N jobs by bytes billed).
*   **Logging & Observability:** `RotatingFileHandler` is now gated behind an `ENABLE_FILE_LOG` environment variable to optimize memory in Cloud Run environments.
*   **Refactors:** Centralized standard date constants (`DAYS_PER_MONTH`), eliminated redundant `.groupby` operations in fluid scaling, and parameterized job IDs for AI Doctor instead of using string interpolation.

### 🛡️ Security & Hardening

*   **XSS Vulnerability Fixes:** Resolved a DOM-based Stored XSS in the Slots Profiler caused by a sanitizer whitelist bypass, and a full-application XSS vulnerability via snapshot hydration bypass. All imported snapshots and table rendering functions now properly HTML-escape user data.
*   **Data Integrity & Honest Empty States:** Removed all hardcoded fallback demo data (including mock project names, dummy schema names, and fabricated dollar amounts) from backend endpoints. Empty queries now return true empty states.
*   **Focus Filter Safeguards:** Removed the `focus_projects` parameter from Capacity Planning and Active Assist endpoints where it previously distorted capacity totals or was ignored. Focus filtering remains fully active on 19 other org-level endpoints.
*   **Environment Variables:** Ensured `.env` loaders only fill in missing variables and no longer override Cloud Run/GKE injected credentials or configurations.
*   **Validation Guardrails:** 
    *   Added `_safe_ident()` validation to BQ-sourced project IDs before SQL interpolation.
    *   Bounded numeric parameters (e.g., capping `lookback_days` to 90) to prevent unbounded org-wide scans or division-by-zero crashes.

### 🐛 Bug Fixes

*   **MV Cost Auditor:** Fixed a bug where MV refresh patterns were silently missed across projects. The auditor now discovers all projects with MV activity and keys the inventory correctly by project, preventing phantom counts and accurately measuring org-wide MV costs.
*   **Slot Utilization Concurrency:** Changed the aggregation method to sum across all concurrent jobs before taking quantiles. Capacity metrics now correctly reflect true org-wide concurrent demand rather than the single largest job's peak.
*   **Cost Attribution & Waste Rules:** 
    *   Rejected `focus_projects` filters in cost attribution, which previously corrupted waste allocation math.
    *   Fixed a bug where waste silently vanished under Rule B (Central Dump) if the central project wasn't configured.
    *   Corrected the exclusion of `NULL` statement_type rows that artificially inflated wasted direct usage.
*   **BI Engine & On-Demand Costs:** BI Engine savings estimates are now only applied to on-demand jobs (not Editions). On-Demand cost simulations no longer erroneously include failed queries that aren't billed by BigQuery.
*   **Data Skew & DML Auditor:** Skew analysis now filters out SCRIPT parent/child jobs to avoid double-counting stages. DML auditor now requires `state = 'DONE'` to prevent in-flight jobs from inflating wasted slot hours.
*   **Fluid Scaling Caps:** Verified that the user-configured `max_bytes_billed_gb` safety cap is consistently forwarded to and enforced by all fluid scaling BigQuery API calls.
*   **HBO Validation:** Projects failing access verification now report as `unknown` status rather than false positives for being enabled.
*   **Time-Travel DDL:** Invalid combinations of `time_travel_rescale` (without setting hours) or non-integer float hours are now strictly rejected to prevent generating unusable BigQuery DDL.
*   **Static Schema Estimates:** Replaced a fabricated row count estimate (based on byte size) with the actual `total_rows` metric from `INFORMATION_SCHEMA.TABLE_STORAGE`.
*   **Exceptions & File Paths:** Addressed silent failures in schema and active assist audits that previously returned empty arrays on 403/404 errors. Corrected `cost_attribution_config.json` relative path resolution for Docker compatibility.

---

## v1.1.0 — 2026-07-08

We are proud to announce the release of **v1.1.0** of the **BigQuery FinOps Optimizer**—an enterprise-grade diagnostic, simulation, and governance suite designed to maximize cost efficiency and eliminate compute waste across Google Cloud BigQuery environments. 

This release introduces dynamic budget safety guardrails, advanced telemetry models, proactive migration guardrails, and complete codebase sanitization to support secure, open-source distribution.

### 🔑 Key Highlights

#### 1. 🛡️ Dynamic Billing Limits & Cost Safety Caps
Introduced a dynamic `max_bytes_billed_gb` safety cap passed to every query execution to prevent runaway charges.

#### 2. 📈 The "Slot Capacity Bucket Method"
Built a high-performance simulation engine to model slot consumption telemetry and generate tiered capacity recommendations.

#### 3. 🤖 AI-Powered Semantic Query Review
Leverages Gemini 3.1 Flash Lite to run high-speed SQL reviews and generate actionable `SET @@reservation` routing snippets.

#### 4. 🔗 Hybrid Cost Attribution Engine
Solves SKU blending resolution for Editions and redistributes unallocated idle capacity waste back to active projects.

#### 5. ⏱️ Fluid Scaling & Cooldown Tax Mitigation
Identifies reservations suffering from the legacy autoscaler's 60-second minimum charge and models the financial impact of transitioning to Fluid Scaling.

#### 6. 🏗️ Proactive Migration Guardrails
Audits the metadata catalog to identify high-risk, unclustered, or unpartitioned tables before they are queried, suggesting optimal clustering columns.

#### 7. 🔒 Sanitization & Repository Hygiene
Conducted a rigorous sweep of all source files and mock data to remove traces of proprietary company identifiers and sensitive paths.

#### 8. 🎯 Project-Scoped Analysis
Added `focus_projects` parameter allowing users to optionally scope org-wide analysis down to a specific subset of up to 50 GCP projects.

### ✨ Low-Level Details

*   **Dynamic Billing Limits & Cost Safety Caps:** Introduced a project-wide `max_bytes_billed_gb` safety cap parameter, dynamically passed to every BigQuery query execution. Includes smart clamping, fallbacks, and local storage state persistence.
*   **The "Slot Capacity Bucket Method" (Compute Capacity Simulation):** Built a high-performance simulation engine using NumPy to model slot consumption telemetry. Evaluates per-minute timelines to generate Tiered Recommendations (Aggressive p80, Balanced p95, Performance Max).
*   **Fluid Scaling & Cooldown Tax Mitigation:** Identifies reservations suffering from the legacy autoscaler's 60-second minimum charge and models the financial impact of transitioning to Fluid Scaling.
*   **Hybrid Cost Attribution Engine:** Solves SKU blending resolution for BigQuery Editions, proportionally redistributes unallocated idle capacity waste, and supports custom attribution rules (Lender vs. Borrower Pays).
*   **AI-Powered Semantic Query Review (AI Doctor):** Leverages BigQuery's native `AI.GENERATE` scalar function to run high-speed SQL reviews using **Gemini 3.1 Flash Lite**, generating actionable `SET @@reservation` routing snippets for expensive queries.
*   **Proactive Migration Guardrails (Static Schema Auditor):** Audits the metadata catalog to identify high-risk, unclustered, or unpartitioned tables before they are queried, suggesting optimal clustering columns and generating remediation DDL.
*   **Sanitization & Repository Hygiene:** Conducted a rigorous sweep of all source files, documentation, and mock data to remove all traces of proprietary company identifiers. Verified zero credentials leak and restructured `.gitignore` for strict privacy.
*   **Project-Scoped Analysis (Focus Projects):** Added `focus_projects` parameter allowing users to scope org-wide analysis down to a specific subset of up to 50 GCP projects.

### ✨ Core FinOps Modules (Introduced in v1.0.0)

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

### 🐛 Additional Fixes & Improvements

*   **AI Doctor Discovery:** Fixed a bug where the discovery query was incorrectly scoped to the admin project instead of the organization, failing to scan the most expensive queries org-wide.
*   **GCP Validation & State Management:** Implemented strict project ID regex validation on global settings. Fixed stale cached results persisting in the UI after scope changes.
*   **Structured Observability:** Introduced a 4-icon logging sequence (`▶`, `⏳`, `✅`, `◼`) with request correlation IDs, clickable BigQuery Console URLs, and strict environment-based `LOG_LEVEL` controls.
*   **AI Performance enhancements:** Increased `max_output_tokens` and reduced `thinking_level` to resolve silent `NULL` returns for complex queries in `AI.GENERATE`. Summarized DDL payloads to reduce token overhead.
*   **UX Enhancements:** Added an "About" panel with live versioning, improved the Active Assist module with "Blind Spot" educational cards, grouped Schema optimization logically in the sidebar, and resolved tooltip flashing issues.

---

## 🛠️ Tech Stack & Architecture

*   **API Framework:** FastAPI (centralized versioning, GZip compression middleware, automated interactive `/docs` landing page).
*   **Data Science Core:** NumPy, Pandas, DB-Types.
*   **Frontend UI:** Vanilla ES6 JavaScript, HTML5, Custom Glassmorphic CSS Engine.
*   **Testing Suite:** 167 comprehensive unit and integration tests (mocked BigQuery endpoints, Pydantic input boundary validation, and SQL injection security checks).
