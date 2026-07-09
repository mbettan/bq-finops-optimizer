# 🚀 Release Notes: BigQuery FinOps Optimizer

---

## v1.1.1 — 2026-07-09

### 🐛 Bug Fixes

#### fix(fluid-scaling): `max_bytes_billed_gb` not sent to backend
*   **Root Cause:** The frontend's `fetchEstimate()` and `fetchJobSimulation()` functions were not including `max_bytes_billed_gb` in the POST body, even though the value was correctly stored in `state.maxBytesBilledGb`. This caused both `/api/fluid-scaling/estimate` and `/api/slots/fluid_simulation` to fall back to the backend's default 200 GiB safety cap, ignoring the user's configured limit.
*   **Fix:** Added `maxBytesBilledGb` to the call sites, function signatures, and request payloads of both `fetchEstimate()` and `fetchJobSimulation()` in `static/app.js` and `docs/static/app.js`.

#### fix(data-integrity): removed all hardcoded fallback data from backend
*   **Root Cause:** Multiple endpoints returned fabricated demo data instead of honest empty results:
    *   **Static Schema Audit** (`POST /api/storage/static_audit`): When the query returned empty results *or* threw an exception, the endpoint returned two hardcoded fake tables (`EDW_WCM_CONTACT.wcm_contact_matching_history`, `ODS_CORE_ECOM.ecom_order_item_ledger`) with fabricated row counts and byte sizes. Users with clean schema hygiene would see phantom risk items.
    *   **Active Assist** (`POST /api/storage/active_assist`): Same two fake tables were returned on empty results or exceptions. Additionally, real recommendations used a hardcoded `savings = 120.0` default, fabricated column suggestions (`CREATED_DATE`, `CUSTOMER_KEY`, `EVENT_TYPE_ID`), and an arbitrary `editions_monthly_savings = savings * 0.8` formula.
    *   **Dashboard stubs** (`/api/dashboard/kpis`, `/opportunities`, `/top-projects`, `/anomalies`): Returned hardcoded fake project names, dollar amounts, and anomaly alerts (e.g., `data-warehouse-prod $18,400/mo`, `analytics-pool idle 80%`, `etl@svc.gserviceaccount.com`).
*   **Fix:**
    *   Static Schema Audit and Active Assist exception/empty handlers now return `[]`.
    *   Active Assist savings fields (`on_demand_monthly_savings`, `editions_monthly_savings`) made `Optional[float] = None` — only populated when BigQuery provides real `cost_projection` data.
    *   Active Assist column suggestions now parsed from `additional_details` instead of hardcoded values.
    *   Dashboard KPI fields made `Optional` with `None` defaults; returns `KpiResponse(stub=True)` until real billing is implemented. Other dashboard stubs return `[]`.

### ✨ Improvements

#### refactor(active-assist): removed `focus_projects` parameter
*   **Rationale:** `INFORMATION_SCHEMA.RECOMMENDATIONS` is inherently scoped to the execution project by BigQuery — there is no cross-project recommendations view. Sending `focus_projects` had no effect. The parameter has been removed from the Active Assist frontend request payload to avoid confusion. Active Assist now cleanly operates at org/execution project scope.

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
