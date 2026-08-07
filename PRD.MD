# Product requirements document (PRD): BigQuery FinOps Optimizer

## 1. Product overview

**Name:** BigQuery FinOps Optimizer  
**Description:** An enterprise-grade diagnostic and simulation suite designed to analyze and optimize GCP data warehousing costs. Featuring core engines for compute capacity simulation, the platform automates cost attribution and SQL anti-pattern detection—enabling enterprise clients to rapidly evaluate financial scenarios and eliminate inefficient workloads.  
**Purpose:** The tool provides a user-friendly interface to configure parameters, execute analytical cost queries against GCP BigQuery, visualize the results, and generate/execute actionable insights (such as altering dataset storage billing models, isolating compute workloads, and right-sizing Editions baselines vs. autoscaling based on configurable
risk profiles).

## 1.1 Problem statement: cost attribution challenge
Currently, the native GCP Billing export cannot accurately attribute BigQuery Editions costs to individual querying projects due to:
*   **SKU Blending:** Blending costs of different Editions and commitment types across all projects.
*   **Admin Cost Dumping:** All unused baseline slots and commitment waste are dumped into the central administration project.
*   **Billing Account Boundaries:** Native attribution fails to track costs if the query project and the admin project reside in different linked GCP Billing Accounts.

## 1.2 Context on History-Based Optimization (HBO)
*   **GA Status (July 2025):** HBO is globally deployed and enabled by default. It cannot be disabled by users.
*   **The Challenge:** Because users cannot disable HBO for A/B testing, this application must use historical telemetry to match optimized queries with their unoptimized predecessors (via `normalized_literals`) to prove ROI.
*   **Memory Limit:** Optimized plans expire after 130 days if the query is not run.
*   **Disambiguation:** This tool focuses on BigQuery History-Based Optimization (query execution tuning) and does not cover BigQuery ML Hyperparameter Optimization.

## 1.3 Context on Fluid Scaling
*   **Overview:** BigQuery Fluid Scaling is a "no minimum," area-under-the-curve billing model for the BigQuery Autoscaler that eliminates the traditional one-minute cooldown window, offering true per-second billing.
*   **The Problem (Cooldown Tax):** In the legacy model, customers were billed for 60 seconds of capacity even if a query only required 5 seconds of burst compute. This led to crowded reservations and sacrificed workload isolation.
*   **The Solution:** Zero-minimum billing allows for effortless workload isolation (creating hundreds of isolated reservations without penalty) and predictable FinOps (clear job-level cost attribution).
*   **Implementation:** Activation is managed via DML at the reservation level. Slot capacity continues to scale in increments of 50 slots.

## 1.4 Goals and non-goals

### Goals
*   **Provide actionable FinOps recommendations:** Automatically identify and quantify cost-saving opportunities across compute and storage.
*   **Enable precise cost attribution:** Overcome native GCP billing limitations to attribute Editions compute costs down to the exact querying project.
*   **Safe Execution:** Provide UI-driven execution for DDL changes (e.g., storage billing models) while keeping the user fully in control.

### Non-goals (Out of scope)
*   **Fully Autonomous Remediation:** The platform is a diagnostic and recommendation tool. It will **not** automatically execute destructive or structural changes (like altering reservations or dropping tables) without explicit user intervention.
*   **BigQuery ML Tuning:** This tool focuses on FinOps and SQL execution tuning. It explicitly does **not** cover BigQuery ML Hyperparameter Optimization.
*   **Multi-Cloud FinOps:** The scope is strictly limited to Google Cloud BigQuery.


## 2. Target audience

- **FinOps Practitioners:** Looking to identify cost-saving opportunities across GCP organizations, evaluate active capacity commitments, and balance workload performance SLAs against cost constraints.
- **Data Engineers / Cloud Admins:** Needing to apply DDL changes to switch storage billing models, isolate on-demand vs. reservation projects, and precisely adjust BigQuery Editions autoscale settings.

## 3. User stories

- _As a FinOps user_, I want to input my custom region and pricing parameters so that the calculations accurately reflect my negotiated GCP rates.
- _As a Cloud Admin_, I want to identify which datasets would be cheaper on a physical storage billing model versus a logical one, so I can switch them and save money.
- _As a Data Engineer_, I want the tool to generate the exact `ALTER SCHEMA` DDL statements for storage optimization, and I want a button to execute them directly from the UI.
- _As a FinOps user_, I want to compare on-demand vs. reservation costs over a specific time window to determine which projects should be isolated from slot reservations.
- _As a Cloud Admin_, I want to view a timeline of my actual slot usage compared to my Editions reservation baselines and autoscale maximums across the entire organization.
- _As a FinOps user_, I want to compare different baseline tier recommendations (Aggressive Savings, Balanced, Performance) based on historical percentiles, so I can align my capacity purchases with the specific risk profile of each workload.
- _As a FinOps user_, I want to analyze job-level costs to identify workloads that are currently running on BigQuery Editions but would be more cost-effective on On-Demand billing, allowing me to eliminate wasted capacity and visualize the savings in a data table.
- _As a FinOps Analyst_, I want to accurately attribute BigQuery Editions costs to individual querying projects, overcoming SKU blending, so that I can hold departments accountable for their actual usage.
- _As a Cloud Billing Administrator_, I want to proportionally distribute unallocated waste (unused baseline slots) back to the projects sharing a reservation, eliminating the "admin dump" mystery.
- _As a Department Head_, I want to see a dashboard showing my department's direct usage costs vs overhead costs, so I can better manage my cloud budget.
- _As a Cloud FinOps Administrator (or BigQuery capacity planner)_, I want to accurately calculate the exact number of BigQuery slot-hours consumed over a specific time period, broken down by autoscaled slots and uncommitted baseline slots, so that I can accurately forecast my BigQuery Enterprise billing, understand my compute overages, and optimize my capacity commitments to save money.
- _As a FinOps Analyst_, I want to compare an optimized query with its earliest unoptimized execution to prove cost savings, since I cannot disable the feature for A/B testing.
- _As a BigQuery Admin_, I want to be alerted to optimized queries that have not run in 100+ days, so I can trigger a "warm-up" run before the 130-day memory expires.
- _As a skeptical user_, I want to run a demo query in a sandbox and watch execution time drop as HBO applies, to validate the feature.

---

## 4. Functional requirements

### 4.1 Global configuration & authentication

- **GCP Authentication:** The application backend will use Application Default Credentials (ADC). When deployed on Cloud Run, it will inherit the attached Service Account's permissions.
- **Global Parameters:** The UI must include a settings panel to define:
  - `Target Organization Project ID`: The central project used to query `INFORMATION_SCHEMA` (e.g., billing export or admin project).
  - `Region`: e.g., `region-us`, `region-eu`, `region-us-east4`.
  - `Focus Projects` (Optional): A comma-separated list of up to 50 specific project IDs to focus the optimization queries on.

### 4.2 Module 1: Storage Cost Optimizer

- **Parameters Input:** Active/Long-term Logical and Physical GiB Prices, Time Travel Rescale, Min Monthly Savings constraints.
- **Execution:** Executes storage metrics queries and processes data in Python.
- **Visualization:** Data table with Project/Dataset Name, Current/Recommended Billing Model, Monthly Savings, and generated DDL.
- **Implementation/Action:** An "Apply Optimization" button to execute `ALTER SCHEMA` DDL.

### 4.3 Module 2: On-Demand vs. Editions Job Analyzer

- **Parameters Input:** On-Demand Price (per TiB), Editions Slot Price, Analysis Time Window (Lookback Days).
- **Execution:** Executes job-level pricing analysis queries to compare the cost of running jobs on Editions (via slot consumption) versus On-Demand (via bytes processed).
- **Visualization:** Data table with Job ID / Project / User, Current/Recommended Billing Model (On-Demand vs. Editions), Estimated Waste on Editions (Savings), and recommended action. This visualization must be consistent with the Storage Cost Optimizer UI.
- **Implementation/Action:** Flag or export jobs/projects that are candidates for being moved to On-Demand billing to eliminate waste.

### 4.4 Module 3: Cost Attribution Engine

- **Objective:** Accurately calculate and attribute BigQuery Editions compute costs down to the GCP Project level, overcoming native limitations of the standard GCP Billing console.
- **Parameters Input:** Billing Month Start/End, Execution Project ID, Admin Project ID, Region.
- **Configurations:**
    - Project-to-Reservation Mapping.
    - Unallocated Waste Rules (Proportional vs Central Dump).
    - Idle Slot Borrowing Rules (Lender Pays vs Borrower Pays).
- **Execution:** 
    - Queries `INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION` (or `JOBS`) to pull raw slot usage grouped by project and reservation.
    - Applies the Hybrid Cost Attribution Model to calculate direct costs and allocate waste.
- **Visualization:** 
    - KPI cards for total provisioned hours.
    - Donut chart for provisioning ratio.
    - Data table showing Project ID, Reservation ID, Direct Usage Cost, Allocated Waste, and Total Cost Attribution.

### 4.5 Module 4: Actual Provisioning & Capacity Planning

- **User Story:** As a Cloud FinOps Administrator (or BigQuery capacity planner), I want to accurately calculate the exact number of BigQuery slot-hours consumed over a specific time period, broken down by autoscaled slots and uncommitted baseline slots, so that I can accurately forecast my BigQuery Enterprise billing, understand my compute overages, and optimize my capacity commitments to save money.
- **Problem Statement:** With BigQuery Enterprise editions, slot capacity is highly dynamic. Customers purchase fixed Capacity Commitments, but they also configure Reservations that have baseline slots and autoscaling limits. Because autoscaling scales up and down continuously based on workload, and capacity commitments can be created or updated at any time, it is incredibly difficult to calculate actual usage. You cannot just look at a static number; you must calculate the exact "area under the curve" (Time × Slots) down to the second. Currently, customers do not have a simple, single metric or dashboard out-of-the-box that tells them:
    - How many hours of compute were spent on unpredictable autoscaling.
    - How many hours of compute were spent on uncommitted baseline slots (baseline slots configured in a reservation that exceeded the pre-purchased capacity commitment, resulting in higher pay-as-you-go costs).
- **Acceptance Criteria:**
    - Provide a way (via this application's dashboard) to easily view `autoscaled_slot_hours` and `baseline_slot_hours`.
    - The calculation must accurately subtract pre-purchased `capacity_slots` from `baseline_slots` so customers only see the billable uncommitted baseline hours.
    - The solution must account for exact timestamp intervals (`end_timestamp - change_timestamp`) to ensure no over/under-billing of fractional hours.

### 4.6 Module 5: Workload Profiler

- **User Story:** As a FinOps Analyst, I want to identify specific queries that are causing inefficient autoscaler scale-ups, so that I can optimize reservation strategies or move workloads to on-demand pricing.
- **Problem Statement:** Small, high-frequency queries (e.g., short transactions or heartbeats) can trigger BigQuery's 1-minute minimum charge for autoscaled slots, leading to high waste when spaced out.
- **Acceptance Criteria:**
    - Identify reservations with high query frequency and low average slot usage.
    - Display a heatmap of activity (Hour vs Day of Week).
    - List the top 100 query patterns ranked by estimated savings impact (dollars saved under Fluid Scaling vs. legacy autoscaler), collapsing nested script child jobs and randomized job IDs, and flagging collapse-ratio leaks.

### 4.7 Module 6: History-Based Optimization (HBO) Insight `[STATUS: IMPLEMENTED]`

- **Objective:** Track, measure, and quantify performance improvements and cost savings generated by HBO, and ensure it is enabled across all projects.
- **Acceptance Criteria:**
    - **Status Check & Action:** Make sure HBO is enabled on all projects in the org. If not, provide the DDL to enable it (`ALTER PROJECT PROJECT_NAME SET OPTIONS (\`region-LOCATION.default_query_optimizer_options\` = 'adaptive=on');`).
    - **Historical A/B Matching:** Identify project queries with the greatest estimated improvement to execution time by comparing optimized runs with the most recent unoptimized run (using `normalized_literals`).
    - **Memory Expiration Tracker:** Identify queries that are nearing the 130-day limit without being run, suggesting a "warm-up" run.
    - **Interactive Sandbox:** Provide a built-in demo using public data (e.g., Wikipedia) to show the "optimization flywheel" in action (requires disabling cache).
- **Visualization:** Data table comparing `new_elapsed_ms` and `original_elapsed_ms` of optimized vs. unoptimized runs, and a list of projects needing HBO enablement.

### 4.8 Module 7: Storage Hygiene & Time Travel Auditor `[STATUS: IMPLEMENTED]`

- **Objective:** Identify tables with high churn and large Time Travel storage overhead, suggesting a reduction in the Time Travel window.
- **Acceptance Criteria:**
    - Scan `INFORMATION_SCHEMA.TABLE_STORAGE` to find tables where `time_travel_physical_bytes` exceeds `active_logical_bytes` (indicating drop/recreate or high churn).
    - Flag datasets for potential reduction of Time Travel window from 7 to 2 days.
    - Flag tables frequently overwritten in datasets using the physical storage billing model.
    - Resolve each dataset's configured `default_time_travel_days` from `INFORMATION_SCHEMA.SCHEMATA_OPTIONS` and display it beside the time-travel bytes, so the operator can see whether the window has already been reduced before recommending a change. `SCHEMATA_OPTIONS` is region-qualified per project, so the lookup batches all in-scope projects into one `UNION ALL` (capped at 50, with a per-project retry fallback); any dataset that cannot be read falls back to BigQuery's 7-day default rather than failing the scan.
- **Visualization:** Data table showing Dataset, Table, Live Size, Time Travel Size, and current Time Travel TTL with a health status.

### 4.9 Module 8: Architectural Anti-Pattern Detection `[STATUS: IMPLEMENTED]`

- **Objective:** Detect inefficient ingestion APIs, DML abuse, and Materialized View refresh costs.
- **Acceptance Criteria:**
    - **DML Abuse Tracker:** Scan `INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION` for high-frequency single-row `INSERT` statements. Aggregation is keyed on the **destination table** (not the user alone), since the table is the unit that hits BigQuery's 1500 table-modifying-operations-per-day ceiling and the unit that gets migrated to the Storage Write API. Reports Active Days and Avg Inserts / Day so a sustained pipeline is distinguishable from a single-day backfill. Default threshold is 100 operations per (destination table, user, project).
    - **Materialized Views:** Detect high refresh costs (Moved to Module 10).
- **Visualization:** List of flagged destination tables with owning user/project, insert count, active days, average inserts per day, wasted slot hours, and estimated waste.

### 4.10 Module 9: BI Engine ROI & Optimization Doctor `[STATUS: IMPLEMENTED]`

- **Objective:** Quantify compute cost avoided by BI Engine and diagnose cache misses.
- **Acceptance Criteria:**
    - Calculate estimated dollars saved by BI Engine (Bytes Processed - Bytes Billed).
    - Identify queries with `bi_engine_mode` != 'FULL' and extract failure reasons (using `code` field).
- **Visualization:** KPI for total savings, and a table of top queries that failed to use BI Engine with reasons.

### 4.11 Module 10: Materialized View FinOps Auditor `[STATUS: IMPLEMENTED]`

- **Objective:** Govern MV limits and detect expensive auto-refreshes.
- **Acceptance Criteria:**
    - Detect "Zombie MVs" where refresh cost exceeds query savings.
- **Visualization:** Table of high-cost MVs.

### 4.12 Module 11: Query Anti-Pattern Linter `[STATUS: IMPLEMENTED]`

- **Objective:** Scan historical queries to detect expensive anti-patterns like `SELECT *`.
- **Acceptance Criteria:**
    - Scan `INFORMATION_SCHEMA.JOBS_BY_PROJECT` (looping through active projects) for expensive jobs (>100GB).
    - Use regex to flag `SELECT *` abuse.
    - Flag queries using `LIMIT` on non-clustered tables for exploration, suggesting free preview options instead.
    - Filter findings by identity type (human vs. service account, inferred from the `gserviceaccount.com` domain), user, project, and anti-pattern type. Filter option lists are rebuilt from the current result set on each scan; a selection that no longer exists in the new data is cleared rather than left silently applied.
    - Offer a **Summary** view that aggregates the per-job rows by (user, project, anti-pattern) into query count, cumulative data billed, and total estimated waste — converting a list of individual offences into a ranked list of owners to contact.
- **Visualization:** "SQL Wall of Shame" listing offending users and queries, with a Detail/Summary toggle and a filter toolbar.

### 4.13 Module 12: Schema & Storage Governance Auditor `[STATUS: PARTIALLY IMPLEMENTED]`

- **Objective:** Audit datasets for cost-control settings like partition filters and expiration rules.
- **Acceptance Criteria:**
    - **Dataset Expiration:** Identify datasets missing `default_table_expiration_days` (using `SCHEMATA` left join `SCHEMATA_OPTIONS`).
    - **Partition Filter:** Identify partitioned tables missing the `require_partition_filter` setting (using Python API loop for top 5 datasets to bypass missing column limits).
- **Visualization:** Checklist of high-risk tables and datasets.

### 4.14 Module 13: Cost Control & Quota Watchdog `[STATUS: IMPLEMENTED]`

- **Objective:** Enforce cost safety guardrails across all diagnostic and analytical query executions to protect cloud budgets.
- **Acceptance Criteria:**
  - Enforce a customizable `max_bytes_billed_gb` safety cap parameter across all application BigQuery query executions.
  - Automatically translate the user setting into BigQuery's `QueryJobConfig.maximum_bytes_billed` (converting GiB to bytes) before submitting query jobs.
  - Implement robust clamping boundaries (minimum of 1 GiB, maximum of 10 TiB, and a safe default fallback of 200 GiB if unset or zero).
  - Validate the input dynamically using Pydantic fields across all request payloads and persist the user's preference in the client's local storage.
- **Visualization:** An adjustable safety input field in the global settings panel displaying the active query cost cap.

### 4.15 Module 14: Observability & Request Tracing `[STATUS: IMPLEMENTED]`

- **Objective:** Provide comprehensive, structured observability for all API endpoints and BigQuery query executions, enabling operators to monitor system health, trace individual requests, and debug query performance in real time.
- **Acceptance Criteria:**
  - **4-Icon Log Sequence:** Every API request must produce a consistent log flow: `[▶]` (endpoint start) → `[⏳]` (query submitted) → `[✅]` (query completed with stats) → `[◼]` (endpoint completed with elapsed time).
  - **Request Correlation IDs:** Every log line must include an 8-character hex request ID (e.g., `[a3f1b2c4]`) automatically injected via middleware. This enables tracing a single request's full lifecycle even when logs from concurrent requests are interleaved.
  - **BQ Console URLs:** Every query completion log must include a clickable URL to the BigQuery Console job results page.
  - **SQL at DEBUG Only:** Full SQL text must be logged at `DEBUG` level only — never at `INFO` — to prevent schema and PII leakage in production logs.
  - **Safety Cap Visibility:** The active `maximum_bytes_billed` safety cap must be visible in both the endpoint start log and every query submission log.
  - **Centralized Query Helpers:** All BigQuery query executions must be routed through instrumented helpers that enforce the safety cap and emit the `⏳`/`✅` log sequence. Zero bare `client.query()` calls may exist outside these helpers.
  - **Configurable Log Level:** The log level must be controllable via the `LOG_LEVEL` environment variable (default: `INFO`, set `DEBUG` to see full SQL).
- **Implementation:**
  - `request_id_var` (`contextvars.ContextVar`) in `src/utils.py` — stores the per-request correlation ID.
  - `RequestIdFilter` (`logging.Filter`) in `src/utils.py` — injects `%(request_id)s` into every `LogRecord` automatically.
  - `inject_request_id` middleware in `src/main.py` — generates a unique 8-char hex ID per HTTP request.
  - Query helpers: `run_query_and_log()`, `run_query_to_df()` in `main.py`; `_run_and_log()` in `hbo.py`, `cost_attribution.py`, `fluid_scaling.py`.

### 4.16 Module 15: Interactive vs. Batch Optimizer `[STATUS: IMPLEMENTED]`

- **Objective:** Protect interactive slot capacity by assigning each *workload* — not each individual job — the correct BigQuery execution priority. BigQuery caps a project at 100 concurrent `INTERACTIVE` queries (and 2,000 concurrent slots on On-Demand); automated pipelines that default to `INTERACTIVE` consume that headroom and starve live dashboards. `BATCH` and `INTERACTIVE` share identical hardware, slot performance, and per-byte/slot-hour pricing, so the recommendation is cost-neutral by construction.
- **Workload Aggregation:** Job executions are grouped into logical workloads via lineage labels (`dbt_model` / `dbt_node` / `dbt`, `airflow_dag_id` / `dag_id` / `composer`, `dataform` / `dataform_workspace`, `looker` / `tableau` / `dashboard_id`, `requestor`), the `scheduled_query_` job-ID prefix, and finally `user_email`. Label keys are matched case-insensitively with `-`/`_` normalized.
- **Acceptance Criteria:**
    - Scan `INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION`, falling back to `JOBS_BY_PROJECT` via a dry-run permission probe when org-level IAM is unavailable.
    - Exclude cache hits, script child jobs (`parent_job_id IS NULL`), and failed jobs so run counts reflect real executions.
    - Classify each workload as:
        - **`UNDER_BATCHED`** — a labeled pipeline running `INTERACTIVE`; a service account running heavy DML (p50 ≥ 10 slots and ≥ 10 min slot-ms on `MERGE`/`INSERT`/`UPDATE`/`DELETE`/`TRUNCATE_TABLE`/`CREATE_TABLE_AS_SELECT`); or any service account majority-`INTERACTIVE` above 1 slot-hour.
        - **`OVER_BATCHED`** — a human or BI connection with p95 `BATCH` queue delay > 30s.
    - Grade confidence `HIGH`/`LOW` from the share of runs carrying lineage labels, and rank by an impact score (interactive slot-hours at risk, or human wait-hours).
- **Visualization:** Workload table with detection-reason tooltips, confidence badges, and a remediation modal emitting tooling-specific snippets — dbt `profiles.yml`, Airflow `BigQueryInsertJobOperator`, Python SDK / `bq` CLI. Dataform and Scheduled Queries expose no priority flag and surface migration guidance instead.

### 4.17 Module 16: Execution Plan & Data Skew Analyzer `[STATUS: IMPLEMENTED]`

- **Objective:** Parse execution plans to detect data skew in joins/partitions.
- **Acceptance Criteria:**
    - Scan `INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION` (using native array fields on `job_stages`) for jobs where max node compute time is >10x the average.
- **Visualization:** List of skewed jobs with ratio and wasted slot hours.

### 4.18 Module 17: AI Doctor & BigQuery Migration Optimization Engine `[STATUS: IMPLEMENTED]`

- **Objective:** Provide automated, enterprise-grade SQL query review, FinOps ROI prioritization, and performance optimization by combining Gemini LLM semantic advice with Google Cloud's official **BigQuery Migration Service API v2** (`bigquerymigration.googleapis.com`).
- **Multi-Strategy Query Selection & FinOps Prioritization Engine:**
    - **Candidate Aggregation by Query Key:** Groups query executions across `JOBS_BY_ORGANIZATION` over 7–90 day lookback windows using `COALESCE(query_info.query_hashes.normalized_literals, CONCAT('job:', job_id))` to aggregate cumulative organizational waste.
    - **5 Discovery Strategy Modes (`discovery_strategy`):**
        1. *Balanced ROI Score (`composite`):* Ranks candidate query templates using a multi-factor score: $0.40 \cdot \log_{10}(GB + 1) + 0.30 \cdot \log_{10}(Execs + 1) + 0.20 \cdot \log_{10}(Slots + 1) + 0.10 \cdot SpillFlag$.
        2. *Cumulative Cost (`cumulative_cost`):* Ranks query templates by aggregate bytes billed across all executions.
        3. *High Frequency (`execution_frequency`):* Identifies high-frequency BI dashboard micro-offenders (`HAVING COUNT(*) > 1`), highlighting execution counts in the UI.
        4. *Memory RAM Spill (`memory_spill`):* Surfaces memory-intensive queries that spill intermediate shuffle bytes to disk (`HAVING bytes_spilled > 0`).
        5. *Total Slot Time (`slot_ms`):* Focuses on heavy slot-consuming query templates.
    - **Multi-Stage Shuffle Spill Unnesting:** Unnests `job_stages` via `(SELECT SUM(s.shuffle_output_bytes_spilled) FROM UNNEST(job_stages) s)` to aggregate intermediate RAM-to-disk spills across all execution stages.
    - **Deterministic Worst-Job Sampling:** Uses `ARRAY_AGG(job_meta ORDER BY total_slot_ms DESC LIMIT 1)[OFFSET(0)]` to atomically isolate the single worst execution instance and its exact metadata payload for AI auditing.
    - **Script Execution Filtering:** Filters out procedural header jobs (`statement_type != 'SCRIPT'`) to eliminate double-counting.
    - **Strict IAM 403 Authorization Guardrails:** Requires `roles/bigquery.resourceViewer` or `roles/bigquery.admin` for organization discovery. Intercepts IAM 403 `Forbidden` exceptions and returns a clean `HTTP 403 Access Denied` response with explicit role guidance, rejecting silent/degraded project-level fallbacks.
- **Architecture & Workflow:**
    - **BigQuery Migration Service API v2 Core:** Leverages native `v2` migration workflows with `BigQuery2BigQuery_Translation` task types to rewrite inefficient BigQuery SQL into high-performance, cost-optimized standard SQL.
    - **Automated Schema Resolution (`SchemaFetcher`):** Automatically discovers referenced tables via BigQuery dry-runs and queries `INFORMATION_SCHEMA.TABLES` to fetch table DDL definitions (base tables, external tables, views), prepending them to the source payload for complete schema context.
    - **Autonomous 2-Stage Discovery & Opt-in YAML Pipeline:**
        1. *Stage 1 (Discovery Pass):* Executes a diagnostic workflow pass without a configuration file to inspect structural query opportunities and capture diagnostic report log messages (`reportLogMessages`).
        2. *Stage 2 (Synthesis Pass):* Analyzes query constructs and API feedback to dynamically synthesize a targeted `optimizer.config.yaml` specifying `type: optimizer` and transformation rules (`REWRITE_CTE_TO_TEMP_TABLE`, `DROP_TEMP_TABLE`, `PRECOMPUTE_INDEPENDENT_SUBSELECTS`, `REGEXP_CONTAINS_TO_LIKE`, `ADD_DISTINCT_TO_SUBQUERY_IN_SET_COMPARISON`).
        3. *Stage 3 (Opt-in Optimization & Dry-Run Pass):* Re-runs the workflow WITH the synthesized `optimizer.config.yaml`, extracts target SQL literals, strips injected schema DDL (preserving multi-statement temp-table scripts), and executes BigQuery dry-run validation to compute byte savings percentage (`bytes_delta_pct`) and cost reduction.
- **Safety & FinOps Guardrails:**
    - **Guaranteed Resource Cleanup:** Uses `try ... finally` execution blocks to guarantee workflow deletion (`client.delete(workflow_name)`) on all exit paths (success, error, exception, or timeout).
    - **Dry-Run Validation Gate:** Required translated SQL to pass BigQuery dry-run validation. If the translated query fails dry-run syntax/schema validation, the engine surfaces `success: False` with explicit error details.
    - **SQL Injection Prevention:** Enforces strict identifier validation (`IDENT_RE = re.compile(r"^[A-Za-z0-9_-]+$")`) before interpolating table and dataset names into `INFORMATION_SCHEMA` queries.
    - **Timeout & Payload Clamping:** Clamps execution timeouts to `[10s, 300s]` and caps maximum input query size to 1 MB to prevent server thread pool exhaustion.
- **Visualization & UI:**
    - "AI Doctor" tab displaying Discovery Priority mode selector, job telemetry (slot-ms, user email, original query, execution count runs badge), Gemini LLM advice, synthesized `optimizer.config.yaml` rules, optimized SQL syntax, and one-click Dry-Run byte reduction validation.

### 4.19 Module 18: Active Assist Recommendations `[STATUS: IMPLEMENTED]`

- **Objective:** Surface native Google Cloud recommendations for partitioning and clustering.
- **Acceptance Criteria:**
    - **Org-Wide Scan (Default):** When no `focus_projects` are specified, query the org-scoped `INFORMATION_SCHEMA.RECOMMENDATIONS_BY_ORGANIZATION` view directly in a single query. This avoids the need for a project-discovery step and eliminates per-project UNION ALL assembly. Only `ACTIVE` recommendations are returned.
    - **Focus Mode:** When `focus_projects` are specified, query each project's `INFORMATION_SCHEMA.RECOMMENDATIONS` view individually via `UNION ALL`.
    - Filter on `recommender = 'google.bigquery.table.PartitionClusterRecommender'` in both modes.
    - Parse `target_resources` to extract dataset and table identifiers, `description` to classify recommendation type (Partition vs Cluster), `primary_impact` for cost projections, and `additional_details.overview` for column suggestions and estimated savings (bytes and slot-ms).
    - Provide a one-click interface for users to review Google's native table tuning suggestions.
- **Visualization:** Interactive data table showing missing partitions/clusters with a "Blind Spot" methodology card indicating what Active Assist misses.

### 4.20 Module 19: Top Spenders (User-Level Cost Attribution) `[STATUS: IMPLEMENTED]`

- **Objective:** Identify top users consuming slots and bytes across the organization.
- **Acceptance Criteria:**
    - Aggregate `total_slot_ms` and `total_bytes_billed` grouping by `user_email`.
- **Visualization:** Leaderboard or data table ranking users by their financial footprint.

### 4.21 Module 20: Dashboard KPIs & FinOps Summary `[STATUS: IMPLEMENTED]`

- **Objective:** Provide a high-level executive summary of FinOps health.
- **Acceptance Criteria:**
    - Calculate aggregate metrics across all underlying modules (e.g., total storage savings, total compute waste, HBO impact).
- **Visualization:** Global KPI cards on the main dashboard view providing a one-glance summary of potential savings.

### 4.22 Module 21: Proactive Resource Warning Scanner `[STATUS: IMPLEMENTED]`

- **Objective:** Catch queries approaching complexity or resource limits before they fail.
- **Acceptance Criteria:**
    - Query `query_info.resource_warning` in `INFORMATION_SCHEMA.JOBS`.
    - Flag jobs with warnings related to query complexity or resource usage.
- **Visualization:** List of queries requiring refactoring.





---

## 5. User interface (UI) design (HTML5 web app)

- **Layout:**
  - **Sidebar:** Navigation to "Storage Optimizer".
  - **Top Bar:** Global Settings (Project ID, Region selector), Snapshot export/import, and the Notification Center.
- **Cross-Cutting Table Behaviour:** Every results table must expose a **Download CSV** action. The export covers the full DataTables result set matching the active search and filter state — not only the rendered page — reads cell text from the DOM so formatted values export as displayed, and prefixes a UTF-8 BOM for Excel compatibility. Tables constructed at render time (rather than present in the initial markup) must register their own export control.
- **Cross-Cutting Deep-Linking:** Job IDs render monospaced with Copy and "Open in BigQuery Console" actions; dataset and table names link to the Console explorer. Any result model sourced from an `*_BY_ORGANIZATION` view must carry the originating `project_id`, since the execution project is not the project that ran the job.
- **Notification Center:** Transient toasts are archived to a bell-icon dropdown with relative timestamps, an unread badge, and clear-all. Notification bodies frequently contain query text and user emails and must be rendered as text, never as markup.
- **Settings Guidance:** Configuration fields state the IAM roles they require and the financial consequence of each choice inline, rather than restating the field name.
- **Responsiveness:** Must be fully responsive on any modern browser.
- **Accessibility:** Controls revealed on hover must toggle `visibility` rather than `opacity` (an opacity-hidden control still accepts keyboard focus), must also reveal on `:focus-within`, and must remain visible under `@media (hover: none)` so touch devices can reach them.

---

## 6. Technical architecture & tech stack

- **Frontend:** HTML5, standard JavaScript (ES6), and Tailwind CSS.
- **Backend:** Python 3.10+ using **FastAPI**.
- **Data Visualization:** Chart.js, DataTables.net.
- **Infrastructure:** Dockerized container, hosted on **GCP Cloud Run**.

---

## 7. Implementation details for the developer

### 7.1 BigQuery permissions required

The Service Account running this application needs:

- `roles/bigquery.jobUser` (to execute queries)
- `roles/bigquery.metadataViewer` (to read `INFORMATION_SCHEMA`)
- `roles/bigquery.resourceAdmin` (to read/modify reservations and capacity commitments)
- `roles/bigquery.dataOwner` (to execute `ALTER SCHEMA` DDL)

### 7.2 Backend Data Extraction Strategy

The application extracts performance and cost metrics exclusively via BigQuery `INFORMATION_SCHEMA` metadata views. This ensures the application never scans the underlying row-level table data, maintaining strict privacy boundaries. 

All extraction logic, aggregations, and formatting are handled programmatically within the Python backend. To guarantee security, all dynamic inputs (such as project IDs and regions) are strictly validated and passed through parameterized execution functions to prevent SQL injection risks.

---

## 8. Setup and deployment (Docker & Cloud Run)

### 8.1 Project structure

```text
/bq-finops-optimizer
  ├── src/                 # FastAPI application & BigQuery logic
  │   ├── main.py          # App entrypoint, 25+ endpoints, middleware, logging config
  │   ├── hbo.py           # History-Based Optimization endpoints
  │   ├── cost_attribution.py  # Cost Attribution Engine
  │   ├── fluid_scaling.py # Fluid Scaling status/estimate/simulate
  │   └── utils.py         # Shared utilities: logging helpers, request ID, validation
  ├── requirements.txt     # Python dependencies
  ├── Dockerfile           # Deployment definition
  └── /static              # HTML/CSS/JS frontend files
      ├── index.html       # Includes Tiered Recommendation Cards
      └── app.js           # Chart.js rendering & Matrix interactivity
```

### 8.2 `requirements.txt`

```text
fastapi==0.103.1
uvicorn==0.23.2
gcp-cloud-bigquery==3.11.4
pydantic==2.3.0
```

### 8.3 `Dockerfile`

```dockerfile
# Use official lightweight Python image
FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .

EXPOSE 8080
CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8080"]
```

### 8.4 Deployment steps (Cloud Run)

```bash
PROJECT_ID="your-gcp-project-id"
REGION="us-central1"
APP_NAME="bq-finops-optimizer"
SERVICE_ACCOUNT="bq-finops-sa@${PROJECT_ID}.iam.gserviceaccount.com"

gcloud builds submit --tag gcr.io/${PROJECT_ID}/${APP_NAME}

gcloud run deploy ${APP_NAME} \
  --image gcr.io/${PROJECT_ID}/${APP_NAME} \
  --platform managed \
  --region ${REGION} \
  --allow-unauthenticated \
  --service-account ${SERVICE_ACCOUNT}
```

---

## 9. Future roadmap

*Roadmap is updated as features are prioritized.*

---

## 10. Quick wins for immediate implementation

Based on the analysis of common query troubleshooting patterns, the following "quick wins" can be implemented to provide immediate value and cost savings without complex infrastructure changes:

### 10.1 Materialized View Rejection Analyzer `[STATUS: IMPLEMENTED]`
*   **Objective:** Identify when MVs are rejected by the optimizer.
*   **Implementation:** Query `MaterializedViewStatistics` in `INFORMATION_SCHEMA.JOBS` to find `rejectedReason`.
*   **Value:** Helps users fix queries to leverage stored results.

### 10.2 BI Engine Acceleration Auditor `[STATUS: IMPLEMENTED]`
*   **Objective:** Identify queries that failed to get full BI Engine acceleration.
*   **Implementation:** Query `BiEngineStatistics` to find `BiEngineReason` for partial/no acceleration.
*   **Value:** Guides users to optimize memory or query structure for better performance and cost.




## 11. Guardrails and Risk Mitigation (Self-Inflicted Costs)

### 11.1 The Risk of `INFORMATION_SCHEMA` Queries
While this application aims to reduce BigQuery costs, running the analysis queries themselves incurs costs:
*   **On-Demand Pricing:** Queries against `INFORMATION_SCHEMA` views incur data processing costs with a minimum of 10 MB per query. Because these views are not cached, every refresh costs money.
*   **Capacity (Editions) Pricing:** These queries consume purchased slots.
*   **Scale Risk:** In massive organizations, `INFORMATION_SCHEMA` views can hold millions of rows of metadata. Unfiltered queries can scan significant amounts of data, leading to unexpected billing.

### 11.2 Mitigations & Best Practices
To prevent this tool from becoming a source of waste, the following guardrails are implemented:
*   **Mandatory Time Filters:** All heavy analysis queries must include a lookback window filter (e.g., `period_start > TIMESTAMP_SUB(...)`) to limit data scanned.
*   **No Auto-Polling:** The UI uses explicit manual actions ("Run Analysis", "Refresh") rather than auto-refresh timers to keep the user in control of the costs.
*   **Fallback to Project Scope (Standard Mode):** When running in standard (org-wide) mode, if organization-wide views (`JOBS_TIMELINE_BY_ORGANIZATION`) fail due to permissions, the tool falls back to project-scoped views (`JOBS_TIMELINE`) to query data from the execution project.
*   **No Fallback for Focus Scoping (Prevention of Silent Data Loss):** When a user explicitly specifies `focus_projects`, organization-level views are a hard requirement. The tool will NOT silently fall back to project-scoped views. Doing so could lead to partial query results where some requested focus projects are silently ignored due to missing permissions. If permissions are missing, the query must fail loudly with a clear 400/403 permission error.
*   **Project-Focus Scoping & Pruning Optimization:** The global `focus_projects` filter uses parameterized `IN UNNEST` constraints on `project_id` to scope queries. Because `JOBS_BY_ORGANIZATION` and `JOBS_TIMELINE_BY_ORGANIZATION` are partitioned by `creation_time` and clustered by `project_id`, BigQuery uses block pruning at runtime to only scan data matching the focus projects, reducing actual bytes scanned compared to dry runs. This also avoids the latency and complexity of running many individual project-level queries or assembling massive dynamic `UNION ALL` statements.
*   **Architectural Efficiency of Org Views over Project/Table Loops:** Querying central organization-wide views prevents the application from making hundreds of sequential or concurrent client-side metadata queries, which would exhaust BigQuery's API concurrency quotas, trigger rate-limiting (HTTP 429), and consume high application memory for in-memory post-processing (joining/filtering).
*   **Region Qualifier Enforcement:** Because BigQuery does not support cross-region organization metadata queries in a single statement, all organization-level `INFORMATION_SCHEMA` queries must target the specific region qualifier where the workloads are located (e.g., `region-us.INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION`).



## 12. Deployment & Repository Architecture

### 12.1 Project Structure
To maintain a clean separation of concerns and enable flexible deployment models, the repository is structured as follows:
*   **`src/`**: Contains the core Python FastAPI backend (`main.py`, `hbo.py`, `fluid_scaling.py`, etc.).
*   **`static/`**: Contains the live frontend assets (JS, CSS, HTML) that interact directly with the FastAPI backend.
*   **`docs/`**: A standalone, pre-compiled static representation of the application designed specifically to be hosted on **GitHub Pages**. This allows users to experience the "Simulator" features without spinning up a live Python server.
*   **`scratch/`**: An ignored directory for temporary scripts, local testing tools, and build scripts.

### 12.2 The Static Simulator (GitHub Pages)
Because FinOps tools require access to highly sensitive billing and organizational data, a live SaaS demo is not feasible. Instead, the application provides a **Static Simulator**:
*   The simulator is built using a purely client-side version of the frontend (`docs/simulator.html`).
*   It fetches a static, **heavily anonymized and sanitized** dummy dataset (`docs/finops-snapshot_dummy.json`).
*   All asynchronous data requests in `app.js` are intercepted in simulator mode, returning the dummy payload instantly. This allows prospective users to explore the interactive visualizations (e.g., slot contention, storage comparisons, fluid scaling models) entirely in their browser.

### 12.3 Data Security & Anonymization
When publishing dummy data to the `docs/` folder, strict sanitization protocols are enforced. Any exported "snapshot" JSON used for the simulator must have all PII, corporate domains, client project IDs, and Landing Zone (`LZ_`) database table names scrubbed to prevent leaking confidential architecture details.
