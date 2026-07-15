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

### 4.2 Module 1: storage cost optimizer

- **Parameters Input:** Active/Long-term Logical and Physical GiB Prices, Time Travel Rescale, Min Monthly Savings constraints.
- **Execution:** Executes storage metrics queries and processes data in Python.
- **Visualization:** Data table with Project/Dataset Name, Current/Recommended Billing Model, Monthly Savings, and generated DDL.
- **Implementation/Action:** An "Apply Optimization" button to execute `ALTER SCHEMA` DDL.

### 4.3 Module 2: on-demand vs. editions job analyzer

- **Parameters Input:** On-Demand Price (per TiB), Editions Slot Price, Analysis Time Window (Lookback Days).
- **Execution:** Executes job-level pricing analysis queries to compare the cost of running jobs on Editions (via slot consumption) versus On-Demand (via bytes processed).
- **Visualization:** Data table with Job ID / Project / User, Current/Recommended Billing Model (On-Demand vs. Editions), Estimated Waste on Editions (Savings), and recommended action. This visualization must be consistent with the Storage Cost Optimizer UI.
- **Implementation/Action:** Flag or export jobs/projects that are candidates for being moved to On-Demand billing to eliminate waste.

### 4.4 Module 3: cost attribution engine

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

### 4.5 Module 4: actual provisioning & capacity planning

- **User Story:** As a Cloud FinOps Administrator (or BigQuery capacity planner), I want to accurately calculate the exact number of BigQuery slot-hours consumed over a specific time period, broken down by autoscaled slots and uncommitted baseline slots, so that I can accurately forecast my BigQuery Enterprise billing, understand my compute overages, and optimize my capacity commitments to save money.
- **Problem Statement:** With BigQuery Enterprise editions, slot capacity is highly dynamic. Customers purchase fixed Capacity Commitments, but they also configure Reservations that have baseline slots and autoscaling limits. Because autoscaling scales up and down continuously based on workload, and capacity commitments can be created or updated at any time, it is incredibly difficult to calculate actual usage. You cannot just look at a static number; you must calculate the exact "area under the curve" (Time × Slots) down to the second. Currently, customers do not have a simple, single metric or dashboard out-of-the-box that tells them:
    - How many hours of compute were spent on unpredictable autoscaling.
    - How many hours of compute were spent on uncommitted baseline slots (baseline slots configured in a reservation that exceeded the pre-purchased capacity commitment, resulting in higher pay-as-you-go costs).
- **Acceptance Criteria:**
    - Provide a way (via this application's dashboard) to easily view `autoscaled_slot_hours` and `baseline_slot_hours`.
    - The calculation must accurately subtract pre-purchased `capacity_slots` from `baseline_slots` so customers only see the billable uncommitted baseline hours.
    - The solution must account for exact timestamp intervals (`end_timestamp - change_timestamp`) to ensure no over/under-billing of fractional hours.

### 4.6 Module 5: workload profiler

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
- **Visualization:** Data table showing Dataset, Table, Live Size, and Time Travel Size with a health status.

### 4.9 Module 8: Architectural Anti-Pattern Detection `[STATUS: IMPLEMENTED]`

- **Objective:** Detect inefficient ingestion APIs, DML abuse, and Materialized View refresh costs.
- **Acceptance Criteria:**
    - **DML Abuse Tracker:** Scan `INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION` for high-frequency single-row `INSERT` statements.
    - **Materialized Views:** Detect high refresh costs (Moved to Module 10).
- **Visualization:** List of flagged users/projects abusing DML with counts and wasted costs.

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

### 4.12 Module 11: The Query Anti-Pattern Linter `[STATUS: IMPLEMENTED]`

- **Objective:** Scan historical queries to detect expensive anti-patterns like `SELECT *`.
- **Acceptance Criteria:**
    - Scan `INFORMATION_SCHEMA.JOBS_BY_PROJECT` (looping through active projects) for expensive jobs (>100GB).
    - Use regex to flag `SELECT *` abuse.
    - Flag queries using `LIMIT` on non-clustered tables for exploration, suggesting free preview options instead.
- **Visualization:** "SQL Wall of Shame" listing offending users and queries.

### 4.13 Module 12: Schema & Storage Governance Auditor `[STATUS: PARTIALLY IMPLEMENTED]`

- **Objective:** Audit datasets for cost-control settings like partition filters and expiration rules.
- **Acceptance Criteria:**
    - **Dataset Expiration:** Identify datasets missing `default_table_expiration_days` (using `SCHEMATA` left join `SCHEMATA_OPTIONS`).
    - **Partition Filter:** Identify partitioned tables missing the `require_partition_filter` setting (using Python API loop for top 5 datasets to bypass missing column limits).
- **Visualization:** Checklist of high-risk tables and datasets.

### 4.14 Module 13: Cost Control & Quota Watchdog [STATUS: IMPLEMENTED]

- **Objective:** Enforce cost safety guardrails across all diagnostic and analytical query executions to protect cloud budgets.
- **Acceptance Criteria:**
  - Enforce a customizable `max_bytes_billed_gb` safety cap parameter across all application BigQuery query executions.
  - Automatically translate the user setting into BigQuery's `QueryJobConfig.maximum_bytes_billed` (converting GiB to bytes) before submitting query jobs.
  - Implement robust clamping boundaries (minimum of 1 GiB, maximum of 10 TiB, and a safe default fallback of 200 GiB if unset or zero).
  - Validate the input dynamically using Pydantic fields across all request payloads and persist the user's preference in the client's local storage.
- **Visualization:** An adjustable safety input field in the global settings panel displaying the active query cost cap.

### 4.14b Module 25: Observability & Request Tracing [STATUS: IMPLEMENTED]

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

### 4.15 Module 14: Interactive vs. Batch Optimizer `[STATUS: IMPLEMENTED]`

- **Objective:** Identify queries submitted with Interactive priority that are candidates for Batch priority to smooth out slot usage peaks.
- **Acceptance Criteria:**
    - Scan `INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION` for jobs with `priority = 'INTERACTIVE'`.
    - Flag jobs that are long-running (>5 minutes), executed during off-peak hours, or generated by service accounts.
- **Visualization:** Data table of candidates for Batch priority with reasoning, dynamic tags, and tooltips.



### 4.17 Module 16: Execution Plan & Data Skew Analyzer `[STATUS: IMPLEMENTED]`

- **Objective:** Parse execution plans to detect data skew in joins/partitions.
- **Acceptance Criteria:**
    - Scan `INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION` (using native array fields on `job_stages`) for jobs where max node compute time is >10x the average.
- **Visualization:** List of skewed jobs with ratio and wasted slot hours.

### 4.18 Module 17: LLM-Powered Semantic Query Analysis (GenAI) `[STATUS: IMPLEMENTED]`

- **Objective:** Use BigQuery's native Vertex AI integration (`AI.GENERATE_TEXT`) to analyze queries for semantic anti-patterns.
- **Acceptance Criteria:**
    - Accept a user-provided remote model name.
    - Pass top queries to Gemini via SQL and parse `result` column.
- **Visualization:** "AI Code Reviewer" tab showing job details and Gemini's advice.





---

## 5. User interface (UI) design (HTML5 web app)

- **Layout:**
  - **Sidebar:** Navigation to "Storage Optimizer".
  - **Top Bar:** Global Settings (Project ID, Region selector).
- **Responsiveness:** Must be fully responsive on any modern browser.

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

### 7.2 Backend SQL handling

#### 7.2.1 BigQuery Editions recommender queries

_Developer Note:_ Module 3 has been updated to query usage across the entire organization using `JOBS_TIMELINE_BY_ORGANIZATION` (to accurately reflect reservation scoping) and utilizes `APPROX_QUANTILES` for the tiered matrix.

**Snippet for Python Backend (FastAPI + GCP BigQuery SDK):**

```python
# 1. Slot Usage Timeline Query (Organization Wide)
def get_slot_usage_timeline(params):
    sql = f"""
    SELECT
      TIMESTAMP_TRUNC(period_start, HOUR) AS timestamp,
      SUM(total_slot_ms) / (1000 * 60 * 60) AS slot_usage,
      SUM(total_bytes_billed) / POW(1024, 4) AS tib_billed,
      reservation_id,
      location AS region
    FROM
      `{params.org_project_id}`.`{params.region}`.INFORMATION_SCHEMA.JOBS_TIMELINE_BY_ORGANIZATION
    WHERE
      period_start > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {params.lookback_days} DAY)
    GROUP BY
      timestamp, reservation_id, location
    ORDER BY timestamp DESC
    """
    # Execute query...

# 2. Tiered Recommendation Matrix (Aggressive, Balanced, Performance)
def get_percentile_recommendations(params):
    sql = f"""
    WITH hourly_usage AS (
        SELECT
          TIMESTAMP_TRUNC(period_start, HOUR) AS timestamp,
          SUM(total_slot_ms) / (1000 * 60 * 60) AS slot_usage,
          reservation_id
        FROM
          `{params.org_project_id}`.`{params.region}`.INFORMATION_SCHEMA.JOBS_TIMELINE_BY_ORGANIZATION
        WHERE
          period_start > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {params.lookback_days} DAY)
          AND reservation_id IS NOT NULL
        GROUP BY
          timestamp, reservation_id
    )
    SELECT
        reservation_id,
        APPROX_QUANTILES(slot_usage, 100)[OFFSET(80)] AS aggressive_baseline_p80,
        APPROX_QUANTILES(slot_usage, 100)[OFFSET(95)] AS balanced_baseline_p95,
        MAX(slot_usage) AS performance_baseline_max
    FROM
        hourly_usage
    GROUP BY
        reservation_id
    """
    # Execute query...

# 3. Editions Reservation Config Query
def get_editions_reservations(params):
    sql = f"""
    SELECT
      CURRENT_TIMESTAMP() AS timestamp,
      reservation_name AS reservation_id,
      project_number AS admin_project_number,
      project_id AS admin_project_id,
      location AS region,
      edition,
      slot_capacity AS baseline,
      (autoscale.max_slots - slot_capacity) AS autoscale_max_slots,
      ignore_idle_slots
    FROM
      `{params.org_project_id}`.`{params.region}`.INFORMATION_SCHEMA.RESERVATIONS
    """
    # Execute query...
```

#### 7.2.2 HBO Impact Estimation Query

```sql
WITH
  jobs AS (
    SELECT
      *,
      query_info.query_hashes.normalized_literals AS query_hash,
      TIMESTAMP_DIFF(end_time, start_time, MILLISECOND) AS elapsed_ms,
      IFNULL(
        ARRAY_LENGTH(JSON_QUERY_ARRAY(query_info.optimization_details.optimizations)) > 0,
        FALSE)
        AS has_history_based_optimization,
    FROM `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
    WHERE EXTRACT(DATE FROM creation_time) > DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
  ),
  most_recent_jobs_without_history_based_optimizations AS (
    SELECT *
    FROM jobs
    WHERE NOT has_history_based_optimization
    QUALIFY ROW_NUMBER() OVER (PARTITION BY query_hash ORDER BY end_time DESC) = 1
  )
SELECT
  job.job_id,
  100 * SAFE_DIVIDE(
    original_job.elapsed_ms - job.elapsed_ms,
    original_job.elapsed_ms) AS percent_execution_time_saved,
  job.elapsed_ms AS new_elapsed_ms,
  original_job.elapsed_ms AS original_elapsed_ms,
FROM jobs AS job
INNER JOIN most_recent_jobs_without_history_based_optimizations AS original_job
  USING (query_hash)
WHERE
  job.has_history_based_optimization
  AND original_job.end_time < job.start_time
ORDER BY percent_execution_time_saved DESC
LIMIT 10;
```

#### 7.2.3 Storage Hygiene & Time Travel Waste Detection

```sql
SELECT
  table_schema AS dataset,
  table_name,
  active_logical_bytes / POW(1024,3) AS live_table_gb,
  time_travel_physical_bytes / POW(1024,3) AS time_travel_gb,
  IF(time_travel_physical_bytes > active_logical_bytes, 'High Churn/Recreate Detected', 'Healthy') as health_status
FROM
  `region-us`.INFORMATION_SCHEMA.TABLE_STORAGE
ORDER BY
  time_travel_gb DESC
LIMIT 20;
```

#### 7.2.4 Compute Simulator (On-Demand vs. Slots)

```sql
SELECT 
  job_id,
  query,
  total_bytes_billed,
  total_slot_ms,
  (total_bytes_billed * 6.25) / POW(1024, 4) AS projected_on_demand_cost,
  (total_slot_ms * 0.04) / (100 * 1000 * 60 * 60) AS projected_slot_cost,
  IF(
    ((total_bytes_billed * 6.25) / POW(1024, 4)) > ((total_slot_ms * 0.04) / (100 * 1000 * 60 * 60)), 
    'MOVE TO EDITIONS/SLOTS', 
    'KEEP ON-DEMAND'
  ) AS pricing_recommendation
FROM `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
WHERE job_type = 'QUERY' 
  AND creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
ORDER BY projected_on_demand_cost DESC
LIMIT 50;
```

#### 7.2.5 DML Abuse Tracker (High-Frequency INSERTs)

```sql
SELECT
  user_email,
  project_id,
  COUNT(job_id) AS insert_job_count,
  SUM(total_slot_ms) / (1000 * 60 * 60) AS wasted_slot_hours,
FROM
  `region-us`.INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION
WHERE
  statement_type = 'INSERT'
  AND creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
GROUP BY
  user_email, project_id
HAVING 
  insert_job_count > 1000 
ORDER BY
  wasted_slot_hours DESC;
```

#### 7.2.6 Billing Export Analysis (Future)

Because API ingestion doesn't log to `INFORMATION_SCHEMA.JOBS`, the app must query the GCP Cloud Billing Export Table (if the user grants the tool access to it) to calculate the exact financial waste.

```sql
-- App SQL: Analyze Billing Export to compare Streaming APIs
SELECT
  sku.description AS ingestion_method,
  SUM(usage.amount) AS total_gb_ingested,
  SUM(cost) AS total_cost
FROM
  `your-billing-project.billing_dataset.gcp_billing_export_v1_XXXX`
WHERE
  service.description = 'BigQuery'
  AND sku.description IN (
    'Streaming Insert', -- This is the legacy tabledata.insertAll SKU
    'BigQuery Storage Write API' -- This is the modern SKU
  )
  AND usage_start_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
GROUP BY
  ingestion_method;
```

#### 7.2.7 BI Engine ROI & Cache Miss Diagnostic

```sql
SELECT
  job_id,
  user_email,
  total_bytes_processed / POW(1024, 3) AS processed_gb,
  total_bytes_billed / POW(1024, 3) AS billed_gb,
  ((total_bytes_processed - total_bytes_billed) / POW(1024, 4)) * 6.25 AS estimated_dollars_saved,
  bi_engine_statistics.bi_engine_mode,
  ARRAY_TO_STRING(
    ARRAY(SELECT reason FROM UNNEST(bi_engine_statistics.bi_engine_reasons)), ', '
  ) AS failure_reasons
FROM `region-us`.INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION
WHERE 
  job_type = 'QUERY'
  AND creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
ORDER BY total_bytes_processed DESC
LIMIT 50;
```

#### 7.2.8 Query Linter (SELECT * Abuse)

```sql
SELECT
  user_email,
  query,
  total_bytes_billed / POW(1024, 3) AS billed_gb,
  total_slot_ms
FROM
  `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
WHERE
  job_type = 'QUERY'
  AND creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
  AND total_bytes_billed > 107374182400
ORDER BY
  billed_gb DESC
LIMIT 50;
```

#### 7.2.9 Partition Filter Guardrail Auditor

```sql
SELECT
  table_catalog AS project_id,
  table_schema AS dataset_id,
  table_name,
  partition_type
FROM
  `region-us`.INFORMATION_SCHEMA.PARTITIONS
WHERE
  partition_type IS NOT NULL
  AND table_name NOT IN (
    SELECT table_name 
    FROM `region-us`.INFORMATION_SCHEMA.TABLES 
    WHERE is_partition_filter_required = 'YES'
  )
GROUP BY 1, 2, 3, 4;
```

#### 7.2.10 Dataset Expiration Auditor

```sql
SELECT
  catalog_name AS project_id,
  schema_name AS dataset_id,
  option_value AS default_table_expiration
FROM
  `region-us`.INFORMATION_SCHEMA.SCHEMATA_OPTIONS
WHERE
  option_name = 'default_table_expiration_days'
  AND option_value IS NULL;
```

#### 7.2.11 Data Skew Detector

```sql
WITH unnested_stages AS (
  SELECT
    job_id,
    user_email,
    total_slot_ms,
    stage.name AS stage_name,
    stage.compute_ms_avg AS avg_compute_ms,
    stage.compute_ms_max AS max_compute_ms
  FROM
    `region-us`.INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION,
    UNNEST(job_stages) AS stage
  WHERE
    creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
    AND job_type = 'QUERY'
)
SELECT
  job_id,
  user_email,
  stage_name,
  avg_compute_ms,
  max_compute_ms,
  SAFE_DIVIDE(max_compute_ms, avg_compute_ms) AS skew_ratio
FROM
  unnested_stages
WHERE
  avg_compute_ms > 0
  AND SAFE_DIVIDE(max_compute_ms, avg_compute_ms) > 10
  AND max_compute_ms > 10000
ORDER BY
  skew_ratio DESC;
```

#### 7.2.12 Batch Candidate Detector

```sql
SELECT
  job_id,
  user_email,
  priority,
  total_slot_ms,
  TIMESTAMP_DIFF(end_time, start_time, MINUTE) AS duration_minutes,
  EXTRACT(HOUR FROM creation_time AT TIME ZONE "UTC") AS run_hour_utc,
  
  CASE 
    WHEN user_email LIKE '%.gserviceaccount.com' THEN 'Service Account'
    WHEN TIMESTAMP_DIFF(end_time, start_time, MINUTE) > 5 THEN 'Long-Running ETL'
    WHEN EXTRACT(HOUR FROM creation_time AT TIME ZONE "UTC") NOT BETWEEN 13 AND 23 THEN 'Off-Peak Hours (US)'
    ELSE 'Other'
  END AS batch_candidate_reason

FROM
  `region-us`.INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION
WHERE
  job_type = 'QUERY'
  AND priority = 'INTERACTIVE'
  AND total_slot_ms > 10000
  
  AND (
    user_email LIKE '%.gserviceaccount.com'
    OR TIMESTAMP_DIFF(end_time, start_time, MINUTE) > 5
    OR EXTRACT(HOUR FROM creation_time AT TIME ZONE "UTC") NOT BETWEEN 13 AND 23
  )
ORDER BY
  total_slot_ms DESC
LIMIT 100;
```



### 7.3 Security Considerations: SQL Injection Risk
- **Problem:** The application uses direct string interpolation for project IDs and regions in SQL queries (e.g., `FROM \`{target_project}\`.\`{params.region}\`.INFORMATION_SCHEMA...`).
- **Requirement:** Implement strict whitelist validation or regex checks for `region` and `org_project_id` in Pydantic models to ensure they only contain valid characters (alphanumeric, hyphens, underscores) and no SQL control characters.

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

All planned and future modules have been moved to [ROADMAP.MD](./ROADMAP.MD).

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

### 10.3 Proactive Resource Warning Scanner `[STATUS: IMPLEMENTED]`
*   **Objective:** Catch queries approaching complexity or resource limits.
*   **Implementation:** Query `query_info.resource_warning` in `INFORMATION_SCHEMA.JOBS`.
*   **Value:** Prevents query failures by refactoring complex queries early.

_Additional quick wins (10.4–10.6) have been moved to [ROADMAP.MD](./ROADMAP.MD)._

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
When publishing dummy data to the `docs/` folder, strict sanitization protocols are enforced. Any exported "snapshot" JSON used for the simulator must have all PII, corporate domains, client project IDs, and internal system/Landing Zone (`LZ_`) database table names scrubbed to prevent leaking confidential architecture details.
