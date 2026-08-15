# ⚡ FinOps Optimizer for BigQuery

[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.100+-green.svg)](https://fastapi.tiangolo.com)
[![GitHub Pages](https://img.shields.io/badge/Live%20Demo-GitHub%20Pages-orange.svg)](https://mbettan.github.io/bq-finops-optimizer/simulator.html)

> *Not a Google product — an independent personal project. Costs shown are estimates; see [DISCLAIMER.md](DISCLAIMER.md).*

An open-source, self-hosted BigQuery FinOps diagnostic toolkit and interactive simulation engine. It analyzes historical telemetry, query workloads, and storage configurations across Google Cloud Organizations to maximize cost efficiency, reduce compute waste, and stage automated governance remediations.

* **Documentation & Demo:** [FinOps Optimizer Documentation](https://mbettan.github.io/bq-finops-optimizer/)
* **Interactive Simulator:** [FinOps Optimizer Simulator](https://mbettan.github.io/bq-finops-optimizer/simulator.html)
* **Runtime Cost Calculator:** [FinOps ROI & Runtime Economics Calculator](https://mbettan.github.io/bq-finops-optimizer/#calculator)

---



## 🎯 Core Optimization Pillars

*   **Storage Optimization**: Identifies and automates transitions between logical and physical billing models.
*   **Compute Right-Sizing**: Evaluates On-Demand vs. Editions pricing, performs **Edition matrix simulation** to find optimal baseline capacities, and analyzes autoscaler performance.
*   **Architectural Diagnostics**: Identifies anti-patterns such as DML abuse, redundant materialized views, and slot-inefficient query designs.
*   **Cost Attribution**: Overcomes GCP billing limitations to proportionally distribute unallocated reservation waste back to business units.
*   **Project-Focus Scoping**: Isolates analytics to specific project boundaries without corrupting org-wide mathematical invariants (like capacity planning or attribution denominators).
*   **AI-Powered Query Analysis**: Uses Gemini models via BigQuery's `AI.GENERATE` function to perform semantic SQL review at scale.
*   **Findings You Can Act On**: Every results table exports to CSV, every job ID deep-links into the BigQuery Console, and the Anti-Pattern Linter rolls per-job findings up by owner so the output is a work queue rather than a log.

---

## ⚙️ FinOps Methodologies & Technical Innovations

### 1. The "Slot Capacity Bucket Method" (Compute Capacity Simulation)
To avoid over-provisioning baseline capacity or over-paying for autoscaled slots, the system uses the **Slot Capacity Bucket Method**. It runs a vectorized simulation using NumPy to model slot consumption over a standard 730-hour billing month:
*   Calculates baseline utilization and autoscale frequencies for every reservation.
*   Generates a **Tiered Recommendation Matrix** (Aggressive, Balanced, and Performance baseline recommendations) based on historical percentiles.
*   Evaluates simulated hourly costs to help you choose capacity structures that align with specific workload SLAs and risk profiles.

### 2. Fluid Scaling & Cooldown Tax Mitigation
Autoscaling workloads are often subject to a "cooldown tax" where a 60-second billing minimum is charged even for queries lasting a fraction of a second. This tool identifies reservations with high-frequency short queries and simulates potential savings from switching to **Fluid Scaling** (true per-second billing with zero minimum capacity).

### 3. Hybrid Cost Attribution Model
Standard GCP billing dumps idle capacity waste into a central administrative project. This application solves the cost attribution challenge by:
*   Mapping organization-wide project usage back to specific reservations.
*   Proportionally distributing unused baseline slots and idle reservation waste back to user projects.
*   Enforcing customizable allocation rules (e.g., Lender Pays vs. Borrower Pays for idle slot borrowing).

### 4. History-Based Optimization (HBO) Proof of Value
BigQuery HBO automatically optimizes queries over time. Since it is enabled by default and cannot be turned off for A/B testing, this tool:
*   Matches optimized execution plans with historical baseline runs using `normalized_literals` query hashes.
*   Tracks execution savings and calculates performance ROI.
*   Monitors plans nearing the 130-day expiration window to suggest automated "warm-up" runs.

### 5. AI-Powered Query Analysis (AI Doctor)
> **Note:** The AI Doctor is the **only module in this entire application that uses AI/GenAI**. All other modules (Storage, Compute, Slots, HBO, Cost Attribution, Governance, Anti-Patterns, etc.) are powered exclusively by SQL queries and Python analytics — no AI, no LLM, no external model calls.

*   **Multi-Strategy ROI Discovery Engine:** Rather than auditing single un-grouped job executions, AI Doctor aggregates query telemetry across `JOBS_BY_ORGANIZATION` by query hash over customizable lookback windows (7 to 90 days). It supports 5 distinct prioritization strategies:
    *   **⚖️ Balanced ROI Score (`composite`)**: Multi-factor scoring formula: $0.40 \cdot \log_{10}(GB + 1) + 0.30 \cdot \log_{10}(Execs + 1) + 0.20 \cdot \log_{10}(Slots + 1) + 0.10 \cdot SpillFlag$.
    *   **💰 Cumulative Cost (`cumulative_cost`)**: Ranks workload templates by aggregate bytes billed across all historical executions.
    *   **🔄 High Frequency (`execution_frequency`)**: Focuses on micro-offender dashboard queries executed repeatedly (`HAVING COUNT(*) > 1`), rendering interactive execution run badges in the UI.
    *   **💾 Memory RAM Spill (`memory_spill`)**: Surfaces memory-intensive queries that spill intermediate shuffle bytes to disk (`HAVING bytes_spilled > 0`).
    *   **⏱️ Total Slot Time (`slot_ms`)**: Focuses on heavy aggregate slot-consuming query templates.
*   **Multi-Stage Shuffle Spill Unnesting:** Unnests `job_stages` via `(SELECT SUM(s.shuffle_output_bytes_spilled) FROM UNNEST(job_stages) s)` to aggregate intermediate RAM-to-disk spills across all execution stages.
*   **Deterministic Worst-Job Sampling:** Uses `ARRAY_AGG(job_meta ORDER BY total_slot_ms DESC LIMIT 1)[OFFSET(0)]` to atomically isolate the single worst execution instance and its exact metadata payload for AI auditing.
*   **Strict IAM Guardrails:** Requires `roles/bigquery.resourceViewer` at the organization level for discovery. Intercepts IAM 403 `Forbidden` exceptions and raises an explicit `HTTP 403 Access Denied` response with clear role guidance.
*   **No model creation required** — no `CREATE MODEL`, no remote model, no BigQuery ML dataset. The function calls the Vertex AI publisher endpoint (`/publishers/google/models/gemini-3.6-flash` by default, or `gemini-3.5-flash-lite`) directly.
*   **No Cloud Resource Connection required** (by default) — works with your existing end-user ADC credentials out of the box. A connection is only needed if deploying on Cloud Run with a service account.
*   **No additional APIs to enable** beyond the Vertex AI API on your project.
*   Sends each SQL statement to Gemini with a structured prompt that checks for 7 common anti-patterns (e.g., `SELECT *`, missing `WHERE` before `JOIN`, `CROSS JOIN`, `COUNT(DISTINCT)` vs. `APPROX_COUNT_DISTINCT`).
*   **Required IAM permissions:** `roles/aiplatform.user` (Vertex AI User) on the execution project — see AI Doctor Permissions below.

#### AI Doctor Model Configuration
The `AI.GENERATE` call is configured with the following production-tuned `model_params`:

| Parameter | Value | Rationale |
| :--- | :--- | :--- |
| **Model** | `gemini-3.6-flash` (default), `gemini-3.5-flash-lite` | `gemini-3.6-flash` provides state-of-the-art reasoning for anti-pattern identification; `gemini-3.5-flash-lite` provides lowest cost and fast inference. |
| **Temperature** | `0.1` | Near-deterministic output for consistent anti-pattern detection. |
| **Max Output Tokens** | `1024` | Provides headroom for 5–7 bullet-point findings. Previous value of `300` caused silent `NULL` returns (`finish_reason: MAX_TOKENS`) when the model's internal reasoning consumed the entire budget. |
| **Thinking Level** | `MINIMAL` | Gemini 3.x parameter. Constrains thinking tokens to preserve budget for output. |
| **Safety Settings** | All 4 categories `OFF` | SQL text is never harmful content. Prevents false blocks from repeated literals or large token volumes. |

#### AI Doctor Architecture
*   **Multi-Strategy Candidate Aggregation:** Aggregates jobs by `COALESCE(query_hashes.normalized_literals, CONCAT('job:', job_id))` and applies multi-stage shuffle spill unnesting (`UNNEST(job_stages)`).
*   **DDL Summarization:** Full column-level DDL dumps are replaced with structural metadata summaries (~150 chars per table): row count, byte size, partition/clustering keys, and column count. The LLM doesn't need column names to detect anti-patterns.
*   **Newline-Aware Truncation:** SQL payloads exceeding 5,000 characters and DDL payloads exceeding 4,000 characters are truncated at the last newline boundary to prevent feeding malformed SQL fragments to the model.
*   **Parallel Execution:** Queries are batched into `UNION ALL` chunks (5 per chunk), with each subquery independently calling `AI.GENERATE`. This enables parallel LLM evaluation within a single BigQuery job.
*   **3-Tier Error Handling:** The backend extracts the full Vertex AI response struct (`result`, `status`, `full_response`) to distinguish between function-level failures (NULL struct), API errors (status populated), and model-level blocks (`finish_reason: MAX_TOKENS` or `SAFETY`).


### 6. Operator Workflow: Export, Deep-Link, Triage & Snapshot Sharing
A diagnostic that can't leave the browser doesn't get acted on. Four cross-cutting capabilities apply to every module:
*   **Universal CSV Export**: Every results table renders a **Download CSV** button. The export covers the entire DataTables result set matching the current search and filter state — not just the visible page — and writes a UTF-8 BOM so Excel opens it without mangling. Cell text is read from the rendered DOM, so byte sizes, currency, and badge labels export exactly as displayed.
*   **Snapshot Export & Import**: Allows exporting all cached `localStorage` diagnostic results into a standalone, shareable `.json` snapshot file (`finops-snapshot_<project>_<timestamp>.json`). Recipient instances can import the snapshot to hydrate the full UI (tables, charts, recommendations) without BigQuery credentials.
*   **BigQuery Console Deep-Links**: Job IDs render monospaced with hover- and focus-revealed **Copy** and **Open in Console** actions; dataset and table names link straight to the Console explorer. Every org-wide result model (`MVResult`, `WarningResult`, `BIResult`, `AIResult`) carries `project_id`, because `JOBS_BY_ORGANIZATION` spans the whole organization and a link built from the execution project would resolve to the wrong one.
*   **Notification Center**: Toasts are archived to a bell-icon dropdown with relative timestamps, an unread badge, and clear-all — so a warning raised during a long scan is still readable afterwards. Bodies render via `textContent`, since notifications routinely carry query text and user emails.

#### Snapshot (Export / Import) Workflow & Security
*   **Primary Use Cases**:
    1.  **Colleague Sharing**: Share full analysis results with team members who lack direct GCP/BigQuery permissions.
    2.  **LLM-Powered Analysis**: Feed structured snapshot JSON directly into LLMs (Gemini, Claude, GPT) for automated executive summaries, JIRA ticket generation, or cost anomaly detection.
    3.  **Troubleshooting & Bug Reporting**: Capture exact application state to reproduce findings or share with maintainers without exposing raw GCP access.
*   **Redaction Controls**: Checking **"Redact sensitive info"** before export scrubs PII emails (replaced with `redacted@example.com`) and raw SQL query strings (replaced with `-- [redacted query]`). *Note:* Project IDs, reservation topologies, cost metrics, and table names remain visible for functional analysis.
*   **Size & Transport Constraints**: Snapshot size is bounded by Chrome's 5 MB per-origin `localStorage` cap (~5–6 MB maximum formatted file size). It compresses to ~500 KB and easily fits within standard email attachment limits (Gmail 25 MB limit).

### 7. Built-in Security & Access Guardrails
*   **Defense-in-Depth Validation**: All BigQuery-derived identifiers (e.g., project IDs, reservation names) are actively sanitized via `_safe_ident()` before DDL generation or query interpolation to prevent second-order SQL injection.
*   **Strict Scope Preservation**: Endpoints correctly differentiate between scoped views (e.g., usage filtering) and mathematical invariants (e.g., capacity planning, fluid scaling estimation, cost attribution) which explicitly reject or bypass scope filters to guarantee financial accuracy.
*   **Data Sanitization**: Complete frontend HTML escaping prevents XSS across anomaly logs, AI recommendations, and query snippets.

### 8. Server-Side Shared Result Cache (GCS FUSE)
Analysis results are cached on the server across instances, cold starts, and team members using Cloud Run Gen 2 Cloud Storage volume mounts (`gcsfuse`):
*   **Zero-Base-Cost Shared Storage**: Results persist in a GCS bucket mounted at `/cache` ($0/month base cost vs. $35–50/month for Cloud Memorystore/Redis), with automatic 7-day retention lifecycle management.
*   **Parameter-Exact SHA-256 Hashing**: Request parameters (lookback days, prices, focus projects) are canonicalized and hashed (`v1/{org}/{region}/{module}/{hash}.json`) so different parameter variations never poison or overwrite each other.
*   **Instant Multi-User Hydration**: When a new team member opens the application or switches devices, the UI checks `/api/cache/status` (accelerated by the 60-second in-memory FUSE stat cache) and auto-populates tables via `hydrateFromServerCache()` at **zero BigQuery cost** (`X-Cache: HIT`).
*   **In-Process Single-Flight Locks**: Prevents concurrent duplicate BigQuery executions during parallel sweeps.
*   **Cache Control**: Full API control via `GET /api/cache/status`, `DELETE /api/cache/{module}`, and `DELETE /api/cache`.

### 9. Runtime Economics & Google Cloud Costs

A common question when evaluating this tool across large Google Cloud organizations is:  
> *"What does it actually cost to run diagnostic sweeps on my environment?"*

The breakdown below details the exact pricing structure for each layer:

#### 1. BigQuery Query Costs (On-Demand & Editions)
* **Zero User Table Scans:** Diagnostic queries strictly inspect `INFORMATION_SCHEMA` metadata views (`JOBS_BY_ORGANIZATION`, `TABLE_STORAGE`, `RESERVATIONS`). The tool **never scans production data tables** (e.g., auditing a 100 TB user dataset incurs $0 in table scan charges).
* **Small Metadata Queries:** Billed at the 10 MB minimum floor = **$0.00006 per query** (~16,000 queries per $1.00).
* **Deep Org-Wide Telemetry Sweeps:** Scanning live system telemetry across millions of historical jobs over 90 days (e.g., 800 GB of metadata) costs **~$4.88 per run** ($6.25 per TiB scanned).
* **BigQuery Editions (Reservations):** **$0.00 per-byte**. Queries consume compute slots from your existing reservation (standard slot rates apply if autoscaling triggers).
* **Server-Side Shared Result Caching:** Subsequent requests for the same parameters return instant `X-Cache: HIT` from GCS FUSE with **$0.00 BigQuery scan charges**.

#### 2. Cloud Run Serverless Hosting
* **Idle Cost (`min-instances = 0`):** **$0.00** (Zero passive cost when idle).
* **Single 10-Minute Scan (1 vCPU, 2 GiB):** **$0.017 per scan** (1.7 cents).
* **Automated Daily Sweeps (30 runs / month):** **$0.52 / month**.
* **Automated Hourly Sweeps (720 runs / month):** **$12.53 / month**.
* **Intra-Region Network Egress:** **$0.00** (Free internal GCP routing).

#### 3. AI Doctor (Vertex AI Gemini 3.6 Flash / 3.5 Flash-Lite)
* Powered by `gemini-3.6-flash` ($1.50 / 1M input, $7.50 / 1M output) or `gemini-3.5-flash-lite` ($0.30 / 1M input, $2.50 / 1M output).
* Running a deep semantic review on **50 query candidates costs ~15.3 cents ($0.153)** with 3.5 Flash-Lite (or ~$0.563 with 3.6 Flash) per run.

#### 📊 Cost Summary Matrix

| Workload Scenario | BigQuery Scan Cost | Cloud Run Hosting | AI Doctor (Gemini) | Total Estimated Cost |
| :--- | :--- | :--- | :--- | :--- |
| **Interactive Ad-Hoc Run** | $0.00006 – $0.05 | $0.0035 (2 min) | $0.153 (50 queries, 3.1 Flash-Lite) | **~$0.16 – $0.21 / run** |
| **Deep Org Sweep (800 GB metadata)** | ~$4.88 | $0.017 (10 min) | $0.153 (3.1 Flash-Lite) | **~$5.05 / run** |
| **Automated Daily Sweeps (Monthly)** | $0.05 – $1.50 / mo | $0.52 / mo | $0.61 / mo (weekly 3.1 Flash-Lite) | **~$1.20 – $2.65 / month** |
| **Automated Hourly Sweeps (Monthly)** | $1.30 – $3.50 / mo | $12.53 / mo | $2.40 / mo | **~$16.20 – $18.45 / month** |

---

## 🔍 Modules & Capabilities

| Module | Purpose | Key Telemetry / Metrics | Actionable Output |
| :--- | :--- | :--- | :--- |
| **Storage Optimizer** | Logical vs. Physical Storage Auditing | Active/Long-term storage bytes, change rates | `ALTER SCHEMA` DDL generator |
| **Active Assist** | Google-native partitioning & clustering recommendations | Recommender API insights | One-click recommendation viewer with "Blind Spot" methodology card |
| **Compute Analyzer** | Compute billing model comparisons | Slot hours vs. Bytes billed | Project/workload billing model selector |
| **Capacity Planner** | Real-time capacity sizing & baseline simulation | Simulated hourly slot-hour logs (NumPy) | Quantile-based reservation baseline matrix |
| **Tiered Recommendations** | Multi-tier baseline capacity suggestions | Per-minute peak slot analysis | Aggressive / Balanced / Performance baselines |
| **Fluid Scaling Simulator** | Cooldown tax and Fluid Scaling evaluation | Billing time-blocks, execution frequencies | High-frequency workload isolation candidates |
| **Cost Attribution Engine** | Custom cost splitting and billing attribution | `JOBS_BY_ORGANIZATION` telemetry | Split-cost CSV/JSON reports |
| **Workload Profiler** | Continuous-trickle query detection | Short execution patterns, reservation usage | Isolated reservation strategies & top queries |
| **Query Anti-Pattern Linter** | Static SQL auditing and performance advice | `SELECT *` patterns, unclustered limits | "SQL Wall of Shame" reporting, filtered by identity/project/pattern with an owner-level summary roll-up |
| **Storage Hygiene Auditor** | Table churn and time travel tracking | Time travel physical bytes, table updates, `default_time_travel_days` | Time travel window reductions with current TTL shown per dataset |
| **BI Engine Optimizer** | BI Engine utilization analysis | BI Engine mode (FULL/PARTIAL/NONE), miss reasons | BI Engine cache diagnostics |
| **Governance & Expiration** | Schema policies and safety check | Expiration settings, partition filters | Non-compliant resource inventory |
| **Resource Warnings** | Dataset/table security & config flags | Public access, stale data, missing policies | Risk-scored warning inventory |
| **DML & MV Auditor** | Materialized view and write operations audits | Refresh costs, single-row DML write loops, per-table insert counts / active days / avg inserts per day | MV and ingestion refactoring alerts (Storage Write API migration candidates) |
| **Interactive vs. Batch** | Workload concurrency & priority optimization | Lineage labels (dbt / Airflow / Dataform / BI / `requestor`), priority mix, queue delay, slot-hours | `UNDER_BATCHED` / `OVER_BATCHED` workload findings with copy-pasteable priority remediation snippets |
| **Data Skew Analyzer** | Join and partition bottlenecks | Max stage duration vs. Avg duration ratios | Code optimization tips for data skew |
| **AI Doctor (GenAI)** | Gemini-powered semantic SQL review via `AI.GENERATE` | SQL query texts, slot consumption | Per-query anti-pattern analysis & rewrite suggestions |
| **HBO Tracker** | History-Based Optimization proof of value | Normalized query hashes, plan expiry dates | Performance ROI & plan warm-up alerts |
| **Top Spenders** | User-level cost attribution | Per-user slot consumption, bytes processed | Top consumer identification |
| **Dashboard KPIs** | Executive overview metrics | Aggregate costs, opportunities, anomalies | One-glance FinOps health summary |

---

## 🛠️ Tech Stack

*   **Backend**: Python 3.10+, FastAPI
*   **Client Core**: Vanilla ES6 JavaScript, HTML5, Custom Glassmorphic CSS Engine
*   **Data Libraries**: NumPy, Pandas, DB-Types
*   **Data Visualization**: Chart.js, DataTables.net
*   **Google Cloud Libraries**: `google-cloud-bigquery`, `google-cloud-bigquery-storage`
*   **AI/ML**: BigQuery `AI.GENERATE` with Gemini 3.6 Flash (default) or Gemini 3.5 Flash-Lite (via Vertex AI global endpoint, `MINIMAL` thinking, safety `OFF`)
*   **Containerization**: Docker (minimal slim-python environment)

---

## 🚀 Getting Started (Local Development)

### 1. Clone & Set Up Environment
```bash
git clone https://github.com/mbettan/bq-finops-optimizer.git
cd bq-finops-optimizer

# Create and activate virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
pip install -r requirements-dev.txt   # pytest, pytest-cov
```

### 2. Run the Server

> **⚠️ IMPORTANT: The app will refuse to start without the `AUTH_ENFORCED_UPSTREAM=true` environment variable.**
>
> This is a security guardrail — the service exposes org-wide BigQuery metadata (query text, user emails, project identifiers) with no application-level authentication. It **must** run behind Cloud Run IAM (`--no-allow-unauthenticated`) or Identity-Aware Proxy (IAP) in any shared/deployed environment. Setting this variable confirms you understand this.

```bash
# Option A: Inline (recommended for quick local runs)
source venv/bin/activate
AUTH_ENFORCED_UPSTREAM=true uvicorn src.main:app --reload --port 8080

# Option B: Add to .env file (loaded automatically on every start)
echo 'AUTH_ENFORCED_UPSTREAM=true' >> .env
uvicorn src.main:app --reload --port 8080
```

Open your browser to [http://127.0.0.1:8080](http://127.0.0.1:8080) to view the interface.

### 3. Configure Settings
In the browser, open the **Settings** panel (gear icon) and set:
*   **GCP Organization Project**: The admin project with access to organization-level `INFORMATION_SCHEMA` views.
*   **Region**: The BigQuery region to analyze (e.g., `us-east4`).
*   **Focus Projects** *(optional)*: A comma-separated list of up to 50 specific project IDs to scope the analysis. When set, all org-level endpoints filter queries to only those projects using parameterized `IN UNNEST(@focus_projects)` clauses. When empty, the full organization is analyzed.
*   **Max Bytes Billed (GiB)**: Safety cap for query costs (default: 800 GiB). Applied to every single BigQuery query execution, including fluid scaling status checks.

**Input Validation:** All project ID fields are validated on save against the GCP project ID specification (`^[a-z][a-z0-9\-]{5,29}$`). Whitespace is stripped automatically (handles bad copy-paste). Invalid values block the save and show specific error messages. When settings change, all cached module results are flushed from `localStorage` to prevent stale data from a previous scope.

### 4. Environment Variables & Authentication Options

The application requires authentication to be configured before starting. You can either deploy behind Cloud Run IAM / IAP or enable direct Google OAuth 2.0.

| Variable | Default | Purpose |
| :--- | :--- | :--- |
| `AUTH_ENFORCED_UPSTREAM` | — | Set `true` if deployed behind Cloud Run IAM (`--no-allow-unauthenticated`) or Identity-Aware Proxy (IAP). |
| `GOOGLE_CLIENT_ID` | — | Google OAuth 2.0 Web Client ID for direct browser login. |
| `GOOGLE_CLIENT_SECRET` | — | Google OAuth 2.0 Client Secret for direct browser login. |
| `AUTH_SECRET_KEY` | *auto* | 32-byte hex key for signing session and state cookies across multi-instance Cloud Run deployments. |
| `ALLOWED_DOMAINS` | — | Comma-separated list of allowed Google Workspace domains (e.g. `example.com,corp.internal`). |
| `ALLOWED_USERS` | — | Comma-separated list of allowed user email addresses (e.g. `finops@example.com,lead@example.com`). |
| `AUTH_SESSION_MAX_AGE` | `604800` | Session cookie lifetime in seconds (default: 7 days). |
| `CACHE_BACKEND` | `off` | Result cache backend: `file` (for GCS FUSE or local directory), `gcs` (REST fallback), or `off`. |
| `CACHE_DIR` | `/cache` | Local path or Cloud Run GCS volume mount path (e.g. `./.cache` for local development). |
| `CACHE_TTL_DEFAULT` | `3600` | Default result TTL in seconds (1 hour). |
| `CACHE_TTL_<MODULE>` | — | Per-module TTL override in seconds (e.g. `CACHE_TTL_JOBS=1800`, `CACHE_TTL_STORAGE=7200`). |
| `CACHE_MAX_ENTRY_MB` | `32` | Maximum payload size in megabytes before skipping cache. |
| `CACHE_COMPRESS` | `auto` | Gzip compression mode: `auto` (compresses above threshold), `always`, or `never`. |
| `CACHE_COMPRESS_OVER_KB` | `512` | Payload threshold in kilobytes for `auto` compression mode. |
| `BQ_ON_DEMAND_USD_PER_TB` | `6.25` | Default on-demand rate per TB for compute simulations. |
| `BQ_EDITIONS_SLOT_HR_RATE` | `0.06` | Default Editions slot-hour baseline rate. |

---

## 🧪 Testing

The project has a comprehensive test suite that validates input boundaries, security controls, business logic, and endpoint contracts **without requiring live BigQuery credentials**.

### Run All Tests
```bash
./venv/bin/python -m pytest tests/ -v
```

### Run by Category

```bash
# Input validation (Pydantic bounds, field validators) — no BQ needed
./venv/bin/python -m pytest tests/test_input_validation.py -v

# Smoke tests (all endpoints, mocked BQ) — replaces manual frontend clicking
./venv/bin/python -m pytest tests/test_smoke_endpoints.py -v

# Security tests (SQL injection, identifier sanitization)
./venv/bin/python -m pytest tests/test_security.py -v

# Fluid scaling financial model
./venv/bin/python -m pytest tests/test_fluid_scaling.py -v

# Pattern extraction engine (job ID normalization)
./venv/bin/python -m pytest tests/test_patterns.py -v

# Project resolution & dummy project rejection
./venv/bin/python -m pytest tests/test_project_resolution.py -v

# Max bytes billed configuration
./venv/bin/python -m pytest tests/test_max_bytes_billed.py -v

# Sibling tracing (job lineage)
./venv/bin/python -m pytest tests/test_sibling_tracing.py -v

# Integration tests (requires live GCP credentials)
./venv/bin/python -m pytest tests/test_integration_client.py -v -m integration
```

### Test Suite Summary

| Test File | Tests | BQ Required? | What It Validates |
| :--- | :--- | :--- | :--- |
| `test_input_validation.py` | ~100 | ❌ No | Pydantic `Field` bounds, `@field_validator` for edition/resolution/dates/time_travel |
| `test_smoke_endpoints.py` | ~35 | ❌ Mocked | Every API endpoint returns 200 with valid payload — replaces manual UI clicking |
| `test_security.py` | 23+ | ❌ No | SQL injection rejection, identifier sanitization, edition/resolution allowlists |
| `test_fluid_scaling.py` | 30+ | ❌ No | Cooldown-waste financial model, rollup summaries, config status logic |
| `test_patterns.py` | 18 | ❌ No | Job ID regex normalization (UUID, hex, bquxjob, airflow, script chains) |
| `test_project_resolution.py` | 6 | ❌ Mocked | `init_bq_client_and_resolve_project`, `reject_dummy_project` |
| `test_max_bytes_billed.py` | 5+ | ❌ Mocked | `max_bytes_billed_gb` param propagation to `maximum_bytes_billed` |
| `test_sibling_tracing.py` | 6+ | ❌ No | Job lineage and sibling tracing via parent_job_id |
| `test_focus_filter.py` | 10 | ❌ No | `build_project_filter()` parameterization, column allow-list, alias validation |
| `test_focus_guard.py` | 41+ | ❌ Mocked | Focus projects wiring: param not silently dropped, schema acceptance, fallback guard, injection rejection |
| `test_ai_doctor.py` | 1 | ❌ Mocked | AI Doctor endpoint, `JOBS_BY_ORGANIZATION` regression assertion |
| `test_integration_client.py` | 2 | ✅ Live | End-to-end against real GCP (marked `@pytest.mark.integration`) |

---

## 🔑 Authentication & GCP Configuration

The tool utilizes **Application Default Credentials (ADC)** to establish client connections. Follow these steps to set up your environment:

### Step 1: Login via Google Cloud SDK
```bash
gcloud auth login
```

### Step 2: Configure the Target Project
```bash
gcloud config set project example-project
```

### Step 3: Configure Application Default Credentials
```bash
gcloud auth application-default login
```

### Step 4: Set API Quota Project (Crucial for organization-level INFORMATION_SCHEMA access)
```bash
gcloud auth application-default set-quota-project example-project
```

---

## 🔒 IAM Roles & Permissions

To query organization-wide metadata (`INFORMATION_SCHEMA` tables scoped with `*_BY_ORGANIZATION`), the service account or authenticated developer identity needs permissions across multiple scopes:

### 1. Project-level Permissions
*   **BigQuery Job User** (`roles/bigquery.jobUser`): Permissions to submit query jobs in the project.
    *   If using a **custom IAM role** instead of the predefined role, ensure `bigquery.jobs.create` is included — without it, the service account cannot execute any query.
*   **BigQuery Metadata Viewer** (`roles/bigquery.metadataViewer`): Permissions to inspect table definitions and schemas.

### 2. Organization-level Permissions (Required for Organisation-scoped Views)
*   **BigQuery Resource Viewer** (`roles/bigquery.resourceViewer`): Required to query org-scoped `INFORMATION_SCHEMA` views — `JOBS_TIMELINE_BY_ORGANIZATION`, `JOBS_BY_ORGANIZATION`, `RESERVATIONS`, and `CAPACITY_COMMITMENT_CHANGES`. Provides read-only access to reservation hierarchies and job telemetry without the ability to create, modify, or delete reservations or commitments.
*   **BigQuery Metadata Viewer** (`roles/bigquery.metadataViewer`) *(optional, at Organization level)*: Enables a fast-path batched `UNION ALL` query across all projects' `SCHEMATA_OPTIONS` for Storage Analysis, and provides access to `TABLE_STORAGE_BY_ORGANIZATION`. Without this, the app automatically falls back to a slower project-by-project loop — the only impact is longer scan times on large organizations.

### 3. Active Assist / Recommender Permissions (Required for Recommendations Module)
*   **BigQuery Partitioning Clustering Recommender Viewer** (`roles/recommender.bigqueryPartitionClusterViewer`): Crucial for retrieving Google's native Active Assist partitioning and clustering recommendations.
    *   **Organization-level Grant (Recommended)**: Must be granted at the **Organization** resource level to query the organization-wide `INFORMATION_SCHEMA.RECOMMENDATIONS_BY_ORGANIZATION` view.
    *   **Project-level Grant**: Can be granted at the individual **Project** level if only analyzing specific standalone projects.

### 4. AI Doctor Permissions (Required for GenAI Query Analysis)
The AI Doctor is the **only module that uses AI**. It calls BigQuery's `AI.GENERATE` function with the Vertex AI publisher endpoint — **no model creation, no remote model, no BigQuery ML dataset, and no Cloud Resource Connection are required** for the default setup.

*   **End-User Credentials (default, zero setup)**:
    *   **Vertex AI User** (`roles/aiplatform.user`): Permission to invoke Gemini models on the project. This is the only IAM role needed.
    *   **Vertex AI API**: Must be enabled on the execution project (`gcloud services enable aiplatform.googleapis.com`).
    *   No `CREATE MODEL`, no connection, no dataset — `AI.GENERATE` calls the publisher endpoint directly.

*   **Cloud Resource Connection (optional, for service accounts / Cloud Run)**: If you specify a Connection Name in Settings (e.g., `us.vertexai`), the connection's service account is used instead of your ADC credentials. Grant the SA the required role:
    ```bash
    gcloud projects add-iam-policy-binding example-project \
      --member="serviceAccount:example-sa@example-project.iam.gserviceaccount.com" \
      --role="roles/aiplatform.user"
    ```

> **All other modules** (Storage, Compute, Slots, HBO, Cost Attribution, Anti-Patterns, Governance, etc.) require **zero AI permissions** — they use only BigQuery `INFORMATION_SCHEMA` SQL queries and Python analytics.

---

## ⚠️ Security & Scale Considerations

1. **Self-Hosted / Local Usage Focus**: The dashboard has no built-in auth system. **Do not expose this application directly to the public internet** without setting up an Identity-Aware Proxy (IAP) or an authentication gateway.
2. **Input Validation (Backend)**: All API parameters are validated through Pydantic `Field` constraints and `@field_validator` decorators. SQL identifiers pass through `_safe_ident()` regex validation. Date parameters use parameterized queries to prevent SQL injection.
3. **Input Validation (Frontend)**: All project ID text inputs are validated against the GCP project ID regex (`^[a-z][a-z0-9\-]{5,29}$`) before saving. Whitespace is stripped on save, and invalid values block persistence.
4. **Error Handling**: All endpoints use a centralized `handle_endpoint_exception()` function that maps GCP error types to safe HTTP responses without leaking internal details.
5. **Large-Scale Quotas**: Querying organization-wide metrics on high-volume environments (10,000+ datasets) can cause query timeout or quota limits. All `lookback_days` parameters are capped at 90 days, and query byte limits are enforced via `maximum_bytes_billed`. The default cap is **800 GiB** — for very large organizations, increase the **Max Bytes Billed (GiB)** setting in the Global Configuration panel.
6. **AI Doctor Cost Control**: `AI.GENERATE` calls are rate-limited by BigQuery's built-in generative AI quotas. The default query limit is 20 queries per analysis run (configurable up to 100). Each Gemini call uses `thinking_level: MINIMAL` and `max_output_tokens: 1024` to ensure complete bullet-point outputs without truncation while minimizing token overhead.

---

## 📊 Observability & Logging

Every API endpoint and BigQuery query produces structured logs designed for real-time monitoring and troubleshooting:

| Icon | Meaning | Example |
|:----:|:--------|:--------|
| `▶` | **Endpoint started** — project, region, scope, lookback, safety cap | `▶ Job Analysis — project=example-org \| scope=1 projects (example-project) \| safety_cap=800 GiB` |
| `⏳` | **Query submitted** — which query is running and the active safety cap | `⏳ Storage Metrics — submitting query (safety cap: 800 GiB)…` |
| `✅` | **Query completed** — elapsed time, bytes processed/billed, cache hit, BQ Console URL | `✅ Storage Metrics — 2.1s \| Processed: 0.42 GiB \| Cache: False \| https://…` |
| `◼` | **Endpoint completed** — total elapsed time for the full request | `◼ Job Analysis — completed in 4.3s` |

*   **Request Correlation IDs**: Every log line includes an 8-character hex request ID (e.g., `[a3f1b2c4]`) automatically injected via middleware. This allows you to trace an entire request's lifecycle—from `▶` to `◼`—even when logs from concurrent requests are interleaved. Use `grep a3f1b2c4 app.log` to isolate a single request.
*   **BQ Console URLs**: Every query log includes a clickable URL that opens the job results directly in the GCP Console.
*   **SQL Tracing**: Full SQL text is logged at `DEBUG` level for every query. Set `LOG_LEVEL=DEBUG` to enable.
*   **Safety Visibility**: The safety cap (in GiB) is shown in both the endpoint start log and every query submission.
*   **Centralized Query Helpers**: All BigQuery query executions are routed through instrumented helpers (`run_query_and_log`, `run_query_to_df`, `_run_and_log`) that enforce the `maximum_bytes_billed` safety cap and emit the `⏳`/`✅` log sequence with timing, byte stats, and BQ Console URLs. Zero bare `client.query()` calls exist outside these helpers.

### Log Format

Each log line follows this format:
```
%(asctime)s - %(name)s - %(levelname)s - [%(request_id)s] %(message)s
```

Example output with interleaved requests:
```
2026-07-08 04:14:00 - src.main - INFO - [a3f1b2c4] ▶ Storage Analysis — project=example-org | region=us | scope=full organization | safety_cap=800 GiB
2026-07-08 04:14:00 - src.main - INFO - [a3f1b2c4] ⏳ Storage Metrics — submitting query (safety cap: 800 GiB)…
2026-07-08 04:14:02 - src.main - INFO - [b7e9d0f1] ▶ HBO Analyze — project=example-org | region=us | ...
2026-07-08 04:14:03 - src.main - INFO - [a3f1b2c4] ✅ Storage Metrics — 3.2s | Job: bqjob_r123 | Processed: 0.42 GiB | Billed: 0.42 GiB | Cache: False | https://...
2026-07-08 04:14:03 - src.main - INFO - [a3f1b2c4] ◼ Storage Analysis — completed in 3.4s
```

Startup logs (emitted before any request) show `[--------]` as the request ID.

### Configuring Log Level

```bash
# Normal operation (default) — shows progress, timing, BQ URLs
AUTH_ENFORCED_UPSTREAM=true uvicorn src.main:app --reload

# Troubleshooting — also shows full SQL for every query
AUTH_ENFORCED_UPSTREAM=true LOG_LEVEL=DEBUG uvicorn src.main:app --reload
```

---

## ⚠️ Disclaimer

> [!IMPORTANT]
> **This is not a Google product.** It is an independent, personal open-source
> project (Apache 2.0), not affiliated with, endorsed by, or supported by Google.
>
> All costs shown are **modelled estimates at public list prices** — treat the
> Cloud Billing Console as your source of truth and verify before acting.
>
> This tool runs on your infrastructure, under your credentials, at your expense,
> and executes billable BigQuery and Vertex AI jobs. Review the code before running
> it, and test all generated SQL/DDL in a non-production environment first.
>
> Provided **AS IS**, without warranty of any kind, with no liability for any
> outcome — including unexpected cloud charges.
>
> **→ Full terms: [DISCLAIMER.md](DISCLAIMER.md)**

