# ⚡ BigQuery FinOps Optimizer

[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.100+-green.svg)](https://fastapi.tiangolo.com)
[![GitHub Pages](https://img.shields.io/badge/Live%20Demo-GitHub%20Pages-orange.svg)](https://mbettan.github.io/bq-finops-optimizer/simulator.html)

An enterprise-grade BigQuery FinOps diagnostic suite and interactive simulation engine. It analyzes historical telemetry, query workloads, and storage configurations across Google Cloud Organizations to maximize cost efficiency, reduce compute waste, and implement automated governance.

🚀 **Try the Interactive Demo directly in your browser:** [BigQuery FinOps Simulator](https://mbettan.github.io/bq-finops-optimizer/simulator.html)

---

## 📖 Table of Contents
1. [Core Optimization Pillars](#-core-optimization-pillars)
2. [FinOps Methodologies & Technical Innovations](#%EF%B8%8F-finops-methodologies--technical-innovations)
3. [Modules & Capabilities](#-modules--capabilities)
4. [Tech Stack](#-tech-stack)
5. [Getting Started (Local Development)](#-getting-started-local-development)
6. [Testing](#-testing)
7. [Authentication & GCP Configuration](#-authentication--gcp-configuration)
8. [IAM Roles & Permissions](#-iam-roles--permissions)
9. [Security & Scale Considerations](#-security--scale-considerations)
10. [Disclaimer](#-disclaimer)

---

## 🎯 Core Optimization Pillars

*   **Storage Optimization**: Identifies and automates transitions between logical and physical billing models.
*   **Compute Right-Sizing**: Evaluates On-Demand vs. Editions pricing, simulates optimal baseline capacities, and analyzes autoscaler performance.
*   **Architectural Diagnostics**: Identifies anti-patterns such as DML abuse, redundant materialized views, and slot-inefficient query designs.
*   **Cost Attribution**: Overcomes GCP billing limitations to proportionally distribute unallocated reservation waste back to business units.
*   **AI-Powered Query Analysis**: Uses Gemini models via BigQuery's `AI.GENERATE` function to perform semantic SQL review at scale.

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
Uses BigQuery's native `AI.GENERATE` scalar function with Gemini models to perform semantic SQL review. For each query:
*   Retrieves the most expensive queries by slot consumption from `INFORMATION_SCHEMA.JOBS_BY_PROJECT`.
*   Sends each SQL statement to Gemini with a structured prompt that checks for 7 common anti-patterns (e.g., `SELECT *`, missing `WHERE` before `JOIN`, `CROSS JOIN`, `COUNT(DISTINCT)` vs. `APPROX_COUNT_DISTINCT`).
*   Uses the state-of-the-art **Gemini 3.1 Flash Lite** model via the global Vertex AI publisher endpoint for high-speed, cost-efficient semantic analysis.
*   Works with end-user credentials (no connection required) or via a BigQuery Cloud Resource Connection for service account authentication.

---

## 🔍 Modules & Capabilities

| Module | Purpose | Key Telemetry / Metrics | Actionable Output |
| :--- | :--- | :--- | :--- |
| **Storage Optimizer** | Logical vs. Physical Storage Auditing | Active/Long-term storage bytes, change rates | `ALTER SCHEMA` DDL generator |
| **Active Assist** | Google-native partitioning & clustering recommendations | Recommender API insights | One-click recommendation viewer |
| **Compute Analyzer** | Compute billing model comparisons | Slot hours vs. Bytes billed | Project/workload billing model selector |
| **Capacity Planner** | Real-time capacity sizing & baseline simulation | Simulated hourly slot-hour logs (NumPy) | Quantile-based reservation baseline matrix |
| **Tiered Recommendations** | Multi-tier baseline capacity suggestions | Per-minute peak slot analysis | Aggressive / Balanced / Performance baselines |
| **Fluid Scaling Simulator** | Cooldown tax and Fluid Scaling evaluation | Billing time-blocks, execution frequencies | High-frequency workload isolation candidates |
| **Cost Attribution Engine** | Custom cost splitting and billing attribution | `JOBS_BY_ORGANIZATION` telemetry | Split-cost CSV/JSON reports |
| **Workload Profiler** | Continuous-trickle query detection | Short execution patterns, reservation usage | Isolated reservation strategies & top queries |
| **Query Anti-Pattern Linter** | Static SQL auditing and performance advice | `SELECT *` patterns, unclustered limits | "SQL Wall of Shame" reporting |
| **Storage Hygiene Auditor** | Table churn and time travel tracking | Time travel physical bytes, table updates | Time travel window reductions |
| **BI Engine Optimizer** | BI Engine utilization analysis | BI Engine mode (FULL/PARTIAL/NONE), miss reasons | BI Engine cache diagnostics |
| **Governance & Expiration** | Schema policies and safety check | Expiration settings, partition filters | Non-compliant resource inventory |
| **Resource Warnings** | Dataset/table security & config flags | Public access, stale data, missing policies | Risk-scored warning inventory |
| **DML & MV Auditor** | Materialized view and write operations audits | Refresh costs, single-row DML write loops | MV and ingestion refactoring alerts |
| **Interactive vs. Batch** | Batch candidate identification | Priority flags, off-peak times, runtime | Batch priority suggestion dashboard |
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
*   **AI/ML**: BigQuery `AI.GENERATE` with Gemini models (via Vertex AI global endpoint)
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
```bash
uvicorn src.main:app --reload --port 8080
```
Open your browser to [http://127.0.0.1:8080](http://127.0.0.1:8080) to view the interface.

### 3. Configure Settings
In the browser, open the **Settings** panel (gear icon) and set:
*   **GCP Organization Project**: The admin project with access to organization-level `INFORMATION_SCHEMA` views.
*   **Region**: The BigQuery region to analyze (e.g., `us-east4`).
*   **Max Bytes Billed (GiB)**: Safety cap for query costs (default: 200 GiB, max: 10 TiB).
*   **Connection Name** *(optional)*: A BigQuery Cloud Resource Connection for AI Doctor (e.g., `us.vertexai`). Leave empty to use end-user credentials.

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
| `test_ai_doctor.py` | 1 | ❌ Mocked | AI Doctor endpoint with mocked LLM results |
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
gcloud config set project YOUR_PROJECT_ID
```

### Step 3: Configure Application Default Credentials
```bash
gcloud auth application-default login
```

### Step 4: Set API Quota Project (Crucial for organization-level INFORMATION_SCHEMA access)
```bash
gcloud auth application-default set-quota-project YOUR_PROJECT_ID
```

---

## 🔒 IAM Roles & Permissions

To query organization-wide metadata (`INFORMATION_SCHEMA` tables scoped with `*_BY_ORGANIZATION`), the service account or authenticated developer identity needs permissions across multiple scopes:

### 1. Project-level Permissions
*   **BigQuery Job User** (`roles/bigquery.jobUser`): Permissions to submit query jobs in the project.
*   **BigQuery Metadata Viewer** (`roles/bigquery.metadataViewer`): Permissions to inspect table definitions and schemas.

### 2. Organization-level Permissions (Required for Organisation-scoped Views)
*   **BigQuery Resource Admin** (`roles/bigquery.resourceAdmin`): Required to retrieve slot metrics from `JOBS_TIMELINE_BY_ORGANIZATION`, `JOBS_BY_ORGANIZATION`, and `TABLE_STORAGE_BY_ORGANIZATION`. Also enables viewing reservation hierarchies.

### 3. Dataset-level Permissions (Optional - for inline execution)
*   **BigQuery Data Owner** (`roles/bigquery.dataOwner`): Required if executing the DDL commands to change storage models directly from the dashboard.

### 4. Active Assist / Recommender Permissions (Required for Recommendations Module)
*   **BigQuery Partitioning Clustering Recommender Viewer** (`roles/recommender.bigqueryPartitionClusterViewer`): Crucial for retrieving Google's native Active Assist partitioning and clustering recommendations.
    *   **Organization-level Grant (Recommended)**: Must be granted at the **Organization** resource level to query the organization-wide `INFORMATION_SCHEMA.RECOMMENDATIONS_BY_ORGANIZATION` view.
    *   **Project-level Grant**: Can be granted at the individual **Project** level if only analyzing specific standalone projects.
*   **Recommender Viewer** (`roles/recommender.viewer`): General viewer role providing broader read access across Google's recommendation engines (can also be granted at the Org or Project level depending on target scope).

### 5. AI Doctor Permissions (Required for GenAI Query Analysis)
The AI Doctor module uses BigQuery's `AI.GENERATE` function with Gemini models. Authentication works in two modes:

*   **End-User Credentials (default, no connection needed)**: If you leave the Connection Name empty in Settings, `AI.GENERATE` uses your own ADC credentials. Your account needs:
    *   **Vertex AI User** (`roles/aiplatform.user`): Permission to invoke Gemini models on the project.

*   **Cloud Resource Connection (for service accounts / Cloud Run)**: If you specify a Connection Name (e.g., `us.vertexai`), the connection's service account is used. Grant the SA the required role:
    ```bash
    gcloud projects add-iam-policy-binding YOUR_PROJECT_ID \
      --member="serviceAccount:CONNECTION_SA_EMAIL" \
      --role="roles/aiplatform.user"
    ```

---

## ⚠️ Security & Scale Considerations

1. **Self-Hosted / Local Usage Focus**: The dashboard has no built-in auth system. **Do not expose this application directly to the public internet** without setting up an Identity-Aware Proxy (IAP) or an authentication gateway.
2. **Input Validation**: All API parameters are validated through Pydantic `Field` constraints and `@field_validator` decorators. SQL identifiers pass through `_safe_ident()` regex validation. Date parameters use parameterized queries to prevent SQL injection.
3. **Error Handling**: All endpoints use a centralized `handle_endpoint_exception()` function that maps GCP error types to safe HTTP responses without leaking internal details.
4. **Large-Scale Quotas**: Querying organization-wide metrics on high-volume environments (10,000+ datasets) can cause query timeout or quota limits. All `lookback_days` parameters are capped at 90 days, and query byte limits are enforced via `maximum_bytes_billed`. The default cap is **200 GiB** — for very large organizations, increase the **Max Bytes Billed (GiB)** setting in the Global Configuration panel (up to 10 TiB).
5. **AI Doctor Cost Control**: `AI.GENERATE` calls are rate-limited by BigQuery's built-in generative AI quotas. The default query limit is 20 queries per analysis run (configurable up to 100). Each Gemini call uses `thinking_budget: 0` and `max_output_tokens: 300` to minimize token costs.

---

## ⚠️ Disclaimer

This tool performs simulations based on historical BigQuery metadata. Simulated pricing estimates and recommended metrics may not fully capture enterprise-specific Google Cloud pricing structures, custom discounts, or blended flat-rate allocations. 

Always review proposed DDL and reservation alterations manually before applying updates to production environments.
