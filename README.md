# ⚡ FinOps Optimizer for BigQuery

[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![CI](https://github.com/mbettan/bq-finops-optimizer/actions/workflows/ci.yml/badge.svg)](https://github.com/mbettan/bq-finops-optimizer/actions/workflows/ci.yml)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.100+-green.svg)](https://fastapi.tiangolo.com)
[![GitHub Pages](https://img.shields.io/badge/Live%20Demo-GitHub%20Pages-orange.svg)](https://mbettan.github.io/bq-finops-optimizer/simulator.html)
<a href="https://shell.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https://github.com/mbettan/bq-finops-optimizer.git"><img src="https://gstatic.com/cloudssh/images/open-btn.svg" height="20" alt="Open in Cloud Shell"></a>

> *Not a Google product — an independent open-source project. Costs shown are modelled estimates; see [DISCLAIMER.md](DISCLAIMER.md).*

An open-source, self-hosted BigQuery FinOps diagnostic toolkit and interactive simulation engine. It analyzes historical telemetry, query workloads, and storage configurations across Google Cloud Organizations to maximize cost efficiency, eliminate compute waste, and generate automated governance remediations.

* **Documentation & Architecture:** [FinOps Optimizer Documentation](https://mbettan.github.io/bq-finops-optimizer/)
* **Interactive Simulator:** [Live FinOps Simulator](https://mbettan.github.io/bq-finops-optimizer/simulator.html)
* **Runtime Cost Calculator:** [FinOps ROI & Runtime Economics Calculator](https://mbettan.github.io/bq-finops-optimizer/#calculator)

---

## 🎯 Core Capabilities & FinOps Methodologies

### Key Technical Innovations
* **Slot Capacity Bucket Simulation**: Vectorized 730-hour NumPy simulation of historical slot consumption to right-size BigQuery Editions baselines, autoscale ceilings, and Aggressive / Balanced / Performance recommendation tiers.
* **Fluid Scaling & Cooldown Tax Mitigation**: Detects high-frequency, short-duration workloads penalized by the 60-second autoscaling minimum and models savings from migrating to **Fluid Scaling** (per-second billing with zero baseline).
* **Storage Billing & 4-State Risk Model**: Audits active/long-term logical vs. physical storage bytes across datasets, classifies unpartitioned/unclustered tables into a transparent 4-tier risk model (**Critical**, **High**, **Medium**, **Low**), and generates `ALTER SCHEMA` DDL scripts.
* **Hybrid Cost Attribution**: Solves central admin-project waste dumping by mapping org-wide slot usage back to reservations and proportionally reallocating idle/unallocated slot capacity across business units (`Lender Pays` vs. `Borrower Pays`).
* **History-Based Optimization (HBO) Proof of Value**: Matches optimized execution plans with historical baseline runs via `normalized_literals` query hashes to quantify execution savings and flag plans nearing the 130-day expiration window.
* **AI-Powered Query Analysis (AI Doctor)**: Uses BigQuery's native `AI.GENERATE` with Gemini (`gemini-3.7-flash` or `gemini-3.5-flash-lite`) over aggregated `JOBS_BY_ORGANIZATION` workload templates. Ranks candidates across 5 prioritization strategies (Balanced ROI, Cumulative Cost, High Frequency, Memory Spill, Total Slot Time) without requiring `CREATE MODEL` or custom ML datasets.
* **Zero Data-Plane Access Guarantee**: Strictly inspects control-plane telemetry via `INFORMATION_SCHEMA`. **Never requests, holds, or uses `bigquery.tables.getData`** — guaranteeing it cannot read a single row of customer table data.

### Modules Overview

| Module | Purpose | Key Telemetry / Metrics | Actionable Output |
| :--- | :--- | :--- | :--- |
| **Storage Optimizer** | Logical vs. physical storage auditing & table risk scoring | Active/long-term bytes, compression ratios, partition/cluster state | `ALTER SCHEMA` DDL generator & 4-tier remediation queue |
| **Active Assist** | Native partitioning & clustering insights | Google Cloud Recommender API | One-click recommendation viewer & DDL export |
| **Compute Analyzer** | On-Demand vs. Editions workload comparison | Slot hours vs. bytes billed per project | Workload billing model selector |
| **Capacity Planner** | Baseline & autoscaling slot simulation | Historical per-minute slot demand (NumPy) | Tiered baseline matrix (Aggressive / Balanced / Performance) |
| **Fluid Scaling Simulator** | Cooldown tax & short-query waste analysis | Billing time-blocks, query execution frequencies | Fluid Scaling migration candidates & savings forecast |
| **Cost Attribution Engine** | Proportional idle slot waste chargeback | `JOBS_BY_ORGANIZATION` & reservation timelines | Split-cost CSV/JSON chargeback reports |
| **Query Anti-Pattern Linter** | Static SQL auditing & owner roll-up | `SELECT *`, unclustered scans, missing filters | Owner-grouped SQL remediation work queue |
| **Storage Hygiene Auditor** | Table churn & time-travel physical bloat | Time-travel bytes, table update frequency, dataset TTL | Time-travel window reduction DDL per dataset |
| **DML & MV Auditor** | High-frequency DML & materialized view refresh audit | Single-row DML write loops, MV refresh slot costs | Storage Write API migration & MV refactoring alerts |
| **Interactive vs. Batch** | Workload concurrency & queue optimization | Job lineage labels (`dbt`, `Airflow`, `BI`), queue delay | `UNDER_BATCHED` / `OVER_BATCHED` priority remediation |
| **Data Skew & BI Engine** | Shuffle bottlenecks & BI Engine cache misses | Max vs. avg stage duration ratios, BI Engine miss reasons | Join skew refactoring & BI Engine sizing guidance |
| **AI Doctor (GenAI)** | Semantic SQL review via `AI.GENERATE` | Multi-stage spill unnesting, worst-job sampling | Per-query anti-pattern analysis & rewrite suggestions |
| **HBO & Governance** | Plan expiration tracking & policy enforcement | Normalized query hashes, partition filter compliance | Expiring plan warm-up alerts & non-compliant table inventory |

Every table supports **Universal CSV Export** (full filtered dataset with UTF-8 BOM), **BigQuery Console Deep-Links**, and **Snapshot Export/Import** (`.json` state sharing with optional PII/SQL redaction so colleagues can hydrate the UI offline without GCP credentials).

---

## 🚀 Deployment & Quickstart

**Prerequisites**: Python 3.10+ and Google Cloud SDK (`gcloud`). Supports **macOS**, **Linux**, **Windows**, **Google Cloud Shell**, and **Cloud Run**.

### 1. Configure GCP Authentication (ADC)
The toolkit uses Application Default Credentials (ADC) to query `INFORMATION_SCHEMA` metadata:

```bash
# Authenticate and set active project
gcloud auth login
gcloud config set project <YOUR_PROJECT_ID>

# Configure Application Default Credentials and API quota project
gcloud auth application-default login
gcloud auth application-default set-quota-project <YOUR_PROJECT_ID>
```

### 2. Choose Your Launch Method

#### Option A: One-Click Local Launch (macOS / Linux / Windows)
The included launchers auto-detect Python, create a virtual environment, install dependencies, and open your browser at `http://127.0.0.1:8080`:

* **macOS / Linux:**
  ```bash
  ./run.sh
  ```
* **Windows (Command Prompt or File Explorer):**
  Double-click **`run.bat`** or run in PowerShell:
  ```powershell
  powershell -ExecutionPolicy Bypass -File .\run.ps1
  ```
* **Manual Setup (Any OS):**
  ```bash
  python3 -m venv venv
  source venv/bin/activate          # Windows: .\venv\Scripts\Activate.ps1
  pip install -r requirements.txt
  uvicorn src.main:app --reload --port 8080
  ```

#### Option B: Google Cloud Shell (Zero Local Setup)
Run directly in your browser with Python and the Cloud SDK pre-installed:

[![Open in Cloud Shell](https://gstatic.com/cloudssh/images/open-btn.svg)](https://shell.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https://github.com/mbettan/bq-finops-optimizer.git)

```bash
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt
gcloud config set project <YOUR_PROJECT_ID>
gcloud auth application-default set-quota-project <YOUR_PROJECT_ID>
uvicorn src.main:app --host 0.0.0.0 --port 8080
```
Click the **Web Preview** icon in the top-right Cloud Shell toolbar and select **Preview on port 8080**.

#### Option C: Deploy to Cloud Run (Serverless Container)
Deploy as a private serverless container governed by Cloud Run IAM (`--no-allow-unauthenticated`). The deployment script supports a split-privilege workflow:

```bash
# 1. Security/Platform Admin: Emit organization IAM bindings for review (zero mutations)
deploy/deploy_cloud_run.sh --emit-iam --org-id <YOUR_ORG_ID> --project <YOUR_PROJECT_ID>

# 2. Evaluator/Deployer: Build and deploy private Cloud Run service
deploy/deploy_cloud_run.sh --deploy --project <YOUR_PROJECT_ID> --region us-central1

# 3. Connect securely from your workstation via authenticated proxy
gcloud run services proxy bq-finops-optimizer --project <YOUR_PROJECT_ID> --region us-central1
```

### 3. Configure Settings in UI
Open the **Settings** panel (gear icon) in the web interface:
* **GCP Organization Project**: Admin/billing project used to execute organization-level `INFORMATION_SCHEMA` queries.
* **Region**: Select from 40+ BigQuery multi-regions and regional endpoints (`US`, `EU`, `us-central1`, `europe-west1`, etc.).
* **Focus Projects *(optional)***: Comma-separated list of up to 50 project IDs to scope analysis while preserving org-wide mathematical invariants.
* **Max Bytes Billed (GiB)**: Hard safety cap enforced on every query execution (default: `800 GiB`).

---

## 🔒 IAM Roles & Security Guardrails

### 🛡️ Control Plane vs. Data Plane Boundary
FinOps Optimizer inspects **control-plane metadata only** (`JOBS_BY_ORGANIZATION`, `TABLE_STORAGE`, `RESERVATIONS`). It **never requests or uses `bigquery.tables.getData`** and cannot access customer table rows.

### Option A: Custom Least-Privilege IAM Role (Enterprise Recommended)
Deploy the auditable custom role (`deploy/roles/bq_finops_reader.yaml`) which grants metadata read access while explicitly excluding `bigquery.tables.getData` and all mutation permissions:

```bash
# 1. Create custom role at Organization level
gcloud iam roles create bqFinOpsReader \
  --organization=<YOUR_ORGANIZATION_ID> \
  --file=deploy/roles/bq_finops_reader.yaml

# 2. Bind custom role at Organization level & Job User at Execution Project level
gcloud organizations add-iam-policy-binding <YOUR_ORGANIZATION_ID> \
  --member="serviceAccount:bq-finops-sa@<YOUR_PROJECT_ID>.iam.gserviceaccount.com" \
  --role="organizations/<YOUR_ORGANIZATION_ID>/roles/bqFinOpsReader"

gcloud projects add-iam-policy-binding <YOUR_PROJECT_ID> \
  --member="serviceAccount:bq-finops-sa@<YOUR_PROJECT_ID>.iam.gserviceaccount.com" \
  --role="roles/bigquery.jobUser"
```

### Option B: Standard Predefined GCP Roles
If using predefined roles, grant:
* **Execution Project**: `roles/bigquery.jobUser` + `roles/bigquery.metadataViewer`
* **Organization Level**: `roles/bigquery.resourceViewer` + `roles/bigquery.metadataViewer`
* **Active Assist Module *(optional)***: `roles/recommender.bigqueryPartitionClusterViewer` (Org or Project level)
* **AI Doctor Module *(optional)***: `roles/aiplatform.user` on the execution project (with `aiplatform.googleapis.com` enabled). No `CREATE MODEL` or Cloud Resource Connection required for default ADC usage.

### 🔍 Machine-Verifiable Permission Auditor
Verify your active credentials or service account before running scans using the zero-dependency permission verifier (`deploy/check_permissions.py`):

```bash
python3 deploy/check_permissions.py --project <YOUR_PROJECT_ID> --org-id <YOUR_ORGANIZATION_ID>
```

| Exit Code | Status | Meaning & Action |
| :--- | :--- | :--- |
| `0` | **PASS** | All required telemetry permissions held; **`bigquery.tables.getData` verified absent**. |
| `1` | **MISSING REQUIRED** | Lists missing permissions and the exact modules they unblock. |
| `2` | **DATA-PLANE DETECTED** | Identity holds `bigquery.tables.getData`. Remove broad roles (`dataViewer`/`admin`) to enforce zero data-plane access, or use `--impersonate-service-account` to test the runtime SA. |
| `3` | **ERROR** | Network/auth failure (e.g., run `gcloud auth application-default login`). |

---

## 💰 Runtime Economics & Google Cloud Costs

Diagnostic queries scan `INFORMATION_SCHEMA` views and **never scan production user tables**.

| Workload Scenario | BigQuery Metadata Scan | Cloud Run Hosting | AI Doctor (Gemini) | Total Estimated Cost |
| :--- | :--- | :--- | :--- | :--- |
| **Interactive Ad-Hoc Run** | $0.00006 – $0.05 (On-Demand) / **$0.00 (Editions)** | $0.0035 (2 min) | $0.153 (50 queries, 3.5 Flash-Lite) | **~$0.16 – $0.21 / run** |
| **Deep Org Sweep (800 GB metadata)** | ~$4.88 (On-Demand) / **$0.00 (Editions)** | $0.017 (10 min) | $0.153 (3.5 Flash-Lite) | **~$0.17 – $5.05 / run** |
| **Automated Daily Sweeps (Monthly)** | $0.05 – $1.50 / mo | $0.52 / mo (`min-instances=0`) | $0.61 / mo (weekly AI review) | **~$1.20 – $2.65 / month** |

---

## 🧪 Testing & Development

The test suite validates input boundaries, SQL identifier sanitization (`_safe_ident`), financial models, and API contracts **100% offline without live GCP credentials**.

```bash
# Run full offline unit test suite (760+ tests)
pytest tests/ -v

# Run specific test suites
pytest tests/test_smoke_endpoints.py -v    # API endpoint contract tests
pytest tests/test_security.py -v           # SQL injection & identifier sanitization
pytest tests/test_bundle_sync.py -v        # Verifies static/ and docs/ bundle sync
```

### Frontend & Region Synchronization
Whenever modifying frontend assets in `static/` (`index.html`, `app.js`, `style.css`) or updating `BQ_REGIONS` in `src/constants.py`, synchronize the static GitHub Pages simulator bundle before committing:

```bash
# Synchronize static/ assets and simulator HTML body into docs/
./scripts/sync_docs_bundle.sh

# Regenerate region <select> options across HTML templates
python3 scripts/generate_region_options.py
```

### Observability & Logging
All BigQuery executions route through centralized helpers (`run_query_and_log`, `run_query_to_df`) that enforce `maximum_bytes_billed` and emit structured request-correlated logs (`[req_id] ▶ Started`, `⏳ Submitted`, `✅ Completed` with elapsed time, GiB billed, and clickable BigQuery Console job URLs). Set `LOG_LEVEL=DEBUG` to log full SQL statements.

---

## 🛠️ Tech Stack & Disclaimer

* **Backend**: Python 3.10+, FastAPI, Pandas, NumPy, `google-cloud-bigquery`
* **Frontend**: Vanilla ES6 JavaScript, HTML5, CSS3, Chart.js, DataTables
* **AI/ML**: BigQuery `AI.GENERATE` with Vertex AI Gemini 3.7 Flash / 3.5 Flash-Lite

> [!IMPORTANT]
> **Disclaimer:** This is not an official Google product. All financial figures are modelled estimates based on public list prices — verify against the Google Cloud Billing Console before modifying production infrastructure. Provided **AS IS** under the Apache 2.0 License without warranty of any kind. See [DISCLAIMER.md](DISCLAIMER.md) for full terms.
