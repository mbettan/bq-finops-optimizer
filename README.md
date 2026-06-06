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
6. [Authentication & GCP Configuration](#-authentication--gcp-configuration)
7. [IAM Roles & Permissions](#-iam-roles--permissions)
8. [Security & Scale Considerations](#-security--scale-considerations)
9. [Disclaimer](#-disclaimer)

---

## 🎯 Core Optimization Pillars

*   **Storage Optimization**: Identifies and automates transitions between logical and physical billing models.
*   **Compute Right-Sizing**: Evaluates On-Demand vs. Editions pricing, simulates optimal baseline capacities, and analyzes autoscaler performance.
*   **Architectural Diagnostics**: Identifies anti-patterns such as DML abuse, redundant materialized views, and slot-inefficient query designs.
*   **Cost Attribution**: Overcomes GCP billing limitations to proportionally distribute unallocated reservation waste back to business units.

---

## ⚙️ FinOps Methodologies & Technical Innovations

### 1. The "Geh Bucket Method" (Compute Capacity Simulation)
To avoid over-provisioning baseline capacity or over-paying for autoscaled slots, the system uses the **Geh Bucket Method**. It runs a vectorized simulation using NumPy to model slot consumption over a standard 730-hour billing month:
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

---

## 🔍 Modules & Capabilities

| Module | Purpose | Key Telemetry / Metrics | Actionable Output |
| :--- | :--- | :--- | :--- |
| **Storage Optimizer** | Logical vs. Physical Storage Auditing | Active/Long-term storage bytes, change rates | `ALTER SCHEMA` DDL generator |
| **Compute Analyzer** | Compute billing model comparisons | Slot hours vs. Bytes billed | Project/workload billing model selector |
| **Capacity Planner** | Real-time capacity sizing & baseline simulation | Simulated hourly slot-hour logs (NumPy) | Quantile-based reservation baseline matrix |
| **Fluid Scaling Simulator** | Cooldown tax and Fluid Scaling evaluation | Billing time-blocks, execution frequencies | High-frequency workload isolation candidates |
| **Cost Attribution Engine** | Custom cost splitting and billing attribution | `JOBS_BY_ORGANIZATION` telemetry | Split-cost CSV/JSON reports |
| **Workload Profiler** | Continuous-trickle query detection | Short execution patterns, reservation usage | Isolated reservation strategies |
| **Query Anti-Pattern Linter** | Static SQL auditing and performance advice | `SELECT *` patterns, unclustered limits | "SQL Wall of Shame" reporting |
| **Storage Hygiene Auditor** | Table churn and time travel tracking | Time travel physical bytes, table updates | Time travel window reductions |
| **BI Engine Optimizer** | BI Engine utilization analysis | BI Engine mode (FULL/PARTIAL/NONE), miss reasons | BI Engine cache diagnostics |
| **Governance & Expiration** | Schema policies and safety check | Expiration settings, partition filters | Non-compliant resource inventory |
| **DML & MV Auditor** | Materialized view and write operations audits | Refresh costs, single-row DML write loops | MV and ingestion refactoring alerts |
| **Interactive vs. Batch** | Batch candidate identification | Priority flags, off-peak times, runtime | Batch priority suggestion dashboard |
| **Data Skew Analyzer** | Join and partition bottlenecks | Max stage duration vs. Avg duration ratios | Code optimization tips for data skew |
| **LLM Query Analyst (GenAI)** | Vertex AI integration for query reviews | SQL query texts, database schemas | AI-generated rewrite suggestions |

---

## 🛠️ Tech Stack

*   **Backend**: Python 3.10+, FastAPI (Asynchronous framework)
*   **Client Core**: Vanilla ES6 JavaScript, HTML5, Custom Glassmorphic CSS Engine
*   **Data Libraries**: NumPy, Pandas, DB-Types
*   **Data Visualization**: Chart.js, DataTables.net
*   **Google Cloud Libraries**: `google-cloud-bigquery`, `google-cloud-bigquery-storage`
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
```

### 2. Run the Server
```bash
uvicorn src.main:app --reload
```
Open your browser to [http://127.0.0.1:8000](http://127.0.0.1:8000) to view the interface.

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

---

## ⚠️ Security & Scale Considerations

1. **Self-Hosted / Local Usage Focus**: The dashboard has no built-in auth system. **Do not expose this application directly to the public internet** without setting up an Identity-Aware Proxy (IAP) or an authentication gateway.
2. **SQL Parameters & Sanitization**: The tool utilizes dynamic parameters for scoped dataset queries. Use minimal privilege access patterns for target Service Accounts.
3. **Large-Scale Quotas**: Querying organization-wide metrics on high-volume environments (10,000+ datasets) can cause query timeout or quota limits. Scoped lookbacks are highly recommended for large environments.

---

## ⚠️ Disclaimer

This tool performs simulations based on historical BigQuery metadata. Simulated pricing estimates and recommended metrics may not fully capture enterprise-specific Google Cloud pricing structures, custom discounts, or blended flat-rate allocations. 

Always review proposed DDL and reservation alterations manually before applying updates to production environments.
