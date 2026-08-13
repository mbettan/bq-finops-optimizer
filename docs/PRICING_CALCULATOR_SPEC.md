# Runtime Economics Calculator Specification & Parity Verification

This specification documents the mathematical formulas, resource consumption models, and pricing derivations used by the BigQuery FinOps Optimizer Runtime Economics Calculator ([docs/static/calculator.js](static/calculator.js) and [docs/static/pricing.js](static/pricing.js)).

---

## 1. BigQuery Metadata Scan Derivations

BigQuery `INFORMATION_SCHEMA` metadata scans cost `$6.25 / TiB` under On-Demand pricing ($0 under BigQuery Editions reservations with dedicated slot capacity).

<!-- BEGIN:GENERATED:bq-metadata -->
| Profile | Projects | Scanned (GiB) | Formula | Cost / Run |
| :--- | :--- | :--- | :--- | :--- |
| **Small (3 Projects)** | 3 | `12 GiB` | `(12 / 1024) × $6.25` | **`$0.0732`** |
| **Medium (25 Projects)** | 25 | `100 GiB` | `(100 / 1024) × $6.25` | **`$0.6104`** |
| **Large (100 Projects)** | 100 | `400 GiB` | `(400 / 1024) × $6.25` | **`$2.4414`** |
| **X-Large (350+ Projects)** | 350 | `1400 GiB` | `(1400 / 1024) × $6.25` | **`$8.5449`** |
<!-- END:GENERATED:bq-metadata -->

---

## 2. Serverless Cloud Run Compute Derivations

Cloud Run compute costs are modeled based on container vCPU, RAM, invocation overhead, and execution duration per sweep.

<!-- BEGIN:GENERATED:cloudrun-compute -->
| Profile | Container Configuration | Active Sec / Run | vCPU Cost | RAM Cost | Invocations | Cost / Run |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Small** | 1 vCPU, 2 GiB RAM (2m) | 120s | $0.002880 | $0.000600 | $0.0000004 | **`$0.0035`** |
| **Medium** | 1 vCPU, 2 GiB RAM (5m) | 300s | $0.007200 | $0.001500 | $0.0000004 | **`$0.0087`** |
| **Large** | 2 vCPU, 4 GiB RAM (10m) | 600s | $0.028800 | $0.006000 | $0.0000004 | **`$0.0348`** |
| **X-Large** | 4 vCPU, 8 GiB RAM (15m) | 900s | $0.086400 | $0.018000 | $0.0000004 | **`$0.1044`** |
<!-- END:GENERATED:cloudrun-compute -->

---

## 3. Vertex AI Gemini Sweep Derivations

AI Doctor anti-pattern analysis costs per sweep across investigation tiers for Gemini 3.6 Flash and Gemini 3.5 Flash-Lite.

<!-- BEGIN:GENERATED:gemini-sweep -->
| Investigation Tier | Queries / Sweep | Context Budget | Gemini 3.6 Flash ($/sweep) | Gemini 3.5 Flash-Lite ($/sweep) | Display (Flash-Lite) |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Off (0)** | 0 | `0 tokens` | **`$0.0000`** (~$0.00) | **`$0.0000`** | **`$0.00 / sweep`** |
| **Top 5** | 5 | `~21.5k tokens` | **`$0.0563`** (~$0.06) | **`$0.0152`** | **`$0.02 / sweep`** |
| **Top 25** | 25 | `~108k tokens` | **`$0.2813`** (~$0.28) | **`$0.0762`** | **`$0.08 / sweep`** |
| **Top 50** | 50 | `~215k tokens` | **`$0.5625`** (~$0.56) | **`$0.1525`** | **`$0.15 / sweep`** |
| **Org (200)** | 200 | `~860k tokens` | **`$2.2500`** (~$2.25) | **`$0.6100`** | **`$0.61 / sweep`** |
<!-- END:GENERATED:gemini-sweep -->

---

## 4. Golden Matrix: Workload Presets & Monthly Totals

Exact baseline projections across standard enterprise organization presets and deployment topologies.

<!-- BEGIN:GENERATED:golden-matrix -->
| Preset / Workload | Projects | Schedule | BigQuery (List) | Cloud Run (Service) | Agent Platform (Model) | Total Monthly Spend |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Small Baseline** | 3 | 30x/mo | $2.20 ($0.0732/run) | $0.00 (Local) | Off ($0.00) | **`$2.20`** |
| **Medium Default** | 25 | 30x/mo | $18.31 ($0.6104/run) | $0.00 (Local) | Off ($0.00) | **`$18.31`** |
| **Medium Cloud Run** | 25 | 30x/mo | $18.31 ($0.6104/run) | $0.26 ($0.0087/run) | Off ($0.00) | **`$18.57`** |
| **Medium + Flash-Lite** | 25 | 30x/mo | $18.31 ($0.6104/run) | $0.00 (Local) | $2.29 ($0.0762/sweep) | **`$20.60`** |
| **Large + Flash** | 100 | 30x/mo | $73.24 ($2.4414/run) | $1.04 ($0.0348/run) | $16.88 ($0.5625/sweep) | **`$91.16`** |
| **X-Large Continuous** | 350 | 720x/mo | $6,152.34 ($8.5449/run) | $75.17 ($0.1044/run) | $1,620.00 ($2.2500/sweep) | **`$7,847.51`** |
<!-- END:GENERATED:golden-matrix -->
