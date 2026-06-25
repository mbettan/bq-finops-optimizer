# 🚀 Release Notes: BigQuery FinOps Optimizer v1.1.0

We are proud to announce the release of **v1.1.0** of the **BigQuery FinOps Optimizer**—an enterprise-grade diagnostic, simulation, and governance suite designed to maximize cost efficiency and eliminate compute waste across Google Cloud BigQuery environments. 

This release marks a major milestone, introducing dynamic budget safety guardrails, advanced telemetry models, proactive migration guardrails, and a complete codebase sanitization to support secure, open-source distribution.

---

## 🔑 Key Highlights in v1.1.0

### 1. 🛡️ Dynamic Billing Limits & Cost Safety Caps (Module 13)
*   **Dynamic Cost Guardrails:** Introduced a project-wide `max_bytes_billed_gb` safety cap parameter, dynamically passed to every single BigQuery query execution in the system.
*   **Budget Protection:** The parameter translates directly into BigQuery's native `QueryJobConfig.maximum_bytes_billed` (converting GiB to bytes), preventing any analytical or diagnostic runs from causing runaway query charges.
*   **Smart Clamping & Fallbacks:** Implemented robust boundary validation (minimum of 1 GiB, maximum of 10 TiB, and a safe default fallback of 200 GiB if the configuration is unset or zero).
*   **State Persistence:** Settings are persisted directly in the client's local storage and dynamically validated using Pydantic models.

### 2. 📈 The "Slot Capacity Bucket Method" (Compute Capacity Simulation)
*   **Vectorized Sizing Engine:** Built a high-performance simulation engine using NumPy to model slot consumption telemetry over a standard 730-hour billing month.
*   **Tiered Recommendation Matrix:** Evaluates per-minute reservation timelines to generate three distinct baseline tier recommendations based on historical percentiles:
    *   🔴 **Aggressive Savings (p80):** Minimizes baseline commitments, leveraging autoscaling for bursts.
    *   🟡 **Balanced (p95):** Optimizes for steady-state workloads with moderate risk tolerance.
    *   🟢 **Performance (Max):** Eliminates autoscaler latency by matching peak slot usage.
*   **FinOps Realism:** Integrates custom Enterprise Discount Agreements (EDAs), Committed-Use Discounts (CUDs), and autoscaling rounding overhead factors (to account for BigQuery's physical 50-slot stepping).

### 3. ⏱️ Fluid Scaling & Cooldown Tax Mitigation
*   **Cooldown Tax Analysis:** Identifies reservations with high-frequency, short-duration queries that suffer from the legacy autoscaler's 60-second minimum charge.
*   **Per-Second Simulation:** Models the financial impact of transitioning to **Fluid Scaling** (true per-second billing with zero minimum capacity), allowing users to isolate workloads without financial penalty.

### 4. 🔗 Hybrid Cost Attribution Engine (Module 3)
*   **SKU Blending Resolution:** Solves the challenge of attributing blended BigQuery Editions costs down to individual querying projects.
*   **Allocated Waste Distribution:** Proportionally redistributes unallocated idle capacity waste (unused baseline reservation slots) back to the active project consumers, eliminating the \"admin project dump\" mystery.
*   **Custom Attribution Rules:** Supports customizable billing rules, including *Lender Pays* vs. *Borrower Pays* models for cross-reservation idle slot borrowing.

### 5. 🤖 AI-Powered Semantic Query Review (AI Doctor)
*   **Native LLM Integration:** Leverages BigQuery's native `AI.GENERATE` scalar function to run high-speed, cost-efficient SQL reviews directly inside the data warehouse.
*   **Structured Auditing:** Uses a custom prompt engine with **Gemini 3.1 Flash Lite** to scan your most expensive queries for 7 critical anti-patterns (e.g., `SELECT *` abuse, unclustered limits, and function-wrapped partition keys).
*   **Actionable Routing Snippets:** Generates copy-pasteable `SET @@reservation` DDL statements to seamlessly route optimized query patterns to designated compute pools.

### 6. 🏗️ Proactive Migration Guardrails (Static Schema Auditor)
*   **Migration Timebomb Detection:** Introduced a static schema scanner that audits the metadata catalog to identify high-risk, unclustered, or unpartitioned tables *before* they are queried:
    *   🔴 **Critical Risk:** Tables exceeding 1 TB in size or 1 Billion rows.
    *   🟡 **High Risk:** Tables exceeding 10 GB in size or 50 Million rows.
*   **Automated Remediation:** Suggests optimal clustering columns (detecting `_id`, `_type`, and `_date` fields) and generates ready-to-run replacement DDL stubs.

### 7. 🔒 Sanitization & Repository Hygiene
*   **Corporate Data Scrubbing:** Conducted a rigorous sweep of all source files, documentation, and mock data to remove all traces of proprietary company identifiers (*\"Wiley\"*, *\"Wylie\"*).
*   **Zero Credentials Leak:** Verified that no GCP service account keys, passwords, private tokens, or local machine absolute paths are hardcoded.
*   **Sanitized Gitignore:** Restructured `.gitignore` to explicitly cover virtual environments (`myenv/`, `venv/`), test cache directories (`.pytest_cache/`), and internal diagnostic snapshots (`heavy_job_ids_ref.md`).

---

## 🛠️ Tech Stack & Architecture

*   **API Framework:** FastAPI (centralized versioning, GZip compression middleware, automated interactive `/docs` landing page).
*   **Data Science Core:** NumPy, Pandas, DB-Types.
*   **Frontend UI:** Vanilla ES6 JavaScript, HTML5, Custom Glassmorphic CSS Engine.
*   **Testing Suite:** 272 comprehensive unit and integration tests (mocked BigQuery endpoints, Pydantic input boundary validation, and SQL injection security checks).

---

*For installation, local setup, IAM permissions, and GCP configuration, please refer to the main [README.md](README.md).*
