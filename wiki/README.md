# FinOps Optimizer for BigQuery — Engineering Wiki

Welcome to the internal engineering wiki and development staging workspace for **FinOps Optimizer for BigQuery**.

---

## 📚 Documentation Index

* 📋 **[Backlog & Issue Tracker](BACKLOG.md)**: Full catalog of active backlog items, feature requests, and priority tiers.
* 🚀 **[Workstream 2 PRD (SQL, Schema & MVs)](specs/prd_workstream_2_sql_schema_mv.md)**: Comprehensive Product Requirements Document for:
  * Multi-Rule Static SQL Anti-Pattern Linter (Issue #13)
  * AI Doctor Join Sequencing & CTE Pushdown (Issue #14)
  * Schema Denormalization (`STRUCT` / `ARRAY`) Candidate Detector (Issue #15)
  * Proactive Materialized View Recommendation Engine (Issue #12)
* 🗺️ **[Full Implementation Plan (Workstreams 1–6)](specs/implementation_plan.md)**: Roadmap for 100% gap coverage across Quota/429 alerts, spend velocity, GenAI attribution, advanced storage lifecycle, commitment planning, execution diagnostics, and UI simulation.
* 📐 **[Historical Specs Archive](specs/README.md)**: Specifications for completed milestones (GAP-28 series).

---

## 🔄 Repository Configuration & Remotes

| Remote | URL | Purpose |
| :--- | :--- | :--- |
| `private` | `git@github.com:mbettan/bq-finops-optimizer-private.git` | Private development, backlog tracking, automated testing, agent scratch & staging. |
| `origin` | `git@github.com:mbettan/bq-finops-optimizer.git` | Public open-source repository & production releases. |

---

## 🛡️ Staging & Promotion Lifecycle

1. **Feature Branching:** Develop features on dedicated branches in the private repository (e.g. `feature/ws2-sql-schema-mv`).
2. **Offline Verification:** Validate test suites (`pytest tests/ -v`) without cloud connectivity.
3. **Security & Zero Data-Plane Audit:** Enforce zero data-plane requirements (`bigquery.tables.getData` never permitted) using `deploy/check_permissions.py`.
4. **Sanitization Check:** Ensure zero client identifiers, PII, local user paths, or internal credentials exist in code or commit history.
5. **Upstream Promotion:** Cherry-pick or push tested, verified release commits to `origin/main` and sync project tracking.
