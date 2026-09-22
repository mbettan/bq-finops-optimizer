# FinOps Optimizer for BigQuery — Engineering Backlog (100% Roadmap Coverage)

This backlog tracks all 22 architectural enhancements, algorithmic optimizations, and user experience features in the private engineering repository (`mbettan/bq-finops-optimizer-private`).

* 📋 **Private Kanban Project Board:** [FinOps Optimizer — Private Engineering Backlog (Project #2)](https://github.com/users/mbettan/projects/2)
* 🔗 **Repository Projects Tab:** [bq-finops-optimizer-private/projects](https://github.com/mbettan/bq-finops-optimizer-private/projects)
* 🐛 **Issue Tracker:** [bq-finops-optimizer-private/issues](https://github.com/mbettan/bq-finops-optimizer-private/issues)

---

## 🎯 Implementation Workstreams Overview

| Workstream | Focus Domain | Key Objectives | Tracked Issues |
| :--- | :--- | :--- | :--- |
| **WS 1** | **Operational Health, Quota & GenAI Governance** | Proactive 429/quota throttling alerts (`>1%`), spend velocity (`+15%` drift), spend stability ($\text{CV}$), production GenAI & BQML workload attribution. | [#16](https://github.com/mbettan/bq-finops-optimizer-private/issues/16), [#17](https://github.com/mbettan/bq-finops-optimizer-private/issues/17), [#18](https://github.com/mbettan/bq-finops-optimizer-private/issues/18) |
| **WS 2** | **SQL Refactoring, Schema Denormalization & MVs** | Multi-rule SQL linter, AI Doctor join sequencing & CTE pushdown, `ARRAY<STRUCT>` schema denormalization, Materialized View Smart Tuning recommender. | [#12](https://github.com/mbettan/bq-finops-optimizer-private/issues/12), [#13](https://github.com/mbettan/bq-finops-optimizer-private/issues/13), [#14](https://github.com/mbettan/bq-finops-optimizer-private/issues/14), [#15](https://github.com/mbettan/bq-finops-optimizer-private/issues/15) |
| **WS 3** | **Storage Lifecycle, 90-Day Discount & Clones** | 90-day long-term storage reset detector, zero-copy `CLONE`/`SNAPSHOT` arbitrage, partition cardinality & unclustered table guardrails. | [#5](https://github.com/mbettan/bq-finops-optimizer-private/issues/5), [#7](https://github.com/mbettan/bq-finops-optimizer-private/issues/7), [#19](https://github.com/mbettan/bq-finops-optimizer-private/issues/19) |
| **WS 4** | **Commercial CUD Modeling & Commitments** | Spend-based CUDs + Composer 3 synergy, peak traffic multiplier (`0.5x–5.0x`), Edition tier & DR advisor, custom EDA/CUD discounts & 50-slot stepping. | [#6](https://github.com/mbettan/bq-finops-optimizer-private/issues/6), [#11](https://github.com/mbettan/bq-finops-optimizer-private/issues/11), [#20](https://github.com/mbettan/bq-finops-optimizer-private/issues/20) |
| **WS 5** | **Reservation Architecture Blueprint & Execution** | Automated 4-tier reservation blueprint + visual SVG diagram + Capacity DDL, OOM/spill failed compute profiler, concurrent duplicate query coalescer, scan vs compute stage profiler. | [#8](https://github.com/mbettan/bq-finops-optimizer-private/issues/8), [#9](https://github.com/mbettan/bq-finops-optimizer-private/issues/9), [#10](https://github.com/mbettan/bq-finops-optimizer-private/issues/10), [#21](https://github.com/mbettan/bq-finops-optimizer-private/issues/21) |
| **WS 6** | **ROI Snapshot Diff Engine & Interactive UI** | Baseline vs. post-optimization snapshot ROI comparator, interactive PXX play sliders, pre-run API discovery, dynamic CRM project dropdowns, custom date pickers. | [#1](https://github.com/mbettan/bq-finops-optimizer-private/issues/1), [#2](https://github.com/mbettan/bq-finops-optimizer-private/issues/2), [#3](https://github.com/mbettan/bq-finops-optimizer-private/issues/3), [#4](https://github.com/mbettan/bq-finops-optimizer-private/issues/4), [#22](https://github.com/mbettan/bq-finops-optimizer-private/issues/22) |

---

## 📋 Complete Issue Catalog (#1 – #22)

| Issue | Workstream | Title | Endpoint / Module |
| :--- | :--- | :--- | :--- |
| **[#1](https://github.com/mbettan/bq-finops-optimizer-private/issues/1)** | WS 6 | Interactive Sliders (Play Axes) for PXX and Autoscale Queueing Simulation | `static/app.js` |
| **[#2](https://github.com/mbettan/bq-finops-optimizer-private/issues/2)** | WS 6 | Pre-Run API Enablement Discovery & Guided Action Buttons | `GET /api/settings/preflight` |
| **[#3](https://github.com/mbettan/bq-finops-optimizer-private/issues/3)** | WS 6 | Dynamic Project Dropdowns via Cloud Resource Manager Discovery | `GET /api/projects/discover` |
| **[#4](https://github.com/mbettan/bq-finops-optimizer-private/issues/4)** | WS 6 | Custom Date/Time Range for Historical Analysis (Start Date & End Date) | `src/main.py`, `static/app.js` |
| **[#5](https://github.com/mbettan/bq-finops-optimizer-private/issues/5)** | WS 3 | Partition Guardrail Cardinality, Table Size & Unpruned Query Waste | `POST /api/governance/analyze` |
| **[#6](https://github.com/mbettan/bq-finops-optimizer-private/issues/6)** | WS 4 | Actionable Query-Level `@@reservation` Routing Snippet Generator | `src/main.py`, `static/app.js` |
| **[#7](https://github.com/mbettan/bq-finops-optimizer-private/issues/7)** | WS 3 | Static Migration Guardrails: Proactive Schema Auditor for Unclustered Big Tables | `POST /api/governance/analyze` |
| **[#8](https://github.com/mbettan/bq-finops-optimizer-private/issues/8)** | WS 5 | Failed Compute Profiler: Query-Level OOM & `resourcesExceeded` Diagnostics | `POST /api/workloads/failed_compute` |
| **[#9](https://github.com/mbettan/bq-finops-optimizer-private/issues/9)** | WS 5 | Concurrent Duplicate Query Analyzer (Request Coalescing & Sweep-Line Overlap) | `POST /api/workloads/duplicate_queries` |
| **[#10](https://github.com/mbettan/bq-finops-optimizer-private/issues/10)** | WS 5 | Scan-Dominated Query Profiler (Input-Stage vs. Compute-Stage Decomposition) | `POST /api/workloads/scan_dominated` |
| **[#11](https://github.com/mbettan/bq-finops-optimizer-private/issues/11)** | WS 4 | Enterprise Contract Realism: Custom EDA/CUD Discounts & 50-Slot Stepping Overhead | `src/pricing.py`, `src/main.py` |
| **[#12](https://github.com/mbettan/bq-finops-optimizer-private/issues/12)** | WS 2 | Proactive Materialized View Recommendation Engine for Recurring Aggregations | `POST /api/mv/candidates` |
| **[#13](https://github.com/mbettan/bq-finops-optimizer-private/issues/13)** | WS 2 | Multi-Rule Static SQL Anti-Pattern Linter (Self-Joins, Unbounded ORDER BY, REGEXP) | `POST /api/antipatterns/linter` |
| **[#14](https://github.com/mbettan/bq-finops-optimizer-private/issues/14)** | WS 2 | AI Doctor: Join Sequencing, Predicate Selectivity & Window Function Rewrites | `POST /api/ai/analyze_query` |
| **[#15](https://github.com/mbettan/bq-finops-optimizer-private/issues/15)** | WS 2 | Schema Denormalization (`STRUCT` / `ARRAY`) Candidate Detector | `POST /api/schema/denormalization_candidates` |
| **[#16](https://github.com/mbettan/bq-finops-optimizer-private/issues/16)** | WS 1 | Quota Throttling (`429`) & Concurrency Saturation Health Alerter | `POST /api/health/triggers` |
| **[#17](https://github.com/mbettan/bq-finops-optimizer-private/issues/17)** | WS 1 | Spend Velocity (`+15%` Budget Drift), Spend Stability ($\text{CV}$) & Cost-per-Query Safeguards | `POST /api/health/spend_velocity` |
| **[#18](https://github.com/mbettan/bq-finops-optimizer-private/issues/18)** | WS 1 | Production GenAI & BQML Workload Profiler (`AI.GENERATE`, `ML.GENERATE_TEXT`, `VECTOR_SEARCH`) | `POST /api/workloads/genai` |
| **[#19](https://github.com/mbettan/bq-finops-optimizer-private/issues/19)** | WS 3 | 90-Day Long-Term Storage Reset Detector & Table Clone/Snapshot Optimizer | `POST /api/storage/clones_and_duplicates` |
| **[#20](https://github.com/mbettan/bq-finops-optimizer-private/issues/20)** | WS 4 | Commercial CUD Modeling (Spend-Based CUDs + Composer 3), Traffic Multiplier & Edition Advisor | `POST /api/slots/simulate`, `POST /api/slots/edition_advisor` |
| **[#21](https://github.com/mbettan/bq-finops-optimizer-private/issues/21)** | WS 5 | Automated Multi-Reservation Architecture Blueprint, Visual SVG Hierarchy & Capacity DDL | `POST /api/reservations/blueprint`, `GET /report/workload-plan/{id}` |
| **[#22](https://github.com/mbettan/bq-finops-optimizer-private/issues/22)** | WS 6 | Baseline vs. Post-Optimization Snapshot Comparison (Realized ROI Diff Engine) | `POST /api/report/compare_snapshots` |

---

## 🔒 Security & Data Boundary Guarantees

1. **Zero Data-Plane Access**: Only metadata views (`INFORMATION_SCHEMA`, `TABLE_STORAGE_BY_*`, `COLUMNS`) are accessed. The tool never executes `SELECT *` or reads underlying user table data records (`bigquery.tables.getData` is never required).
2. **Read-Only Inspection**: Automated DDL and reservation routing scripts are generated as recommendations for user review; no mutations are executed without explicit operator action.
3. **Zero PII & Sanitization**: Strictly zero client names, internal terminology, or personal credentials exist in any issue or codebase file.
