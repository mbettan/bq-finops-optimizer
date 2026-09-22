# FinOps Optimizer for BigQuery — Engineering Backlog

This backlog tracks all architectural enhancements, algorithmic improvements, and user experience features planned for the FinOps Optimizer for BigQuery.

---

## 🎯 Implementation Workstreams Overview

| Workstream | Focus Domain | Key Objectives | Relevant Issues |
| :--- | :--- | :--- | :--- |
| **WS 1** | **Operational Alerts & Spend Velocity** | Proactive quota/429 alerts, anomaly detection, GenAI workload attribution. | Planned (WS1) |
| **WS 2** | **SQL Refactoring, Schema & MVs** | Multi-rule SQL linter, join sequencing, schema denormalization (`STRUCT`/`ARRAY`), materialized view discovery. | [#12](https://github.com/mbettan/bq-finops-optimizer-private/issues/12), [#13](https://github.com/mbettan/bq-finops-optimizer-private/issues/13), [#14](https://github.com/mbettan/bq-finops-optimizer-private/issues/14), [#15](https://github.com/mbettan/bq-finops-optimizer-private/issues/15) |
| **WS 3** | **Advanced Storage Lifecycle** | 90-day long-term pricing preservation, table clones vs. snapshots, physical vs. logical storage arbitrage. | [#5](https://github.com/mbettan/bq-finops-optimizer-private/issues/5), [#7](https://github.com/mbettan/bq-finops-optimizer-private/issues/7) |
| **WS 4** | **Commitment & Architecture** | Spend-based CUD modeling, multi-reservation topology recommendations, EDA discount modeling. | [#6](https://github.com/mbettan/bq-finops-optimizer-private/issues/6), [#11](https://github.com/mbettan/bq-finops-optimizer-private/issues/11) |
| **WS 5** | **Execution Efficiency & Concurrency** | Failed compute profiler (OOM/spill), concurrent duplicate query coalescence, scan-dominated query decomposition. | [#8](https://github.com/mbettan/bq-finops-optimizer-private/issues/8), [#9](https://github.com/mbettan/bq-finops-optimizer-private/issues/9), [#10](https://github.com/mbettan/bq-finops-optimizer-private/issues/10) |
| **WS 6** | **UI/UX & Interactive Planning** | What-if simulation sliders, dynamic project discovery, custom date pickers, pre-run API discovery. | [#1](https://github.com/mbettan/bq-finops-optimizer-private/issues/1), [#2](https://github.com/mbettan/bq-finops-optimizer-private/issues/2), [#3](https://github.com/mbettan/bq-finops-optimizer-private/issues/3), [#4](https://github.com/mbettan/bq-finops-optimizer-private/issues/4) |

---

## 📋 Active Issue Catalog

### 1. Interactive Sliders (Play Axes) for PXX and Autoscale Queueing Simulation
- **Issue**: [#1](https://github.com/mbettan/bq-finops-optimizer-private/issues/1)
- **Labels**: `feature-request`, `slots-optimizer`, `community-reported`
- **Summary**: Replace static percentile inputs with real-time dynamic sliders (P50 to P99) and autoscale target utilization controls. Allows BigQuery users to simulate slot headroom, queueing delay risks, and cost trade-offs interactively before applying reservation updates.

### 2. Pre-Run API Enablement Discovery & Guided Action Buttons
- **Issue**: [#2](https://github.com/mbettan/bq-finops-optimizer-private/issues/2)
- **Labels**: `feature-request`, `settings-core`, `ui-ux`, `community-reported`
- **Summary**: Perform pre-flight discovery checks against required Google Cloud APIs (`bigquery.googleapis.com`, `bigqueryreservation.googleapis.com`, `cloudresourcemanager.googleapis.com`). Surface inline guided action buttons and IAM remediation snippets when permissions or APIs are missing.

### 3. Dynamic Project Dropdowns via Cloud Resource Manager Discovery
- **Issue**: [#3](https://github.com/mbettan/bq-finops-optimizer-private/issues/3)
- **Labels**: `feature-request`, `settings-core`, `ui-ux`, `community-reported`
- **Summary**: Provide searchable project and folder dropdowns populated dynamically via Cloud Resource Manager API, eliminating manual project ID typing and scope errors across multi-project environments.

### 4. Custom Date/Time Range for Historical Analysis (Start Date & End Date)
- **Issue**: [#4](https://github.com/mbettan/bq-finops-optimizer-private/issues/4)
- **Labels**: `feature-request`, `settings-core`, `ui-ux`, `community-reported`
- **Summary**: Expand analysis windows beyond fixed presets (1d, 7d, 14d, 30d) by adding an interactive date-time range picker to analyze historical incidents, billing billing periods, and seasonal peaks.

### 5. Partition Guardrail Cardinality, Table Size & Unpruned Query Waste
- **Issue**: [#5](https://github.com/mbettan/bq-finops-optimizer-private/issues/5)
- **Labels**: `feature-request`, `storage-hygiene`, `governance`, `community-reported`
- **Summary**: Detect tables exceeding partition cardinality thresholds (>4,000 partitions) causing query planner latency, and quantify byte/slot waste from queries running without partition filter predicates (`require_partition_filter = FALSE`).

### 6. Actionable Query-Level @@reservation Routing Snippet Generator
- **Issue**: [#6](https://github.com/mbettan/bq-finops-optimizer-private/issues/6)
- **Labels**: `feature-request`, `slots-optimizer`, `ui-ux`
- **Summary**: Automatically generate ready-to-run SQL routing snippets (`SET @@dataset_project_id`, `SET @@reservation_id`) and Airflow / dbt connection configurations to route heavy ad-hoc or batch queries to dedicated low-cost or batch reservations.

### 7. Static Migration Guardrails: Proactive Schema Auditor for Unclustered Big Tables
- **Issue**: [#7](https://github.com/mbettan/bq-finops-optimizer-private/issues/7)
- **Labels**: `feature-request`, `storage-hygiene`, `governance`
- **Summary**: Audit warehouse metadata to flag unpartitioned and unclustered tables exceeding 10 GB. Generate automated DDL migration scripts (`CREATE OR REPLACE TABLE ... PARTITION BY ... CLUSTER BY ... AS SELECT * FROM ...`).

### 8. Failed Compute Profiler: Query-Level OOM & resourcesExceeded Diagnostics
- **Issue**: [#8](https://github.com/mbettan/bq-finops-optimizer-private/issues/8)
- **Labels**: `feature-request`, `ai-doctor`, `cost-attribution`
- **Summary**: Inspect failed queries in `INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION` matching `resourcesExceeded` and memory limits. Pinpoint memory-spilling stages, Cartesian products, and generate remediation suggestions.

### 9. Concurrent Duplicate Query Analyzer (Request Coalescing & Sweep-Line Overlap)
- **Issue**: [#9](https://github.com/mbettan/bq-finops-optimizer-private/issues/9)
- **Labels**: `feature-request`, `ai-doctor`, `batch-vs-interactive`
- **Summary**: Identify identical or semantically duplicate SQL statements executed concurrently within short timeframes (e.g., overlapping dashboard refreshes). Recommend query result caching and proxy request coalescing.

### 10. Scan-Dominated Query Profiler (Input-Stage vs. Compute-Stage Decomposition)
- **Issue**: [#10](https://github.com/mbettan/bq-finops-optimizer-private/issues/10)
- **Labels**: `feature-request`, `ai-doctor`, `slots-optimizer`
- **Summary**: Decompose query execution stages between read I/O and compute processing. Identify queries where >80% of execution time is spent scanning data vs. computing, highlighting clustering candidates and projection trimming opportunities.

### 11. Enterprise Contract Realism: Custom EDA/CUD Discounts & 50-Slot Stepping Overhead
- **Issue**: [#11](https://github.com/mbettan/bq-finops-optimizer-private/issues/11)
- **Labels**: `feature-request`, `cost-attribution`, `settings-core`
- **Summary**: Allow BigQuery users to configure negotiated discount percentages (EDA / CUD) across On-Demand, Standard, Enterprise, and Enterprise Plus editions. Account for BigQuery's 50-slot autoscaling stepping granularity in simulation models.

### 12. Proactive Materialized View Recommendation Engine for Recurring Aggregations
- **Issue**: [#12](https://github.com/mbettan/bq-finops-optimizer-private/issues/12)
- **Labels**: `feature-request`, `ai-doctor`, `storage-hygiene`
- **Summary**: Automatically identify recurring high-frequency aggregation subqueries on large partitioned tables. Propose candidate Materialized Views (MVs), estimate slot and byte savings, and generate standard `CREATE MATERIALIZED VIEW` DDL.

### 13. Multi-Rule Static SQL Anti-Pattern Linter
- **Issue**: [#13](https://github.com/mbettan/bq-finops-optimizer-private/issues/13)
- **Labels**: `feature-request`, `ai-doctor`, `storage-hygiene`
- **Summary**: Static AST / regex linter flagging common cost drivers: self-joins (`JOIN` on same table without partition pruning), unbounded `ORDER BY` without `LIMIT`, expensive non-anchored regex (`REGEXP_CONTAINS`), and unindexed `LIKE '%...'` predicates.

### 14. AI Doctor: Join Sequencing, Predicate Selectivity & Window Function Rewrites
- **Issue**: [#14](https://github.com/mbettan/bq-finops-optimizer-private/issues/14)
- **Labels**: `feature-request`, `ai-doctor`
- **Summary**: Semantic AI optimization engine that restructures multi-table joins to place highest-selectivity filters first, pushes predicates down into CTEs before shuffling, and optimizes window functions (`QUALIFY ROW_NUMBER() = 1`).

### 15. Schema Denormalization (STRUCT / ARRAY) Candidate Detector
- **Issue**: [#15](https://github.com/mbettan/bq-finops-optimizer-private/issues/15)
- **Labels**: `feature-request`, `storage-hygiene`, `governance`
- **Summary**: Identify relational parent-child table pairs frequently joined at high volume. Propose nested and repeated fields (`ARRAY<STRUCT<...>>`), eliminate shuffle joins, and calculate edition vs. on-demand cost trade-offs with DML churn safeguards.

---

## 🔒 Security & Data Boundary Guarantees

All features in this backlog strictly adhere to the following principles:
1. **Zero Data-Plane Access**: Only metadata views (`INFORMATION_SCHEMA`, `TABLE_STORAGE_BY_*`, `COLUMNS`) are accessed. The tool never executes `SELECT *` or reads underlying table data records (`bigquery.tables.getData` is never required).
2. **Read-Only Inspection**: Automated DDL and reservation routing scripts are generated as recommendations for user review; no mutations are executed without explicit operator intervention.
3. **Enterprise Compliance**: No PII, customer credentials, or private query values are transmitted to external endpoints or third-party services.
