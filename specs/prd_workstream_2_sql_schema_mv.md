# Product Requirements Document (PRD): SQL Refactoring, Schema Denormalization & Materialized View Discovery

**Product:** FinOps Optimizer for BigQuery  
**Scope:** Workstream 2 — Query Anti-Pattern Linting, AI Rewrite Rules, Nested/Repeated Schema Modernization, and Proactive Materialized View Discovery  
**Data Boundary:** 100% Control-Plane Metadata (`INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION`, `TABLES`, `COLUMNS`, `TABLE_STORAGE_BY_ORGANIZATION`). Zero `bigquery.tables.getData` permissions required.

---

## Overview of Modules Covered

| Feature ID | Module Name | Target Endpoint | Primary Objective |
| :--- | :--- | :--- | :--- |
| **Feature 2.1** | **Multi-Rule Static SQL Anti-Pattern Linter** | `POST /api/antipatterns/linter` | Deterministically detect expensive SQL anti-patterns (`SELECT *`, Self-Joins, Unbounded `ORDER BY`, `REGEXP_CONTAINS` over `LIKE`, Unaggregated `CROSS JOIN`) at zero LLM token cost. |
| **Feature 2.2** | **AI Doctor: Join Sequencing, Predicate Selectivity & Window Function Rewrites** | `POST /api/ai/analyze` | Upgrade semantic SQL optimization rules to convert self-joins to analytic window functions, sequence largest tables first in `JOIN`s, order `WHERE` predicates by selectivity, and propose `STRUCT`/`ARRAY` nesting. |
| **Feature 2.3** | **Schema Denormalization (`STRUCT` / `ARRAY`) Candidate Detector** | `POST /api/schema/denormalization_candidates` | Mine cross-query `referenced_tables` join graphs to detect over-normalized relational tables repeatedly joined at scale, quantify shuffle/compute waste, and generate nested `ARRAY<STRUCT<...>>` DDL. |
| **Feature 2.4** | **Proactive Materialized View (MV) Candidate & Smart-Tuning Recommender** | `POST /api/mv/candidates` | Identify recurring base-table `GROUP BY` aggregation patterns across the organization, model savings from BigQuery Smart Tuning automatic query rerouting, and emit `CREATE MATERIALIZED VIEW` DDL. |

---

## Feature 2.1: Multi-Rule Static SQL Anti-Pattern Linter

### 1. Problem Statement
Currently, `analyze_query_linter` (`src/main.py`, `POST /api/antipatterns/linter`) only inspects historical queries for a single pattern: `SELECT * FROM` (`[SELECT * ABUSE]`) on queries billing over 100 GiB. Consequently, major structural anti-patterns that drive high slot contention, shuffle memory spills, and full-table scans—specifically **Self-Joins**, **Unbounded `ORDER BY`**, **`REGEXP_CONTAINS` string matching**, and **Unaggregated `CROSS JOIN`s**—go undetected unless those specific queries happen to be sampled in a paid AI Doctor run.

### 2. User Stories
* *As a Data Engineering Lead*, I want a fast, deterministic static linter that scans historical queries across all projects for `SELECT *`, self-joins, unbounded `ORDER BY`, `REGEXP_CONTAINS`, and `CROSS JOIN` anti-patterns without incurring LLM token costs.
* *As a FinOps Analyst*, I want the "SQL Wall of Shame" summary view to group violations by `(user_email, project_id, abuse_type)` so I can prioritize engineering remediation by the specific anti-pattern driving the most waste.

### 3. Functional Requirements & Detection Rules
The static linter query against `INFORMATION_SCHEMA.JOBS_BY_PROJECT` / `JOBS_BY_ORGANIZATION` must strip SQL comments (`--...` and `/*...*/`) and string literals before evaluating five deterministic regex/metadata rules on completed `QUERY` jobs exceeding a configurable `min_billed_gb` threshold (default `10 GiB`, previously hardcoded at `100 GiB`):

| Rule Code | Badge Label | Detection Logic (SQL Pushdown) | Recommended Fix |
| :--- | :--- | :--- | :--- |
| `SELECT_STAR` | `[SELECT * ABUSE]` | `REGEXP_CONTAINS(clean_sql, r'(?i)\bSELECT\s+\*\s+FROM\b')` | Project only required columns to leverage columnar pruning. |
| `SELF_JOIN` | `[SELF-JOIN → WINDOW]` | `ARRAY_LENGTH(referenced_tables) > ARRAY_LENGTH(ARRAY(SELECT DISTINCT AS STRUCT * FROM UNNEST(referenced_tables)))` AND `REGEXP_CONTAINS(clean_sql, r'(?i)\bJOIN\b')` | Replace self-joins with analytic window functions (`LAG`, `LEAD`, `ROW_NUMBER() OVER(PARTITION BY ...)`). |
| `ORDER_BY_NO_LIMIT` | `[ORDER BY WITHOUT LIMIT]` | `REGEXP_CONTAINS(clean_sql, r'(?i)\bORDER\s+BY\b')` AND NOT `REGEXP_CONTAINS(clean_sql, r'(?i)\bLIMIT\s+\d+')` (outside `OVER(...)` clauses) | Add a `LIMIT` clause when sorting large datasets to prevent single-node sort bottlenecks and `resourcesExceeded` errors. |
| `REGEXP_OVER_LIKE` | `[REGEXP OVER LIKE]` | `REGEXP_CONTAINS(clean_sql, r'(?i)\bREGEXP_CONTAINS\s*\([^,]+,\s*r?[\'"][a-zA-Z0-9_% -]+[\'"]\s*\)')` | Replace simple substring/prefix `REGEXP_CONTAINS` checks with `LIKE` or `STARTS_WITH()` to reduce CPU slot consumption. |
| `CROSS_JOIN` | `[UNAGGREGATED CROSS JOIN]` | `REGEXP_CONTAINS(clean_sql, r'(?i)\bCROSS\s+JOIN\b')` (excluding `UNNEST(...)`) | Pre-aggregate inputs before executing a `CROSS JOIN` or replace with an equi-join condition. |

### 4. API & Data Model Specification (`src/main.py`)
```python
class AntiPatternParams(FocusMixin):
    org_project_id: Optional[str] = None
    lookback_days: int = Field(default=7, ge=1, le=90)
    limit_per_project: int = Field(default=20, ge=1, le=200)
    min_billed_gb: float = Field(default=10.0, ge=1.0, le=10000.0)
    abuse_types: Optional[List[str]] = None  # Optional filter: ['SELECT_STAR', 'SELF_JOIN', ...]
    max_bytes_billed_gb: Optional[int] = None

class LinterResult(BaseModel):
    project_id: str
    job_id: str
    user_email: str
    query_snippet: str
    abuse_type: str               # Badge label e.g. "[SELF-JOIN → WINDOW]"
    rule_code: str = "SELECT_STAR"
    remediation_hint: str = ""
    billed_gb: float
    slot_hours: float = 0.0
    estimated_waste_usd: float = 0.0
```

### 5. Acceptance Criteria
1. Queries matching multiple anti-patterns emit the highest-severity `abuse_type` as primary and list all triggered `rule_code` badges in the UI detail row.
2. `SELF_JOIN` detection uses BigQuery's native `referenced_tables` array (when a single base table appears multiple times alongside a `JOIN` keyword) or alias self-join regex fallback, eliminating false positives from `UNION ALL` queries without joins.
3. `ORDER_BY_NO_LIMIT` strips `OVER (... ORDER BY ...)` window specifications before checking for top-level `ORDER BY` without `LIMIT`.
4. The frontend filter toolbar (`static/app.js`) dynamically populates the **Anti-Pattern Type** dropdown with all 5 rules and aggregates waste per owner in the **Summary** tab.

---

## Feature 2.2: AI Doctor Advanced SQL & Schema Rewrite Engine

### 1. Problem Statement
While `analyze_ai_queries` (`src/main.py`, `POST /api/ai/analyze`) and `src/migration_optimizer.py` currently audit queries for `SELECT *`, `CROSS JOIN`, `ORDER BY` without `LIMIT`, and `REGEXP_CONTAINS`, the LLM prompt and diagnostic synthesizer lack explicit instructions for three high-ROI structural optimizations:
1. **Self-Join to Window Function Refactoring:** Rewriting `FROM table_a t1 JOIN table_a t2 ON ...` into single-pass `OVER (PARTITION BY ... ORDER BY ...)` analytic functions.
2. **Explicit `JOIN` Table Sequencing & Selective `WHERE` Predicate Ordering:** Placing the largest table (by `size_bytes` from `<schema_context>`) on the left side of `JOIN` trees followed by decreasing table sizes, and ordering `WHERE` clauses so the most selective partition/cluster/equality predicates execute before expensive expressions or UDFs.
3. **Denormalization Advisory (`ARRAY<STRUCT<...>>`):** Flagging queries that repeatedly join 1:N parent-child tables and providing an inline denormalization note alongside the rewritten SQL.

### 2. User Stories
* *As a Data Engineer*, when I inspect an expensive query in AI Doctor, I want the rewritten SQL (`OPTIMIZED_SQL`) to automatically sequence the largest table first in `JOIN` clauses using actual byte sizes from `INFORMATION_SCHEMA.TABLE_STORAGE` and convert any self-joins into `WINDOW` functions.
* *As a Database Architect*, I want AI Doctor to reorder `WHERE` predicates from highest selectivity (partition filter $\rightarrow$ cluster key equality $\rightarrow$ range filters $\rightarrow$ string functions) and warn me when joined tables should be denormalized into nested `STRUCT`/`ARRAY` fields.

### 3. Functional Requirements
1. **Prompt & Schema Context Enrichment (`src/main.py`):**
   * Sort `schemas_context` in descending order of `size_bytes` and explicitly annotate each table in `<schema_context>` with its rank (`[LARGEST TABLE — Place 1st in JOIN]`, `[2nd Largest]`, etc.).
   * Add mandatory SQL refactoring instructions to `prompt_content`:
     * **Rule A (Self-Join Elimination):** If any table in `<schema_context>` is joined to itself, rewrite the self-join using analytic window functions (`LAG`, `LEAD`, `FIRST_VALUE`, `LAST_VALUE`, `ROW_NUMBER`, or `COUNT/SUM OVER`).
     * **Rule B (Join Order by Table Size):** Sequence `JOIN` operations with the largest table (`size_bytes`) first in the `FROM` clause, followed by tables in decreasing byte-size order to optimize broadcast vs. hash shuffle distribution.
     * **Rule C (`WHERE` Clause Selectivity Ordering):** Order predicates in `WHERE` clauses starting with (1) Partition column filters, (2) Clustered column equality/IN filters, (3) Numeric/Date comparisons, and (4) `LIKE` / `REGEXP` / complex functions last.
     * **Rule D (Schema Denormalization Advice):** When a query joins a large parent table ($>10\text{ GiB}$) with a child detail table on an entity key (`order_id`, `user_id`, `transaction_id`, `session_id`), emit a `⚠️ Schema Modernization:` bullet recommending nesting the child rows as `ARRAY<STRUCT<...>>` inside the parent table.

### 4. Acceptance Criteria
1. Every table passed into `<schema_context>` includes its formatted size and explicit join-order rank when multiple tables are referenced.
2. AI Doctor's bulleted violation list categorizes findings with `[HIGH]` for un-rewritten self-joins on tables $>1\text{ GiB}$ and `[MEDIUM]` for suboptimal `JOIN` table ordering or `WHERE` selectivity ordering.
3. Rewritten queries continue to pass the existing dry-run syntax validation and echo-suppression gates.

---

## Feature 2.3: Schema Denormalization (`STRUCT` / `ARRAY`) Candidate Detector

### 1. Problem Statement
Relational database habits (3NF / star / snowflake schemas with dozens of normalized tables) perform poorly in BigQuery because distributed shuffle joins across multi-terabyte tables consume massive slot-hours and network I/O. Currently, the application audits individual tables in isolation (`run_static_schema_audit` checks if a single table is partitioned/clustered), but has **no visibility into cross-table join patterns** or where **native `STRUCT` and `ARRAY` (Nested & Repeated) denormalization** would eliminate expensive recurring `JOIN` operations.

### 2. User Stories
* *As a Data Architect*, I want to see which table pairs across my organization are most frequently joined together in high-cost queries, along with the cumulative slot-hours and TiB scanned by those joins.
* *As a Data Engineer*, I want the tool to inspect the schemas of frequently joined parent-child tables and generate a concrete `CREATE OR REPLACE TABLE ... AS SELECT ..., ARRAY_AGG(STRUCT(...))` DDL template so I can modernize normalized schemas into nested BigQuery structures.

### 3. Telemetry, Heuristics & Bifurcated Mathematical Model
1. **Join Graph Extraction & Cartesian Pair Filtering (`INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION`):**
   * Filter `job_type = 'QUERY'`, `state = 'DONE'`, `statement_type != 'SCRIPT'`, where `ARRAY_LENGTH(referenced_tables) BETWEEN 2 AND 10` and `REGEXP_CONTAINS(query, r'(?i)\bJOIN\b')`.
   * Generate ordered canonical table pairs `(table_a, table_b)` from `UNNEST(referenced_tables)` within each job.
   * **Anti-Phantom Pair Confirmation (Fixing Cartesian `referenced_tables` Noise):** Because `referenced_tables` flattens tables across independent CTEs and `UNION ALL` branches, a candidate pair is retained **only** if at least one sampled query text confirms both tables (or their `FROM`/`JOIN` aliases) and a valid join key co-occur inside an `ON` or `USING` predicate (`REGEXP_CONTAINS(clean_sql, r'(?i)\b(ON|USING)\b[^;]+?\b' || key_name || r'\b')`).
   * Aggregate across the lookback window (`lookback_days`, default 30), partitioning by execution billing mode (`reservation_id IS NOT NULL` for Editions vs. `reservation_id IS NULL` for On-Demand):
     * `join_execution_count`: Total confirmed join executions.
     * `distinct_query_patterns`: `COUNT(DISTINCT query_info.query_hashes.normalized_literals)`.
     * `editions_slot_hours`: $\sum_{\text{res}} \text{total\_slot\_ms} / 3.6 \times 10^6$.
     * `on_demand_bytes_processed_gib`: $\sum_{\text{od}} \text{total\_bytes\_processed} / 2^{30}$.
     * `total_shuffle_spill_gib`: $\sum \text{shuffle\_output\_bytes\_spilled} / 2^{30}$.
2. **Asymmetric & Audit-Safe Join Key Resolution (`INFORMATION_SCHEMA.TABLE_STORAGE` & `COLUMNS`):**
   * Designate the table with higher `total_rows` as the **Child / Detail Table** and the lower-row-count entity table as the **Parent / Header Table**.
   * **Audit Column Exclusion List:** Strip generic metadata/audit columns before key matching (`created_at`, `updated_at`, `deleted_at`, `ingested_at`, `status`, `state`, `type`, `is_active`, `is_deleted`, `source`, `version`, `country`, `region`, `env`, `tenant_id`, `org_id`).
   * **Symmetric & Asymmetric FK Matching:**
     1. *Symmetric Entity Keys:* Exact match on columns ending in `_id`, `_key`, `_uuid`, or `_code` (e.g., `parent.order_id = child.order_id`).
     2. *Asymmetric Primary/Foreign Keys:* Match `parent.id` against `child.<parent_table_singular>_id` (e.g., `orders.id = order_items.order_id`, stripping trailing `s`/`es`/`ies` from `parent_table`).
     3. *SQL Predicate Verification:* Confirm the resolved `<parent_join_key>` and `<child_join_key>` appear in the sampled query's `ON` / `USING` clause.
3. **DML Churn & Write Amplification Guardrail:**
   * Query `INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION` for daily DML/append operations targeting the child table (`statement_type IN ('INSERT', 'UPDATE', 'DELETE', 'MERGE')`).
   * Because updating or appending elements inside a nested `ARRAY<STRUCT>` requires rewriting the parent table partition, if `child_daily_dml_count > 100`, attach an explicit `high_dml_churn_warning`:
     > `⚠️ High DML Churn Detected on Child Table (<N> modifications/day): Denormalizing into an ARRAY<STRUCT> will require partition rewrites on child updates. Recommended only if child data is largely immutable post-insertion or rebuilt in batch.`
4. **Bifurcated Financial Savings Model (Editions Slots vs. On-Demand Bytes):**
   * BigQuery's columnar storage format stores nested `ARRAY<STRUCT>` subfields in separate column streams. Consequently, denormalization eliminates distributed hash-join shuffle overhead (massive slot reduction on Editions) while reducing On-Demand scanned bytes primarily by eliminating redundant foreign-key and join-index scans:
     * **Editions / Reservation Savings ($\$/\text{mo}$):** **40% reduction** in join slot-hours:
       $$\text{Editions Monthly Savings USD} = 0.40 \times \left(\frac{30}{\text{lookback\_days}}\right) \times \text{editions\_slot\_hours} \times \text{Slot-Hour Rate}$$
     * **On-Demand Savings ($\$/\text{mo}$):** **10% reduction** in child-table bytes processed (weighted by child-to-combined byte ratio $r_{\text{child}} = \frac{\text{child\_size}}{\text{parent\_size} + \text{child\_size}}$):
       $$\text{On-Demand Monthly Savings USD} = 0.10 \times r_{\text{child}} \times \left(\frac{30}{\text{lookback\_days}}\right) \times \text{on\_demand\_tiB} \times \text{On-Demand TiB Rate}$$
     * **Total Estimated Monthly Savings:** $\text{Editions Monthly Savings USD} + \text{On-Demand Monthly Savings USD}$.
5. **Pre-Aggregated Subquery Denormalization DDL Template (Avoids `GROUP BY ALL` Failures):**
   * Never emit `SELECT p.*, ARRAY_AGG(...) GROUP BY ALL`, which fails at runtime when `p.*` contains un-groupable types (`JSON`, `GEOGRAPHY`, existing `ARRAY`/`STRUCT`) and causes severe memory pressure on wide parent rows. Instead, pre-aggregate child records by `<child_join_key>` in a subquery before joining to `p.*`:
   ```sql
   -- Denormalize `<child_project>.<child_dataset>.<child_table>` into `<parent_project>.<parent_dataset>.<parent_table>_denorm`
   -- Join Keys: p.`<parent_join_key>` = c.`<child_join_key>` | Eliminates ~<join_execution_count> joins (<combined_slot_hours> slot-hrs)
   CREATE OR REPLACE TABLE `<parent_project>.<parent_dataset>.<parent_table>_denorm`
   PARTITION BY <parent_partition_expr>
   CLUSTER BY <parent_join_key> AS
   SELECT
     p.*,
     COALESCE(c.child_items, ARRAY<STRUCT<<child_struct_type_signature>>>[]) AS <child_table>_items
   FROM `<parent_project>.<parent_dataset>.<parent_table>` AS p
   LEFT JOIN (
     SELECT
       `<child_join_key>`,
       ARRAY_AGG(
         STRUCT(<child_non_key_columns>)
         IGNORE NULLS
       ) AS child_items
     FROM `<child_project>.<child_dataset>.<child_table>`
     GROUP BY `<child_join_key>`
   ) AS c
     ON p.`<parent_join_key>` = c.`<child_join_key>`;
   ```

### 4. API & Data Model Specification (`src/main.py`)
```python
class DenormalizationParams(FocusMixin):
    org_project_id: Optional[str] = None
    lookback_days: int = Field(default=30, ge=1, le=90)
    min_join_executions: int = Field(default=5, ge=2, le=1000)
    limit: int = Field(default=25, ge=1, le=100)
    max_bytes_billed_gb: Optional[int] = None

class DenormalizationCandidateResult(BaseModel):
    parent_table_fqn: str
    child_table_fqn: str
    parent_size_gib: float
    child_size_gib: float
    parent_join_key: str
    child_join_key: str
    inferred_join_keys: List[str]
    join_execution_count: int
    distinct_query_patterns: int
    combined_slot_hours: float
    combined_bytes_processed_gib: float
    shuffle_spill_gib: float
    child_daily_dml_count: float = 0.0
    high_dml_churn_warning: Optional[str] = None
    editions_monthly_savings_usd: float = 0.0
    on_demand_monthly_savings_usd: float = 0.0
    estimated_monthly_savings_usd: float
    sample_job_id: str
    sample_project_id: str
    recommended_ddl: str
```

### 5. Acceptance Criteria
1. `POST /api/schema/denormalization_candidates` returns ranked table pairs ordered by `estimated_monthly_savings_usd DESC`, with separate `editions_monthly_savings_usd` (40% slot reduction) and `on_demand_monthly_savings_usd` (10% child scan reduction) breakdowns.
2. Generated DDL strictly uses the **pre-aggregated child subquery pattern** (`GROUP BY <child_join_key>` inside `LEFT JOIN (...)`), ensuring 100% compatibility with parent tables containing `JSON`, `GEOGRAPHY`, `ARRAY`, or `STRUCT` columns.
3. Join key resolution filters out generic audit columns (`created_at`, `status`, etc.), resolves asymmetric `id` $\leftrightarrow$ `<singular_parent>_id` keys, and verifies co-occurrence inside the query's `ON` or `USING` clause.
4. Child tables exceeding 100 DML/append operations per day surface `high_dml_churn_warning` in both the UI badge and the Executive Assessment Report (`SCH-DENORM-01`).

---

## Feature 2.4: Proactive Materialized View (MV) Candidate & Smart-Tuning Recommender

### 1. Problem Statement
Currently, the application includes two Materialized View endpoints—`analyze_mv_costs` (`src/main.py`, `/api/antipatterns/mv`, which finds expensive refreshes on *existing* MVs) and `analyze_mv_rejections` (`src/main.py`, `/api/mv/analyze`, which diagnoses why *existing* MVs were skipped by the optimizer). However, the application **cannot discover where NEW Materialized Views should be created** for BigQuery users who are running recurring `GROUP BY` aggregations directly against raw base tables.

### 2. User Stories
* *As a Analytics Engineer*, I want to automatically discover the top recurring `GROUP BY` aggregation queries running against base tables so I can convert them into precomputed Materialized Views.
* *As a FinOps Practitioner*, I want to see the net monthly savings (Query Compute Saved via BigQuery **Smart Tuning** minus Estimated Background Refresh Cost) before creating a Materialized View.
* *As a BI Developer*, I want a generated `CREATE MATERIALIZED VIEW` DDL statement with `enable_refresh = true` and an appropriate `refresh_interval_minutes` so that existing Looker/Tableau dashboards querying the base table automatically accelerate via Smart Tuning without changing dashboard SQL.

### 3. Telemetry, Smart-Tuning Eligibility & Net ROI Model
1. **Candidate Aggregation Discovery (`INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION`):**
   * Group completed `QUERY` jobs by `query_info.query_hashes.normalized_literals` where:
     * `execution_count >= min_executions` (default `5` runs in the lookback window),
     * `ARRAY_LENGTH(referenced_tables) BETWEEN 1 AND 3` (BigQuery MVs support single base tables and inner/left joins),
     * `statement_type = 'SELECT'`,
     * Query text contains `GROUP BY` and standard MV-compatible aggregate functions (`SUM`, `COUNT`, `AVG`, `MIN`, `MAX`, `COUNT_STAR`, `APPROX_COUNT_DISTINCT`, `HLL_COUNT.INIT`),
     * Excludes queries containing non-deterministic functions incompatible with MVs (`RAND()`, `CURRENT_TIMESTAMP()`, `SESSION_USER()`, `EXTERNAL_QUERY`).
2. **Net Financial ROI Calculation (Savings Minus Refresh Overhead):**
   * **Gross Monthly Query Cost ($C_{\text{gross}}$):** Monthly-normalized compute cost of all historical executions matching the `normalized_literals` hash (plus sibling queries hitting the same base table with `GROUP BY`).
   * **Smart Tuning Reduction Factor ($\eta_{\text{scan}} = 0.85$):** Pre-aggregated MVs typically reduce bytes scanned and slot-ms by **85%–98%**.
   * **Estimated Monthly Background Refresh Cost ($C_{\text{refresh}}$):** Inferred from the base table's DML/append frequency in `JOBS_BY_ORGANIZATION` (`avg_daily_modifications`, capped by `refresh_interval_minutes = 60` i.e., max 24 refreshes/day, scanning only incremental delta partitions estimated at $2\%$ of base table size per refresh).
   * **Net Monthly Savings ($\text{USD}/\text{mo}$):**
     $$\text{Net Monthly Savings USD} = \max\left(0,\; (0.85 \times C_{\text{gross}}) - C_{\text{refresh}}\right)$$
3. **Generated `CREATE MATERIALIZED VIEW` DDL:**
   * Synthesizes a valid `CREATE MATERIALIZED VIEW IF NOT EXISTS` DDL block from the representative query's `SELECT ... FROM ... WHERE ... GROUP BY ...` projection (stripping `ORDER BY` and `LIMIT`, which are not permitted in BigQuery MV definitions, while noting that Smart Tuning still accelerates outer queries that apply `ORDER BY` / `LIMIT`).
   ```sql
   -- Smart Tuning MV Candidate for `<base_table_fqn>`
   -- Matched Executions: <execution_count> runs | Net Savings: $<net_monthly_savings_usd>/mo
   -- Note: BigQuery Smart Tuning automatically reroutes base-table queries to this MV.
   CREATE MATERIALIZED VIEW IF NOT EXISTS `<project_id>.<dataset_id>.mv_<base_table>_agg_<short_hash>`
   OPTIONS (
     enable_refresh = true,
     refresh_interval_minutes = 60,
     max_staleness = INTERVAL "4:0:0" HOUR TO SECOND
   ) AS
   <sanitized_select_from_where_group_by_sql>;
   ```

### 4. API & Data Model Specification (`src/main.py`)
```python
class MVCandidateParams(FocusMixin):
    org_project_id: Optional[str] = None
    lookback_days: int = Field(default=30, ge=1, le=90)
    min_executions: int = Field(default=5, ge=2, le=1000)
    limit: int = Field(default=20, ge=1, le=100)
    max_bytes_billed_gb: Optional[int] = None

class MVCandidateResult(BaseModel):
    query_hash: str
    project_id: str
    base_tables: List[str]
    sample_job_id: str
    sample_user_email: str
    execution_count: int
    total_bytes_processed_gib: float
    total_slot_hours: float
    gross_monthly_cost_usd: float
    est_monthly_refresh_cost_usd: float
    net_monthly_savings_usd: float
    smart_tuning_compatible: bool = True
    compatibility_notes: str = ""
    recommended_mv_ddl: str
```

### 5. Acceptance Criteria
1. `POST /api/mv/candidates` filters out queries using non-deterministic functions (`CURRENT_DATE`, `RAND`) or transforms `ORDER BY`/`LIMIT` out of the inner MV definition while preserving valid `SELECT ... GROUP BY` clauses.
2. Candidates where `est_monthly_refresh_cost_usd >= 0.85 * gross_monthly_cost_usd` (e.g., ultra-high-churn streaming tables with low query reuse) are automatically suppressed so the tool never recommends creating a future "Zombie MV".
3. Integrates into the **Materialized Views** UI tab (alongside *Zombie MV Refresh Auditor* and *MV Rejection Analyzer*) and into **Section 4 / Section 6** of `src/report_generator.py` under finding ID `QRY-MV-NEW-01`.
