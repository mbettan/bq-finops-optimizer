# GAP-28.2 — Risk Status Semantics & FinOps Glossary

| Field | Value |
| :--- | :--- |
| **Spec ID** | GAP-28.2 |
| **Epic** | [GAP-28 — Enterprise Field Readiness](README.md) |
| **Status** | `SHIPPED` |
| **Module** | Storage Hygiene & Time Travel |
| **Target Release** | v1.4.4 |
| **Depends On** | — (soft: [GAP-28.0](GAP-28.0-ci-foundation.md) bundle-sync gate) |
| **Blocks** | — |
| **Estimated Effort** | M — 2-3 days |
| **Owner** | _unassigned_ |
| **Last Updated** | September 16, 2026 |

---

## 1. Problem

Enterprise users ask two questions about the Storage Optimizer table:

1. **"What does *Risk Status* actually mean?"** — it is undocumented anywhere in the product.
2. **"Why is *everything* red?"** — because the badge has exactly two states and one of them is the
   `else` branch. A 4 MiB table renders as **High Risk**.

A severity scale where nothing is ever low erodes trust in every other number on the page.

Compounding it: `Critical` / `High` are **already used for a completely unrelated scale** in the exported
report, so a customer reading both artifacts sees the same words meaning different things.

## 2. Evidence

**Current badge logic** — `static/app.js:2065-2072`:

```javascript
const sizeGB = row.size_bytes / (1024 * 1024 * 1024);
if (sizeGB > 1000 || row.row_count > 1000000000) {
    // Critical
} else {
    // High   ← everything else. No floor. No partition/cluster term.
}
```

**Fact 1 — the result set is already pre-filtered.** `src/main.py:555`:

```sql
AND (COALESCE(s.total_partitions, 0) = 0 OR c.clustering_fields IS NULL)
```

The table only ever lists tables missing partitioning **or** clustering. "Everything is red" is
structural, not a bug in the query. Note the `OR`: a table that *is* partitioned but not clustered still
appears — and today is graded purely on size, so a partitioned 2 TiB table is labelled **Critical**.

**Fact 2 — the signals we need are already on every row.** `src/main.py:494-497`:

```python
is_partitioned: bool
partition_column: Optional[str] = None
is_clustered: bool
clustering_fields: Optional[str] = None
```

They are already rendered by `getPartitionStatus` / `getClusterStatus` in `static/app.js` — the badge
simply ignores them. **No backend change is required.**

**Fact 3 — the vocabulary collision.** `src/report_generator.py:92-96` and `:1967`:

```python
PRIORITY_ORDER = {"Critical": -1, "High": 0, "Medium": 1, "Low": 2, "Info": 3}
# "Priority bands: Critical ≥ 75, High ≥ 50, Medium ≥ 25, Low ≥ 10, Info < 10."
```

That is a 0-100 recommendation impact score — unrelated to storage risk, identical words.

## 3. Goals / Non-Goals

**Goals**
* Make the badge discriminate, using signals already present on the row.
* Document all risk vocabularies used in the product, in the product.
* Remove the ambiguity between the three scales.

**Non-Goals**
* Changing the storage audit SQL or its pre-filter.
* Adding a new "risk score" number.
* Re-grading capacity SLA tiers (documented here, not changed).

## 4. Design

### 4.1 Four-state storage risk model

Single shared function in `static/app.js`, consumed by the table, the tooltip and the glossary:

```javascript
const GIB = 1024 ** 3;

/**
 * Storage risk for a table in the Storage Optimizer audit.
 * NOTE: the backend already restricts this table to rows missing partitioning
 * OR clustering (src/main.py:555), so `managed === 2` is not reachable here.
 */
const classifyStorageRisk = (row) => {
  const sizeGiB = (row.size_bytes || 0) / GIB;
  const rows    = Number(row.row_count) || 0;
  const managed = (row.is_partitioned ? 1 : 0) + (row.is_clustered ? 1 : 0);
  const isLarge = sizeGiB > 1000 || rows > 1e9;

  if (sizeGiB < 1)              return 'low';
  if (isLarge && managed === 0) return 'critical';
  if (isLarge)                  return 'high';
  if (managed === 0)            return 'medium';
  return 'low';
};
```

| Badge | Condition | Meaning shown in tooltip |
| :--- | :--- | :--- |
| 🔴 **Critical** | (> 1 TiB **or** > 1 B rows) **and** neither partitioned nor clustered | Full-scan blowout risk; highest remediation ROI. |
| 🟠 **High** | (> 1 TiB **or** > 1 B rows) **and** exactly one optimization present | Large but partially managed. |
| 🟡 **Medium** | 1 GiB – 1 TiB **and** neither optimization present | Worth fixing opportunistically. |
| ⚪ **Low** | < 1 GiB | Listed for completeness; negligible cost impact. |

> [!WARNING]
> **Customer-visible output change.** Tables previously badged **High** will move to **Medium** or **Low**.
> A saved snapshot rendered by an older bundle will disagree with a new one. Requires a `RELEASE_NOTES.md`
> entry and a one-line note in the Storage Optimizer view subtitle for the first release.

Badge markup must use theme tokens, not the current hardcoded `#f87171` / `rgba(239,68,68,.15)` — see
[GAP-28.3](GAP-28.3-dual-theme.md). If 28.3 has not landed yet, emit a CSS class
(`risk-badge risk-badge--critical`) rather than an inline style, so the sweep has nothing to do here.

### 4.2 Disambiguating three scales

| # | Scale | Where | Values | Basis |
| :--- | :--- | :--- | :--- | :--- |
| 1 | **Storage Risk** | Storage Optimizer badge | Critical / High / Medium / Low | Table size + optimization state (§4.1) |
| 2 | **Capacity SLA Risk** | Slots simulator tiers | Low / Balanced / Aggressive | Share of slot-demand percentile covered by baseline: ≈p99 / ≈p90 / ≈p75 |
| 3 | **Recommendation Priority** | Exported report | Critical / High / Medium / Low / Info | 0-100 impact score, banded ≥75 / ≥50 / ≥25 / ≥10 / <10 (`src/report_generator.py:96`) |

### 4.3 Delivery

* **Inline `(?)` icons** next to every risk badge and every tier label, with a micro-tooltip giving the
  one-line definition and the exact threshold that fired for *that* row.
* **"Methodology & FinOps Glossary" modal**, reachable from the top nav, documenting all three scales with
  their exact numeric thresholds, plus a short "Why is my table listed at all?" note explaining the
  `src/main.py:555` pre-filter.
* Glossary thresholds are **rendered from the same constants** the classifier uses, so documentation and
  behavior cannot drift.

## 5. Implementation Tasks

- [ ] Extract thresholds into named constants (`RISK_LARGE_GIB = 1000`, `RISK_LARGE_ROWS = 1e9`, `RISK_MIN_GIB = 1`).
- [ ] Implement `classifyStorageRisk(row)` and replace the inline conditional at `static/app.js:2065`.
- [ ] Render badges via CSS classes (`risk-badge--{level}`), removing the inline color literals.
- [ ] Add `(?)` tooltips stating which threshold fired for that row.
- [ ] Build the "Methodology & FinOps Glossary" modal covering all three scales, driven by the shared constants.
- [ ] Add the "Why is my table listed?" explanation of the `main.py:555` pre-filter.
- [ ] Add a one-release transitional note in the Storage Optimizer view subtitle about the reclassification.
- [ ] Run `scripts/sync_docs_bundle.sh`.
- [ ] Tests at every boundary (see AC 1).
- [ ] `RELEASE_NOTES.md` entry flagging the customer-visible change.

## 6. Acceptance Criteria

1. `classifyStorageRisk` is unit-tested at every boundary: 0.99/1.0/1.01 GiB, 999/1000/1001 GiB, 1e9−1 / 1e9 / 1e9+1 rows, × all four `is_partitioned`/`is_clustered` combinations.
2. A 4 MiB unpartitioned table renders **Low**, not High.
3. A 2 TiB partitioned-but-unclustered table renders **High**, not Critical.
4. A 2 TiB table with neither optimization renders **Critical**.
5. Every badge carries a `(?)` tooltip naming the threshold that fired.
6. The glossary modal documents all three scales with numeric thresholds, and those numbers are read from the same constants used by the classifier (asserted by a test).
7. No color literal remains in the badge rendering path.
8. `RELEASE_NOTES.md` states plainly that some tables will change severity after upgrade.
9. `static/` and `docs/static/` remain byte-identical.

## 7. Risks & Mitigations

| Risk | Mitigation |
| :--- | :--- |
| Users notice tables "improving" without any action and distrust the tool | Explicit release note + one-release in-product transitional note |
| Downgrading to Low hides a genuinely expensive small table | The pre-filter still lists it; Low means "negligible *storage* cost", stated in the tooltip |
| Three scales still confuse people | Q1 (rename report bands to P0…P3/FYI) resolves it properly — see Open Questions |
| Saved snapshots re-render with different badges | Expected and documented; badges are derived at render time, never persisted |

## 8. Rollback

Revert `classifyStorageRisk` to the two-state conditional. The glossary modal and tooltips are additive
and should be kept — they are net-positive regardless of the classifier.

## 9. Open Questions

| # | Question | Default |
| :--- | :--- | :--- |
| 1 | Rename report priority bands to `P0 / P1 / P2 / P3 / FYI` to end the collision? *(epic Q1)* | Keep current names; disambiguate in the glossary only. |
| 2 | Should **Low** rows be hidden behind a "show all" toggle? | No — hiding rows invites "is it complete?" questions. |
| 3 | Should the badge factor in Time Travel / fail-safe bytes? | No — out of scope; storage hygiene has its own view. |
