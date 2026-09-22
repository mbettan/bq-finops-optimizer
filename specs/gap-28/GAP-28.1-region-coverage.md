# GAP-28.1 — Global Region Coverage & Error Taxonomy

| Field | Value |
| :--- | :--- |
| **Spec ID** | GAP-28.1 |
| **Epic** | [GAP-28 — Enterprise Field Readiness](README.md) |
| **Status** | `SHIPPED` |
| **Module** | Settings & Project Scoping |
| **Target Release** | v1.4.4 |
| **Depends On** | — (soft: [GAP-28.0](GAP-28.0-ci-foundation.md) bundle-sync gate) |
| **Blocks** | — |
| **Estimated Effort** | M — 2-3 days |
| **Owner** | _unassigned_ |
| **Last Updated** | September 16, 2026 |

---

## 1. Problem

The Region dropdown offers **seven** options. Five are US, one is `europe-west1`, one is the EU
multi-region. Every evaluator with EU-resident data concludes the product does not support their
geography and stops.

Behind that, the codebase carries two incompatible region conventions, so "just add more options" is the
one change guaranteed to produce a partial, confusing failure.

## 2. Evidence

**Current dropdown** — `static/index.html:502-510`:

```html
<select id="cfg-region">
  <option value="region-us">US (Multi-region)</option>
  <option value="region-eu">EU (Multi-region)</option>
  <option value="region-us-east4">us-east4</option>
  <option value="region-us-east1">us-east1</option>
  <option value="region-us-west1">us-west1</option>
  <option value="region-us-central1">us-central1</option>
  <option value="region-europe-west1">europe-west1</option>
</select>
```

**The convention split (Trap T3):**

| Location | Form expected | Code |
| :--- | :--- | :--- |
| UI + all Pydantic models | **prefixed** | `region: str = "region-us"` — `src/main.py:460, 788, 1225, 1353, 1537, 2005` |
| BigQuery job `location` | **bare** | `location=params.region.replace("region-", "")` — `src/main.py:2628` |
| Project discovery | **bare** | `def discover_projects_via_table_storage(billing_project, region="us")` — `src/bqrecommender.py:226`, building ``f"FROM `region-{region}`.…"`` at `:234` |

Passing `region-us` into `bqrecommender` yields ``region-region-us`` — a silent, path-dependent failure.

**Existing error-handling precedent** — `src/main.py:2170-2174` already converts a `Forbidden` into an
HTTP 403 naming the exact missing role. That pattern is the right one; it is simply not applied
consistently across region-scoped paths.

## 3. Goals / Non-Goals

**Goals**
* Offer every BigQuery region, grouped by geography, in both the app and the Pages simulator.
* Collapse the two conventions into one canonical form with explicit converters.
* Make an unsupported or empty region produce an actionable message, never a raw stack trace.
* Preserve a user's custom/exotic region across reloads.

**Non-Goals**
* Auto-detecting the user's regions by probing (expensive, needs permissions we may not have).
* Multi-region simultaneous analysis (separate feature).
* Changing any INFORMATION_SCHEMA query shape.

## 4. Design

### 4.1 Canonical region registry — `src/constants.py`

`src/constants.py` is currently 15 lines and becomes the single source of truth. Canonical form is
**prefixed** (`region-europe-west1`), matching the UI and every Pydantic model — the smallest diff.

```python
"""Canonical BigQuery region registry and normalization helpers."""

# (canonical_value, label, group, supports_org_views)
BQ_REGIONS: tuple[tuple[str, str, str, bool], ...] = (
    ("region-us", "US (multi-region)", "Multi-region", True),
    ("region-eu", "EU (multi-region)", "Multi-region", True),

    ("region-europe-west1",      "europe-west1 (Belgium)",      "Europe", True),
    ("region-europe-west2",      "europe-west2 (London)",       "Europe", True),
    ("region-europe-west3",      "europe-west3 (Frankfurt)",    "Europe", True),
    ("region-europe-west4",      "europe-west4 (Netherlands)",  "Europe", True),
    ("region-europe-west6",      "europe-west6 (Zurich)",       "Europe", True),
    ("region-europe-west8",      "europe-west8 (Milan)",        "Europe", True),
    ("region-europe-west9",      "europe-west9 (Paris)",        "Europe", True),
    ("region-europe-west10",     "europe-west10 (Berlin)",      "Europe", False),
    ("region-europe-west12",     "europe-west12 (Turin)",       "Europe", False),
    ("region-europe-north1",     "europe-north1 (Finland)",     "Europe", True),
    ("region-europe-central2",   "europe-central2 (Warsaw)",    "Europe", True),
    ("region-europe-southwest1", "europe-southwest1 (Madrid)",  "Europe", True),

    ("region-us-central1",             "us-central1 (Iowa)",              "Americas", True),
    ("region-us-east1",                "us-east1 (South Carolina)",       "Americas", True),
    ("region-us-east4",                "us-east4 (N. Virginia)",          "Americas", True),
    ("region-us-west1",                "us-west1 (Oregon)",               "Americas", True),
    ("region-us-west2",                "us-west2 (Los Angeles)",          "Americas", True),
    ("region-us-west4",                "us-west4 (Las Vegas)",            "Americas", True),
    ("region-northamerica-northeast1", "northamerica-northeast1 (Montréal)","Americas", True),
    ("region-southamerica-east1",      "southamerica-east1 (São Paulo)",  "Americas", True),

    ("region-asia-northeast1",      "asia-northeast1 (Tokyo)",        "Asia-Pacific", True),
    ("region-asia-southeast1",      "asia-southeast1 (Singapore)",    "Asia-Pacific", True),
    ("region-asia-south1",          "asia-south1 (Mumbai)",           "Asia-Pacific", True),
    ("region-australia-southeast1", "australia-southeast1 (Sydney)",  "Asia-Pacific", True),

    ("region-me-west1",    "me-west1 (Tel Aviv)", "Middle East", True),
    ("region-me-central1", "me-central1 (Doha)",  "Middle East", False),
)

_VALID = {r[0] for r in BQ_REGIONS}


def normalize_region(value: str) -> str:
    """Accept 'us', 'region-us', 'REGION-US' → canonical 'region-us'."""
    v = (value or "").strip().lower()
    if not v:
        return "region-us"
    return v if v.startswith("region-") else f"region-{v}"


def bare_region(value: str) -> str:
    """Canonical → BigQuery job `location` ('region-europe-west1' → 'europe-west1')."""
    return normalize_region(value).removeprefix("region-")


def is_known_region(value: str) -> bool:
    return normalize_region(value) in _VALID
```

> [!NOTE]
> `supports_org_views` must be **verified against current Google Cloud documentation during
> implementation**, not taken from this table on faith. The flag drives dropdown annotation and the 404
> message, so a wrong value produces a misleading (not broken) experience.

### 4.2 Call-site migration

| Location | Change |
| :--- | :--- |
| All Pydantic models with a `region` field | Add a `field_validator` calling `normalize_region` so any inbound form is canonicalized once, at the edge |
| `src/main.py:2628` | `location=params.region.replace("region-", "")` → `location=bare_region(params.region)` |
| `src/bqrecommender.py:226-234` | Accept either form: build SQL with `normalize_region(region)` and pass `bare_region(region)` as the job `location`. **This is a behavior fix** — today the function is correct only for bare input |
| Any other `replace("region-", "")` | Replace with `bare_region()` (grep before finishing) |

### 4.3 Dropdown rendering

Both `static/index.html` and `docs/static/`-backed `docs/simulator.html` get `<optgroup>`-grouped options
generated from `BQ_REGIONS`, ordered Multi-region → Europe → Americas → Asia-Pacific → Middle East.
Regions with `supports_org_views=False` render with a trailing `⚠` in the label.

Custom-region preservation in `initUI()`:

```javascript
// Preserve an exotic/custom region the user previously saved.
const saved = localStorage.getItem('bq_region');
if (saved && !select.querySelector(`option[value="${CSS.escape(saved)}"]`)) {
  const group = document.createElement('optgroup');
  group.label = 'Custom';
  const opt = document.createElement('option');
  opt.value = saved;
  opt.textContent = `${saved} (custom)`;
  group.appendChild(opt);
  select.appendChild(group);
}
select.value = saved || 'region-us';
```

> [!WARNING]
> Build options with `createElement` / `textContent`, never `innerHTML` with an interpolated
> `localStorage` value.

### 4.4 Error taxonomy

Listing every region is only safe if failures explain themselves. One helper, applied to every
region-scoped query path, generalizing `src/main.py:2170-2174`:

| Condition | Upstream | HTTP | Message |
| :--- | :--- | :--- | :--- |
| Caller lacks the org role | `google.api_core.exceptions.Forbidden` | 403 | "Organization-level `roles/bigquery.resourceViewer` is required to read `{view}` in `{region}`. Ask your admin to bind it at the organization node — see README §IAM Roles & Permissions." |
| View unavailable in region | `NotFound` | 404 | "`{view}` is not available in `{region}`. This region does not expose organization-scoped INFORMATION_SCHEMA. Try `region-eu`, or pick a region without the ⚠ marker." |
| No activity in region | empty result | 200 | Empty-state card: "No BigQuery activity found in `{region}` over the last `{n}` days." — **not** an error |
| Unknown / malformed region | validation | 400 | "`{value}` is not a recognised BigQuery region." |

## 5. Implementation Tasks

- [ ] Verify each region's `supports_org_views` value against current Google Cloud documentation.
- [ ] Write `BQ_REGIONS`, `normalize_region`, `bare_region`, `is_known_region` in `src/constants.py`.
- [ ] Add `field_validator`s to every Pydantic model carrying a `region`.
- [ ] Replace `src/main.py:2628` string surgery with `bare_region()`; grep for other `replace("region-"` sites.
- [ ] Fix the bare/prefixed inconsistency in `src/bqrecommender.py`.
- [ ] Add the region error-taxonomy helper; wire it into every region-scoped query path.
- [ ] Regenerate the `<optgroup>` dropdown in `static/index.html` **and** `docs/simulator.html`.
- [ ] Implement custom-region preservation in `initUI()` (DOM API, not `innerHTML`).
- [ ] Run `scripts/sync_docs_bundle.sh`.
- [ ] Tests: round-trip, `bqrecommender` equivalence, taxonomy mapping, dropdown parity.
- [ ] `RELEASE_NOTES.md` entry.

## 6. Acceptance Criteria

1. Every `<option value>` in both HTML files is `region-` prefixed and present in `BQ_REGIONS`; a test asserts dropdown ⊆ registry and Europe ⊆ dropdown.
2. `bare_region(normalize_region(x)) == x.removeprefix("region-")` holds for every entry in `BQ_REGIONS`.
3. `discover_projects_via_table_storage` produces byte-identical SQL for `"us"` and `"region-us"`.
4. Selecting a `supports_org_views=False` region returns the taxonomy 404 message — no stack trace, no generic 500.
5. A `Forbidden` on any region-scoped path returns a 403 naming the required role and the view.
6. A region with no activity renders the empty-state card, not an error toast.
7. A custom region written to `localStorage.bq_region` survives a reload and a settings save, and appears under a "Custom" optgroup.
8. `static/` and `docs/static/` remain byte-identical (GAP-28.0 gate).

## 7. Risks & Mitigations

| Risk | Mitigation |
| :--- | :--- |
| `supports_org_views` flags are wrong | Verify against docs at implementation time; the 404 copy still guides the user even if a flag is wrong |
| A `region` field is missed and a raw value reaches a query | Validators at the model edge, plus a grep-based test for `replace("region-"` outside `constants.py` |
| Listing ~30 regions makes the dropdown unwieldy | `<optgroup>` grouping + geographic ordering; the field objection is absence, not length |
| Saved snapshots contain a now-normalized region string | `normalize_region` is idempotent and accepts both forms, so old snapshots keep loading |

## 8. Rollback

Revert the dropdown to the previous seven options and keep `constants.py` — the helpers are additive and
harmless. The `bqrecommender` fix should be kept regardless; it repairs a latent bug.

## 9. Open Questions

| # | Question | Default |
| :--- | :--- | :--- |
| 1 | Should the ⚠ regions be disabled rather than annotated? | No — annotate; a user may have valid reasons to try, and the 404 explains. |
| 2 | Offer a free-text "other region" input alongside the dropdown? | No — `localStorage` preservation covers the exotic case without new UI. |
