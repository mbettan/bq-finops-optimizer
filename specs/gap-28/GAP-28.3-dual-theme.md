# GAP-28.3 — Dual Theme Engine (Corporate Light Mode)

| Field | Value |
| :--- | :--- |
| **Spec ID** | GAP-28.3 |
| **Epic** | [GAP-28 — Enterprise Field Readiness](README.md) |
| **Status** | `SHIPPED` |
| **Module** | General UI / UX |
| **Target Release** | v1.4.4 |
| **Depends On** | [GAP-28.0 — CI Foundation](GAP-28.0-ci-foundation.md) *(hard: the visual + contrast gates)* |
| **Blocks** | — |
| **Estimated Effort** | L — 6-8 days |
| **Owner** | _unassigned_ |
| **Last Updated** | September 16, 2026 |

---

## 1. Problem

The product is dark-mode only. Enterprise finance stakeholders will not put text-on-black into a CFO
deck, and several evaluations have stalled on presentation rather than substance.

This blocks the artifact that actually travels *inside* the customer organization, so it is a
distribution problem disguised as a cosmetic one.

## 2. Evidence

### 2.1 The token system is not what a light theme normally assumes

`static/style.css:6` — tokens are declared on a **`.dark-theme` class**, in **two layers**:

```css
.dark-theme {
    /* layer 1 — raw palette, shadcn-style HSL triplets */
    --background: 222 47% 5%;
    --primary: 217 91% 60%;
    --chart-2: 162 72% 46%;

    /* layer 2 — derived application mapping */
    --bg-primary: hsl(var(--background));
    --accent-color: hsl(var(--primary));
    --text-primary: hsl(var(--foreground));
    --glass-bg: hsla(222, 41%, 8%, 0.65);
    --success: var(--chart-2);
}
```

Applied at `static/index.html:108` and `docs/simulator.html:420` as `<body class="dark-theme">`.
**358** `var(--…)` call sites already consume it.

### 2.2 Scale of hardcoded color outside the token system

| Surface | Color literals | Notes |
| :--- | ---: | :--- |
| `static/style.css` | 210 (106 unique) | Many outside the token block |
| `static/index.html` | 753 `style="…"` attributes | Layout **and** color mixed in the same attribute |
| `static/app.js` | 324 | Inside template literals that build table rows |

Representative — `static/app.js:2068`:

```javascript
riskBadge = `<span style="… background: rgba(239, 68, 68, 0.15); color: #f87171;
             border: 1px solid rgba(239, 68, 68, 0.3);">Critical Risk</span>`;
```

`#f87171` on `#ffffff` is **2.6:1**. WCAG AA requires **4.5:1**. The nine most frequent literals
(`#94a3b8` ×17, `#cbd5e1`, `#38bdf8`, `#60a5fa`, `#4ade80`, `#facc15`, `#f87171`, `#fbbf24`, `#10b981`)
all fail on white.

### 2.3 Charting

Chart.js **v4.5.0** (`static/index.html:44`). Only **three** chart instances exist and all are already
tracked on `state`: `state.jobsScatterChart` (`app.js:1647`), `state.actualProvisioningChart`
(`app.js:3029`), `state.slotsChart` (`app.js:3159`). None of them set tick or grid colors — they inherit
`Chart.defaults`.

## 3. Goals / Non-Goals

**Goals**
* A Corporate Light theme matching the Google Cloud Console aesthetic, toggleable at runtime.
* Dark remains the default; explicit user choice persists.
* No page reload, no state loss, and **no BigQuery refetch** on toggle.
* WCAG AA contrast in both themes, enforced in CI.

**Non-Goals**
* Theming the exported HTML/PDF report (`static/report.css`, 1,196 lines) — epic Q2, deferred.
* A theme editor or additional themes.
* Any visual redesign — same layout, different palette.

## 4. Design

### 4.1 Token restructure (Trap T1)

> **Rule:** swapping `class="dark-theme"` → `class="light-theme"` removes **layer 2 as well**, so every
> `--bg-primary`, `--glass-bg`, `--accent-color`, `--text-tertiary`, `--success` resolves to nothing. The
> page renders unstyled, not light.
>
> **Solution:** split the layers. Hoisting layer 2 to `:root` does **not** work — `var(--background)` is
> defined on `body` and custom properties do not inherit upward. Both blocks must target the same element.

```css
/* ── Layer 2: shared derived mapping — theme-agnostic ── */
.dark-theme,
.light-theme {
    --bg-primary: hsl(var(--background));
    --bg-secondary: hsl(var(--card));
    --accent-color: hsl(var(--primary));
    --text-primary: hsl(var(--foreground));
    --text-secondary: hsl(var(--muted-foreground));
    --success: var(--chart-2);
    --success-bg: hsla(var(--chart-2), 0.1);
    --warning: var(--chart-3);
    --danger: var(--destructive);
    --tier-balanced: var(--chart-1);
    /* …every existing derived token, relocated verbatim out of .dark-theme… */
}

/* ── Layer 1: dark palette — values unchanged ── */
.dark-theme { --background: 222 47% 5%; /* … */ }

/* ── Layer 1: Corporate Light palette ── */
.light-theme {
    color-scheme: light;
    --background: 210 17% 98%;      /* #f8f9fa */
    --foreground: 210 8% 13%;       /* #202124 */
    --card: 0 0% 100%;
    --card-foreground: 210 8% 13%;
    --popover: 0 0% 100%;
    --primary: 217 89% 51%;         /* #1a73e8 */
    --primary-foreground: 0 0% 100%;
    --secondary: 210 17% 95%;       /* #f1f3f4 */
    --muted: 210 17% 95%;
    --muted-foreground: 213 5% 39%; /* #5f6368 — 5.9:1 on white ✔ */
    --accent: 214 89% 95%;          /* #e8f0fe */
    --destructive: 2 68% 45%;       /* #c5221f — 5.9:1 ✔ */
    --border: 213 9% 86%;           /* #dadce0 */
    --input: 213 9% 86%;
    --ring: 217 89% 51%;
    --chart-1: 217 89% 51%;
    --chart-2: 145 79% 27%;         /* #137333 — 5.1:1 ✔ */
    --chart-3: 35 100% 46%;         /* #ea8600 */
    --chart-4: 270 60% 45%;
    --chart-5: 350 70% 45%;

    /* dark-optimised derivations that must be re-stated, not inherited */
    --bg-tertiary: hsl(210 17% 93%);
    --text-tertiary: hsl(213 5% 53%);
    --accent-glow: hsla(217, 89%, 51%, 0.10);
    --accent-glow-strong: hsla(217, 89%, 51%, 0.24);
    --glass-bg: hsla(0, 0%, 100%, 0.88);
    --glass-bg-hover: hsla(0, 0%, 100%, 0.96);
    --glass-border: hsl(var(--border));
    --glass-blur: blur(12px);
    --shadow-sm: 0 1px 2px hsla(210, 8%, 13%, 0.08);
    --shadow-md: 0 1px 3px hsla(210,8%,13%,0.12), 0 4px 8px hsla(210,8%,13%,0.06);
    --shadow-lg: 0 8px 24px hsla(210, 8%, 13%, 0.12);
    --shadow-xl: 0 16px 48px hsla(210, 8%, 13%, 0.16);
    --shadow-glow: 0 0 0 3px hsla(217, 89%, 51%, 0.15);
}
```

Also required: `<meta name="color-scheme" content="dark light">` so native form controls, scrollbars and
the `::selection` highlight follow the theme.

### 4.2 Toggle, persistence and Chart.js reactivity

Bootstrap **before first paint** (inline in `<head>`) to avoid a flash of the wrong theme:

```html
<script>
  (function () {
    var stored = localStorage.getItem('bq_theme');
    var theme = stored ||
      (window.matchMedia('(prefers-color-scheme: light)').matches ? 'light' : 'dark');
    document.documentElement.dataset.bootTheme = theme;  // body class applied on DOMContentLoaded
  })();
</script>
```

Runtime toggle:

```javascript
const applyTheme = (theme) => {                        // 'light' | 'dark'
  document.body.classList.toggle('light-theme', theme === 'light');
  document.body.classList.toggle('dark-theme',  theme !== 'light');
  localStorage.setItem('bq_theme', theme);

  const css  = getComputedStyle(document.body);
  const tick = css.getPropertyValue('--text-secondary').trim();
  const grid = css.getPropertyValue('--glass-border').trim();

  if (window.Chart) {
    Chart.defaults.color = tick;        // legend labels, tooltips, axis titles
    Chart.defaults.borderColor = grid;  // grid lines
    [state.jobsScatterChart, state.actualProvisioningChart, state.slotsChart]
      .filter(Boolean)
      .forEach(c => c.update('none')); // no animation, no refetch
  }

  const icon = document.querySelector('#theme-toggle i');
  if (icon) icon.className = theme === 'light' ? 'fa-solid fa-sun' : 'fa-solid fa-moon';
};
```

> [!IMPORTANT]
> Use `Chart.getChart()` / the tracked `state.*` handles and `Chart.defaults`. **`Chart.instances` is not
> part of the documented Chart.js v4 API** — the documented surface is `Chart.getChart(key)` plus
> `Chart.defaults.color` / `.backgroundColor` / `.borderColor`
> (chartjs.org/docs/latest/general/colors, /developers/api).

Per-series hardcoded colors (e.g. `borderColor: '#10b981'` at `app.js:3150`) migrate to `--chart-1…5` and
are re-read on toggle.

Toggle button lives in the topbar:

```html
<button id="theme-toggle" class="btn-icon" title="Toggle light / dark theme"
        aria-label="Toggle theme" aria-pressed="false">
  <i class="fa-solid fa-moon"></i>
</button>
```

### 4.3 The sweep — codemod with blast-radius controls (Trap T2)

A token swap alone repaints the chrome and leaves every badge dark-on-white (§2.2), so the sweep is
required. It is executed by `scripts/theme_codemod.py` with safety rails borrowed from our circuit-breaker
patterns:

| Rail | Behavior |
| :--- | :--- |
| **Default mode `report`** | Writes `docs/theme_migration_audit.csv`. **Never writes source.** |
| **`--mode apply --view <id>`** | Rewrites one view's markup/JS block only. One view per commit. |
| **Mapping** | Exact literal match first; otherwise nearest token in OKLab with ΔE ≤ 0.04. Beyond that → `UNMAPPED`, left untouched for a human. |
| **Panic threshold** | Aborts if a single file would exceed **150** replacements without `--force`. |
| **Self-exclusion** | Never rewrites inside the `.dark-theme` / `.light-theme` blocks, `static/report.css`, or asset references. |
| **Exemption list** | `scripts/theme_exempt.txt` — brand colors and any palette that must stay theme-invariant. |
| **Scope guard** | Color declarations only. `margin`, `grid-column`, `opacity` and other layout-only inline styles are left alone. |

**Audit artifact** — `docs/theme_migration_audit.csv`:

| Column | Purpose |
| :--- | :--- |
| `file`, `line` | Exact location |
| `literal` | Original `#hex` / `rgba()` |
| `proposed_token` | Mapped `var(--…)` |
| `confidence` | `exact` \| `near(ΔE)` \| `unmapped` |
| `action` | `applied` \| `skipped-exempt` \| `manual-review` |
| `contrast_after` | Computed ratio against the resolved light background |

This gives reviewers a diffable table instead of a 4,000-line PR.

**Commit order** (each independently revertible): Overview → Slots/Capacity → Storage → Cost Attribution →
HBO → Governance → Linter/Anti-patterns → Settings → Modals/Toasts → Nav/Topbar.

### 4.4 Verification

Enabled from the jobs stubbed in GAP-28.0:

* **`visual`** — Playwright screenshots of every view × {dark, light} × {`static/`, `docs/`}, diffed against committed baselines.
* **`contrast`** — walks computed styles asserting ≥ 4.5:1 (≥ 3.0:1 for ≥24px text), and fails if any `manual-review` row remains in the audit CSV at release-candidate tag time.

### 4.5 Options rejected

| Option | Why not |
| :--- | :--- |
| `:root` + `[data-theme="light"]` | Requires renaming or re-homing all 358 `var()` call sites for no functional gain |
| Token layer only, ship with known gaps | 2.6:1 badges on white — worse than no light mode for the audience that asked |
| CSS `light-dark()` | Cleaner long-term, but rewrites every token declaration and drops support for an explicit override independent of OS preference |

## 5. Implementation Tasks

- [ ] **Prereq:** GAP-28.0 merged; `visual` + `contrast` jobs exist (skipped).
- [ ] Split layer 2 into the shared `.dark-theme, .light-theme` block (no value changes — pure relocation, verify with a dark-mode snapshot diff).
- [ ] Author the `.light-theme` palette; add the `color-scheme` meta tag.
- [ ] Add the pre-paint bootstrap script + `prefers-color-scheme` default.
- [ ] Add `#theme-toggle` to the topbar with `aria-pressed` wiring.
- [ ] Implement `applyTheme()` with the Chart.js v4 API.
- [ ] Migrate the three charts' per-series colors to `--chart-1…5`.
- [ ] Build `scripts/theme_codemod.py` with every rail in §4.3; land `report` mode and the audit CSV **first**, as its own PR.
- [ ] Apply view-by-view (10 commits); resolve `unmapped` rows by hand.
- [ ] Run `scripts/sync_docs_bundle.sh` after each view.
- [ ] Commit Playwright baselines for both themes; enable `visual` + `contrast` in CI.
- [ ] Update `README.md` with a light-mode screenshot; add the `RELEASE_NOTES.md` entry.

## 6. Acceptance Criteria

1. Relocating layer 2 produces **zero** visual diff in dark mode (Playwright baseline unchanged).
2. Toggling to light mode leaves no element using an unresolved custom property (no unstyled regions).
3. Zero `manual-review` rows remain in `docs/theme_migration_audit.csv`.
4. All text/background pairs are ≥ 4.5:1 (≥ 3.0:1 for ≥24px) in **both** themes, asserted by the `contrast` job.
5. Toggling updates all three charts with **zero** network requests (asserted by a Playwright request counter) and no page reload.
6. Theme choice persists across reload; a first visit with no stored preference honors `prefers-color-scheme`; an explicit choice always wins over OS preference.
7. Native form controls, scrollbars and `::selection` follow the active theme.
8. `static/` and `docs/static/` renderings are snapshot-identical.
9. The toggle is keyboard reachable and exposes correct `aria-pressed` state.
10. No hardcoded color literal remains in `static/app.js` outside `scripts/theme_exempt.txt`.

## 7. Risks & Mitigations

| Risk | Mitigation |
| :--- | :--- |
| The sweep regresses dark mode | Dark-mode Playwright baselines are committed **before** the sweep starts; every view commit is diffed against them |
| Codemod makes a bad nearest-token match | ΔE ≤ 0.04 ceiling; anything beyond is `unmapped` and reviewed by a human; audit CSV records every decision |
| A 4,000-line PR is unreviewable | One view per commit + the audit CSV as the reviewer's index |
| Glassmorphism (`backdrop-filter`, translucent surfaces) looks wrong on light | Light palette uses near-opaque `--glass-bg` (0.88) and reduced blur; visually reviewed per view |
| Flash of incorrect theme on load | Pre-paint inline bootstrap in `<head>` (§4.2) |
| Snapshot tests become flaky | Pin browser version, disable animations in the test profile, use a fixed viewport |

## 8. Rollback

Per-view commits revert individually. Full rollback = revert the sweep commits and remove the
`#theme-toggle` button; the shared layer-2 block is behavior-neutral and can stay. Worst case the product
returns to dark-only with a cleaner token structure.

## 9. Open Questions

| # | Question | Default |
| :--- | :--- | :--- |
| 1 | Light-mode the exported report (`static/report.css`)? *(epic Q2)* | Deferred past v1.5.0. |
| 2 | Should light become the default once it's proven? | No — dark stays default; light is opt-in for this release. |
| 3 | Keep glassmorphism in light mode, or go flat like Cloud Console? | Keep, at near-opaque; revisit after the first view lands and can be looked at. |
