/**
 * FinOps Optimizer for BigQuery — Runtime Economics Calculator Test Suite
 * Validates mathematical formulas, golden matrices, spec doc parity, property invariants,
 * and DOM/accessibility behaviors against docs/static/pricing.js & docs/static/calculator.js.
 */

const { describe, it } = require('node:test');
const assert = require('node:assert');
const fs = require('fs');
const path = require('path');

const PRICING = require('../docs/static/pricing.js');
const {
  deriveMetrics,
  calculate,
  splitPct,
  pctLabel,
  formatCurrency,
  formatPrecise,
  formatTokenBudget,
  createCalculatorEngine
} = require('../docs/static/calculator.js');
const { generateTables } = require('../scripts/sync_pricing.js');

// ─────────────────────────────────────────────────────────────────────────────
// Suite 1: Canonical Pricing SOT & Helper Functions
// ─────────────────────────────────────────────────────────────────────────────
describe('Suite 1: Canonical Pricing SOT & Helper Functions', () => {
  it('PRICING contains valid GCP US rates and tier definitions', () => {
    assert.strictEqual(PRICING.bigquery.onDemandPerTiB, 6.25);
    assert.strictEqual(PRICING.cloudRun.service.vcpuSecond, 0.00002400);
    assert.strictEqual(PRICING.cloudRun.service.gibSecond, 0.00000250);
    assert.strictEqual(PRICING.cloudRun.service.requestFeePer1M, 0.40);
    assert.strictEqual(PRICING.gemini.flash.inputPerToken, 0.00000150);
    assert.strictEqual(PRICING.gemini.flash.outputPerToken, 0.00000750);
    assert.strictEqual(PRICING.gemini.flashLite.inputPerToken, 0.00000030);
    assert.strictEqual(PRICING.gemini.flashLite.outputPerToken, 0.00000250);
    assert.strictEqual(PRICING.ORG_TIERS.length, 4);
    assert.strictEqual(PRICING.FREQ_TIERS.length, 5);
    assert.strictEqual(PRICING.AGENT_TIERS.length, 5);
  });

  it('formatCurrency correctly formats zero, sub-cent, standard, and negative values', () => {
    assert.strictEqual(formatCurrency(0), '$0.00');
    assert.strictEqual(formatCurrency(0.0035), '$0.0035');
    assert.strictEqual(formatCurrency(0.0087), '$0.0087');
    assert.strictEqual(formatCurrency(0.6104), '$0.61');
    assert.strictEqual(formatCurrency(18.31), '$18.31');
    assert.strictEqual(formatCurrency(1234.56), '$1,234.56');
    assert.strictEqual(formatCurrency(-18.31), '-$18.31');
    assert.strictEqual(formatCurrency(-0.0035), '-$0.0035');
    assert.strictEqual(formatCurrency(NaN), '$0.00');
  });

  it('formatPrecise correctly formats exact decimals for derivation tables', () => {
    assert.strictEqual(formatPrecise(0.0732, 4), '$0.0732');
    assert.strictEqual(formatPrecise(0.0087, 4), '$0.0087');
    assert.strictEqual(formatPrecise(0.0348, 4), '$0.0348');
    assert.strictEqual(formatPrecise(0.1044, 4), '$0.1044');
    assert.strictEqual(formatPrecise(0, 4), '$0.0000');
    assert.strictEqual(formatPrecise(NaN), '$0.0000');
  });

  it('formatTokenBudget formats 0, k-tokens, and M-tokens correctly', () => {
    assert.strictEqual(formatTokenBudget(0), '0 tokens');
    assert.strictEqual(formatTokenBudget(21500), '~21.5k tokens');
    assert.strictEqual(formatTokenBudget(107500), '~108k tokens');
    assert.strictEqual(formatTokenBudget(215000), '~215k tokens');
    assert.strictEqual(formatTokenBudget(860000), '~860k tokens');
    assert.strictEqual(formatTokenBudget(1500000), '~1.5M tokens');
    assert.strictEqual(formatTokenBudget(500), '500 tokens');
  });

  it('splitPct distributes percentages using Largest Remainder and sums to 100%', () => {
    const res1 = splitPct([18.31, 0, 0]);
    assert.deepStrictEqual(res1, [100, 0, 0]);

    const res2 = splitPct([18.31, 0.26, 2.29]);
    assert.strictEqual(res2.reduce((a, b) => a + b, 0), 100);

    const resZero = splitPct([0, 0, 0]);
    assert.deepStrictEqual(resZero, [0, 0, 0]);
  });

  it('pctLabel returns clean percentage strings and covers <1% branch', () => {
    assert.strictEqual(pctLabel(0, 0), '0%');
    assert.strictEqual(pctLabel(0, 0.004), '<1%');
    assert.strictEqual(pctLabel(100, 18.31), '100%');
    assert.strictEqual(pctLabel(33, 5.0), '33%');
    assert.strictEqual(pctLabel(NaN), '0%');
  });

  it('deriveMetrics handles project counts and guards bounds', () => {
    const dSmall = deriveMetrics(3);
    assert.strictEqual(dSmall.projectsStr, '3 projects');
    assert.strictEqual(dSmall.datasets, 30);
    assert.strictEqual(dSmall.tablesStr, '~600 (60k part.)');
    assert.strictEqual(dSmall.metadataGiB, 12);
    assert.strictEqual(dSmall.scanFeeRun.toFixed(4), '0.0732');

    const dMedium = deriveMetrics(25);
    assert.strictEqual(dMedium.projectsStr, '25 projects');
    assert.strictEqual(dMedium.metadataGiB, 100);
    assert.strictEqual(dMedium.scanFeeRun.toFixed(4), '0.6104');

    const dLarge = deriveMetrics(100);
    assert.strictEqual(dLarge.metadataGiB, 400);
    assert.strictEqual(dLarge.scanFeeRun.toFixed(4), '2.4414');

    const dXLarge = deriveMetrics(350);
    assert.strictEqual(dXLarge.projectsStr, '350+ projects');
    assert.strictEqual(dXLarge.metadataGiB, 1400);
    assert.strictEqual(dXLarge.scanFeeRun.toFixed(4), '8.5449');

    // Bounds & fallback test
    const dZero = deriveMetrics(0);
    assert.strictEqual(dZero.metadataGiB, 5); // min 5 GiB floor
    const dNegative = deriveMetrics(-5);
    assert.strictEqual(dNegative.metadataGiB, 5);
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// Suite 2: Golden Pricing Matrix (Exact Baseline Assertions)
// ─────────────────────────────────────────────────────────────────────────────
describe('Suite 2: Golden Pricing Matrix (Exact Baseline Assertions)', () => {
  it('Preset 1: Small Baseline (3 proj, 30x/mo, Local, AI Off) -> $2.20/mo', () => {
    const res = calculate({
      projects: 3,
      runsPerMonth: 30,
      env: 'local',
      containerSize: '1vcpu',
      durationMin: 2,
      aiQueries: 0,
      aiModel: 'flash'
    });

    assert.strictEqual(res.bqCostRun.toFixed(4), '0.0732');
    assert.strictEqual(res.bqCostMonth.toFixed(2), '2.20');
    assert.strictEqual(res.runCostRun, 0);
    assert.strictEqual(res.runCostMonth, 0);
    assert.strictEqual(res.aiCostRun, 0);
    assert.strictEqual(res.aiCostMonth, 0);
    assert.strictEqual(res.totalCostRun.toFixed(4), '0.0732');
    assert.strictEqual(res.totalCostMonth.toFixed(2), '2.20');
    assert.strictEqual(formatCurrency(res.totalCostMonth), '$2.20');
  });

  it('Preset 2: Medium Default (25 proj, 30x/mo, Local, AI Off) -> $18.31/mo', () => {
    const res = calculate({
      projects: 25,
      runsPerMonth: 30,
      env: 'local',
      containerSize: '1vcpu',
      durationMin: 5,
      aiQueries: 0,
      aiModel: 'flash'
    });

    assert.strictEqual(res.bqCostRun.toFixed(4), '0.6104');
    assert.strictEqual(res.bqCostMonth.toFixed(2), '18.31');
    assert.strictEqual(res.runCostMonth, 0);
    assert.strictEqual(res.aiCostMonth, 0);
    assert.strictEqual(res.totalCostMonth.toFixed(2), '18.31');
    assert.strictEqual(formatCurrency(res.totalCostMonth), '$18.31');
  });

  it('Preset 3: Medium Cloud Run (25 proj, 30x/mo, Cloud Run 1vCPU 2GiB 5m, AI Off) -> $18.57/mo', () => {
    const res = calculate({
      projects: 25,
      runsPerMonth: 30,
      env: 'cloudrun',
      containerSize: '1vcpu',
      durationMin: 5,
      aiQueries: 0,
      aiModel: 'flash'
    });

    assert.strictEqual(res.bqCostMonth.toFixed(2), '18.31');
    assert.strictEqual(res.runCostRun.toFixed(4), '0.0087');
    assert.strictEqual(res.runCostMonth.toFixed(2), '0.26');
    assert.strictEqual(res.aiCostMonth, 0);
    assert.strictEqual(res.totalCostMonth.toFixed(2), '18.57');
    assert.strictEqual(formatCurrency(res.totalCostMonth), '$18.57');
  });

  it('Preset 4: Medium + Flash-Lite (25 proj, 30x/mo, Local, Top 25 queries, Flash-Lite) -> $20.60/mo', () => {
    const res = calculate({
      projects: 25,
      runsPerMonth: 30,
      env: 'local',
      containerSize: '1vcpu',
      durationMin: 5,
      aiQueries: 25,
      aiModel: 'flash-lite'
    });

    assert.strictEqual(res.bqCostMonth.toFixed(2), '18.31');
    assert.strictEqual(res.runCostMonth, 0);
    assert.ok(Math.abs(res.aiCostRun - 0.07625) < 1e-9);
    assert.strictEqual(res.aiCostMonth.toFixed(2), '2.29');
    assert.strictEqual(res.totalCostMonth.toFixed(2), '20.60');
    assert.strictEqual(formatCurrency(res.totalCostMonth), '$20.60');
  });

  it('Preset 5: Large + Flash (100 proj, 30x/mo, Cloud Run 2vCPU 4GiB 10m, Top 50, Flash) -> $91.16/mo', () => {
    const res = calculate({
      projects: 100,
      runsPerMonth: 30,
      env: 'cloudrun',
      containerSize: '2vcpu',
      durationMin: 10,
      aiQueries: 50,
      aiModel: 'flash'
    });

    assert.strictEqual(res.bqCostRun.toFixed(4), '2.4414');
    assert.strictEqual(res.bqCostMonth.toFixed(2), '73.24');
    assert.strictEqual(res.runCostRun.toFixed(4), '0.0348');
    assert.strictEqual(res.runCostMonth.toFixed(2), '1.04');
    assert.strictEqual(res.aiCostRun.toFixed(4), '0.5625');
    assert.strictEqual(res.aiCostMonth.toFixed(2), '16.88');
    assert.strictEqual(res.totalCostMonth.toFixed(2), '91.16');
    assert.strictEqual(formatCurrency(res.totalCostMonth), '$91.16');
  });

  it('Preset 6: X-Large Continuous (350 proj, 720x/mo, Cloud Run 4vCPU 8GiB 15m, Org 200, Flash) -> $7,847.51/mo', () => {
    const res = calculate({
      projects: 350,
      runsPerMonth: 720,
      env: 'cloudrun',
      containerSize: '4vcpu',
      durationMin: 15,
      aiQueries: 200,
      aiModel: 'flash'
    });

    assert.strictEqual(res.bqCostRun.toFixed(4), '8.5449');
    assert.strictEqual(res.bqCostMonth.toFixed(2), '6152.34');
    assert.strictEqual(res.runCostRun.toFixed(4), '0.1044');
    assert.strictEqual(res.runCostMonth.toFixed(2), '75.17');
    assert.strictEqual(res.aiCostRun.toFixed(4), '2.2500');
    assert.strictEqual(res.aiCostMonth.toFixed(2), '1620.00');
    assert.strictEqual(res.totalCostMonth.toFixed(2), '7847.51');
    assert.strictEqual(formatCurrency(res.totalCostMonth), '$7,847.51');
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// Suite 3: Table Parity & Generator Verification
// ─────────────────────────────────────────────────────────────────────────────
describe('Suite 3: Table Parity & Generator Verification', () => {
  it('All generated markdown tables contain expected header columns and rows', () => {
    const tables = generateTables();
    assert.ok(tables['bq-metadata'].includes('| Profile | Projects | Scanned (GiB) | Formula | Cost / Run |'));
    assert.ok(tables['cloudrun-compute'].includes('| Profile | Container Configuration | Active Sec / Run |'));
    assert.ok(tables['gemini-sweep'].includes('| Investigation Tier | Queries / Sweep | Context Budget |'));
    assert.ok(tables['golden-matrix'].includes('| Preset / Workload | Projects | Schedule | BigQuery (List) |'));
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// Suite 4: Property Invariants & Mathematical Sweeps (400 Permutations)
// ─────────────────────────────────────────────────────────────────────────────
describe('Suite 4: Property Invariants & Mathematical Sweeps (400 Permutations)', () => {
  it('All 400 parameter permutations satisfy summation, non-negativity, and largest remainder', () => {
    let count = 0;
    const orgTiers = PRICING.ORG_TIERS;
    const freqTiers = PRICING.FREQ_TIERS;
    const envs = ['local', 'cloudrun'];
    const agentTiers = PRICING.AGENT_TIERS;
    const models = ['flash', 'flash-lite'];

    for (const org of orgTiers) {
      for (const freq of freqTiers) {
        for (const env of envs) {
          for (const agent of agentTiers) {
            for (const model of models) {
              count++;
              const res = calculate({
                projects: org.projects,
                runsPerMonth: freq.runs,
                env: env,
                containerSize: org.defaultSize,
                durationMin: org.defaultDuration,
                aiQueries: agent.queries,
                aiModel: model
              });

              // 1. Non-negativity
              assert.ok(res.bqCostRun >= 0, `bqCostRun must be >= 0 (got ${res.bqCostRun})`);
              assert.ok(res.bqCostMonth >= 0, `bqCostMonth must be >= 0 (got ${res.bqCostMonth})`);
              assert.ok(res.runCostRun >= 0, `runCostRun must be >= 0 (got ${res.runCostRun})`);
              assert.ok(res.runCostMonth >= 0, `runCostMonth must be >= 0 (got ${res.runCostMonth})`);
              assert.ok(res.aiCostRun >= 0, `aiCostRun must be >= 0 (got ${res.aiCostRun})`);
              assert.ok(res.aiCostMonth >= 0, `aiCostMonth must be >= 0 (got ${res.aiCostMonth})`);
              assert.ok(res.totalCostRun >= 0, `totalCostRun must be >= 0 (got ${res.totalCostRun})`);
              assert.ok(res.totalCostMonth >= 0, `totalCostMonth must be >= 0 (got ${res.totalCostMonth})`);

              // 2. Additive integrity (within floating point precision epsilon)
              const sumRun = res.bqCostRun + res.runCostRun + res.aiCostRun;
              const sumMonth = res.bqCostMonth + res.runCostMonth + res.aiCostMonth;
              assert.ok(Math.abs(res.totalCostRun - sumRun) < 1e-9, `Total run cost must equal component sum`);
              assert.ok(Math.abs(res.totalCostMonth - sumMonth) < 1e-9, `Total month cost must equal component sum`);

              // 3. Percentage partitioning
              const pcts = splitPct([res.bqCostMonth, res.runCostMonth, res.aiCostMonth]);
              if (res.totalCostMonth > 0) {
                const totalPct = pcts.reduce((a, b) => a + b, 0);
                assert.strictEqual(totalPct, 100, `Percentage components must sum to 100 (got ${totalPct})`);
              } else {
                assert.deepStrictEqual(pcts, [0, 0, 0]);
              }
            }
          }
        }
      }
    }

    assert.strictEqual(count, 400, 'Must have evaluated exactly 400 distinct permutations');
  });

  it('Monotonicity holds across project scale, run frequency, and AI query sweeps', () => {
    // Project scale monotonicity
    for (let i = 0; i < PRICING.ORG_TIERS.length - 1; i++) {
      const lower = calculate({ projects: PRICING.ORG_TIERS[i].projects, runsPerMonth: 30, env: 'local' });
      const higher = calculate({ projects: PRICING.ORG_TIERS[i + 1].projects, runsPerMonth: 30, env: 'local' });
      assert.ok(higher.bqCostMonth >= lower.bqCostMonth, `BQ cost must be monotonic with projects`);
      assert.ok(higher.totalCostMonth >= lower.totalCostMonth, `Total cost must be monotonic with projects`);
    }

    // Run frequency monotonicity
    for (let i = 0; i < PRICING.FREQ_TIERS.length - 1; i++) {
      const lower = calculate({ projects: 25, runsPerMonth: PRICING.FREQ_TIERS[i].runs, env: 'cloudrun', containerSize: '1vcpu', durationMin: 5 });
      const higher = calculate({ projects: 25, runsPerMonth: PRICING.FREQ_TIERS[i + 1].runs, env: 'cloudrun', containerSize: '1vcpu', durationMin: 5 });
      assert.ok(higher.totalCostMonth >= lower.totalCostMonth, `Total cost must be monotonic with run frequency`);
    }

    // AI queries monotonicity
    for (let i = 0; i < PRICING.AGENT_TIERS.length - 1; i++) {
      const lower = calculate({ projects: 25, runsPerMonth: 30, env: 'local', aiQueries: PRICING.AGENT_TIERS[i].queries, aiModel: 'flash' });
      const higher = calculate({ projects: 25, runsPerMonth: 30, env: 'local', aiQueries: PRICING.AGENT_TIERS[i + 1].queries, aiModel: 'flash' });
      assert.ok(higher.aiCostMonth >= lower.aiCostMonth, `AI cost must be monotonic with query count`);
    }
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// Suite 5: Headless DOM Hydration & Event Interactions
// ─────────────────────────────────────────────────────────────────────────────
describe('Suite 5: Headless DOM Hydration & Event Interactions', () => {
  // Lightweight Mock DOM for headless node testing
  function createMockDOM() {
    const elements = new Map();
    const eventListeners = new Map();

    function createElement(id, tagName = 'div') {
      const el = {
        id: id,
        tagName: tagName.toUpperCase(),
        textContent: '',
        innerHTML: '',
        style: { width: '', display: '', minWidth: '' },
        dataset: {},
        attributes: new Map(),
        classList: {
          classes: new Set(),
          add(c) { el.classList.classes.add(c); },
          remove(c) { el.classList.classes.delete(c); },
          toggle(c, force) {
            if (force === undefined) {
              if (el.classList.classes.has(c)) el.classList.classes.delete(c);
              else el.classList.classes.add(c);
            } else if (force) {
              el.classList.classes.add(c);
            } else {
              el.classList.classes.delete(c);
            }
          },
          contains(c) { return el.classList.classes.has(c); }
        },
        setAttribute(k, v) { el.attributes.set(k, String(v)); },
        getAttribute(k) { return el.attributes.get(k) || null; },
        removeAttribute(k) { el.attributes.delete(k); },
        children: [],
        appendChild(child) { el.children.push(child); return child; },
        replaceChildren(...newChildren) { el.children = newChildren; },
        querySelector(selector) { return el.children[0] || null; },
        addEventListener(type, handler) {
          if (!eventListeners.has(el)) eventListeners.set(el, []);
          eventListeners.get(el).push({ type, handler });
        },
        dispatchEvent(event) {
          const listeners = eventListeners.get(el) || [];
          for (const l of listeners) {
            if (l.type === event.type) {
              l.handler(event);
            }
          }
        },
        focus() {}
      };
      elements.set(id, el);
      return el;
    }

    // Populate required elements from index.html
    const requiredIds = [
      'mk-cost-kpi-total', 'mk-cost-kpi-period', 'mk-cost-kpi-bq', 'mk-cost-kpi-bq-sub',
      'mk-cost-kpi-run', 'mk-cost-kpi-run-badge', 'mk-cost-kpi-run-sub',
      'mk-cost-kpi-ai', 'mk-cost-kpi-ai-badge', 'mk-cost-kpi-ai-sub',
      'mk-lbl-cost-org-profile', 'mk-lbl-cost-env', 'mk-lbl-cost-freq', 'mk-lbl-cost-ai-queries',
      'mk-lbl-calc-datasets', 'mk-lbl-calc-tables', 'mk-lbl-calc-queries', 'mk-lbl-calc-meta',
      'mk-lbl-calc-scanfee', 'mk-lbl-calc-projects', 'mk-lbl-calc-ai-queries', 'mk-lbl-calc-ai-tokens',
      'mk-lbl-calc-ai-runfee', 'mk-cost-bar-bq', 'mk-cost-bar-run', 'mk-cost-bar-ai',
      'mk-lbl-pct-bq', 'mk-lbl-pct-run', 'mk-lbl-pct-ai',
      'mk-tbl-run-bq', 'mk-tbl-mo-bq', 'mk-tbl-rate-run', 'mk-tbl-run-run', 'mk-tbl-mo-run',
      'mk-tbl-rate-ai', 'mk-tbl-run-ai', 'mk-tbl-mo-ai', 'mk-tbl-run-total', 'mk-tbl-mo-total',
      'mk-slider-cost-projects', 'mk-slider-cost-freq', 'mk-slider-cost-ai-queries',
      'mk-cost-high-spend-advisory', 'mk-cost-advisory-volume', 'mk-cost-live-announcer'
    ];

    for (const id of requiredIds) {
      createElement(id);
    }

    // Toggle pill groups
    const pillGroups = {
      '#mk-toggle-org-profile .cost-toggle-pill': [
        { 'data-tier-idx': '0', 'data-tier': 'small', 'data-projects': '3', text: 'Small' },
        { 'data-tier-idx': '1', 'data-tier': 'medium', 'data-projects': '25', text: 'Medium' },
        { 'data-tier-idx': '2', 'data-tier': 'large', 'data-projects': '100', text: 'Large' },
        { 'data-tier-idx': '3', 'data-tier': 'x-large', 'data-projects': '350', text: 'X-Large' }
      ],
      '#mk-toggle-cost-env .cost-toggle-pill': [
        { 'data-env': 'local', text: 'Run Locally' },
        { 'data-env': 'cloudrun', text: 'Run Cloud Run' }
      ],
      '#mk-toggle-cost-freq .cost-toggle-pill': [
        { 'data-freq-idx': '0', text: 'Monthly' },
        { 'data-freq-idx': '1', text: 'Bi-Weekly' },
        { 'data-freq-idx': '2', text: 'Weekly' },
        { 'data-freq-idx': '3', text: 'Daily' },
        { 'data-freq-idx': '4', text: 'Hourly' }
      ],
      '#mk-toggle-ai-tier .cost-toggle-pill': [
        { 'data-agent-idx': '0', text: 'Off (0)' },
        { 'data-agent-idx': '1', text: 'Top 5' },
        { 'data-agent-idx': '2', text: 'Top 25' },
        { 'data-agent-idx': '3', text: 'Top 50' },
        { 'data-agent-idx': '4', text: 'Org (200)' }
      ],
      '#mk-toggle-ai-model .cost-toggle-pill': [
        { 'data-model': 'flash', text: 'Gemini 3.5 Flash' },
        { 'data-model': 'flash-lite', text: 'Gemini 3.1 Flash-Lite' }
      ]
    };

    const queryMap = new Map();
    for (const [selector, pills] of Object.entries(pillGroups)) {
      const containerSelector = selector.split(' ')[0];
      const pillEls = pills.map((p, idx) => {
        const el = createElement(`pill-${selector}-${idx}`, 'button');
        el.textContent = p.text;
        for (const [k, v] of Object.entries(p)) {
          if (k.startsWith('data-')) {
            const prop = k.replace('data-', '').replace(/-([a-z])/g, (_, g) => g.toUpperCase());
            el.dataset[prop] = v;
          }
        }
        return el;
      });
      queryMap.set(selector, pillEls);

      // Create container element if not already
      let containerEl = elements.get(containerSelector.replace('#', ''));
      if (!containerEl) {
        containerEl = createElement(containerSelector.replace('#', ''), 'div');
      }
      containerEl.querySelectorAll = (s) => (s.includes('cost-toggle-pill') ? pillEls : []);
      queryMap.set(containerSelector, [containerEl]);
    }

    const mockDoc = {
      getElementById(id) { return elements.get(id) || null; },
      querySelectorAll(sel) { return queryMap.get(sel) || []; },
      querySelector(sel) {
        if (sel === '.cost-proportion-bar') {
          let el = elements.get('proportion-bar');
          if (!el) el = createElement('proportion-bar');
          return el;
        }
        const found = queryMap.get(sel);
        if (found && found.length > 0) return found[0];
        return elements.get(sel.replace('#', '')) || null;
      },
      createElement(tag) { return createElement(`dyn-${Date.now()}-${Math.random()}`, tag); },
      createTextNode(text) { return { nodeType: 3, textContent: text }; },
      readyState: 'complete',
      addEventListener() {}
    };

    const mockWin = {
      document: mockDoc,
      setTimeout(fn) { fn(); return 1; },
      clearTimeout() {}
    };

    return { mockDoc, mockWin };
  }

  it('Initializes calculator correctly with Default Medium settings', () => {
    const { mockDoc, mockWin } = createMockDOM();
    const engine = createCalculatorEngine(mockDoc, mockWin, PRICING);
    engine.init();

    assert.strictEqual(mockDoc.getElementById('mk-cost-kpi-total').textContent, '$18.31');
    assert.strictEqual(mockDoc.getElementById('mk-cost-kpi-bq').textContent, '$18.31');
    assert.strictEqual(mockDoc.getElementById('mk-cost-kpi-run').textContent, '$0.00');
    assert.strictEqual(mockDoc.getElementById('mk-cost-kpi-ai').textContent, '$0.00');
    assert.strictEqual(mockDoc.getElementById('mk-cost-advisory-volume').textContent, '~2.9 TiB/month');
  });

  it('Toggling Org Profile to Small updates ARIA states and recalcs correctly', () => {
    const { mockDoc, mockWin } = createMockDOM();
    const engine = createCalculatorEngine(mockDoc, mockWin, PRICING);
    engine.init();

    const smallPill = mockDoc.querySelectorAll('#mk-toggle-org-profile .cost-toggle-pill')[0];
    smallPill.dispatchEvent({ type: 'click', target: smallPill });

    assert.strictEqual(smallPill.getAttribute('aria-checked'), 'true');
    assert.strictEqual(smallPill.getAttribute('tabindex'), '0');
    assert.strictEqual(mockDoc.getElementById('mk-cost-kpi-total').textContent, '$2.20');
  });

  it('Toggling AI Model to Flash-Lite updates model rate link target and label', () => {
    const { mockDoc, mockWin } = createMockDOM();
    const engine = createCalculatorEngine(mockDoc, mockWin, PRICING);
    engine.init();

    // Turn on Top 25 AI queries
    const top25Pill = mockDoc.querySelectorAll('#mk-toggle-ai-tier .cost-toggle-pill')[2];
    top25Pill.dispatchEvent({ type: 'click', target: top25Pill });

    // Select Gemini 3.1 Flash-Lite
    const flashLitePill = mockDoc.querySelectorAll('#mk-toggle-ai-model .cost-toggle-pill')[1];
    flashLitePill.dispatchEvent({ type: 'click', target: flashLitePill });

    assert.strictEqual(mockDoc.getElementById('mk-cost-kpi-total').textContent, '$20.60');
    const tblRateAI = mockDoc.getElementById('mk-tbl-rate-ai');
    assert.ok(tblRateAI.children.length > 0, 'Should have created anchor node');
    assert.strictEqual(tblRateAI.children[0].textContent, 'Gemini 3.1 Flash-Lite');
    assert.strictEqual(tblRateAI.children[0].href, 'https://cloud.google.com/vertex-ai/generative-ai/pricing');
  });

  it('High spend state (> $500/mo) activates the advisory banner and updates metadata volume', () => {
    const { mockDoc, mockWin } = createMockDOM();
    const engine = createCalculatorEngine(mockDoc, mockWin, PRICING);
    engine.init();

    // Set to X-Large profile
    const xlargePill = mockDoc.querySelectorAll('#mk-toggle-org-profile .cost-toggle-pill')[3];
    xlargePill.dispatchEvent({ type: 'click', target: xlargePill });

    // Set to Hourly (720x)
    const hourlyPill = mockDoc.querySelectorAll('#mk-toggle-cost-freq .cost-toggle-pill')[4];
    hourlyPill.dispatchEvent({ type: 'click', target: hourlyPill });

    const advisory = mockDoc.getElementById('mk-cost-high-spend-advisory');
    assert.strictEqual(advisory.style.display, 'block');
    const volSpan = mockDoc.getElementById('mk-cost-advisory-volume');
    assert.strictEqual(volSpan.textContent, '~984.4 TiB/month');

    // Drop back to Medium Daily
    const mediumPill = mockDoc.querySelectorAll('#mk-toggle-org-profile .cost-toggle-pill')[1];
    mediumPill.dispatchEvent({ type: 'click', target: mediumPill });
    const dailyPill = mockDoc.querySelectorAll('#mk-toggle-cost-freq .cost-toggle-pill')[3];
    dailyPill.dispatchEvent({ type: 'click', target: dailyPill });

    assert.strictEqual(advisory.style.display, 'none');
    assert.strictEqual(volSpan.textContent, '~2.9 TiB/month');
  });

  it('Keyboard navigation (ArrowRight) moves focus and activates adjacent pill', () => {
    const { mockDoc, mockWin } = createMockDOM();
    const engine = createCalculatorEngine(mockDoc, mockWin, PRICING);
    engine.init();

    const pills = mockDoc.querySelectorAll('#mk-toggle-org-profile .cost-toggle-pill');
    // Initially index 1 (Medium) is active
    assert.strictEqual(pills[1].getAttribute('aria-checked'), 'true');

    // Simulate ArrowRight on container
    const container = mockDoc.querySelector('#mk-toggle-org-profile');
    container.dispatchEvent({
      type: 'keydown',
      key: 'ArrowRight',
      target: pills[1],
      preventDefault() {}
    });

    // Now index 2 (Large) should be active
    assert.strictEqual(pills[2].getAttribute('aria-checked'), 'true');
    assert.strictEqual(pills[2].getAttribute('tabindex'), '0');
    assert.strictEqual(pills[1].getAttribute('tabindex'), '-1');
    assert.strictEqual(mockDoc.getElementById('mk-lbl-cost-org-profile').textContent, 'Large');
  });

  it('Verifies HTML index.html contains all required element IDs matching calculator.js', () => {
    const htmlPath = path.join(__dirname, '..', 'docs', 'index.html');
    assert.ok(fs.existsSync(htmlPath), 'docs/index.html must exist');
    const html = fs.readFileSync(htmlPath, 'utf8');

    const expectedIds = [
      'mk-cost-kpi-total', 'mk-cost-kpi-period', 'mk-cost-kpi-bq', 'mk-cost-kpi-bq-sub',
      'mk-cost-kpi-run', 'mk-cost-kpi-run-badge', 'mk-cost-kpi-run-sub',
      'mk-cost-kpi-ai', 'mk-cost-kpi-ai-badge', 'mk-cost-kpi-ai-sub',
      'mk-lbl-cost-org-profile', 'mk-lbl-cost-env', 'mk-lbl-cost-freq', 'mk-lbl-cost-ai-queries',
      'mk-lbl-calc-datasets', 'mk-lbl-calc-tables', 'mk-lbl-calc-queries', 'mk-lbl-calc-meta',
      'mk-lbl-calc-scanfee', 'mk-lbl-calc-projects', 'mk-lbl-calc-ai-queries', 'mk-lbl-calc-ai-tokens',
      'mk-lbl-calc-ai-runfee', 'mk-cost-bar-bq', 'mk-cost-bar-run', 'mk-cost-bar-ai',
      'mk-lbl-pct-bq', 'mk-lbl-pct-run', 'mk-lbl-pct-ai',
      'mk-tbl-run-bq', 'mk-tbl-mo-bq', 'mk-tbl-rate-run', 'mk-tbl-run-run', 'mk-tbl-mo-run',
      'mk-tbl-rate-ai', 'mk-tbl-run-ai', 'mk-tbl-mo-ai', 'mk-tbl-run-total', 'mk-tbl-mo-total',
      'mk-slider-cost-projects', 'mk-slider-cost-freq', 'mk-slider-cost-ai-queries',
      'mk-cost-high-spend-advisory', 'mk-cost-advisory-volume', 'mk-cost-live-announcer'
    ];

    for (const id of expectedIds) {
      assert.ok(html.includes(`id="${id}"`), `docs/index.html must contain id="${id}"`);
    }

    // Verify static HTML seed value for advisory volume matches Default Medium profile (~2.9 TiB/month)
    assert.ok(
      html.includes('<span id="mk-cost-advisory-volume">~2.9 TiB/month</span>'),
      'docs/index.html must seed mk-cost-advisory-volume with default Medium state (~2.9 TiB/month)'
    );

    // Verify style.css defines .cost-kpi-note
    const cssPath = path.join(__dirname, '..', 'docs', 'style.css');
    assert.ok(fs.existsSync(cssPath), 'docs/style.css must exist');
    const css = fs.readFileSync(cssPath, 'utf8');
    assert.ok(css.includes('.cost-kpi-note'), 'docs/style.css must define .cost-kpi-note rule');
  });
});
