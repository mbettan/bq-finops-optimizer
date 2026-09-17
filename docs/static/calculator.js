/**
 * FinOps Optimizer for BigQuery — Runtime Economics Calculator Engine (v1.4.1)
 *
 * UMD pattern: exports BQCalculator to global window in browser,
 * and module.exports in Node.js environments.
 */
(function(root, factory) {
  if (typeof module === 'object' && module.exports) {
    var pricing = require('./pricing.js');
    module.exports = factory(pricing);
  } else {
    root.BQCalculator = factory(root.BQ_PRICING);
  }
}(typeof self !== 'undefined' ? self : this, function(PRICING) {
  'use strict';

  if (!PRICING) {
    throw new Error('BQ_PRICING configuration must be loaded before calculator.js');
  }

  var ORG_TIERS = PRICING.ORG_TIERS || [
    { tier: 'small', projects: 3, label: 'Small', defaultSize: '1vcpu', defaultDuration: 2 },
    { tier: 'medium', projects: 25, label: 'Medium', defaultSize: '1vcpu', defaultDuration: 5 },
    { tier: 'large', projects: 100, label: 'Large', defaultSize: '2vcpu', defaultDuration: 10 },
    { tier: 'x-large', projects: 350, label: 'X-Large', defaultSize: '4vcpu', defaultDuration: 15 }
  ];

  var FREQ_TIERS = PRICING.FREQ_TIERS || [
    { freq: 'monthly', runs: 1, label: 'Monthly (1x)' },
    { freq: 'biweekly', runs: 2, label: 'Bi-Weekly (2x)' },
    { freq: 'weekly', runs: 4, label: 'Weekly (4x)' },
    { freq: 'daily', runs: 30, label: 'Daily (30x)' },
    { freq: 'hourly', runs: 720, label: 'Hourly (720x)' }
  ];

  var AGENT_TIERS = PRICING.AGENT_TIERS || [
    { tier: 'off', queries: 0, label: 'Off (0 - Heuristic Only)' },
    { tier: 'top5', queries: 5, label: 'Top 5 (~5 Queries / sweep)' },
    { tier: 'top25', queries: 25, label: 'Top 25 (~25 Queries / sweep)' },
    { tier: 'top50', queries: 50, label: 'Top 50 (~50 Queries / sweep)' },
    { tier: 'org200', queries: 200, label: 'Org (~200 Queries / sweep)' }
  ];

  function formatCurrency(val) {
    if (typeof val !== 'number' || !isFinite(val)) return '$0.00';
    var sign = val < 0 ? '-' : '';
    var v = Math.abs(val);
    if (v === 0) return '$0.00';
    if (v < 0.01) return sign + '$' + v.toFixed(4);
    return sign + '$' + v.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  }

  function formatPrecise(val, decimals) {
    var d = decimals !== undefined ? decimals : 4;
    if (typeof val !== 'number' || !isFinite(val)) {
      return '$' + (0).toFixed(d);
    }
    var sign = val < 0 ? '-' : '';
    return sign + '$' + Math.abs(val).toFixed(d);
  }

  function formatTokenBudget(totalTokens) {
    if (!isFinite(totalTokens) || totalTokens <= 0) return '0 tokens';
    if (totalTokens >= 1000000) return '~' + (totalTokens / 1000000).toFixed(1) + 'M tokens';
    if (totalTokens >= 100000) return '~' + Math.round(totalTokens / 1000) + 'k tokens';
    if (totalTokens >= 1000) {
      var k = totalTokens / 1000;
      return '~' + (k % 1 === 0 ? k.toFixed(0) : k.toFixed(1)) + 'k tokens';
    }
    return totalTokens + ' tokens';
  }

  function splitPct(parts) {
    var total = parts.reduce(function(a, b) { return a + b; }, 0);
    if (!isFinite(total) || total <= 0) return parts.map(function() { return 0; });
    var raw = parts.map(function(v) { return (v / total) * 100; });
    var fl = raw.map(Math.floor);
    var rem = 100 - fl.reduce(function(a, b) { return a + b; }, 0);
    raw.map(function(v, i) { return [v - fl[i], i]; })
       .sort(function(a, b) { return b[0] - a[0]; })
       .slice(0, rem).forEach(function(p) { fl[p[1]]++; });
    return fl;
  }

  function pctLabel(p, v) {
    if (typeof p !== 'number' || !isFinite(p)) return '0%';
    return (v > 0 && p === 0) ? '<1%' : p + '%';
  }

  function deriveMetrics(projects) {
    var p = (typeof projects === 'number' && isFinite(projects) && projects > 0) ? projects : 1;
    var datasets = Math.round(p * 10);
    var tables = Math.round(p * 200);
    var partitionsK = Math.round(p * 20);
    var partitionsStr = partitionsK >= 1000 ? (partitionsK / 1000).toFixed(1) + 'M' : partitionsK + 'k';
    var queriesDay = Math.round(p * 2000);
    var queriesStr = queriesDay >= 1000000 ? (queriesDay / 1000000).toFixed(1) + 'M' : (queriesDay >= 1000 ? Math.round(queriesDay / 1000) + 'k' : queriesDay);
    var metadataGiB = Math.max(PRICING.assumptions.minMetadataGiB, Math.round(p * PRICING.assumptions.metadataGiBPerProject));
    var scanFeeRun = (metadataGiB / 1024) * PRICING.bigquery.onDemandPerTiB;
    var projectsStr = p >= 350 ? '350+ projects' : p + ' projects';
    return {
      projects: p,
      projectsStr: projectsStr,
      datasets: datasets,
      tablesStr: '~' + tables.toLocaleString() + ' (' + partitionsStr + ' part.)',
      queriesStr: '~' + queriesStr + ' / day',
      metadataGiB: metadataGiB,
      scanFeeRun: scanFeeRun
    };
  }

  function calculate(customState) {
    var s = customState || {};
    var projects = s.projects !== undefined ? s.projects : 25;
    var runs = s.runsPerMonth !== undefined ? s.runsPerMonth : 30;
    var env = s.env || 'local';
    var containerSize = s.containerSize || '1vcpu';
    var durationMin = s.durationMin !== undefined ? s.durationMin : 5;
    var aiQueries = s.aiQueries !== undefined ? s.aiQueries : 0;
    var aiModel = s.aiModel || 'flash';

    var derived = deriveMetrics(projects);

    var bqCostRun = derived.scanFeeRun;
    var bqCostMonth = bqCostRun * runs;

    var runCostRun = 0, runCostMonth = 0;
    if (env === 'cloudrun') {
      var vCPUs = 1, ramGiB = 2;
      if (containerSize === '4vcpu') { vCPUs = 4; ramGiB = 8; }
      else if (containerSize === '2vcpu') { vCPUs = 2; ramGiB = 4; }
      var activeSecRun = durationMin * 60;
      runCostMonth = (vCPUs * activeSecRun * runs * PRICING.cloudRun.service.vcpuSecond) +
                     (ramGiB * activeSecRun * runs * PRICING.cloudRun.service.gibSecond) +
                     (runs * PRICING.cloudRun.service.requestFeePer1M / 1000000);
      runCostRun = runs > 0 ? runCostMonth / runs : 0;
    }

    var model = aiModel === 'flash-lite' ? PRICING.gemini.flashLite : PRICING.gemini.flash;
    var aiCostRun = (aiQueries * PRICING.tokensPerQuery.prompt * model.inputPerToken) +
                    (aiQueries * PRICING.tokensPerQuery.output * model.outputPerToken);
    var aiCostMonth = aiCostRun * runs;

    return {
      runs: runs,
      derived: derived,
      bqCostRun: bqCostRun,
      bqCostMonth: bqCostMonth,
      runCostRun: runCostRun,
      runCostMonth: runCostMonth,
      aiCostRun: aiCostRun,
      aiCostMonth: aiCostMonth,
      totalCostRun: bqCostRun + runCostRun + aiCostRun,
      totalCostMonth: bqCostMonth + runCostMonth + aiCostMonth
    };
  }

  function createCalculatorEngine(doc, win, pricingConfig) {
    var d = doc || (typeof document !== 'undefined' ? document : null);
    var w = win || (typeof window !== 'undefined' ? window : null);
    var cfg = pricingConfig || PRICING;

    var state = {
      env: 'local',
      tierIndex: 1,
      projects: 25,
      freqIndex: 3,
      runsPerMonth: 30,
      durationMin: 5,
      containerSize: '1vcpu',
      aiTierIndex: 0,
      aiQueries: 0,
      aiModel: 'flash'
    };

    var liveTimer = null;
    var aiAnchorNode = null;
    var aiStatusSpan = null;

    function render() {
      if (!d) return;

      var res = calculate(state);
      var isLocal = state.env === 'local';
      var isAIOff = state.aiQueries === 0;

      // Derived Technical Footprint
      var elDatasets = d.getElementById('mk-lbl-calc-datasets');
      if (elDatasets) elDatasets.textContent = '~' + res.derived.datasets.toLocaleString() + ' datasets';
      var elTables = d.getElementById('mk-lbl-calc-tables');
      if (elTables) elTables.textContent = res.derived.tablesStr;
      var elQueries = d.getElementById('mk-lbl-calc-queries');
      if (elQueries) elQueries.textContent = res.derived.queriesStr;
      var elCalcMeta = d.getElementById('mk-lbl-calc-meta');
      if (elCalcMeta) {
        elCalcMeta.textContent = res.derived.metadataGiB >= 1024 ? (res.derived.metadataGiB / 1024).toFixed(2) + ' TiB / run' : res.derived.metadataGiB + ' GiB / run';
      }
      var elCalcScanFee = d.getElementById('mk-lbl-calc-scanfee');
      if (elCalcScanFee) elCalcScanFee.textContent = '~' + formatCurrency(res.derived.scanFeeRun) + ' / run';
      var elProjects = d.getElementById('mk-lbl-calc-projects');
      if (elProjects) elProjects.textContent = res.derived.projectsStr;

      // Execution Mode Label
      var lblEnv = d.getElementById('mk-lbl-cost-env');
      if (lblEnv) lblEnv.textContent = isLocal ? 'Run Locally' : 'Cloud Run';

      // Agent Platform Footprint
      var elAiCalcQ = d.getElementById('mk-lbl-calc-ai-queries');
      if (elAiCalcQ) elAiCalcQ.textContent = isAIOff ? '0 / sweep (Off)' : '~' + state.aiQueries + ' queries / sweep';
      var elAiCalcTokens = d.getElementById('mk-lbl-calc-ai-tokens');
      if (elAiCalcTokens) {
        var totalTokens = state.aiQueries * (cfg.tokensPerQuery.prompt + cfg.tokensPerQuery.output);
        elAiCalcTokens.textContent = formatTokenBudget(totalTokens);
      }
      var elAiCalcRunFee = d.getElementById('mk-lbl-calc-ai-runfee');
      if (elAiCalcRunFee) elAiCalcRunFee.textContent = formatCurrency(res.aiCostRun) + ' / sweep';

      // Top Summary KPI Cards
      var elTotal = d.getElementById('mk-cost-kpi-total');
      if (elTotal) elTotal.textContent = formatCurrency(res.totalCostMonth);
      var elPeriod = d.getElementById('mk-cost-kpi-period');
      if (elPeriod) elPeriod.textContent = 'Estimated spend per month (' + state.runsPerMonth + ' ' + (state.runsPerMonth === 1 ? 'run' : 'runs') + ')';
      var elBQ = d.getElementById('mk-cost-kpi-bq');
      if (elBQ) elBQ.textContent = formatCurrency(res.bqCostMonth);
      var elBQSub = d.getElementById('mk-cost-kpi-bq-sub');
      if (elBQSub) elBQSub.textContent = '$' + cfg.bigquery.onDemandPerTiB.toFixed(2) + '/TiB · ~' + formatCurrency(res.bqCostRun) + ' / run (' + state.runsPerMonth + 'x/mo)';

      var elRun = d.getElementById('mk-cost-kpi-run');
      if (elRun) elRun.textContent = formatCurrency(res.runCostMonth);
      var elRunBadge = d.getElementById('mk-cost-kpi-run-badge');
      if (elRunBadge) elRunBadge.textContent = isLocal ? 'Local ($0)' : 'Scale-to-Zero';
      var elRunSub = d.getElementById('mk-cost-kpi-run-sub');
      if (elRunSub) elRunSub.textContent = isLocal ? 'Self-hosted / $0 compute' : 'Serverless scale-to-zero';

      var elAI = d.getElementById('mk-cost-kpi-ai');
      if (elAI) elAI.textContent = formatCurrency(res.aiCostMonth);
      var elAIBadge = d.getElementById('mk-cost-kpi-ai-badge');
      if (elAIBadge) elAIBadge.textContent = isAIOff ? 'Optional ($0)' : (state.aiModel === 'flash-lite' ? '3.1 Flash-Lite' : '3.5 Flash');
      var elAISub = d.getElementById('mk-cost-kpi-ai-sub');
      if (elAISub) elAISub.textContent = isAIOff ? 'Deterministic Heuristics (0 AI)' : '~' + state.aiQueries + ' queries / sweep (~' + formatCurrency(res.aiCostRun) + '/sweep)';

      // Itemized Table Rows
      var tblRunBQ = d.getElementById('mk-tbl-run-bq');
      if (tblRunBQ) tblRunBQ.textContent = formatCurrency(res.bqCostRun);
      var tblMoBQ = d.getElementById('mk-tbl-mo-bq');
      if (tblMoBQ) tblMoBQ.textContent = formatCurrency(res.bqCostMonth);

      var tblRateRun = d.getElementById('mk-tbl-rate-run');
      if (tblRateRun) tblRateRun.textContent = isLocal ? '$0.00 (Local)' : state.containerSize.toUpperCase().replace('VCPU', ' vCPU');
      var tblRunRun = d.getElementById('mk-tbl-run-run');
      if (tblRunRun) tblRunRun.textContent = formatCurrency(res.runCostRun);
      var tblMoRun = d.getElementById('mk-tbl-mo-run');
      if (tblMoRun) tblMoRun.textContent = formatCurrency(res.runCostMonth);

      var tblRateAI = d.getElementById('mk-tbl-rate-ai');
      if (tblRateAI && aiAnchorNode) {
        var isLite = state.aiModel === 'flash-lite';
        var activeModel = isLite ? cfg.gemini.flashLite : cfg.gemini.flash;
        aiAnchorNode.href = activeModel.source;
        aiAnchorNode.textContent = activeModel.name;
        if (aiStatusSpan) {
          aiStatusSpan.textContent = isAIOff ? ' (Off)' : '';
        }
      }
      var tblRunAI = d.getElementById('mk-tbl-run-ai');
      if (tblRunAI) tblRunAI.textContent = formatCurrency(res.aiCostRun);
      var tblMoAI = d.getElementById('mk-tbl-mo-ai');
      if (tblMoAI) tblMoAI.textContent = formatCurrency(res.aiCostMonth);

      var tblRunTotal = d.getElementById('mk-tbl-run-total');
      if (tblRunTotal) tblRunTotal.textContent = formatCurrency(res.totalCostRun);
      var tblMoTotal = d.getElementById('mk-tbl-mo-total');
      if (tblMoTotal) tblMoTotal.textContent = formatCurrency(res.totalCostMonth);

      // Spend Proportion Bar
      var pcts = splitPct([res.bqCostMonth, res.runCostMonth, res.aiCostMonth]);
      var barBQ = d.getElementById('mk-cost-bar-bq');
      if (barBQ) {
        barBQ.style.width = pcts[0] + '%';
        barBQ.style.minWidth = res.bqCostMonth > 0 ? '3px' : '0px';
      }
      var barRun = d.getElementById('mk-cost-bar-run');
      if (barRun) {
        barRun.style.width = pcts[1] + '%';
        barRun.style.minWidth = res.runCostMonth > 0 ? '3px' : '0px';
      }
      var barAI = d.getElementById('mk-cost-bar-ai');
      if (barAI) {
        barAI.style.width = pcts[2] + '%';
        barAI.style.minWidth = res.aiCostMonth > 0 ? '3px' : '0px';
      }
      var elBarContainer = d.querySelector('.cost-proportion-bar');
      if (elBarContainer) {
        elBarContainer.setAttribute('aria-label', 'BigQuery ' + pctLabel(pcts[0], res.bqCostMonth) + ', Cloud Run ' + pctLabel(pcts[1], res.runCostMonth) + ', Agent Platform ' + pctLabel(pcts[2], res.aiCostMonth));
      }

      var lblPctBQ = d.getElementById('mk-lbl-pct-bq');
      if (lblPctBQ) lblPctBQ.textContent = pctLabel(pcts[0], res.bqCostMonth);
      var lblPctRun = d.getElementById('mk-lbl-pct-run');
      if (lblPctRun) lblPctRun.textContent = pctLabel(pcts[1], res.runCostMonth);
      var lblPctAI = d.getElementById('mk-lbl-pct-ai');
      if (lblPctAI) lblPctAI.textContent = pctLabel(pcts[2], res.aiCostMonth);

      // High Spend (> $500/mo) Dynamic Advisory Banner
      var advisoryBanner = d.getElementById('mk-cost-high-spend-advisory');
      if (advisoryBanner) {
        var monthlyMetadataGiB = res.derived.metadataGiB * state.runsPerMonth;
        var metadataVolStr = monthlyMetadataGiB >= 1024 ? '~' + (monthlyMetadataGiB / 1024).toFixed(1) + ' TiB/month' : '~' + monthlyMetadataGiB + ' GiB/month';
        var volSpan = d.getElementById('mk-cost-advisory-volume');
        if (volSpan) {
          volSpan.textContent = metadataVolStr;
        }
        var isHighSpend = res.totalCostMonth >= 500;
        advisoryBanner.style.display = isHighSpend ? 'block' : 'none';
      }

      // Centralized Debounced Status Announcement
      var liveAnnouncer = d.getElementById('mk-cost-live-announcer');
      if (liveAnnouncer && w) {
        w.clearTimeout(liveTimer);
        liveTimer = w.setTimeout(function() {
          liveAnnouncer.textContent = 'Estimated ' + formatCurrency(res.totalCostMonth) + ' per month: BigQuery ' +
            formatCurrency(res.bqCostMonth) + ', Cloud Run ' + formatCurrency(res.runCostMonth) + ', Agent ' + formatCurrency(res.aiCostMonth) + '.';
        }, 500);
      }
    }

    function updateRadiogroup(containerSelector, activeVal) {
      var pills = d.querySelectorAll(containerSelector + ' .cost-toggle-pill');
      pills.forEach(function(b, i) {
        var val = b.dataset.tierIdx !== undefined ? b.dataset.tierIdx :
                  b.dataset.freqIdx !== undefined ? b.dataset.freqIdx :
                  b.dataset.agentIdx !== undefined ? b.dataset.agentIdx :
                  (b.dataset.env || b.dataset.model || String(i));
        var on = String(val) === String(activeVal);
        b.classList.toggle('active', on);
        b.setAttribute('aria-checked', on ? 'true' : 'false');
        b.setAttribute('tabindex', on ? '0' : '-1');
        b.tabIndex = on ? 0 : -1;
      });
    }

    function applyTierConfig(idx) {
      var t = ORG_TIERS[idx];
      if (!t) return;
      state.tierIndex = idx;
      state.projects = t.projects;
      state.containerSize = t.defaultSize;
      state.durationMin = t.defaultDuration;

      var sliderP = d.getElementById('mk-slider-cost-projects');
      if (sliderP) {
        sliderP.value = idx;
        sliderP.setAttribute('aria-valuenow', idx);
        sliderP.setAttribute('aria-valuetext', t.label);
      }
      var lblP = d.getElementById('mk-lbl-cost-org-profile');
      if (lblP) lblP.textContent = t.label;
      updateRadiogroup('#mk-toggle-org-profile', idx);
      render();
    }

    function applyFreqTierConfig(idx) {
      var cfgItem = FREQ_TIERS[idx];
      if (!cfgItem) return;
      state.freqIndex = idx;
      state.runsPerMonth = cfgItem.runs;

      var sliderFreq = d.getElementById('mk-slider-cost-freq');
      if (sliderFreq) {
        sliderFreq.value = idx;
        sliderFreq.setAttribute('aria-valuenow', idx);
        sliderFreq.setAttribute('aria-valuetext', cfgItem.label);
      }
      var lblFreq = d.getElementById('mk-lbl-cost-freq');
      if (lblFreq) lblFreq.textContent = cfgItem.label;
      updateRadiogroup('#mk-toggle-cost-freq', idx);
      render();
    }

    function applyAgentTierConfig(idx) {
      var t = AGENT_TIERS[idx];
      if (!t) return;
      state.aiTierIndex = idx;
      state.aiQueries = t.queries;

      var sliderAI = d.getElementById('mk-slider-cost-ai-queries');
      if (sliderAI) {
        sliderAI.value = idx;
        sliderAI.setAttribute('aria-valuenow', idx);
        sliderAI.setAttribute('aria-valuetext', t.label);
      }
      var lblAI = d.getElementById('mk-lbl-cost-ai-queries');
      if (lblAI) lblAI.textContent = t.label;
      updateRadiogroup('#mk-toggle-ai-tier', idx);
      render();
    }

    function setupRadiogroupKeyNav(containerSelector, applyFn) {
      var container = d.querySelector(containerSelector);
      if (!container) return;

      container.addEventListener('keydown', function(e) {
        var buttons = Array.from(d.querySelectorAll(containerSelector + ' .cost-toggle-pill'));
        var currentIdx = buttons.findIndex(function(b) { return b.getAttribute('aria-checked') === 'true'; });
        if (currentIdx === -1) currentIdx = 0;

        var newIdx = currentIdx;
        if (e.key === 'ArrowRight' || e.key === 'ArrowDown') {
          if (e.preventDefault) e.preventDefault();
          newIdx = (currentIdx + 1) % buttons.length;
        } else if (e.key === 'ArrowLeft' || e.key === 'ArrowUp') {
          if (e.preventDefault) e.preventDefault();
          newIdx = (currentIdx - 1 + buttons.length) % buttons.length;
        } else if (e.key === 'Home') {
          if (e.preventDefault) e.preventDefault();
          newIdx = 0;
        } else if (e.key === 'End') {
          if (e.preventDefault) e.preventDefault();
          newIdx = buttons.length - 1;
        } else if (e.key === ' ') {
          if (e.preventDefault) e.preventDefault();
          var targetBtn = e.target ? (e.target.closest ? e.target.closest('.cost-toggle-pill') : e.target) : null;
          var targetIdx = buttons.indexOf(targetBtn);
          if (targetIdx !== -1) newIdx = targetIdx;
        } else {
          return;
        }

        applyFn(newIdx);
        if (buttons[newIdx] && buttons[newIdx].focus) buttons[newIdx].focus();
      });
    }

    function init() {
      if (!d) return;

      // Stable anchor element initialization (F12)
      var tblRateAI = d.getElementById('mk-tbl-rate-ai');
      if (tblRateAI) {
        tblRateAI.textContent = '';
        aiAnchorNode = d.createElement('a');
        aiAnchorNode.target = '_blank';
        aiAnchorNode.rel = 'noopener noreferrer';
        aiAnchorNode.className = 'cost-external-link';
        aiStatusSpan = d.createElement('span');
        tblRateAI.appendChild(aiAnchorNode);
        tblRateAI.appendChild(aiStatusSpan);
      }

      // 1. Organization Scale Profile
      d.querySelectorAll('#mk-toggle-org-profile .cost-toggle-pill').forEach(function(p) {
        p.addEventListener('click', function() {
          var idx = parseInt(p.dataset.tierIdx, 10);
          if (!isNaN(idx)) applyTierConfig(idx);
        });
      });
      var sliderP = d.getElementById('mk-slider-cost-projects');
      if (sliderP) {
        sliderP.addEventListener('input', function(e) {
          applyTierConfig(parseInt(e.target.value, 10));
        });
      }
      setupRadiogroupKeyNav('#mk-toggle-org-profile', applyTierConfig);

      // 2. Execution Mode
      d.querySelectorAll('#mk-toggle-cost-env .cost-toggle-pill').forEach(function(p) {
        p.addEventListener('click', function() {
          state.env = p.dataset.env || 'local';
          updateRadiogroup('#mk-toggle-cost-env', state.env);
          render();
        });
      });
      setupRadiogroupKeyNav('#mk-toggle-cost-env', function(idx) {
        var buttons = d.querySelectorAll('#mk-toggle-cost-env .cost-toggle-pill');
        if (buttons[idx]) {
          state.env = buttons[idx].dataset.env || 'local';
          updateRadiogroup('#mk-toggle-cost-env', state.env);
          render();
        }
      });

      // 3. Audit Frequency
      d.querySelectorAll('#mk-toggle-cost-freq .cost-toggle-pill').forEach(function(p) {
        p.addEventListener('click', function() {
          var idx = parseInt(p.dataset.freqIdx, 10);
          if (!isNaN(idx)) applyFreqTierConfig(idx);
        });
      });
      var sliderFreq = d.getElementById('mk-slider-cost-freq');
      if (sliderFreq) {
        sliderFreq.addEventListener('input', function(e) {
          applyFreqTierConfig(parseInt(e.target.value, 10));
        });
      }
      setupRadiogroupKeyNav('#mk-toggle-cost-freq', applyFreqTierConfig);

      // 4. Agent Platform Tier & Model
      d.querySelectorAll('#mk-toggle-ai-tier .cost-toggle-pill').forEach(function(p) {
        p.addEventListener('click', function() {
          var idx = parseInt(p.dataset.agentIdx, 10);
          if (!isNaN(idx)) applyAgentTierConfig(idx);
        });
      });
      var sliderAI = d.getElementById('mk-slider-cost-ai-queries');
      if (sliderAI) {
        sliderAI.addEventListener('input', function(e) {
          applyAgentTierConfig(parseInt(e.target.value, 10));
        });
      }
      setupRadiogroupKeyNav('#mk-toggle-ai-tier', applyAgentTierConfig);

      d.querySelectorAll('#mk-toggle-ai-model .cost-toggle-pill').forEach(function(p) {
        p.addEventListener('click', function() {
          state.aiModel = p.dataset.model || 'flash';
          updateRadiogroup('#mk-toggle-ai-model', state.aiModel);
          render();
        });
      });
      setupRadiogroupKeyNav('#mk-toggle-ai-model', function(idx) {
        var buttons = d.querySelectorAll('#mk-toggle-ai-model .cost-toggle-pill');
        if (buttons[idx]) {
          state.aiModel = buttons[idx].dataset.model || 'flash';
          updateRadiogroup('#mk-toggle-ai-model', state.aiModel);
          render();
        }
      });

      // Hydrate and normalize initial states
      updateRadiogroup('#mk-toggle-cost-env', state.env);
      updateRadiogroup('#mk-toggle-ai-model', state.aiModel);
      applyTierConfig(state.tierIndex);
      applyFreqTierConfig(state.freqIndex);
      applyAgentTierConfig(state.aiTierIndex);
    }

    return {
      state: state,
      init: init,
      render: render,
      applyTierConfig: applyTierConfig,
      applyFreqTierConfig: applyFreqTierConfig,
      applyAgentTierConfig: applyAgentTierConfig
    };
  }

  // Default browser auto-initialization
  var defaultEngine = null;
  if (typeof document !== 'undefined') {
    defaultEngine = createCalculatorEngine();
    if (document.readyState === 'loading') {
      document.addEventListener('DOMContentLoaded', defaultEngine.init);
    } else {
      defaultEngine.init();
    }
  }

  return {
    PRICING: PRICING,
    ORG_TIERS: ORG_TIERS,
    FREQ_TIERS: FREQ_TIERS,
    AGENT_TIERS: AGENT_TIERS,
    formatCurrency: formatCurrency,
    formatPrecise: formatPrecise,
    formatTokenBudget: formatTokenBudget,
    splitPct: splitPct,
    pctLabel: pctLabel,
    deriveMetrics: deriveMetrics,
    calculate: calculate,
    createCalculatorEngine: createCalculatorEngine,
    init: function() {
      if (defaultEngine) defaultEngine.init();
    },
    render: function() {
      if (defaultEngine) defaultEngine.render();
    }
  };
}));
