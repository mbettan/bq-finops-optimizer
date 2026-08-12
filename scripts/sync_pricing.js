#!/usr/bin/env node
/**
 * FinOps Optimizer for BigQuery — Documentation Table Synchronization Script
 *
 * Generates pricing and mathematical verification tables in docs/PRICING_CALCULATOR_SPEC.md
 * directly from the canonical docs/static/calculator.js engine.
 *
 * Usage:
 *   node scripts/sync_pricing.js          # Updates docs/PRICING_CALCULATOR_SPEC.md in place
 *   node scripts/sync_pricing.js --check  # Verifies markdown parity without modifying files (exits 1 on drift)
 */

const fs = require('fs');
const path = require('path');

const specPath = path.resolve(__dirname, '../docs/PRICING_CALCULATOR_SPEC.md');
const pricing = require('../docs/static/pricing.js');
const calculator = require('../docs/static/calculator.js');

function generateBqTable() {
  const tiers = [
    { name: 'Small (3 Projects)', projects: 3 },
    { name: 'Medium (25 Projects)', projects: 25 },
    { name: 'Large (100 Projects)', projects: 100 },
    { name: 'X-Large (350+ Projects)', projects: 350 }
  ];

  let rows = [];
  rows.push('| Profile | Projects | Scanned (GiB) | Formula | Cost / Run |');
  rows.push('| :--- | :--- | :--- | :--- | :--- |');

  for (const t of tiers) {
    const derived = calculator.deriveMetrics(t.projects);
    const cost = calculator.formatPrecise(derived.scanFeeRun, 4);
    rows.push(`| **${t.name}** | ${t.projects} | \`${derived.metadataGiB} GiB\` | \`(${derived.metadataGiB} / 1024) × $${pricing.bigquery.onDemandPerTiB.toFixed(2)}\` | **\`${cost}\`** |`);
  }

  return rows.join('\n');
}

function generateCloudRunTable() {
  const tiers = [
    { name: 'Small', vcpu: 1, ram: 2, min: 2, projects: 3 },
    { name: 'Medium', vcpu: 1, ram: 2, min: 5, projects: 25 },
    { name: 'Large', vcpu: 2, ram: 4, min: 10, projects: 100 },
    { name: 'X-Large', vcpu: 4, ram: 8, min: 15, projects: 350 }
  ];

  let rows = [];
  rows.push('| Profile | Container Configuration | Active Sec / Run | vCPU Cost | RAM Cost | Invocations | Cost / Run |');
  rows.push('| :--- | :--- | :--- | :--- | :--- | :--- | :--- |');

  for (const t of tiers) {
    const sec = t.min * 60;
    const vcpuCost = t.vcpu * sec * pricing.cloudRun.service.vcpuSecond;
    const ramCost = t.ram * sec * pricing.cloudRun.service.gibSecond;
    const reqCost = pricing.cloudRun.service.requestFeePer1M / 1000000;
    const totalRun = vcpuCost + ramCost + reqCost;
    rows.push(`| **${t.name}** | ${t.vcpu} vCPU, ${t.ram} GiB RAM (${t.min}m) | ${sec}s | $${vcpuCost.toFixed(6)} | $${ramCost.toFixed(6)} | $${reqCost.toFixed(7)} | **\`${calculator.formatPrecise(totalRun, 4)}\`** |`);
  }

  return rows.join('\n');
}

function generateGeminiSweepTable() {
  const queriesList = [
    { name: 'Off (0)', queries: 0 },
    { name: 'Top 5', queries: 5 },
    { name: 'Top 25', queries: 25 },
    { name: 'Top 50', queries: 50 },
    { name: 'Org (200)', queries: 200 }
  ];

  let rows = [];
  rows.push('| Investigation Tier | Queries / Sweep | Context Budget | Gemini 3.5 Flash ($/sweep) | Gemini 3.1 Flash-Lite ($/sweep) | Display (Flash-Lite) |');
  rows.push('| :--- | :--- | :--- | :--- | :--- | :--- |');

  for (const q of queriesList) {
    const flashRes = calculator.calculate({ projects: 25, runsPerMonth: 1, env: 'local', aiQueries: q.queries, aiModel: 'flash' });
    const liteRes = calculator.calculate({ projects: 25, runsPerMonth: 1, env: 'local', aiQueries: q.queries, aiModel: 'flash-lite' });
    const totalTok = q.queries * (pricing.tokensPerQuery.prompt + pricing.tokensPerQuery.output);
    const tokBudget = calculator.formatTokenBudget(totalTok);

    const flashCost = calculator.formatPrecise(flashRes.aiCostRun, 4);
    const liteCost = calculator.formatPrecise(liteRes.aiCostRun, 4);
    const displayLite = calculator.formatCurrency(liteRes.aiCostRun);

    rows.push(`| **${q.name}** | ${q.queries} | \`${tokBudget}\` | **\`${flashCost}\`** (~${calculator.formatCurrency(flashRes.aiCostRun)}) | **\`${liteCost}\`** | **\`${displayLite} / sweep\`** |`);
  }

  return rows.join('\n');
}

function generateGoldenMatrixTable() {
  const presets = [
    { label: 'Small Baseline', projects: 3, runs: 30, env: 'local', aiQ: 0, model: 'flash' },
    { label: 'Medium Default', projects: 25, runs: 30, env: 'local', aiQ: 0, model: 'flash' },
    { label: 'Medium Cloud Run', projects: 25, runs: 30, env: 'cloudrun', size: '1vcpu', dur: 5, aiQ: 0, model: 'flash' },
    { label: 'Medium + Flash-Lite', projects: 25, runs: 30, env: 'local', aiQ: 25, model: 'flash-lite' },
    { label: 'Large + Flash', projects: 100, runs: 30, env: 'cloudrun', size: '2vcpu', dur: 10, aiQ: 50, model: 'flash' },
    { label: 'X-Large Continuous', projects: 350, runs: 720, env: 'cloudrun', size: '4vcpu', dur: 15, aiQ: 200, model: 'flash' }
  ];

  let rows = [];
  rows.push('| Preset / Workload | Projects | Schedule | BigQuery (List) | Cloud Run (Service) | Agent Platform (Model) | Total Monthly Spend |');
  rows.push('| :--- | :--- | :--- | :--- | :--- | :--- | :--- |');

  for (const p of presets) {
    const res = calculator.calculate({
      projects: p.projects,
      runsPerMonth: p.runs,
      env: p.env,
      containerSize: p.size || '1vcpu',
      durationMin: p.dur || 5,
      aiQueries: p.aiQ,
      aiModel: p.model
    });

    const bqStr = `${calculator.formatCurrency(res.bqCostMonth)} (${calculator.formatPrecise(res.bqCostRun, 4)}/run)`;
    const runStr = p.env === 'local' ? `${calculator.formatCurrency(0)} (Local)` : `${calculator.formatCurrency(res.runCostMonth)} (${calculator.formatPrecise(res.runCostRun, 4)}/run)`;
    const aiStr = p.aiQ === 0 ? `Off (${calculator.formatCurrency(0)})` : `${calculator.formatCurrency(res.aiCostMonth)} (${calculator.formatPrecise(res.aiCostRun, 4)}/sweep)`;
    const totalStr = `**\`${calculator.formatCurrency(res.totalCostMonth)}\`**`;

    rows.push(`| **${p.label}** | ${p.projects} | ${p.runs}x/mo | ${bqStr} | ${runStr} | ${aiStr} | ${totalStr} |`);
  }

  return rows.join('\n');
}

function generateTables() {
  return {
    'bq-metadata': generateBqTable(),
    'cloudrun-compute': generateCloudRunTable(),
    'gemini-sweep': generateGeminiSweepTable(),
    'golden-matrix': generateGoldenMatrixTable()
  };
}

function replaceBlock(content, markerName, newBlockContent) {
  const startMarker = `<!-- BEGIN:GENERATED:${markerName} -->`;
  const endMarker = `<!-- END:GENERATED:${markerName} -->`;
  const startIndex = content.indexOf(startMarker);
  const endIndex = content.indexOf(endMarker);

  if (startIndex === -1 || endIndex === -1) {
    throw new Error(`Markers for ${markerName} not found in spec document.`);
  }

  return content.slice(0, startIndex + startMarker.length) + '\n' +
         newBlockContent + '\n' +
         content.slice(endIndex);
}

function main() {
  if (!fs.existsSync(specPath)) {
    console.error(`FATAL: Spec file not found at: ${specPath}`);
    process.exit(1);
  }

  let content = fs.readFileSync(specPath, 'utf8');
  let updatedContent = content;

  const tables = generateTables();
  for (const [markerName, tableContent] of Object.entries(tables)) {
    try {
      updatedContent = replaceBlock(updatedContent, markerName, tableContent);
    } catch (e) {
      console.error(`FATAL: Failed to replace section "${markerName}": ${e.message}`);
      process.exit(1);
    }
  }

  const isCheck = process.argv.includes('--check');

  if (isCheck) {
    if (content !== updatedContent) {
      console.error('Error: Documentation spec tables are out of sync with pricing.js / calculator.js!');
      console.error('Run `node scripts/sync_pricing.js` to regenerate docs/PRICING_CALCULATOR_SPEC.md.');
      process.exit(1);
    } else {
      console.log('✓ Documentation spec tables are 100% in sync with calculator engine.');
      process.exit(0);
    }
  } else {
    fs.writeFileSync(specPath, updatedContent, 'utf8');
    console.log('✓ Successfully synchronized docs/PRICING_CALCULATOR_SPEC.md from calculator engine.');
  }
}

if (require.main === module) {
  main();
}

module.exports = {
  generateBqTable,
  generateCloudRunTable,
  generateGeminiSweepTable,
  generateGoldenMatrixTable,
  generateTables,
  replaceBlock,
  main
};
