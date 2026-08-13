/**
 * FinOps Optimizer for BigQuery — Canonical Pricing Configuration
 * Single Source of Truth for runtime economics calculations.
 *
 * UMD pattern: exports BQ_PRICING to global window in browser,
 * and module.exports in Node.js environments.
 */
(function(root, factory) {
  if (typeof module === 'object' && module.exports) {
    module.exports = factory();
  } else {
    root.BQ_PRICING = factory();
  }
}(typeof self !== 'undefined' ? self : this, function() {
  'use strict';

  var ORG_TIERS = Object.freeze([
    { tier: 'small', projects: 3, label: 'Small', defaultSize: '1vcpu', defaultDuration: 2 },
    { tier: 'medium', projects: 25, label: 'Medium', defaultSize: '1vcpu', defaultDuration: 5 },
    { tier: 'large', projects: 100, label: 'Large', defaultSize: '2vcpu', defaultDuration: 10 },
    { tier: 'x-large', projects: 350, label: 'X-Large', defaultSize: '4vcpu', defaultDuration: 15 }
  ]);

  var FREQ_TIERS = Object.freeze([
    { freq: 'monthly', runs: 1, label: 'Monthly (1x)' },
    { freq: 'biweekly', runs: 2, label: 'Bi-Weekly (2x)' },
    { freq: 'weekly', runs: 4, label: 'Weekly (4x)' },
    { freq: 'daily', runs: 30, label: 'Daily (30x)' },
    { freq: 'hourly', runs: 720, label: 'Hourly (720x)' }
  ]);

  var AGENT_TIERS = Object.freeze([
    { tier: 'off', queries: 0, label: 'Off (0 - Heuristic Only)' },
    { tier: 'top5', queries: 5, label: 'Top 5 (~5 Queries / sweep)' },
    { tier: 'top25', queries: 25, label: 'Top 25 (~25 Queries / sweep)' },
    { tier: 'top50', queries: 50, label: 'Top 50 (~50 Queries / sweep)' },
    { tier: 'org200', queries: 200, label: 'Org (~200 Queries / sweep)' }
  ]);

  return Object.freeze({
    schemaVersion: '1.0.0',
    effectiveDate: '2026-08-12',
    currency: 'USD',
    region: 'US',
    bigquery: Object.freeze({
      pricingModel: 'On-Demand',
      onDemandPerTiB: 6.25,
      source: 'https://cloud.google.com/bigquery/pricing#on_demand_pricing',
      retrieved: '2026-08-12'
    }),
    cloudRun: Object.freeze({
      service: Object.freeze({
        vcpuSecond: 0.00002400,
        gibSecond: 0.00000250,
        requestFeePer1M: 0.40,
        source: 'https://cloud.google.com/run/pricing',
        retrieved: '2026-08-12'
      })
    }),
    gemini: Object.freeze({
      flash: Object.freeze({
        name: 'Gemini 3.6 Flash',
        inputPer1M: 1.50,
        outputPer1M: 7.50,
        inputPerToken: 0.00000150,
        outputPerToken: 0.00000750,
        source: 'https://cloud.google.com/vertex-ai/generative-ai/pricing',
        retrieved: '2026-08-13'
      }),
      flashLite: Object.freeze({
        name: 'Gemini 3.5 Flash-Lite',
        inputPer1M: 0.30,
        outputPer1M: 2.50,
        inputPerToken: 0.00000030,
        outputPerToken: 0.00000250,
        source: 'https://cloud.google.com/vertex-ai/generative-ai/pricing',
        retrieved: '2026-08-13'
      })
    }),
    tokensPerQuery: Object.freeze({
      prompt: 3500,
      output: 800
    }),
    assumptions: Object.freeze({
      metadataGiBPerProject: 4,
      minMetadataGiB: 5
    }),
    ORG_TIERS: ORG_TIERS,
    FREQ_TIERS: FREQ_TIERS,
    AGENT_TIERS: AGENT_TIERS
  });
}));
