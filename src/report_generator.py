"""
Report Generator — One-Click Executive FinOps Report.

This module is a **pure renderer**: it receives all data via POST body
and never imports ``init_bq_client_and_resolve_project`` or makes any
BigQuery API calls.  This keeps it stateless and testable without BQ
client mocks.

Architecture:
  POST /api/report/prepare  → render + cache → { report_id }
  GET  /api/report/manifest → canonical module registry (JS fetches on init)
  GET  /report/pending      → spinner page while POST is in flight
  GET  /report/error        → error page with reason
  GET  /report/view/{id}    → serve cached HTML + CSP header
"""

from __future__ import annotations

import html as html_lib
import json
import logging
import os
import re
import secrets
import tempfile
import threading
import time
from collections import OrderedDict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional

from fastapi import APIRouter, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field

from .constants import (
    EDITIONS_SLOT_HR_RATE,
    ON_DEMAND_USD_PER_TB,
    __version__,
)

logger = logging.getLogger(__name__)

router = APIRouter(tags=["report"])

# ---------------------------------------------------------------------------
# 1.  Canonical Module Registry — single source of truth
# ---------------------------------------------------------------------------
# JS fetches this via GET /api/report/manifest.  Python's ReportAggregator
# also consults it.  There is NO duplicated list in JavaScript.

REPORT_MODULES: List[Dict[str, Any]] = [
    {"keys": ["bq_job_results"],               "endpoint": "/api/jobs/analyze",                "label": "Compute & Pricing",               "section": 3, "billable_note": None},
    {"keys": ["bq_slots_results"],             "endpoint": "/api/slots/analyze",               "label": "Slot Analysis",                   "section": 3, "billable_note": None},
    {"keys": ["bq_slots_simulation_results"],   "endpoint": "/api/slots/simulate",             "label": "Slot Simulation",                 "section": 3, "billable_note": None},
    {"keys": ["bq_slots_tiered"],              "endpoint": "/api/slots/tiered_recommendations","label": "Tiered Slot Recommendations",     "section": 3, "billable_note": None},
    {"keys": ["bq_fluid_simulation_data"],     "endpoint": "/api/slots/fluid_simulation", "label": "Fluid Scaling", "section": 3, "billable_note": None},
    {"keys": ["bq_top_spenders"],              "endpoint": "/api/users/top_spenders",          "label": "Top Spenders",                    "section": 4, "billable_note": None},
    {"keys": ["bq_cost_attribution_results"],   "endpoint": "/api/cost-attribution/calculate", "label": "Cost Attribution",                "section": 4, "billable_note": None},
    {"keys": ["bq_linter_results"],            "endpoint": "/api/antipatterns/linter",          "label": "Anti-Pattern Linter",             "section": 4, "billable_note": None},
    {"keys": ["bq_batch_results"],             "endpoint": "/api/antipatterns/batch_candidates","label": "Batch Candidates",                "section": 4, "billable_note": None},
    {"keys": ["bq_antipatterns_results"],       "endpoint": "/api/antipatterns/dml",            "label": "DML Abuse",                       "section": 4, "billable_note": None},
    {"keys": ["bq_skew_results"],              "endpoint": "/api/antipatterns/skew",            "label": "Skew Analysis",                   "section": 4, "billable_note": None},
    {"keys": ["bq_mv_results"],                "endpoint": "/api/antipatterns/mv",              "label": "MV Candidates",                   "section": 6, "billable_note": None},
    {"keys": ["bq_storage_results"],           "endpoint": "/api/storage/analyze",              "label": "Storage TCO",                     "section": 5, "billable_note": None},
    {"keys": ["bq_static_audit_results"],      "endpoint": "/api/storage/static_audit",         "label": "Static Audit",                    "section": 5, "billable_note": None},
    {"keys": ["bq_hygiene_results"],           "endpoint": "/api/storage/hygiene",              "label": "Table Hygiene",                   "section": 5, "billable_note": None},
    {"keys": ["bq_gov_results"],               "endpoint": "/api/governance/analyze",           "label": "Governance",                      "section": 5, "billable_note": None},
    {"keys": ["bq_active_assist_results"],     "endpoint": "/api/storage/active_assist",        "label": "Active Assist",                   "section": 5, "billable_note": None},
    {"keys": ["bq_performance_results"],       "endpoint": "/api/resource_warnings/analyze",    "label": "Performance Warnings",            "section": 6, "billable_note": None},
    {"keys": ["bq_ai_results"],                "endpoint": "/api/ai/analyze",                   "label": "AI Doctor",                       "section": 4, "billable_note": "slow \u2014 cross-project scan, 15\u201360s"},
    {"keys": ["bq_bi_results"],                "endpoint": "/api/bi/analyze",                   "label": "BI Analyzer",                     "section": 4, "billable_note": None},
    {"keys": ["bq_hbo_results"],                "endpoint": "/api/hbo/analyze",                  "label": "HBO",                             "section": 6, "billable_note": None},
]

# Keys we accept from the POST body — no others are serialised.
REPORT_RELEVANT_KEYS: set[str] = set()
for _m in REPORT_MODULES:
    REPORT_RELEVANT_KEYS.update(_m["keys"])
REPORT_RELEVANT_KEYS.update({"bq_org_project", "bq_region", "bq_admin_project", "bq_focus_projects"})

# Settings keys always included
_SETTINGS_KEYS = {"bq_org_project", "bq_region", "bq_admin_project", "bq_focus_projects"}


# ---------------------------------------------------------------------------
# 2.  Pydantic models
# ---------------------------------------------------------------------------

# Legacy band ordering (kept for backward compat in sorting)
PRIORITY_ORDER = {"Critical": -1, "High": 0, "Medium": 1, "Low": 2, "Info": 3}

# ── Scoring bands (§5.2 of template spec) ─────────────────────────
SCORE_BANDS = [
    (75, "Critical", "▲"),
    (50, "High",     "▲"),
    (25, "Medium",   "●"),
    (10, "Low",      "▼"),
    (0,  "Info",     "○"),
]


def _score_to_band(score: float) -> tuple[str, str]:
    """Return (band_name, glyph) for a numeric score."""
    for threshold, name, glyph in SCORE_BANDS:
        if score >= threshold:
            return name, glyph
    return "Info", "○"


# ── Impact points lookup (savings as % of baseline) ───────────────
_IMPACT_THRESHOLDS = [(0.35, 13), (0.15, 8), (0.05, 5), (0.01, 2), (0.0, 1)]

def _impact_points(savings_pct: float) -> int:
    """Map savings-as-%-of-baseline to impact points."""
    for threshold, pts in _IMPACT_THRESHOLDS:
        if savings_pct >= threshold:
            return pts
    return 1


# ── Effort multiplier lookup ──────────────────────────────────────
_EFFORT_MULTIPLIER = {
    "XS": 1.50, "S": 1.25, "M": 1.00, "L": 0.75, "XL": 0.50,
    # Map legacy Low/Medium/High to new bands
    "Low": 1.25, "Medium": 1.00, "High": 0.75,
}

# ── Confidence multiplier ─────────────────────────────────────────
_CONFIDENCE_MULTIPLIER = {"High": 1.00, "Medium": 0.75, "Low": 0.50}


class Finding(BaseModel):
    finding_id: str
    ref_id: str
    title: str
    priority: str = "Medium"  # Computed from score band
    effort: str = "Medium"
    category: str
    pillar: str = "Query"     # Pricing | Query | Storage | Governance
    lever: str = "Usage"      # Rate | Usage | Waste | Risk
    description: str
    recommendation: str
    official_docs_url: str
    impact_usd_monthly: Optional[float] = None
    source_module: str
    evidence: Optional[str] = None
    affected_objects: Optional[str] = None
    remediation_steps: Optional[str] = None
    # ── Scoring fields ──
    score: float = 0.0
    score_band: str = "Info"
    score_glyph: str = "○"
    impact_points: int = 0
    risk_points: int = 0
    confidence: str = "Medium"
    horizon: str = "M1"       # W1 | M1 | Q1 | Ongoing
    impact_pct_of_baseline: Optional[float] = None
    exceeds_baseline: bool = False
    overlap_group: Optional[str] = None
    depends_on: Optional[str] = None


# ── Pillar & Lever mappings per finding_id ────────────────────────
_FINDING_META: dict[str, dict[str, str]] = {
    "PRICING_MODEL":     {"pillar": "Pricing",    "lever": "Rate",  "horizon": "M1",      "risk": "0", "confidence": "High"},
    "CAPACITY_CEILING":  {"pillar": "Pricing",    "lever": "Risk",  "horizon": "W1",      "risk": "8", "confidence": "High"},
    "MICRO_QUERY":       {"pillar": "Query",      "lever": "Usage", "horizon": "Q1",      "risk": "0", "confidence": "Medium", "overlap_group": "billing_floor"},
    "SELECT_STAR":       {"pillar": "Query",      "lever": "Usage", "horizon": "M1",      "risk": "0", "confidence": "High"},
    "BATCH_ELIGIBLE":    {"pillar": "Query",      "lever": "Usage", "horizon": "M1",      "risk": "0", "confidence": "Medium"},
    "DML_ABUSE":         {"pillar": "Query",      "lever": "Usage", "horizon": "Q1",      "risk": "0", "confidence": "Medium"},
    "SLOT_SKEW":         {"pillar": "Query",      "lever": "Usage", "horizon": "Q1",      "risk": "0", "confidence": "Low"},
    "MV_CANDIDATES":     {"pillar": "Query",      "lever": "Usage", "horizon": "Q1",      "risk": "0", "confidence": "Medium"},
    "UNPARTITIONED":     {"pillar": "Storage",    "lever": "Usage", "horizon": "Q1",      "risk": "0", "confidence": "High"},
    "STALE_TABLES":      {"pillar": "Storage",    "lever": "Waste", "horizon": "M1",      "risk": "0", "confidence": "High"},
    "STORAGE_TCO":       {"pillar": "Storage",    "lever": "Rate",  "horizon": "M1",      "risk": "0", "confidence": "High", "depends_on": "TIMETRAVL_OVERHEAD"},
    "GUARDRAILS":        {"pillar": "Governance", "lever": "Risk",  "horizon": "W1",      "risk": "8", "confidence": "High"},
    "HBO":               {"pillar": "Query",      "lever": "Usage", "horizon": "M1",      "risk": "0", "confidence": "Medium"},
    "PERF_WARNINGS":     {"pillar": "Query",      "lever": "Usage", "horizon": "Q1",      "risk": "0", "confidence": "Low"},
    "MANUAL_BACKUPS":    {"pillar": "Storage",    "lever": "Waste", "horizon": "M1",      "risk": "0", "confidence": "High"},
    "LTS_FORFEITURE":    {"pillar": "Storage",    "lever": "Rate",  "horizon": "Q1",      "risk": "0", "confidence": "Medium", "depends_on": "TIMETRAVL_OVERHEAD"},
    "MIN_BILLING_FLOOR": {"pillar": "Query",      "lever": "Usage", "horizon": "Q1",      "risk": "0", "confidence": "Medium", "overlap_group": "billing_floor"},
    "TIMETRAVL_OVERHEAD":{"pillar": "Storage",    "lever": "Rate",  "horizon": "M1",      "risk": "0", "confidence": "Low"},
}


class ReportPrepareRequest(BaseModel):
    snapshot: dict[str, Any]
    lookback_days: int = Field(default=30, ge=1, le=365)


# ---------------------------------------------------------------------------
# 3.  ReportAggregator — parse and normalise the snapshot
# ---------------------------------------------------------------------------

class ReportAggregator:
    """Parse each ``bq_*`` key into typed Python structures."""

    def __init__(self, snapshot: dict[str, Any]) -> None:
        # Only keep allowlisted keys
        self._raw = {k: v for k, v in snapshot.items() if k in REPORT_RELEVANT_KEYS}
        self.parse_errors: list[str] = []

    def _parse(self, key: str) -> Any:
        """Parse a single key.  Accepts both JSON strings and pre-parsed objects."""
        val = self._raw.get(key)
        if val is None:
            return None
        if isinstance(val, (dict, list)):
            return val
        try:
            return json.loads(val)
        except (json.JSONDecodeError, TypeError) as exc:
            self.parse_errors.append(f"{key}: {exc}")
            return None

    def aggregate(self) -> dict[str, Any]:
        data: dict[str, Any] = {}
        for key in REPORT_RELEVANT_KEYS:
            if key in _SETTINGS_KEYS:
                data[key] = self._raw.get(key)
            else:
                data[key] = self._parse(key)
        data["_parse_errors"] = self.parse_errors
        return data


# ---------------------------------------------------------------------------
# 4.  FindingsSynthesizer — threshold-based findings
# ---------------------------------------------------------------------------

_FINDING_DEFS: list[dict[str, Any]] = [
    {
        "finding_id": "PRICING_MODEL", "ref_id": "ID-01",
        "title": "Pricing Model Optimization",
        "priority": "High", "effort": "Medium", "category": "Compute & Pricing",
        "description": "Pricing model analysis based on realized slot consumption vs on-demand spend.",
        "recommendation": "See break-even analysis in Section 3.",
        "official_docs_url": "https://cloud.google.com/bigquery/docs/editions-intro",
        "source_module": "slots_simulate",
        "keys": ["bq_job_results"],
    },
    {
        "finding_id": "CAPACITY_CEILING", "ref_id": "ID-02",
        "title": "Capacity Ceiling Risk",
        "priority": "High", "effort": "Low", "category": "Compute & Pricing",
        "description": "Suggested autoscale maximum exceeds the performance baseline, indicating capacity risk.",
        "recommendation": "Review tiered slot recommendations and adjust autoscale ceilings.",
        "official_docs_url": "https://cloud.google.com/bigquery/docs/slots-autoscaling-intro",
        "source_module": "slots_tiered",
        "keys": ["bq_slots_tiered"],
    },
    {
        "finding_id": "MICRO_QUERY", "ref_id": "ID-03",
        "title": "Micro-Query Batching",
        "priority": "Medium", "effort": "Medium", "category": "Query Performance",
        "description": "A high rate of very small queries suggests potential for batching or consolidation.",
        "recommendation": "Batch micro-queries using scripting, scheduled queries, or multi-statement transactions.",
        "official_docs_url": "https://cloud.google.com/bigquery/docs/multi-statement-queries",
        "source_module": "top_spenders",
        "keys": ["bq_top_spenders"],
    },
    {
        "finding_id": "SELECT_STAR", "ref_id": "ID-04",
        "title": "SELECT * Anti-Patterns",
        "priority": "Medium", "effort": "Low", "category": "Query Performance",
        "description": "Queries using SELECT * scan all columns. BigQuery\u2019s Capacitor format stores data columnar — SELECT * defeats column pruning and multiplies bytes billed.",
        "recommendation": "Audit view DDL via INFORMATION_SCHEMA.VIEWS WHERE view_definition LIKE '%SELECT *%'; replace with explicit column lists. For ad-hoc preview, use the table preview / tabledata.list API (free).",
        "official_docs_url": "https://cloud.google.com/bigquery/docs/best-practices-costs#avoid_select_",
        "source_module": "linter",
        "keys": ["bq_linter_results"],
    },
    {
        "finding_id": "BATCH_ELIGIBLE", "ref_id": "ID-05",
        "title": "Batch-Eligible Workloads",
        "priority": "Medium", "effort": "Low", "category": "Query Performance",
        "description": "Some workloads are candidates for batch scheduling, reducing peak slot usage.",
        "recommendation": "Schedule non-urgent queries using BigQuery batch priority or Cloud Scheduler.",
        "official_docs_url": "https://cloud.google.com/bigquery/docs/running-queries#batch",
        "source_module": "batch_candidates",
        "keys": ["bq_batch_results"],
    },
    {
        "finding_id": "DML_ABUSE", "ref_id": "ID-06",
        "title": "DML Abuse Patterns",
        "priority": "Medium", "effort": "Medium", "category": "Query Performance",
        "description": "Frequent single-row DML operations detected, which are inefficient in BigQuery.",
        "recommendation": "Batch DML operations using MERGE or streaming inserts.",
        "official_docs_url": "https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax",
        "source_module": "dml",
        "keys": ["bq_antipatterns_results"],
    },
    {
        "finding_id": "SLOT_SKEW", "ref_id": "ID-07",
        "title": "Slot Skew / Hot Spots",
        "priority": "Medium", "effort": "High", "category": "Query Performance",
        "description": "Some queries exhibit significant data skew, causing uneven slot utilization.",
        "recommendation": "Investigate partition pruning, clustering, and query restructuring.",
        "official_docs_url": "https://cloud.google.com/bigquery/docs/clustered-tables",
        "source_module": "skew",
        "keys": ["bq_skew_results"],
    },
    {
        "finding_id": "MV_CANDIDATES", "ref_id": "ID-08",
        "title": "Materialized View Candidates",
        "priority": "Medium", "effort": "Medium", "category": "Query Performance",
        "description": "Repeated aggregation patterns detected that would benefit from materialized views.",
        "recommendation": "Create materialized views for frequently-run aggregation queries.",
        "official_docs_url": "https://cloud.google.com/bigquery/docs/materialized-views-intro",
        "source_module": "mv",
        "keys": ["bq_mv_results"],
    },
    {
        "finding_id": "UNPARTITIONED", "ref_id": "ID-09",
        "title": "Unpartitioned Tables",
        "priority": "Medium", "effort": "Low", "category": "Storage & Lifecycle",
        "description": "Active Assist recommends partitioning for one or more large tables.",
        "recommendation": "Partition tables by ingestion time or a date/timestamp column. Enforce require_partition_filter = true on large tables to prevent full-table scans.",
        "official_docs_url": "https://cloud.google.com/bigquery/docs/partitioned-tables",
        "source_module": "active_assist",
        "keys": ["bq_static_audit_results"],
    },
    {
        "finding_id": "STALE_TABLES", "ref_id": "ID-10",
        "title": "Stale / Orphan Tables",
        "priority": "Low", "effort": "Low", "category": "Storage & Lifecycle",
        "description": "Tables with zero reads in the past 90+ days may be candidates for archival or deletion.",
        "recommendation": "Set defaultTableExpirationMs at dataset level. For compliance data, archive to GCS Coldline/Archive tier.",
        "official_docs_url": "https://cloud.google.com/bigquery/docs/managing-tables#deleting_a_table",
        "source_module": "hygiene",
        "keys": ["bq_hygiene_results"],
    },
    {
        "finding_id": "STORAGE_TCO", "ref_id": "ID-11",
        "title": "Storage TCO Optimization",
        "priority": "Low", "effort": "Low", "category": "Storage & Lifecycle",
        "description": "Logical-to-physical storage ratio exceeds 2\u00d7 on some datasets, indicating potential savings from physical billing.",
        "recommendation": "Switch eligible datasets to physical storage billing model. Note: reduce max_time_travel_hours to 48h first to minimize time-travel charges. There is a 14-day cooldown between billing-model changes.",
        "official_docs_url": "https://cloud.google.com/bigquery/docs/storage_billing_models",
        "source_module": "storage",
        "keys": ["bq_storage_results"],
    },
    {
        "finding_id": "GUARDRAILS", "ref_id": "ID-12",
        "title": "Cost Guardrails Missing",
        "priority": "High", "effort": "Low", "category": "Governance",
        "description": "No custom quotas or max_bytes_billed constraints detected \u2014 a single runaway query can cause unexpected costs.",
        "recommendation": "Configure project-level custom quotas and set max_bytes_billed on critical jobs.",
        "official_docs_url": "https://cloud.google.com/bigquery/docs/custom-quotas",
        "source_module": "governance",
        "keys": ["bq_gov_results"],
    },
    {
        "finding_id": "HBO", "ref_id": "ID-13",
        "title": "HBO Optimization Opportunities",
        "priority": "Medium", "effort": "Medium", "category": "Compute & Pricing",
        "description": "History-based optimization identifies queries that could benefit from optimized execution plans.",
        "recommendation": "Review HBO recommendations and enable for qualifying workloads.",
        "official_docs_url": "https://cloud.google.com/bigquery/docs/history-based-optimizations",
        "source_module": "hbo",
        "keys": ["bq_hbo_results"],
    },
    {
        "finding_id": "PERF_WARNINGS", "ref_id": "ID-14",
        "title": "Performance Resource Warnings",
        "priority": "Low", "effort": "Medium", "category": "Query Performance",
        "description": "Resource warnings indicate queries that spill to disk or exceed memory targets.",
        "recommendation": "Optimise flagged queries to reduce memory pressure and avoid spill-to-disk.",
        "official_docs_url": "https://cloud.google.com/bigquery/docs/best-practices-performance-overview",
        "source_module": "performance_warnings",
        "keys": ["bq_performance_results"],
    },
    # ── New findings from expert review ──────────────────────────────
    {
        "finding_id": "MANUAL_BACKUPS", "ref_id": "ID-15",
        "title": "Manual Backup Tables \u2192 Zero-Copy Snapshots",
        "priority": "Low", "effort": "Low", "category": "Storage & Lifecycle",
        "description": "Tables with naming patterns like _bkp, _backup, _ss, _bkup suggest manual copy-based backups. BigQuery table snapshots use differential pointers at near-zero incremental cost.",
        "recommendation": "Replace manual backup tables with BigQuery table snapshots (bq cp --snapshot). Delete the full-copy backups to reclaim storage.",
        "official_docs_url": "https://cloud.google.com/bigquery/docs/table-snapshots-intro",
        "source_module": "static_audit",
        "keys": ["bq_static_audit_results"],
    },
    {
        "finding_id": "LTS_FORFEITURE", "ref_id": "ID-16",
        "title": "Long-Term Storage Discount Forfeiture",
        "priority": "Medium", "effort": "Medium", "category": "Storage & Lifecycle",
        "description": "Tables with high churn ratios (>0.5) reset the 90-day timer for long-term storage pricing, doubling the rate from $0.01 to $0.02/GiB/mo.",
        "recommendation": "Investigate high-churn tables: are the UPDATE/MERGE operations necessary? Consider append-only patterns with periodic compaction.",
        "official_docs_url": "https://cloud.google.com/bigquery/pricing#storage",
        "source_module": "hygiene",
        "keys": ["bq_hygiene_results"],
    },
    {
        "finding_id": "MIN_BILLING_FLOOR", "ref_id": "ID-17",
        "title": "10 MB Minimum Billing Floor Impact",
        "priority": "Medium", "effort": "Medium", "category": "Query Performance",
        "description": "BigQuery bills a minimum of 10 MB per table referenced per query. High-frequency micro-queries accumulate significant billing-floor waste.",
        "recommendation": "Consolidate micro-queries via multi-statement transactions, MERGE, or batch pipelines.",
        "official_docs_url": "https://cloud.google.com/bigquery/pricing#on_demand_pricing",
        "source_module": "top_spenders",
        "keys": ["bq_top_spenders"],
    },
    {
        "finding_id": "TIMETRAVL_OVERHEAD", "ref_id": "ID-18",
        "title": "Time-Travel & Fail-Safe Overhead on Physical Billing",
        "priority": "Low", "effort": "Low", "category": "Storage & Lifecycle",
        "description": "Physical storage billing charges for time-travel (up to 7d) and fail-safe (7d) bytes. High-churn datasets on physical billing may negate the compression savings.",
        "recommendation": "Reduce max_time_travel_hours to 48h on high-churn datasets before switching to physical billing. Note the 14-day cooldown between billing model changes.",
        "official_docs_url": "https://cloud.google.com/bigquery/docs/storage_billing_models",
        "source_module": "hygiene",
        "keys": ["bq_hygiene_results", "bq_storage_results"],
    },
]


class FindingsSynthesizer:
    """Evaluate threshold-based triggers and return findings list."""

    def __init__(self, data: dict[str, Any]) -> None:
        self._data = data

    def _key_present_and_nonempty(self, key: str) -> bool:
        val = self._data.get(key)
        if val is None:
            return False
        if isinstance(val, (list, dict)) and len(val) == 0:
            return False
        return True

    @staticmethod
    def _dedup_rows(rows: list[dict], key_fields: list[str], metric_field: str | None = None) -> list[dict]:
        """Deduplicate rows by key_fields, keeping the one with the largest metric_field value."""
        sf = FindingsSynthesizer._safe_float
        seen: dict[str, dict] = {}
        for r in rows:
            if not isinstance(r, dict):
                continue
            key = "|".join(str(r.get(k, "")) for k in key_fields)
            if key in seen:
                if metric_field and sf(r.get(metric_field)) > sf(seen[key].get(metric_field)):
                    seen[key] = r
            else:
                seen[key] = r
        return list(seen.values())

    def _check_trigger(self, fdef: dict[str, Any]) -> tuple[bool, Optional[float], dict[str, str]]:
        """Return (triggered, estimated_impact_usd_monthly, enrichment_dict).

        enrichment_dict may contain 'description', 'recommendation', 'evidence',
        'affected_objects', 'remediation_steps' to override finding-def defaults.
        """
        fid = fdef["finding_id"]
        enrichment: dict[str, str] = {}
        sf = FindingsSynthesizer._safe_float

        # Precondition: all source keys must be present and non-empty
        for k in fdef["keys"]:
            if not self._key_present_and_nonempty(k):
                return False, None, enrichment

        # ── ID-01: Pricing Model ─────────────────────────────────────
        if fid == "PRICING_MODEL":
            lookback = self._data.get("_lookback_days", 30) or 30
            monthly_factor = 30.0 / max(lookback, 1)
            window_hours = max(lookback, 1) * 24.0

            # Use unified baseline cost from bq_top_spenders (same as §1 & §3)
            top = self._data.get("bq_top_spenders")
            if isinstance(top, list) and top:
                total_bytes = sum(sf(r.get("total_bytes_billed")) for r in top if isinstance(r, dict))
                window_ondemand_cost = (total_bytes / (1024**4)) * ON_DEMAND_USD_PER_TB
                ondemand_cost = window_ondemand_cost * monthly_factor
            else:
                # Fallback to bq_job_results if top_spenders unavailable
                jobs = self._data.get("bq_job_results")
                if isinstance(jobs, dict):
                    window_ondemand_cost = sum(
                        sf(ps.get("total_on_demand_cost"))
                        for ps in jobs.get("project_summaries", [])
                        if isinstance(ps, dict)
                    )
                    ondemand_cost = window_ondemand_cost * monthly_factor
                else:
                    return False, None, enrichment
            if ondemand_cost <= 0:
                return False, None, enrichment

            # Get editions cost: simulation (monthly at 3yr rate) or jobs (normalized to monthly)
            sim = self._data.get("bq_slots_simulation_results")
            editions_cost = 0.0
            if isinstance(sim, list) and sim:
                best = min(sim, key=lambda r: sf(r.get("total_3yr")))
                editions_cost = sf(best.get("total_3yr"))
            else:
                jobs = self._data.get("bq_job_results")
                if isinstance(jobs, dict):
                    raw_editions = sum(
                        sf(ps.get("total_editions_cost"))
                        for ps in jobs.get("project_summaries", [])
                        if isinstance(ps, dict)
                    )
                    editions_cost = raw_editions * monthly_factor

            # Derive slot metrics from top_spenders (consistent with baseline)
            total_slot_ms = sum(sf(r.get("total_slot_ms")) for r in top if isinstance(r, dict)) if isinstance(top, list) else 0
            avg_slots = (total_slot_ms / 3_600_000) / window_hours if total_slot_ms > 0 else 0
            # Fall back to slots API if near zero
            if avg_slots < 1.0:
                slots_data = self._data.get("bq_slots_results")
                if isinstance(slots_data, dict) and sf(slots_data.get("total_slot_ms")) > 0:
                    avg_slots = (sf(slots_data.get("total_slot_ms")) / 3_600_000) / window_hours
            breakeven_enterprise = ondemand_cost / (0.06 * 730) if ondemand_cost > 0 else 0
            breakeven_standard = ondemand_cost / (0.04 * 730) if ondemand_cost > 0 else 0

            if editions_cost > 0 and editions_cost < ondemand_cost * 0.9:
                # Editions IS cheaper — recommend migration
                savings = ondemand_cost - editions_cost
                enrichment["description"] = (
                    f"On-demand compute costs {_format_usd(ondemand_cost)}/mo. "
                    f"BigQuery Editions at the 3-year commitment rate would cost {_format_usd(editions_cost)}/mo, "
                    f"a {((ondemand_cost - editions_cost) / ondemand_cost * 100):.0f}% reduction. "
                    f"Measured average: ~{avg_slots:,.0f} slots."
                )
                enrichment["recommendation"] = (
                    f"Migrate to BigQuery Editions (Enterprise PAYG). "
                    f"Start with baseline 0, autoscale ceiling based on P95 measured slots. "
                    f"Run 3\u20136 months to establish the floor, then buy 1yr/3yr commitments for the baseline only."
                )
                return True, round(savings, 2), enrichment
            else:
                # On-demand IS cheaper or comparable — recommend staying
                enrichment["description"] = (
                    f"On-demand compute costs {_format_usd(ondemand_cost)}/mo. "
                    f"Editions projection is {_format_usd(editions_cost)}/mo "
                    f"({editions_cost / ondemand_cost:.1f}\u00d7 on-demand). "
                    f"Measured slot consumption: ~{avg_slots:,.0f} average slots, "
                    f"break-even at {breakeven_enterprise:,.0f} slots (Enterprise $0.06/slot-hr)."
                )
                enrichment["recommendation"] = (
                    f"Remain on On-Demand pricing. Editions becomes cheaper only when sustained average "
                    f"slot consumption drops below {breakeven_enterprise:,.0f} slots (Enterprise) or "
                    f"{breakeven_standard:,.0f} slots (Standard). "
                    f"Focus savings on demand-side optimization (see query findings below)."
                )
                enrichment["priority"] = "Medium"
                return True, None, enrichment

        # ── ID-03: Micro-Query ───────────────────────────────────────
        if fid == "MICRO_QUERY":
            top = self._data.get("bq_top_spenders")
            if isinstance(top, list):
                high_freq = sorted(
                    [r for r in top if isinstance(r, dict) and sf(r.get("query_count")) > 50000],
                    key=lambda r: -sf(r.get("query_count"))
                )
                if high_freq:
                    parts = [f"{r.get('user_email', '?')} ({sf(r.get('query_count')):,.0f} queries)" for r in high_freq[:3]]
                    enrichment["evidence"] = f"Top micro-query sources: {'; '.join(parts)}."
                    total_queries = sum(sf(r.get("query_count")) for r in high_freq)
                    floor_tb = total_queries * 10 / 1e6  # 10 MB per query in TB
                    floor_cost = floor_tb * ON_DEMAND_USD_PER_TB
                    enrichment["affected_objects"] = ", ".join(r.get("user_email", "?") for r in high_freq[:5])
                    if floor_cost > 1:
                        enrichment["evidence"] += f" Billing floor waste: ~{floor_tb:,.1f} TB / {_format_usd(floor_cost)}/mo."
                    return True, round(floor_cost, 2) if floor_cost > 1 else None, enrichment
            # Fallback to original logic
            if isinstance(top, list):
                count = sum(sf(r.get("query_count") or r.get("sub_min_query_count") or 0) for r in top if isinstance(r, dict))
            elif isinstance(top, dict):
                count = sf(top.get("sub_min_query_count") or top.get("micro_query_count"))
            else:
                return False, None, enrichment
            lookback = self._data.get("_lookback_days", 30) or 30
            if count / max(lookback, 1) > 15:
                return True, None, enrichment
            return False, None, enrichment

        # ── ID-04: SELECT * ──────────────────────────────────────────
        if fid == "SELECT_STAR":
            linter = self._data.get("bq_linter_results")
            if isinstance(linter, list):
                select_star = [
                    r for r in linter
                    if isinstance(r, dict) and (
                        r.get("abuse_type", "").upper() in ("SELECT_STAR", "SELECT *", "SELECTSTAR")
                        or r.get("pattern", "").upper() in ("SELECT_STAR", "SELECT *", "SELECTSTAR")
                    )
                ]
                if select_star:
                    total_gb = sum(sf(r.get("billed_gb")) for r in select_star)
                    top3 = sorted(select_star, key=lambda r: -sf(r.get("billed_gb")))[:3]
                    parts = []
                    for r in top3:
                        user = r.get("user_email", "?")
                        gb = sf(r.get("billed_gb"))
                        snippet = (r.get("query_snippet") or "")[:80]
                        parts.append(f"{user} ({gb:,.1f} GB): {snippet}")
                    enrichment["evidence"] = f"{len(select_star)} queries, {total_gb:,.0f} GB total. Top offenders: {'; '.join(parts)}"
                    enrichment["affected_objects"] = ", ".join(set(r.get("user_email", "?") for r in select_star[:10]))
                    impact = (total_gb / 1024) * ON_DEMAND_USD_PER_TB  # Convert GB to TiB
                    return True, round(impact, 2) if impact > 1 else None, enrichment
            return False, None, enrichment

        # ── ID-05: Batch Eligible ────────────────────────────────────
        if fid == "BATCH_ELIGIBLE":
            batch = self._data.get("bq_batch_results")
            if isinstance(batch, list) and batch:
                total_slot_hrs = sum(sf(r.get("total_slot_hours")) for r in batch if isinstance(r, dict))
                total_cost = total_slot_hrs * EDITIONS_SLOT_HR_RATE
                top3 = sorted(batch, key=lambda r: -sf(r.get("total_slot_hours")))[:3]
                parts = [f"{r.get('workload_name', '?')} ({sf(r.get('total_slot_hours')):,.0f} slot-hrs)" for r in top3]
                enrichment["evidence"] = f"{len(batch)} batch candidates, {total_slot_hrs:,.0f} total slot-hours (\u2248{_format_usd(total_cost)}/mo at Editions rates). Top: {'; '.join(parts)}"
                # Item 6: Downgrade to Low when impact is trivial (< $100/mo)
                if total_cost < 100:
                    enrichment["priority"] = "Low"
                return True, round(total_cost, 2) if total_cost > 5 else None, enrichment
            return False, None, enrichment

        # ── ID-10: Stale Tables ──────────────────────────────────────
        if fid == "STALE_TABLES":
            hygiene = self._data.get("bq_hygiene_results")
            if isinstance(hygiene, list) and hygiene:
                # FIX #4: Dedup by table_name to prevent fan-out inflation
                hygiene = self._dedup_rows(hygiene, ["dataset", "table_name"], "live_active_physical_gb")
                stale = [r for r in hygiene if isinstance(r, dict) and "stale" in str(r.get("health_status", "")).lower()]
                if stale:
                    total_gb = sum(sf(r.get("live_active_physical_gb")) for r in stale)
                    top3 = sorted(stale, key=lambda r: -sf(r.get("live_active_physical_gb")))[:3]
                    parts = [f"{r.get('table_name', '?')} ({sf(r.get('live_active_physical_gb')):,.0f} GB)" for r in top3]
                    enrichment["evidence"] = f"{len(stale)} stale tables, {total_gb:,.0f} GB total. Top: {'; '.join(parts)}"
                    return True, None, enrichment
                # Not stale? Still trigger if we have hygiene data (generic)
                return True, None, enrichment
            return False, None, enrichment

        # ── ID-11: Storage TCO ───────────────────────────────────────
        if fid == "STORAGE_TCO":
            storage = self._data.get("bq_storage_results")
            if isinstance(storage, dict):
                datasets = storage.get("datasets", [])
                if isinstance(datasets, list) and datasets:
                    total_savings = sum(sf(d.get("monthly_savings")) for d in datasets if isinstance(d, dict))
                    positive = [d for d in datasets if isinstance(d, dict) and sf(d.get("monthly_savings")) > 0]
                    top3 = sorted(positive, key=lambda d: -sf(d.get("monthly_savings")))[:3]
                    parts = [f"{d.get('dataset_name', '?')} ({_format_usd(sf(d.get('monthly_savings')))})" for d in top3]
                    enrichment["evidence"] = f"{len(positive)} datasets with savings potential totalling {_format_usd(total_savings)}/mo. Top: {'; '.join(parts)}"
                    return True, round(total_savings, 2) if total_savings > 0 else None, enrichment
            return False, None, enrichment

        # ── ID-12: Guardrails ────────────────────────────────────────
        if fid == "GUARDRAILS":
            gov = self._data.get("bq_gov_results")
            if isinstance(gov, dict):
                has_quota = gov.get("custom_quota") or gov.get("has_custom_quota")
                has_mbb = gov.get("max_bytes_billed") or gov.get("has_max_bytes_billed")
                has_quota_key = "custom_quota" in gov or "has_custom_quota" in gov
                has_mbb_key = "max_bytes_billed" in gov or "has_max_bytes_billed" in gov
                if has_quota_key and has_mbb_key and not has_quota and not has_mbb:
                    enrichment["evidence"] = "Neither custom quotas nor max_bytes_billed detected. The project is fully exposed to unbounded single-query spend."
                    return True, None, enrichment
                elif has_quota_key and not has_quota:
                    enrichment["evidence"] = "max_bytes_billed is set but custom quotas are not configured."
                    return True, None, enrichment
                elif has_mbb_key and not has_mbb:
                    enrichment["evidence"] = "Custom quotas are set but max_bytes_billed is not enforced on jobs."
                    return True, None, enrichment
            elif isinstance(gov, list):
                has_quota_keys = any("custom_quota" in r or "has_custom_quota" in r for r in gov if isinstance(r, dict))
                if has_quota_keys and not any(r.get("custom_quota") or r.get("has_custom_quota") for r in gov if isinstance(r, dict)):
                    enrichment["evidence"] = "No custom quotas detected across governance checks."
                    return True, None, enrichment
            return False, None, enrichment

        # ── ID-15: Manual Backups ────────────────────────────────────
        if fid == "MANUAL_BACKUPS":
            import re
            static = self._data.get("bq_static_audit_results")
            if isinstance(static, list):
                # FIX #4: Dedup by table_id
                static = self._dedup_rows(static, ["dataset_id", "table_id"], "size_bytes")
                backup_pattern = re.compile(r'(bkp|backup|_bkup_|_ss$|_ss_|_copy_|_old$)', re.IGNORECASE)
                backups = [r for r in static if isinstance(r, dict) and backup_pattern.search(r.get("table_id", ""))]
                if backups:
                    total_bytes = sum(sf(r.get("size_bytes")) for r in backups)
                    top3 = sorted(backups, key=lambda r: -sf(r.get("size_bytes")))[:3]
                    parts = [f"{r.get('table_id', '?')} ({_format_bytes(sf(r.get('size_bytes')))})" for r in top3]
                    enrichment["evidence"] = f"{len(backups)} backup tables detected ({_format_bytes(total_bytes)} total). Examples: {'; '.join(parts)}"
                    enrichment["affected_objects"] = ", ".join(r.get("table_id", "?") for r in backups[:5])
                    # Storage cost: ~$0.02/GB/mo for active
                    impact = total_bytes / (1024**3) * 0.02
                    return True, round(impact, 2) if impact > 1 else None, enrichment
            return False, None, enrichment

        # ── ID-16: LTS Forfeiture ────────────────────────────────────
        if fid == "LTS_FORFEITURE":
            hygiene = self._data.get("bq_hygiene_results")
            if isinstance(hygiene, list):
                # FIX #4: Dedup by table_name to prevent fan-out inflation
                hygiene = self._dedup_rows(hygiene, ["dataset", "table_name"], "live_active_physical_gb")
                high_churn = [r for r in hygiene if isinstance(r, dict) and sf(r.get("churn_ratio")) > 0.5 and sf(r.get("live_active_physical_gb")) > 10]
                if high_churn:
                    total_gb = sum(sf(r.get("live_active_physical_gb")) for r in high_churn)
                    top3 = sorted(high_churn, key=lambda r: -sf(r.get("live_active_physical_gb")))[:3]
                    parts = [f"{r.get('table_name', '?')} ({sf(r.get('live_active_physical_gb')):,.0f} GB, churn {sf(r.get('churn_ratio')):.2f})" for r in top3]
                    enrichment["evidence"] = f"{len(high_churn)} tables with churn >0.5 totalling {total_gb:,.0f} GB. LTS penalty: ~{_format_usd(total_gb * 0.01)}/mo. Top: {'; '.join(parts)}"
                    return True, round(total_gb * 0.01, 2), enrichment
            return False, None, enrichment

        # ── ID-17: Min Billing Floor ─────────────────────────────────
        if fid == "MIN_BILLING_FLOOR":
            top = self._data.get("bq_top_spenders")
            if isinstance(top, list):
                high_freq = [r for r in top if isinstance(r, dict) and sf(r.get("query_count")) > 100000]
                if high_freq:
                    total_queries = sum(sf(r.get("query_count")) for r in high_freq)
                    floor_tb = total_queries * 10 / 1e6
                    floor_cost = floor_tb * ON_DEMAND_USD_PER_TB
                    parts = [f"{r.get('user_email', '?')} ({sf(r.get('query_count')):,.0f} queries)" for r in sorted(high_freq, key=lambda r: -sf(r.get("query_count")))[:3]]
                    enrichment["evidence"] = f"{len(high_freq)} principals with >100K queries. {total_queries:,.0f} queries \u00d7 10 MB floor \u2248 {floor_tb:,.1f} TB of pure billing-floor waste (\u2248{_format_usd(floor_cost)}/mo). Top: {'; '.join(parts)}"
                    return True, round(floor_cost, 2) if floor_cost > 1 else None, enrichment
            return False, None, enrichment

        # ── ID-18: Time-Travel Overhead ──────────────────────────────
        if fid == "TIMETRAVL_OVERHEAD":
            hygiene = self._data.get("bq_hygiene_results")
            storage = self._data.get("bq_storage_results")
            if isinstance(hygiene, list) and isinstance(storage, dict):
                # Find high-churn tables recommended for physical billing
                high_churn_datasets = set()
                for r in hygiene:
                    if isinstance(r, dict) and sf(r.get("churn_ratio")) > 0.5:
                        high_churn_datasets.add(r.get("dataset", ""))
                physical_recs = [d for d in storage.get("datasets", []) if isinstance(d, dict) and d.get("better_on", "").lower() == "physical" and d.get("dataset_name", "") in high_churn_datasets]
                if physical_recs:
                    parts = [f"{d.get('dataset_name', '?')}" for d in physical_recs[:3]]
                    enrichment["evidence"] = f"{len(physical_recs)} datasets recommended for physical billing have high-churn tables. Time-travel/fail-safe bytes may negate savings. Affected: {', '.join(parts)}"
                    return True, None, enrichment
            return False, None, enrichment

        if fid == "CAPACITY_CEILING":
            tiered = self._data.get("bq_slots_tiered")
            if isinstance(tiered, list) and tiered:
                tiered = tiered[0] if isinstance(tiered[0], dict) else {}
            elif not isinstance(tiered, dict):
                tiered = {}
            autoscale_max = sf(tiered.get("suggested_autoscale_max"))
            perf_max = sf(tiered.get("performance_baseline_max"))
            if perf_max > 0 and autoscale_max > perf_max * 1.5:
                return True, None, enrichment
            return False, None, enrichment

        # ── ID-09: Unpartitioned (enriched with static audit) ────────
        if fid == "UNPARTITIONED":
            static = self._data.get("bq_static_audit_results")
            if isinstance(static, list) and static:
                # FIX #4: Dedup by table_id to prevent fan-out inflation
                static = self._dedup_rows(static, ["dataset_id", "table_id"], "size_bytes")
                unpartitioned = [
                    r for r in static
                    if isinstance(r, dict)
                    and not r.get("is_partitioned")
                    and sf(r.get("size_bytes")) > 0
                ]
                if unpartitioned:
                    total_bytes = sum(sf(r.get("size_bytes")) for r in unpartitioned)
                    total_tb = total_bytes / (1024 ** 4)  # TiB
                    top5 = sorted(unpartitioned, key=lambda r: -sf(r.get("size_bytes")))[:5]
                    parts = [f"{r.get('table_id', '?')} ({_format_bytes(sf(r.get('size_bytes')))})" for r in top5]
                    also_unclustered = [r for r in unpartitioned if not r.get("is_clustered")]
                    enrichment["evidence"] = (
                        f"{len(unpartitioned)} unpartitioned tables totalling {_format_bytes(total_bytes)} "
                        f"({len(also_unclustered)} also unclustered). "
                        f"Top: {'; '.join(parts)}"
                    )
                    enrichment["affected_objects"] = ", ".join(r.get("table_id", "?") for r in top5)
                    # Upgrade to HIGH if total > 100 TB
                    if total_tb > 100:
                        enrichment["priority"] = "High"
                    return True, None, enrichment
            # Fallback to Active Assist trigger
            aa = self._data.get("bq_active_assist_results")
            if isinstance(aa, list) and aa:
                return True, None, enrichment
            if isinstance(aa, dict) and aa:
                return True, None, enrichment
            return False, None, enrichment

        # Generic: trigger if result list/dict contains findings
        for k in fdef["keys"]:
            val = self._data.get(k)
            if isinstance(val, list) and len(val) > 0:
                return True, None, enrichment
            if isinstance(val, dict) and len(val) > 0:
                has_content = any(
                    (isinstance(v, (list, dict, set)) and len(v) > 0) or
                    (isinstance(v, (int, float)) and v > 0) or
                    (isinstance(v, str) and len(v.strip()) > 0)
                    for v in val.values()
                )
                if has_content:
                    return True, None, enrichment
        return False, None, enrichment

    @staticmethod
    def _safe_float(v: Any) -> float:
        try:
            return float(v) if v is not None else 0.0
        except (ValueError, TypeError):
            return 0.0

    @staticmethod
    def _as_dict(v: Any) -> dict:
        """Safely coerce a value to a dict. Lists → first dict element. None/other → {}."""
        if isinstance(v, dict):
            return v
        if isinstance(v, list) and v and isinstance(v[0], dict):
            return v[0]
        return {}

    def synthesize(self) -> list[Finding]:
        findings: list[Finding] = []
        all_defs = list(_FINDING_DEFS)  # Copy to avoid mutation

        # ── Compute baselines for impact scoring ──
        sf = FindingsSynthesizer._safe_float
        compute_baseline = 0.0
        storage_baseline = 0.0
        lookback = self._data.get("_lookback_days", 30) or 30
        monthly_factor = 30.0 / max(lookback, 1)
        top = self._data.get("bq_top_spenders")
        if isinstance(top, list):
            window_bytes = sum(
                sf(r.get("total_bytes_billed")) for r in top if isinstance(r, dict)
            ) / (1024**4) * ON_DEMAND_USD_PER_TB
            compute_baseline = window_bytes * monthly_factor
        storage_data = self._data.get("bq_storage_results")
        if isinstance(storage_data, dict):
            for ds in storage_data.get("datasets", []):
                if isinstance(ds, dict):
                    storage_baseline += sf(ds.get("monthly_spending"))

        for fdef in all_defs:
            triggered, impact, enrichment = self._check_trigger(fdef)
            if triggered:
                fid = fdef["finding_id"]
                meta = _FINDING_META.get(fid, {})
                pillar = meta.get("pillar", "Query")
                lever = meta.get("lever", "Usage")
                horizon = meta.get("horizon", "M1")
                risk_pts = int(meta.get("risk", "0"))
                confidence = enrichment.get("confidence_override", meta.get("confidence", "Medium"))

                # ── Impact points ──
                if impact and impact > 0:
                    # Determine baseline for this finding's pillar
                    baseline = compute_baseline if pillar in ("Pricing", "Query") else storage_baseline
                    if baseline > 0:
                        savings_pct = impact / baseline
                    else:
                        savings_pct = 0.01  # Assume small if no baseline
                    imp_pts = _impact_points(savings_pct)
                    impact_pct = savings_pct * 100
                else:
                    imp_pts = 1  # Minimum for any triggered finding
                    impact_pct = None

                # ── Compute score ──
                effort_str = enrichment.get("effort", fdef["effort"])
                effort_mult = _EFFORT_MULTIPLIER.get(effort_str, 1.0)
                conf_mult = _CONFIDENCE_MULTIPLIER.get(confidence, 0.75)
                raw_score = (imp_pts + risk_pts) * conf_mult * effort_mult * 5
                score = min(raw_score, 100.0)

                # ── Override rules ──
                band, glyph = _score_to_band(score)
                # Rule 1: Risk ≥ 8 → minimum HIGH
                if risk_pts >= 8 and PRIORITY_ORDER.get(band, 9) > PRIORITY_ORDER.get("High", 0):
                    band, glyph = "High", "▲"
                    score = max(score, 50.0)

                findings.append(Finding(
                    finding_id=fid,
                    ref_id=fdef["ref_id"],
                    title=fdef["title"],
                    priority=band,
                    effort=effort_str,
                    category=fdef["category"],
                    pillar=pillar,
                    lever=lever,
                    description=enrichment.get("description", fdef["description"]),
                    recommendation=enrichment.get("recommendation", fdef["recommendation"]),
                    official_docs_url=fdef["official_docs_url"],
                    impact_usd_monthly=impact,
                    source_module=fdef["source_module"],
                    evidence=enrichment.get("evidence"),
                    affected_objects=enrichment.get("affected_objects"),
                    remediation_steps=enrichment.get("remediation_steps"),
                    score=round(score, 1),
                    score_band=band,
                    score_glyph=glyph,
                    impact_points=imp_pts,
                    risk_points=risk_pts,
                    confidence=confidence,
                    horizon=horizon,
                    impact_pct_of_baseline=round(impact_pct, 1) if impact_pct else None,
                    exceeds_baseline=bool(impact_pct and impact_pct > 100),
                    overlap_group=meta.get("overlap_group"),
                    depends_on=meta.get("depends_on"),
                ))
        # Sort by score descending (highest priority first)
        findings.sort(key=lambda f: -f.score)
        return findings


# ---------------------------------------------------------------------------
# 5.  HTMLReportRenderer
# ---------------------------------------------------------------------------

MAX_TABLE_ROWS = 20


def _esc(val: Any) -> str:
    """HTML-escape a value for use in text nodes / quoted attributes."""
    return html_lib.escape(str(val)) if val is not None else ""


def _json_safe(obj: Any) -> str:
    """Serialize to JSON safe for embedding inside <script type=application/json>."""
    return (
        json.dumps(obj, default=str)
        .replace("<", "\\u003c")
        .replace(">", "\\u003e")
        .replace("&", "\\u0026")
        .replace("\u2028", "\\u2028")
        .replace("\u2029", "\\u2029")
    )


def _clamp(val: Any, lo: float = 0.0, hi: float = 100.0) -> float:
    """Coerce to float and clamp to [lo, hi]."""
    try:
        return max(lo, min(hi, float(val)))
    except (ValueError, TypeError):
        return lo


def _format_usd(val: Any) -> str:
    try:
        v = float(val)
        if abs(v) >= 1_000_000:
            return f"${v / 1_000_000:,.1f}M"
        if abs(v) >= 1_000:
            return f"${v / 1_000:,.1f}K"
        return f"${v:,.2f}"
    except (ValueError, TypeError):
        return "$\u2014"


def _format_number(val: Any) -> str:
    try:
        return f"{float(val):,.0f}"
    except (ValueError, TypeError):
        return "\u2014"


def _format_bytes(val: Any) -> str:
    """Format bytes to human-readable GB/TB."""
    try:
        b = float(val)
    except (ValueError, TypeError):
        return "\u2014"
    if b >= 1e12:
        return f"{b / 1e12:,.1f} TB"
    if b >= 1e9:
        return f"{b / 1e9:,.1f} GB"
    if b >= 1e6:
        return f"{b / 1e6:,.1f} MB"
    return f"{b:,.0f} B"


def _format_pct(val: Any) -> str:
    """Format a percentage value."""
    try:
        return f"{float(val):.1f}%"
    except (ValueError, TypeError):
        return "\u2014"


def _format_round2(val: Any) -> str:
    """Round a float to 2 decimal places."""
    try:
        return f"{float(val):,.2f}"
    except (ValueError, TypeError):
        return "\u2014"


def _format_round0(val: Any) -> str:
    """Round a float to 0 decimal places (whole number)."""
    try:
        return f"{float(val):,.0f}"
    except (ValueError, TypeError):
        return "\u2014"


def _bq_link(val: Any, *, kind: str = "project", project: str = "", dataset: str = "") -> str:
    """Wrap a value in a clickable link to the BQ Console."""
    name = _esc(str(val)) if val else "\u2014"
    if not name or name == "\u2014":
        return name
    base = "https://console.cloud.google.com/bigquery"
    if kind == "project":
        url = f"{base}?project={_esc(str(val))}"
    elif kind == "dataset":
        url = f"{base}?project={_esc(project)}&d={_esc(str(val))}&page=dataset"
    elif kind == "table":
        url = f"{base}?project={_esc(project)}&d={_esc(dataset)}&t={_esc(str(val))}&page=table"
    else:
        return name
    return f'<a href="{url}" target="_blank" rel="noopener" class="console-link">{name}</a>'


def _reservation_link(val: Any, **kwargs) -> str:
    """Wrap a reservation_id in a clickable link to BQ Console reservations.
    
    reservation_id format: 'admin_project:location.reservation_name'
    e.g. 'demo-admin:us-east4.res-default'
    Also handles simple names like 'prod-reservation' using the row's project_id.
    """
    name = _esc(str(val)) if val else "\u2014"
    if not name or name == "\u2014":
        return name
    raw = str(val)
    row = kwargs.get("row", {})
    # Parse admin_project:location.name
    if ":" in raw:
        admin_project = raw.split(":")[0]
    else:
        # Fallback to the row's project_id for simple reservation names
        admin_project = row.get("project_id", "") if isinstance(row, dict) else ""
    if admin_project:
        url = f"https://console.cloud.google.com/bigquery/admin/reservations?project={_esc(admin_project)}"
        return f'<a href="{url}" target="_blank" rel="noopener" class="console-link">{name}</a>'
    return name


class HTMLReportRenderer:
    """Build the complete self-contained HTML report document."""

    def __init__(
        self,
        data: dict[str, Any],
        findings: list[Finding],
        lookback_days: int,
        *,
        nonce: str = "",
    ) -> None:
        self._data = data
        self._findings = findings
        try:
            self._lookback_days = max(int(lookback_days or 30), 1)
        except (ValueError, TypeError):
            self._lookback_days = 30
        self._nonce = nonce
        self._css = self._load_css()
        self._baseline = self._compute_baseline()

    def _compute_baseline(self) -> dict[str, float]:
        """Compute financial baselines from bq_top_spenders — single source of truth."""
        sf = FindingsSynthesizer._safe_float
        lookback_days = max(self._lookback_days, 1)
        monthly_factor = 30.0 / lookback_days
        window_hours = lookback_days * 24.0

        b: dict[str, float] = {
            "bytes_billed_tib": 0.0,
            "window_on_demand_cost": 0.0,
            "on_demand_cost": 0.0,
            "total_slot_ms": 0.0,
            "avg_slots": 0.0,
            "storage_cost": 0.0,
            "total_compute": 0.0,
            "total_in_scope": 0.0,
        }
        top = self._data.get("bq_top_spenders")
        if isinstance(top, list):
            total_bytes = sum(sf(r.get("total_bytes_billed")) for r in top if isinstance(r, dict))
            b["bytes_billed_tib"] = total_bytes / (1024**4)
            b["window_on_demand_cost"] = b["bytes_billed_tib"] * ON_DEMAND_USD_PER_TB
            b["on_demand_cost"] = b["window_on_demand_cost"] * monthly_factor
            total_slot_ms = sum(sf(r.get("total_slot_ms")) for r in top if isinstance(r, dict))
            b["total_slot_ms"] = total_slot_ms
            b["avg_slots"] = (total_slot_ms / 3_600_000) / window_hours if total_slot_ms > 0 else 0.0

        # FIX #2: If avg_slots is near zero, try the slots API for better data
        slots = self._data.get("bq_slots_results")
        if b["avg_slots"] < 1.0 and isinstance(slots, dict):
            slot_ms = sf(slots.get("total_slot_ms"))
            if slot_ms > 0:
                b["avg_slots"] = (slot_ms / 3_600_000) / window_hours
                b["total_slot_ms"] = slot_ms

        # FIX #3: Storage baseline — include all storage categories
        storage = self._data.get("bq_storage_results")
        if isinstance(storage, dict):
            for ds in storage.get("datasets", []):
                if isinstance(ds, dict):
                    b["storage_cost"] += sf(ds.get("monthly_spending"))
        # Add time-travel overhead from hygiene data
        hygiene = self._data.get("bq_hygiene_results")
        if isinstance(hygiene, list):
            for r in hygiene:
                if isinstance(r, dict):
                    tt_gb = sf(r.get("time_travel_gb"))
                    if tt_gb > 0:
                        b["storage_cost"] += tt_gb * 0.02 / 1024  # $0.02/GB active rate

        b["total_compute"] = b["on_demand_cost"]  # For now; enhanced when reservation data exists
        b["total_in_scope"] = b["total_compute"] + b["storage_cost"]
        return b

    @staticmethod
    def _callout(callout_type: str, content: str) -> str:
        """Render a styled callout box per the template spec taxonomy."""
        glyphs = {
            "verdict": "◆", "reconciliation": "⚖", "assumption": "ƒ",
            "do-not": "⊘", "caveat": "⚠", "temporal": "⏱", "quick-win": "⚡",
        }
        glyph = glyphs.get(callout_type, "•")
        return (
            f'<div class="callout callout-{callout_type}">'
            f'<span class="callout-glyph">{glyph}</span>'
            f'<div class="callout-body">{content}</div>'
            f'</div>'
        )

    @staticmethod
    def _load_css() -> str:
        css_path = Path(__file__).resolve().parent.parent / "static" / "report.css"
        try:
            return css_path.read_text(encoding="utf-8")
        except FileNotFoundError:
            logger.warning("report.css not found — using minimal fallback")
            return "body { font-family: Inter, -apple-system, 'Segoe UI', Roboto, sans-serif; }"

    def render(self) -> str:
        return "\n".join([
            "<!DOCTYPE html>",
            '<html lang="en">',
            self._head(),
            "<body>",
            self._cover(),
            self._toc(),
            self._section_1_executive_summary(),
            self._section_2_methodology(),
            self._section_3_pricing(),
            self._section_4_query(),
            self._section_5_storage(),
            self._section_6_findings(),
            self._section_7_appendix(),
            self._debug_footer(),
            self._toolbar_script(),
            "</body>",
            "</html>",
        ])

    def _head(self) -> str:
        project = _esc(self._data.get('bq_org_project', ''))
        title = f"Report — {project}" if project else "Report"
        # Inline SVG favicon: a ⚡ bolt icon matching the cover logo
        favicon = (
            "data:image/svg+xml,"
            "%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 32 32'%3E"
            "%3Crect width='32' height='32' rx='6' fill='%234285F4'/%3E"
            "%3Ctext x='16' y='24' font-size='22' text-anchor='middle' fill='white'%3E⚡%3C/text%3E"
            "%3C/svg%3E"
        )
        return f"""<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{title}</title>
<link rel="icon" href="{favicon}">
<meta name="description" content="Executive BigQuery cost optimization assessment generated by FinOps Optimizer v{_esc(__version__)}">
<style>{self._css}</style>
</head>"""

    # ── Cover Page ────────────────────────────────────────────────────
    def _cover(self) -> str:
        project = _esc(self._data.get("bq_org_project") or "Not Configured")
        region = _esc(self._data.get("bq_region") or "\u2014")
        now = time.strftime("%Y-%m-%d %H:%M UTC", time.gmtime())
        release_url = f"https://github.com/mbettan/bq-finops-optimizer/releases/tag/v{__version__}"
        return f"""<div class="report-cover">
  <div class="cover-logo">\u26a1</div>
  <h1 class="cover-title">FinOps Optimizer — Assessment Report</h1>
  <div class="cover-meta">
    <div class="meta-item"><span class="meta-label">Project</span><span class="meta-value">{project}</span></div>
    <div class="meta-item"><span class="meta-label">Region</span><span class="meta-value">{region}</span></div>
    <div class="meta-item"><span class="meta-label">Assessment Date</span><span class="meta-value">{now}</span></div>
    <div class="meta-item"><span class="meta-label">Lookback Window</span><span class="meta-value">{self._lookback_days}-Day Analysis</span></div>
    <div class="meta-item"><span class="meta-label">Tool Version</span><span class="meta-value cover-links"><a href="{release_url}" target="_blank" rel="noopener">FinOps Optimizer v{_esc(__version__)}</a></span></div>
    <div class="meta-item"><span class="meta-label">Links</span><span class="meta-value cover-links"><a href="https://mbettan.github.io/bq-finops-optimizer/" target="_blank" rel="noopener">Website</a> · <a href="https://github.com/mbettan/bq-finops-optimizer" target="_blank" rel="noopener">GitHub</a></span></div>
  </div>
  <p class="cover-disclaimer">This document contains identifiable user data (email addresses). Handle accordingly.</p>
</div>"""

    # ── Table of Contents ─────────────────────────────────────────────
    # WARNING: Do NOT add .report-section to this element — breaks print page breaks
    def _toc(self) -> str:
        sections = [
            (1, "Executive Summary",               self._has_data("bq_job_results")),
            (2, "Scope & Approach",                 True),
            (3, "Compute & Pricing Analysis",       self._has_data("bq_slots_simulation_results")),
            (4, "Query Efficiency Analysis",        self._has_data("bq_top_spenders")),
            (5, "Storage & Lifecycle Analysis",     self._has_data("bq_storage_results")),
            (6, "Findings & Action Items",          len(self._findings) > 0),
            (7, "Appendix & Glossary",              True),
        ]
        items = ""
        for num, title, has in sections:
            suffix = "" if has else ' <span class="toc-no-data">\u2014 no data</span>'
            items += f'<li><a href="#section-{num}">{num}. {_esc(title)}{suffix}</a></li>\n'
        return f"""<div class="report-toc">
  <h2>Table of Contents</h2>
  <ol class="toc-list">{items}</ol>
</div>"""

    # ── Section 1: Executive Summary ──────────────────────────────────
    def _section_1_executive_summary(self) -> str:
        sf = FindingsSynthesizer._safe_float
        b = self._baseline
        content = ""

        project = _esc(self._data.get("bq_org_project") or "this project")
        region = _esc(self._data.get("bq_region") or "us")

        # ── §1.1 Verdict callout ──
        pricing_finding = next((f for f in self._findings if f.finding_id == "PRICING_MODEL"), None)
        guardrails = next((f for f in self._findings if f.finding_id == "GUARDRAILS"), None)
        total_savings = sum(f.impact_usd_monthly for f in self._findings if f.impact_usd_monthly)

        # Determine dominant driver
        top_finding = self._findings[0] if self._findings else None
        dominant = f"The dominant cost driver is <strong>{_esc(top_finding.title.lower())}</strong>." if top_finding else ""
        pricing_direction = ""
        if pricing_finding:
            pricing_direction = f" {pricing_finding.description}"
        risk_text = ""
        if guardrails:
            risk_text = (
                f' The highest <em>risk</em> item — independent of cost — is the '
                f'<strong>absence of cost guardrails</strong> '
                f'({_esc(guardrails.ref_id)}: score {guardrails.score:.0f}).'
            )

        verdict_text = (
            f'Project <strong>{project}</strong> (region <code>{region}</code>) '
            f'processed <strong>{b["bytes_billed_tib"]:,.0f} TiB</strong> in {self._lookback_days} days '
            f'at an on-demand compute cost of <strong>{_format_usd(b["on_demand_cost"])}/mo</strong>, '
            f'averaging <strong>{b["avg_slots"]:,.0f} slots</strong>. '
            f'{dominant}'
            f'{pricing_direction}'
            f'{risk_text}'
        )
        if total_savings > 0:
            savings_pct = (total_savings / b["total_in_scope"] * 100) if b["total_in_scope"] > 0 else 0
            verdict_text += (
                f' Quantified savings across all findings total '
                f'<strong>{_format_usd(total_savings)}/mo</strong> '
                f'({savings_pct:.0f}% of in-scope spend, findings may overlap). '
                f'Full quantification: \u00a76.'
            )

        content += self._callout("verdict", verdict_text)

        # ── §1.2 Financial Baseline & Reconciliation (Block D) ──
        # Get editions cost for comparison
        sim = self._data.get("bq_slots_simulation_results")
        editions_cost = 0.0
        if isinstance(sim, list) and sim:
            best_row = min(sim, key=lambda r: sf(r.get("total_3yr")))
            editions_cost = sf(best_row.get("total_3yr"))

        reconciliation_pct = ""
        if b["bytes_billed_tib"] > 0:
            derived_cost = b["bytes_billed_tib"] * ON_DEMAND_USD_PER_TB
            if derived_cost > 0:
                jobs_cost = 0.0
                jobs = self._data.get("bq_job_results")
                if isinstance(jobs, dict):
                    jobs_cost = sum(sf(ps.get("total_on_demand_cost")) for ps in jobs.get("project_summaries", []) if isinstance(ps, dict))
                if jobs_cost > 0:
                    variance = abs(derived_cost - jobs_cost) / derived_cost * 100
                    status = "✅" if variance < 1 else ("⚠️" if variance < 5 else "❌")
                    reconciliation_pct = f" {status} Variance: {variance:.1f}%"

        content += f"""<h3>Financial Baseline & Reconciliation</h3>
<table class="data-table baseline-table">
  <thead><tr><th>Measure</th><th>Value</th><th>Derivation</th></tr></thead>
  <tbody>
    <tr><td>Bytes billed ({self._lookback_days}d)</td><td class="num">{b["bytes_billed_tib"]:,.0f} TiB</td><td><code>SUM(total_bytes_billed)</code> from top_spenders</td></tr>
    <tr><td><strong>On-demand compute cost</strong></td><td class="num"><strong>{_format_usd(b["on_demand_cost"])}/mo</strong></td><td>TiB × ${ON_DEMAND_USD_PER_TB}/TiB{reconciliation_pct}</td></tr>
    <tr><td>Average slots</td><td class="num">{b["avg_slots"]:,.0f}</td><td><code>SUM(total_slot_ms)/3.6e6 ÷ 730h</code></td></tr>
    <tr><td>Editions cost (3yr rate)</td><td class="num">{_format_usd(editions_cost)}/mo</td><td>Slot simulation at ${EDITIONS_SLOT_HR_RATE}/slot-hr</td></tr>
    <tr><td>Storage cost</td><td class="num">{_format_usd(b["storage_cost"])}/mo</td><td>Active + long-term, by billing model</td></tr>
    <tr><td><strong>Total in-scope spend</strong></td><td class="num"><strong>{_format_usd(b["total_in_scope"])}/mo</strong></td><td>Compute + storage</td></tr>
  </tbody>
</table>"""

        content += self._callout("reconciliation",
            f'On-demand cost derives from <code>SUM(total_bytes_billed)</code> × ${ON_DEMAND_USD_PER_TB}/TiB. '
            f'Every savings figure in this report is expressed as a percentage of one of these baselines.'
        )

        # ── §1.3 KPI tiles ──
        content += f"""<div class="summary-grid">
  <div class="summary-card"><div class="summary-value">{_format_usd(b["on_demand_cost"])}</div><div class="summary-label">On-Demand Cost/mo</div></div>
  <div class="summary-card"><div class="summary-value">{_format_usd(editions_cost)}</div><div class="summary-label">Editions Cost/mo (3yr)</div></div>
  <div class="summary-card"><div class="summary-value">{_format_usd(total_savings)}</div><div class="summary-label">Modelled Savings/mo (may overlap)</div></div>
  <div class="summary-card"><div class="summary-value">{len(self._findings)}</div><div class="summary-label">Findings Triggered</div></div>
</div>"""

        # ── §1.3 Top Priorities (score-sorted, already sorted) ──
        top3 = self._findings[:3]
        if top3:
            li_parts = []
            for f in top3:
                impact_suffix = ""
                if f.impact_usd_monthly:
                    pct_text = f" ({f.impact_pct_of_baseline:.0f}% of {'compute' if f.pillar in ('Pricing', 'Query') else 'storage'})" if f.impact_pct_of_baseline else ""
                    impact_suffix = f" \u2014 {_format_usd(f.impact_usd_monthly)}/mo{pct_text}"
                li_parts.append(
                    f'<li><span class="priority-badge priority-{f.priority.lower()}">{f.score_glyph} {_esc(f.priority)}</span> '
                    f'<span class="score-badge">{f.score:.0f}</span> '
                    f'<strong>{_esc(f.ref_id)}</strong>: {_esc(f.title)}{impact_suffix}</li>'
                )
            items = "".join(li_parts)
            content += f'<div class="top-priorities"><h3>Priority Actions</h3><ol>{items}</ol></div>'

        # ── §1.4 Temporal Relevance ──
        content += self._callout("temporal",
            f'This assessment reflects {self._lookback_days} days of <code>INFORMATION_SCHEMA.JOBS</code> data. '
            f'Pricing rates are as of the assessment date and vary by region. '
            f'Re-measure at 30/60/90 day intervals to track progress.'
        )

        content += '<p class="savings-disclaimer">Estimates are non-binding modelling based on historical usage. Savings de-overlapped by lever — see §6.</p>'
        return self._wrap_section(1, "Executive Summary", content)

    # ── Section 2: Scope & Approach ────────────────────────────────────────
    def _section_2_methodology(self) -> str:
        region = _esc(self._data.get("bq_region") or "\u2014")
        project = _esc(self._data.get("bq_org_project") or "\u2014")
        admin = _esc(self._data.get("bq_admin_project") or project)

        content = f"""\
<div class="methodology">
<p class="scope-intro">This assessment evaluates BigQuery spend, architecture, and operational risk across
four core FinOps dimensions using {self._lookback_days}-day execution telemetry from
<code>INFORMATION_SCHEMA</code> and Cloud Billing.</p>

<h3>Analysis Dimensions</h3>
<table class="data-table">
  <thead><tr>
    <th>Dimension</th>
    <th>What We Evaluate</th>
    <th>Primary Telemetry &amp; Signals</th>
    <th>Key Cost &amp; FinOps Levers</th>
  </tr></thead>
  <tbody>
    <tr>
      <td><strong>Compute Model &amp; Capacity Sizing</strong></td>
      <td>Alignment between pricing tier (On-Demand vs.\u00a0Editions) and actual slot consumption profile.</td>
      <td><code>total_slot_ms</code>, baseline vs.\u00a0peak slot ratios, idle slot availability.</td>
      <td>Edition selection (Standard/Enterprise), slot commitments, idle slot sharing across reservations.</td>
    </tr>
    <tr>
      <td><strong>Query Execution &amp; Efficiency</strong></td>
      <td>Query anti-patterns, redundant data scans, and compute inefficiencies.</td>
      <td><code>total_bytes_billed</code>, column pruning rate, <code>SELECT *</code> frequency, 10\u00a0MB floor impact.</td>
      <td>Partitioning/clustering pruning, Materialized Views, BI Engine acceleration.</td>
    </tr>
    <tr>
      <td><strong>Storage Architecture &amp; Lifecycle Economics</strong></td>
      <td>Storage billing models, compression efficiencies, and backup overhead.</td>
      <td>Logical vs.\u00a0physical byte ratios, table churn (&gt;0.5), Time-Travel overhead.</td>
      <td>Physical storage billing adoption, 48h Time-Travel limit, zero-copy Table Snapshots, dataset TTLs.</td>
    </tr>
    <tr>
      <td><strong>Governance, Attribution &amp; Risk Control</strong></td>
      <td>Spend visibility, project guardrails, and uncontrolled cost exposure.</td>
      <td>Custom quota configurations, <code>max_bytes_billed</code> settings, reservation assignments.</td>
      <td>Execution vs.\u00a0storage project decoupling, custom user/project quotas, budget alerts.</td>
    </tr>
  </tbody>
</table>

<h3>Detailed Dimension Breakdown</h3>
<div class="dimension-breakdown">

<div class="dimension-card">
  <div class="dimension-number">1</div>
  <div class="dimension-body">
    <h4>Compute Model &amp; Capacity Sizing</h4>
    <p><strong>Scope:</strong> Evaluates whether compute capacity matches workload patterns and if the pricing
    structure minimizes unit cost.</p>
    <p><strong>Focus Areas:</strong> Comparing On-Demand ($6.25/TB) against Editions slot-hour pricing;
    tuning baseline versus autoscaling slot limits; locking in 1-year or 3-year commitments for sustained
    compound usage; and consolidating workloads under shared Admin Projects to leverage idle slot sharing.</p>
  </div>
</div>

<div class="dimension-card">
  <div class="dimension-number">2</div>
  <div class="dimension-body">
    <h4>Query Execution &amp; Efficiency</h4>
    <p><strong>Scope:</strong> Targets query execution anti-patterns that unnecessarily inflate scanned byte
    counts or slot consumption.</p>
    <p><strong>Focus Areas:</strong> Eliminating column-pruning failures (<code>SELECT *</code>); enforcing required
    partition filters (<code>require_partition_filter</code>); deploying Materialized Views for recurring
    aggregations; and assigning BI Engine capacity to eliminate scan costs on cached BI queries.</p>
  </div>
</div>

<div class="dimension-card">
  <div class="dimension-number">3</div>
  <div class="dimension-body">
    <h4>Storage Architecture &amp; Lifecycle Economics</h4>
    <p><strong>Scope:</strong> Optimizes dataset storage pricing models, table backup strategies, and lifecycle management.</p>
    <p><strong>Focus Areas:</strong> Migrating high-compression datasets to Physical Storage Billing; replacing
    full copy tables with zero-copy Table Snapshots; reducing Time-Travel retention from 7\u00a0days to 2\u00a0days
    on high-churn tables; and configuring automated partition and table expirations (TTLs).</p>
  </div>
</div>

<div class="dimension-card">
  <div class="dimension-number">4</div>
  <div class="dimension-body">
    <h4>Governance, Attribution &amp; Risk Control</h4>
    <p><strong>Scope:</strong> Establishes organizational controls to prevent runaway costs and enable spend attribution.</p>
    <p><strong>Focus Areas:</strong> Decoupling data storage projects from execution billing projects; applying
    project-level custom quotas and <code>max_bytes_billed</code> query flags; mapping reservation assignments to
    business units; and configuring programmatic budget alerts.</p>
  </div>
</div>

</div>

<h3>Data Sources &amp; Assessment Limitations</h3>
<div class="scope-box">
  <ul class="scope-details">
    <li><strong>Environment Scope:</strong> Target project <code>{project}</code> in region
    <code>{region}</code>, managed via administrative project <code>{admin}</code>.</li>
    <li><strong>Primary Telemetry:</strong> <code>{region}.INFORMATION_SCHEMA.JOBS</code> analyzing
    {self._lookback_days}\u00a0days of execution metadata (retained natively up to 180\u00a0days).</li>
    <li><strong>Reservation Attribution:</strong> Jobs billed under slot reservations report
    <code>total_bytes_billed\u00a0=\u00a00</code> and are evaluated via slot-hour consumption
    (<code>total_slot_ms</code>).</li>
    <li><strong>Regional Isolation:</strong> Metadata is strictly region-scoped; jobs or datasets
    located outside <code>{region}</code> are excluded from this baseline.</li>
    <li><strong>Principal Aggregation:</strong> Top spender metrics are aggregated per service account
    or user principal; identical queries executed across multiple callers are attributed to the
    initiating principal identity.</li>
  </ul>
</div>
</div>"""
        return self._wrap_section(2, "Scope & Approach", content, always_has_data=True)

    # ── Section 3: Pricing & Slots ────────────────────────────────────
    def _section_3_pricing(self) -> str:
        content = ""
        sim = self._data.get("bq_slots_simulation_results")
        jobs = self._data.get("bq_job_results")
        tiered = self._data.get("bq_slots_tiered")
        slots = self._data.get("bq_slots_results")

        if sim or jobs:
            # Unified baseline on-demand cost (same as Section 1)
            b = self._baseline
            ondemand_cost = b["on_demand_cost"]
            editions_cost = 0.0
            if isinstance(sim, list) and sim:
                best_row = min(sim, key=lambda r: FindingsSynthesizer._safe_float(r.get("total_3yr")))
                editions_cost = FindingsSynthesizer._safe_float(best_row.get("total_3yr"))
            elif isinstance(jobs, dict):
                lookback_days = max(self._lookback_days, 1)
                monthly_factor = 30.0 / lookback_days
                raw_editions = sum(
                    FindingsSynthesizer._safe_float(ps.get("total_editions_cost"))
                    for ps in jobs.get("project_summaries", [])
                    if isinstance(ps, dict)
                )
                editions_cost = raw_editions * monthly_factor

            content += f"""<h3>Pricing Model Comparison</h3>
<table class="data-table">
  <thead><tr><th>Model</th><th>Estimated Monthly Cost (30-day baseline)</th><th>Rate</th></tr></thead>
  <tbody>
    <tr><td>On-Demand</td><td>{_format_usd(ondemand_cost)}</td><td>${ON_DEMAND_USD_PER_TB}/TiB</td></tr>
    <tr><td>Editions (Projected)</td><td>{_format_usd(editions_cost)}</td><td>${EDITIONS_SLOT_HR_RATE}/slot-hr</td></tr>
  </tbody>
</table>"""

            # ── Break-even analysis ──
            be_enterprise = ondemand_cost / (0.06 * 730) if ondemand_cost > 0 else 0
            be_standard = ondemand_cost / (0.04 * 730) if ondemand_cost > 0 else 0
            be_ent_1yr = ondemand_cost / (0.048 * 730) if ondemand_cost > 0 else 0
            be_ent_3yr = ondemand_cost / (0.036 * 730) if ondemand_cost > 0 else 0
            avg_slots = b["avg_slots"]  # Use baseline avg_slots (consistent with Section 1)

            if ondemand_cost <= 0:
                verdict = '<span class="verdict-neutral">\u2139\ufe0f Workload runs on reservations. On-Demand pricing comparison not applicable.</span>'
            elif editions_cost > 0 and editions_cost < ondemand_cost * 0.9:
                verdict = f'<span class="verdict-positive">\u2705 Editions is {((ondemand_cost - editions_cost) / ondemand_cost * 100):.0f}% cheaper. Migrate to Editions.</span>'
            elif editions_cost > ondemand_cost:
                verdict = f'<span class="verdict-negative">\u274c On-Demand is cheaper ({editions_cost / ondemand_cost:.1f}\u00d7). Remain on On-Demand.</span>'
            else:
                verdict = '<span class="verdict-neutral">\u2796 Comparable costs. Evaluate based on performance and governance needs.</span>'

            content += f"""<div class="break-even-box">
<h3>Break-Even Analysis</h3>
<p>At {_format_usd(ondemand_cost)}/mo on-demand, BigQuery Editions becomes cheaper only if <strong>sustained average</strong> slot consumption stays below:</p>
<table class="data-table">
  <thead><tr><th>Tier</th><th>Rate</th><th>Break-Even (slots)</th></tr></thead>
  <tbody>
    <tr><td>Enterprise PAYG</td><td>$0.06/slot-hr</td><td>{be_enterprise:,.0f} slots</td></tr>
    <tr><td>Enterprise 1yr commit</td><td>$0.048/slot-hr</td><td>{be_ent_1yr:,.0f} slots</td></tr>
    <tr><td>Enterprise 3yr commit</td><td>$0.036/slot-hr</td><td>{be_ent_3yr:,.0f} slots</td></tr>
    <tr><td>Standard PAYG</td><td>$0.04/slot-hr</td><td>{be_standard:,.0f} slots</td></tr>
  </tbody>
</table>
<p>Measured average consumption: <strong>~{avg_slots:,.0f} slots</strong></p>
<p class="verdict">{verdict}</p>
</div>"""

        if tiered:
            content += self._render_table(
                "Tiered Slot Recommendations", tiered,
                ["reservation_id", "balanced_baseline_p95", "suggested_autoscale_max", "performance_baseline_max"],
                ["Reservation", "Balanced P95", "Autoscale Max", "Perf. Max"],
                formatters={"reservation_id": _reservation_link},
            )

        return self._wrap_section(3, "Compute & Pricing Analysis", content)

    # ── Section 4: Query Optimizations ────────────────────────────────
    @staticmethod
    def _derive_billing_model(cost: float, bytes_billed: float) -> str:
        """Infer On-Demand vs Reservation from cost/bytes ratio."""
        if bytes_billed <= 0 and cost > 0:
            return "Reservation (slot-attributed)"
        if bytes_billed > 0:
            tib = bytes_billed / (1024 ** 4)
            expected = tib * ON_DEMAND_USD_PER_TB
            if expected > 0 and abs(cost - expected) / expected < 0.15:
                return "On-Demand"
            elif cost > expected * 2:
                return "Reservation (slot-attributed)"
        return "On-Demand"

    def _section_4_query(self) -> str:
        content = ""
        top = self._data.get("bq_top_spenders")
        if top and isinstance(top, list):
            sf = FindingsSynthesizer._safe_float
            for row in top:
                if isinstance(row, dict) and "billing_model" not in row:
                    row["billing_model"] = self._derive_billing_model(
                        sf(row.get("total_actual_cost")),
                        sf(row.get("total_bytes_billed")),
                    )
            content += self._render_table(
                "Top Spenders", top,
                ["user_email", "total_bytes_billed", "total_actual_cost", "query_count", "billing_model"],
                ["User", "Bytes Billed", "Cost (USD)", "Queries", "Billing Model"],
                formatters={"total_bytes_billed": _format_bytes, "total_actual_cost": _format_usd, "query_count": _format_number},
            )
            content += '<div class="table-footnote"><em>Billing Model is inferred: if cost reconciles to bytes\u00d7$6.25/TiB \u00b115%, it is On-Demand. Service accounts with zero bytes billed but real cost are slot-attributed (Reservation).</em></div>'

        linter = self._data.get("bq_linter_results")
        if linter:
            # FIX #4: Unescape any pre-encoded HTML entities in query snippets
            import html as _html_mod
            for row in linter:
                if isinstance(row, dict) and "query_snippet" in row:
                    row["query_snippet"] = _html_mod.unescape(str(row["query_snippet"]))
            content += self._render_table(
                "Query Optimization Issues", linter,
                ["abuse_type", "user_email", "billed_gb", "query_snippet"],
                ["Abuse Type", "User", "Billed GB", "Query Snippet"],
                formatters={"billed_gb": _format_round0},
            )

        batch = self._data.get("bq_batch_results")
        if batch:
            content += self._render_table(
                "Batch Candidates", batch,
                ["workload_name", "total_slot_hours", "total_job_runs", "recommended_priority"],
                ["Workload", "Slot Hours", "Job Runs", "Priority"],
                formatters={"total_slot_hours": _format_round2, "total_job_runs": _format_number},
            )

        cost_attr_raw = self._data.get("bq_cost_attribution_results")
        if cost_attr_raw:
            # Response is {"attributions": [...], ...} — extract the inner list
            if isinstance(cost_attr_raw, dict):
                cost_attr = cost_attr_raw.get("attributions", [])
            else:
                cost_attr = cost_attr_raw
            content += self._render_table(
                "Cost Attribution", cost_attr,
                ["project_id", "reservation_id", "direct_usage_cost_usd", "total_cost_attribution_usd"],
                ["Project", "Reservation", "Direct Cost", "Total Attributed"],
                formatters={
                    "project_id": lambda v, row=None: _bq_link(v, kind="project"),
                    "reservation_id": _reservation_link,
                    "direct_usage_cost_usd": _format_usd,
                    "total_cost_attribution_usd": _format_usd,
                },
            )

        return self._wrap_section(4, "Query Efficiency Analysis", content)

    # ── Section 5: Storage ────────────────────────────────────────────
    @staticmethod
    def _filter_storage(datasets: list[dict]) -> list[dict]:
        """Filter internal datasets and dedupe contradictory rows."""
        import re
        internal_re = re.compile(r'^(_script|_bqc|_SESSION)', re.IGNORECASE)
        sf = FindingsSynthesizer._safe_float
        filtered = [d for d in datasets if isinstance(d, dict) and not internal_re.match(d.get("dataset_name", ""))]
        seen: dict[str, dict] = {}
        for d in filtered:
            key = d.get("dataset_name", "")
            if key in seen:
                if sf(d.get("monthly_savings")) > sf(seen[key].get("monthly_savings")):
                    seen[key] = d
            else:
                seen[key] = d
        return list(seen.values())

    @staticmethod
    def _derive_issue(row: dict) -> str:
        """Derive an actionable issue label from static audit metadata."""
        import re
        sf = FindingsSynthesizer._safe_float
        table_id = row.get("table_id", "")
        size = sf(row.get("size_bytes"))
        is_part = row.get("is_partitioned")
        is_clust = row.get("is_clustered")
        size_tb = size / (1024 ** 4)
        if re.search(r'(_bkp|_backup|_bkup|_ss$|_ss_|_copy_|_old$)', table_id, re.IGNORECASE):
            return '\U0001F4E6 Convert to snapshot'
        if not is_part and size_tb > 10:
            return '\u26a0\ufe0f Partition + require_filter'
        if not is_part and size_tb > 1:
            return '\u26a0\ufe0f Partition recommended'
        if not is_clust and size_tb > 1:
            return '\U0001F50D Consider clustering'
        return '\u2705 OK'

    def _section_5_storage(self) -> str:
        content = ""
        storage_raw = self._data.get("bq_storage_results")
        if storage_raw:
            if isinstance(storage_raw, dict):
                storage = storage_raw.get("datasets", [])
            else:
                storage = storage_raw
            population_count = len(storage)
            storage = self._filter_storage(storage)
            content += self._render_table(
                "Storage Analysis", storage,
                ["dataset_name", "currently_on", "better_on", "monthly_spending", "monthly_savings"],
                ["Dataset", "Current Model", "Recommended", "Monthly Cost", "Savings"],
                formatters={
                    "dataset_name": lambda v, row=None: _bq_link(v, kind="dataset",
                        project=row.get("project_name", "") if row else ""),
                    "monthly_spending": _format_usd,
                    "monthly_savings": _format_usd,
                },
            )
            if population_count != len(storage):
                content += f'<div class="table-footnote"><em>{population_count} datasets analyzed; {population_count - len(storage)} internal/duplicate datasets filtered. Showing {len(storage)} unique datasets.</em></div>'

        static = self._data.get("bq_static_audit_results")
        if static and isinstance(static, list):
            for row in static:
                if isinstance(row, dict) and "issue" not in row:
                    row["issue"] = self._derive_issue(row)
            # FIX #5: Deduplicate by (dataset_id, table_id)
            seen_static: dict[str, dict] = {}
            for row in static:
                if not isinstance(row, dict):
                    continue
                key = f"{row.get('dataset_id', '')}|{row.get('table_id', '')}"
                if key not in seen_static:
                    seen_static[key] = row
            static_deduped = list(seen_static.values())
            content += self._render_table(
                "Static Audit", static_deduped,
                ["table_id", "dataset_id", "is_partitioned", "is_clustered", "size_bytes", "issue"],
                ["Table", "Dataset", "Partitioned", "Clustered", "Size", "Issue"],
                formatters={
                    "table_id": lambda v, row=None: _bq_link(v, kind="table",
                        project=row.get("project_id", "") if row else "",
                        dataset=row.get("dataset_id", "") if row else ""),
                    "dataset_id": lambda v, row=None: _bq_link(v, kind="dataset",
                        project=row.get("project_id", "") if row else ""),
                    "size_bytes": _format_bytes,
                },
            )

        hygiene = self._data.get("bq_hygiene_results")
        if hygiene:
            # FIX #5: Deduplicate by (dataset, table_name)
            seen_hyg: dict[str, dict] = {}
            for row in hygiene:
                if not isinstance(row, dict):
                    continue
                key = f"{row.get('dataset', '')}|{row.get('table_name', '')}"
                if key not in seen_hyg:
                    seen_hyg[key] = row
            hygiene_deduped = list(seen_hyg.values())
            content += self._render_table(
                "Table Hygiene", hygiene_deduped,
                ["table_name", "dataset", "live_active_physical_gb", "health_status", "churn_ratio"],
                ["Table", "Dataset", "Active GB", "Health", "Churn Ratio"],
                formatters={
                    "table_name": lambda v, row=None: _bq_link(v, kind="table",
                        project=row.get("project_id", "") if row else "",
                        dataset=row.get("dataset", "") if row else ""),
                    "dataset": lambda v, row=None: _bq_link(v, kind="dataset",
                        project=row.get("project_id", "") if row else ""),
                    "live_active_physical_gb": _format_round2,
                    "churn_ratio": _format_round2,
                },
            )

        return self._wrap_section(5, "Storage & Lifecycle Analysis", content)

    # ── Section 6: Findings & Recommendations ──────────────────────────
    def _section_6_findings(self) -> str:
        content = ""

        # ── Tier 1: Findings Register (compact summary table) ──
        if self._findings:
            reg_rows = ""
            for f in self._findings:
                impact_cell = _format_usd(f.impact_usd_monthly) if f.impact_usd_monthly else "\u2014"
                baseline_label = "compute" if f.pillar in ("Pricing", "Query") else "storage"
                if f.impact_pct_of_baseline:
                    ceiling_tag = ' \u26a0\ufe0f ceiling' if f.exceeds_baseline else ''
                    pct_cell = f"{f.impact_pct_of_baseline:.0f}% of {baseline_label}{ceiling_tag}"
                else:
                    pct_cell = "\u2014"
                reg_rows += (
                    f'<tr>'
                    f'<td class="mono"><a href="#finding-{_esc(f.ref_id)}">{_esc(f.ref_id)}</a></td>'
                    f'<td>{_esc(f.title)}</td>'
                    f'<td>{_esc(f.pillar)}</td>'
                    f'<td>{_esc(f.lever)}</td>'
                    f'<td class="num"><strong>{f.score:.0f}</strong></td>'
                    f'<td><span class="priority-badge priority-{f.priority.lower()}">{f.score_glyph} {_esc(f.priority)}</span></td>'
                    f'<td class="num">{impact_cell}</td>'
                    f'<td class="num">{pct_cell}</td>'
                    f'<td>{_esc(f.confidence[0]) if f.confidence else "—"}</td>'
                    f'<td>{_esc(f.effort)}</td>'
                    f'<td>{_esc(f.horizon)}</td>'
                    f'</tr>'
                )
            content += f"""<h3>Findings Register</h3>
<p class="register-intro">Sorted by deterministic score (descending). Score = (Impact + Risk) \u00d7 Confidence \u00d7 Effort \u00d7 5, capped at 100.</p>
<table class="data-table findings-register">
  <thead><tr>
    <th>Ref</th><th>Finding</th><th>Pillar</th><th>Lever</th><th>Score</th>
    <th>Priority</th><th>Impact $/mo</th><th>% of Baseline</th><th>Conf.</th><th>Effort</th><th>Horizon</th>
  </tr></thead>
  <tbody>{reg_rows}</tbody>
</table>"""

        # ── Consolidated Savings Summary (grouped by lever) ──
        findings_with_impact = [f for f in self._findings if f.impact_usd_monthly]
        if findings_with_impact:
            lever_groups: dict[str, list[Finding]] = {}
            for f in findings_with_impact:
                lever_groups.setdefault(f.lever, []).append(f)

            rows = ""
            grand_total = 0.0
            has_overlap = False
            for lever in ["Rate", "Usage", "Waste", "Risk"]:
                group = lever_groups.get(lever, [])
                if not group:
                    continue
                for f in sorted(group, key=lambda x: -(x.impact_usd_monthly or 0)):
                    grand_total += f.impact_usd_monthly or 0
                    overlap_mark = ''
                    if f.overlap_group:
                        overlap_mark = ' \u2020'
                        has_overlap = True
                    ceiling_mark = ''
                    if f.exceeds_baseline:
                        ceiling_mark = ' \u26a0\ufe0f'
                    rows += (
                        f'<tr><td>{_esc(f.ref_id)}: {_esc(f.title)}{overlap_mark}{ceiling_mark}</td>'
                        f'<td>{_esc(lever)}</td>'
                        f'<td><span class="priority-badge priority-{f.priority.lower()}">{f.score_glyph} {_esc(f.priority)}</span></td>'
                        f'<td class="num">{_format_usd(f.impact_usd_monthly)}</td>'
                        f'<td>{_esc(f.confidence)}</td>'
                        f'<td>{_esc(f.effort)}</td>'
                        f'<td>{_esc(f.horizon)}</td></tr>'
                    )
            rows += f'<tr class="total-row"><td><strong>Sum total (may overlap)</strong></td><td></td><td></td><td class="num"><strong>{_format_usd(grand_total)}</strong></td><td></td><td></td><td></td></tr>'

            content += f"""<h3>Consolidated Savings Summary</h3>
<table class="data-table">
  <thead><tr><th>Finding</th><th>Lever</th><th>Priority</th><th>Est. Savings/mo</th><th>Conf.</th><th>Effort</th><th>Horizon</th></tr></thead>
  <tbody>{rows}</tbody>
</table>"""
            content += self._callout("reconciliation",
                'Rate and Usage savings compound multiplicatively, not additively. '
                'Findings sharing an affected-resource set are counted once at the larger estimate.'
            )
            if has_overlap:
                content += self._callout("reconciliation",
                    '\u2020 Findings marked with \u2020 share the same underlying waste (billing-floor micro-queries). '
                    'Do not sum their savings independently.'
                )

        # ── Tier 2: Detailed Finding Cards (5 C\u2019s structure) ──
        if self._findings:
            content += "<h3>Detailed Findings</h3>"
        for f in self._findings:
            impact_html = ""
            if f.impact_usd_monthly:
                ceiling_suffix = " \u2014 theoretical ceiling" if f.exceeds_baseline else ""
                pct_text = f" ({f.impact_pct_of_baseline:.0f}% of {'compute' if f.pillar in ('Pricing', 'Query') else 'storage'} baseline{ceiling_suffix})" if f.impact_pct_of_baseline else ""
                impact_html = f'<div class="fc-field"><span class="fc-label">CONSEQUENCE</span>{_format_usd(f.impact_usd_monthly)}/mo{pct_text}</div>'

            evidence_html = f'<div class="fc-field"><span class="fc-label">EVIDENCE</span>{_esc(f.evidence)}</div>' if f.evidence else ""
            affected_html = f'<div class="fc-field"><span class="fc-label">AFFECTED</span><code>{_esc(f.affected_objects)}</code></div>' if f.affected_objects else ""
            remediation_html = f'<div class="fc-field"><span class="fc-label">STEPS</span>{_esc(f.remediation_steps)}</div>' if f.remediation_steps else ""

            content += f"""<div class="finding-card" id="finding-{_esc(f.ref_id)}">
  <div class="finding-header">
    <span class="finding-ref">{_esc(f.ref_id)}</span>
    <span class="score-badge">{f.score:.0f}</span>
    <span class="priority-badge priority-{f.priority.lower()}">{f.score_glyph} {_esc(f.priority)}</span>
    <span class="pillar-badge">{_esc(f.pillar)}</span>
    <span class="lever-badge">{_esc(f.lever)}</span>
    <span class="effort-badge">Effort: {_esc(f.effort)}</span>
    <span class="conf-badge">Conf: {_esc(f.confidence)}</span>
    <span class="horizon-badge">{_esc(f.horizon)}</span>
  </div>
  <h4 class="finding-title">{_esc(f.title)}</h4>
  <div class="fc-field"><span class="fc-label">CONDITION</span>{_esc(f.description)}</div>
  {evidence_html}
  {impact_html}
  {affected_html}
  <div class="fc-field"><span class="fc-label">RECOMMENDATION</span>{_esc(f.recommendation)}</div>
  {remediation_html}
  <a class="finding-docs" href="{_esc(f.official_docs_url)}" target="_blank" rel="noopener">Documentation \u2192</a>
</div>"""

        # ── Checks Passed / Not Applicable ──
        triggered_ids = {f.finding_id for f in self._findings}
        passed_items = ""
        for fdef in _FINDING_DEFS:
            fid = fdef["finding_id"]
            ref = fdef["ref_id"]
            title = fdef["title"]
            is_evaluated = all(self._is_module_evaluated(k) for k in fdef["keys"])
            if fid in triggered_ids:
                continue
            if is_evaluated:
                passed_items += f'<tr class="check-passed"><td>{_esc(ref)}</td><td>{_esc(title)}</td><td>\u2705 Passed</td></tr>'
            else:
                passed_items += f'<tr class="check-na"><td>{_esc(ref)}</td><td>{_esc(title)}</td><td>\u2b1c Not run (no data)</td></tr>'
        if passed_items:
            content += f"""<h3>Checks Passed / Not Applicable</h3>
<table class="data-table">
  <thead><tr><th>Ref</th><th>Check</th><th>Status</th></tr></thead>
  <tbody>{passed_items}</tbody>
</table>"""

        # ── Phased Roadmap (driven by findings horizon) ──
        # FIX #6: Sort within each phase so dependencies come before dependents
        def _dep_sort(flist: list[Finding]) -> list[Finding]:
            """Topological sort: findings with depends_on come after their prerequisite."""
            id_set = {f.finding_id for f in flist}
            deps = []
            no_deps = []
            for f in flist:
                if f.depends_on and f.depends_on in id_set:
                    deps.append(f)
                else:
                    no_deps.append(f)
            return no_deps + deps  # Prerequisites first, then dependents

        w1 = _dep_sort([f for f in self._findings if f.horizon == "W1"])
        m1 = _dep_sort([f for f in self._findings if f.horizon == "M1"])
        q1 = _dep_sort([f for f in self._findings if f.horizon == "Q1"])

        def _hz(flist: list[Finding]) -> str:
            if not flist:
                return ""
            return ", ".join(f"<strong>{_esc(f.ref_id)}</strong>: {_esc(f.title)}" for f in flist)

        content += f"""<h3>Implementation Roadmap</h3>
<div class="roadmap">
  <div class="roadmap-phase">
    <div class="phase-label">Week 1</div>
    <div class="phase-content"><strong>Quick wins:</strong> {_hz(w1) or 'Configure custom quotas, set <code>max_bytes_billed</code> on critical jobs.'}</div>
  </div>
  <div class="roadmap-phase">
    <div class="phase-label">Month 1</div>
    <div class="phase-content"><strong>Demand reduction:</strong> {_hz(m1) or 'Audit <code>SELECT *</code> views, replace manual backups with snapshots.'}</div>
  </div>
  <div class="roadmap-phase">
    <div class="phase-label">Quarter 1</div>
    <div class="phase-content"><strong>Structural optimizations:</strong> {_hz(q1) or 'Partition top tables, refactor micro-query pipelines.'}</div>
  </div>
  <div class="roadmap-phase">
    <div class="phase-label">Ongoing</div>
    <div class="phase-content"><strong>Re-measure:</strong> Track KPIs (TiB billed/mo, $/TiB, avg slots). Reassess pricing model at 30/60/90 day intervals.</div>
  </div>
</div>"""

        return self._wrap_section(6, "Findings & Action Items", content)

    # ── Section 7: Appendix & Glossary ───────────────────────────────
    def _section_7_appendix(self) -> str:
        ai_used = self._has_data("bq_ai_results")
        ai_provenance = (
            "<strong>AI / LLM Usage:</strong> Vertex AI (Gemini) was utilized exclusively for the semantic query rewrites in Section 4 (AI Doctor). All other calculations, financial projections, rule-based heuristics, and scorecards were computed deterministically by local algorithms."
            if ai_used
            else "<strong>AI / LLM Usage:</strong> None. This assessment was generated 100% deterministically by local algorithms and rule-based heuristics without the use of any Generative AI or Large Language Models (LLMs)."
        )

        content = f"""<h3>Glossary of Terms</h3>
<table class="data-table">
  <thead><tr><th>Term</th><th>Definition</th></tr></thead>
  <tbody>
    <tr><td>Slot</td><td>Unit of BigQuery compute capacity (~0.5 vCPU + RAM). Queries consume slots proportional to data scanned and complexity.</td></tr>
    <tr><td>Slot-hour</td><td>1 slot sustained for 1 hour. The billing unit for Editions pricing.</td></tr>
    <tr><td>Bytes billed</td><td>Data scanned after partition/column pruning, rounded up to a <strong>10 MB minimum per table referenced</strong>.</td></tr>
    <tr><td>On-Demand</td><td>Pay-per-query at <strong>${ON_DEMAND_USD_PER_TB}/TiB</strong> billed. No commitment; 2,000 concurrent slot cap per project.</td></tr>
    <tr><td>Editions</td><td>Slot-based pricing. Enterprise: $0.06/slot-hr (PAYG), $0.048 (1yr), $0.036 (3yr). Standard: $0.04/slot-hr. Autoscale + baseline split.</td></tr>
    <tr><td>Logical billing</td><td>Storage charged on uncompressed logical size. Default for new datasets.</td></tr>
    <tr><td>Physical billing</td><td>Storage charged on compressed physical size + time-travel + fail-safe bytes. Often cheaper for compressed data.</td></tr>
    <tr><td>Churn ratio</td><td>Fraction of table bytes rewritten per period via DML. High churn resets the 90-day long-term storage discount timer.</td></tr>
  </tbody>
</table>

<h3>Scoring Methodology</h3>
<p>Each finding is assigned a deterministic score using the formula:</p>
<div class="scope-box">
  <p><strong>Score</strong> = (Impact + Risk) &times; Confidence &times; Effort &times; 5, capped at 100</p>
  <ul>
    <li><strong>Impact (0\u201310):</strong> Financial weight derived from estimated savings as a percentage of the relevant baseline.</li>
    <li><strong>Risk (0\u201310):</strong> Operational or governance risk independent of dollar impact (e.g., missing guardrails).</li>
    <li><strong>Confidence (0.5\u20131.0):</strong> H = 1.0, M = 0.75, L = 0.5. Reflects data quality and assumption strength.</li>
    <li><strong>Effort (0.5\u20131.0):</strong> Low = 1.0, Medium = 0.75, High = 0.5. Inversely proportional to implementation complexity.</li>
  </ul>
  <p><strong>Priority bands:</strong> Critical \u2265 75, High \u2265 50, Medium \u2265 25, Low \u2265 10, Info &lt; 10.</p>
</div>

<h3>Report Generation</h3>
<div class="scope-box">
  <p>This assessment was generated by <strong><a href="https://github.com/mbettan/bq-finops-optimizer" target="_blank" rel="noopener" style="color: inherit; text-decoration: underline;">FinOps Optimizer v{_esc(__version__)}</a></strong>.
  All telemetry data was queried directly from BigQuery <code>INFORMATION_SCHEMA</code> views for project <code>{_esc(self._data.get('bq_org_project') or 'selected scope')}</code> in region <code>{_esc(self._data.get('bq_region') or 'US')}</code>.</p>
  <p style="margin-top: 0.6rem;">{ai_provenance}</p>
</div>

<h3>Disclaimer</h3>
<div class="savings-disclaimer" style="margin-top: 1rem; line-height: 1.6;">
  <p><strong>Not a Google Product:</strong> This tool is an independent, personal open-source project (Apache 2.0) and is not affiliated with, endorsed by, or supported by Google.</p>
  <p style="margin-top: 0.5rem;"><strong>Cost Modeling & Verification:</strong> All costs and savings figures presented in this assessment are mathematical projections calculated using standard Google Cloud public list prices (${ON_DEMAND_USD_PER_TB}/TiB on-demand, $0.06/slot-hour Editions baseline). They do not reflect enterprise contractual discounts, Custom Pricing Agreements (CPAs), or Committed Use Discounts (CUDs). Always verify findings against the Google Cloud Billing Console prior to making architectural or contractual commitments.</p>
  <p style="margin-top: 0.5rem;"><strong>Execution & Safety:</strong> Provided "AS IS", without warranty of any kind. All recommended DDL queries, partition modifications, and table alterations should be thoroughly tested and validated in non-production environments first.</p>
</div>"""
        return self._wrap_section(7, "Appendix & Glossary", content, always_has_data=True)

    # ── Helpers ───────────────────────────────────────────────────────

    def _is_module_evaluated(self, key: str) -> bool:
        """True if the module was run and stored in the snapshot (even if 0 findings returned)."""
        val = self._data.get(key)
        return val is not None

    def _has_data(self, key: str) -> bool:
        val = self._data.get(key)
        if val is None:
            return False
        if isinstance(val, (list, dict)) and len(val) == 0:
            return False
        return True

    def _wrap_section(self, num: int, title: str, content: str, *, always_has_data: bool = False) -> str:
        if not content and not always_has_data:
            placeholder = '<div class="section-placeholder"><p>No data available \u2014 run this module in the app to populate this section.</p></div>'
            return f'<div class="report-section" id="section-{num}"><h2>{num}. {_esc(title)}</h2>{placeholder}</div>'
        return f'<div class="report-section" id="section-{num}"><h2>{num}. {_esc(title)}</h2>{content}</div>'

    def _render_table(
        self, title: str, data: Any, keys: list[str], headers: list[str],
        *, formatters: dict[str, Any] | None = None,
    ) -> str:
        rows = data if isinstance(data, list) else ([data] if isinstance(data, dict) else [])
        total = len(rows)
        capped = rows[:MAX_TABLE_ROWS]
        header_html = "".join(f"<th>{_esc(h)}</th>" for h in headers)
        fmts = formatters or {}
        body_html = ""
        for row in capped:
            if not isinstance(row, dict):
                continue
            _dash = "\u2014"
            cells = ""
            for k in keys:
                raw = row.get(k, _dash)
                if k in fmts:
                    fn = fmts[k]
                    try:
                        formatted = fn(raw, row=row)
                    except TypeError:
                        formatted = fn(raw)
                    cells += f"<td>{formatted}</td>"
                else:
                    cells += f"<td>{_esc(raw)}</td>"
            body_html += f"<tr>{cells}</tr>"
        footer = ""
        if total > MAX_TABLE_ROWS:
            footer = f'<div class="table-cap-footer">Showing top {MAX_TABLE_ROWS} of {total} \u2014 re-run this module in the app for full results.</div>'
        return f"""<h3>{_esc(title)}</h3>
<table class="data-table"><thead><tr>{header_html}</tr></thead><tbody>{body_html}</tbody></table>
{footer}"""

    def _debug_footer(self) -> str:
        errors = self._data.get("_parse_errors", [])
        if not errors:
            return ""
        items = "".join(f"<li>{_esc(e)}</li>" for e in errors)
        return f"""<details class="debug-footer">
<summary>Parse errors ({len(errors)})</summary>
<ul>{items}</ul>
</details>"""

    def _toolbar_script(self) -> str:
        toolbar_data = {
            "version": __version__,
            "project": self._data.get("bq_org_project"),
            "region": self._data.get("bq_region"),
            "lookback_days": self._lookback_days,
            "findings": [f.model_dump() for f in self._findings],
            "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }
        return f"""<script id="report-data" type="application/json">{_json_safe(toolbar_data)}</script>
<script nonce="{_esc(self._nonce)}">
(function() {{
  const data = JSON.parse(document.getElementById('report-data').textContent);
  const toolbar = document.createElement('div');
  toolbar.className = 'report-toolbar';
  toolbar.innerHTML = `
    <button id="btn-print" title="Print / Save as PDF">&#x1F5A8;&#xFE0F; Print</button>
    <button id="btn-copy-markdown" title="Copy full report as Markdown">&#x1F4DD; Markdown</button>
    <button id="btn-toggle-theme" title="Toggle light/dark theme">\u2600\ufe0f/&#x1F319;</button>
    <button id="btn-download-json" title="Download findings as JSON">&#x1F4CA; JSON</button>
  `;
  document.body.appendChild(toolbar);
  document.getElementById('btn-print').addEventListener('click', function() {{ window.print(); }});
  document.getElementById('btn-copy-markdown').addEventListener('click', function() {{
    const p = data.project || 'Unknown';
    const ts = data.generated_at || '';
    const lb = data.lookback_days || 30;
    let md = '# BigQuery Cost Optimization Report\\n\\n';
    md += '| Field | Value |\\n|---|---|\\n';
    md += '| Project | `' + p + '` |\\n';
    md += '| Generated | ' + ts + ' |\\n';
    md += '| Lookback | ' + lb + ' days |\\n\\n';
    const narr = document.querySelector('.narrative-summary');
    if (narr) {{ md += '## Executive Summary\\n\\n' + narr.innerText.trim() + '\\n\\n'; }}
    const cards = document.querySelectorAll('.summary-card');
    if (cards.length) {{
      md += '| Metric | Value |\\n|---|---|\\n';
      cards.forEach(function(c) {{
        const v = c.querySelector('.summary-value');
        const l = c.querySelector('.summary-label');
        if (v && l) md += '| ' + l.innerText + ' | **' + v.innerText + '** |\\n';
      }});
      md += '\\n';
    }}
    const be = document.querySelector('.break-even-box');
    if (be) {{
      md += '## Break-Even Analysis\\n\\n';
      const beRows = be.querySelectorAll('tbody tr');
      if (beRows.length) {{
        md += '| Tier | Rate | Break-Even |\\n|---|---|---|\\n';
        beRows.forEach(function(r) {{
          const cells = r.querySelectorAll('td');
          if (cells.length >= 3) md += '| ' + cells[0].innerText + ' | ' + cells[1].innerText + ' | ' + cells[2].innerText + ' |\\n';
        }});
        md += '\\n';
      }}
      const verdict = be.querySelector('.verdict');
      if (verdict) md += '**Verdict:** ' + verdict.innerText + '\\n\\n';
    }}
    if (data.findings && data.findings.length) {{
      md += '## Findings & Recommendations\\n\\n';
      data.findings.forEach(function(f) {{
        md += '### ' + f.ref_id + ': ' + f.title + '\\n\\n';
        md += '- **Priority:** ' + f.priority + '\\n';
        md += '- **Effort:** ' + f.effort + '\\n';
        if (f.impact_usd_monthly) md += '- **Est. Impact:** $' + f.impact_usd_monthly.toFixed(2) + '/mo\\n';
        md += '\\n' + f.description + '\\n\\n';
        if (f.evidence) md += '> **Evidence:** ' + f.evidence + '\\n\\n';
        if (f.affected_objects) md += '> **Affected:** `' + f.affected_objects + '`\\n\\n';
        md += '**Recommendation:** ' + f.recommendation + '\\n\\n';
        if (f.remediation_steps) md += '**Steps:** ' + f.remediation_steps + '\\n\\n';
        md += '[Documentation](' + f.official_docs_url + ')\\n\\n---\\n\\n';
      }});
    }}
    const sf = (data.findings || []).filter(function(f) {{ return f.impact_usd_monthly; }});
    if (sf.length) {{
      md += '## Consolidated Savings Summary\\n\\n';
      md += '| Finding | Priority | Est. Savings/mo | Effort |\\n|---|---|---|---|\\n';
      let tot = 0;
      sf.sort(function(a, b) {{ return (b.impact_usd_monthly || 0) - (a.impact_usd_monthly || 0); }});
      sf.forEach(function(f) {{
        tot += f.impact_usd_monthly || 0;
        md += '| ' + f.ref_id + ': ' + f.title + ' | ' + f.priority + ' | $' + (f.impact_usd_monthly || 0).toFixed(2) + ' | ' + f.effort + ' |\\n';
      }});
      md += '| **Total (may overlap)** | | **$' + tot.toFixed(2) + '** | |\\n\\n';
    }}
    const phases = document.querySelectorAll('.roadmap-phase');
    if (phases.length) {{
      md += '## Implementation Roadmap\\n\\n';
      phases.forEach(function(ph) {{
        const label = ph.querySelector('.phase-label');
        const content = ph.querySelector('.phase-content');
        if (label && content) md += '**' + label.innerText.trim() + ':** ' + content.innerText.trim() + '\\n\\n';
      }});
    }}
    if (navigator.clipboard && navigator.clipboard.writeText) {{
      navigator.clipboard.writeText(md).then(function() {{
        const btn = document.getElementById('btn-copy-markdown');
        const orig = btn.innerHTML;
        btn.innerHTML = '✅ Copied!';
        setTimeout(function() {{ btn.innerHTML = orig; }}, 2000);
      }}).catch(function() {{ alert('Copy failed'); }});
    }} else {{
      const ta = document.createElement('textarea');
      ta.value = md; document.body.appendChild(ta);
      ta.select(); document.execCommand('copy');
      document.body.removeChild(ta);
    }}
  }});
  document.getElementById('btn-toggle-theme').addEventListener('click', function() {{
    document.documentElement.classList.toggle('light-mode');
  }});
  document.getElementById('btn-download-json').addEventListener('click', function() {{
    const blob = new Blob([JSON.stringify(data, null, 2)], {{ type: 'application/json' }});
    const a = document.createElement('a');
    a.href = URL.createObjectURL(blob);
    a.download = 'bq-finops-report-' + (data.project || 'report') + '.json';
    a.click(); URL.revokeObjectURL(a.href);
  }});
}})();
</script>"""


# ---------------------------------------------------------------------------
# 6.  Report cache
# ---------------------------------------------------------------------------

@dataclass
class ReportEntry:
    html: str
    nonce: str
    created: float


_SAFE_REPORT_ID_RE = re.compile(r"^[A-Za-z0-9_-]{12,64}$")
_REPORT_CACHE_DIR = Path(tempfile.gettempdir()) / "bq_finops_report_cache"
_REPORT_CACHE_DIR.mkdir(mode=0o700, parents=True, exist_ok=True)
try:
    os.chmod(_REPORT_CACHE_DIR, 0o700)
except Exception:
    pass
_report_cache: OrderedDict[str, ReportEntry] = OrderedDict()
_cache_lock = threading.Lock()
_CACHE_MAX = 50
_CACHE_TTL_S = 86400  # 24 hours (persisted to disk across restarts)


def _evict_expired() -> None:
    """Remove all expired entries from memory and disk. Must be called under ``_cache_lock``."""
    now = time.time()
    expired = [k for k, v in _report_cache.items() if now - v.created > _CACHE_TTL_S]
    for k in expired:
        del _report_cache[k]
        try:
            (_REPORT_CACHE_DIR / f"{k}.json").unlink(missing_ok=True)
        except Exception:
            pass
    try:
        if _REPORT_CACHE_DIR.exists():
            for f in _REPORT_CACHE_DIR.glob("*.json"):
                if now - f.stat().st_mtime > _CACHE_TTL_S:
                    f.unlink(missing_ok=True)
    except Exception as e:
        logger.warning(f"Failed to evict expired reports from disk: {e}")


# ---------------------------------------------------------------------------
# 7.  Pending / Error page templates
# ---------------------------------------------------------------------------

REPORT_PENDING_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Generating Assessment Report…</title>
<style>
  body { margin:0; min-height:100vh; display:flex; align-items:center; justify-content:center;
         background:#0f172a; color:#e2e8f0; font-family:Inter,-apple-system,'Segoe UI',Roboto,sans-serif; }
  .spinner { text-align:center; }
  .spinner .logo { font-size:4rem; animation: pulse 1.5s ease-in-out infinite; }
  @keyframes pulse { 0%,100% { opacity:.6; transform:scale(1); } 50% { opacity:1; transform:scale(1.1); } }
  .spinner p { margin-top:1.5rem; font-size:1.1rem; opacity:.8; }
</style>
</head>
<body>
<div class="spinner">
  <div class="logo">⚡</div>
  <p>Generating your assessment report…</p>
</div>
</body>
</html>"""

_REPORT_ERROR_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Assessment Report</title>
<style>
  body {{ margin:0; min-height:100vh; display:flex; align-items:center; justify-content:center;
         background:#0f172a; color:#e2e8f0; font-family:Inter,-apple-system,'Segoe UI',Roboto,sans-serif; }}
  .error {{ text-align:center; max-width:480px; padding: 2rem; }}
  .error .icon {{ font-size:3rem; margin-bottom:1rem; opacity: 0.6; }}
  .error h1 {{ font-size:1.3rem; margin:0 0 .5rem; color: #38bdf8; }}
  .error p {{ opacity:.8; font-size:.95rem; line-height: 1.6; }}
  .btn-back {{ display: inline-block; margin-top: 1.5rem; padding: 0.6rem 1.4rem; background: #3b82f6; color: #fff; text-decoration: none; border-radius: 8px; font-weight: 600; font-size: 0.9rem; }}
  .btn-back:hover {{ background: #2563eb; }}
</style>
</head>
<body>
<div class="error">
  <div class="icon">⚡</div>
  <h1>Assessment Report</h1>
  <p>{reason}</p>
  <a href="/#full-report" class="btn-back">Open Assessment in App</a>
</div>
</body>
</html>"""


# ---------------------------------------------------------------------------
# 8.  Routes
# ---------------------------------------------------------------------------

@router.get("/api/report/manifest")
def get_report_manifest():
    """Canonical module registry — JS fetches this on init()."""
    return REPORT_MODULES


@router.post("/api/report/prepare")
def prepare_report(request: ReportPrepareRequest):
    """Render the HTML report, cache it server-side and on disk, return report_id."""
    t0 = time.time()
    aggregator = ReportAggregator(request.snapshot)
    data = aggregator.aggregate()
    data["_lookback_days"] = request.lookback_days
    # Include bq_report_meta if present (sweep freshness)
    if "bq_report_meta" in request.snapshot:
        data["bq_report_meta"] = request.snapshot["bq_report_meta"]
    findings = FindingsSynthesizer(data).synthesize()
    nonce = secrets.token_urlsafe(16)
    html = HTMLReportRenderer(data, findings, request.lookback_days, nonce=nonce).render()
    report_id = secrets.token_urlsafe(12)
    now = time.time()
    with _cache_lock:
        _evict_expired()
        _report_cache[report_id] = ReportEntry(html=html, nonce=nonce, created=now)
        while len(_report_cache) > _CACHE_MAX:
            evicted_id, _ = _report_cache.popitem(last=False)
            try:
                (_REPORT_CACHE_DIR / f"{evicted_id}.json").unlink(missing_ok=True)
            except Exception:
                pass
        try:
            cache_file = _REPORT_CACHE_DIR / f"{report_id}.json"
            cache_file.write_text(json.dumps({"html": html, "nonce": nonce, "created": now}), encoding="utf-8")
            try:
                os.chmod(cache_file, 0o600)
            except Exception:
                pass
        except Exception as e:
            logger.warning(f"Failed to persist report to disk: {e}")

    elapsed = time.time() - t0
    logger.info(
        "Report generated: id=%s sections=6 findings=%d keys=%d duration=%.1fms",
        report_id, len(findings), len(data), elapsed * 1000,
    )
    return {"report_id": report_id}


@router.get("/report/pending", response_class=HTMLResponse)
def report_pending():
    """Spinner page shown while report POST is in flight."""
    return HTMLResponse(content=REPORT_PENDING_HTML)


@router.get("/report/error", response_class=HTMLResponse)
def report_error(reason: str = ""):
    """Error page with optional reason."""
    safe_reason = _esc(reason) if reason else "An unexpected error occurred."
    return HTMLResponse(content=_REPORT_ERROR_TEMPLATE.format(reason=safe_reason))


@router.get("/report/view/{report_id}", response_class=HTMLResponse)
def get_report(report_id: str):
    """Serve a cached report with CSP header, falling back to disk cache."""
    if not _SAFE_REPORT_ID_RE.match(report_id):
        return HTMLResponse(
            content=_REPORT_ERROR_TEMPLATE.format(
                reason="Invalid report identifier format."
            ),
            status_code=404
        )
    entry = None
    with _cache_lock:
        _evict_expired()
        entry = _report_cache.get(report_id)
        if not entry:
            try:
                cache_file = (_REPORT_CACHE_DIR / f"{report_id}.json").resolve()
                if str(cache_file).startswith(str(_REPORT_CACHE_DIR.resolve())) and cache_file.exists():
                    raw = json.loads(cache_file.read_text(encoding="utf-8"))
                    if time.time() - raw.get("created", 0) <= _CACHE_TTL_S:
                        entry = ReportEntry(html=raw["html"], nonce=raw["nonce"], created=raw["created"])
                        _report_cache[report_id] = entry
            except Exception as e:
                logger.warning(f"Failed to read report from disk cache: {e}")

        if not entry or (time.time() - entry.created) > _CACHE_TTL_S:
            _report_cache.pop(report_id, None)
            try:
                (_REPORT_CACHE_DIR / f"{report_id}.json").unlink(missing_ok=True)
            except Exception:
                pass
            return HTMLResponse(
                content=_REPORT_ERROR_TEMPLATE.format(
                    reason="This assessment report has expired or the server was restarted. Please return to the Assessment page and click <strong>Generate Report</strong> to create a fresh report."
                ),
                status_code=404
            )

    return HTMLResponse(
        content=entry.html,
        headers={
            "Content-Security-Policy": (
                f"default-src 'none'; style-src 'unsafe-inline'; "
                f"script-src 'nonce-{entry.nonce}'; "
                f"connect-src blob:; img-src data:; font-src data:; "
                f"frame-ancestors 'none'; base-uri 'none'; form-action 'none'"
            ),
            "Cache-Control": "no-store",
            "X-Content-Type-Options": "nosniff",
            "Referrer-Policy": "no-referrer",
        },
    )
