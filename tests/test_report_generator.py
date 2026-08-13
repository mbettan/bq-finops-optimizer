"""
Tests for the report_generator module.

Covers: ReportAggregator, FindingsSynthesizer, HTMLReportRenderer,
and the FastAPI routes (manifest, prepare, pending, error, view).
"""

import json
import os
import time

import pytest
from fastapi.testclient import TestClient

# Ensure auth env var is set before importing app
os.environ.setdefault("AUTH_ENFORCED_UPSTREAM", "true")

from src.main import app
from src.report_generator import (
    REPORT_MODULES,
    REPORT_RELEVANT_KEYS,
    FindingsSynthesizer,
    HTMLReportRenderer,
    ReportAggregator,
    _CACHE_MAX,
    _CACHE_TTL_S,
    _cache_lock,
    _report_cache,
)


@pytest.fixture
def client():
    return TestClient(app)


@pytest.fixture
def minimal_snapshot():
    """A minimal valid snapshot with just enough data to trigger findings."""
    return {
        "bq_org_project": "test-project-123",
        "bq_region": "US",
        "bq_job_results": json.dumps({
            "total_terabytes_processed": 100,
            "total_queries": 50000,
        }),
        "bq_slots_simulation_results": json.dumps([
            {"total_3yr": 250, "total_payg": 350, "tier": "Enterprise", "editions_cost": 250}
        ]),
        "bq_top_spenders": json.dumps([
            {"user_email": "alice@example.com", "total_bytes_billed": 100 * (1024**4), "total_tib": 100, "total_cost_usd": 625.0, "query_count": 20000}
        ]),
    }


@pytest.fixture
def full_snapshot(minimal_snapshot):
    """Snapshot with data for most modules."""
    return {
        **minimal_snapshot,
        "bq_linter_results": json.dumps([
            {"pattern": "SELECT *", "severity": "High", "query_count": 120, "description": "Scanning all columns"}
        ]),
        "bq_batch_results": json.dumps([
            {"query_pattern": "SELECT COUNT(*)", "avg_slot_ms": 50, "frequency": 200}
        ]),
        "bq_antipatterns_results": json.dumps([
            {"pattern": "Single-row INSERT", "count": 45}
        ]),
        "bq_skew_results": json.dumps([
            {"query_id": "q1", "skew_factor": 12.5}
        ]),
        "bq_storage_results": json.dumps([
            {"dataset": "analytics", "logical_gb": 100, "physical_gb": 45, "ratio": 2.2, "monthly_cost_usd": 2.25}
        ]),
        "bq_hygiene_results": json.dumps([
            {"table_id": "old_table", "days_since_last_read": 120, "size_gb": 5.3}
        ]),
        "bq_gov_results": json.dumps({"custom_quota": False, "has_custom_quota": False}),
        "bq_hbo_results": json.dumps({"recommendations": [{"query": "q1"}]}),
    }


@pytest.fixture(autouse=True)
def _clear_cache():
    """Ensure report cache is clean before each test."""
    with _cache_lock:
        _report_cache.clear()
    yield
    with _cache_lock:
        _report_cache.clear()


# ---------------------------------------------------------------------------
# ReportAggregator
# ---------------------------------------------------------------------------

class TestReportAggregator:

    def test_parse_json_strings(self, minimal_snapshot):
        agg = ReportAggregator(minimal_snapshot)
        data = agg.aggregate()
        assert data["bq_org_project"] == "test-project-123"
        assert isinstance(data["bq_job_results"], dict)
        assert data["bq_job_results"]["total_terabytes_processed"] == 100

    def test_parse_pre_parsed_objects(self):
        snap = {
            "bq_org_project": "proj",
            "bq_job_results": {"total_terabytes_processed": 50},
        }
        agg = ReportAggregator(snap)
        data = agg.aggregate()
        assert data["bq_job_results"]["total_terabytes_processed"] == 50

    def test_corrupt_json_degrades_gracefully(self):
        snap = {"bq_job_results": "not valid json {{{"}
        agg = ReportAggregator(snap)
        data = agg.aggregate()
        assert data["bq_job_results"] is None
        assert len(data["_parse_errors"]) == 1
        assert "bq_job_results" in data["_parse_errors"][0]

    def test_filters_non_allowlisted_keys(self):
        snap = {"bq_org_project": "proj", "evil_key": "should be excluded"}
        agg = ReportAggregator(snap)
        data = agg.aggregate()
        assert "evil_key" not in data

    def test_missing_keys_return_none(self):
        agg = ReportAggregator({})
        data = agg.aggregate()
        assert data["bq_job_results"] is None
        assert data["bq_org_project"] is None


# ---------------------------------------------------------------------------
# FindingsSynthesizer
# ---------------------------------------------------------------------------

class TestFindingsSynthesizer:

    def test_pricing_model_finding_triggers(self, minimal_snapshot):
        agg = ReportAggregator(minimal_snapshot)
        data = agg.aggregate()
        data["_lookback_days"] = 30
        findings = FindingsSynthesizer(data).synthesize()
        ref_ids = [f.ref_id for f in findings]
        assert "ID-01" in ref_ids
        id01 = next(f for f in findings if f.ref_id == "ID-01")
        assert id01.priority in ("High", "Critical", "Medium", "Info")
        assert id01.impact_usd_monthly is not None
        assert id01.impact_usd_monthly > 0

    def test_no_findings_on_empty_data(self):
        data = {k: None for k in REPORT_RELEVANT_KEYS}
        data["_parse_errors"] = []
        data["_lookback_days"] = 30
        findings = FindingsSynthesizer(data).synthesize()
        assert len(findings) == 0

    def test_select_star_finding(self):
        data = {k: None for k in REPORT_RELEVANT_KEYS}
        data["_parse_errors"] = []
        data["_lookback_days"] = 30
        data["bq_linter_results"] = [{"pattern": "SELECT *", "count": 5}]
        findings = FindingsSynthesizer(data).synthesize()
        ref_ids = [f.ref_id for f in findings]
        assert "ID-04" in ref_ids

    def test_guardrails_finding(self):
        data = {k: None for k in REPORT_RELEVANT_KEYS}
        data["_parse_errors"] = []
        data["_lookback_days"] = 30
        data["bq_gov_results"] = {"custom_quota": False, "has_custom_quota": False}
        findings = FindingsSynthesizer(data).synthesize()
        ref_ids = [f.ref_id for f in findings]
        assert "ID-12" in ref_ids

    def test_findings_sorted_by_score_descending(self, full_snapshot):
        agg = ReportAggregator(full_snapshot)
        data = agg.aggregate()
        data["_lookback_days"] = 30
        findings = FindingsSynthesizer(data).synthesize()
        scores = [f.score for f in findings]
        assert scores == sorted(scores, reverse=True)

    def test_pricing_not_triggered_when_savings_too_low(self):
        snap = {
            "bq_job_results": json.dumps({"total_terabytes_processed": 10}),
            "bq_slots_simulation_results": json.dumps({"editions_cost": 60}),
        }
        agg = ReportAggregator(snap)
        data = agg.aggregate()
        data["_lookback_days"] = 30
        findings = FindingsSynthesizer(data).synthesize()
        ref_ids = [f.ref_id for f in findings]
        # On-demand = 10 * 6.25 = 62.5, editions = 60, savings = 4% < 10% threshold
        assert "BQ-01" not in ref_ids

    def test_empty_list_does_not_trigger(self):
        data = {k: None for k in REPORT_RELEVANT_KEYS}
        data["_parse_errors"] = []
        data["_lookback_days"] = 30
        data["bq_linter_results"] = []  # empty list
        findings = FindingsSynthesizer(data).synthesize()
        ref_ids = [f.ref_id for f in findings]
        assert "BQ-04" not in ref_ids


# ---------------------------------------------------------------------------
# HTMLReportRenderer
# ---------------------------------------------------------------------------

class TestHTMLReportRenderer:

    def test_renders_valid_html(self, minimal_snapshot):
        agg = ReportAggregator(minimal_snapshot)
        data = agg.aggregate()
        data["_lookback_days"] = 30
        findings = FindingsSynthesizer(data).synthesize()
        html = HTMLReportRenderer(data, findings, 30, nonce="testnonce").render()
        assert html.startswith("<!DOCTYPE html>")
        assert "</html>" in html
        assert "test-project-123" in html

    def test_nonce_in_script_tag(self, minimal_snapshot):
        agg = ReportAggregator(minimal_snapshot)
        data = agg.aggregate()
        data["_lookback_days"] = 30
        html = HTMLReportRenderer(data, [], 30, nonce="abc123xyz").render()
        assert 'nonce="abc123xyz"' in html

    def test_html_escaping(self):
        snap = {"bq_org_project": '<script>alert("xss")</script>'}
        agg = ReportAggregator(snap)
        data = agg.aggregate()
        data["_lookback_days"] = 30
        html = HTMLReportRenderer(data, [], 30).render()
        assert "<script>alert" not in html
        assert "&lt;script&gt;" in html

    def test_empty_data_renders_placeholders(self):
        data = {k: None for k in REPORT_RELEVANT_KEYS}
        data["_parse_errors"] = []
        data["_lookback_days"] = 30
        html = HTMLReportRenderer(data, [], 30).render()
        assert "No data available" in html

    def test_table_capping(self):
        # Generate more than MAX_TABLE_ROWS items
        rows = [{"user_email": f"user{i}@x.com", "total_tib": i, "total_cost_usd": i * 6.25, "query_count": i * 10}
                for i in range(30)]
        data = {k: None for k in REPORT_RELEVANT_KEYS}
        data["bq_top_spenders"] = rows
        data["_parse_errors"] = []
        data["_lookback_days"] = 30
        html = HTMLReportRenderer(data, [], 30).render()
        assert "Showing top 20 of 30" in html

    def test_toolbar_json_safe(self):
        snap = {"bq_org_project": "test</script><script>evil"}
        agg = ReportAggregator(snap)
        data = agg.aggregate()
        data["_lookback_days"] = 30
        html = HTMLReportRenderer(data, [], 30).render()
        assert "</script><script>" not in html
        # The JSON-safe encoding replaces < and >
        assert "\\u003c" in html

    def test_findings_section_renders_cards(self, full_snapshot):
        agg = ReportAggregator(full_snapshot)
        data = agg.aggregate()
        data["_lookback_days"] = 30
        findings = FindingsSynthesizer(data).synthesize()
        html = HTMLReportRenderer(data, findings, 30).render()
        assert "finding-card" in html
        for f in findings:
            assert f.ref_id in html


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

class TestManifestRoute:

    def test_get_manifest(self, client):
        resp = client.get("/api/report/manifest")
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)
        assert len(data) == len(REPORT_MODULES)
        # Each entry has keys, endpoint, label
        for entry in data:
            assert "keys" in entry
            assert "endpoint" in entry
            assert "label" in entry
            assert isinstance(entry["keys"], list)


class TestPrepareRoute:

    def test_prepare_returns_report_id(self, client, minimal_snapshot):
        resp = client.post("/api/report/prepare", json={
            "snapshot": minimal_snapshot,
            "lookback_days": 30,
        })
        assert resp.status_code == 200
        data = resp.json()
        assert "report_id" in data
        assert isinstance(data["report_id"], str)
        assert len(data["report_id"]) > 0

    def test_prepare_with_empty_snapshot(self, client):
        resp = client.post("/api/report/prepare", json={
            "snapshot": {},
            "lookback_days": 30,
        })
        assert resp.status_code == 200
        data = resp.json()
        assert "report_id" in data

    def test_prepare_caches_entry(self, client, minimal_snapshot):
        resp = client.post("/api/report/prepare", json={
            "snapshot": minimal_snapshot,
            "lookback_days": 30,
        })
        report_id = resp.json()["report_id"]
        with _cache_lock:
            assert report_id in _report_cache


class TestViewRoute:

    def test_view_returns_html(self, client, minimal_snapshot):
        # First prepare
        resp = client.post("/api/report/prepare", json={
            "snapshot": minimal_snapshot,
            "lookback_days": 30,
        })
        report_id = resp.json()["report_id"]

        # Then view
        resp = client.get(f"/report/view/{report_id}")
        assert resp.status_code == 200
        assert "text/html" in resp.headers["content-type"]
        assert "<!DOCTYPE html>" in resp.text
        assert "Content-Security-Policy" in resp.headers
        assert "nonce-" in resp.headers["Content-Security-Policy"]

    def test_view_404_for_unknown_id(self, client):
        resp = client.get("/report/view/nonexistent-id")
        assert resp.status_code == 404

    def test_view_expired_report(self, client, minimal_snapshot):
        resp = client.post("/api/report/prepare", json={
            "snapshot": minimal_snapshot,
            "lookback_days": 30,
        })
        report_id = resp.json()["report_id"]
        # Manually expire it
        with _cache_lock:
            _report_cache[report_id].created = time.time() - _CACHE_TTL_S - 10
        resp = client.get(f"/report/view/{report_id}")
        assert resp.status_code == 404


class TestPendingRoute:

    def test_pending_returns_spinner(self, client):
        resp = client.get("/report/pending")
        assert resp.status_code == 200
        assert "Generating" in resp.text


class TestErrorRoute:

    def test_error_with_reason(self, client):
        resp = client.get("/report/error?reason=Something+broke")
        assert resp.status_code == 200
        assert "Something broke" in resp.text

    def test_error_escapes_html(self, client):
        resp = client.get("/report/error?reason=<script>alert(1)</script>")
        assert resp.status_code == 200
        assert "<script>alert(1)</script>" not in resp.text
        assert "&lt;script&gt;" in resp.text

    def test_error_without_reason(self, client):
        resp = client.get("/report/error")
        assert resp.status_code == 200
        assert "unexpected error" in resp.text


# ---------------------------------------------------------------------------
# Cache Eviction
# ---------------------------------------------------------------------------

class TestCacheEviction:

    def test_cache_max_entries(self, client, minimal_snapshot):
        """Cache evicts oldest entries when exceeding _CACHE_MAX."""
        report_ids = []
        for _ in range(15):
            resp = client.post("/api/report/prepare", json={
                "snapshot": minimal_snapshot,
                "lookback_days": 30,
            })
            report_ids.append(resp.json()["report_id"])

        with _cache_lock:
            assert len(_report_cache) <= _CACHE_MAX


# ---------------------------------------------------------------------------
# Integration: Full Pipeline
# ---------------------------------------------------------------------------

class TestFullPipeline:

    def test_full_report_pipeline(self, client, full_snapshot):
        """End-to-end: prepare → view → verify HTML structure."""
        resp = client.post("/api/report/prepare", json={
            "snapshot": full_snapshot,
            "lookback_days": 30,
        })
        assert resp.status_code == 200
        report_id = resp.json()["report_id"]

        resp = client.get(f"/report/view/{report_id}")
        assert resp.status_code == 200
        html = resp.text

        # Verify key sections exist
        assert "Executive Summary" in html
        assert "Scope &amp; Approach" in html
        assert "Compute &amp; Pricing Analysis" in html
        assert "Query Efficiency Analysis" in html
        assert "Storage &amp; Lifecycle Analysis" in html
        assert "Findings &amp; Action Items" in html
        assert "Appendix &amp; Glossary" in html

        # Verify findings are present
        assert "ID-01" in html  # Pricing should trigger
        assert "finding-card" in html

        # Verify CSP header
        csp = resp.headers.get("Content-Security-Policy", "")
        assert "script-src 'nonce-" in csp
        assert "connect-src blob:" in csp
