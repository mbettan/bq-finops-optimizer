"""
Module 6: Smoke Integration Tests

Lightweight end-to-end tests that hit every endpoint with valid payloads
and a mocked BigQuery layer. Verifies that each endpoint:
1. Returns HTTP 200
2. Returns valid JSON
3. Does NOT crash with empty BQ results

These replace the manual "open frontend and click every tab" workflow.
Run time: ~2 seconds (no real BQ calls).
"""

import pytest
from unittest.mock import MagicMock
from fastapi.testclient import TestClient
from src.main import app

client = TestClient(app)


# ---------------------------------------------------------------------------
# Dashboard GET endpoints (no BQ calls — return stub data)
# ---------------------------------------------------------------------------

class TestDashboardSmoke:
    """Dashboard endpoints return hardcoded stubs — zero mocking needed."""

    def test_kpis_returns_200(self):
        response = client.get("/api/dashboard/kpis")
        assert response.status_code == 200
        data = response.json()
        assert "mtdSpend" in data
        assert "stub" in data
        assert data["stub"] is True

    def test_opportunities_returns_200(self):
        response = client.get("/api/dashboard/opportunities")
        assert response.status_code == 200
        assert isinstance(response.json(), list)

    def test_top_projects_returns_200(self):
        response = client.get("/api/dashboard/top-projects")
        assert response.status_code == 200
        assert isinstance(response.json(), list)

    def test_anomalies_returns_200(self):
        response = client.get("/api/dashboard/anomalies")
        assert response.status_code == 200
        assert isinstance(response.json(), list)

    def test_kpi_response_schema_complete(self):
        """Verify all expected KPI fields are present."""
        data = client.get("/api/dashboard/kpis").json()
        expected_fields = {
            "mtdSpend", "mtdSpendDelta", "forecastSpend",
            "lastMonthSpend", "potentialSavings",
            "opportunityCount", "anomalyCount", "stub",
        }
        assert expected_fields <= set(data.keys())


# ---------------------------------------------------------------------------
# Cost Attribution config endpoint (file-based, no BQ)
# ---------------------------------------------------------------------------

class TestCostAttributionConfigSmoke:
    def test_get_config_returns_200(self):
        response = client.get("/api/cost-attribution/config")
        assert response.status_code == 200
        data = response.json()
        assert "waste_rule" in data


# ---------------------------------------------------------------------------
# POST endpoints with mocked BQ — sorted by module
# ---------------------------------------------------------------------------

# Shared base payloads
_BASE = {"org_project_id": "valid-proj", "region": "region-us"}
_BASE_WITH_ADMIN = {**_BASE, "admin_project_id": "valid-proj"}

# Each tuple: (endpoint_path, payload, description)
SIMPLE_POST_ENDPOINTS = [
    # --- Storage ---
    (
        "/api/storage/analyze",
        {**_BASE},
        "Storage billing model analyzer",
    ),
    (
        "/api/storage/hygiene",
        {**_BASE, "limit": 10},
        "Storage hygiene analyzer",
    ),
    # --- Job Analysis ---
    (
        "/api/jobs/analyze",
        {**_BASE, "lookback_days": 3, "limit_jobs": 100},
        "On-demand vs editions job analyzer",
    ),
    # --- Anti-patterns ---
    (
        "/api/antipatterns/dml",
        {**_BASE, "lookback_days": 1},
        "DML abuse detector",
    ),
    (
        "/api/antipatterns/mv",
        {**_BASE},
        "MV opportunity detector",
    ),
    (
        "/api/antipatterns/linter",
        {**_BASE, "lookback_days": 7, "limit_per_project": 10},
        "Query anti-pattern linter",
    ),
    (
        "/api/antipatterns/skew",
        {**_BASE, "lookback_days": 7},
        "Data skew detector",
    ),
    (
        "/api/antipatterns/batch_candidates",
        {**_BASE, "lookback_days": 7},
        "Batch candidate detector",
    ),
    # --- BI Engine ---
    (
        "/api/bi/analyze",
        {**_BASE, "lookback_days": 7, "limit": 10},
        "BI Engine analyzer",
    ),
    # --- Governance ---
    (
        "/api/governance/analyze",
        {**_BASE},
        "Governance analyzer",
    ),
    # --- MV / Resource Warnings ---
    (
        "/api/mv/analyze",
        {**_BASE},
        "Materialized view analyzer",
    ),
    (
        "/api/resource_warnings/analyze",
        {**_BASE},
        "Resource warning analyzer",
    ),
    # --- Slots ---
    (
        "/api/slots/analyze",
        {**_BASE, "lookback_days": 3, "window_minutes": 5, "percentile": 90},
        "Slot analyzer",
    ),
    (
        "/api/slots/tiered_recommendations",
        {**_BASE, "lookback_days": 3},
        "Tiered slot recommendations",
    ),
    (
        "/api/slots/utilization",
        {**_BASE, "lookback_days": 3},
        "Slot utilization timeline",
    ),
    (
        "/api/slots/simulate",
        {**_BASE, "lookback_days": 3, "max_baseline": 500, "step_size": 100},
        "Slot simulation",
    ),
    (
        "/api/slots/peak",
        {**_BASE, "lookback_days": 7},
        "Peak slots",
    ),
    (
        "/api/slots/profiler",
        {**_BASE_WITH_ADMIN, "lookback_days": 3},
        "Workload profiler",
    ),
    (
        "/api/slots/actual_provisioning",
        {**_BASE_WITH_ADMIN, "lookback_days": 3, "edition": "ENTERPRISE"},
        "Actual provisioning",
    ),
    # --- Users ---
    (
        "/api/users/top_spenders",
        {**_BASE_WITH_ADMIN, "lookback_days": 3},
        "Top spenders",
    ),
    # --- HBO ---
    (
        "/api/hbo/analyze",
        {**_BASE, "lookback_days": 3, "limit": 5},
        "HBO analyze",
    ),
    (
        "/api/hbo/summary",
        {**_BASE, "lookback_days": 3},
        "HBO summary",
    ),
    (
        "/api/hbo/performance_insights",
        {**_BASE, "lookback_days": 3},
        "HBO performance insights",
    ),
    # --- Cost Attribution ---
    (
        "/api/cost-attribution/calculate",
        {
            "billing_month_start": "2026-01-01",
            "billing_month_end": "2026-01-31",
            "org_project_id": "valid-proj",
            "region": "region-us",
        },
        "Cost attribution calculator",
    ),
    # --- Fluid Scaling ---
    (
        "/api/fluid-scaling/status",
        {**_BASE},
        "Fluid scaling status",
    ),
]


@pytest.mark.parametrize(
    "endpoint, payload, desc",
    SIMPLE_POST_ENDPOINTS,
    ids=[t[2] for t in SIMPLE_POST_ENDPOINTS],
)
def test_endpoint_returns_200_with_empty_bq(endpoint, payload, desc, mock_bq_all):
    """
    Every endpoint should return 200 when BQ returns empty results.
    This is the smoke test that replaces manual frontend clicking.
    """
    response = client.post(endpoint, json=payload)
    assert response.status_code == 200, (
        f"{desc} ({endpoint}) returned {response.status_code}: {response.text[:500]}"
    )
    # Verify the response is valid JSON
    data = response.json()
    assert data is not None


# ---------------------------------------------------------------------------
# HBO Status — needs multi-project resolution mocking
# ---------------------------------------------------------------------------

def test_hbo_status_returns_200(mock_bq_all):
    """HBO status checks projects in threads; requires mock to handle list_datasets."""
    # Override list_datasets to return one mock dataset
    mock_dataset = MagicMock()
    mock_dataset.project = "valid-proj"
    mock_bq_all.list_datasets.return_value = [mock_dataset]

    response = client.post("/api/hbo/status", json={
        "org_project_id": "valid-proj",
        "region": "region-us",
        "lookback_days": 3,
    })
    assert response.status_code == 200
    assert isinstance(response.json(), list)


# ---------------------------------------------------------------------------
# Fluid Scaling Estimate — needs capacity/usage DataFrames
# ---------------------------------------------------------------------------

def test_fluid_estimate_returns_200(mock_bq_all):
    """Fluid estimate needs at least empty DataFrames from BQ."""
    response = client.post("/api/fluid-scaling/estimate", json={
        "org_project_id": "valid-proj",
        "admin_project_id": "valid-proj",
        "region": "region-us",
        "lookback_days": 3,
        "price_per_slot_hr": 0.06,
    })
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, dict)


# ---------------------------------------------------------------------------
# Fluid Simulation — needs query results with specific row shape
# ---------------------------------------------------------------------------

def test_fluid_simulation_returns_200_empty(mock_bq_all):
    """Fluid simulation with no matching jobs should return empty patterns."""
    response = client.post("/api/slots/fluid_simulation", json={
        "org_project_id": "valid-proj",
        "region": "region-us",
        "lookback_days": 3,
        "edition_slot_hr_rate": 0.06,
        "cooldown_window": 60,
    })
    assert response.status_code == 200
    data = response.json()
    assert "patterns" in data
    assert "disclaimer" in data


# ---------------------------------------------------------------------------
# Profiler Queries — needs query results
# ---------------------------------------------------------------------------

def test_profiler_queries_returns_200(mock_bq_all):
    """Profiler queries endpoint returns list of jobs."""
    response = client.post("/api/slots/profiler/queries", json={
        "org_project_id": "valid-proj",
        "admin_project_id": "valid-proj",
        "region": "region-us",
        "lookback_days": 3,
    })
    assert response.status_code == 200


# ---------------------------------------------------------------------------
# Response Schema Spot-Checks
# ---------------------------------------------------------------------------

class TestResponseSchemas:
    """Verify key response fields exist on selected endpoints."""

    def test_kpi_has_stub_flag(self):
        data = client.get("/api/dashboard/kpis").json()
        assert "stub" in data
        assert isinstance(data["stub"], bool)

    def test_storage_returns_dict_with_datasets(self, mock_bq_all):
        response = client.post("/api/storage/analyze", json=_BASE)
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, dict)
        assert "datasets" in data
        assert isinstance(data["datasets"], list)

    def test_hbo_summary_has_totals(self, mock_bq_all):
        response = client.post("/api/hbo/summary", json=_BASE)
        data = response.json()
        assert response.status_code == 200
        assert "total_optimized_jobs" in data
        assert "total_saved_slot_hours" in data
        assert "total_estimated_savings_usd" in data

    def test_governance_has_expiration_and_filter_issues(self, mock_bq_all):
        response = client.post("/api/governance/analyze", json=_BASE)
        data = response.json()
        assert response.status_code == 200
        assert "expiration_issues" in data
        assert "filter_issues" in data

    def test_fluid_sim_has_patterns_and_disclaimer(self, mock_bq_all):
        response = client.post("/api/slots/fluid_simulation", json={
            **_BASE, "lookback_days": 3,
        })
        assert response.status_code == 200
        data = response.json()
        assert "patterns" in data
        assert "disclaimer" in data
        assert isinstance(data["disclaimer"], str)
