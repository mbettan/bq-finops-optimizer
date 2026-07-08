"""
Module: Focus Projects Guard Tests

Tests that verify focus_projects is correctly wired end-to-end:
1. When focus_projects is passed, @focus_projects actually appears in BQ query params
2. All param classes accept focus_projects without validation errors
3. The tiered recommendations fallback guard raises instead of silently dropping scope
"""

import pytest
from unittest.mock import MagicMock, patch, call
from fastapi.testclient import TestClient
from google.cloud import bigquery
from src.main import app

client = TestClient(app)


# ---------------------------------------------------------------------------
# Helper: extract @focus_projects from all mock_client.query() calls
# ---------------------------------------------------------------------------

def _extract_focus_params(mock_client):
    """Inspect all mock_client.query(...) calls and return any
    @focus_projects ArrayQueryParameter values found."""
    focus_values = []
    for c in mock_client.query.call_args_list:
        # query() called as query(sql, job_config=...)
        _, kwargs = c
        job_config = kwargs.get("job_config")
        if job_config is None and len(c.args) > 1:
            job_config = c.args[1]
        if job_config and hasattr(job_config, "query_parameters"):
            for p in (job_config.query_parameters or []):
                if hasattr(p, "name") and p.name == "focus_projects":
                    focus_values.append(p.values)
    return focus_values


def _extract_focus_from_run_query(mock_run_query):
    """Inspect all run_query_and_log() calls and return any
    @focus_projects ArrayQueryParameter values found."""
    focus_values = []
    for c in mock_run_query.call_args_list:
        _, kwargs = c
        qp = kwargs.get("query_parameters") or []
        for p in qp:
            if hasattr(p, "name") and p.name == "focus_projects":
                focus_values.append(p.values)
    return focus_values


# ---------------------------------------------------------------------------
# 1. Guard test: focus_projects is NOT silently dropped from queries
# ---------------------------------------------------------------------------

# Endpoints and their payloads, covering all four source files.
# We pick one representative from each file + a few special cases.
_FOCUS_TEST_PROJECTS = ["proj-alpha", "proj-beta"]

ENDPOINTS_TO_GUARD = [
    # (endpoint, payload, description)
    (
        "/api/storage/analyze",
        {"org_project_id": "valid-proj", "region": "region-us",
         "focus_projects": _FOCUS_TEST_PROJECTS},
        "Storage analyzer (main.py)",
    ),
    (
        "/api/jobs/analyze",
        {"org_project_id": "valid-proj", "region": "region-us",
         "lookback_days": 3, "limit_jobs": 100,
         "focus_projects": _FOCUS_TEST_PROJECTS},
        "Job analyzer (main.py)",
    ),
    (
        "/api/antipatterns/dml",
        {"org_project_id": "valid-proj", "region": "region-us",
         "lookback_days": 1,
         "focus_projects": _FOCUS_TEST_PROJECTS},
        "DML abuse (main.py)",
    ),
    (
        "/api/antipatterns/skew",
        {"org_project_id": "valid-proj", "region": "region-us",
         "lookback_days": 7,
         "focus_projects": _FOCUS_TEST_PROJECTS},
        "Data skew (main.py)",
    ),
    (
        "/api/bi/analyze",
        {"org_project_id": "valid-proj", "region": "region-us",
         "lookback_days": 7, "limit": 10,
         "focus_projects": _FOCUS_TEST_PROJECTS},
        "BI Engine (main.py)",
    ),
    (
        "/api/slots/analyze",
        {"org_project_id": "valid-proj", "region": "region-us",
         "lookback_days": 3, "window_minutes": 5, "percentile": 90,
         "focus_projects": _FOCUS_TEST_PROJECTS},
        "Slot analyzer (main.py)",
    ),
    (
        "/api/slots/peak",
        {"org_project_id": "valid-proj", "region": "region-us",
         "lookback_days": 7,
         "focus_projects": _FOCUS_TEST_PROJECTS},
        "Peak slots (main.py)",
    ),
    (
        "/api/governance/analyze",
        {"org_project_id": "valid-proj", "region": "region-us",
         "focus_projects": _FOCUS_TEST_PROJECTS},
        "Governance (main.py)",
    ),
    (
        "/api/users/top_spenders",
        {"org_project_id": "valid-proj", "region": "region-us",
         "admin_project_id": "valid-proj", "lookback_days": 3,
         "focus_projects": _FOCUS_TEST_PROJECTS},
        "Top spenders (main.py)",
    ),
    # Sub-routers
    (
        "/api/hbo/analyze",
        {"org_project_id": "valid-proj", "region": "region-us",
         "lookback_days": 3, "limit": 5,
         "focus_projects": _FOCUS_TEST_PROJECTS},
        "HBO analyze (hbo.py)",
    ),
    (
        "/api/hbo/summary",
        {"org_project_id": "valid-proj", "region": "region-us",
         "lookback_days": 3,
         "focus_projects": _FOCUS_TEST_PROJECTS},
        "HBO summary (hbo.py)",
    ),
    (
        "/api/cost-attribution/calculate",
        {"org_project_id": "valid-proj", "region": "region-us",
         "billing_month_start": "2026-01-01",
         "billing_month_end": "2026-01-31",
         "focus_projects": _FOCUS_TEST_PROJECTS},
        "Cost attribution (cost_attribution.py)",
    ),
    (
        "/api/fluid-scaling/estimate",
        {"org_project_id": "valid-proj", "admin_project_id": "valid-proj", "region": "region-us",
         "lookback_days": 3, "price_per_slot_hr": 0.06,
         "focus_projects": _FOCUS_TEST_PROJECTS},
        "Fluid scaling estimate (fluid_scaling.py)",
    ),
]


@pytest.mark.parametrize(
    "endpoint, payload, desc",
    ENDPOINTS_TO_GUARD,
    ids=[t[2] for t in ENDPOINTS_TO_GUARD],
)
def test_focus_param_not_silently_dropped(endpoint, payload, desc, mock_bq_all):
    """When focus_projects is in the request, @focus_projects MUST appear
    in the BigQuery query_parameters. If it doesn't, the filter was
    silently ignored — which is the exact bug this guard catches."""
    response = client.post(endpoint, json=payload)
    # Endpoint should succeed (or at least not 422 — validation must pass)
    assert response.status_code != 422, (
        f"{desc}: focus_projects was rejected by validation: {response.text[:300]}"
    )

    # Check that @focus_projects appeared in at least one query() call
    focus_found = _extract_focus_params(mock_bq_all)
    assert len(focus_found) > 0, (
        f"{desc}: focus_projects was accepted but @focus_projects never appeared "
        f"in query_parameters. The filter was silently dropped! "
        f"query() was called {mock_bq_all.query.call_count} times."
    )
    # Verify the correct values were passed
    assert _FOCUS_TEST_PROJECTS in focus_found, (
        f"{desc}: @focus_projects was present but with wrong values: {focus_found}"
    )


# ---------------------------------------------------------------------------
# 2. Schema acceptance: every param class accepts focus_projects
# ---------------------------------------------------------------------------

_ALL_ENDPOINTS_WITH_FOCUS = [
    # Covers ALL POST endpoints that should accept focus_projects
    ("/api/storage/analyze", {"org_project_id": "valid-proj", "region": "region-us"}),
    ("/api/storage/hygiene", {"org_project_id": "valid-proj", "region": "region-us", "limit": 10}),
    ("/api/jobs/analyze", {"org_project_id": "valid-proj", "region": "region-us", "lookback_days": 3, "limit_jobs": 100}),
    ("/api/antipatterns/dml", {"org_project_id": "valid-proj", "region": "region-us", "lookback_days": 1}),
    ("/api/antipatterns/mv", {"org_project_id": "valid-proj", "region": "region-us"}),
    ("/api/antipatterns/linter", {"org_project_id": "valid-proj", "region": "region-us", "lookback_days": 7, "limit_per_project": 10}),
    ("/api/antipatterns/skew", {"org_project_id": "valid-proj", "region": "region-us", "lookback_days": 7}),
    ("/api/antipatterns/batch_candidates", {"org_project_id": "valid-proj", "region": "region-us", "lookback_days": 7}),
    ("/api/bi/analyze", {"org_project_id": "valid-proj", "region": "region-us", "lookback_days": 7, "limit": 10}),
    ("/api/governance/analyze", {"org_project_id": "valid-proj", "region": "region-us"}),
    ("/api/mv/analyze", {"org_project_id": "valid-proj", "region": "region-us"}),
    ("/api/resource_warnings/analyze", {"org_project_id": "valid-proj", "region": "region-us"}),
    ("/api/slots/analyze", {"org_project_id": "valid-proj", "region": "region-us", "lookback_days": 3, "window_minutes": 5, "percentile": 90}),
    ("/api/slots/tiered_recommendations", {"org_project_id": "valid-proj", "region": "region-us", "lookback_days": 3}),
    ("/api/slots/utilization", {"org_project_id": "valid-proj", "region": "region-us", "lookback_days": 3}),
    ("/api/slots/simulate", {"org_project_id": "valid-proj", "region": "region-us", "lookback_days": 3, "max_baseline": 500, "step_size": 100}),
    ("/api/slots/peak", {"org_project_id": "valid-proj", "region": "region-us", "lookback_days": 7}),
    ("/api/slots/profiler", {"org_project_id": "valid-proj", "region": "region-us", "admin_project_id": "valid-proj", "lookback_days": 3}),
    ("/api/users/top_spenders", {"org_project_id": "valid-proj", "region": "region-us", "admin_project_id": "valid-proj", "lookback_days": 3}),
    ("/api/hbo/analyze", {"org_project_id": "valid-proj", "region": "region-us", "lookback_days": 3, "limit": 5}),
    ("/api/hbo/summary", {"org_project_id": "valid-proj", "region": "region-us", "lookback_days": 3}),
    ("/api/hbo/performance_insights", {"org_project_id": "valid-proj", "region": "region-us", "lookback_days": 3}),
    ("/api/cost-attribution/calculate", {"org_project_id": "valid-proj", "region": "region-us", "billing_month_start": "2026-01-01", "billing_month_end": "2026-01-31"}),
    ("/api/fluid-scaling/estimate", {"org_project_id": "valid-proj", "admin_project_id": "valid-proj", "region": "region-us", "lookback_days": 3, "price_per_slot_hr": 0.06}),
]


@pytest.mark.parametrize(
    "endpoint, base_payload",
    _ALL_ENDPOINTS_WITH_FOCUS,
    ids=[e[0] for e in _ALL_ENDPOINTS_WITH_FOCUS],
)
def test_focus_projects_accepted_by_schema(endpoint, base_payload, mock_bq_all):
    """Every endpoint MUST accept focus_projects without returning 422.
    This catches missing FocusMixin on a param class."""
    payload = {**base_payload, "focus_projects": ["test-project-1"]}
    response = client.post(endpoint, json=payload)
    assert response.status_code != 422, (
        f"{endpoint}: focus_projects rejected by schema validation — "
        f"likely missing FocusMixin: {response.text[:300]}"
    )


@pytest.mark.parametrize(
    "endpoint, base_payload",
    _ALL_ENDPOINTS_WITH_FOCUS,
    ids=[e[0] for e in _ALL_ENDPOINTS_WITH_FOCUS],
)
def test_empty_focus_projects_accepted(endpoint, base_payload, mock_bq_all):
    """Empty focus_projects (org-wide mode) must also be accepted."""
    payload = {**base_payload, "focus_projects": []}
    response = client.post(endpoint, json=payload)
    assert response.status_code != 422, (
        f"{endpoint}: empty focus_projects rejected by schema: {response.text[:300]}"
    )


# ---------------------------------------------------------------------------
# 3. Tiered recommendations fallback guard
# ---------------------------------------------------------------------------

def test_tiered_recs_fallback_guard_raises_when_focus_active(mock_bq_all):
    """When focus_projects is active and the Org query fails,
    the endpoint MUST raise — never silently fall back to project-level."""
    # Make the first query raise an access error
    mock_bq_all.query.side_effect = Exception("Access Denied: Dataset not found")

    payload = {
        "org_project_id": "valid-proj",
        "region": "region-us",
        "lookback_days": 3,
        "focus_projects": ["proj-alpha"],
    }
    response = client.post("/api/slots/tiered_recommendations", json=payload)
    # Should get 500 (re-raised), NOT 200 with silently unscoped data
    assert response.status_code == 500, (
        f"Expected 500 (re-raise) but got {response.status_code}. "
        f"The fallback guard may have silently dropped focus scope!"
    )


def test_tiered_recs_fallback_allowed_without_focus(mock_bq_all):
    """Without focus_projects, the endpoint SHOULD fall back to
    project-level on Access Denied (existing behavior preserved)."""
    from tests.conftest import _make_mock_job

    call_count = 0
    def side_effect(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise Exception("Access Denied: Dataset not found")
        return _make_mock_job()

    mock_bq_all.query.side_effect = side_effect

    payload = {
        "org_project_id": "valid-proj",
        "region": "region-us",
        "lookback_days": 3,
        # No focus_projects — fallback should work
    }
    response = client.post("/api/slots/tiered_recommendations", json=payload)
    # Should succeed via fallback
    assert response.status_code == 200, (
        f"Expected 200 (fallback) but got {response.status_code}: {response.text[:300]}"
    )


# ---------------------------------------------------------------------------
# 4. Validation rejection tests (via endpoint, not unit)
# ---------------------------------------------------------------------------

def test_focus_projects_rejects_unsafe_chars_via_endpoint(mock_bq_all):
    """SQL-injection-style project IDs must be rejected at the endpoint level."""
    payload = {
        "org_project_id": "valid-proj",
        "region": "region-us",
        "focus_projects": ["valid-proj", "'; DROP TABLE --"],
    }
    response = client.post("/api/storage/analyze", json=payload)
    assert response.status_code == 400, (
        f"Expected 400 for unsafe focus_projects chars, got {response.status_code}"
    )


def test_focus_projects_cap_exceeded_via_endpoint(mock_bq_all):
    """More than 50 focus projects must be rejected."""
    payload = {
        "org_project_id": "valid-proj",
        "region": "region-us",
        "focus_projects": [f"project-{i}" for i in range(51)],
    }
    response = client.post("/api/storage/analyze", json=payload)
    assert response.status_code == 400, (
        f"Expected 400 for >50 focus_projects, got {response.status_code}"
    )


def test_focus_projects_dummy_rejected_via_endpoint(mock_bq_all):
    """Dummy project IDs in focus_projects must be rejected."""
    payload = {
        "org_project_id": "valid-proj",
        "region": "region-us",
        "focus_projects": ["your-project-id"],
    }
    response = client.post("/api/storage/analyze", json=payload)
    assert response.status_code == 400, (
        f"Expected 400 for dummy focus_projects, got {response.status_code}"
    )
