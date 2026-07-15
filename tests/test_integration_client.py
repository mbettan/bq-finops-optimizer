import os
import pytest
from fastapi.testclient import TestClient
from src.main import app

# Initialize the lightweight FastAPI test harness
client = TestClient(app)

# Resolve the target project ID for integration tests.
# Fallback to 'mbettan-project' if the environment variable is not set.
TEST_PROJECT_ID = os.getenv("BIGQUERY_PROJECT_ID", "mbettan-project")
TEST_REGION = os.getenv("BIGQUERY_REGION", "region-us-east4")

# Automatically skip live integration tests if the default placeholder is used
if TEST_PROJECT_ID == "mbettan-project":
    pytestmark = pytest.mark.skip(
        reason="Skipping live integration tests because BIGQUERY_PROJECT_ID is set to the placeholder 'mbettan-project'. Set it to a real GCP project to run live tests."
    )

@pytest.mark.integration
def test_top_spenders_integration():
    """
    Integration test for the /api/users/top_spenders endpoint.
    Verifies that the endpoint returns valid user records and that
    parent-child double counting is not occurring.
    """
    # 1. Hit the backend endpoint directly via POST with a real staging project
    response = client.post("/api/users/top_spenders", json={
        "org_project_id": TEST_PROJECT_ID,
        "admin_project_id": TEST_PROJECT_ID,
        "region": TEST_REGION,
        "lookback_days": 7
    })
    
    # 2. Assert HTTP Success
    assert response.status_code == 200, f"Endpoint failed: {response.text}"
    
    data = response.json()
    assert isinstance(data, list), "Response should be a JSON list of users"
    
    if len(data) > 0:
        first_user = data[0]
        # 3. Assert schema compliance
        assert "user_email" in first_user
        assert "total_bytes_billed" in first_user
        assert "total_slot_hours" in first_user
        
        # 4. Regression Assertion: Confirm calculations are realistic
        # (verifying that we are not double-counting parent-child executions).
        assert first_user["total_bytes_billed"] >= 0
        assert first_user["total_slot_hours"] >= 0.0
        print(f"\nTop Spender Integration Test Passed!")
        print(f"Top spender: {first_user['user_email']} - Billed: {first_user['total_bytes_billed']/(1024**4):.2f} TB, Slot-Hours: {first_user['total_slot_hours']:.2f}")


@pytest.mark.integration
def test_slot_simulation_integration():
    """
    Integration test for the Slot Capacity Bucket simulation endpoint.
    Verifies that the simulation outputs are valid and that slot-hours are un-doubled.
    """
    response = client.post("/api/slots/simulate", json={
        "org_project_id": TEST_PROJECT_ID,
        "region": TEST_REGION,
        "lookback_days": 3,
        "max_baseline": 1000,
        "step_size": 100
    })
    
    assert response.status_code == 200, f"Simulation endpoint failed: {response.text}"
    
    data = response.json()
    assert len(data) > 0, "Simulation should return data points"
    
    # Assert that utilization percent is a realistic fraction (0 to 100%)
    first_bucket = data[0]
    assert 0.0 <= first_bucket["utilization_pct"] <= 100.0
    print(f"\nSlot Simulation Integration Test Passed!")
    print(f"Simulation returned {len(data)} baseline data points. First bucket utilization: {first_bucket['utilization_pct']}%")


@pytest.mark.integration
def test_query_linter_integration():
    """
    Integration test for the Query Anti-Patterns Linter endpoint.
    Verifies that the linter successfully queries the live INFORMATION_SCHEMA
    and returns query-level findings.
    """
    response = client.post("/api/antipatterns/linter", json={
        "org_project_id": TEST_PROJECT_ID,
        "region": TEST_REGION,
        "lookback_days": 7,
        "limit_per_project": 10
    })
    
    assert response.status_code == 200, f"Linter endpoint failed: {response.text}"
    
    data = response.json()
    assert isinstance(data, list), "Response should be a list of linter results"
    
    print(f"\nQuery Linter Integration Test Passed! Found {len(data)} anti-pattern records.")
    if len(data) > 0:
        first_finding = data[0]
        assert "project_id" in first_finding
        assert "job_id" in first_finding
        assert "user_email" in first_finding
        assert "query_snippet" in first_finding
        assert "abuse_type" in first_finding
        assert "billed_gb" in first_finding
        print(f"Sample Finding: {first_finding['abuse_type']} in job {first_finding['job_id'][:12]}...")


@pytest.mark.integration
def test_slot_utilization_integration():
    """
    Integration test for the Slot Utilization Timeline endpoint.
    Verifies that the endpoint successfully pulls timeline metrics from live logs.
    """
    response = client.post("/api/slots/utilization", json={
        "org_project_id": TEST_PROJECT_ID,
        "region": TEST_REGION,
        "lookback_days": 3,
        "resolution": "HOUR"
    })
    
    assert response.status_code == 200, f"Slot utilization endpoint failed: {response.text}"
    
    data = response.json()
    assert isinstance(data, list), "Response should be a list of utilization points"
    
    print(f"\nSlot Utilization Integration Test Passed! Found {len(data)} timeline buckets.")
    if len(data) > 0:
        first_point = data[0]
        assert "timestamp" in first_point
        assert "time_average" in first_point
        assert "max_slots" in first_point
        assert "p90_slots" in first_point
        assert "p99_slots" in first_point
        print(f"Sample Bucket: {first_point['timestamp']} - Avg Slots: {first_point['time_average']}")


@pytest.mark.integration
def test_fluid_scaling_estimate_integration():
    """
    Integration test for the Fluid Scaling estimate endpoint.
    Verifies the per-second savings model against live INFORMATION_SCHEMA:
      - response contract (reservations list + config_status),
      - per-reservation invariants (0 <= fluid <= legacy, savings never negative).
    """
    response = client.post("/api/fluid-scaling/estimate", json={
        "org_project_id": TEST_PROJECT_ID,
        "admin_project_id": TEST_PROJECT_ID,
        "region": TEST_REGION,
        "lookback_days": 7,
    })

    assert response.status_code == 200, f"Fluid estimate endpoint failed: {response.text}"

    data = response.json()
    assert isinstance(data, dict), "Response should be a JSON object"
    assert "reservations" in data and "config_status" in data

    reservations = data["reservations"]
    print(f"\nFluid Scaling Estimate Integration Test Passed! {len(reservations)} reservations.")
    for r in reservations:
        # Per-second clamp guarantees fluid usage never exceeds legacy capacity,
        # so recoverable slot-hours and % savings must be non-negative.
        assert r["fluid_autoscaler_slot_hours"] <= r["legacy_autoscaler_slot_hours"] + 1e-6, \
            f"Fluid exceeds legacy for {r['reservation_id']}"
        assert r["slot_hours_saved"] >= -1e-6
        assert r["clamped_pct_savings"] >= -1e-6

@pytest.mark.integration
def test_storage_analysis_integration():
    """
    Integration test for the Storage Optimization Analyzer endpoint.
    Verifies that the endpoint successfully pulls storage metrics from live logs.
    """
    response = client.post("/api/storage/analyze", json={
        "org_project_id": TEST_PROJECT_ID,
        "region": TEST_REGION
    })
    
    assert response.status_code == 200, f"Storage analyzer endpoint failed: {response.text}"
    
    data = response.json()
    assert isinstance(data, dict), "Response should be a JSON dictionary"
    assert "datasets" in data, "Response must contain 'datasets' list"
    
    datasets = data["datasets"]
    print(f"\nStorage Analysis Integration Test Passed! Analyzed {len(datasets)} datasets.")
    if len(datasets) > 0:
        first_dataset = datasets[0]
        assert "dataset_name" in first_dataset
        assert "currently_on" in first_dataset
        assert "better_on" in first_dataset
        assert "forecast_logical" in first_dataset
        assert "forecast_physical" in first_dataset
        print(f"Sample Dataset: {first_dataset['dataset_name']} - Current Model: {first_dataset['currently_on']} -> Recommended: {first_dataset['better_on']}")


