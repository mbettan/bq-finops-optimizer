import pytest
from fastapi.testclient import TestClient
from main import app

client = TestClient(app)

# Map endpoints to a baseline valid payload (using minimal fields) and an injection payload.
# If they pass validation, they would normally proceed to BigQuery client initialization
# (which might fail or succeed depending on credentials, but validation happens FIRST).
SECURITY_TEST_CASES = [
    ("/api/storage/analyze", {"org_project_id": "proj`ect", "region": "region-us"}),
    ("/api/jobs/analyze", {"org_project_id": "proj`ect", "region": "region-us"}),
    ("/api/storage/hygiene", {"org_project_id": "proj`ect", "region": "region-us"}),
    ("/api/antipatterns/dml", {"org_project_id": "proj`ect", "region": "region-us"}),
    ("/api/antipatterns/mv", {"org_project_id": "proj`ect", "region": "region-us"}),
    ("/api/antipatterns/linter", {"org_project_id": "proj`ect", "region": "region-us"}),
    ("/api/antipatterns/skew", {"org_project_id": "proj`ect", "region": "region-us"}),
    ("/api/antipatterns/batch_candidates", {"org_project_id": "proj`ect", "region": "region-us"}),
    ("/api/ai/analyze", {"org_project_id": "proj`ect", "region": "region-us", "model_name": "proj.ds.model"}),
    ("/api/bi/analyze", {"org_project_id": "proj`ect", "region": "region-us"}),
    ("/api/governance/analyze", {"org_project_id": "proj`ect", "region": "region-us"}),
    ("/api/mv/analyze", {"org_project_id": "proj`ect", "region": "region-us"}),
    ("/api/resource_warnings/analyze", {"org_project_id": "proj`ect", "region": "region-us"}),
    ("/api/slots/analyze", {"org_project_id": "proj`ect", "region": "region-us"}),
    ("/api/slots/tiered_recommendations", {"org_project_id": "proj`ect", "region": "region-us"}),
    ("/api/slots/utilization", {"org_project_id": "proj`ect", "region": "region-us"}),
    ("/api/slots/simulate", {"org_project_id": "proj`ect", "region": "region-us"}),
    ("/api/slots/fluid_simulation", {"org_project_id": "proj`ect", "region": "region-us"}),
    ("/api/slots/actual_provisioning", {"org_project_id": "proj`ect", "region": "region-us"}),
    ("/api/slots/peak", {"org_project_id": "proj`ect", "region": "region-us"}),
    ("/api/slots/profiler", {"org_project_id": "proj`ect", "region": "region-us"}),
    ("/api/slots/profiler/queries", {"org_project_id": "proj`ect", "region": "region-us"}),
    ("/api/users/top_spenders", {"org_project_id": "proj`ect", "region": "region-us"}),
]

@pytest.mark.parametrize("endpoint, payload", SECURITY_TEST_CASES)
def test_endpoints_reject_invalid_org_project_id(endpoint, payload):
    """Every endpoint must validate org_project_id against SQL injection patterns and return 400."""
    response = client.post(endpoint, json=payload)
    assert response.status_code == 400
    assert "Invalid org_project_id" in response.json()["detail"]


EDITION_TEST_CASES = [
    ("/api/slots/actual_provisioning", {"org_project_id": "valid-proj", "region": "region-us", "edition": "BAD"}),
]

@pytest.mark.parametrize("endpoint, payload", EDITION_TEST_CASES)
def test_endpoints_reject_invalid_edition(endpoint, payload):
    """Endpoints checking edition must reject invalid values with 400."""
    response = client.post(endpoint, json=payload)
    assert response.status_code == 400
    assert "Invalid edition" in response.json()["detail"]


RESOLUTION_TEST_CASES = [
    ("/api/slots/utilization", {"org_project_id": "valid-proj", "region": "region-us", "resolution": "BAD"}),
]

@pytest.mark.parametrize("endpoint, payload", RESOLUTION_TEST_CASES)
def test_endpoints_reject_invalid_resolution(endpoint, payload):
    """Endpoints checking resolution must reject invalid values with 400."""
    response = client.post(endpoint, json=payload)
    assert response.status_code == 400
    assert "Invalid resolution" in response.json()["detail"]


MODEL_NAME_TEST_CASES = [
    ("/api/ai/analyze", {"org_project_id": "valid-proj", "region": "region-us", "model_name": "invalid-model-format"}),
]

@pytest.mark.parametrize("endpoint, payload", MODEL_NAME_TEST_CASES)
def test_endpoints_reject_invalid_model_name(endpoint, payload):
    """Endpoints checking model_name must reject invalid values with 400."""
    response = client.post(endpoint, json=payload)
    assert response.status_code == 400
    assert "Invalid model_name" in response.json()["detail"]
