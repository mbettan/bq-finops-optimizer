import pytest
from fastapi.testclient import TestClient
from src.main import app

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
    ("/api/ai/analyze", {"org_project_id": "proj`ect", "region": "region-us"}),
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
    """Endpoints checking edition must reject invalid values with 422."""
    response = client.post(endpoint, json=payload)
    assert response.status_code == 422
    error_detail = response.json()["detail"][0]
    assert "edition" in error_detail["loc"]


RESOLUTION_TEST_CASES = [
    ("/api/slots/utilization", {"org_project_id": "valid-proj", "region": "region-us", "resolution": "BAD"}),
]

@pytest.mark.parametrize("endpoint, payload", RESOLUTION_TEST_CASES)
def test_endpoints_reject_invalid_resolution(endpoint, payload):
    """Endpoints checking resolution must reject invalid values with 422."""
    response = client.post(endpoint, json=payload)
    assert response.status_code == 422
    error_detail = response.json()["detail"][0]
    assert "resolution" in error_detail["loc"]





DUMMY_PROJECT_TEST_CASES = [
    ("/api/storage/analyze", {"org_project_id": "example-sandbox", "region": "region-us"}),
    ("/api/slots/analyze", {"org_project_id": "your-project-id", "region": "region-us"}),
    ("/api/slots/tiered_recommendations", {"org_project_id": "example-sandbox", "region": "region-us"}),
    ("/api/slots/utilization", {"org_project_id": "your-project-id", "region": "region-us"}),
    ("/api/slots/simulate", {"org_project_id": "example-sandbox", "region": "region-us"}),
    ("/api/slots/fluid_simulation", {"org_project_id": "your-project-id", "region": "region-us"}),
    ("/api/slots/actual_provisioning", {"org_project_id": "example-sandbox", "region": "region-us"}),
    ("/api/slots/peak", {"org_project_id": "your-project-id", "region": "region-us"}),
    ("/api/slots/profiler", {"org_project_id": "example-sandbox", "region": "region-us"}),
    ("/api/slots/profiler/queries", {"org_project_id": "your-project-id", "region": "region-us"}),
    ("/api/users/top_spenders", {"org_project_id": "example-sandbox", "region": "region-us"}),
    ("/api/fluid-scaling/status", {"org_project_id": "example-sandbox", "region": "region-us"}),
    ("/api/fluid-scaling/estimate", {"org_project_id": "your-project-id", "region": "region-us"}),
]

@pytest.mark.parametrize("endpoint, payload", DUMMY_PROJECT_TEST_CASES)
def test_endpoints_reject_dummy_project_id(endpoint, payload):
    """Every endpoint must reject dummy/placeholder project IDs and return 400."""
    response = client.post(endpoint, json=payload)
    assert response.status_code == 400
    assert "dummy placeholder" in response.json()["detail"]


def _detail_to_message(detail, fallback="Unknown error"):
    if not detail:
        return fallback
    if isinstance(detail, str):
        return detail
    if isinstance(detail, list):
        parts = []
        for d in detail:
            if isinstance(d, str):
                parts.append(d)
            elif isinstance(d, dict) and "msg" in d:
                loc = d.get("loc", [])
                loc_str = " → ".join(str(l) for l in loc) if isinstance(loc, list) else ""
                parts.append(f"{loc_str}: {d['msg']}" if loc_str else d["msg"])
            else:
                parts.append(str(d))
        return "; ".join(parts)
    return str(detail)

def test_region_injection_blocked():
    """Verify that SQL-injection-like region values are rejected.
    Returns 422 because region validation moved to a Pydantic @field_validator."""
    payload = {
        "org_project_id": "valid-proj",
        "region": "region-us`.evil"
    }
    response = client.post("/api/ai/analyze", json=payload)
    assert response.status_code == 422
    assert "Invalid region" in _detail_to_message(response.json().get("detail"))

def test_region_canonicalisation_and_exotic_regions():
    """Ensure canonicalisation works and exotic regions are accepted."""
    from src.utils import RegionMixin
    from src.hbo import HBOCommonParams
    from src.cost_attribution import CostAttributionParams
    from src.fluid_scaling import FluidScalingParams
    from src.main import StorageParams, AIParams
    
    classes = [RegionMixin, HBOCommonParams, CostAttributionParams, FluidScalingParams, StorageParams, AIParams]
    for cls in classes:
        kwargs = {
            "org_project_id": "valid",
            "billing_month_start": "2026-09-01",
            "billing_month_end": "2026-09-30"
        }
        # Only pass kwargs that exist in the model
        valid_kwargs = {k: v for k, v in kwargs.items() if k in cls.model_fields}
        
        # Check canonicalisation
        assert cls(region="US", **valid_kwargs).region == "region-us"
        assert cls(region="us", **valid_kwargs).region == "region-us"
        assert cls(region="region-us", **valid_kwargs).region == "region-us"
        
        # Check exotic region is accepted
        exotic = "region-mars-central1"
        assert cls(region=exotic, **valid_kwargs).region == exotic
        
        # Check injection is rejected
        from pydantic import ValidationError
        with pytest.raises(ValidationError) as exc_info:
            cls(region="region-us`.evil", **valid_kwargs)
        assert "Invalid region" in str(exc_info.value)

def test_limit_validation_bounds():
    """Verify that limit must be between 1 and 100."""
    # Under lower bound (0)
    payload = {
        "org_project_id": "valid-proj",
        "region": "region-us",
        "limit": 0
    }
    response = client.post("/api/ai/analyze", json=payload)
    assert response.status_code == 422  # Pydantic validation error

    # Over upper bound (101)
    payload["limit"] = 101
    response = client.post("/api/ai/analyze", json=payload)
    assert response.status_code == 422  # Pydantic validation error


# ---------------------------------------------------------------------------
# AI Doctor strategy-aware injection tests
# ---------------------------------------------------------------------------

AI_DOCTOR_STRATEGIES = ["composite", "cumulative_cost", "execution_frequency", "memory_spill", "slot_ms"]


@pytest.mark.parametrize("strategy", AI_DOCTOR_STRATEGIES)
def test_ai_doctor_region_injection_all_strategies(strategy):
    """SQL-injection-like region values must be rejected across all strategies."""
    response = client.post("/api/ai/analyze", json={
        "org_project_id": "valid-proj",
        "region": "region-us`.evil",
        "discovery_strategy": strategy,
    })
    assert response.status_code == 422
    assert "Invalid region" in _detail_to_message(response.json().get("detail"))


@pytest.mark.parametrize("strategy", AI_DOCTOR_STRATEGIES)
def test_ai_doctor_org_project_injection_with_strategy(strategy):
    """Backtick injection in org_project_id must be rejected for every strategy."""
    response = client.post("/api/ai/analyze", json={
        "org_project_id": "proj`ect",
        "region": "region-us",
        "discovery_strategy": strategy,
    })
    assert response.status_code == 400
    assert "Invalid org_project_id" in response.json()["detail"]

