import pytest
from unittest.mock import patch
from pydantic import BaseModel
from typing import Optional
from src.utils import get_max_bytes_billed, DEFAULT_MAX_BYTES_BILLED
from fastapi.testclient import TestClient
from src.main import app

client = TestClient(app)

class MockParams(BaseModel):
    max_bytes_billed_gb: Optional[int] = None

def test_get_max_bytes_billed_none():
    # Calling helper with None or empty params should fallback to default 800 GiB
    assert get_max_bytes_billed(None) == DEFAULT_MAX_BYTES_BILLED
    params = MockParams()
    assert get_max_bytes_billed(params) == DEFAULT_MAX_BYTES_BILLED

def test_get_max_bytes_billed_valid():
    # Calling helper with valid input (e.g. 400 GiB) should return 400 GiB in bytes
    params = MockParams(max_bytes_billed_gb=400)
    assert get_max_bytes_billed(params) == 400 * 1024**3

def test_get_max_bytes_billed_clamped_min():
    # If the user enters a negative value or zero, it is clamped to min 1 GiB or default.
    # Note: 0 falls back to default 800 GiB, while negative values clamp to 1 GiB.
    params_neg = MockParams(max_bytes_billed_gb=-5)
    assert get_max_bytes_billed(params_neg) == 1 * 1024**3
    
    params_zero = MockParams(max_bytes_billed_gb=0)
    assert get_max_bytes_billed(params_zero) == DEFAULT_MAX_BYTES_BILLED

def test_get_max_bytes_billed_clamped_max():
    # If the user enters a value above 10 TiB (10240 GiB), it clamps to 10240.
    params = MockParams(max_bytes_billed_gb=20000)
    assert get_max_bytes_billed(params) == 10240 * 1024**3

def test_pydantic_accepts_max_bytes_billed_gb_on_endpoints(mock_bq_all):
    """
    Verify that our Pydantic models successfully accept max_bytes_billed_gb
    without throwing 422 errors and that it flows correctly.
    """
    # 1. Storage Analyze
    response = client.post("/api/storage/analyze", json={
        "org_project_id": "valid-proj",
        "region": "region-us",
        "max_bytes_billed_gb": 500
    })
    assert response.status_code == 200
    
    # 2. Slots Tiered Recommendations
    response2 = client.post("/api/slots/tiered_recommendations", json={
        "org_project_id": "valid-proj",
        "region": "region-us",
        "lookback_days": 3,
        "max_bytes_billed_gb": 400
    })
    assert response2.status_code == 200


def test_bytes_billed_limit_exceeded_returns_400_on_fluid_endpoints():
    """Verify bytesBilledLimitExceeded maps to 400 on fluid scaling endpoints."""
    from google.api_core import exceptions as gax_exc

    err = gax_exc.BadRequest("Custom message: bytesBilledLimitExceeded")
    # Add error reasons list to mock gax error
    err._errors = [{"reason": "bytesBilledLimitExceeded"}]

    with patch("src.fluid_scaling._run_and_log", side_effect=err):
        # 1. Fluid Scaling Status
        res_status = client.post("/api/fluid-scaling/status", json={
            "org_project_id": "valid-proj",
            "region": "region-us",
        })
        assert res_status.status_code == 400
        assert "bytes-billed safety cap" in res_status.json()["detail"]

        # 2. Fluid Scaling Estimate
        res_est = client.post("/api/fluid-scaling/estimate", json={
            "org_project_id": "valid-proj",
            "region": "region-us",
        })
        assert res_est.status_code == 400
        assert "bytes-billed safety cap" in res_est.json()["detail"]

    with patch("src.main.run_query_to_df", side_effect=err):
        # 3. Fluid Simulation
        res_sim = client.post("/api/slots/fluid_simulation", json={
            "org_project_id": "valid-proj",
            "region": "region-us",
        })
        assert res_sim.status_code == 400
        assert "bytes-billed safety cap" in res_sim.json()["detail"]

