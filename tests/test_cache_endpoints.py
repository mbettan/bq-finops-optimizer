"""Endpoint tests for server-side caching and cache-control endpoints."""

import os
import pytest

os.environ.setdefault("AUTH_ENFORCED_UPSTREAM", "true")
os.environ.setdefault("CACHE_BACKEND", "file")

from fastapi.testclient import TestClient
from unittest.mock import MagicMock
from src.main import app
from src import cache as C


@pytest.fixture
def client():
    return TestClient(app)


def test_second_identical_post_is_a_hit_and_skips_bigquery(client, mock_bq_all):
    payload = {
        "org_project_id": "test-project",
        "region": "region-us",
        "lookback_days": 30,
        "on_demand_rate_per_tb": 6.25,
        "edition_slot_hr_rate": 0.06,
        "slot_step_size": 100,
    }
    r1 = client.post("/api/jobs/analyze", json=payload)
    assert r1.status_code == 200
    assert r1.headers.get("X-Cache") == "MISS"

    r2 = client.post("/api/jobs/analyze", json=payload)
    assert r2.status_code == 200
    assert r2.headers.get("X-Cache") == "HIT"
    assert r1.json() == r2.json()
    assert mock_bq_all.query.call_count == 1  # BigQuery only called once


def test_refresh_bypasses_and_rewrites(client, mock_bq_all):
    payload = {
        "org_project_id": "test-project",
        "region": "region-us",
        "lookback_days": 30,
        "on_demand_rate_per_tb": 6.25,
        "edition_slot_hr_rate": 0.06,
        "slot_step_size": 100,
    }
    r1 = client.post("/api/jobs/analyze", json=payload)
    assert r1.status_code == 200

    r2 = client.post("/api/jobs/analyze?refresh=true", json=payload)
    assert r2.status_code == 200
    assert r2.headers.get("X-Cache") == "BYPASS"
    assert mock_bq_all.query.call_count == 2


def test_scope_map_still_classifies_focus_endpoints(client):
    """The decorator appends `response`/`refresh` KEYWORD_ONLY at the END so
    `params` stays first and /api/meta/scope-map keeps working. If someone
    reorders those parameters, every focus-scoped endpoint's frontend payload
    breaks silently — this is the tripwire."""
    m = client.get("/api/meta/scope-map").json()
    assert m["/api/storage/analyze"] == "focus"
    assert m["/api/slots/analyze"] == "org"


def test_cached_response_still_satisfies_response_model(client, mock_bq_all):
    """Guards the decision: bodies stay unwrapped."""
    payload = {
        "org_project_id": "test-project",
        "region": "region-us",
        "lookback_days": 7,
        "limit_per_project": 10,
    }
    r1 = client.post("/api/antipatterns/linter", json=payload)
    assert r1.status_code == 200
    r2 = client.post("/api/antipatterns/linter", json=payload)
    assert r2.status_code == 200
    assert r2.headers.get("X-Cache") == "HIT"
    assert isinstance(r2.json(), list)  # not {"data": [...]}


def test_cache_control_endpoints(client, mock_bq_all):
    payload = {
        "org_project_id": "test-project",
        "region": "region-us",
        "lookback_days": 30,
        "on_demand_rate_per_tb": 6.25,
        "edition_slot_hr_rate": 0.06,
        "slot_step_size": 100,
    }
    # Initial status
    res = client.get("/api/cache/status?org_project_id=test-project&region=region-us")
    assert res.status_code == 200
    status_list = res.json()
    assert len(status_list) > 0
    jobs_entry = next((e for e in status_list if e["module"] == "jobs"), None)
    assert jobs_entry is not None
    assert jobs_entry["fresh"] is False

    # Run query to populate cache
    r = client.post("/api/jobs/analyze", json=payload)
    assert r.status_code == 200

    # Status should now report fresh
    res = client.get("/api/cache/status?org_project_id=test-project&region=region-us")
    status_list = res.json()
    jobs_entry = next((e for e in status_list if e["module"] == "jobs"), None)
    assert jobs_entry["fresh"] is True
    assert jobs_entry["cached_at"] is not None

    # Delete module cache
    del_mod = client.delete("/api/cache/jobs?org_project_id=test-project&region=region-us")
    assert del_mod.status_code == 200
    assert del_mod.json()["deleted"] >= 1

    # Status should now report stale
    res = client.get("/api/cache/status?org_project_id=test-project&region=region-us")
    jobs_entry = next((e for e in res.json() if e["module"] == "jobs"), None)
    assert jobs_entry["fresh"] is False

    # Run again and delete all scope cache
    client.post("/api/jobs/analyze", json=payload)
    del_scope = client.delete("/api/cache?org_project_id=test-project&region=region-us")
    assert del_scope.status_code == 200
    assert del_scope.json()["deleted"] >= 1
