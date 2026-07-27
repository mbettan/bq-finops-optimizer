"""Regression tests for finding #1 — JOBS_BY_ORGANIZATION scans must be time-bounded.

INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION is partitioned on creation_time with
180-day retention. A query without a creation_time predicate cannot prune
partitions and reads the org's entire job history; LIMIT does not bound bytes
scanned. These tests fail if that predicate is ever dropped again.
"""
import pytest

_BASE = {"org_project_id": "test-project", "region": "region-us"}

JOB_GOVERNANCE_ENDPOINTS = [
    "/api/mv/analyze",
    "/api/resource_warnings/analyze",
]


def _captured_sql(mock_client):
    assert mock_client.query.call_args is not None, "no query was submitted"
    return mock_client.query.call_args[0][0]


@pytest.mark.parametrize("endpoint", JOB_GOVERNANCE_ENDPOINTS)
def test_query_is_time_bounded(test_client, mock_bq_all, endpoint):
    """Every JOBS_BY_ORGANIZATION read must carry a creation_time predicate."""
    resp = test_client.post(endpoint, json={**_BASE})
    assert resp.status_code == 200
    sql = _captured_sql(mock_bq_all)
    assert "creation_time >" in sql
    assert "TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL" in sql


@pytest.mark.parametrize("endpoint", JOB_GOVERNANCE_ENDPOINTS)
def test_default_lookback_is_30_days(test_client, mock_bq_all, endpoint):
    """Omitting lookback_days must not produce an unbounded scan."""
    resp = test_client.post(endpoint, json={**_BASE})
    assert resp.status_code == 200
    assert "INTERVAL 30 DAY" in _captured_sql(mock_bq_all)


@pytest.mark.parametrize("endpoint", JOB_GOVERNANCE_ENDPOINTS)
def test_explicit_lookback_reaches_sql(test_client, mock_bq_all, endpoint):
    resp = test_client.post(endpoint, json={**_BASE, "lookback_days": 7})
    assert resp.status_code == 200
    assert "INTERVAL 7 DAY" in _captured_sql(mock_bq_all)


@pytest.mark.parametrize("endpoint", JOB_GOVERNANCE_ENDPOINTS)
@pytest.mark.parametrize("bad", [0, -1, 91, 999])
def test_out_of_range_lookback_rejected(test_client, mock_bq_all, endpoint, bad):
    """Bounds are what make f-string interpolation of this value safe."""
    resp = test_client.post(endpoint, json={**_BASE, "lookback_days": bad})
    assert resp.status_code == 422


@pytest.mark.parametrize("endpoint", JOB_GOVERNANCE_ENDPOINTS)
def test_non_integer_lookback_rejected(test_client, mock_bq_all, endpoint):
    """Guards the interpolation site against anything non-numeric."""
    resp = test_client.post(endpoint, json={**_BASE, "lookback_days": "30 DAY) OR TRUE--"})
    assert resp.status_code == 422


@pytest.mark.parametrize("endpoint", JOB_GOVERNANCE_ENDPOINTS)
def test_unknown_field_rejected(test_client, mock_bq_all, endpoint):
    """extra='forbid' turns frontend/backend skew into a loud 422 rather than
    a silent revert to an unbounded scan."""
    resp = test_client.post(endpoint, json={**_BASE, "lookbackDays": 30})
    assert resp.status_code == 422


def test_mv_query_is_deterministically_ordered(test_client, mock_bq_all):
    """LIMIT 50 with no ORDER BY returned an arbitrary 50 rows from a 180-day
    window, varying run to run."""
    resp = test_client.post("/api/mv/analyze", json={**_BASE})
    assert resp.status_code == 200
    assert "ORDER BY creation_time DESC" in _captured_sql(mock_bq_all)


def test_governance_endpoint_still_accepts_audit_type(test_client, mock_bq_all):
    """/api/governance/analyze must keep tolerating the ignored audit_type field
    the UI sends. Regression guard against adding extra='forbid' to the shared
    GovernanceParams base."""
    resp = test_client.post(
        "/api/governance/analyze", json={**_BASE, "audit_type": "expiration"}
    )
    assert resp.status_code == 200
