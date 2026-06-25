"""
Module 1: Input Validation Tests

Tests that Pydantic Field bounds and @field_validator constraints correctly
reject invalid input BEFORE any handler code or BigQuery calls execute.

No mocking required — all tests use FastAPI TestClient only.
"""

import pytest
from fastapi.testclient import TestClient
from src.main import app

client = TestClient(app)


# ---------------------------------------------------------------------------
# lookback_days bounds (ge=1, le=90) — all POST endpoints with this field
# ---------------------------------------------------------------------------

LOOKBACK_ENDPOINTS = [
    ("/api/jobs/analyze",              {"org_project_id": "valid-proj", "region": "region-us"}),
    ("/api/antipatterns/dml",          {"org_project_id": "valid-proj", "region": "region-us"}),
    ("/api/antipatterns/linter",       {"org_project_id": "valid-proj", "region": "region-us"}),
    ("/api/antipatterns/skew",         {"org_project_id": "valid-proj", "region": "region-us"}),
    ("/api/antipatterns/batch_candidates", {"org_project_id": "valid-proj", "region": "region-us"}),
    ("/api/bi/analyze",                {"org_project_id": "valid-proj", "region": "region-us"}),
    ("/api/slots/analyze",             {"org_project_id": "valid-proj", "region": "region-us"}),
    ("/api/slots/tiered_recommendations", {"org_project_id": "valid-proj", "region": "region-us"}),
    ("/api/slots/utilization",         {"org_project_id": "valid-proj", "region": "region-us"}),
    ("/api/slots/actual_provisioning", {"org_project_id": "valid-proj", "region": "region-us"}),
    ("/api/slots/peak",                {"org_project_id": "valid-proj", "region": "region-us"}),
    ("/api/slots/profiler",            {"org_project_id": "valid-proj", "region": "region-us"}),
    ("/api/slots/profiler/queries",    {"org_project_id": "valid-proj", "region": "region-us"}),
    ("/api/users/top_spenders",        {"org_project_id": "valid-proj", "region": "region-us"}),
    ("/api/slots/fluid_simulation",    {"org_project_id": "valid-proj", "region": "region-us"}),
]


@pytest.mark.parametrize("endpoint, base_payload", LOOKBACK_ENDPOINTS)
def test_lookback_days_zero_rejected(endpoint, base_payload):
    """lookback_days=0 must be rejected (ge=1)."""
    payload = {**base_payload, "lookback_days": 0}
    response = client.post(endpoint, json=payload)
    assert response.status_code == 422, f"{endpoint} accepted lookback_days=0"


@pytest.mark.parametrize("endpoint, base_payload", LOOKBACK_ENDPOINTS)
def test_lookback_days_negative_rejected(endpoint, base_payload):
    """lookback_days=-5 must be rejected (ge=1)."""
    payload = {**base_payload, "lookback_days": -5}
    response = client.post(endpoint, json=payload)
    assert response.status_code == 422, f"{endpoint} accepted lookback_days=-5"


@pytest.mark.parametrize("endpoint, base_payload", LOOKBACK_ENDPOINTS)
def test_lookback_days_over_90_rejected(endpoint, base_payload):
    """lookback_days=91 must be rejected (le=90)."""
    payload = {**base_payload, "lookback_days": 91}
    response = client.post(endpoint, json=payload)
    assert response.status_code == 422, f"{endpoint} accepted lookback_days=91"


# ---------------------------------------------------------------------------
# limit / limit_per_project / limit_jobs bounds
# ---------------------------------------------------------------------------

LIMIT_TEST_CASES = [
    # (endpoint, base_payload, field_name, bad_value_low, bad_value_high)
    ("/api/storage/hygiene", {"org_project_id": "valid-proj"}, "limit", 0, 501),
    ("/api/bi/analyze", {"org_project_id": "valid-proj"}, "limit", 0, 501),
    ("/api/antipatterns/linter", {"org_project_id": "valid-proj"}, "limit_per_project", 0, 1001),
    ("/api/jobs/analyze", {"org_project_id": "valid-proj"}, "limit_jobs", 0, 10001),
]


@pytest.mark.parametrize("endpoint, base_payload, field, bad_low, bad_high", LIMIT_TEST_CASES)
def test_limit_under_minimum_rejected(endpoint, base_payload, field, bad_low, bad_high):
    """Limit values below minimum must be rejected."""
    payload = {**base_payload, field: bad_low}
    response = client.post(endpoint, json=payload)
    assert response.status_code == 422, f"{endpoint} accepted {field}={bad_low}"


@pytest.mark.parametrize("endpoint, base_payload, field, bad_low, bad_high", LIMIT_TEST_CASES)
def test_limit_over_maximum_rejected(endpoint, base_payload, field, bad_low, bad_high):
    """Limit values above maximum must be rejected."""
    payload = {**base_payload, field: bad_high}
    response = client.post(endpoint, json=payload)
    assert response.status_code == 422, f"{endpoint} accepted {field}={bad_high}"


# ---------------------------------------------------------------------------
# Slots-specific field bounds (percentile, window_minutes)
# ---------------------------------------------------------------------------

class TestSlotsFieldBounds:
    BASE = {"org_project_id": "valid-proj", "region": "region-us"}
    ENDPOINT = "/api/slots/analyze"

    def test_percentile_zero_rejected(self):
        response = client.post(self.ENDPOINT, json={**self.BASE, "percentile": 0})
        assert response.status_code == 422

    def test_percentile_100_rejected(self):
        response = client.post(self.ENDPOINT, json={**self.BASE, "percentile": 100})
        assert response.status_code == 422

    def test_window_minutes_zero_rejected(self):
        response = client.post(self.ENDPOINT, json={**self.BASE, "window_minutes": 0})
        assert response.status_code == 422

    def test_window_minutes_61_rejected(self):
        response = client.post(self.ENDPOINT, json={**self.BASE, "window_minutes": 61})
        assert response.status_code == 422


# ---------------------------------------------------------------------------
# DML threshold bounds
# ---------------------------------------------------------------------------

class TestDMLThresholdBounds:
    BASE = {"org_project_id": "valid-proj", "region": "region-us"}
    ENDPOINT = "/api/antipatterns/dml"

    def test_threshold_zero_rejected(self):
        response = client.post(self.ENDPOINT, json={**self.BASE, "threshold": 0})
        assert response.status_code == 422

    def test_threshold_negative_rejected(self):
        response = client.post(self.ENDPOINT, json={**self.BASE, "threshold": -100})
        assert response.status_code == 422


# ---------------------------------------------------------------------------
# Edition Pydantic validator (on SlotActualParams)
# ---------------------------------------------------------------------------

class TestEditionValidator:
    BASE = {"org_project_id": "valid-proj", "region": "region-us"}
    ENDPOINT = "/api/slots/actual_provisioning"

    @pytest.mark.parametrize("edition", ["STANDARD", "ENTERPRISE", "ENTERPRISE_PLUS"])
    def test_valid_editions_accepted(self, edition):
        """Valid editions must not trigger a 422 (they may trigger 400 from
        _validate_safe_params or downstream errors, but never 422)."""
        payload = {**self.BASE, "edition": edition}
        response = client.post(self.ENDPOINT, json=payload)
        assert response.status_code != 422, f"Edition {edition} was rejected by Pydantic"

    @pytest.mark.parametrize("edition", ["FREE", "BASIC", "enterprise", "Standard", ""])
    def test_invalid_editions_rejected(self, edition):
        """Invalid or mis-cased editions must be rejected."""
        payload = {**self.BASE, "edition": edition}
        response = client.post(self.ENDPOINT, json=payload)
        assert response.status_code in (400, 422), f"Edition '{edition}' was not rejected"


# ---------------------------------------------------------------------------
# Resolution Pydantic validator (on SlotUtilizationParams)
# ---------------------------------------------------------------------------

class TestResolutionValidator:
    BASE = {"org_project_id": "valid-proj", "region": "region-us"}
    ENDPOINT = "/api/slots/utilization"

    @pytest.mark.parametrize("resolution", ["MINUTE", "HOUR", "DAY"])
    def test_valid_resolutions_accepted(self, resolution):
        payload = {**self.BASE, "resolution": resolution}
        response = client.post(self.ENDPOINT, json=payload)
        assert response.status_code != 422, f"Resolution {resolution} was rejected by Pydantic"

    @pytest.mark.parametrize("resolution", ["SECOND", "minute", "WEEK", ""])
    def test_invalid_resolutions_rejected(self, resolution):
        payload = {**self.BASE, "resolution": resolution}
        response = client.post(self.ENDPOINT, json=payload)
        assert response.status_code in (400, 422), f"Resolution '{resolution}' was not rejected"


# ---------------------------------------------------------------------------
# time_travel_hours Pydantic validator (on StorageParams)
# ---------------------------------------------------------------------------

class TestTimeTravelHoursValidator:
    BASE = {"org_project_id": "valid-proj", "region": "region-us"}
    ENDPOINT = "/api/storage/analyze"

    @pytest.mark.parametrize("hours", [48, 72, 96, 120, 144, 168])
    def test_valid_tt_hours_accepted(self, hours):
        payload = {**self.BASE, "time_travel_hours": hours}
        response = client.post(self.ENDPOINT, json=payload)
        assert response.status_code != 422, f"time_travel_hours={hours} was rejected"

    @pytest.mark.parametrize("hours", [0, 24, 50, 100, 200, -1])
    def test_invalid_tt_hours_rejected(self, hours):
        payload = {**self.BASE, "time_travel_hours": hours}
        response = client.post(self.ENDPOINT, json=payload)
        assert response.status_code == 422, f"time_travel_hours={hours} was not rejected"

    def test_tt_hours_none_accepted(self):
        """Omitting time_travel_hours (None default) must be valid."""
        payload = {**self.BASE}
        response = client.post(self.ENDPOINT, json=payload)
        assert response.status_code != 422, "Omitting time_travel_hours was rejected"


# ---------------------------------------------------------------------------
# Cost Attribution date format validator
# ---------------------------------------------------------------------------

class TestCostAttributionDateValidator:
    ENDPOINT = "/api/cost-attribution/calculate"

    def test_valid_dates_accepted(self):
        """YYYY-MM-DD dates must not trigger 422."""
        payload = {
            "billing_month_start": "2026-01-01",
            "billing_month_end": "2026-01-31",
            "org_project_id": "valid-proj",
            "region": "region-us",
        }
        response = client.post(self.ENDPOINT, json=payload)
        assert response.status_code != 422

    @pytest.mark.parametrize("bad_date", [
        "01-01-2026",           # wrong order
        "2026/01/01",           # wrong separator
        "not-a-date",           # text
        "2026-01-01') OR 1=1--",  # injection attempt
        "",                      # empty
    ])
    def test_invalid_date_start_rejected(self, bad_date):
        payload = {
            "billing_month_start": bad_date,
            "billing_month_end": "2026-01-31",
            "org_project_id": "valid-proj",
        }
        response = client.post(self.ENDPOINT, json=payload)
        assert response.status_code == 422, f"Date '{bad_date}' was not rejected"

    @pytest.mark.parametrize("bad_date", [
        "31-01-2026",
        "yesterday",
        "2026-13-01",           # invalid month
    ])
    def test_invalid_date_end_rejected(self, bad_date):
        payload = {
            "billing_month_start": "2026-01-01",
            "billing_month_end": bad_date,
            "org_project_id": "valid-proj",
        }
        response = client.post(self.ENDPOINT, json=payload)
        assert response.status_code == 422, f"Date '{bad_date}' was not rejected"


# ---------------------------------------------------------------------------
# FluidSimParams cooldown_window bounds
# ---------------------------------------------------------------------------

class TestFluidSimBounds:
    BASE = {"org_project_id": "valid-proj", "region": "region-us"}
    ENDPOINT = "/api/slots/fluid_simulation"

    def test_cooldown_zero_rejected(self):
        response = client.post(self.ENDPOINT, json={**self.BASE, "cooldown_window": 0})
        assert response.status_code == 422

    def test_cooldown_301_rejected(self):
        response = client.post(self.ENDPOINT, json={**self.BASE, "cooldown_window": 301})
        assert response.status_code == 422

    def test_edition_slot_hr_rate_zero_rejected(self):
        response = client.post(self.ENDPOINT, json={**self.BASE, "edition_slot_hr_rate": 0})
        assert response.status_code == 422

    def test_edition_slot_hr_rate_negative_rejected(self):
        response = client.post(self.ENDPOINT, json={**self.BASE, "edition_slot_hr_rate": -0.06})
        assert response.status_code == 422


# ---------------------------------------------------------------------------
# HBO model bounds (verify lookback_days has no bounds yet — regression anchor)
# ---------------------------------------------------------------------------

class TestHBOParamsAcceptance:
    """HBO endpoints don't yet have Field bounds on lookback_days.
    These tests document the current behavior so future bound additions are tracked."""

    @pytest.mark.parametrize("endpoint", [
        "/api/hbo/analyze",
        "/api/hbo/summary",
        "/api/hbo/performance_insights",
    ])
    def test_hbo_lookback_large_value_not_rejected_by_pydantic(self, endpoint):
        """HBO models currently lack Field bounds — large values pass Pydantic."""
        payload = {"org_project_id": "valid-proj", "lookback_days": 365}
        response = client.post(endpoint, json=payload)
        # Should NOT be 422 (no Pydantic bound), but may be 400/500 from handler
        assert response.status_code != 422
