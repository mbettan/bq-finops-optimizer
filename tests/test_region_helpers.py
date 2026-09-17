import pytest
from src.constants import normalize_region, bare_region, is_known_region, supports_org_views, BQ_REGIONS, DEFAULT_REGION
from src.main import region_query_error
from google.api_core import exceptions
import re

def test_normalize_region():
    assert normalize_region("US") == "region-us"
    assert normalize_region("us") == "region-us"
    assert normalize_region("REGION-US") == "region-us"
    assert normalize_region(" Region-US ") == "region-us"
    assert normalize_region("region-us") == "region-us"
    assert normalize_region("region-region-us") == "region-region-us"
    assert normalize_region(None) == DEFAULT_REGION
    assert normalize_region("") == DEFAULT_REGION

def test_bare_region():
    assert bare_region("region-us") == "us"
    assert bare_region("us") == "us"
    assert bare_region("REGION-US") == "us"

def test_is_known_region():
    assert is_known_region("region-us") is True
    assert is_known_region("us") is True
    assert is_known_region("region-mars-central1") is False

def test_supports_org_views():
    assert supports_org_views("region-mars-central1") is True
    assert supports_org_views("region-africa-south1") is False

def test_region_query_error():
    err_forbidden = exceptions.Forbidden("message")
    exc = region_query_error(err_forbidden, view="test_view", region="region-us")
    assert exc.status_code == 403
    
    err_bad_request = exceptions.BadRequest("message")
    exc = region_query_error(err_bad_request, view="test_view", region="region-us")
    assert exc.status_code == 400

    err_not_found = exceptions.NotFound("message")
    exc = region_query_error(err_not_found, view="test_view", region="region-africa-south1")
    assert exc.status_code == 404
    assert "region-africa-south1" in exc.detail
    assert "does not expose organization-scoped INFORMATION_SCHEMA views" in exc.detail

    exc = region_query_error(err_not_found, view="test_view", region="region-us")
    assert exc.status_code == 404
    assert "does not expose organization-scoped INFORMATION_SCHEMA views" not in exc.detail

def test_all_bq_regions_match_regex():
    from src.utils import _REGION_RE
    for region_tuple in BQ_REGIONS:
        region = region_tuple[0]
        assert re.match(_REGION_RE, normalize_region(region)), f"{region} failed to match {_REGION_RE}"


def test_region_query_error_retryable_403():
    err_rate_limit = exceptions.Forbidden("Rate limit exceeded", errors=[{"reason": "rateLimitExceeded"}])
    exc = region_query_error(err_rate_limit, view="test_view", region="region-us")
    assert exc.status_code == 429
    assert exc.headers.get("Retry-After") == "5"

    err_quota = exceptions.Forbidden("Quota exceeded", errors=[{"reason": "quotaExceeded"}])
    exc = region_query_error(err_quota, view="test_view", region="region-us")
    assert exc.status_code == 429
    assert exc.headers.get("Retry-After") == "5"


def test_pricing_overrides_respected():
    from src.main import StorageParams, JobAnalysisParams, UserProfilerParams
    # User explicitly sets price equal to US baseline in a non-US region
    sp = StorageParams(region="region-asia-northeast1", active_logical_price=0.02)
    assert sp.active_logical_price == 0.02

    # Unset field resolves to regional price
    sp_default = StorageParams(region="region-asia-northeast1")
    assert sp_default.active_logical_price != 0.02

    # JobAnalysisParams override
    jap = JobAnalysisParams(region="region-europe-west1", on_demand_rate_per_tb=6.25)
    assert jap.on_demand_rate_per_tb == 6.25

    # UserProfilerParams override
    upp = UserProfilerParams(region="region-europe-west1", od_price=6.25)
    assert upp.od_price == 6.25

