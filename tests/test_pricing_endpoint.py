"""Unit tests for the GET /api/pricing endpoint.

The pricing endpoint requires no BigQuery calls — it reads from the
in-memory SKU cache — so no mocking is needed (like dashboard stubs).
"""

import pytest
from fastapi.testclient import TestClient
from src.main import app

client = TestClient(app)

# ---------------------------------------------------------------------------
# Expected response schema fields
# ---------------------------------------------------------------------------
_EXPECTED_FIELDS = {
    "region",
    "on_demand_usd_per_tib",
    "active_logical_gib_mo",
    "long_term_logical_gib_mo",
    "active_physical_gib_mo",
    "long_term_physical_gib_mo",
    "editions_slot_hr_rate",
    "source",
}


class TestPricingEndpointBasic:
    """Core endpoint contract: status, schema, types."""

    def test_returns_200_default_region(self):
        """GET /api/pricing with no params returns 200 (default = US)."""
        resp = client.get("/api/pricing")
        assert resp.status_code == 200

    def test_returns_200_with_explicit_region(self):
        resp = client.get("/api/pricing", params={"region": "region-europe-west3"})
        assert resp.status_code == 200

    def test_response_is_valid_json(self):
        resp = client.get("/api/pricing")
        data = resp.json()
        assert isinstance(data, dict)

    def test_response_schema_complete(self):
        """All expected fields are present in the response."""
        data = client.get("/api/pricing").json()
        assert _EXPECTED_FIELDS <= set(data.keys()), (
            f"Missing fields: {_EXPECTED_FIELDS - set(data.keys())}"
        )

    def test_source_is_sku_file(self):
        data = client.get("/api/pricing").json()
        assert data["source"] == "sku_file"

    def test_all_prices_are_floats(self):
        data = client.get("/api/pricing").json()
        for field in _EXPECTED_FIELDS - {"region", "source"}:
            assert isinstance(data[field], (int, float)), f"{field} is not numeric"
            assert data[field] > 0, f"{field} should be > 0 for US"


class TestPricingEndpointUSBaseline:
    """Verify US defaults match the former hardcoded constants."""

    def test_us_on_demand_rate(self):
        data = client.get("/api/pricing", params={"region": "region-us"}).json()
        assert data["on_demand_usd_per_tib"] == 6.25

    def test_us_active_logical_storage(self):
        data = client.get("/api/pricing", params={"region": "region-us"}).json()
        assert data["active_logical_gib_mo"] == 0.02

    def test_us_long_term_logical_storage(self):
        data = client.get("/api/pricing", params={"region": "region-us"}).json()
        assert data["long_term_logical_gib_mo"] == 0.01

    def test_us_active_physical_storage(self):
        data = client.get("/api/pricing", params={"region": "region-us"}).json()
        assert data["active_physical_gib_mo"] == 0.04

    def test_us_long_term_physical_storage(self):
        data = client.get("/api/pricing", params={"region": "region-us"}).json()
        assert data["long_term_physical_gib_mo"] == 0.02

    def test_us_editions_rate(self):
        data = client.get("/api/pricing", params={"region": "region-us"}).json()
        assert data["editions_slot_hr_rate"] == 0.06


class TestPricingEndpointRegionalDifferentiation:
    """Verify non-US regions return different (correct) prices."""

    def test_europe_west3_more_expensive(self):
        """Frankfurt on-demand should exceed US rate."""
        data = client.get("/api/pricing", params={"region": "region-europe-west3"}).json()
        assert data["on_demand_usd_per_tib"] > 6.25
        assert data["region"] == "europe-west3"

    def test_sao_paulo_highest_on_demand(self):
        data = client.get("/api/pricing", params={"region": "southamerica-east1"}).json()
        assert data["on_demand_usd_per_tib"] == 11.25

    def test_eu_multiregion_mapping(self):
        """EU alias should map to 'europe' and return distinct physical prices."""
        data = client.get("/api/pricing", params={"region": "region-eu"}).json()
        assert data["region"] == "europe"
        # EU physical prices differ from US
        us = client.get("/api/pricing", params={"region": "region-us"}).json()
        assert (
            data["active_physical_gib_mo"] != us["active_physical_gib_mo"]
            or data["long_term_physical_gib_mo"] != us["long_term_physical_gib_mo"]
        )

    def test_storage_prices_vary_by_region(self):
        us = client.get("/api/pricing", params={"region": "us"}).json()
        ew3 = client.get("/api/pricing", params={"region": "europe-west3"}).json()
        assert ew3["active_logical_gib_mo"] != us["active_logical_gib_mo"]


class TestPricingEndpointRegionFormats:
    """Verify various region string formats are accepted."""

    @pytest.mark.parametrize("region_input,expected_bare", [
        ("region-us", "us"),
        ("us", "us"),
        ("US", "us"),
        ("region-europe-west3", "europe-west3"),
        ("europe-west3", "europe-west3"),
    ])
    def test_format_accepted(self, region_input, expected_bare):
        resp = client.get("/api/pricing", params={"region": region_input})
        assert resp.status_code == 200
        data = resp.json()
        assert data["region"] == expected_bare

    def test_consistent_prices_across_formats(self):
        """'region-us' and 'us' should return identical prices."""
        d1 = client.get("/api/pricing", params={"region": "region-us"}).json()
        d2 = client.get("/api/pricing", params={"region": "us"}).json()
        for field in _EXPECTED_FIELDS - {"region", "source"}:
            assert d1[field] == d2[field], f"{field} differs between formats"


class TestPricingEndpointFallback:
    """Unknown regions should fall back gracefully to US defaults."""

    def test_unknown_region_returns_200(self):
        resp = client.get("/api/pricing", params={"region": "region-mars-1"})
        assert resp.status_code == 200

    def test_unknown_region_returns_us_prices(self):
        unknown = client.get("/api/pricing", params={"region": "region-mars-1"}).json()
        us = client.get("/api/pricing", params={"region": "region-us"}).json()
        assert unknown["source"] == "us_default_fallback"
        assert us["source"] == "sku_file"
        for field in _EXPECTED_FIELDS - {"region", "source"}:
            assert unknown[field] == us[field], (
                f"{field}: unknown={unknown[field]} != us={us[field]}"
            )

    def test_empty_region_returns_200(self):
        """Empty string defaults to US."""
        resp = client.get("/api/pricing", params={"region": ""})
        assert resp.status_code == 200
