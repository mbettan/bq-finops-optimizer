"""Unit tests for src.pricing — regional BigQuery pricing resolution."""

import pytest
from src.pricing import (
    RegionPricing,
    get_pricing,
    get_all_regions,
    _parse_sku_file,
    _US_DEFAULTS,
    _PRICING_CACHE,
)


# ---------------------------------------------------------------------------
# Test: SKU file parsing
# ---------------------------------------------------------------------------

class TestSKUParsing:

    def test_parse_all_regions(self):
        """All 59 regions parse without error, all 5 price fields > 0."""
        regions = get_all_regions()
        assert len(regions) >= 50, f"Expected ≥50 regions, got {len(regions)}"
        for r in regions:
            if r == "global":  # Free-tier-only pseudo-region, $0 prices expected
                continue
            rp = get_pricing(r)
            assert rp.on_demand_usd_per_tib > 0, f"{r}: on_demand is 0"
            assert rp.active_logical_gib_mo > 0, f"{r}: active_logical is 0"
            assert rp.long_term_logical_gib_mo > 0, f"{r}: long_term_logical is 0"
            assert rp.active_physical_gib_mo > 0, f"{r}: active_physical is 0"
            assert rp.long_term_physical_gib_mo > 0, f"{r}: long_term_physical is 0"

    def test_tiered_rate_extraction(self):
        """Free tier ($0) is skipped; paid tier is extracted."""
        # US multi-region has a free tier for active logical storage (first 10 GiB free)
        us = get_pricing("us")
        # The price should be > 0 (the paid tier), not 0 (the free tier)
        assert us.active_logical_gib_mo > 0


# ---------------------------------------------------------------------------
# Test: US baseline
# ---------------------------------------------------------------------------

class TestUSBaseline:

    def test_us_matches_former_defaults(self):
        """US prices match the hardcoded defaults the app previously used."""
        us = get_pricing("region-us")
        assert us.on_demand_usd_per_tib == 6.25
        assert us.active_logical_gib_mo == 0.02
        assert us.long_term_logical_gib_mo == 0.01
        assert us.active_physical_gib_mo == 0.04
        assert us.long_term_physical_gib_mo == 0.02
        assert us.editions_slot_hr_rate == 0.06


# ---------------------------------------------------------------------------
# Test: EU multi-region mapping
# ---------------------------------------------------------------------------

class TestEUMapping:

    def test_eu_returns_europe_rates(self):
        """get_pricing('eu') returns the 'europe' SKU rates, not US defaults."""
        eu = get_pricing("region-eu")
        # EU physical storage prices differ from US
        assert eu.active_physical_gib_mo != _US_DEFAULTS.active_physical_gib_mo or \
               eu.long_term_physical_gib_mo != _US_DEFAULTS.long_term_physical_gib_mo, \
               "EU should have at least one price different from US defaults"

    def test_eu_and_europe_same(self):
        """'eu' alias should resolve to same prices as direct 'europe' lookup."""
        eu = get_pricing("eu")
        europe = get_pricing("europe")
        assert eu == europe


# ---------------------------------------------------------------------------
# Test: Region format variants
# ---------------------------------------------------------------------------

class TestRegionFormats:

    @pytest.mark.parametrize("input_region", [
        "region-us", "us", "US", " Region-US ", "REGION-US",
    ])
    def test_all_formats_resolve_identically(self, input_region):
        """Various region formats all resolve to the same US pricing."""
        rp = get_pricing(input_region)
        us = get_pricing("us")
        assert rp == us

    @pytest.mark.parametrize("input_region", [
        "region-europe-west3", "europe-west3", "EUROPE-WEST3",
    ])
    def test_non_us_formats(self, input_region):
        """Non-US region formats resolve correctly."""
        rp = get_pricing(input_region)
        assert rp.on_demand_usd_per_tib > 6.25  # Frankfurt is more expensive than US


# ---------------------------------------------------------------------------
# Test: Unknown region fallback
# ---------------------------------------------------------------------------

class TestFallback:

    def test_unknown_region_returns_us_defaults(self):
        """Unknown region falls back to US pricing."""
        rp = get_pricing("region-mars-1")
        assert rp == _US_DEFAULTS

    def test_unknown_region_logs_warning(self, caplog):
        """Unknown region emits a warning log."""
        import logging
        with caplog.at_level(logging.WARNING, logger="src.pricing"):
            get_pricing("region-mars-1")
        assert any("mars-1" in record.message for record in caplog.records)


# ---------------------------------------------------------------------------
# Test: Physical storage split
# ---------------------------------------------------------------------------

class TestPhysicalStorageSplit:

    def test_active_vs_longterm_differ(self):
        """Active and long-term physical prices should differ."""
        ew3 = get_pricing("europe-west3")
        assert ew3.active_physical_gib_mo != ew3.long_term_physical_gib_mo
        assert ew3.active_physical_gib_mo > ew3.long_term_physical_gib_mo


# ---------------------------------------------------------------------------
# Test: Regional price variation
# ---------------------------------------------------------------------------

class TestPriceVariation:

    def test_sao_paulo_more_expensive(self):
        """São Paulo on-demand ($11.25) ≠ US ($6.25)."""
        sp = get_pricing("southamerica-east1")
        us = get_pricing("us")
        assert sp.on_demand_usd_per_tib == 11.25
        assert sp.on_demand_usd_per_tib != us.on_demand_usd_per_tib

    def test_me_central2_premium(self):
        """me-central2 is one of the most expensive regions."""
        mc2 = get_pricing("me-central2")
        us = get_pricing("us")
        assert mc2.on_demand_usd_per_tib > us.on_demand_usd_per_tib
        assert mc2.active_logical_gib_mo > us.active_logical_gib_mo


# ---------------------------------------------------------------------------
# Test: StorageParams integration
# ---------------------------------------------------------------------------

class TestStorageParamsIntegration:

    def test_us_region_gets_us_prices(self):
        """StorageParams with default region gets US prices."""
        from src.main import StorageParams
        p = StorageParams(region="region-us")
        assert p.active_logical_price == 0.02
        assert p.long_term_logical_price == 0.01

    def test_nonus_region_gets_regional_prices(self):
        """StorageParams with europe-west3 gets Frankfurt prices."""
        from src.main import StorageParams
        p = StorageParams(region="region-europe-west3")
        ew3 = get_pricing("europe-west3")
        assert p.active_logical_price == ew3.active_logical_gib_mo
        assert p.long_term_logical_price == ew3.long_term_logical_gib_mo
        assert p.active_physical_price == ew3.active_physical_gib_mo
        assert p.long_term_physical_price == ew3.long_term_physical_gib_mo


# ---------------------------------------------------------------------------
# Test: RegionPricing is immutable
# ---------------------------------------------------------------------------

class TestImmutability:

    def test_frozen_dataclass(self):
        """RegionPricing should be immutable (frozen=True)."""
        rp = get_pricing("us")
        with pytest.raises(AttributeError):
            rp.on_demand_usd_per_tib = 999.0

    def test_fallback_when_cache_empty(self, monkeypatch):
        """When cache is empty, get_pricing falls back to US defaults."""
        import src.pricing
        monkeypatch.setattr(src.pricing, "_PRICING_CACHE", {})
        rp = get_pricing("europe-west1")
        assert rp == _US_DEFAULTS

