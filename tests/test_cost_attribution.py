"""
Dedicated money-math unit tests for cost_attribution.py.

Modelled on tests/test_physical_bytes_decomposition.py — the one file in the
repo that pins real dollars against real src code.

These tests exercise the financial math in calculate_cost_attribution:
  - slot-hour conversion (total_slot_ms / 3600000)
  - Rule A proportional waste allocation
  - Rule B central dump
  - Negative-waste clamp (C1a)
  - Zero-denominator guard (C1b)
  - Rounding consistency (M6: direct + waste == total)
  - Reservation-ID normalization (short vs dotted vs colon)
  - Unconfigured reservation accounting
  - Σ attributions == Σ bill (the master invariant)

None of these tests make real BigQuery calls.
"""

from __future__ import annotations

import math
import pytest
from unittest.mock import patch, MagicMock
from collections import defaultdict

from src.cost_attribution import (
    calculate_cost_attribution,
    CostAttributionParams,
    CostAttributionConfig,
    ReservationConfig,
    load_config,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_mock_row(project_id: str, reservation_id: str, total_slot_ms: int):
    """Create a mock BQ row for JOBS_BY_ORGANIZATION aggregation."""
    row = MagicMock()
    row.project_id = project_id
    row.reservation_id = reservation_id
    row.total_slot_ms = total_slot_ms
    return row


def _make_params(**overrides):
    defaults = {
        "billing_month_start": "2026-07-01",
        "billing_month_end": "2026-07-31",
        "org_project_id": "test-org",
        "admin_project_id": "test-admin",
        "region": "us",
    }
    defaults.update(overrides)
    return CostAttributionParams(**defaults)


def _run_attribution(rows, config, **param_overrides):
    """Call calculate_cost_attribution with mocked BQ and config."""
    params = _make_params(**param_overrides)

    with patch("src.cost_attribution.init_bq_client_and_resolve_project") as mock_init, \
         patch("src.cost_attribution._run_and_log", return_value=rows), \
         patch("src.cost_attribution.load_config", return_value=config), \
         patch("src.cost_attribution.log_endpoint_start"), \
         patch("src.cost_attribution.log_endpoint_end"):
        mock_init.return_value = (MagicMock(), "test-admin")
        return calculate_cost_attribution(params)


# ---------------------------------------------------------------------------
# Slot-hour conversion
# ---------------------------------------------------------------------------

class TestSlotHourConversion:
    """total_slot_ms / 3600000 must produce correct slot-hours."""

    def test_exact_one_hour(self):
        """3,600,000 ms = exactly 1 slot-hour."""
        config = CostAttributionConfig(
            waste_rule="A",
            reservations={"res-a": ReservationConfig(sku_rate=0.06, total_admin_bill=100.0)},
        )
        rows = [_make_mock_row("proj-1", "res-a", 3_600_000)]
        result = _run_attribution(rows, config)
        attr = result["attributions"][0]
        assert attr["slot_hours"] == 1.0

    def test_fractional_hours(self):
        """1,800,000 ms = 0.5 slot-hours."""
        config = CostAttributionConfig(
            waste_rule="A",
            reservations={"res-a": ReservationConfig(sku_rate=0.06, total_admin_bill=100.0)},
        )
        rows = [_make_mock_row("proj-1", "res-a", 1_800_000)]
        result = _run_attribution(rows, config)
        assert result["attributions"][0]["slot_hours"] == 0.5

    def test_zero_slot_ms(self):
        """0 ms = 0 slot-hours (cache hit)."""
        config = CostAttributionConfig(
            waste_rule="A",
            reservations={"res-a": ReservationConfig(sku_rate=0.06, total_admin_bill=100.0)},
        )
        rows = [_make_mock_row("proj-1", "res-a", 0)]
        result = _run_attribution(rows, config)
        assert result["attributions"][0]["slot_hours"] == 0.0

    def test_null_slot_ms_treated_as_zero(self):
        """None total_slot_ms → 0 slot-hours via (row.total_slot_ms or 0)."""
        config = CostAttributionConfig(
            waste_rule="A",
            reservations={"res-a": ReservationConfig(sku_rate=0.06, total_admin_bill=100.0)},
        )
        rows = [_make_mock_row("proj-1", "res-a", None)]
        result = _run_attribution(rows, config)
        assert result["attributions"][0]["slot_hours"] == 0.0

    def test_large_value(self):
        """36,000,000 ms = 10 slot-hours."""
        config = CostAttributionConfig(
            waste_rule="A",
            reservations={"res-a": ReservationConfig(sku_rate=0.06, total_admin_bill=100.0)},
        )
        rows = [_make_mock_row("proj-1", "res-a", 36_000_000)]
        result = _run_attribution(rows, config)
        assert result["attributions"][0]["slot_hours"] == 10.0


# ---------------------------------------------------------------------------
# Rule A: proportional waste allocation
# ---------------------------------------------------------------------------

class TestRuleAProportionalWaste:
    """Waste = bill - direct; distributed proportionally by slot-hours."""

    def test_single_project_gets_all_waste(self):
        """One project in a reservation gets 100% of waste."""
        config = CostAttributionConfig(
            waste_rule="A",
            reservations={"res-a": ReservationConfig(sku_rate=0.04, total_admin_bill=100.0)},
        )
        # 10 slot-hours × $0.04 = $0.40 direct. Waste = $100 - $0.40 = $99.60
        rows = [_make_mock_row("proj-1", "res-a", 36_000_000)]  # 10 hrs
        result = _run_attribution(rows, config)
        attr = result["attributions"][0]
        assert attr["direct_usage_cost_usd"] == 0.40
        assert attr["allocated_waste_cost_usd"] == 99.60
        assert attr["total_cost_attribution_usd"] == 100.0

    def test_two_projects_proportional_split(self):
        """Two projects: 75%/25% slot-hours → 75%/25% waste."""
        config = CostAttributionConfig(
            waste_rule="A",
            reservations={"res-a": ReservationConfig(sku_rate=0.04, total_admin_bill=100.0)},
        )
        # proj-1: 7.5 hrs, proj-2: 2.5 hrs → total 10 hrs
        rows = [
            _make_mock_row("proj-1", "res-a", 27_000_000),  # 7.5 hrs
            _make_mock_row("proj-2", "res-a", 9_000_000),   # 2.5 hrs
        ]
        result = _run_attribution(rows, config)
        attrs = {a["project_id"]: a for a in result["attributions"]}

        # Direct: 7.5 × 0.04 = 0.30, 2.5 × 0.04 = 0.10
        # Total direct: 0.40. Waste: 99.60
        # proj-1 waste: 99.60 × 0.75 = 74.70
        # proj-2 waste: 99.60 × 0.25 = 24.90
        assert attrs["proj-1"]["direct_usage_cost_usd"] == 0.30
        assert attrs["proj-2"]["direct_usage_cost_usd"] == 0.10
        assert attrs["proj-1"]["total_cost_attribution_usd"] == 75.0
        assert attrs["proj-2"]["total_cost_attribution_usd"] == 25.0

        # Master invariant: Σ total == bill
        total = sum(a["total_cost_attribution_usd"] for a in result["attributions"])
        assert total == 100.0

    def test_three_projects_sum_equals_bill(self):
        """Σ attributions == total_admin_bill across 3 projects."""
        bill = 1500.0
        config = CostAttributionConfig(
            waste_rule="A",
            reservations={"prod": ReservationConfig(sku_rate=0.06, total_admin_bill=bill)},
        )
        rows = [
            _make_mock_row("proj-a", "prod", 36_000_000),   # 10 hrs
            _make_mock_row("proj-b", "prod", 72_000_000),   # 20 hrs
            _make_mock_row("proj-c", "prod", 108_000_000),  # 30 hrs
        ]
        result = _run_attribution(rows, config)

        total = sum(a["total_cost_attribution_usd"] for a in result["attributions"])
        assert total == bill


# ---------------------------------------------------------------------------
# Negative waste clamp (C1a)
# ---------------------------------------------------------------------------

class TestNegativeWasteClamp:
    """When direct cost > bill (e.g. CUD discount), waste = 0, not negative."""

    def test_usage_exceeds_bill(self):
        """Rate × hours > bill → waste clamped to 0."""
        config = CostAttributionConfig(
            waste_rule="A",
            reservations={"res-a": ReservationConfig(sku_rate=10.0, total_admin_bill=50.0)},
        )
        # 10 slot-hours × $10 = $100 direct, but bill is only $50
        rows = [_make_mock_row("proj-1", "res-a", 36_000_000)]
        result = _run_attribution(rows, config)
        attr = result["attributions"][0]

        assert attr["direct_usage_cost_usd"] == 100.0
        assert attr["allocated_waste_cost_usd"] == 0.0
        assert attr["total_cost_attribution_usd"] == 100.0

    def test_total_exceeds_bill_without_inflation(self):
        """Even though total > bill, we don't artificially reduce direct to match.
        The over-attribution is intentional and visible."""
        config = CostAttributionConfig(
            waste_rule="A",
            reservations={"res-a": ReservationConfig(sku_rate=1.0, total_admin_bill=5.0)},
        )
        rows = [
            _make_mock_row("proj-1", "res-a", 36_000_000),  # 10 hrs × $1 = $10
        ]
        result = _run_attribution(rows, config)
        attr = result["attributions"][0]
        # Direct = $10, waste clamped to 0
        assert attr["allocated_waste_cost_usd"] == 0.0


# ---------------------------------------------------------------------------
# Zero-denominator guard (C1b)
# ---------------------------------------------------------------------------

class TestZeroDenominatorGuard:
    """When total slot-hours = 0 (all cache hits), waste is not divided."""

    def test_all_cache_hits(self):
        """Zero total_slot_ms → direct=0, waste=0, no ZeroDivisionError."""
        config = CostAttributionConfig(
            waste_rule="A",
            reservations={"res-a": ReservationConfig(sku_rate=0.06, total_admin_bill=100.0)},
        )
        rows = [_make_mock_row("proj-1", "res-a", 0)]
        result = _run_attribution(rows, config)
        attr = result["attributions"][0]
        assert attr["direct_usage_cost_usd"] == 0.0
        assert attr["allocated_waste_cost_usd"] == 0.0
        assert attr["total_cost_attribution_usd"] == 0.0


# ---------------------------------------------------------------------------
# Rounding consistency (M6)
# ---------------------------------------------------------------------------

class TestRoundingConsistency:
    """direct + waste must ALWAYS equal total (no rounding drift)."""

    @pytest.mark.parametrize("slot_ms,rate,bill", [
        (3_600_000, 0.033, 100.0),   # 1 hr × $0.033 = $0.033 direct
        (5_400_000, 0.077, 200.0),   # 1.5 hrs × $0.077 = tricky rounding
        (7_123_456, 0.049, 150.0),   # non-round slot-ms
        (1, 0.01, 1000.0),           # sub-millisecond precision
        (999_999_999, 0.06, 500.0),  # large slot-ms
    ])
    def test_direct_plus_waste_equals_total(self, slot_ms, rate, bill):
        """For every row, direct + waste == total to the cent."""
        config = CostAttributionConfig(
            waste_rule="A",
            reservations={"res-a": ReservationConfig(sku_rate=rate, total_admin_bill=bill)},
        )
        rows = [_make_mock_row("proj-1", "res-a", slot_ms)]
        result = _run_attribution(rows, config)
        attr = result["attributions"][0]

        computed_sum = round(attr["direct_usage_cost_usd"] + attr["allocated_waste_cost_usd"], 2)
        assert computed_sum == attr["total_cost_attribution_usd"], (
            f"Rounding drift: {attr['direct_usage_cost_usd']} + "
            f"{attr['allocated_waste_cost_usd']} = {computed_sum} ≠ "
            f"{attr['total_cost_attribution_usd']}"
        )


# ---------------------------------------------------------------------------
# Reservation-ID normalization
# ---------------------------------------------------------------------------

class TestReservationIdNormalization:
    """Config lookup must match both short and fully-qualified reservation IDs."""

    @pytest.mark.parametrize("bq_res_id", [
        "prod-res",                           # short form (from BQ)
        "admin-project.US.prod-res",          # dotted form
        "admin-project:US.prod-res",          # colon form
    ])
    def test_all_id_formats_match_config(self, bq_res_id):
        """Reservation configured by short name matches any BQ format."""
        config = CostAttributionConfig(
            waste_rule="A",
            reservations={"prod-res": ReservationConfig(sku_rate=0.06, total_admin_bill=100.0)},
        )
        rows = [_make_mock_row("proj-1", bq_res_id, 3_600_000)]
        result = _run_attribution(rows, config)

        # Should NOT end up in unattributed
        assert result["is_complete"] is True
        assert len(result["attributions"]) == 1


# ---------------------------------------------------------------------------
# Unconfigured reservation tracking
# ---------------------------------------------------------------------------

class TestUnconfiguredReservations:
    """Reservations not in config are tracked, not silently dropped."""

    def test_missing_reservation_is_unattributed(self):
        config = CostAttributionConfig(
            waste_rule="A",
            reservations={},  # no reservations configured
        )
        rows = [_make_mock_row("proj-1", "unknown-res", 7_200_000)]  # 2 hrs
        result = _run_attribution(rows, config)

        assert result["is_complete"] is False
        assert len(result["attributions"]) == 0
        assert result["total_unattributed_slot_hours"] == 2.0
        assert result["unattributed_reservations"][0]["reservation_id"] == "unknown-res"

    def test_mixed_configured_and_unconfigured(self):
        config = CostAttributionConfig(
            waste_rule="A",
            reservations={"res-a": ReservationConfig(sku_rate=0.06, total_admin_bill=100.0)},
        )
        rows = [
            _make_mock_row("proj-1", "res-a", 3_600_000),       # configured
            _make_mock_row("proj-2", "res-unknown", 7_200_000),  # not configured
        ]
        result = _run_attribution(rows, config)

        assert result["is_complete"] is False
        assert len(result["attributions"]) == 1
        assert result["total_unattributed_slot_hours"] == 2.0


# ---------------------------------------------------------------------------
# Rule B: central dump
# ---------------------------------------------------------------------------

class TestRuleBCentralDump:
    """Waste_rule='B' dumps all waste to central cost center."""

    def test_waste_goes_to_central(self):
        config = CostAttributionConfig(
            waste_rule="B",
            central_cost_center_project="central-it",
            reservations={"res-a": ReservationConfig(sku_rate=0.04, total_admin_bill=100.0)},
        )
        # 10 slot-hours × $0.04 = $0.40 direct. Waste = $99.60
        rows = [_make_mock_row("proj-1", "res-a", 36_000_000)]
        result = _run_attribution(rows, config)

        attrs = {a["project_id"]: a for a in result["attributions"]}
        # proj-1 gets only direct cost, no waste
        assert attrs["proj-1"]["direct_usage_cost_usd"] == 0.40
        assert attrs["proj-1"]["allocated_waste_cost_usd"] == 0.0
        # central-it gets all waste
        assert attrs["central-it"]["allocated_waste_cost_usd"] == 99.60

    def test_rule_b_requires_central_project(self):
        """Rule B without central_cost_center_project → 400."""
        from fastapi.exceptions import HTTPException
        config = CostAttributionConfig(
            waste_rule="B",
            central_cost_center_project=None,
            reservations={"res-a": ReservationConfig(sku_rate=0.04, total_admin_bill=100.0)},
        )
        rows = [_make_mock_row("proj-1", "res-a", 36_000_000)]
        with pytest.raises(HTTPException) as exc_info:
            _run_attribution(rows, config)
        assert exc_info.value.status_code == 400


# ---------------------------------------------------------------------------
# Pydantic validation
# ---------------------------------------------------------------------------

class TestPydanticValidation:
    """M7: sku_rate and total_admin_bill reject negative/inf/nan."""

    def test_negative_sku_rate_rejected(self):
        from pydantic import ValidationError
        with pytest.raises(ValidationError):
            ReservationConfig(sku_rate=-1.0, total_admin_bill=100.0)

    def test_inf_sku_rate_rejected(self):
        from pydantic import ValidationError
        with pytest.raises(ValidationError):
            ReservationConfig(sku_rate=float("inf"), total_admin_bill=100.0)

    def test_nan_total_admin_bill_rejected(self):
        from pydantic import ValidationError
        with pytest.raises(ValidationError):
            ReservationConfig(sku_rate=0.06, total_admin_bill=float("nan"))

    def test_zero_values_accepted(self):
        """ge=0 means zero is valid."""
        cfg = ReservationConfig(sku_rate=0.0, total_admin_bill=0.0)
        assert cfg.sku_rate == 0.0


# ---------------------------------------------------------------------------
# Focus filter (display, not computation)
# ---------------------------------------------------------------------------

class TestFocusFilter:
    """Focus projects filter DISPLAY, not waste computation."""

    def test_focus_filters_output_but_computes_on_full_org(self):
        config = CostAttributionConfig(
            waste_rule="A",
            reservations={"res-a": ReservationConfig(sku_rate=0.04, total_admin_bill=100.0)},
        )
        rows = [
            _make_mock_row("proj-1", "res-a", 27_000_000),  # 7.5 hrs
            _make_mock_row("proj-2", "res-a", 9_000_000),   # 2.5 hrs
        ]
        result = _run_attribution(rows, config, focus_projects=["proj-1"])

        # Only proj-1 in output
        assert len(result["attributions"]) == 1
        assert result["attributions"][0]["project_id"] == "proj-1"
        # But the waste computation used the full 10-hour denominator,
        # so proj-1 gets 75% of $100 = $75, not 100% of $100.
        assert result["attributions"][0]["total_cost_attribution_usd"] == 75.0


# ---------------------------------------------------------------------------
# Mutation detection: the five original bugs from §1
# ---------------------------------------------------------------------------

class TestMutationDetection:
    """These tests would have caught the §1 mutations — the 'safety net'
    that previously didn't exist."""

    def test_mutation_2_slot_hour_divisor(self):
        """Mutation #2: changing / 3600000.0 to / 1000.0 would produce
        3600× inflated slot-hours. This test pins the divisor."""
        config = CostAttributionConfig(
            waste_rule="A",
            reservations={"res-a": ReservationConfig(sku_rate=1.0, total_admin_bill=1000.0)},
        )
        rows = [_make_mock_row("proj-1", "res-a", 3_600_000)]
        result = _run_attribution(rows, config)
        # 3,600,000 ms / 3,600,000 = 1.0 slot-hour
        # If divisor were 1000, this would be 3600 slot-hours
        assert result["attributions"][0]["slot_hours"] == 1.0

    def test_mutation_5_proportional_not_full_waste(self):
        """Mutation #5: replacing allocated_waste = waste × share with
        allocated_waste = waste (full amount). With 2 equal projects,
        each should get 50% of waste, not 100%."""
        config = CostAttributionConfig(
            waste_rule="A",
            reservations={"res-a": ReservationConfig(sku_rate=0.0, total_admin_bill=100.0)},
        )
        rows = [
            _make_mock_row("proj-1", "res-a", 3_600_000),  # 1 hr
            _make_mock_row("proj-2", "res-a", 3_600_000),  # 1 hr
        ]
        result = _run_attribution(rows, config)
        # With rate=0, all $100 is waste. Each gets 50%.
        # Mutation would give each $100 → total $200.
        total = sum(a["total_cost_attribution_usd"] for a in result["attributions"])
        assert total == 100.0
        for attr in result["attributions"]:
            assert attr["total_cost_attribution_usd"] == 50.0
