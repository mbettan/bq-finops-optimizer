"""
Regression test for the physical bytes decomposition in get_storage_metrics.

Ensures fail_safe_physical_bytes is never subtracted from active_physical_bytes
when computing active_core, which would cause fail-safe costs to silently cancel
out and underestimate physical billing.

Background:
  Per BigQuery INFORMATION_SCHEMA.TABLE_STORAGE docs:
    active_physical_bytes = live data + time_travel   (does NOT include fail-safe)
    fail_safe_physical_bytes is a separate column

  A past bug subtracted fail_safe from active_physical, causing FS × active_price
  to vanish from the physical cost forecast.

Ref: https://cloud.google.com/bigquery/docs/information-schema-table-storage#schema
"""

from __future__ import annotations

import math
import pytest
from unittest.mock import patch, MagicMock

from src.main import get_storage_metrics, StorageParams


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

GIB = 1024 ** 3


def _make_mock_row(**kwargs):
    """Create a dict-like mock row that supports both attribute and key access."""
    row = MagicMock()
    row.__getitem__ = lambda self, key: kwargs[key]
    for key, value in kwargs.items():
        setattr(row, key, value)
    return row


def _make_storage_row(
    project="proj-1",
    dataset="ds-1",
    active_logical_bytes=100 * GIB,
    long_term_logical_bytes=50 * GIB,
    active_physical_bytes=30 * GIB,     # = live (20 GiB) + time_travel (10 GiB)
    time_travel_physical_bytes=10 * GIB,
    fail_safe_physical_bytes=5 * GIB,
    long_term_physical_bytes=8 * GIB,
):
    """Build a single BQ row with known byte values."""
    return _make_mock_row(
        project_name=project,
        dataset_name=dataset,
        active_logical_bytes=active_logical_bytes,
        long_term_logical_bytes=long_term_logical_bytes,
        active_physical_bytes=active_physical_bytes,
        time_travel_physical_bytes=time_travel_physical_bytes,
        fail_safe_physical_bytes=fail_safe_physical_bytes,
        long_term_physical_bytes=long_term_physical_bytes,
    )


def _call_get_storage_metrics(rows, **param_overrides):
    """Call get_storage_metrics with mocked BQ query returning `rows`."""
    params = StorageParams(**param_overrides)
    mock_client = MagicMock()

    with patch("src.main.run_query_and_log", return_value=rows):
        with patch("src.main.build_project_filter", return_value=("", [])):
            return get_storage_metrics(mock_client, params)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestPhysicalBytesDecomposition:
    """Regression tests: fail-safe must be counted exactly once in physical cost."""

    def test_failsafe_included_in_physical_cost(self):
        """Core regression test: fail-safe bytes must contribute to forecast_physical.

        With known values:
          active_physical = 30 GiB (= 20 live + 10 TT)
          time_travel     = 10 GiB
          fail_safe       = 5 GiB
          long_term       = 8 GiB
          active_price    = $0.04/GiB, lt_price = $0.02/GiB

        Expected physical cost = (20 + 10 + 5) * 0.04 + 8 * 0.02 = 1.40 + 0.16 = 1.56
        """
        rows = [_make_storage_row()]
        results = _call_get_storage_metrics(
            rows,
            active_physical_price=0.04,
            long_term_physical_price=0.02,
        )

        assert len(results) == 1
        r = results[0]

        # Expected: (live=20 + tt=10 + fs=5) * 0.04 + lt=8 * 0.02
        expected_physical = (20 + 10 + 5) * 0.04 + 8 * 0.02
        assert r["forecast_physical"] == pytest.approx(expected_physical, rel=1e-9), (
            f"forecast_physical={r['forecast_physical']}, expected={expected_physical}. "
            "If fail_safe was subtracted from active_physical, it cancels out and "
            "the cost drops by fs * active_price."
        )

    def test_failsafe_not_subtracted_from_active_core(self):
        """Directly verify: active_core = active_physical - time_travel (NOT - fail_safe).

        The buggy formula was: active_core = active - tt - fs = 20 - 5 = 15
        The correct formula is: active_core = active - tt = 30 - 10 = 20
        """
        rows = [_make_storage_row()]
        results = _call_get_storage_metrics(rows)

        r = results[0]
        # With default rescale=1.0:
        # total_physical_gib = active_core + tt + fs + lt = 20 + 10 + 5 + 8 = 43
        expected_total = (30 - 10) + 10 + 5 + 8  # = 43 GiB
        assert r["total_physical_gib"] == pytest.approx(expected_total, rel=1e-9), (
            f"total_physical_gib={r['total_physical_gib']}, expected={expected_total}. "
            "If fail_safe was incorrectly subtracted from active_core, this will be 38."
        )

    def test_buggy_formula_would_fail(self):
        """Prove that the old buggy formula produces a DIFFERENT (wrong) result.

        Old: active_core = max(0, 30 - 10 - 5) = 15
             forecast_physical = (15 + 10 + 5) * 0.04 + 8 * 0.02 = 1.20 + 0.16 = 1.36

        Correct: active_core = max(0, 30 - 10) = 20
                 forecast_physical = (20 + 10 + 5) * 0.04 + 8 * 0.02 = 1.40 + 0.16 = 1.56
        """
        rows = [_make_storage_row()]
        results = _call_get_storage_metrics(
            rows,
            active_physical_price=0.04,
            long_term_physical_price=0.02,
        )

        r = results[0]
        buggy_value = (15 + 10 + 5) * 0.04 + 8 * 0.02  # 1.36
        assert r["forecast_physical"] != pytest.approx(buggy_value, rel=1e-9), (
            "forecast_physical matches the BUGGY value — fail_safe is being "
            "subtracted from active_physical, causing it to cancel out."
        )

    def test_zero_failsafe_unaffected(self):
        """When fail_safe = 0, both old and new formulas agree (the bug was invisible)."""
        rows = [_make_storage_row(fail_safe_physical_bytes=0)]
        results = _call_get_storage_metrics(
            rows,
            active_physical_price=0.04,
            long_term_physical_price=0.02,
        )

        r = results[0]
        # (20 + 10 + 0) * 0.04 + 8 * 0.02 = 1.20 + 0.16 = 1.36
        expected = (20 + 10 + 0) * 0.04 + 8 * 0.02
        assert r["forecast_physical"] == pytest.approx(expected, rel=1e-9)

    def test_time_travel_rescale_preserves_failsafe(self):
        """With time_travel_rescale < 1.0, fail-safe must still be fully counted.

        rescale = 0.5 means TT contribution halves, but FS must remain untouched.
          active_core = 30 - 10 = 20
          forecast_physical = (20 + 5 + 5) * 0.04 + 8 * 0.02 = 1.20 + 0.16 = 1.36
        """
        rows = [_make_storage_row()]
        results = _call_get_storage_metrics(
            rows,
            active_physical_price=0.04,
            long_term_physical_price=0.02,
            time_travel_rescale=0.5,
            time_travel_hours=48,  # required when rescale < 1.0
        )

        r = results[0]
        # tt_rescaled = 10 * 0.5 = 5
        expected = (20 + 5 + 5) * 0.04 + 8 * 0.02  # = 1.36
        assert r["forecast_physical"] == pytest.approx(expected, rel=1e-9)

    def test_large_failsafe_relative_to_active(self):
        """Even when fail_safe > live data, it must still be counted once.

        This is the scenario where the old bug's max(0, ...) clamp would activate,
        masking the subtraction but still producing wrong results.
        """
        rows = [_make_storage_row(
            active_physical_bytes=15 * GIB,   # live=5 + tt=10
            time_travel_physical_bytes=10 * GIB,
            fail_safe_physical_bytes=20 * GIB,  # larger than live data
            long_term_physical_bytes=8 * GIB,
        )]
        results = _call_get_storage_metrics(
            rows,
            active_physical_price=0.04,
            long_term_physical_price=0.02,
        )

        r = results[0]
        # active_core = 15 - 10 = 5 (live)
        # forecast_physical = (5 + 10 + 20) * 0.04 + 8 * 0.02 = 1.40 + 0.16 = 1.56
        expected = (5 + 10 + 20) * 0.04 + 8 * 0.02
        assert r["forecast_physical"] == pytest.approx(expected, rel=1e-9), (
            "With the old bug, max(0, 15-10-20) would clamp to 0, giving "
            f"{(0 + 10 + 20) * 0.04 + 8 * 0.02} instead of {expected}."
        )


# ---------------------------------------------------------------------------
# Algebraic Invariant Tests — no hardcoded expected values
#
# These tests encode mathematical PROPERTIES that must hold for ANY valid
# BigQuery storage row. They catch formula bugs regardless of specific values.
# ---------------------------------------------------------------------------


# Sweep across a diverse range of realistic byte values
_BYTE_SCENARIOS = [
    # (active_phys, time_travel, fail_safe, long_term)  — all in GiB
    (100, 10, 5, 50),       # typical dataset
    (10, 10, 0, 0),         # active == time_travel, no FS
    (10, 0, 0, 100),        # no time travel, mostly long-term
    (500, 200, 100, 300),   # large dataset, large FS
    (1, 0, 50, 0),          # tiny active, large FS (edge case)
    (1000, 999, 500, 1000), # near-max time travel
    (50, 10, 10, 10),       # equal FS and TT
    (0.1, 0.01, 0.001, 0.5),  # sub-GiB fractional values
]


def _reference_physical_cost(active_phys_gib, tt_gib, fs_gib, lt_gib,
                              active_price, lt_price, tt_rescale=1.0):
    """Canonical reference formula from Google's Example 3 documentation.

    Physical cost = (active_physical + fail_safe) * active_price + long_term * lt_price

    When tt_rescale != 1.0, we adjust:
      = (active_physical - tt + tt*rescale + fail_safe) * active_price + lt * lt_price

    This is the ground truth — if our code doesn't match this, it's wrong.
    """
    live = active_phys_gib - tt_gib
    return (live + tt_gib * tt_rescale + fs_gib) * active_price + lt_gib * lt_price


class TestAlgebraicInvariants:
    """Tests that verify mathematical properties — no fake expected values."""

    @pytest.mark.parametrize("active_phys,tt,fs,lt", _BYTE_SCENARIOS)
    def test_matches_google_reference_formula(self, active_phys, tt, fs, lt):
        """The optimizer must produce the same result as Google's canonical formula.

        This is the ultimate invariant: for any valid input, our result must equal
        (active_physical - tt + tt*rescale + fs) * active_price + lt * lt_price.
        No hardcoded expected value — the reference formula IS the test oracle.
        """
        active_price = 0.04
        lt_price = 0.02

        rows = [_make_storage_row(
            active_physical_bytes=int(active_phys * GIB),
            time_travel_physical_bytes=int(tt * GIB),
            fail_safe_physical_bytes=int(fs * GIB),
            long_term_physical_bytes=int(lt * GIB),
        )]
        results = _call_get_storage_metrics(
            rows, active_physical_price=active_price, long_term_physical_price=lt_price
        )
        r = results[0]

        expected = _reference_physical_cost(active_phys, tt, fs, lt, active_price, lt_price)
        assert r["forecast_physical"] == pytest.approx(expected, rel=1e-6), (
            f"Mismatch vs Google reference formula for "
            f"active={active_phys}, tt={tt}, fs={fs}, lt={lt}"
        )

    @pytest.mark.parametrize("active_phys,tt,fs,lt", _BYTE_SCENARIOS)
    def test_matches_reference_with_rescale(self, active_phys, tt, fs, lt):
        """Same as above but with time_travel_rescale=0.5 to catch rescale bugs."""
        active_price = 0.04
        lt_price = 0.02
        rescale = 0.5

        rows = [_make_storage_row(
            active_physical_bytes=int(active_phys * GIB),
            time_travel_physical_bytes=int(tt * GIB),
            fail_safe_physical_bytes=int(fs * GIB),
            long_term_physical_bytes=int(lt * GIB),
        )]
        results = _call_get_storage_metrics(
            rows,
            active_physical_price=active_price,
            long_term_physical_price=lt_price,
            time_travel_rescale=rescale,
            time_travel_hours=48,  # required when rescale < 1.0
        )
        r = results[0]

        expected = _reference_physical_cost(
            active_phys, tt, fs, lt, active_price, lt_price, tt_rescale=rescale
        )
        assert r["forecast_physical"] == pytest.approx(expected, rel=1e-6), (
            f"Mismatch vs reference (rescale={rescale}) for "
            f"active={active_phys}, tt={tt}, fs={fs}, lt={lt}"
        )

    @pytest.mark.parametrize("active_phys,tt,fs,lt", _BYTE_SCENARIOS)
    def test_failsafe_monotonicity(self, active_phys, tt, fs, lt):
        """Increasing fail_safe must ALWAYS increase forecast_physical.

        This is a monotonicity invariant: ∂(cost)/∂(fs) > 0.
        If fail_safe is being subtracted then re-added (cancelling out),
        doubling fs would have NO effect — this test catches that.
        """
        base_rows = [_make_storage_row(
            active_physical_bytes=int(active_phys * GIB),
            time_travel_physical_bytes=int(tt * GIB),
            fail_safe_physical_bytes=int(fs * GIB),
            long_term_physical_bytes=int(lt * GIB),
        )]
        doubled_fs_rows = [_make_storage_row(
            active_physical_bytes=int(active_phys * GIB),
            time_travel_physical_bytes=int(tt * GIB),
            fail_safe_physical_bytes=int((fs + 10) * GIB),  # add 10 GiB of FS
            long_term_physical_bytes=int(lt * GIB),
        )]

        base_result = _call_get_storage_metrics(base_rows)[0]
        doubled_result = _call_get_storage_metrics(doubled_fs_rows)[0]

        assert doubled_result["forecast_physical"] > base_result["forecast_physical"], (
            f"Adding 10 GiB of fail_safe did NOT increase physical cost. "
            f"base={base_result['forecast_physical']}, "
            f"with_extra_fs={doubled_result['forecast_physical']}. "
            "This means fail_safe is being cancelled out in the formula."
        )

    @pytest.mark.parametrize("active_phys,tt,fs,lt", _BYTE_SCENARIOS)
    def test_failsafe_sensitivity_is_exact(self, active_phys, tt, fs, lt):
        """The cost increase from +1 GiB of fail_safe must equal exactly active_price.

        This is the partial derivative test: ∂(cost)/∂(fs) = active_price.
        Any formula error (double-count, omission) will produce a different slope.
        """
        active_price = 0.044

        base_rows = [_make_storage_row(
            active_physical_bytes=int(active_phys * GIB),
            time_travel_physical_bytes=int(tt * GIB),
            fail_safe_physical_bytes=int(fs * GIB),
            long_term_physical_bytes=int(lt * GIB),
        )]
        plus_one_rows = [_make_storage_row(
            active_physical_bytes=int(active_phys * GIB),
            time_travel_physical_bytes=int(tt * GIB),
            fail_safe_physical_bytes=int((fs + 1) * GIB),  # +1 GiB
            long_term_physical_bytes=int(lt * GIB),
        )]

        base = _call_get_storage_metrics(base_rows, active_physical_price=active_price)[0]
        plus = _call_get_storage_metrics(plus_one_rows, active_physical_price=active_price)[0]

        delta = plus["forecast_physical"] - base["forecast_physical"]
        assert delta == pytest.approx(active_price, rel=1e-6), (
            f"∂cost/∂fs should be {active_price} but got {delta}. "
            f"If 0.0, fail_safe is being cancelled. "
            f"If {2*active_price}, fail_safe is double-counted."
        )

    @pytest.mark.parametrize("active_phys,tt,fs,lt", _BYTE_SCENARIOS)
    def test_physical_cost_never_negative(self, active_phys, tt, fs, lt):
        """Physical cost must always be >= 0 for any valid input."""
        rows = [_make_storage_row(
            active_physical_bytes=int(active_phys * GIB),
            time_travel_physical_bytes=int(tt * GIB),
            fail_safe_physical_bytes=int(fs * GIB),
            long_term_physical_bytes=int(lt * GIB),
        )]
        r = _call_get_storage_metrics(rows)[0]
        assert r["forecast_physical"] >= 0, "Physical cost must never be negative"
        assert r["total_physical_gib"] >= 0, "Physical volume must never be negative"

    @pytest.mark.parametrize("active_phys,tt,fs,lt", _BYTE_SCENARIOS)
    def test_conservation_active_plus_failsafe(self, active_phys, tt, fs, lt):
        """When rescale=1.0, total active-rate cost must equal
        (active_physical + fail_safe) * active_price.

        This is the simplest correct formula from Google's docs.
        Any decomposition bug breaks this identity.
        """
        active_price = 0.04

        rows = [_make_storage_row(
            active_physical_bytes=int(active_phys * GIB),
            time_travel_physical_bytes=int(tt * GIB),
            fail_safe_physical_bytes=int(fs * GIB),
            long_term_physical_bytes=int(lt * GIB),
        )]
        r = _call_get_storage_metrics(rows, active_physical_price=active_price)[0]

        # Strip out the long-term component to isolate the active-rate portion
        lt_cost = lt * 0.02  # default lt price
        active_rate_cost = r["forecast_physical"] - lt_cost

        expected_active_rate_cost = (active_phys + fs) * active_price
        assert active_rate_cost == pytest.approx(expected_active_rate_cost, rel=1e-6), (
            f"Active-rate portion should be (active_phys + fs) * price = "
            f"({active_phys} + {fs}) * {active_price} = {expected_active_rate_cost}, "
            f"but got {active_rate_cost}"
        )

