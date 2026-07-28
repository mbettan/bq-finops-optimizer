"""
Dedicated money-math unit tests for hbo.py.

Modelled on tests/test_physical_bytes_decomposition.py — the one file in the
repo that pins real dollars against real src code.

These tests exercise the financial math in analyze_hbo and get_hbo_summary:
  - Slot-hour conversion (total_slot_ms / 3600000)
  - M1 proportionality formula: saved_slots = post_slots × (prev - dur) / dur
  - Savings = saved_slot_hours × price_per_slot_hr
  - Percent time saved calculation
  - Monthly projection in get_hbo_summary
  - Zero/edge cases (div-by-zero guards, no optimized jobs)
  - DAYS_PER_MONTH constant correctness (365.25/12)

None of these tests make real BigQuery calls.
"""

from __future__ import annotations

import math
import pytest
from unittest.mock import patch, MagicMock

from src.hbo import (
    analyze_hbo,
    get_hbo_summary,
    HBOAnalyzeParams,
    HBOCommonParams,
    HBOResult,
    HBOSummary,
    DAYS_PER_MONTH,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_analyze_row(
    job_id: str = "job-1",
    duration_ms: int = 5000,
    total_slot_ms: int = 3_600_000,
    prev_exec_ms: int = 10000,
    project_id: str = "test-project",
):
    """Create a mock BQ row for the analyze_hbo query."""
    row = MagicMock()
    row.job_id = job_id
    row.project_id = project_id
    # creation_time needs .isoformat() support
    ct = MagicMock()
    ct.isoformat.return_value = "2026-07-28T00:00:00+00:00"
    row.creation_time = ct
    row.user_email = "user@test.com"
    row.query_hash = "abc123"
    row.start_time = None
    row.end_time = None
    row.duration_ms = duration_ms
    row.total_slot_ms = total_slot_ms
    row.prev_exec_ms = prev_exec_ms
    return row


def _make_summary_row(
    total_optimized_jobs: int = 10,
    total_saved_time_ms: int = 50000,
    total_saved_slot_hours: float = 5.0,
    avg_percent_time_saved: float = 30.0,
):
    """Create a mock BQ row for the get_hbo_summary query."""
    row = MagicMock()
    row.total_optimized_jobs = total_optimized_jobs
    row.total_saved_time_ms = total_saved_time_ms
    row.total_saved_slot_hours = total_saved_slot_hours
    row.avg_percent_time_saved = avg_percent_time_saved
    return row


def _make_analyze_params(**overrides):
    defaults = {
        "org_project_id": "test-org",
        "region": "us",
        "lookback_days": 7,
        "price_per_slot_hr": 0.06,
    }
    defaults.update(overrides)
    return HBOAnalyzeParams(**defaults)


def _make_common_params(**overrides):
    defaults = {
        "org_project_id": "test-org",
        "region": "us",
        "lookback_days": 7,
        "price_per_slot_hr": 0.06,
    }
    defaults.update(overrides)
    return HBOCommonParams(**defaults)


def _run_analyze(rows, **param_overrides):
    """Call analyze_hbo with mocked BQ."""
    params = _make_analyze_params(**param_overrides)

    with patch("src.hbo.init_bq_client_and_resolve_project") as mock_init, \
         patch("src.hbo._run_and_log", return_value=rows), \
         patch("src.hbo.build_project_filter", return_value=("", [])), \
         patch("src.hbo.log_endpoint_start"), \
         patch("src.hbo.log_endpoint_end"):
        mock_init.return_value = (MagicMock(), "test-admin")
        return analyze_hbo(params)


def _run_summary(rows, **param_overrides):
    """Call get_hbo_summary with mocked BQ."""
    params = _make_common_params(**param_overrides)

    with patch("src.hbo.init_bq_client_and_resolve_project") as mock_init, \
         patch("src.hbo._run_and_log", return_value=rows), \
         patch("src.hbo.build_project_filter", return_value=("", [])), \
         patch("src.hbo.log_endpoint_start"), \
         patch("src.hbo.log_endpoint_end"):
        mock_init.return_value = (MagicMock(), "test-admin")
        return get_hbo_summary(params)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

class TestConstants:
    """Verify the DAYS_PER_MONTH constant matches the expected value."""

    def test_days_per_month(self):
        """365.25 / 12 = 30.4375"""
        assert DAYS_PER_MONTH == pytest.approx(30.4375, abs=0.001)


# ---------------------------------------------------------------------------
# Analyze: slot-hour savings formula (M1)
# ---------------------------------------------------------------------------

class TestAnalyzeSavingsFormula:
    """saved_slot_hours = (total_slot_ms / 3600000) × (prev_ms - dur_ms) / dur_ms"""

    def test_exact_50_percent_time_saved(self):
        """prev=10000ms, dur=5000ms → 50% time saved.
        Slots = 1 hr × (10000-5000)/5000 = 1 hr saved."""
        rows = [_make_analyze_row(
            duration_ms=5000,
            total_slot_ms=3_600_000,  # 1 slot-hour
            prev_exec_ms=10000,
        )]
        results = _run_analyze(rows)
        assert len(results) == 1

        r = results[0]
        assert r.saved_slot_hours == pytest.approx(1.0, abs=0.001)
        assert r.estimated_savings_usd == pytest.approx(0.06, abs=0.001)
        assert r.percent_execution_time_saved == 50.0

    def test_90_percent_time_saved(self):
        """prev=10000ms, dur=1000ms → 90% time saved.
        Slots = 1 hr × (10000-1000)/1000 = 9 hrs saved."""
        rows = [_make_analyze_row(
            duration_ms=1000,
            total_slot_ms=3_600_000,
            prev_exec_ms=10000,
        )]
        results = _run_analyze(rows)
        r = results[0]
        assert r.saved_slot_hours == pytest.approx(9.0, abs=0.001)
        assert r.estimated_savings_usd == pytest.approx(0.54, abs=0.001)

    def test_custom_price_per_slot_hr(self):
        """Price parameter correctly affects dollar savings."""
        rows = [_make_analyze_row(
            duration_ms=5000,
            total_slot_ms=3_600_000,
            prev_exec_ms=10000,
        )]
        # Standard rate: 1 saved-hr × $0.06 = $0.06
        results_std = _run_analyze(rows, price_per_slot_hr=0.06)
        # Enterprise Plus: 1 saved-hr × $0.10 = $0.10
        results_ent = _run_analyze(rows, price_per_slot_hr=0.10)

        assert results_std[0].estimated_savings_usd == pytest.approx(0.06, abs=0.001)
        assert results_ent[0].estimated_savings_usd == pytest.approx(0.10, abs=0.001)

    def test_no_improvement_filtered_out(self):
        """prev_exec_ms <= 0 → row is skipped (no savings)."""
        rows = [_make_analyze_row(prev_exec_ms=0)]
        results = _run_analyze(rows)
        assert len(results) == 0

    def test_null_prev_exec_ms_filtered(self):
        """None prev_exec_ms → treated as 0 → skipped."""
        rows = [_make_analyze_row(prev_exec_ms=None)]
        results = _run_analyze(rows)
        assert len(results) == 0

    def test_null_total_slot_ms_treated_as_zero(self):
        """None total_slot_ms → 0 → saved_slot_hours = 0."""
        rows = [_make_analyze_row(
            duration_ms=5000,
            total_slot_ms=None,
            prev_exec_ms=10000,
        )]
        results = _run_analyze(rows)
        assert len(results) == 1
        assert results[0].saved_slot_hours == 0.0

    def test_duration_ms_zero_guard(self):
        """duration_ms=0 → clamped to 1 to avoid ZeroDivisionError."""
        rows = [_make_analyze_row(
            duration_ms=0,
            total_slot_ms=3_600_000,
            prev_exec_ms=10000,
        )]
        # Should not raise — duration is clamped to max(0, 1) = 1
        results = _run_analyze(rows)
        assert len(results) == 1
        # saved = 1hr × (10000-1)/1 ≈ 9999 slot-hours
        assert results[0].saved_slot_hours > 0


# ---------------------------------------------------------------------------
# Analyze: mutation detection
# ---------------------------------------------------------------------------

class TestAnalyzeMutationDetection:
    """These tests would have caught the §1 mutations."""

    def test_mutation_1_hbo_divisor(self):
        """Mutation #1: changing / 3600000.0 to / 360000.0 would inflate
        savings by 10×. Pin the exact conversion."""
        rows = [_make_analyze_row(
            duration_ms=5000,
            total_slot_ms=3_600_000,  # exactly 1 slot-hour
            prev_exec_ms=10000,
        )]
        results = _run_analyze(rows)
        # Correct: 1hr × (10000-5000)/5000 = 1.0 saved slot-hours
        # With 360000: 10 × 1.0 = 10.0 saved slot-hours
        assert results[0].saved_slot_hours == pytest.approx(1.0, abs=0.01)

    def test_mutation_1_dollar_impact(self):
        """Mutation #1 at $0.06/hr: 1.0 saved × $0.06 = $0.06, not $0.60."""
        rows = [_make_analyze_row(
            duration_ms=5000,
            total_slot_ms=3_600_000,
            prev_exec_ms=10000,
        )]
        results = _run_analyze(rows, price_per_slot_hr=0.06)
        assert results[0].estimated_savings_usd == pytest.approx(0.06, abs=0.001)


# ---------------------------------------------------------------------------
# Analyze: sorting and limiting
# ---------------------------------------------------------------------------

class TestAnalyzeSortingAndLimit:
    """Results sorted by saved_slot_hours DESC, limited to params.limit."""

    def test_sorted_by_saved_slot_hours_desc(self):
        rows = [
            _make_analyze_row(job_id="small", duration_ms=9000, total_slot_ms=360_000, prev_exec_ms=10000),
            _make_analyze_row(job_id="large", duration_ms=1000, total_slot_ms=36_000_000, prev_exec_ms=10000),
        ]
        results = _run_analyze(rows)
        assert results[0].job_id == "large"

    def test_limit_respected(self):
        rows = [
            _make_analyze_row(job_id=f"job-{i}", duration_ms=5000, total_slot_ms=3_600_000, prev_exec_ms=10000)
            for i in range(20)
        ]
        results = _run_analyze(rows, limit=5)
        assert len(results) == 5


# ---------------------------------------------------------------------------
# Summary: monthly projection
# ---------------------------------------------------------------------------

class TestSummaryMonthlyProjection:
    """get_hbo_summary projects lookback-window savings to monthly."""

    def test_7_day_lookback_projection(self):
        """7-day lookback: monthly = (saved/7) × 30.4375"""
        rows = [_make_summary_row(total_saved_slot_hours=70.0)]  # 70 hrs in 7 days
        result = _run_summary(rows, lookback_days=7)

        # daily = 70/7 = 10 hrs/day. Monthly = 10 × 30.4375 = 304.375
        assert result.total_saved_slot_hours == pytest.approx(304.375, abs=0.1)
        # USD: 304.375 × $0.06 = $18.2625
        assert result.total_estimated_savings_usd == pytest.approx(18.2625, abs=0.01)

    def test_30_day_lookback_projection(self):
        """30-day lookback: monthly = (saved/30) × 30.4375"""
        rows = [_make_summary_row(total_saved_slot_hours=300.0)]
        result = _run_summary(rows, lookback_days=30)

        # daily = 300/30 = 10 hrs/day. Monthly = 10 × 30.4375 = 304.375
        assert result.total_saved_slot_hours == pytest.approx(304.375, abs=0.1)

    def test_1_day_lookback(self):
        """1-day lookback: monthly = saved × 30.4375"""
        rows = [_make_summary_row(total_saved_slot_hours=10.0)]
        result = _run_summary(rows, lookback_days=1)

        assert result.total_saved_slot_hours == pytest.approx(304.375, abs=0.1)

    def test_custom_price_affects_summary(self):
        """Enterprise Plus pricing ($0.10) vs Standard ($0.04)."""
        rows = [_make_summary_row(total_saved_slot_hours=70.0)]

        result_std = _run_summary(rows, lookback_days=7, price_per_slot_hr=0.04)
        result_ent = _run_summary(rows, lookback_days=7, price_per_slot_hr=0.10)

        # Same slot savings, but different USD
        assert result_std.total_saved_slot_hours == result_ent.total_saved_slot_hours
        assert result_ent.total_estimated_savings_usd > result_std.total_estimated_savings_usd
        # Ratio should be exactly 0.10/0.04 = 2.5
        ratio = result_ent.total_estimated_savings_usd / result_std.total_estimated_savings_usd
        assert ratio == pytest.approx(2.5, abs=0.01)


# ---------------------------------------------------------------------------
# Summary: zero/empty results
# ---------------------------------------------------------------------------

class TestSummaryEmptyResults:
    """Summary with no optimized jobs returns zeros."""

    def test_no_rows_returns_zeros(self):
        """Empty BQ result → zero summary."""
        result = _run_summary([])
        assert result.total_optimized_jobs == 0
        assert result.total_saved_slot_hours == 0.0
        assert result.total_estimated_savings_usd == 0.0

    def test_null_slot_hours_treated_as_zero(self):
        """None total_saved_slot_hours → 0."""
        rows = [_make_summary_row(total_saved_slot_hours=None)]
        result = _run_summary(rows)
        assert result.total_saved_slot_hours == 0.0
        assert result.total_estimated_savings_usd == 0.0


# ---------------------------------------------------------------------------
# Summary: time_base field
# ---------------------------------------------------------------------------

class TestSummaryTimeBase:
    """The time_base field documents that values are monthly-projected."""

    def test_time_base_label(self):
        rows = [_make_summary_row()]
        result = _run_summary(rows)
        assert result.time_base == "monthly_projected"
