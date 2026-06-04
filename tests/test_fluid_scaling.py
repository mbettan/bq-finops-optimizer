"""
Unit tests for fluid_scaling.py — the financial model.
"""

from __future__ import annotations

import pandas as pd
import pytest

from src.fluid_scaling import (
    DAYS_PER_MONTH,
    DAYS_PER_YEAR,
    SECONDS_PER_HOUR,
    SECONDS_PER_MINUTE,
    FluidEstimateMetric,
    _normalize_region,
    _parse_option_value,
    _rollup_to_summaries,
    _safe_ident,
    _strip_qualifier,
    _to_metric,
    _ReservationSummary,
    _build_config_status,
    FluidScalingConfigStatus,
    get_effective_fluid_scaling_reservations,
)
from fastapi import HTTPException


# ---------------------------------------------------------------------------
# Fixtures and helpers
# ---------------------------------------------------------------------------

def _make_capacity(
    reservation_id: str,
    start: str,
    seconds: int,
    baseline: float,
    autoscale_current: float,
    edition: str = "enterprise",
) -> pd.DataFrame:
    """Build a per-second capacity DataFrame for one reservation."""
    return pd.DataFrame({
        "reservation_id": [reservation_id] * seconds,
        "edition": [edition] * seconds,
        "period_start": pd.date_range(start, periods=seconds, freq="s", tz="UTC"),
        "baseline_slots": [baseline] * seconds,
        "autoscale_current_slots": [autoscale_current] * seconds,
    })


def _make_usage(
    reservation_id: str,
    start: str,
    seconds: int,
    used: float,
    edition: str = "enterprise",
) -> pd.DataFrame:
    """Build a per-second usage DataFrame for one reservation."""
    return pd.DataFrame({
        "reservation_id": [reservation_id] * seconds,
        "edition": [edition] * seconds,
        "period_start": pd.date_range(start, periods=seconds, freq="s", tz="UTC"),
        "used_slots": [used] * seconds,
    })


def _empty_capacity() -> pd.DataFrame:
    return pd.DataFrame(columns=["reservation_id", "edition", "period_start", "baseline_slots", "autoscale_current_slots"])


def _empty_usage() -> pd.DataFrame:
    return pd.DataFrame(columns=["reservation_id", "edition", "period_start", "used_slots"])


# ---------------------------------------------------------------------------
# _rollup_to_summaries — the cooldown-waste model
# ---------------------------------------------------------------------------

class TestRollupToSummaries:
    """End-to-end validation of the per-second → rollup to summaries."""

    def test_empty_inputs_return_empty(self):
        result = _rollup_to_summaries(_empty_capacity(), _empty_usage())
        assert result == []

    def test_empty_capacity_with_usage(self):
        usage = pd.DataFrame({
            "reservation_id": ["r1"],
            "edition": ["enterprise"],
            "period_start": [pd.Timestamp("2025-01-01 00:00:00", tz="UTC")],
            "used_slots": [1000.0]
        })
        result = _rollup_to_summaries(_empty_capacity(), usage)
        assert len(result) == 1
        assert result[0].reservation_id == "r1"
        assert result[0].legacy_slot_seconds == 0
        assert result[0].fluid_slot_seconds == 0

    def test_empty_usage_with_capacity(self):
        capacity = _make_capacity("r1", "2025-01-01 00:00:00", seconds=60,
                                  baseline=0, autoscale_current=1000)
        result = _rollup_to_summaries(capacity, _empty_usage())
        assert len(result) == 1
        assert result[0].reservation_id == "r1"
        assert result[0].legacy_slot_seconds == 60000
        assert result[0].fluid_slot_seconds == 0


    def test_cooldown_waste_captured_correctly(self):
        # 60 seconds capacity with 1000 autoscaled slots
        # only 10 seconds of usage with 1000 used slots
        capacity = _make_capacity("r1", "2025-01-01 00:00:00", seconds=60,
                                  baseline=0, autoscale_current=1000)
        # Note: the usage df contains per-minute totals or per-second totals.
        # Since usage in _rollup_to_summaries is floored to minute and divided by 60,
        # we can provide 60 seconds with used_slots = 1000 * 10 (total slot-seconds for that minute is 10000)
        # Or we can just build one row for usage in that minute:
        usage = pd.DataFrame({
            "reservation_id": ["r1"],
            "edition": ["enterprise"],
            "period_start": [pd.Timestamp("2025-01-01 00:00:00", tz="UTC")],
            "used_slots": [1000.0 * 10]  # 1000 slots used for 10 seconds = 10000 slot-seconds total for the minute
        })
        
        summaries = _rollup_to_summaries(capacity, usage)
        assert len(summaries) == 1
        s = summaries[0]
        assert s.reservation_id == "r1"
        assert s.legacy_slot_seconds == 1000 * 60
        assert s.fluid_slot_seconds == 1000 * 10

    def test_steady_state_no_savings(self):
        capacity = _make_capacity("r1", "2025-01-01 00:00:00", seconds=60,
                                  baseline=0, autoscale_current=500)
        usage = pd.DataFrame({
            "reservation_id": ["r1"],
            "edition": ["enterprise"],
            "period_start": [pd.Timestamp("2025-01-01 00:00:00", tz="UTC")],
            "used_slots": [500.0 * 60]  # 500 slots used for 60 seconds
        })
        s = _rollup_to_summaries(capacity, usage)[0]
        assert s.legacy_slot_seconds == 30_000
        assert s.fluid_slot_seconds == 30_000

    def test_idle_reservation_baseline_only(self):
        # Pure baseline reservation, no autoscaler current slots
        capacity = _make_capacity("r1", "2025-01-01 00:00:00", seconds=60,
                                  baseline=200, autoscale_current=0)
        usage = _empty_usage()
        s = _rollup_to_summaries(capacity, usage)[0]
        assert s.legacy_slot_seconds == 0
        assert s.fluid_slot_seconds == 0

    def test_autoscale_burst_with_baseline(self):
        capacity = _make_capacity("r1", "2025-01-01 00:00:00", seconds=60,
                                  baseline=100, autoscale_current=900)  # provisioned = 1000
        usage = pd.DataFrame({
            "reservation_id": ["r1"],
            "edition": ["enterprise"],
            "period_start": [pd.Timestamp("2025-01-01 00:00:00", tz="UTC")],
            "used_slots": [1000.0 * 10]  # 1000 slots used for 10 seconds
        })
        s = _rollup_to_summaries(capacity, usage)[0]
        # Legacy slot seconds (autoscaled_current_slots) = 900 * 60 = 54000
        assert s.legacy_slot_seconds == 900 * 60
        # Fluid slot seconds = SUM(autoscaled_used_slots)
        # Minute-aggregate clamp (NO /60):
        #   legacy = Σ autoscale_current = 900 * 60 = 54000
        #   baseline_slot_seconds       = 100 * 60 = 6000
        #   autoscale_capacity_ss       = 900 * 60 = 54000
        #   used_above_baseline         = 10000 - 6000 = 4000
        #   fluid = MIN(4000, 54000)    = 4000
        assert abs(s.fluid_slot_seconds - 4000.0) < 1.0

    def test_doc_vs_clamped_savings_and_metric(self):
        capacity = _make_capacity("r1", "2025-01-01 00:00:00", seconds=60,
                                  baseline=500, autoscale_current=100)
        usage = pd.DataFrame({
            "reservation_id": ["r1"],
            "edition": ["enterprise"],
            "period_start": [pd.Timestamp("2025-01-01 00:00:00", tz="UTC")],
            "used_slots": [550.0 * 60]  # 33000 total slot-seconds
        })
        summaries = _rollup_to_summaries(capacity, usage)
        assert len(summaries) == 1
        s = summaries[0]
        assert s.legacy_slot_seconds == 6000.0
        assert s.fluid_slot_seconds == 3000.0
        assert s.total_pure_used_seconds == 33000.0
        
        m = _to_metric(s, price_per_slot_hr=0.06, lookback_days=7)
        assert m.clamped_pct_savings == 50.0


    def test_usage_not_duplicated_on_minute_merge(self):
        """Regression: per-second usage rows must be summed to minute grain BEFORE
        merging onto per-minute capacity, or capacity fans out 60x."""
        # 60 seconds of capacity at 100 autoscale slots → legacy = 100*60 = 6000
        capacity = _make_capacity("r1", "2025-01-01 00:00:00", seconds=60,
                                  baseline=0, autoscale_current=100)
        # 60 SECOND-LEVEL usage rows in the same minute (the trigger for the bug)
        usage = _make_usage("r1", "2025-01-01 00:00:00", seconds=60, used=50.0)  # 50 slots each second

        s = _rollup_to_summaries(capacity, usage)[0]

        # legacy MUST stay 6000, NOT 6000*60 = 360000 (the bug)
        assert s.legacy_slot_seconds == 6000, (
            f"Capacity fanned out: got {s.legacy_slot_seconds}, expected 6000 "
            f"(60x bug reintroduced)"
        )
        # used = 50*60 = 3000 slot-seconds for the minute
        assert s.total_pure_used_seconds == 3000

    def test_metric_field_names_are_stable(self):
        expected_fields = {
            "reservation_id",
            "reservation_short_name",
            "fluid_autoscaler_slot_hours",
            "legacy_autoscaler_slot_hours",
            "total_pure_used_slot_hours",
            "slot_hours_saved",
            "clamped_pct_savings",
            "estimated_usd_saved_window",
            "extrapolated_monthly_usd",
            "extrapolated_annual_usd",
            "status",
        }
        actual_fields = set(FluidEstimateMetric.model_fields.keys())
        assert actual_fields == expected_fields


# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------

class TestSafeIdent:
    @pytest.mark.parametrize("value", [
        "my-project",
        "my_project",
        "project123",
        "region-us",
        "region-us-east4",
        "example.com:legacy-project",
    ])
    def test_valid_inputs_pass_through(self, value):
        assert _safe_ident(value, "test") == value

    @pytest.mark.parametrize("value", [
        "",
        "has spaces",
        "drop;table",
        "back`tick",
        'quote"in',
        "newline\n",
    ])
    def test_invalid_inputs_raise_400(self, value):
        with pytest.raises(HTTPException) as exc:
            _safe_ident(value, "test")
        assert exc.value.status_code == 400


class TestNormalizeRegion:
    def test_already_prefixed(self):
        assert _normalize_region("region-us-east4") == "region-us-east4"

    def test_unprefixed(self):
        assert _normalize_region("us-east4") == "region-us-east4"

    def test_empty_default(self):
        assert _normalize_region("") == "region-us"

    def test_none_default(self):
        assert _normalize_region(None) == "region-us"

    def test_whitespace_stripped(self):
        assert _normalize_region("  us-east4  ") == "region-us-east4"


class TestStripQualifier:
    def test_dot_separated(self):
        assert _strip_qualifier("project.location.my-res") == "my-res"

    def test_colon_and_dot(self):
        assert _strip_qualifier("project:location.my-res") == "my-res"

    def test_unqualified(self):
        assert _strip_qualifier("my-res") == "my-res"

    def test_none(self):
        assert _strip_qualifier(None) == "(unassigned)"

    def test_empty(self):
        assert _strip_qualifier("") == "(unassigned)"


class TestParseOptionValue:
    def test_bracketed_quoted_list(self):
        assert _parse_option_value('["a", "b", "c"]') == {"a", "b", "c"}

    def test_unbracketed_list(self):
        assert _parse_option_value('"a", "b"') == {"a", "b"}

    def test_single_quotes(self):
        assert _parse_option_value("['a', 'b']") == {"a", "b"}

    def test_empty_string(self):
        assert _parse_option_value("") == set()

    def test_whitespace_in_items(self):
        assert _parse_option_value('[ "a" , "b" ]') == {"a", "b"}

    def test_empty_brackets(self):
        assert _parse_option_value("[]") == set()


# ---------------------------------------------------------------------------
# _build_config_status — actionable-reservation / DDL recommendation logic
# ---------------------------------------------------------------------------

def _summary(reservation_id: str, status: str) -> _ReservationSummary:
    """Minimal summary; only reservation_id and status matter for config logic."""
    return _ReservationSummary(
        reservation_id=reservation_id,
        legacy_slot_seconds=0.0,
        fluid_slot_seconds=0.0,
        total_pure_used_seconds=0.0,
        status=status,
    )


class TestBuildConfigStatus:
    ORG = "my-org-project"
    REGION = "region-us-east4"

    def test_external_admin_excluded_from_config_ddl(self):
        """
        Regression: 'External Admin' reservations are owned by a different admin
        project and CANNOT be enabled via ALTER PROJECT here, so they must not
        appear in missing_reservations nor leak into the generated DDL.
        """
        summaries = [
            _summary("proj:us-east4.res-default", "Active"),
            _summary("default-pipeline", "External Admin"),
        ]
        cfg = _build_config_status(
            summaries, enabled_reservations=set(),
            admin_project=self.ORG, region=self.REGION,
        )

        assert cfg.missing_reservations == ["res-default"]
        assert "default-pipeline" not in cfg.missing_reservations
        assert cfg.ddl is not None
        # The External Admin reservation must never appear in the DDL payload.
        assert "default-pipeline" not in cfg.ddl
        assert "res-default" in cfg.ddl
        assert cfg.enabled is False

    def test_unassigned_excluded(self):
        summaries = [
            _summary("", "Active"),                       # -> "(unassigned)"
            _summary("proj:us-east4.res-a", "Active"),
        ]
        cfg = _build_config_status(
            summaries, enabled_reservations=set(),
            admin_project=self.ORG, region=self.REGION,
        )
        assert cfg.missing_reservations == ["res-a"]
        assert "(unassigned)" not in cfg.missing_reservations
        assert "(unassigned)" not in (cfg.ddl or "")

    def test_all_enabled_reports_fully_enabled_no_ddl(self):
        summaries = [
            _summary("proj:us-east4.res-a", "Active"),
            _summary("proj:us-east4.res-b", "Idle"),
        ]
        cfg = _build_config_status(
            summaries,
            enabled_reservations={"res-a", "res-b"},
            admin_project=self.ORG, region=self.REGION,
        )
        assert cfg.enabled is True
        assert cfg.missing_reservations == []
        assert sorted(cfg.configured_reservations) == ["res-a", "res-b"]
        assert cfg.ddl is None

    def test_ddl_unions_existing_enabled_entries(self):
        """DDL must not drop reservations already enabled in the option list."""
        summaries = [_summary("proj:us-east4.res-new", "Active")]
        cfg = _build_config_status(
            summaries,
            enabled_reservations={"res-existing"},
            admin_project=self.ORG, region=self.REGION,
        )
        assert cfg.missing_reservations == ["res-new"]
        # Both the pre-existing and the new reservation must be present in the DDL.
        assert "res-existing" in cfg.ddl
        assert "res-new" in cfg.ddl
        # Sorted, comma-joined, double-quoted
        assert '["res-existing", "res-new"]' in cfg.ddl

    def test_external_admin_not_counted_as_configured_either(self):
        """
        An External Admin reservation that happens to be in the enabled set
        should not be surfaced as 'configured' here — it's outside this
        project's actionable scope.
        """
        summaries = [
            _summary("default-pipeline", "External Admin"),
            _summary("proj:us-east4.res-a", "Active"),
        ]
        cfg = _build_config_status(
            summaries,
            enabled_reservations={"default-pipeline", "res-a"},
            admin_project=self.ORG, region=self.REGION,
        )
        assert cfg.configured_reservations == ["res-a"]
        assert "default-pipeline" not in cfg.configured_reservations
        assert cfg.enabled is True   # res-a is the only actionable one, and it's enabled
        assert cfg.missing_reservations == []

    def test_ddl_targets_admin_project_and_region(self):
        summaries = [_summary("proj:us-east4.res-a", "Active")]
        cfg = _build_config_status(
            summaries, enabled_reservations=set(),
            admin_project=self.ORG, region=self.REGION,
        )
        assert f"ALTER PROJECT `{self.ORG}`" in cfg.ddl
        assert f"`{self.REGION}.preflight_fluid_autoscaling_reservations`" in cfg.ddl

    def test_empty_summaries_fully_enabled_no_ddl(self):
        cfg = _build_config_status(
            [], enabled_reservations=set(),
            admin_project=self.ORG, region=self.REGION,
        )
        assert cfg.enabled is True
        assert cfg.missing_reservations == []
        assert cfg.configured_reservations == []
        assert cfg.ddl is None


# ---------------------------------------------------------------------------
# Column-count contract — guards against thead/model/sort-index drift
# ---------------------------------------------------------------------------

class TestColumnContract:
    """The estimate table renders exactly the FluidEstimateMetric fields the
    frontend maps. If this count changes, the HTML <thead>, the empty-state
    colspan, and the DataTables `order` index ALL must be updated together."""

    # The 10 columns rendered by renderResults(), in display order.
    EXPECTED_ESTIMATE_COLUMNS = [
        "reservation_id",          # 0  Reservation ID
        "status",                  # 1  Status
        "legacy_autoscaler_slot_hours",   # 2  Legacy Slot-Hrs
        "fluid_autoscaler_slot_hours",    # 3  Fluid Slot-Hrs
        "total_pure_used_slot_hours",     # 4  Total Used Slot-Hrs
        "slot_hours_saved",        # 5  Recoverable Slot-Hrs
        "clamped_pct_savings",     # 6  % Savings
        "estimated_usd_saved_window",     # 7  $ Window
        "extrapolated_monthly_usd",       # 8  $ Monthly
        "extrapolated_annual_usd",        # 9  $ Annual  <-- DataTables sort index
    ]

    def test_estimate_table_is_ten_columns(self):
        """PRD §4.1 erroneously says 11; the contract is 10."""
        assert len(self.EXPECTED_ESTIMATE_COLUMNS) == 10

    def test_annual_is_last_column_for_sort_index(self):
        """`order: [[9, 'desc']]` must point at extrapolated_annual_usd."""
        assert self.EXPECTED_ESTIMATE_COLUMNS[9] == "extrapolated_annual_usd"
        assert self.EXPECTED_ESTIMATE_COLUMNS.index("extrapolated_annual_usd") == 9

    def test_all_displayed_columns_exist_on_model(self):
        """Every rendered column (except the two display-only ones) must be a
        real FluidEstimateMetric field, so the JS row template can't reference
        a key that doesn't exist."""
        model_fields = set(FluidEstimateMetric.model_fields.keys())
        for col in self.EXPECTED_ESTIMATE_COLUMNS:
            assert col in model_fields, f"Rendered column {col!r} missing from model"


def test_config_status_matches_short_unquoted_option():
    """Real-world option format is unquoted short names: [res-a, res-b].
    Active reservations (short names) must be recognized as configured, not missing."""
    enabled = _parse_option_value("[res-standard-sandbox, res-default, res-standard-experiments]")
    assert enabled == {"res-standard-sandbox", "res-default", "res-standard-experiments"}

    active = {"res-default", "res-standard-experiments", "res-standard-sandbox"}
    enabled_norm = {_strip_qualifier(r) for r in enabled}
    active_norm = {_strip_qualifier(r) for r in active}
    assert (active_norm - enabled_norm) == set(), "All reservations should be configured, none missing"


def test_effective_options_empty_falls_back_to_project_options(monkeypatch):
    """EFFECTIVE_PROJECT_OPTIONS returning 0 rows must NOT report 'not enabled' —
    it must fall through to PROJECT_OPTIONS. Reproduces the real-org bug where
    preflight_fluid_autoscaling_reservations is absent from the EFFECTIVE view."""

    class FakeRow:
        def __init__(self, v): self.option_value = v

    class FakeQueryJob:
        def __init__(self, rows): self._rows = rows
        def result(self): return self._rows

    class FakeClient:
        def query(self, sql):
            if "EFFECTIVE_PROJECT_OPTIONS" in sql:
                return FakeQueryJob([])  # empty, like the real bug
            if "PROJECT_OPTIONS" in sql:
                return FakeQueryJob([FakeRow("[res-default, res-standard-sandbox, res-standard-experiments]")])
            return FakeQueryJob([])

    result = get_effective_fluid_scaling_reservations(FakeClient(), "admin-proj", "region-us-east4")
    assert result == {"res-default", "res-standard-sandbox", "res-standard-experiments"}, \
        f"Expected fallback to PROJECT_OPTIONS, got {result}"


def test_parse_option_value_quoted_and_unquoted():
    """BigQuery stores the value UNQUOTED in PROJECT_OPTIONS even when set with
    quotes (confirmed: real org returned '[res-a, res-b]'). Parser must handle
    quoted, unquoted, and single-quoted forms."""
    assert _parse_option_value('["res-a", "res-b"]') == {"res-a", "res-b"}
    assert _parse_option_value('[res-a, res-b]') == {"res-a", "res-b"}        # the REAL format
    assert _parse_option_value("['res-a', 'res-b']") == {"res-a", "res-b"}
    assert _parse_option_value('[res-standard-sandbox, res-default, res-standard-experiments]') == \
        {"res-standard-sandbox", "res-default", "res-standard-experiments"}


def test_config_status_recognizes_enabled_short_names():
    """Active reservations (short names) present in the option must be reported
    as configured, NOT missing."""
    enabled = {_strip_qualifier(r) for r in
               _parse_option_value("[res-default, res-standard-experiments, res-standard-sandbox]")}
    active = {_strip_qualifier(r) for r in
              {"res-default", "res-standard-experiments", "res-standard-sandbox"}}
    assert (active - enabled) == set(), "All reservations should be configured"




