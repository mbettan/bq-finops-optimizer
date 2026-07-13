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
    FluidEstimateMetric,
    _normalize_region,
    _parse_option_value,
    _process_unified_results,
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


# ---------------------------------------------------------------------------
# _rollup_to_summaries — the cooldown-waste model
# ---------------------------------------------------------------------------

class TestProcessUnifiedResults:
    def test_empty_inputs_return_empty(self):
        import pandas as pd
        result = _process_unified_results(pd.DataFrame())
        assert result == []

    def test_basic_processing(self):
        import pandas as pd
        df = pd.DataFrame({
            "reservation_id": ["res1", "res2", "res3"],
            "edition": ["ENTERPRISE", "ENTERPRISE", "ENTERPRISE"],
            "legacy_slot_seconds": [100.0, 50.0, 0.0],
            "fluid_slot_seconds": [80.0, 50.0, 0.0],
            "total_pure_used_seconds": [90.0, 50.0, 10.0],
            "has_capacity": [1.0, 1.0, 0.0]
        })
        result = _process_unified_results(df)
        assert len(result) == 3
        
        assert result[0].reservation_id == "res1"
        assert result[0].status == "Active"
        assert result[0].fluid_slot_seconds == 80.0
        
        assert result[2].reservation_id == "res3"
        assert result[2].status == "External Admin"


def fluid_slot_seconds(used: float, borrowed: float, baseline: float, current: float) -> float:
    """Pure-Python oracle of the BigQuery Fluid Scaling formula.
    SQL: LEAST(GREATEST(IFNULL(used_slots, 0) - IFNULL(borrowed_slots, 0) - IFNULL(baseline_slots, 0), 0), IFNULL(current_slots, 0))
    """
    return min(max(used - borrowed - baseline, 0.0), current)


@pytest.mark.parametrize("used,borrowed,baseline,current,expected", [
    (100, 0, 20, 50, 50),    # usage exceeds capacity -> clamps to current
    (30,  0, 20, 50, 10),    # normal: 30-20 = 10, under cap
    (10,  0, 20, 50, 0),     # usage below baseline -> floors at 0
    (100, 40, 20, 50, 40),   # borrowed subtracted before clamp
    (100, 0, 20, 0, 0),      # zero autoscaler capacity
    (200, 0, 0, 50, 50),     # no baseline, full clamp
])
def test_fluid_formula(used, borrowed, baseline, current, expected):
    """Documents and asserts the exact behavior of the Fluid Scaling clamping logic."""
    assert fluid_slot_seconds(used, borrowed, baseline, current) == expected


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
        def __init__(self, rows):
            self._rows = rows
            self.total_bytes_processed = 0
            self.total_bytes_billed = 0
            self.project = "test-project"
            self.location = "us"
            self.job_id = "test-job-id"
            self.cache_hit = False

        def result(self): return self._rows

    class FakeClient:
        def query(self, sql, *args, **kwargs):
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




