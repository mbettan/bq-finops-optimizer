"""#77: Active Assist on-demand savings are priced per binary TiB.

`overview.bytesSavedMonthly` is a byte count; on-demand is billed per TiB
(2**40 bytes), so savings = bytes / 2**40 * regional $/TiB.
"""

from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from src.main import (
    active_assist_bytes_saved_monthly,
    active_assist_editions_savings,
    active_assist_on_demand_savings,
    app,
    get_pricing,
)

TIB = 1024 ** 4
US_RATE = 6.25  # $/TiB used for the pure-function tests

client = TestClient(app)


# ── Pure helpers ──────────────────────────────────────────────────────────


class TestOnDemandSavings:
    def test_one_tib_costs_the_per_tib_rate(self):
        assert active_assist_on_demand_savings(None, {"bytesSavedMonthly": TIB}, US_RATE) == pytest.approx(6.25)

    def test_decimal_terabyte_is_no_longer_inflated(self):
        # 10**12 bytes is 0.909 TiB -> $5.684
        got = active_assist_on_demand_savings(None, {"bytesSavedMonthly": 10 ** 12}, US_RATE)
        assert got == pytest.approx(10 ** 12 / TIB * 6.25)
        assert got == pytest.approx(5.684, abs=1e-3)

    def test_int64_serialized_as_json_string(self):
        overview = {"bytesSavedMonthly": str(TIB)}
        assert active_assist_on_demand_savings(None, overview, US_RATE) == pytest.approx(6.25)

    def test_regional_rate_is_applied(self):
        assert active_assist_on_demand_savings(None, {"bytesSavedMonthly": 2 * TIB}, 7.5) == pytest.approx(15.0)

    def test_bytes_field_beats_tb_field(self):
        overview = {"bytesSavedMonthly": TIB, "bytesSavedMonthlyTb": 5}
        assert active_assist_on_demand_savings(None, overview, US_RATE) == pytest.approx(6.25)

    def test_tb_field_is_last_resort_and_read_conservatively(self):
        # read as decimal TB, the more conservative of the two possible units
        assert active_assist_bytes_saved_monthly({"bytesSavedMonthlyTb": 1.0}) == 10 ** 12
        got = active_assist_on_demand_savings(None, {"bytesSavedMonthlyTb": 1.0}, US_RATE)
        assert got == pytest.approx(5.684, abs=1e-3)

    def test_bytes_beat_cost_projection(self):
        impact = {"cost_projection": {"cost_in_local_currency": -999.0}}
        assert active_assist_on_demand_savings(impact, {"bytesSavedMonthly": TIB}, US_RATE) == pytest.approx(6.25)

    def test_plain_numeric_projection_is_fallback_with_sign_dropped(self):
        impact = {"cost_projection": {"cost_in_local_currency": -120.5}}
        assert active_assist_on_demand_savings(impact, {}, US_RATE) == pytest.approx(120.5)

    @pytest.mark.parametrize("cost_proj", [
        {"cost_in_local_currency": {"currency_code": "USD", "units": -120, "nanos": 0}},  # Money (#83)
        {"cost_savings": 99.0},  # not a real field
        {"cost_in_local_currency": "abc"},
    ])
    def test_unsupported_projection_shapes_never_raise(self, cost_proj):
        assert active_assist_on_demand_savings({"cost_projection": cost_proj}, {}, US_RATE) == 0.0

    @pytest.mark.parametrize("overview", [
        {}, None, "not-a-dict", {"bytesSavedMonthly": "abc"}, {"bytesSavedMonthly": -5},
        {"bytesSavedMonthly": True}, {"bytesSavedMonthly": float("nan")},
    ])
    def test_missing_or_bad_bytes_yield_zero(self, overview):
        assert active_assist_on_demand_savings(None, overview, US_RATE) == 0.0


class TestEditionsSavings:
    def test_slot_ms_to_slot_hours(self):
        assert active_assist_editions_savings({"slotMsSavedMonthly": 3_600_000}, 0.06) == pytest.approx(0.06)

    def test_string_and_bad_values(self):
        assert active_assist_editions_savings({"slotMsSavedMonthly": "7200000"}, 0.06) == pytest.approx(0.12)
        assert active_assist_editions_savings({"slotMsSavedMonthly": "abc"}, 0.06) == 0.0
        assert active_assist_editions_savings(None, 0.06) == 0.0


# ── Endpoint ──────────────────────────────────────────────────────────────


def _row(table, overview, primary_impact=None, description="Cluster this table"):
    return {
        "project_id": "proj-a",
        "target_resources": [f"//bigquery.googleapis.com/projects/proj-a/datasets/ds/tables/{table}"],
        "description": description,
        "primary_impact": primary_impact or {"category": "COST", "cost_projection": None},
        "additional_details": {"overview": overview},
    }


class TestActiveAssistEndpoint:
    def test_tib_pricing_and_bad_row_does_not_fail_the_list(self):
        rp = get_pricing("region-us")
        rows = [
            _row("good", {"bytesSavedMonthly": str(TIB), "slotMsSavedMonthly": 3_600_000,
                          "clusterColumns": ["a", "b"]}),
            # structured Money projection: row kept, savings 0.0
            _row("money", {}, {"category": "COST", "cost_projection": {
                "cost_in_local_currency": {"currency_code": "USD", "units": -10, "nanos": 0}}}),
            # overview delivered as JSON text
            _row("textjson", '{"bytesSavedMonthly": "%d"}' % (2 * TIB)),
        ]
        with patch("src.main.run_query_and_log", return_value=rows):
            response = client.post("/api/storage/active_assist", json={
                "org_project_id": "valid-proj", "region": "region-us",
            })
        assert response.status_code == 200
        by_table = {r["table_id"]: r for r in response.json()}
        assert set(by_table) == {"good", "money", "textjson"}

        assert by_table["good"]["on_demand_monthly_savings"] == pytest.approx(rp.on_demand_usd_per_tib)
        assert by_table["good"]["editions_monthly_savings"] == pytest.approx(rp.editions_slot_hr_rate)
        assert by_table["good"]["cluster_columns"] == ["a", "b"]
        assert by_table["money"]["on_demand_monthly_savings"] == 0.0
        assert by_table["textjson"]["on_demand_monthly_savings"] == pytest.approx(2 * rp.on_demand_usd_per_tib)
