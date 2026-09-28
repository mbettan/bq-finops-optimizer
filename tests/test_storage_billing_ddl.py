"""
Tests for storage billing model DDL from POST /api/storage/analyze.

The generated ALTER SCHEMA statement must:
  * use BigQuery's documented uppercase values ('PHYSICAL' / 'LOGICAL');
  * match the exact expected formatting;
  * start with a comment warning that the change takes up to 24 hours and
    locks the dataset's billing model for 14 days.

The API response itself is unchanged: better_on / currently_on stay lowercase
because the UI badges and report_generator read them.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from fastapi import HTTPException

from src.main import (
    STORAGE_BILLING_MODEL_WARNING,
    StorageParams,
    analyze_storage,
    build_storage_billing_ddl,
)

SNAPSHOT_PATH = Path(__file__).resolve().parents[1] / "docs" / "finops-snapshot_dummy.json"
LOWERCASE_ENUM_RE = re.compile(r"storage_billing_model='(physical|logical)'")
EXPECTED_ROW_KEYS = {
    "project_name", "dataset_name", "forecast_logical", "forecast_physical",
    "forecast_compare", "better_on", "currently_on", "monthly_spending",
    "monthly_savings", "monthly_savings_pct", "ddl",
}


def _assert_billing_ddl(ddl: str, project: str, dataset: str, model: str, tt_hours=None) -> None:
    """Shared checks for one generated billing-model statement."""
    *comment_lines, statement = ddl.splitlines()
    tt_clause = f", max_time_travel_hours={tt_hours}" if tt_hours is not None else ""
    # Exact match covers casing, spacing and the optional TT clause.
    assert statement == (
        f"ALTER SCHEMA `{project}.{dataset}` "
        f"SET OPTIONS(storage_billing_model='{model}'{tt_clause});"
    )
    assert comment_lines, "missing warning comment"
    assert all(line.startswith("--") for line in comment_lines)
    assert "24 hours" in ddl and "14 days" in ddl
    assert LOWERCASE_ENUM_RE.search(ddl) is None


# ---------------------------------------------------------------------------
# Helper: build_storage_billing_ddl
# ---------------------------------------------------------------------------

class TestBuildStorageBillingDdl:

    @pytest.mark.parametrize("tt_hours", [None, 48, 168])
    @pytest.mark.parametrize("target_model, expected", [
        ("physical", "PHYSICAL"),
        ("logical", "LOGICAL"),
        ("PHYSICAL", "PHYSICAL"),
        ("Logical", "LOGICAL"),
        (" physical ", "PHYSICAL"),
    ])
    def test_uppercase_value_warning_and_spacing(self, target_model, expected, tt_hours):
        ddl = build_storage_billing_ddl("my-proj", "my_dataset", target_model, tt_hours)
        _assert_billing_ddl(ddl, "my-proj", "my_dataset", expected, tt_hours)

    def test_warning_comes_first(self):
        ddl = build_storage_billing_ddl("my-proj", "my_dataset", "physical")
        assert ddl.startswith(STORAGE_BILLING_MODEL_WARNING + "\n")

    def test_domain_scoped_project_is_accepted(self):
        ddl = build_storage_billing_ddl("example.com:my-proj", "ds", "logical")
        _assert_billing_ddl(ddl, "example.com:my-proj", "ds", "LOGICAL")

    @pytest.mark.parametrize("bad_model", ["cold", "", None, "physical_v2"])
    def test_rejects_unknown_model(self, bad_model):
        with pytest.raises(ValueError):
            build_storage_billing_ddl("my-proj", "my_dataset", bad_model)

    @pytest.mark.parametrize("project, dataset", [
        ("my-proj`; DROP SCHEMA x; --", "ds"),
        ("my-proj", "ds`"),
        ("my-proj", "ds\nDROP SCHEMA x"),
        ("my proj", "ds"),
        ("", "ds"),
    ])
    def test_rejects_unsafe_identifiers(self, project, dataset):
        with pytest.raises(HTTPException) as exc_info:
            build_storage_billing_ddl(project, dataset, "physical")
        assert exc_info.value.status_code == 400


# ---------------------------------------------------------------------------
# Endpoint: analyze_storage (BigQuery helpers mocked, fully offline)
# ---------------------------------------------------------------------------

_METRICS = [
    # Physical forecast is cheaper -> recommend LOGICAL -> PHYSICAL.
    {"project_name": "proj-a", "dataset_name": "ds_to_physical",
     "forecast_logical": 500.0, "forecast_physical": 100.0, "total_physical_gib": 1000.0},
    # Logical forecast is cheaper -> recommend PHYSICAL -> LOGICAL.
    {"project_name": "proj-a", "dataset_name": "ds_to_logical",
     "forecast_logical": 100.0, "forecast_physical": 500.0, "total_physical_gib": 1000.0},
]


def _run_analyze_storage(**param_overrides):
    with patch("src.main.init_bq_client_and_resolve_project", return_value=(MagicMock(), "proj-a")), \
         patch("src.main.get_org_storage_billing_model", return_value="LOGICAL"), \
         patch("src.main.get_storage_metrics", return_value=_METRICS), \
         patch("src.main.get_physical_datasets", return_value=({("proj-a", "ds_to_logical")}, set())):
        return analyze_storage(StorageParams(**param_overrides))


class TestAnalyzeStorageDdl:

    @pytest.mark.parametrize("tt_hours", [None, 168])
    def test_every_recommendation_has_valid_ddl(self, tt_hours):
        overrides = {} if tt_hours is None else {"time_travel_hours": tt_hours}
        rows = {r["dataset_name"]: r for r in _run_analyze_storage(**overrides)["datasets"]}

        assert set(rows) == {"ds_to_physical", "ds_to_logical"}
        _assert_billing_ddl(rows["ds_to_physical"]["ddl"], "proj-a", "ds_to_physical", "PHYSICAL", tt_hours)
        _assert_billing_ddl(rows["ds_to_logical"]["ddl"], "proj-a", "ds_to_logical", "LOGICAL", tt_hours)

    def test_response_contract_unchanged(self):
        rows = {r["dataset_name"]: r for r in _run_analyze_storage()["datasets"]}

        for row in rows.values():
            assert set(row) == EXPECTED_ROW_KEYS
        assert (rows["ds_to_physical"]["currently_on"], rows["ds_to_physical"]["better_on"]) == ("logical", "physical")
        assert (rows["ds_to_logical"]["currently_on"], rows["ds_to_logical"]["better_on"]) == ("physical", "logical")


# ---------------------------------------------------------------------------
# Offline simulator sample data (docs/finops-snapshot_dummy.json)
# ---------------------------------------------------------------------------

class TestOfflineSnapshotDdl:

    def test_sample_storage_ddl_matches_helper(self):
        snapshot = json.loads(SNAPSHOT_PATH.read_text(encoding="utf-8"))
        rows = json.loads(snapshot["data"]["bq_storage_results"])["datasets"]
        assert rows, "sample snapshot has no storage recommendations"

        stale = []
        for row in rows:
            match = re.search(r"max_time_travel_hours=(\d+)\);$", row["ddl"])
            tt_hours = int(match.group(1)) if match else None
            expected = build_storage_billing_ddl(
                row["project_name"], row["dataset_name"], row["better_on"], tt_hours
            )
            if row["ddl"] != expected:
                stale.append(f"{row['project_name']}.{row['dataset_name']}")

        assert not stale, (
            f"{len(stale)} sample DDL(s) are out of date (e.g. {stale[:3]}); "
            "regenerate docs/finops-snapshot_dummy.json with build_storage_billing_ddl()"
        )
