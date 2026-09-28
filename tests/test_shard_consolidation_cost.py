"""Tests for the date-shard consolidation cost estimate (#80).

On-demand CTAS is billed on the logical (uncompressed) bytes of every column
read from every scanned table. ``SELECT *`` over ``prefix_*`` reads all columns
of all shards, so the estimate uses ``total_logical_bytes`` (active and
long-term) priced at the region's on-demand rate.
"""

from types import SimpleNamespace
from unittest.mock import patch

from fastapi.testclient import TestClient

from src.main import app
from src.pricing import get_pricing

client = TestClient(app)

_ENDPOINT = "/api/storage/shard_consolidation"


def _row(**overrides):
    base = dict(
        project_id="proj-a",
        dataset_id="analytics",
        table_prefix="events",
        shard_count=30,
        min_date="20240101",
        max_date="20240130",
        total_logical_gib=0.0,
        total_physical_gib=0.0,
        total_scan_gib=0.0,
    )
    base.update(overrides)
    return SimpleNamespace(**base)


def _post(rows, region="region-us"):
    with patch("src.main.run_query_and_log") as mock_run:
        mock_run.return_value = rows
        response = client.post(_ENDPOINT, json={
            "org_project_id": "valid-proj",
            "region": region,
        })
    assert response.status_code == 200, response.text
    return response.json(), mock_run.call_args[0][1]


class TestShardConsolidationSql:
    def test_sql_sums_total_logical_bytes_as_scan_size(self):
        _, sql = _post([])
        assert "total_logical_bytes" in sql
        assert "SUM(total_logical_bytes) / POW(1024,3) AS total_scan_gib" in sql

    def test_sql_reads_table_storage_by_organization(self):
        _, sql = _post([])
        assert "INFORMATION_SCHEMA.TABLE_STORAGE_BY_ORGANIZATION" in sql


class TestShardConsolidationCost:
    def test_one_tib_logical_costs_regional_rate_in_us(self):
        data, _ = _post([_row(total_scan_gib=1024.0, total_physical_gib=100.0)])
        ddl = data[0]["ddl"]
        assert "~1,024.0 GiB of logical (uncompressed) data" in ddl
        assert "estimated on-demand cost: $6.25 at $6.25/TiB" in ddl

    def test_cost_ignores_physical_bytes(self):
        """Compressed physical size must not drive the scan estimate."""
        data, _ = _post([_row(total_scan_gib=2048.0, total_physical_gib=1.0)])
        assert "estimated on-demand cost: $12.50" in data[0]["ddl"]

    def test_long_term_shards_are_still_billed(self):
        """Shards older than 90 days have no active bytes but are still scanned."""
        data, _ = _post([_row(
            total_logical_gib=0.0,
            total_physical_gib=0.0,
            total_scan_gib=5120.0,
        )])
        ddl = data[0]["ddl"]
        assert "~5,120.0 GiB" in ddl
        assert "estimated on-demand cost: $31.25" in ddl

    def test_uses_region_specific_rate(self):
        region = "region-southamerica-east1"
        rate = get_pricing(region).on_demand_usd_per_tib
        assert rate != 6.25  # guard: the test needs a non-US rate
        data, _ = _post([_row(total_scan_gib=1024.0)], region=region)
        ddl = data[0]["ddl"]
        assert f"estimated on-demand cost: ${rate:,.2f} at ${rate:,.2f}/TiB" in ddl
        assert "$6.25/TiB" not in ddl

    def test_null_scan_size_yields_zero_cost(self):
        data, _ = _post([_row(total_scan_gib=None)])
        assert "estimated on-demand cost: $0.00" in data[0]["ddl"]

    def test_response_fields_unchanged(self):
        data, _ = _post([_row(
            total_logical_gib=12.5, total_physical_gib=3.0, total_scan_gib=40.0,
        )])
        item = data[0]
        assert item["total_logical_gib"] == 12.5
        assert item["total_physical_gib"] == 3.0
        assert "total_scan_gib" not in item
        assert "CREATE TABLE IF NOT EXISTS `proj-a.analytics.events`" in item["ddl"]


class TestOfflineSnapshotShardDdl:
    """The offline simulator sample must match what the endpoint returns."""

    def test_sample_shard_rows_match_endpoint(self):
        import json
        from pathlib import Path

        snapshot_path = Path(__file__).resolve().parents[1] / "docs" / "finops-snapshot_dummy.json"
        snapshot = json.loads(snapshot_path.read_text(encoding="utf-8"))
        rows = json.loads(snapshot["data"]["bq_shard_results"])
        assert rows, "sample snapshot has no shard recommendations"

        stale = []
        for row in rows:
            data, _ = _post([_row(
                project_id=row["project_id"],
                dataset_id=row["dataset_id"],
                table_prefix=row["table_prefix"],
                shard_count=row["shard_count"],
                min_date=row["min_date"],
                max_date=row["max_date"],
                total_logical_gib=row["total_logical_gib"],
                total_physical_gib=row["total_physical_gib"],
                total_scan_gib=row["total_logical_gib"],
            )])
            got = data[0]
            if got["ddl"] != row["ddl"] or got["warning_note"] != row["warning_note"]:
                stale.append(f"{row['dataset_id']}.{row['table_prefix']}")

        assert not stale, (
            f"sample shard DDL out of date for {stale}; regenerate "
            "bq_shard_results in docs/finops-snapshot_dummy.json from the endpoint"
        )
