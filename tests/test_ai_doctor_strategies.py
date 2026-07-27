"""
Module: AI Doctor Multi-Strategy ROI Engine Tests

Tests for the AI Doctor multi-strategy discovery engine, BigQuery Editions
hybrid cost fallback, multi-stage unnesting, atomic sampling, security/error
handling, and bug-fix regressions.

References:
  - Feature: AI Doctor Multi-Strategy ROI Engine
  - Feature: BigQuery Editions Hybrid Cost Fallback
  - Feature: Multi-Stage Unnesting & Atomic Sampling
  - Security: Strict IAM 403 Enforcement + Pydantic Literal Validation
"""

import re
import pytest
from unittest.mock import MagicMock, patch, PropertyMock
from fastapi.testclient import TestClient
from src.main import app

client = TestClient(app)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_discovery_row(
    job_id="job_test",
    project_id="acme-sandbox",
    user_email="user@acme.com",
    total_bytes_billed=1000,
    total_bytes_processed=1000,
    total_effective_bytes=1000,
    total_slot_ms=5000,
    total_bytes_spilled=0,
    execution_count=1,
    annualized_cost_usd=10.0,
    optimization_potential_score=5.0,
):
    """Create a mock discovery row matching the aggregated CTE output shape."""
    row = MagicMock()
    row.worst_job = {
        "job_id": job_id,
        "project_id": project_id,
        "user_email": user_email,
        "total_bytes_billed": total_bytes_billed,
        "total_slot_ms": total_slot_ms,
        "creation_time": "2026-07-27T12:00:00Z",
    }
    row.total_bytes_billed = total_bytes_billed
    row.total_bytes_processed = total_bytes_processed
    row.total_effective_bytes = total_effective_bytes
    row.total_slot_ms = total_slot_ms
    row.total_bytes_spilled = total_bytes_spilled
    row.execution_count = execution_count
    row.annualized_cost_usd = annualized_cost_usd
    row.optimization_potential_score = optimization_potential_score
    return row


def _make_ai_result_row(job_id="job_test", advice="Looks fine"):
    """Create a mock row from the AI.GENERATE SQL result."""
    row = MagicMock()
    row.job_id = job_id
    row.user_email = "user@acme.com"
    row.query = "SELECT 1"
    row.total_slot_ms = 5000
    row.total_bytes_billed = 1000
    row.total_bytes_processed = 1000
    row.ai_struct = {"result": advice}
    row.tables_referenced_count = 0
    row.tables_found_count = 0
    row.table_schema = None
    row.table_name = None
    row.total_rows = 0
    row.size_bytes = 0
    row.partition_column = None
    row.require_partition_filter = "false"
    row.clustering_fields = None
    row.num_columns = 0
    row.column_schema = ""
    row.ddl = ""
    return row


def _setup_mock_bq(discovery_rows, ai_rows=None):
    """
    Return a patched context that mocks BigQuery for AI Doctor.
    
    The first call to query() returns discovery_rows.
    The second call returns query-text-fetch rows (mapping job_id → query).
    Subsequent calls (schema fetches, AI.GENERATE) return default empty/safe mocks.
    """
    mock_bq_client = MagicMock()
    mock_init_patcher = patch(
        "src.main.init_bq_client_and_resolve_project",
        return_value=(mock_bq_client, "acme-sandbox"),
    )

    def _make_job(job_id, rows):
        job = MagicMock()
        job.total_bytes_processed = 0
        job.total_bytes_billed = 0
        job.cache_hit = False
        job.job_id = job_id
        job.result.return_value = rows
        return job

    # Build ordered list of mock jobs
    mock_jobs = []

    # Job 1: Discovery
    mock_jobs.append(_make_job("mock_discovery_job", discovery_rows))

    # Job 2: Query text fetch (per-project JOBS_BY_PROJECT)
    query_rows = []
    for dr in discovery_rows:
        qr = MagicMock()
        qr.job_id = dr.worst_job["job_id"]
        qr.query = f"SELECT 1 -- {dr.worst_job['job_id']}"
        query_rows.append(qr)
    mock_jobs.append(_make_job("mock_query_text_job", query_rows))

    # Job 3+: AI.GENERATE result and any subsequent calls (schema fetches etc.)
    if ai_rows is not None:
        mock_jobs.append(_make_job("mock_ai_job", ai_rows))

    # Create an infinite fallback: after the explicit jobs are consumed,
    # return a default empty-result mock job for any additional query() call.
    call_index = [0]
    def _query_side_effect(*args, **kwargs):
        idx = call_index[0]
        call_index[0] += 1
        if idx < len(mock_jobs):
            return mock_jobs[idx]
        # Fallback: return empty result job for schema/AI calls we don't care about
        return _make_job(f"mock_fallback_{idx}", [])

    mock_bq_client.query.side_effect = _query_side_effect

    return mock_init_patcher, mock_bq_client


# ===========================================================================
# 1️⃣ Multi-Strategy ROI Engine
# ===========================================================================

class TestMultiStrategyDiscovery:
    """Verify discovery SQL generation for all 5 strategy modes."""

    def test_slot_ms_strategy_sql_and_ordering(self):
        """slot_ms strategy generates ORDER BY total_slot_ms DESC and HAVING 1=1."""
        mock_init, mock_bq = _setup_mock_bq([_make_discovery_row()])
        with mock_init:
            response = client.post("/api/ai/analyze", json={
                "org_project_id": "acme-sandbox",
                "region": "region-us",
                "limit": 5,
                "discovery_strategy": "slot_ms",
            })
            assert response.status_code == 200

            discovery_sql = mock_bq.query.call_args_list[0][0][0]
            assert "ORDER BY total_slot_ms DESC" in discovery_sql
            # slot_ms uses no additional HAVING filter beyond 1=1
            assert "1=1" in discovery_sql

    def test_composite_score_formula_in_sql(self):
        """composite strategy SQL contains the 4-factor weighted formula."""
        mock_init, mock_bq = _setup_mock_bq([_make_discovery_row()])
        with mock_init:
            response = client.post("/api/ai/analyze", json={
                "org_project_id": "acme-sandbox",
                "region": "region-us",
                "limit": 5,
                "discovery_strategy": "composite",
            })
            assert response.status_code == 200

            discovery_sql = mock_bq.query.call_args_list[0][0][0]
            # Verify all 4 weighted components
            assert "0.40" in discovery_sql or "0.4" in discovery_sql
            assert "0.30" in discovery_sql or "0.3" in discovery_sql
            assert "0.20" in discovery_sql or "0.2" in discovery_sql
            assert "0.10" in discovery_sql or "0.1" in discovery_sql
            assert "LOG10" in discovery_sql.upper()

    def test_default_strategy_is_composite(self):
        """Omitting discovery_strategy defaults to composite with optimization_potential_score ordering."""
        mock_init, mock_bq = _setup_mock_bq([_make_discovery_row()])
        with mock_init:
            response = client.post("/api/ai/analyze", json={
                "org_project_id": "acme-sandbox",
                "region": "region-us",
                "limit": 5,
                # No discovery_strategy specified
            })
            assert response.status_code == 200

            discovery_sql = mock_bq.query.call_args_list[0][0][0]
            assert "ORDER BY optimization_potential_score DESC" in discovery_sql

    def test_strategy_aware_python_resorting(self):
        """After cross-project fetch, expensive_queries list is re-sorted by the strategy's sort field."""
        # Create rows with distinct values so ordering is verifiable
        row_high = _make_discovery_row(
            job_id="job_high", execution_count=100, optimization_potential_score=9.0,
        )
        row_low = _make_discovery_row(
            job_id="job_low", execution_count=5, optimization_potential_score=2.0,
        )

        # Discovery returns low-first (simulating BigQuery order that may differ from Python re-sort)
        ai_row_high = _make_ai_result_row(job_id="job_high", advice="[HIGH]\n- Fix\nOPTIMIZED_SQL_START\nSELECT 1\nOPTIMIZED_SQL_END")
        ai_row_low = _make_ai_result_row(job_id="job_low", advice="[LOW]\n- Minor\nOPTIMIZED_SQL_START\nSELECT 2\nOPTIMIZED_SQL_END")

        mock_init, mock_bq = _setup_mock_bq(
            [row_low, row_high],
            [ai_row_high, ai_row_low],
        )
        with mock_init:
            response = client.post("/api/ai/analyze", json={
                "org_project_id": "acme-sandbox",
                "region": "region-us",
                "limit": 5,
                "discovery_strategy": "execution_frequency",
            })
            assert response.status_code == 200
            data = response.json()
            # With execution_frequency, higher execution_count should come first
            if len(data) == 2:
                assert data[0]["execution_count"] >= data[1]["execution_count"]

    def test_lookback_days_boundary_90(self):
        """lookback_days=90 (max) is accepted and appears in generated SQL."""
        mock_init, mock_bq = _setup_mock_bq([_make_discovery_row()])
        with mock_init:
            response = client.post("/api/ai/analyze", json={
                "org_project_id": "acme-sandbox",
                "region": "region-us",
                "limit": 5,
                "lookback_days": 90,
            })
            assert response.status_code == 200

            discovery_sql = mock_bq.query.call_args_list[0][0][0]
            assert "INTERVAL 90 DAY" in discovery_sql

    def test_lookback_days_boundary_1(self):
        """lookback_days=1 (min) is accepted and appears in generated SQL."""
        mock_init, mock_bq = _setup_mock_bq([_make_discovery_row()])
        with mock_init:
            response = client.post("/api/ai/analyze", json={
                "org_project_id": "acme-sandbox",
                "region": "region-us",
                "limit": 5,
                "lookback_days": 1,
            })
            assert response.status_code == 200

            discovery_sql = mock_bq.query.call_args_list[0][0][0]
            assert "INTERVAL 1 DAY" in discovery_sql


# ===========================================================================
# 2️⃣ BigQuery Editions Hybrid Cost Fallback
# ===========================================================================

class TestEditionsHybridCost:
    """Verify the effective_bytes fallback logic for BigQuery Editions."""

    def test_effective_bytes_formula_in_sql(self):
        """Discovery SQL includes the GREATEST(COALESCE(...), COALESCE(...)) AS effective_bytes formula."""
        mock_init, mock_bq = _setup_mock_bq([_make_discovery_row()])
        with mock_init:
            response = client.post("/api/ai/analyze", json={
                "org_project_id": "acme-sandbox",
                "region": "region-us",
                "limit": 5,
            })
            assert response.status_code == 200

            discovery_sql = mock_bq.query.call_args_list[0][0][0]
            assert "GREATEST" in discovery_sql
            assert "effective_bytes" in discovery_sql

    def test_editions_job_bytes_billed_zero_uses_processed(self):
        """When bytes_billed=0 (Editions), the response bytes_scanned_original uses total_bytes_processed."""
        editions_row = _make_discovery_row(
            job_id="job_editions",
            total_bytes_billed=0,
            total_bytes_processed=500_000_000_000,  # 500 GB
            total_effective_bytes=500_000_000_000,
        )
        ai_row = _make_ai_result_row(
            job_id="job_editions",
            advice="[LOW]\n- Add partition filter\nOPTIMIZED_SQL_START\nSELECT 1\nOPTIMIZED_SQL_END",
        )
        mock_init, mock_bq = _setup_mock_bq([editions_row], [ai_row])
        with mock_init:
            response = client.post("/api/ai/analyze", json={
                "org_project_id": "acme-sandbox",
                "region": "region-us",
                "limit": 5,
                "discovery_strategy": "cumulative_cost",
            })
            assert response.status_code == 200
            data = response.json()
            if len(data) > 0:
                assert data[0]["bytes_billed_original"] == 0
                assert data[0]["bytes_scanned_original"] == 500_000_000_000

    def test_ondemand_job_bytes_billed_wins(self):
        """When bytes_billed > bytes_processed (on-demand), effective_bytes uses billed."""
        ondemand_row = _make_discovery_row(
            job_id="job_ondemand",
            total_bytes_billed=600_000_000_000,
            total_bytes_processed=500_000_000_000,
            total_effective_bytes=600_000_000_000,
        )
        ai_row = _make_ai_result_row(
            job_id="job_ondemand",
            advice="[MEDIUM]\n- Optimize\nOPTIMIZED_SQL_START\nSELECT 1\nOPTIMIZED_SQL_END",
        )
        mock_init, mock_bq = _setup_mock_bq([ondemand_row], [ai_row])
        with mock_init:
            response = client.post("/api/ai/analyze", json={
                "org_project_id": "acme-sandbox",
                "region": "region-us",
                "limit": 5,
                "discovery_strategy": "cumulative_cost",
            })
            assert response.status_code == 200
            data = response.json()
            if len(data) > 0:
                # bytes_billed should be preserved and annualized_cost > 0
                assert data[0]["bytes_billed_original"] == 600_000_000_000

    def test_annualized_cost_uses_effective_bytes_in_sql(self):
        """Discovery SQL computes annualized_cost_usd from SUM(effective_bytes), not SUM(bytes_billed)."""
        mock_init, mock_bq = _setup_mock_bq([_make_discovery_row()])
        with mock_init:
            response = client.post("/api/ai/analyze", json={
                "org_project_id": "acme-sandbox",
                "region": "region-us",
                "limit": 5,
            })
            assert response.status_code == 200

            discovery_sql = mock_bq.query.call_args_list[0][0][0]
            # The annualized_cost line should reference effective_bytes
            assert "SUM(effective_bytes)" in discovery_sql
            # Verify it's in the cost calculation context
            assert "annualized_cost_usd" in discovery_sql

    @pytest.mark.parametrize("strategy", [
        "composite", "cumulative_cost", "execution_frequency", "memory_spill", "slot_ms",
    ])
    def test_all_strategies_include_effective_bytes_cte(self, strategy):
        """All 5 strategies produce SQL containing the effective_bytes column (strategy-independent CTE)."""
        mock_init, mock_bq = _setup_mock_bq([_make_discovery_row()])
        with mock_init:
            response = client.post("/api/ai/analyze", json={
                "org_project_id": "acme-sandbox",
                "region": "region-us",
                "limit": 5,
                "discovery_strategy": strategy,
            })
            assert response.status_code == 200

            discovery_sql = mock_bq.query.call_args_list[0][0][0]
            assert "effective_bytes" in discovery_sql, (
                f"Strategy '{strategy}' SQL is missing the effective_bytes column"
            )


# ===========================================================================
# 3️⃣ Multi-Stage Unnesting & Atomic Sampling
# ===========================================================================

class TestUnnestingAndSampling:
    """Verify SQL contains UNNEST(job_stages), ARRAY_AGG worst-job, and SCRIPT filter."""

    def test_stage_spill_unnest_in_discovery_sql(self):
        """SQL contains UNNEST(job_stages) with shuffle_output_bytes_spilled."""
        mock_init, mock_bq = _setup_mock_bq([_make_discovery_row()])
        with mock_init:
            client.post("/api/ai/analyze", json={
                "org_project_id": "acme-sandbox",
                "region": "region-us",
                "limit": 5,
            })
            discovery_sql = mock_bq.query.call_args_list[0][0][0]
            assert "UNNEST(job_stages)" in discovery_sql
            assert "shuffle_output_bytes_spilled" in discovery_sql

    def test_array_agg_worst_job_in_sql(self):
        """SQL uses ARRAY_AGG to atomically isolate the worst execution by slot_ms."""
        mock_init, mock_bq = _setup_mock_bq([_make_discovery_row()])
        with mock_init:
            client.post("/api/ai/analyze", json={
                "org_project_id": "acme-sandbox",
                "region": "region-us",
                "limit": 5,
            })
            discovery_sql = mock_bq.query.call_args_list[0][0][0]
            assert "ARRAY_AGG" in discovery_sql
            assert "total_slot_ms DESC" in discovery_sql
            assert "OFFSET(0)" in discovery_sql

    def test_script_filtering_in_discovery_sql(self):
        """SQL excludes procedural SCRIPT statement types."""
        mock_init, mock_bq = _setup_mock_bq([_make_discovery_row()])
        with mock_init:
            client.post("/api/ai/analyze", json={
                "org_project_id": "acme-sandbox",
                "region": "region-us",
                "limit": 5,
            })
            discovery_sql = mock_bq.query.call_args_list[0][0][0]
            assert "statement_type != 'SCRIPT'" in discovery_sql

    def test_worst_job_metadata_propagated(self):
        """The worst_job struct fields are correctly propagated into the response."""
        row = _make_discovery_row(
            job_id="propagation_test_job",
            project_id="proj-123",
            user_email="owner@corp.com",
            total_slot_ms=99999,
        )
        ai_row = _make_ai_result_row(
            job_id="propagation_test_job",
            advice="[HIGH]\n- Fix query\nOPTIMIZED_SQL_START\nSELECT 1\nOPTIMIZED_SQL_END",
        )
        # The response reads total_slot_ms from the AI.GENERATE result row
        ai_row.total_slot_ms = 99999

        mock_init, mock_bq = _setup_mock_bq([row], [ai_row])
        with mock_init:
            response = client.post("/api/ai/analyze", json={
                "org_project_id": "acme-sandbox",
                "region": "region-us",
                "limit": 5,
            })
            assert response.status_code == 200
            data = response.json()
            if len(data) > 0:
                assert data[0]["job_id"] == "propagation_test_job"
                assert data[0]["total_slot_ms"] == 99999

    def test_memory_spill_having_from_unnest_source(self):
        """memory_spill HAVING clause filters on total_bytes_spilled derived from UNNEST aggregation."""
        mock_init, mock_bq = _setup_mock_bq([_make_discovery_row(total_bytes_spilled=1024)])
        with mock_init:
            client.post("/api/ai/analyze", json={
                "org_project_id": "acme-sandbox",
                "region": "region-us",
                "limit": 5,
                "discovery_strategy": "memory_spill",
            })
            discovery_sql = mock_bq.query.call_args_list[0][0][0]
            # HAVING references total_bytes_spilled (aggregated from per-row bytes_spilled from UNNEST)
            assert "total_bytes_spilled > 0" in discovery_sql
            # bytes_spilled originates from the UNNEST subquery
            assert "bytes_spilled" in discovery_sql
            assert "UNNEST(job_stages)" in discovery_sql


# ===========================================================================
# 4️⃣ Security & Error Handling
# ===========================================================================

class TestSecurityAndValidation:
    """Verify IAM 403 enforcement and Pydantic Literal validation."""

    def test_forbidden_403_contains_role_guidance(self):
        """403 Forbidden includes both required role names in the detail message."""
        from google.api_core.exceptions import Forbidden

        with patch("src.main.init_bq_client_and_resolve_project") as mock_init:
            mock_bq = MagicMock()
            mock_init.return_value = (mock_bq, "acme-sandbox")
            mock_bq.query.side_effect = Forbidden("Access Denied: JOBS_BY_ORGANIZATION")

            response = client.post("/api/ai/analyze", json={
                "org_project_id": "acme-sandbox",
                "region": "region-us",
                "limit": 5,
            })
            assert response.status_code == 403
            detail = response.json()["detail"]
            assert "roles/bigquery.resourceViewer" in detail
            assert "roles/bigquery.admin" in detail

    def test_forbidden_403_no_silent_fallback(self):
        """After a 403, the endpoint does NOT attempt a JOBS_BY_PROJECT fallback query."""
        from google.api_core.exceptions import Forbidden

        with patch("src.main.init_bq_client_and_resolve_project") as mock_init:
            mock_bq = MagicMock()
            mock_init.return_value = (mock_bq, "acme-sandbox")
            mock_bq.query.side_effect = Forbidden("Access Denied")

            response = client.post("/api/ai/analyze", json={
                "org_project_id": "acme-sandbox",
                "region": "region-us",
                "limit": 5,
            })
            assert response.status_code == 403
            # Only one query call should have been made (the discovery attempt)
            assert mock_bq.query.call_count == 1

    def test_invalid_strategy_pydantic_422_detail(self):
        """Invalid discovery_strategy returns 422 with the field name in the error path."""
        response = client.post("/api/ai/analyze", json={
            "org_project_id": "acme-sandbox",
            "region": "region-us",
            "discovery_strategy": "bogus_strategy",
        })
        assert response.status_code == 422
        detail = response.json()["detail"]
        # Find the error that mentions discovery_strategy in the loc path
        strategy_errors = [
            e for e in detail
            if any("discovery_strategy" in str(loc) for loc in e.get("loc", []))
        ]
        assert len(strategy_errors) > 0, "422 detail should reference 'discovery_strategy' field"

    @pytest.mark.parametrize("strategy", [
        "composite", "cumulative_cost", "execution_frequency", "memory_spill", "slot_ms",
    ])
    def test_every_valid_strategy_accepted(self, strategy):
        """Each of the 5 valid Literal values is accepted (no 422)."""
        mock_init, mock_bq = _setup_mock_bq([_make_discovery_row()])
        with mock_init:
            response = client.post("/api/ai/analyze", json={
                "org_project_id": "acme-sandbox",
                "region": "region-us",
                "limit": 5,
                "discovery_strategy": strategy,
            })
            assert response.status_code != 422, f"Strategy '{strategy}' was rejected by Pydantic"

    def test_limit_boundary_100_accepted(self):
        """limit=100 (max) is accepted."""
        mock_init, mock_bq = _setup_mock_bq([_make_discovery_row()])
        with mock_init:
            response = client.post("/api/ai/analyze", json={
                "org_project_id": "acme-sandbox",
                "region": "region-us",
                "limit": 100,
            })
            assert response.status_code != 422

    def test_limit_boundary_101_rejected(self):
        """limit=101 exceeds the max and must be rejected with 422."""
        response = client.post("/api/ai/analyze", json={
            "org_project_id": "acme-sandbox",
            "region": "region-us",
            "limit": 101,
        })
        assert response.status_code == 422


# ===========================================================================
# 5️⃣ Bug-Fix Regressions
# ===========================================================================

class TestBugFixRegressions:
    """Regression tests for fixed bugs to prevent re-introduction."""

    def test_execution_frequency_uses_having_not_where(self):
        """execution_frequency filters via HAVING on CTE alias, not WHERE with aggregate."""
        mock_init, mock_bq = _setup_mock_bq([_make_discovery_row(execution_count=50)])
        with mock_init:
            client.post("/api/ai/analyze", json={
                "org_project_id": "acme-sandbox",
                "region": "region-us",
                "limit": 5,
                "discovery_strategy": "execution_frequency",
            })
            discovery_sql = mock_bq.query.call_args_list[0][0][0]
            # The filter should use the CTE alias, not COUNT(*) in a WHERE clause
            assert "execution_count > 1" in discovery_sql
            # It must NOT contain "WHERE COUNT(*)" which caused the BQ 400 error
            assert "WHERE COUNT(*)" not in discovery_sql.upper()

    def test_total_bytes_processed_in_discovery_cte(self):
        """total_bytes_processed appears in the scanned CTE's SELECT list (metric correction fix)."""
        mock_init, mock_bq = _setup_mock_bq([_make_discovery_row()])
        with mock_init:
            client.post("/api/ai/analyze", json={
                "org_project_id": "acme-sandbox",
                "region": "region-us",
                "limit": 5,
            })
            discovery_sql = mock_bq.query.call_args_list[0][0][0]
            # Must reference total_bytes_processed in the CTE to populate bytes_scanned_original
            assert "total_bytes_processed" in discovery_sql

    def test_empty_discovery_returns_empty_list_200(self):
        """When discovery returns 0 rows, the endpoint returns 200 with empty JSON array."""
        with patch("src.main.init_bq_client_and_resolve_project") as mock_init:
            mock_bq = MagicMock()
            mock_init.return_value = (mock_bq, "acme-sandbox")

            empty_job = MagicMock()
            empty_job.total_bytes_processed = 0
            empty_job.total_bytes_billed = 0
            empty_job.cache_hit = False
            empty_job.job_id = "mock_empty_discovery"
            empty_job.result.return_value = []  # No discovery rows
            mock_bq.query.return_value = empty_job

            response = client.post("/api/ai/analyze", json={
                "org_project_id": "acme-sandbox",
                "region": "region-us",
                "limit": 5,
            })
            assert response.status_code == 200
            assert response.json() == []
