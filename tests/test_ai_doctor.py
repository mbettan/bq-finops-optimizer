import pytest
from unittest.mock import MagicMock, patch
from fastapi.testclient import TestClient
from src.main import app

client = TestClient(app)


def test_ai_doctor_schema_aware_analysis():
    """Test the full AI Doctor flow with structured output parsing: severity, optimized SQL, bytes metadata."""
    # Mock row representing the AI.GENERATE result with v2 structured output
    mock_ai_row = MagicMock(
        job_id="expensive_select_job_123",
        user_email="data_engineer@acme.com",
        query="SELECT * FROM `acme-sandbox.dataset.table` LIMIT 10",
        total_slot_ms=125000,
        total_bytes_billed=5368709120,
        total_bytes_processed=5368709120,
        worst_job={
            "job_id": "expensive_select_job_123",
            "project_id": "acme-sandbox",
            "user_email": "data_engineer@acme.com",
            "query": "SELECT * FROM `acme-sandbox.dataset.table` LIMIT 10",
            "total_bytes_billed": 5368709120,
            "total_slot_ms": 125000,
            "creation_time": "2026-07-27T12:00:00Z"
        },
        execution_count=10,
        annualized_cost_usd=150.0,
        optimization_potential_score=8.5,
        ai_struct={"result": (
            "[HIGH]\n"
            "- Avoid SELECT * and specify columns to prune bytes.\n"
            "- Add partition filter for order_date.\n"
            "OPTIMIZED_SQL_START\n"
            "SELECT order_id, customer_id FROM `acme-sandbox.dataset.table` WHERE order_date >= '2025-01-01'\n"
            "OPTIMIZED_SQL_END"
        )},
        tables_referenced_count=3,
        tables_found_count=2,
        table_schema="dataset",
        table_name="table",
        total_rows=1000000,
        size_bytes=104857600,
        partition_column="created_date",
        require_partition_filter="false",
        clustering_fields="user_id",
        num_columns=10,
        column_schema="order_id (INT64), customer_id (STRING), order_date (DATE)",
        ddl=""
    )

    with patch("src.main.init_bq_client_and_resolve_project") as mock_init:
        mock_bq_client = MagicMock()
        mock_init.return_value = (mock_bq_client, "acme-sandbox")
        
        mock_job = MagicMock()
        mock_job.total_bytes_processed = 0
        mock_job.total_bytes_billed = 0
        mock_job.cache_hit = False
        mock_job.job_id = "mock_ai_doctor_job_001"
        mock_job.result.return_value = [mock_ai_row]
        mock_bq_client.query.return_value = mock_job
        
        mock_table = MagicMock()
        mock_table.num_rows = 1000000
        mock_table.num_bytes = 104857600
        mock_table.time_partitioning = MagicMock()
        mock_table.time_partitioning.field = "created_date"
        mock_table.clustering_fields = ["user_id"]
        mock_field = MagicMock()
        mock_field.name = "created_date"
        mock_field.field_type = "DATE"
        mock_table.schema = [mock_field]
        mock_bq_client.get_table.return_value = mock_table
        
        payload = {
            "org_project_id": "acme-sandbox",
            "region": "region-us",
            "limit": 10,
            "lookback_days": 14
        }
        
        response = client.post("/api/ai/analyze", json=payload)
        
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1
        
        result = data[0]
        assert result["job_id"] == "expensive_select_job_123"
        assert result["user_email"] == "data_engineer@acme.com"
        assert result["total_slot_ms"] == 125000
        assert result["query"] == "SELECT * FROM `acme-sandbox.dataset.table` LIMIT 10"
        assert result["tables_referenced_count"] == 3
        assert result["tables_found_count"] == 2
        assert "Avoid SELECT *" in result["gemini_optimization_advice"]

        # v2: Verify new fields
        assert result["severity"] == "HIGH"
        assert result["optimized_query"] is not None
        assert result["optimized_query"].startswith("SELECT order_id")
        assert result["bytes_scanned_original"] == 5368709120
        assert result["bytes_billed_original"] == 5368709120
        assert result["on_demand_rate_usd_per_tb"] == 6.25
        # Verify markers are stripped from advice text
        assert "OPTIMIZED_SQL_START" not in result["gemini_optimization_advice"]
        assert "OPTIMIZED_SQL_END" not in result["gemini_optimization_advice"]
        assert "[HIGH]" not in result["gemini_optimization_advice"]

        # Verify SQL construction logic
        assert mock_bq_client.query.call_count >= 2
        
        discovery_sql = mock_bq_client.query.call_args_list[0][0][0]
        assert "JOBS_BY_ORGANIZATION" in discovery_sql
        assert "JOBS_BY_PROJECT" not in discovery_sql
        # v2: Verify bytes columns in discovery query
        assert "total_bytes_billed" in discovery_sql
        assert "bytes_billed" in discovery_sql

        called_args, called_kwargs = mock_bq_client.query.call_args
        called_sql = called_args[0]
        called_job_config = called_kwargs.get("job_config")
        
        assert "AI.GENERATE" in called_sql
        assert "@prompt_c0_a0" in called_sql
        assert "endpoint => 'https://aiplatform.googleapis.com/v1/projects/acme-sandbox/locations/global/publishers/google/models/gemini-3.6-flash'" in called_sql
        assert "connection_id" not in called_sql
        # v2: Verify increased token limit
        assert "max_output_tokens" in called_sql
        assert "8192" in called_sql
        
        assert called_job_config is not None
        params = called_job_config.query_parameters
        assert len(params) == 7
        
        param_names = {p.name for p in params}
        expected_names = {
            "job_id_c0_a0", "email_c0_a0", "slot_ms_c0_a0", 
            "query_c0_a0", "ref_count_c0_a0", "found_count_c0_a0", "prompt_c0_a0"
        }
        assert param_names == expected_names
        
        email_param = next(p for p in params if p.name == "email_c0_a0")
        assert email_param.value == "data_engineer@acme.com"

        # v2: Verify prompt content includes partition alignment + XML tags + column list
        prompt_param = next(p for p in params if p.name == "prompt_c0_a0")
        assert "PARTITION ALIGNMENT INSTRUCTIONS" in prompt_param.value
        assert "<schema_context>" in prompt_param.value
        assert "</schema_context>" in prompt_param.value
        assert "<user_query>" in prompt_param.value
        assert "Columns (" in prompt_param.value
        assert "[HIGH]" in prompt_param.value  # Severity instructions in prompt


def test_ai_doctor_custom_model_selection():
    mock_ai_row = MagicMock(
        job_id="job_456",
        user_email="data@acme.com",
        query="SELECT 1",
        worst_job={
            "job_id": "job_456",
            "project_id": "acme-sandbox",
            "user_email": "data@acme.com",
            "query": "SELECT 1",
            "total_bytes_billed": 0,
            "total_slot_ms": 50000,
            "creation_time": "2026-07-27T12:00:00Z"
        },
        execution_count=1,
        annualized_cost_usd=0.0,
        optimization_potential_score=1.0,
        total_slot_ms=50000,
        total_bytes_billed=0,
        total_bytes_processed=0,
        ai_struct={"result": "Looks good"},
        tables_referenced_count=0,
        tables_found_count=0,
        table_schema=None,
        table_name=None,
        total_rows=0,
        size_bytes=0,
        partition_column=None,
        require_partition_filter="false",
        clustering_fields=None,
        num_columns=0,
        column_schema="",
        ddl=""
    )

    with patch("src.main.init_bq_client_and_resolve_project") as mock_init:
        mock_bq_client = MagicMock()
        mock_init.return_value = (mock_bq_client, "acme-sandbox")
        
        mock_job = MagicMock()
        mock_job.total_bytes_processed = 0
        mock_job.total_bytes_billed = 0
        mock_job.cache_hit = False
        mock_job.job_id = "mock_job_002"
        mock_job.result.return_value = [mock_ai_row]
        mock_bq_client.query.return_value = mock_job
        
        payload = {
            "org_project_id": "acme-sandbox",
            "region": "region-us",
            "limit": 5,
            "lookback_days": 7,
            "model": "gemini-3.6-flash"
        }
        
        response = client.post("/api/ai/analyze", json=payload)
        assert response.status_code == 200
        
        called_args, _ = mock_bq_client.query.call_args
        called_sql = called_args[0]
        assert "gemini-3.6-flash" in called_sql


def test_ai_doctor_no_severity_no_markers():
    """Advice with no severity tag and no markers → severity is None, optimized_query is None, advice untouched."""
    mock_ai_row = MagicMock(
        job_id="job_plain",
        user_email="u@test.com",
        query="SELECT 1",
        worst_job={
            "job_id": "job_plain",
            "project_id": "acme-sandbox",
            "user_email": "u@test.com",
            "query": "SELECT 1",
            "total_bytes_billed": 0,
            "total_slot_ms": 10000,
            "creation_time": "2026-07-27T12:00:00Z"
        },
        execution_count=1,
        annualized_cost_usd=0.0,
        optimization_potential_score=1.0,
        total_slot_ms=10000,
        total_bytes_billed=0,
        total_bytes_processed=0,
        ai_struct={"result": "This query looks fine but could use minor improvements."},
        tables_referenced_count=0,
        tables_found_count=0,
        table_schema=None,
        table_name=None,
        total_rows=0,
        size_bytes=0,
        partition_column=None,
        require_partition_filter="false",
        clustering_fields=None,
        num_columns=0,
        column_schema="",
        ddl=""
    )

    with patch("src.main.init_bq_client_and_resolve_project") as mock_init:
        mock_bq_client = MagicMock()
        mock_init.return_value = (mock_bq_client, "acme-sandbox")
        
        mock_job = MagicMock()
        mock_job.total_bytes_processed = 0
        mock_job.total_bytes_billed = 0
        mock_job.cache_hit = False
        mock_job.job_id = "mock_job_neg"
        mock_job.result.return_value = [mock_ai_row]
        mock_bq_client.query.return_value = mock_job
        
        response = client.post("/api/ai/analyze", json={
            "org_project_id": "acme-sandbox", "region": "region-us", "limit": 5
        })
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1
        
        result = data[0]
        assert result["severity"] is None
        assert result["optimized_query"] is None
        assert result["gemini_optimization_advice"] == "This query looks fine but could use minor improvements."


def test_ai_doctor_rejects_destructive_optimized_sql():
    """Output safety guard: if Gemini generates DROP/DELETE, optimized_query should be None."""
    mock_ai_row = MagicMock(
        job_id="job_bad",
        user_email="u@test.com",
        query="SELECT 1",
        worst_job={
            "job_id": "job_bad",
            "project_id": "acme-sandbox",
            "user_email": "u@test.com",
            "query": "SELECT 1",
            "total_bytes_billed": 0,
            "total_slot_ms": 10000,
            "creation_time": "2026-07-27T12:00:00Z"
        },
        execution_count=1,
        annualized_cost_usd=0.0,
        optimization_potential_score=1.0,
        total_slot_ms=10000,
        total_bytes_billed=0,
        total_bytes_processed=0,
        ai_struct={"result": (
            "[HIGH]\n- Terrible query\n"
            "OPTIMIZED_SQL_START\n"
            "DROP TABLE `acme-sandbox.dataset.table`\n"
            "OPTIMIZED_SQL_END"
        )},
        tables_referenced_count=0,
        tables_found_count=0,
        table_schema=None,
        table_name=None,
        total_rows=0,
        size_bytes=0,
        partition_column=None,
        require_partition_filter="false",
        clustering_fields=None,
        num_columns=0,
        column_schema="",
        ddl=""
    )

    with patch("src.main.init_bq_client_and_resolve_project") as mock_init:
        mock_bq_client = MagicMock()
        mock_init.return_value = (mock_bq_client, "acme-sandbox")
        
        mock_job = MagicMock()
        mock_job.total_bytes_processed = 0
        mock_job.total_bytes_billed = 0
        mock_job.cache_hit = False
        mock_job.job_id = "mock_job_bad"
        mock_job.result.return_value = [mock_ai_row]
        mock_bq_client.query.return_value = mock_job
        
        response = client.post("/api/ai/analyze", json={
            "org_project_id": "acme-sandbox", "region": "region-us", "limit": 5
        })
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1
        
        result = data[0]
        assert result["optimized_query"] is None  # Safety guard discarded it
        assert result["severity"] == "HIGH"


@pytest.mark.parametrize("dml_prefix", ["UPDATE", "INSERT INTO", "DELETE FROM", "MERGE INTO"])
def test_ai_doctor_accepts_dml_optimized_sql(dml_prefix):
    """Output safety guard: verify DML optimizations (UPDATE, INSERT, DELETE, MERGE) are accepted."""
    dml_sql = f"{dml_prefix} `acme-sandbox.dataset.table` SET x = 1 WHERE y = 2" if "UPDATE" in dml_prefix else f"{dml_prefix} `acme-sandbox.dataset.table`"
    mock_ai_row = MagicMock(
        job_id="job_dml",
        user_email="u@test.com",
        query="SELECT 1",
        worst_job={
            "job_id": "job_dml",
            "project_id": "acme-sandbox",
            "user_email": "u@test.com",
            "query": "SELECT 1",
            "total_bytes_billed": 0,
            "total_slot_ms": 10000,
            "creation_time": "2026-07-27T12:00:00Z"
        },
        execution_count=1,
        annualized_cost_usd=0.0,
        optimization_potential_score=1.0,
        total_slot_ms=10000,
        total_bytes_billed=0,
        total_bytes_processed=0,
        ai_struct={"result": (
            f"[MEDIUM]\n- Optimize DML predicate\n"
            f"OPTIMIZED_SQL_START\n"
            f"{dml_sql}\n"
            f"OPTIMIZED_SQL_END"
        )},
        tables_referenced_count=0,
        tables_found_count=0,
        table_schema=None,
        table_name=None,
        total_rows=0,
        size_bytes=0,
        partition_column=None,
        require_partition_filter="false",
        clustering_fields=None,
        num_columns=0,
        column_schema="",
        ddl=""
    )

    with patch("src.main.init_bq_client_and_resolve_project") as mock_init, \
         patch("src.main.run_migration_translation", return_value=None):
        mock_bq_client = MagicMock()
        mock_init.return_value = (mock_bq_client, "acme-sandbox")

        mock_job = MagicMock()
        mock_job.total_bytes_processed = 0
        mock_job.total_bytes_billed = 0
        mock_job.cache_hit = False
        mock_job.job_id = "mock_job_dml"
        mock_job.result.return_value = [mock_ai_row]
        mock_bq_client.query.return_value = mock_job

        response = client.post("/api/ai/analyze", json={
            "org_project_id": "acme-sandbox", "region": "region-us", "limit": 5
        })
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1

        result = data[0]
        assert result["optimized_query"] is not None
        assert result["optimized_query"] == dml_sql




def test_ai_doctor_discovery_strategies():
    """Verify discovery_strategy choices generate expected SQL clauses and orderings."""
    from google.api_core.exceptions import Forbidden
    
    mock_ai_row = MagicMock(
        job_id="job_freq",
        user_email="user@acme.com",
        query="SELECT 1",
        worst_job={
            "job_id": "job_freq",
            "project_id": "acme-sandbox",
            "user_email": "user@acme.com",
            "query": "SELECT 1",
            "total_bytes_billed": 1000,
            "total_slot_ms": 5000,
            "creation_time": "2026-07-27T12:00:00Z"
        },
        execution_count=50,
        annualized_cost_usd=25.0,
        optimization_potential_score=5.5,
        total_slot_ms=5000,
        total_bytes_billed=1000,
        total_bytes_processed=1000,
        ai_struct={"result": "Looks fine"},
        tables_referenced_count=0,
        tables_found_count=0
    )

    strategies_to_test = [
        ("execution_frequency", "execution_count > 1", "ORDER BY execution_count DESC"),
        ("memory_spill", "total_bytes_spilled > 0", "ORDER BY total_bytes_spilled DESC"),
        ("cumulative_cost", "1=1", "ORDER BY total_effective_bytes DESC"),
        ("composite", "1=1", "ORDER BY optimization_potential_score DESC"),
    ]

    for strategy_name, expected_having, expected_order in strategies_to_test:
        with patch("src.main.init_bq_client_and_resolve_project") as mock_init:
            mock_bq_client = MagicMock()
            mock_init.return_value = (mock_bq_client, "acme-sandbox")
            
            mock_job = MagicMock()
            mock_job.total_bytes_processed = 0
            mock_job.total_bytes_billed = 0
            mock_job.cache_hit = False
            mock_job.job_id = "mock_job_strat"
            mock_job.result.return_value = [mock_ai_row]
            mock_bq_client.query.return_value = mock_job

            response = client.post("/api/ai/analyze", json={
                "org_project_id": "acme-sandbox",
                "region": "region-us",
                "limit": 5,
                "discovery_strategy": strategy_name
            })
            assert response.status_code == 200
            
            discovery_sql = mock_bq_client.query.call_args_list[0][0][0]
            assert expected_having in discovery_sql
            assert expected_order in discovery_sql


def test_ai_doctor_forbidden_403_raises_http_exception():
    """Verify IAM 403 Forbidden raises clean HTTP 403 with role guidance."""
    from google.api_core.exceptions import Forbidden

    with patch("src.main.init_bq_client_and_resolve_project") as mock_init:
        mock_bq_client = MagicMock()
        mock_init.return_value = (mock_bq_client, "acme-sandbox")
        mock_bq_client.query.side_effect = Forbidden("Access Denied: JOBS_BY_ORGANIZATION")

        response = client.post("/api/ai/analyze", json={
            "org_project_id": "acme-sandbox",
            "region": "region-us",
            "limit": 5
        })
        assert response.status_code == 403
        data = response.json()
        assert "roles/bigquery.resourceViewer" in data["detail"]


def test_ai_doctor_invalid_strategy_raises_422():
    """Verify invalid discovery_strategy string is rejected by Pydantic validation."""
    response = client.post("/api/ai/analyze", json={
        "org_project_id": "acme-sandbox",
        "discovery_strategy": "invalid_strategy_name"
    })
    assert response.status_code == 422


def test_ai_doctor_hybrid_editions_fallback():
    """Verify cumulative_cost strategy includes effective_bytes fallback for Editions slot jobs (bytes_billed = 0)."""
    # Editions job: 0 bytes billed, 500 GB processed
    mock_editions_row = MagicMock(
        job_id="job_editions",
        user_email="editions@acme.com",
        query="SELECT 1",
        worst_job={
            "job_id": "job_editions",
            "project_id": "acme-sandbox",
            "user_email": "editions@acme.com",
            "query": "SELECT 1",
            "total_bytes_billed": 0,
            "total_slot_ms": 50000,
            "creation_time": "2026-07-27T12:00:00Z"
        },
        execution_count=10,
        annualized_cost_usd=162.5,
        optimization_potential_score=7.2,
        total_slot_ms=50000,
        total_bytes_billed=0,
        total_bytes_processed=500000000000,
        total_effective_bytes=500000000000,
        ai_struct={"result": "[LOW]\n- Add partition filter\nOPTIMIZED_SQL_START\nSELECT col FROM `dataset.large_table` WHERE _PARTITIONDATE = CURRENT_DATE()\nOPTIMIZED_SQL_END"},
        tables_referenced_count=0,
        tables_found_count=0
    )

    with patch("src.main.init_bq_client_and_resolve_project") as mock_init:
        mock_bq_client = MagicMock()
        mock_init.return_value = (mock_bq_client, "acme-sandbox")
        
        mock_job = MagicMock()
        mock_job.total_bytes_processed = 500000000000
        mock_job.total_bytes_billed = 0
        mock_job.cache_hit = False
        mock_job.job_id = "job_editions"
        mock_job.result.return_value = [mock_editions_row]
        mock_bq_client.query.return_value = mock_job

        response = client.post("/api/ai/analyze", json={
            "org_project_id": "acme-sandbox",
            "region": "region-us",
            "limit": 5,
            "discovery_strategy": "cumulative_cost"
        })
        assert response.status_code == 200
        
        # Verify SQL contains effective_bytes calculation and ORDER BY total_effective_bytes
        discovery_sql = mock_bq_client.query.call_args_list[0][0][0]
        assert "effective_bytes" in discovery_sql
        assert "ORDER BY total_effective_bytes DESC" in discovery_sql
        
        data = response.json()
        assert len(data) == 1
        assert data[0]["bytes_scanned_original"] == 500000000000
        assert data[0]["bytes_billed_original"] == 0
        assert data[0]["annualized_cost_usd"] == 162.5


def test_ai_doctor_end_to_end_with_migration_api_rewrite():
    """Verify that when Gemini produces architectural advice and Migration API rewrites the query,
    AI Doctor outputs the optimized SQL, applied YAML, and human-readable compiler badges."""
    query = (
        "WITH expensive_cte AS (\n"
        "  SELECT id, name, status, REGEXP_CONTAINS(name, r'^PROD_') AS is_prod\n"
        "  FROM `acme-sandbox.dataset.orders`\n"
        ")\n"
        "SELECT * FROM expensive_cte WHERE is_prod = TRUE\n"
        "UNION ALL\n"
        "SELECT * FROM expensive_cte WHERE status = 'COMPLETED'"
    )

    optimized_sql = (
        "CREATE TEMP TABLE expensive_cte AS\n"
        "  SELECT id, name, status, name LIKE 'PROD_%' AS is_prod\n"
        "  FROM `acme-sandbox.dataset.orders`;\n\n"
        "SELECT * FROM expensive_cte WHERE is_prod = TRUE\n"
        "UNION ALL\n"
        "SELECT * FROM expensive_cte WHERE status = 'COMPLETED';"
    )

    mock_ai_row = MagicMock(
        job_id="job_repeated_cte_123",
        user_email="data_eng@acme.com",
        query=query,
        total_slot_ms=300000,
        total_bytes_billed=10737418240,
        total_bytes_processed=10737418240,
        worst_job={
            "job_id": "job_repeated_cte_123",
            "project_id": "acme-sandbox",
            "user_email": "data_eng@acme.com",
            "query": query,
            "total_bytes_billed": 10737418240,
            "total_slot_ms": 300000,
            "creation_time": "2026-08-07T12:00:00Z"
        },
        execution_count=5,
        annualized_cost_usd=250.0,
        optimization_potential_score=9.2,
        # Gemini provides architectural advice without OPTIMIZED_SQL_START markers
        ai_struct={"result": (
            "[HIGH]\n"
            "- Repeated evaluation of expensive CTE across UNION ALL blocks.\n"
            "- REGEXP_CONTAINS causes regex engine overhead on simple prefix matching."
        )},
        tables_referenced_count=1,
        tables_found_count=1,
        table_schema="dataset",
        table_name="orders",
        total_rows=5000000,
        size_bytes=10737418240,
        partition_column=None,
        require_partition_filter="false",
        clustering_fields=None,
        num_columns=4,
        column_schema="id (INT64), name (STRING), status (STRING)",
        ddl=""
    )

    with patch("src.main.init_bq_client_and_resolve_project") as mock_init, \
         patch("src.main.run_migration_translation") as mock_migration:

        mock_bq_client = MagicMock()
        mock_init.return_value = (mock_bq_client, "acme-sandbox")

        mock_job = MagicMock()
        mock_job.total_bytes_processed = 0
        mock_job.total_bytes_billed = 0
        mock_job.cache_hit = False
        mock_job.job_id = "mock_discovery_job"
        mock_job.result.return_value = [mock_ai_row]
        mock_bq_client.query.return_value = mock_job

        from src.migration_optimizer import TranslationResponse, MigrationIssue
        mock_migration.return_value = TranslationResponse(
            translated_sql=optimized_sql,
            original_sql=query,
            success=True,
            applied_config_yaml="type: optimizer\ntransformations:\n  - name: REWRITE_CTE_TO_TEMP_TABLE\n  - name: REGEXP_CONTAINS_TO_LIKE\n",
            issues=[
                MigrationIssue(category="OPTIMIZATION", message="Common Table Expression has been rewritten: REWRITE_CTE_TO_TEMP_TABLE"),
                MigrationIssue(category="OPTIMIZATION", message="REGEXP_CONTAINS has been rewritten: REGEXP_CONTAINS_TO_LIKE"),
            ]
        )

        response = client.post("/api/ai/analyze", json={
            "org_project_id": "acme-sandbox",
            "region": "region-us",
            "limit": 10,
            "lookback_days": 7
        })

        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1
        res = data[0]

        assert res["optimized_query"] == optimized_sql
        assert res["optimized_query"] != res["query"]
        assert "REWRITE_CTE_TO_TEMP_TABLE" in res["migration_applied_yaml"]
        assert "REGEXP_CONTAINS_TO_LIKE" in res["migration_applied_yaml"]

        # Advice should contain human-readable compiler bullet points prepended
        assert "Automated Compiler Rewrite" in res["gemini_optimization_advice"]
        assert "Converted heavy Common Table Expressions (CTEs)" in res["gemini_optimization_advice"]
        assert "Replaced `REGEXP_CONTAINS()` with fast `LIKE`" in res["gemini_optimization_advice"]



