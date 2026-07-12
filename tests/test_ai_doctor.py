import pytest
from unittest.mock import MagicMock, patch
from fastapi.testclient import TestClient
from src.main import app

client = TestClient(app)

def test_ai_doctor_schema_aware_analysis():
    # Mock row representing the result of the AI.GENERATE_TEXT query.
    # It contains the query details along with our new DDL count metrics and LLM advice.
    mock_ai_row = MagicMock(
        job_id="expensive_select_job_123",
        project_id="acme-sandbox",
        user_email="data_engineer@acme.com",
        total_slot_ms=125000,
        query="SELECT * FROM `acme-sandbox.dataset.table` LIMIT 10",
        ai_struct={"result": "- Avoid SELECT * and specify columns to prune bytes.\n- Add partition filter for date."},
        tables_referenced_count=3,
        tables_found_count=2
    )

    # Mocking the BigQuery client and resolution helper
    with patch("src.main.init_bq_client_and_resolve_project") as mock_init:
        mock_bq_client = MagicMock()
        mock_init.return_value = (mock_bq_client, "acme-sandbox")
        
        # Configure metadata fields on the mock job to satisfy run_query_and_log
        mock_job = MagicMock()
        mock_job.total_bytes_processed = 0
        mock_job.total_bytes_billed = 0
        mock_job.cache_hit = False
        mock_job.job_id = "mock_ai_doctor_job_001"
        mock_job.result.return_value = [mock_ai_row]
        
        # Assign mock query return value
        mock_bq_client.query.return_value = mock_job
        
        # Configure get_table mock to simulate table schema retrieval
        mock_table = MagicMock()
        mock_table.num_rows = 1000000
        mock_table.num_bytes = 104857600  # 100 MB
        mock_table.time_partitioning = MagicMock()
        mock_table.time_partitioning.field = "created_date"
        mock_table.clustering_fields = ["user_id"]
        mock_field = MagicMock()
        mock_field.name = "created_date"
        mock_field.field_type = "DATE"
        mock_table.schema = [mock_field]
        mock_bq_client.get_table.return_value = mock_table
        
        # Call the AI Code Doctor endpoint
        payload = {
            "org_project_id": "acme-sandbox",
            "region": "region-us",
            "limit": 10,
            "lookback_days": 14
        }
        
        response = client.post("/api/ai/analyze", json=payload)
        
        # 1. Verify API succeeded
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


        
        # 2. Verify SQL Query construction logic
        assert mock_bq_client.query.call_count >= 3
        
        # Verify the first call (discovery query) uses org-level view
        discovery_sql = mock_bq_client.query.call_args_list[0][0][0]
        assert "JOBS_BY_ORGANIZATION" in discovery_sql, "Discovery query must use JOBS_BY_ORGANIZATION, not JOBS_BY_PROJECT"
        assert "JOBS_BY_PROJECT" not in discovery_sql

        # Verify the second call (the UNION ALL AI analysis query)
        called_args, called_kwargs = mock_bq_client.query.call_args
        called_sql = called_args[0]
        called_job_config = called_kwargs.get("job_config")
        
        assert "AI.GENERATE" in called_sql
        assert "@prompt_c0_a0" in called_sql
        assert "endpoint => 'https://aiplatform.googleapis.com/v1/projects/acme-sandbox/locations/global/publishers/google/models/gemini-3.1-flash-lite'" in called_sql
        assert "connection_id" not in called_sql
        
        # Verify query parameters are passed securely
        assert called_job_config is not None
        params = called_job_config.query_parameters
        assert len(params) == 7
        
        param_names = {p.name for p in params}
        expected_names = {
            "job_id_c0_a0", "email_c0_a0", "slot_ms_c0_a0", 
            "query_c0_a0", "ref_count_c0_a0", "found_count_c0_a0", "prompt_c0_a0"
        }
        assert param_names == expected_names
        
        # Assert specific parameter values
        email_param = next(p for p in params if p.name == "email_c0_a0")
        assert email_param.value == "data_engineer@acme.com"
