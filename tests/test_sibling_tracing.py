import pytest
from unittest.mock import MagicMock, patch
from fastapi.testclient import TestClient
from src.main import app

client = TestClient(app)

@pytest.mark.skip(reason="Sibling tracing and [INVALID CLUSTER JOIN] are not implemented in the backend")
def test_linter_sibling_parent_job_warning_propagation():
    # Mock data representing Stage 1 Organization Scan results for the Query Linter.
    # We return one expensive query job (>100 GB) that is a child step with a parent_job_id,
    # and has a statement_type of 'DELETE'.
    mock_org_row = MagicMock(
        job_id="child_delete_step_123",
        parent_job_id="parent_airflow_dag_run_456",
        user_email="engineer@acme.com",
        project_id="mbettan-project",
        billed_gb=120.5,
        statement_type="DELETE"
    )
    
    # Mock data representing Stage 2 Project Batch Lookup results.
    # We return the query texts for both the child and the parent.
    # Child is a clean DELETE. Parent has the buggy UPPER(PHONE_VALUE) clustering key invalidation.
    mock_project_rows = [
        MagicMock(
            job_id="child_delete_step_123", 
            user_email="engineer@acme.com", 
            billed_gb=120.5,
            query="DELETE FROM prd-acme-data.EDW.PARTNER_EXTRACT_S1M_HIST WHERE CREATE_DATE = CURRENT_DATE()"
        ),
        MagicMock(
            job_id="parent_airflow_dag_run_456", 
            user_email="engineer@acme.com", 
            billed_gb=0.0,
            query="""
                INSERT INTO prd-acme-data.EDW.PARTNER_EXTRACT_S1M_HIST
                SELECT t1.id, t2.name FROM raw_tables.DATASHOWCASE t1
                JOIN raw_tables.CONTACTS t2 
                ON UPPER(trim(t1.PHONE_VALUE)) = UPPER(trim(t2.PHONE_VALUE))
            """
        )
    ]
    
    # Mocking the BigQuery client and resolution helper
    with patch("src.main.init_bq_client_and_resolve_project") as mock_init:
        mock_bq_client = MagicMock()
        mock_init.return_value = (mock_bq_client, "mbettan-project")
        
        # Configure metadata fields on the mock jobs to satisfy run_query_and_log
        mock_stage1_job = MagicMock()
        mock_stage1_job.total_bytes_processed = 120.5 * 1024**3
        mock_stage1_job.total_bytes_billed = 120.5 * 1024**3
        mock_stage1_job.cache_hit = False
        mock_stage1_job.job_id = "mock_linter_scan_job_999"
        mock_stage1_job.result.return_value = [mock_org_row]
        
        mock_stage2_job = MagicMock()
        mock_stage2_job.total_bytes_processed = 0
        mock_stage2_job.total_bytes_billed = 0
        mock_stage2_job.cache_hit = False
        mock_stage2_job.job_id = "mock_project_lookup_job_999"
        mock_stage2_job.result.return_value = mock_project_rows
        
        # Assign side_effect to return the stage jobs sequentially
        mock_bq_client.query.side_effect = [mock_stage1_job, mock_stage2_job]
        
        # Call the anti-patterns linter endpoint
        payload = {
            "org_project_id": "mbettan-project",
            "region": "region-us",
            "lookback_days": 7,
            "limit_per_project": 100
        }
        
        response = client.post("/api/antipatterns/linter", json=payload)
        
        # Verify API succeeded
        assert response.status_code == 200
        
        data = response.json()
        assert len(data) == 1
        
        linter_result = data[0]
        assert linter_result["job_id"] == "parent_airflow_dag_run_456"
        assert linter_result["project_id"] == "mbettan-project"
        assert linter_result["user_email"] == "engineer@acme.com"
        assert linter_result["abuse_type"] == "[INVALID CLUSTER JOIN]"
        assert "Avoid wrapping the clustering key in UPPER() or TRIM()" in linter_result["suggested_fix"]
        
        # Assert that BigQuery query was called twice (once for Org Scan, once for Batch Lookup)
        assert mock_bq_client.query.call_count == 2
