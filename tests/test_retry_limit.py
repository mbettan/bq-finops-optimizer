import pytest
from unittest.mock import MagicMock
from google.api_core import exceptions as gax_exc
from google.cloud import bigquery
from src.utils import run_query_with_retry_limit

def test_run_query_with_retry_limit_retries_and_succeeds():
    mock_client = MagicMock()
    mock_job = MagicMock()
    mock_results = MagicMock()
    
    # Fail once with InternalServerError, then succeed
    mock_client.query.return_value = mock_job
    mock_job.result.side_effect = [
        gax_exc.InternalServerError("Transient internal error"),
        mock_results
    ]
    
    job, res = run_query_with_retry_limit(mock_client, "SELECT 1", MagicMock(), description="Test Query", max_attempts=5, initial_delay=0.01)
    
    assert job == mock_job
    assert res == mock_results
    assert mock_job.result.call_count == 2

def test_run_query_with_retry_limit_fails_after_5_attempts():
    mock_client = MagicMock()
    mock_job = MagicMock()
    
    mock_client.query.return_value = mock_job
    mock_job.result.side_effect = gax_exc.InternalServerError("An internal error occurred: 33494873")
    
    with pytest.raises(gax_exc.InternalServerError):
        run_query_with_retry_limit(mock_client, "SELECT 1", MagicMock(), description="Test Query", max_attempts=5, initial_delay=0.01)
    
    # Must attempt exactly 5 times before giving up
    assert mock_job.result.call_count == 5
