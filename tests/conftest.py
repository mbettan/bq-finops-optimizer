"""
Shared test fixtures for the BigQuery FinOps Optimizer test suite.

Provides mock BigQuery client fixtures that prevent any real GCP API calls
during testing, enabling fast offline test execution.
"""

import pytest
import pandas as pd
from unittest.mock import MagicMock, patch
from fastapi.testclient import TestClient
from src.main import app


@pytest.fixture
def test_client():
    """FastAPI test client instance."""
    return TestClient(app)


@pytest.fixture
def mock_bq_row_factory():
    """Factory to create mock BigQuery row objects with named attributes."""
    def _make_row(**kwargs):
        row = MagicMock()
        for key, value in kwargs.items():
            setattr(row, key, value)
        return row
    return _make_row


def _make_mock_job():
    """Create a mock BigQuery query job with both .result() iteration
    and .result().to_dataframe() support."""
    job = MagicMock()

    # The result object: iterable (empty list) AND supports .to_dataframe()
    result_obj = MagicMock()
    result_obj.__iter__ = MagicMock(return_value=iter([]))
    result_obj.__len__ = MagicMock(return_value=0)
    result_obj.to_dataframe.return_value = pd.DataFrame()

    job.result.return_value = result_obj
    job.total_bytes_processed = 0
    job.total_bytes_billed = 0
    job.cache_hit = False
    job.job_id = "mock-job-id"
    # Also support direct .to_dataframe() on the job itself
    job.to_dataframe.return_value = pd.DataFrame()
    return job


@pytest.fixture
def mock_bq_job():
    """A pre-configured mock BigQuery query job that returns empty results."""
    return _make_mock_job()


@pytest.fixture
def mock_bq_all():
    """
    Patches init_bq_client_and_resolve_project in ALL modules (main, hbo,
    cost_attribution, fluid_scaling) so no real BQ client is ever created.

    Returns the mock_client — can be configured to return specific query results.
    """
    mock_client = MagicMock()
    mock_client.project = "test-project"

    # Default: queries return empty results with metadata + to_dataframe support
    mock_client.query.return_value = _make_mock_job()

    # Also mock list_datasets / list_tables for governance endpoints
    mock_client.list_datasets.return_value = []
    mock_client.list_tables.return_value = []

    resolve_return = (mock_client, "test-project")

    patches = {
        "main": patch("src.main.init_bq_client_and_resolve_project", return_value=resolve_return),
        "hbo": patch("src.hbo.init_bq_client_and_resolve_project", return_value=resolve_return),
        "cost_attribution": patch(
            "src.cost_attribution.init_bq_client_and_resolve_project",
            return_value=resolve_return,
        ),
        "fluid_scaling": patch(
            "src.fluid_scaling.init_bq_client_and_resolve_project",
            return_value=resolve_return,
        ),
    }

    started = {name: p.start() for name, p in patches.items()}
    yield mock_client
    for p in patches.values():
        p.stop()
