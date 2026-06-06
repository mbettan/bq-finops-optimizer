import pytest
from unittest.mock import MagicMock, patch
from fastapi import HTTPException
from pydantic import BaseModel
from typing import Optional
from src.utils import init_bq_client_and_resolve_project, reject_dummy_project

class MockParams(BaseModel):
    org_project_id: Optional[str] = None

def test_resolve_with_explicit_valid_project():
    params = MockParams(org_project_id="valid-project-id")
    with patch("src.utils.bigquery.Client") as mock_client_class:
        mock_client = MagicMock()
        mock_client.project = "resolved-project-id"
        mock_client_class.return_value = mock_client
        
        client, resolved_project = init_bq_client_and_resolve_project(params)
        
        mock_client_class.assert_called_once_with(project="valid-project-id")
        assert resolved_project == "valid-project-id"

def test_resolve_with_client_fallback():
    params = MockParams(org_project_id=None)
    with patch("src.utils.bigquery.Client") as mock_client_class:
        mock_client = MagicMock()
        mock_client.project = "resolved-project-id"
        mock_client_class.return_value = mock_client
        
        client, resolved_project = init_bq_client_and_resolve_project(params)
        
        mock_client_class.assert_called_once_with()
        assert resolved_project == "resolved-project-id"

def test_resolve_empty_string_falls_back():
    params = MockParams(org_project_id="  ")
    with patch("src.utils.bigquery.Client") as mock_client_class:
        mock_client = MagicMock()
        mock_client.project = "resolved-project-id"
        mock_client_class.return_value = mock_client
        
        client, resolved_project = init_bq_client_and_resolve_project(params)
        
        mock_client_class.assert_called_once_with()
        assert resolved_project == "resolved-project-id"

def test_resolve_raises_if_empty():
    params = MockParams(org_project_id="")
    with patch("src.utils.bigquery.Client") as mock_client_class:
        mock_client = MagicMock()
        mock_client.project = ""
        mock_client_class.return_value = mock_client
        
        with pytest.raises(HTTPException) as exc_info:
            init_bq_client_and_resolve_project(params)
        assert exc_info.value.status_code == 400
        assert "GCP Project ID must be specified" in exc_info.value.detail

def test_resolve_raises_if_dummy():
    params = MockParams(org_project_id="mbettan-sandbox")
    with patch("src.utils.bigquery.Client") as mock_client_class:
        mock_client = MagicMock()
        mock_client.project = "mbettan-sandbox"
        mock_client_class.return_value = mock_client
        
        with pytest.raises(HTTPException) as exc_info:
            init_bq_client_and_resolve_project(params)
        assert exc_info.value.status_code == 400
        assert "dummy placeholder" in exc_info.value.detail

def test_resolve_raises_if_invalid_format():
    params = MockParams(org_project_id="project;drop table jobs;")
    with patch("src.utils.bigquery.Client") as mock_client_class:
        mock_client = MagicMock()
        mock_client.project = "project"
        mock_client_class.return_value = mock_client
        
        with pytest.raises(HTTPException) as exc_info:
            init_bq_client_and_resolve_project(params)
        assert exc_info.value.status_code == 400
        assert "Invalid project_id" in exc_info.value.detail

def test_reject_dummy_project_direct():
    # Reject placeholders
    with pytest.raises(HTTPException) as exc_info:
        reject_dummy_project("mbettan-sandbox")
    assert exc_info.value.status_code == 400
    assert "dummy placeholder" in exc_info.value.detail
    
    with pytest.raises(HTTPException) as exc_info:
        reject_dummy_project("your-project-id")
    assert exc_info.value.status_code == 400
    assert "dummy placeholder" in exc_info.value.detail
    
    # Accept valid project ID
    reject_dummy_project("valid-project-id")
    reject_dummy_project(None)
    reject_dummy_project("")
