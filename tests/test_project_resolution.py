import pytest
from unittest.mock import MagicMock, patch
from fastapi import HTTPException
from pydantic import BaseModel
from typing import Optional
from src.utils import init_bq_client_and_resolve_project, reject_dummy_project, validate_focus_projects

class MockParams(BaseModel):
    org_project_id: Optional[str] = None

def test_resolve_with_explicit_valid_project():
    params = MockParams(org_project_id="valid-project-id")
    with patch("src.utils.google.auth.default", return_value=(MagicMock(), "adc-project")), \
         patch("src.utils.bigquery.Client") as mock_client_class:
        mock_client = MagicMock()
        mock_client.project = "resolved-project-id"
        mock_client_class.return_value = mock_client
        
        client, resolved_project = init_bq_client_and_resolve_project(params)
        
        assert mock_client_class.call_count == 1
        assert mock_client_class.call_args[1]["project"] == "valid-project-id"
        assert "credentials" in mock_client_class.call_args[1]
        assert resolved_project == "valid-project-id"

def test_resolve_with_client_fallback():
    params = MockParams(org_project_id=None)
    with patch("src.utils.google.auth.default", return_value=(MagicMock(), "adc-project")), \
         patch("src.utils.bigquery.Client") as mock_client_class:
        mock_client = MagicMock()
        mock_client.project = "resolved-project-id"
        mock_client_class.return_value = mock_client
        
        client, resolved_project = init_bq_client_and_resolve_project(params)
        
        assert mock_client_class.call_count == 1
        assert mock_client_class.call_args[1]["project"] == "adc-project"
        assert "credentials" in mock_client_class.call_args[1]
        assert resolved_project == "resolved-project-id"

def test_resolve_empty_string_falls_back():
    params = MockParams(org_project_id="  ")
    with patch("src.utils.google.auth.default", return_value=(MagicMock(), "adc-project")), \
         patch("src.utils.bigquery.Client") as mock_client_class:
        mock_client = MagicMock()
        mock_client.project = "resolved-project-id"
        mock_client_class.return_value = mock_client
        
        client, resolved_project = init_bq_client_and_resolve_project(params)
        
        assert mock_client_class.call_count == 1
        assert mock_client_class.call_args[1]["project"] == "adc-project"
        assert "credentials" in mock_client_class.call_args[1]
        assert resolved_project == "resolved-project-id"

def test_resolve_raises_if_empty():
    params = MockParams(org_project_id="")
    with patch("src.utils.google.auth.default", return_value=(MagicMock(), "adc-project")), \
         patch("src.utils.bigquery.Client") as mock_client_class:
        mock_client = MagicMock()
        mock_client.project = ""
        mock_client_class.return_value = mock_client
        
        with pytest.raises(HTTPException) as exc_info:
            init_bq_client_and_resolve_project(params)
        assert exc_info.value.status_code == 400
        assert "GCP Project ID must be specified" in exc_info.value.detail

def test_resolve_raises_if_dummy():
    params = MockParams(org_project_id="mbettan-sandbox")
    with patch("src.utils.google.auth.default", return_value=(MagicMock(), "adc-project")), \
         patch("src.utils.bigquery.Client") as mock_client_class:
        mock_client = MagicMock()
        mock_client.project = "mbettan-sandbox"
        mock_client_class.return_value = mock_client
        
        with pytest.raises(HTTPException) as exc_info:
            init_bq_client_and_resolve_project(params)
        assert exc_info.value.status_code == 400
        assert "dummy placeholder" in exc_info.value.detail

def test_resolve_raises_if_invalid_format():
    params = MockParams(org_project_id="project;drop table jobs;")
    with patch("src.utils.google.auth.default", return_value=(MagicMock(), "adc-project")), \
         patch("src.utils.bigquery.Client") as mock_client_class:
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


# --- validate_focus_projects tests ---

def test_validate_focus_projects_trims_whitespace():
    result = validate_focus_projects([" proj-a ", "proj-b"])
    assert result == ["proj-a", "proj-b"]

def test_validate_focus_projects_deduplicates():
    result = validate_focus_projects(["proj-a", "proj-b", "proj-a"])
    assert result == ["proj-a", "proj-b"]

def test_validate_focus_projects_empty_to_none():
    assert validate_focus_projects([]) is None

def test_validate_focus_projects_none_to_none():
    assert validate_focus_projects(None) is None

def test_validate_focus_projects_whitespace_only_to_none():
    assert validate_focus_projects(["", " ", "  "]) is None

def test_validate_focus_projects_rejects_dummy():
    with pytest.raises(HTTPException) as exc_info:
        validate_focus_projects(["mbettan-sandbox"])
    assert exc_info.value.status_code == 400
    assert "dummy placeholder" in exc_info.value.detail

def test_validate_focus_projects_rejects_unsafe_chars():
    with pytest.raises(HTTPException) as exc_info:
        validate_focus_projects(["proj;DROP TABLE"])
    assert exc_info.value.status_code == 400

def test_validate_focus_projects_cap_exceeded():
    projects = [f"proj-{i}" for i in range(51)]
    with pytest.raises(HTTPException) as exc_info:
        validate_focus_projects(projects)
    assert exc_info.value.status_code == 400
    assert "at most 50" in exc_info.value.detail

def test_validate_focus_projects_cap_at_limit():
    projects = [f"proj-{i}" for i in range(50)]
    result = validate_focus_projects(projects)
    assert len(result) == 50

def test_validate_focus_projects_valid_single():
    result = validate_focus_projects(["my-project-123"])
    assert result == ["my-project-123"]

def test_validate_focus_projects_valid_gcp_formats():
    """GCP project IDs can contain hyphens, dots, and colons."""
    result = validate_focus_projects(["example.com:my-project", "my-org-project-123"])
    assert result == ["example.com:my-project", "my-org-project-123"]
