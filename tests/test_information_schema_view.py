"""Unit tests for analysis-scope → INFORMATION_SCHEMA view mapping."""

import pytest
from fastapi import HTTPException

from src.utils import (
    information_schema_view,
    validate_analysis_scope,
    analysis_scope_from_params,
)


@pytest.mark.parametrize(
    "base_name,scope,expected",
    [
        ("JOBS", "organization", "JOBS_BY_ORGANIZATION"),
        ("JOBS", "folder", "JOBS_BY_FOLDER"),
        ("JOBS", "project", "JOBS_BY_PROJECT"),
        ("JOBS_TIMELINE", "organization", "JOBS_TIMELINE_BY_ORGANIZATION"),
        ("JOBS_TIMELINE", "folder", "JOBS_TIMELINE_BY_FOLDER"),
        ("JOBS_TIMELINE", "project", "JOBS_TIMELINE_BY_PROJECT"),
        ("TABLE_STORAGE", "organization", "TABLE_STORAGE_BY_ORGANIZATION"),
        ("TABLE_STORAGE", "folder", "TABLE_STORAGE_BY_FOLDER"),
        ("TABLE_STORAGE", "project", "TABLE_STORAGE_BY_PROJECT"),
    ],
)
def test_information_schema_view_mapping(base_name, scope, expected):
    assert information_schema_view(base_name, scope) == expected


def test_information_schema_view_defaults_to_organization():
    assert information_schema_view("JOBS") == "JOBS_BY_ORGANIZATION"


def test_information_schema_view_rejects_unknown_base():
    with pytest.raises(ValueError, match="Unsupported INFORMATION_SCHEMA base"):
        information_schema_view("RECOMMENDATIONS", "organization")


def test_information_schema_view_rejects_unknown_scope():
    with pytest.raises(ValueError, match="Unsupported analysis scope"):
        information_schema_view("JOBS", "team")


@pytest.mark.parametrize(
    "raw,expected",
    [
        (None, "organization"),
        ("", "organization"),
        ("organization", "organization"),
        ("FOLDER", "folder"),
        (" project ", "project"),
    ],
)
def test_validate_analysis_scope_normalizes(raw, expected):
    assert validate_analysis_scope(raw) == expected


def test_validate_analysis_scope_rejects_invalid():
    with pytest.raises(HTTPException) as exc_info:
        validate_analysis_scope("team")
    assert exc_info.value.status_code == 400
    assert "analysis_scope" in exc_info.value.detail


def test_analysis_scope_from_params_defaults():
    class _P:
        pass

    assert analysis_scope_from_params(_P()) == "organization"


def test_analysis_scope_from_params_reads_field():
    class _P:
        analysis_scope = "folder"

    assert analysis_scope_from_params(_P()) == "folder"
