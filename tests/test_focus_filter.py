"""Tests for build_project_filter() — the parameterized SQL clause builder."""
import pytest
from src.utils import build_project_filter


def test_none_returns_empty():
    clause, params = build_project_filter(None)
    assert clause == ""
    assert params == []


def test_empty_list_returns_empty():
    clause, params = build_project_filter([])
    assert clause == ""
    assert params == []


def test_single_project():
    clause, params = build_project_filter(["proj-a"])
    assert clause == "AND project_id IN UNNEST(@focus_projects)"
    assert len(params) == 1
    assert params[0].name == "focus_projects"
    assert params[0].array_type == "STRING"
    assert params[0].values == ["proj-a"]


def test_multiple_projects():
    clause, params = build_project_filter(["proj-a", "proj-b"])
    assert clause == "AND project_id IN UNNEST(@focus_projects)"
    assert len(params) == 1
    assert params[0].values == ["proj-a", "proj-b"]


def test_custom_column_project_name():
    clause, params = build_project_filter(["proj-a"], column="project_name")
    assert clause == "AND project_name IN UNNEST(@focus_projects)"


def test_invalid_column_raises():
    with pytest.raises(ValueError, match="Unsupported filter column"):
        build_project_filter(["proj-a"], column="evil_column")


def test_table_alias():
    clause, params = build_project_filter(["proj-a"], table_alias="j")
    assert clause == "AND j.project_id IN UNNEST(@focus_projects)"


def test_table_alias_with_column():
    clause, params = build_project_filter(["proj-a"], column="project_name", table_alias="t")
    assert clause == "AND t.project_name IN UNNEST(@focus_projects)"


def test_invalid_table_alias_raises():
    with pytest.raises(ValueError, match="Invalid table alias"):
        build_project_filter(["proj-a"], table_alias="evil; --")


def test_invalid_table_alias_starts_with_number():
    with pytest.raises(ValueError, match="Invalid table alias"):
        build_project_filter(["proj-a"], table_alias="1bad")


def test_returned_param_is_array_of_strings():
    clause, params = build_project_filter(["a", "b"])
    assert len(params) == 1
    assert params[0].name == "focus_projects"
    assert params[0].array_type == "STRING"
    assert params[0].values == ["a", "b"]
