from unittest.mock import MagicMock, patch
from fastapi.testclient import TestClient
from src.main import app
from src.migration_optimizer import (
    TranslationParams,
    TranslationResponse,
    SchemaFetcher,
    strip_leading_ddl,
    split_statements,
    synthesize_optimizer_yaml,
    SENTINEL,
)

client = TestClient(app)


def test_strip_leading_ddl_with_sentinel():
    sql = f"CREATE TABLE test (a INT64);\n\n{SENTINEL}\n\nSELECT * FROM test"
    assert strip_leading_ddl(sql) == "SELECT * FROM test"


def test_split_statements():
    sql = "SELECT 1; SELECT 2;"
    stmts = split_statements(sql)
    assert stmts == ["SELECT 1", "SELECT 2"]


def test_synthesize_optimizer_yaml():
    sql = """
    WITH cte AS (SELECT a FROM t)
    SELECT * FROM cte WHERE REGEXP_CONTAINS(a, '^x')
    AND a IN (SELECT b FROM t2)
    """
    yaml_str = synthesize_optimizer_yaml(sql, [])
    assert yaml_str is not None
    assert "experimental_optimizer" in yaml_str
    assert "REWRITE_CTE_TO_TEMP_TABLE" in yaml_str
    assert "REGEXP_CONTAINS_TO_LIKE" in yaml_str
    assert "ADD_DISTINCT_TO_SUBQUERY_IN_SET_COMPARISON" in yaml_str


def test_synthesize_yaml_anti_join_and_merge():
    """Verify anti-join and MERGE transformations are auto-detected."""
    sql = """
    MERGE INTO target t
    USING source s ON t.id = s.id
    WHEN MATCHED THEN UPDATE SET t.val = s.val;

    SELECT a FROM t1
    LEFT JOIN t2 ON t1.id = t2.id
    WHERE t2.id IS NULL
    """
    yaml_str = synthesize_optimizer_yaml(sql, [])
    assert yaml_str is not None
    assert "ANTI_JOIN_EXPLICIT_NOT_NULL" in yaml_str
    assert "MERGE_PRECOMPUTE_PRUNING_BOUNDARIES" in yaml_str


def test_synthesize_yaml_numeric_and_diagnostics():
    """Verify NUMERIC detection and Pass 1 diagnostic-driven opt-in."""
    sql = "SELECT CAST(x AS NUMERIC) FROM t"
    yaml_str = synthesize_optimizer_yaml(sql, [])
    assert yaml_str is not None
    assert "REWRITE_ZERO_SCALE_NUMERIC_AS_INTEGER" in yaml_str

    # Pass 1 diagnostic mentions a transformation name → auto-opt-in
    from src.migration_optimizer import MigrationIssue
    issue = MigrationIssue(category="OPTIMIZATION", message="Consider APPROXIMATE_RANGE_PARTITIONS for table t")
    yaml_str2 = synthesize_optimizer_yaml("SELECT 1", [issue])
    assert yaml_str2 is not None
    assert "APPROXIMATE_RANGE_PARTITIONS" in yaml_str2


def test_sql_injection_validation_in_schema_fetcher():
    fetcher = SchemaFetcher(project="valid-project")
    # Malformed table ref with quotes/backticks
    malformed_refs = ["valid_proj.valid_ds.table;DROP TABLE x;", "bad`proj.bad`ds.table"]
    ddl_list = fetcher.fetch_ddl(malformed_refs)
    assert ddl_list == []


@patch("src.main.run_migration_translation")
def test_translate_endpoint_schema(mock_run):
    mock_run.return_value = TranslationResponse(
        translated_sql="SELECT 1 AS test",
        original_sql="SELECT 1 AS test",
        bytes_before=0,
        bytes_after=0,
        bytes_delta_pct=0.0,
        success=True,
    )

    response = client.post(
        "/api/ai/translate",
        json={
            "query": "SELECT 1 AS test",
            "project_id": "gcp-sbx-prj-test-prcs01",
            "auto_ddl": False,
            "dry_run_compare": False,
        },
    )
    assert response.status_code == 200
    data = response.json()
    assert data["translated_sql"] == "SELECT 1 AS test"
    assert data["success"] is True
