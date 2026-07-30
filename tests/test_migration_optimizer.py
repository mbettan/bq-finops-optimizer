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
    assert "type: optimizer" in yaml_str
    assert "REWRITE_CTE_TO_TEMP_TABLE" in yaml_str
    assert "REGEXP_CONTAINS_TO_LIKE" in yaml_str
    assert "ADD_DISTINCT_TO_SUBQUERY_IN_SET_COMPARISON" in yaml_str


def test_synthesize_yaml_anti_join_and_merge():
    """Verify anti-join and MERGE transformations are auto-detected."""
    from src.migration_optimizer import MigrationIssue
    sql = """
    MERGE INTO target t
    USING source s ON t.id = s.id
    WHEN MATCHED THEN UPDATE SET t.val = s.val;

    SELECT a FROM t1
    LEFT JOIN t2 ON t1.id = t2.id
    WHERE t2.id IS NULL
    """
    issues = [
        MigrationIssue(category="OPTIMIZATION", message="Consider ANTI_JOIN_EXPLICIT_NOT_NULL"),
        MigrationIssue(category="OPTIMIZATION", message="Consider MERGE_PRECOMPUTE_PRUNING_BOUNDARIES"),
    ]
    yaml_str = synthesize_optimizer_yaml(sql, issues)
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


def test_synthesize_yaml_cte_threshold_is_4():
    """CTE rewriting threshold must be 4 (compiler default), not 1.

    Per Tom Wall (2026-07-27): threshold=1 converts every CTE to CTAS + temp
    table reference, which can hurt more than it helps. The compiler default
    threshold is 4.
    """
    sql = "WITH cte AS (SELECT 1) SELECT * FROM cte"
    yaml_str = synthesize_optimizer_yaml(sql, [])
    assert yaml_str is not None
    assert "REWRITE_CTE_TO_TEMP_TABLE" in yaml_str
    assert "threshold: 4" in yaml_str
    assert "threshold: 1" not in yaml_str


def test_synthesize_yaml_minimal_sql_returns_none():
    """Minimal SQL with no patterns and no diagnostics should return None.

    Per Tom Wall (2026-07-27): these optimizations have domain-specific
    tradeoffs and aren't universally a good thing to do. Only opt in
    when evidence suggests they'll help.
    """
    yaml_str = synthesize_optimizer_yaml("SELECT 1", [])
    assert yaml_str is None


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
            "project_id": "dev-test-project01",
            "auto_ddl": False,
            "dry_run_compare": False,
        },
    )
    assert response.status_code == 200
    data = response.json()
    assert data["translated_sql"] == "SELECT 1 AS test"
    assert data["success"] is True


# ---------------------------------------------------------------------------
# C3 Regression: _execute_workflow_pass must NOT strip translated SQL
# ---------------------------------------------------------------------------

from src.migration_optimizer import (
    _execute_workflow_pass,
    MigrationClient,
    HttpResult,
    MigrationIssue,
)


def _make_mock_client(workflow_payload, subtask_payload=None):
    """Create a mock MigrationClient that returns predetermined payloads."""
    mock = MagicMock(spec=MigrationClient)
    # create_workflow returns 200 with a resource name
    mock.create_workflow.return_value = HttpResult(
        status=200,
        body={"name": "projects/p/locations/us/workflows/w-123"},
        text="",
    )
    # wait returns the workflow payload
    mock.wait.return_value = workflow_payload
    # subtasks returns 200 with subtask payload (or empty)
    mock.subtasks.return_value = HttpResult(
        status=200,
        body=subtask_payload or {"subtasks": []},
        text="",
    )
    # delete always succeeds
    mock.delete.return_value = HttpResult(status=200, body={}, text="")
    return mock


def _make_params_for_pass():
    return TranslationParams(
        query="SELECT * FROM t",
        project_id="test-project",
        timeout_seconds=30,
    )


class TestC3EchoStripping:
    """Regression tests for C3: only strip echoed input, never translated output."""

    def test_translated_sql_is_kept_when_different_from_input(self):
        """When the API returns DIFFERENT SQL in walk_literals, it's a real
        translation — C3 must NOT strip it."""
        source_sql = "SELECT * FROM my_table WHERE x > 1\n"
        translated_sql = "SELECT * FROM my_table WHERE x > 1 -- optimized\n"

        # Workflow payload contains the translated SQL under input.sql
        workflow_payload = {
            "state": "COMPLETED",
            "tasks": {"t": {"targetReturnLiterals": [
                {"relativePath": "input.sql", "literalString": translated_sql}
            ]}}
        }
        mock_client = _make_mock_client(workflow_payload)
        files = [("input.sql", source_sql)]
        params = _make_params_for_pass()

        literals, issues, ok, err = _execute_workflow_pass(mock_client, files, params)

        assert ok is True
        assert "input.sql" in literals, (
            "C3 regression: translated SQL was stripped even though it differs from input"
        )
        assert literals["input.sql"] == translated_sql

    def test_echoed_input_is_stripped(self):
        """When the API echoes the EXACT source input back, C3 must strip it."""
        source_sql = "SELECT * FROM my_table WHERE x > 1\n"

        # Workflow payload echoes the source SQL verbatim
        workflow_payload = {
            "state": "COMPLETED",
            "tasks": {"t": {"targetReturnLiterals": [
                {"relativePath": "input.sql", "literalString": source_sql}
            ]}}
        }
        mock_client = _make_mock_client(workflow_payload)
        files = [("input.sql", source_sql)]
        params = _make_params_for_pass()

        literals, issues, ok, err = _execute_workflow_pass(mock_client, files, params)

        assert ok is True
        assert "input.sql" not in literals, (
            "C3: echoed source SQL should have been stripped but was kept"
        )

    def test_subtask_output_overrides_after_echo_strip(self):
        """When walk_literals echoes the input (stripped by C3), but the subtask
        endpoint returns the REAL translated SQL, the subtask wins."""
        source_sql = "SELECT * FROM my_table\n"
        real_translation = "SELECT * FROM my_table -- rewritten by compiler\n"

        # Main payload echoes source
        workflow_payload = {
            "state": "COMPLETED",
            "tasks": {"t": {"targetReturnLiterals": [
                {"relativePath": "input.sql", "literalString": source_sql}
            ]}}
        }
        # Subtask carries the real translation
        subtask_payload = {
            "subtasks": [{
                "resource": {
                    "targetReturnLiterals": [
                        {"relativePath": "input.sql", "literalString": real_translation}
                    ]
                }
            }]
        }
        mock_client = _make_mock_client(workflow_payload, subtask_payload)
        files = [("input.sql", source_sql)]
        params = _make_params_for_pass()

        literals, issues, ok, err = _execute_workflow_pass(mock_client, files, params)

        assert ok is True
        assert literals.get("input.sql") == real_translation

