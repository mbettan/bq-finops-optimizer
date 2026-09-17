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


class TestPass1TimeoutResilience:
    """Pass 1 discovery timeout must not abort the workflow; Pass 2 should proceed."""

    @patch("src.migration_optimizer.Auth")
    @patch("src.migration_optimizer.MigrationClient")
    @patch("src.migration_optimizer._execute_workflow_pass")
    def test_pass1_timeout_falls_back_to_pass2(self, mock_exec, mock_client_cls, mock_auth_cls):
        from src.migration_optimizer import run_migration_translation

        mock_auth = MagicMock()
        mock_auth.adc_project = "test-project"
        mock_auth_cls.return_value = mock_auth

        # Pass 1 raises TimeoutError; Pass 2 succeeds
        mock_exec.side_effect = [
            TimeoutError("Workflow timed out after 300s"),
            ({"input.sql": "SELECT 1 AS x"}, [], True, None),
        ]

        params = TranslationParams(
            query="SELECT 1 AS x",
            auto_opt_in_yaml=True,
            dry_run_compare=False,
        )

        res = run_migration_translation(params)

        assert res.success is True
        assert res.translated_sql == "SELECT 1 AS x"
        assert mock_exec.call_count == 2


class TestDiagnosticIssueExtraction:
    """Test that extract_migration_issues ignores file literals and extracts genuine issues."""

    def test_extract_migration_issues_ignores_literal_string(self):
        from src.migration_optimizer import extract_migration_issues

        payload = {
            "tasks": {
                "translation-task": {
                    "targetReturnLiterals": [
                        {"relativePath": "input.sql", "literalString": "SELECT * FROM my_table WHERE x = 'NUMERIC'"}
                    ]
                }
            },
            "subtasks": [
                {
                    "message": "Optimization available: consider MERGE_PRECOMPUTE_PRUNING_BOUNDARIES",
                    "category": "OPTIMIZATION",
                    "code": "OPT_001",
                }
            ]
        }

        issues = extract_migration_issues(payload)
        assert len(issues) == 1
        assert "MERGE_PRECOMPUTE_PRUNING_BOUNDARIES" in issues[0].message
        assert all("SELECT * FROM" not in i.message for i in issues)


class TestYamlSynthesisGuards:
    """Verify synthesize_optimizer_yaml avoids false opt-ins and duplicate rules."""

    def test_sql_comments_mentioning_numeric_do_not_trigger_zero_scale(self):
        from src.migration_optimizer import synthesize_optimizer_yaml

        sql = "SELECT id, name -- this column used to be a numeric string\nFROM users"
        yaml_str = synthesize_optimizer_yaml(sql, [])
        assert yaml_str is None

    def test_where_subquery_does_not_trigger_projection_scope(self):
        from src.migration_optimizer import synthesize_optimizer_yaml

        sql = "SELECT a, b FROM table1 WHERE x IN (SELECT id FROM table2)"
        yaml_str = synthesize_optimizer_yaml(sql, [])
        assert yaml_str is not None
        assert "scope: PREDICATE" in yaml_str
        assert "scope: PROJECTION" not in yaml_str

    def test_no_duplicate_transformations_when_diagnostics_repeat_pattern(self):
        from src.migration_optimizer import synthesize_optimizer_yaml, MigrationIssue

        sql = "WITH cte AS (SELECT 1) SELECT * FROM cte"
        issues = [MigrationIssue(category="OPTIMIZATION", message="Consider REWRITE_CTE_TO_TEMP_TABLE")]
        yaml_str = synthesize_optimizer_yaml(sql, issues)
        assert yaml_str is not None
        assert yaml_str.count("REWRITE_CTE_TO_TEMP_TABLE") == 1


class TestEndToEndMigrationOptimizer:
    """Test full 2-stage discovery and translation pipeline on anti-pattern queries."""

    @patch("src.migration_optimizer._execute_workflow_pass")
    @patch("src.migration_optimizer.SchemaFetcher.fetch_ddl", return_value=[])
    def test_cte_and_regexp_to_like_rewrite_pipeline(self, mock_ddl, mock_pass):
        from src.migration_optimizer import run_migration_translation, TranslationParams, MigrationIssue

        query = (
            "WITH expensive_cte AS (\n"
            "  SELECT id, name, status, REGEXP_CONTAINS(name, r'^PROD_') AS is_prod\n"
            "  FROM `my_project.my_dataset.orders`\n"
            ")\n"
            "SELECT * FROM expensive_cte WHERE is_prod = TRUE\n"
            "UNION ALL\n"
            "SELECT * FROM expensive_cte WHERE status = 'COMPLETED'"
        )

        optimized_sql = (
            "CREATE TEMP TABLE expensive_cte AS\n"
            "  SELECT id, name, status, name LIKE 'PROD_%' AS is_prod\n"
            "  FROM `my_project.my_dataset.orders`;\n\n"
            "SELECT * FROM expensive_cte WHERE is_prod = TRUE\n"
            "UNION ALL\n"
            "SELECT * FROM expensive_cte WHERE status = 'COMPLETED';"
        )

        # Pass 1: discovery diagnostics
        # Pass 2: actual translation
        mock_pass.side_effect = [
            ({}, [
                MigrationIssue(category="OPTIMIZATION", message="Common Table Expression has been rewritten: REWRITE_CTE_TO_TEMP_TABLE"),
                MigrationIssue(category="OPTIMIZATION", message="REGEXP_CONTAINS has been rewritten: REGEXP_CONTAINS_TO_LIKE"),
            ], True, None),
            ({"input.sql": optimized_sql}, [], True, None),
        ]

        params = TranslationParams(
            query=query,
            project_id="test-project",
            auto_opt_in_yaml=True,
            dry_run_compare=False,
        )

        res = run_migration_translation(params)

        assert res.success is True
        assert res.translated_sql == optimized_sql
        assert res.applied_config_yaml is not None
        assert "REWRITE_CTE_TO_TEMP_TABLE" in res.applied_config_yaml
        assert "REGEXP_CONTAINS_TO_LIKE" in res.applied_config_yaml




