"""
migration_optimizer.py — BigQuery Migration API SQL Translation & Optimization Engine.

Provides automated BigQuery-to-BigQuery query rewriting, local schema DDL auto-resolution,
2-stage discovery & opt-in YAML synthesis, and dry-run byte delta validation.
"""

from __future__ import annotations

import logging
import re
import time
from dataclasses import dataclass, field
from typing import Any, Iterable, Optional

import requests
import google.auth
from google.auth.transport.requests import Request as GoogleAuthRequest
from pydantic import BaseModel, Field

logger = logging.getLogger("src.migration_optimizer")

# --------------------------------------------------------------------------
# Constants & Defaults
# --------------------------------------------------------------------------

API_HOST = "https://bigquerymigration.googleapis.com"
DEFAULT_API_VERSION = "v2"
DEFAULT_TASK_TYPE = "BigQuery2BigQuery_Translation"
SENTINEL = "-- __BQXLATE_QUERY_BEGIN__"

IDENT_RE = re.compile(r"^[A-Za-z0-9_-]+$")

TABLE_REF_RE = re.compile(
    r"\b(?:FROM|JOIN|INTO|UPDATE|TABLE)\s+"
    r"(`[^`]+`|[A-Za-z_][\w\-]*(?:\.[A-Za-z_][\w\-]*){1,2})",
    re.I,
)


# --------------------------------------------------------------------------
# Pydantic Schemas for API Endpoints
# --------------------------------------------------------------------------

class MigrationIssue(BaseModel):
    category: str = Field("WARNING", description="Issue severity: INFO, WARNING, ERROR")
    code: Optional[str] = Field(None, description="Diagnostic issue code")
    message: str = Field(..., description="Diagnostic issue description")


class TranslationParams(BaseModel):
    query: str = Field(..., description="Source SQL query to optimize/translate")
    project_id: Optional[str] = Field(None, description="GCP Project ID")
    location: str = Field("us", description="BigQuery region location (e.g. us, eu)")
    auto_ddl: bool = Field(True, description="Automatically fetch DDL for referenced tables via INFORMATION_SCHEMA")
    auto_opt_in_yaml: bool = Field(True, description="Perform 2-stage discovery to synthesize optimizer.config.yaml")
    config_yaml_content: Optional[str] = Field(None, description="Verbatim contents of translation.config.yaml")
    config_filename: str = Field("optimizer.config.yaml", description="Configuration filename ending in .config.yaml")
    default_database: Optional[str] = Field(None, description="Default database for unqualified table names")
    schema_search_path: list[str] = Field(default_factory=list, description="Schema search path")
    dry_run_compare: bool = Field(True, description="Perform dry-run byte comparison before/after translation")
    keep_ddl_in_output: bool = Field(False, description="Include prepended schema DDL in the final translated output")
    timeout_seconds: int = Field(300, description="Maximum polling timeout in seconds")


class TranslationResponse(BaseModel):
    translated_sql: str
    original_sql: str
    bytes_before: Optional[int] = None
    bytes_after: Optional[int] = None
    bytes_delta_pct: Optional[float] = None
    workflow_name: Optional[str] = None
    issues: list[MigrationIssue] = Field(default_factory=list)
    applied_config_yaml: Optional[str] = None
    success: bool = True
    error_message: Optional[str] = None


# --------------------------------------------------------------------------
# Auth & HTTP Client
# --------------------------------------------------------------------------

class Auth:
    def __init__(self, scopes: Iterable[str] = ("https://www.googleapis.com/auth/cloud-platform",)):
        self.creds, self.adc_project = google.auth.default(scopes=list(scopes))

    def token(self) -> str:
        if not self.creds.valid:
            self.creds.refresh(GoogleAuthRequest())
        return self.creds.token

    def headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self.token()}", "Content-Type": "application/json"}


@dataclass
class HttpResult:
    status: int
    body: Any
    text: str


class Http:
    def __init__(self, auth: Auth):
        self.auth = auth
        self.session = requests.Session()

    def call(self, method: str, url: str, payload: dict | None = None, retries: int = 3) -> HttpResult:
        backoff = 1.5
        is_idempotent = method.upper() in ("GET", "DELETE")
        max_attempts = retries if is_idempotent else 1

        last: HttpResult | None = None
        for attempt in range(max_attempts):
            try:
                resp = self.session.request(
                    method, url, headers=self.auth.headers(),
                    json=payload, timeout=60,
                )
                try:
                    body = resp.json()
                except ValueError:
                    body = None
                last = HttpResult(resp.status_code, body, resp.text)
                if resp.status_code == 429 and attempt < max_attempts - 1:
                    time.sleep(3.0 * (attempt + 1))
                    continue
                if resp.status_code in (500, 502, 503, 504) and attempt < max_attempts - 1:
                    time.sleep(backoff ** (attempt + 1))
                    continue
                return last
            except requests.exceptions.RequestException as exc:
                last = HttpResult(500, None, str(exc))
                if attempt < max_attempts - 1:
                    time.sleep(backoff ** (attempt + 1))
                    continue
                return last
        return last  # type: ignore[return-value]


# --------------------------------------------------------------------------
# SQL & Schema Helpers
# --------------------------------------------------------------------------

def split_statements(sql: str) -> list[str]:
    out, buf = [], []
    i, n = 0, len(sql)
    quote = None
    while i < n:
        c = sql[i]
        nxt = sql[i + 1] if i + 1 < n else ""
        if quote:
            buf.append(c)
            if c == "\\" and quote in ("'", '"'):
                if nxt:
                    buf.append(nxt); i += 2; continue
            elif c == quote:
                quote = None
            i += 1
            continue
        if c == "-" and nxt == "-":
            j = sql.find("\n", i)
            j = n if j == -1 else j
            buf.append(sql[i:j]); i = j; continue
        if c == "/" and nxt == "*":
            j = sql.find("*/", i + 2)
            j = n if j == -1 else j + 2
            buf.append(sql[i:j]); i = j; continue
        if c in ("'", '"', "`"):
            quote = c; buf.append(c); i += 1; continue
        if c == ";":
            stmt = "".join(buf).strip()
            if stmt:
                out.append(stmt)
            buf = []; i += 1; continue
        buf.append(c); i += 1
    tail = "".join(buf).strip()
    if tail:
        out.append(tail)
    return out


def strip_leading_ddl(sql: str) -> str:
    """Strips injected schema DDL prepended before SENTINEL.
    Preserves multi-statement outputs (e.g. CREATE TEMP TABLE ...; SELECT ...)."""
    if SENTINEL in sql:
        return sql.split(SENTINEL, 1)[1].strip()
    return sql.strip()


def regex_table_refs(sql: str) -> set[str]:
    found = set()
    for m in TABLE_REF_RE.finditer(sql):
        raw = m.group(1)
        # Strip all backticks
        clean = raw.replace("`", "").strip()
        if clean.upper() in ("SELECT", "UNNEST", "LATERAL"):
            continue
        if "." in clean:
            found.add(clean)
    return found


@dataclass
class SchemaFetcher:
    project: str
    _client: Any = field(default=None, init=False, repr=False)

    @property
    def client(self):
        if self._client is None:
            from google.cloud import bigquery
            self._client = bigquery.Client(project=self.project)
        return self._client

    def dry_run_refs(self, sql: str) -> list[str]:
        from google.cloud import bigquery
        cfg = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
        job = self.client.query(sql, job_config=cfg)
        return [f"{t.project}.{t.dataset_id}.{t.table_id}" for t in (job.referenced_tables or [])]

    def discover(self, sql: str) -> list[str]:
        try:
            refs = self.dry_run_refs(sql)
            if refs:
                return refs
        except Exception:
            pass
        out = []
        for ref in sorted(regex_table_refs(sql)):
            parts = [p for p in ref.split(".") if p]
            if len(parts) == 3:
                out.append(".".join(parts))
            elif len(parts) == 2:
                out.append(f"{self.project}.{parts[0]}.{parts[1]}")
        return out

    def fetch_ddl(self, refs: list[str]) -> list[str]:
        seen: dict[str, tuple[str, str]] = {}
        by_dataset: dict[tuple[str, str], list[str]] = {}

        for fqn in refs:
            parts = fqn.split(".")
            if len(parts) == 3:
                proj, ds, table = parts[0], parts[1], parts[2]
                # SQL Injection validation gate
                if IDENT_RE.match(proj) and IDENT_RE.match(ds) and IDENT_RE.match(table):
                    by_dataset.setdefault((proj, ds), []).append(table)

        for (proj, ds), names in by_dataset.items():
            q = (
                f"SELECT table_name, table_type, ddl "
                f"FROM `{proj}`.`{ds}`.INFORMATION_SCHEMA.TABLES "
                f"WHERE table_name IN UNNEST(@names)"
            )
            from google.cloud import bigquery
            cfg = bigquery.QueryJobConfig(query_parameters=[
                bigquery.ArrayQueryParameter("names", "STRING", names)
            ])
            try:
                rows = list(self.client.query(q, job_config=cfg).result())
                for r in rows:
                    key = f"{proj}.{ds}.{r.table_name}"
                    seen[key] = (r.table_type or "", r.ddl or "")
            except Exception as e:
                logger.warning(f"Failed to fetch DDL for {proj}.{ds}: {e}")
                continue

        rank = {"BASE TABLE": 0, "EXTERNAL": 1, "VIEW": 2, "MATERIALIZED VIEW": 3}
        ordered = sorted(seen.items(), key=lambda kv: rank.get(kv[1][0].upper(), 9))
        return [ddl.rstrip().rstrip(";") + ";" for _, (_t, ddl) in ordered if ddl]


# --------------------------------------------------------------------------
# Migration Client
# --------------------------------------------------------------------------

class MigrationClient:
    def __init__(self, http: Http, project: str, location: str = "us"):
        self.http, self.project, self.location = http, project, location

    def _base(self, version: str) -> str:
        return f"{API_HOST}/{version}/projects/{self.project}/locations/{self.location}"

    def create_workflow(self, files: list[tuple[str, str]], default_db: str | None = None,
                        search_path: list[str] | None = None) -> HttpResult:
        mappings = [{
            "sourceSpec": {"literal": {"relativePath": p, "literalString": t}},
            "targetSpec": {"relativePath": p}
        } for p, t in files]

        # S1-3 Fix: Only request .sql files back as target return literals
        target_sql_literals = [p for p, _ in files if p.endswith(".sql")]

        details: dict[str, Any] = {
            "sourceTargetMapping": mappings,
            "targetReturnLiterals": target_sql_literals,
        }
        if default_db or search_path:
            env: dict[str, Any] = {}
            if default_db:
                env["defaultDatabase"] = default_db
            if search_path:
                env["schemaSearchPath"] = search_path
            details["sourceEnvironment"] = env

        payload = {
            "displayName": "bq-finops-optimizer-translation",
            "tasks": {
                "translation-task": {
                    "type": DEFAULT_TASK_TYPE,
                    "translationDetails": details
                }
            }
        }
        return self.http.call("POST", f"{self._base(DEFAULT_API_VERSION)}/workflows", payload)

    def get(self, name: str) -> HttpResult:
        return self.http.call("GET", f"{API_HOST}/{DEFAULT_API_VERSION}/{name}")

    def subtasks(self, name: str) -> HttpResult:
        return self.http.call("GET", f"{API_HOST}/{DEFAULT_API_VERSION}/{name}/subtasks")

    def delete(self, name: str) -> HttpResult:
        return self.http.call("DELETE", f"{API_HOST}/{DEFAULT_API_VERSION}/{name}")

    def wait(self, name: str, timeout: int = 300, interval: float = 2.0) -> dict:
        deadline = time.time() + timeout
        while time.time() < deadline:
            res = self.get(name)
            if res.status != 200:
                raise RuntimeError(f"Poll failed [{res.status}]: {res.text[:500]}")
            body = res.body or {}
            state = body.get("state")
            if state in ("COMPLETED", "PAUSED", "FAILED", "STATE_FAILED", "CANCELLED"):
                return body
            time.sleep(interval)
        raise TimeoutError(f"Workflow {name} timed out after {timeout}s")


# --------------------------------------------------------------------------
# Subtask & Issue Mining
# --------------------------------------------------------------------------

def walk_literals(node: Any, acc: dict[str, str] | None = None) -> dict[str, str]:
    acc = {} if acc is None else acc
    if isinstance(node, dict):
        path = node.get("relativePath") or node.get("relative_path")
        text = node.get("literalString") or node.get("literal_string")
        if path and isinstance(text, str) and path.endswith(".sql"):
            acc[path] = text
        for v in node.values():
            walk_literals(v, acc)
    elif isinstance(node, list):
        for v in node:
            walk_literals(v, acc)
    return acc

def extract_target_literals_from_subtasks(subtasks_body: Any) -> dict[str, str]:
    """Extracts target translated SQL literals strictly from subtask outputs."""
    literals = {}
    if not isinstance(subtasks_body, dict):
        return literals

    subtasks = subtasks_body.get("subtasks", [])
    for sub in subtasks:
        details = sub.get("metrics", []) + [sub.get("resource", {})]
        # Recursively search for targetReturnLiterals in subtask output
        def _mine(node):
            if isinstance(node, dict):
                path = node.get("relativePath") or node.get("relative_path")
                text = node.get("literalString") or node.get("literal_string")
                if path and isinstance(text, str) and path.endswith(".sql"):
                    literals[path] = text
                for v in node.values():
                    _mine(v)
            elif isinstance(node, list):
                for item in node:
                    _mine(item)

        _mine(sub)
    return literals


def extract_migration_issues(body: Any) -> list[MigrationIssue]:
    """Extracts structured diagnostic issues from subtasks and workflow reports."""
    issues: list[MigrationIssue] = []
    if not isinstance(body, dict):
        return issues

    def _search(node):
        if isinstance(node, dict):
            # Check for log message or issue dicts
            msg = node.get("message") or node.get("literalString") or node.get("details")
            code = str(node.get("code") or node.get("type") or "")
            cat = str(node.get("severity") or node.get("category") or "WARNING").upper()
            if msg and isinstance(msg, str) and len(msg.strip()) > 5:
                issues.append(MigrationIssue(category=cat, code=code if code else None, message=msg.strip()))
            for v in node.values():
                _search(v)
        elif isinstance(node, list):
            for item in node:
                _search(item)

    _search(body)
    return issues


def synthesize_optimizer_yaml(sql: str, issues: list[MigrationIssue]) -> str | None:
    """Analyzes SQL constructs and API diagnostic issues to synthesize a targeted
    optimizer.config.yaml specification for opting in to transformations.

    Covers all 7 public stable transformations per:
    https://docs.cloud.google.com/bigquery/docs/config-yaml-translation#optimize_and_improve_the_performance_of_translated_sql
    Uses both SQL pattern matching and Pass 1 diagnostic issue analysis."""
    transformations = []
    seen_names: set[str] = set()
    sql_upper = sql.upper()
    issue_text = " ".join(i.message for i in issues).upper() if issues else ""

    def _add(name: str, parameters: dict | None = None):
        key = f"{name}:{parameters}" if parameters else name
        if key not in seen_names:
            seen_names.add(key)
            entry: dict = {"name": name}
            if parameters:
                entry["parameters"] = parameters
            transformations.append(entry)

    # 1. CTE Rewriting → Temp Tables + cleanup
    if "WITH " in sql_upper and " AS (" in sql_upper:
        _add("REWRITE_CTE_TO_TEMP_TABLE", {"threshold": 1})
        _add("DROP_TEMP_TABLE")

    # 2. Scalar Subquery Precomputation
    #    PREDICATE scope: WHERE / JOIN ON subqueries → DECLARE variable
    has_predicate_subselect = (
        re.search(r"\bWHERE\b.*\(\s*SELECT\b", sql_upper, re.DOTALL)
        or re.search(r"\bJOIN\b.*\bON\b.*\(\s*SELECT\b", sql_upper, re.DOTALL)
    )
    #    PROJECTION scope: scalar subqueries in SELECT list
    has_projection_subselect = re.search(r"\bSELECT\b[^;]*\(\s*SELECT\b", sql_upper, re.DOTALL)

    if has_predicate_subselect:
        _add("PRECOMPUTE_INDEPENDENT_SUBSELECTS", {"scope": "PREDICATE"})
    if has_projection_subselect:
        _add("PRECOMPUTE_INDEPENDENT_SUBSELECTS", {"scope": "PROJECTION"})

    # 3. Zero-Scale Numeric → INT64 (cross-dialect: Snowflake NUMBER(38,0) → INT64)
    if re.search(r"\bNUMERIC\b|\bBIGNUMERIC\b|\bNUMBER\s*\(\s*\d+\s*,\s*0\s*\)", sql_upper) or "ZERO_SCALE" in issue_text or "NUMERIC" in issue_text:
        _add("REWRITE_ZERO_SCALE_NUMERIC_AS_INTEGER", {"bigint": 18})

    # 4. Subquery Set Comparison Distinctness
    if re.search(r"\bIN\s*\(\s*SELECT\b", sql_upper):
        _add("ADD_DISTINCT_TO_SUBQUERY_IN_SET_COMPARISON")

    # 5. Regex to Like Conversion
    if "REGEXP_CONTAINS" in sql_upper:
        _add("REGEXP_CONTAINS_TO_LIKE")

    # 6. Pass 1 diagnostic-driven: auto-opt-in to any public transformations the API recommends
    public_transformations = {
        "PRECOMPUTE_INDEPENDENT_SUBSELECTS", "REWRITE_CTE_TO_TEMP_TABLE",
        "REWRITE_ZERO_SCALE_NUMERIC_AS_INTEGER", "DROP_TEMP_TABLE",
        "REGEXP_CONTAINS_TO_LIKE", "ADD_DISTINCT_TO_SUBQUERY_IN_SET_COMPARISON",
        "APPROXIMATE_RANGE_PARTITIONS",
    }
    for name in public_transformations:
        if name in issue_text:
            _add(name)

    if not transformations:
        return None

    yaml_lines = ["type: optimizer", "transformations:"]
    for t in transformations:
        yaml_lines.append(f"  - name: {t['name']}")
        if "parameters" in t:
            yaml_lines.append("    parameters:")
            for pk, pv in t["parameters"].items():
                yaml_lines.append(f"      {pk}: {pv}")

    return "\n".join(yaml_lines) + "\n"


# --------------------------------------------------------------------------
# Single Workflow Execution with Try/Finally Resource Cleanup
# --------------------------------------------------------------------------

def _execute_workflow_pass(client: MigrationClient, files: list[tuple[str, str]],
                           params: TranslationParams) -> tuple[dict[str, str], list[MigrationIssue], bool, str | None]:
    """Executes a single Migration Workflow pass, ensuring try/finally workflow deletion."""
    create_res = client.create_workflow(files, params.default_database, params.schema_search_path)
    if create_res.status not in (200, 201):
        return {}, [], False, f"Workflow creation failed [{create_res.status}]: {create_res.text[:500]}"

    workflow_name = (create_res.body or {}).get("name")
    if not workflow_name:
        return {}, [], False, "Workflow created without resource name."

    try:
        final_payload = client.wait(workflow_name, timeout=params.timeout_seconds)
        state = final_payload.get("state")
        if state in ("FAILED", "STATE_FAILED", "CANCELLED"):
            return {}, [], False, f"Migration workflow terminated in state {state}"

        # Extract target literals from final_payload and subtasks
        target_literals = walk_literals(final_payload)
        issues = extract_migration_issues(final_payload)

        sub_res = client.subtasks(workflow_name)
        if sub_res.status == 200:
            sub_literals = extract_target_literals_from_subtasks(sub_res.body)
            target_literals.update(sub_literals)
            issues.extend(extract_migration_issues(sub_res.body))

        return target_literals, issues, True, None

    finally:
        # S1-5 Fix: Always delete workflow resource regardless of outcome/timeout
        try:
            client.delete(workflow_name)
        except Exception as e:
            logger.warning(f"Failed to delete workflow resource {workflow_name}: {e}")


# --------------------------------------------------------------------------
# Core Orchestration Function
# --------------------------------------------------------------------------

def run_migration_translation(params: TranslationParams, scoped_client: Any = None) -> TranslationResponse:
    try:
        auth = Auth()
        project = params.project_id or auth.adc_project
        if not project:
            return TranslationResponse(
                translated_sql=params.query, original_sql=params.query,
                success=False, error_message="No GCP Project ID resolved."
            )

        # Hard clamp timeout to prevent server thread pool exhaustion (S2-6)
        timeout_sec = min(max(params.timeout_seconds, 10), 300)
        params.timeout_seconds = timeout_sec

        query_sql = params.query.strip()
        if len(query_sql) > 1_000_000:
            return TranslationResponse(
                translated_sql=query_sql, original_sql=query_sql,
                success=False, error_message="Query size exceeds 1MB limit."
            )

        # Step 1: Auto DDL Resolution
        ddl_parts = []
        if params.auto_ddl:
            try:
                fetcher = SchemaFetcher(project)
                refs = fetcher.discover(query_sql)
                if refs:
                    ddl_parts = fetcher.fetch_ddl(refs)
            except Exception as e:
                logger.warning(f"Schema discovery failed: {e}")

        combined_parts = []
        if ddl_parts:
            combined_parts.append("\n\n".join(ddl_parts))
        combined_parts.append(SENTINEL)
        combined_parts.append(query_sql)
        combined_input = "\n\n".join(combined_parts) + "\n"
        logger.info(f"▶ [BQ Migration API] Starting translation & optimization workflow for project '{project}' (Location: '{params.location}')")

        http = Http(auth)
        client = MigrationClient(http, project, params.location)

        applied_yaml = params.config_yaml_content

        # Step 2: S1-1 Real 2-Pass Discovery Pipeline
        if not applied_yaml and params.auto_opt_in_yaml:
            logger.info("[BQ Migration API] Pass 1 (Discovery): Executing workflow without YAML to harvest diagnostics...")
            disc_files = [("input.sql", combined_input)]
            disc_literals, disc_issues, disc_ok, _ = _execute_workflow_pass(client, disc_files, params)
            if disc_ok:
                synthesized = synthesize_optimizer_yaml(query_sql, disc_issues)
                if synthesized:
                    applied_yaml = synthesized
                    logger.info(f"[BQ Migration API] Pass 1 Completed. Synthesized optimizer.config.yaml:\n{applied_yaml}")

        # Pass 2 / Execution Pass: Run with applied YAML (or direct)
        files = [("input.sql", combined_input)]
        if applied_yaml:
            cfg_name = params.config_filename
            if not cfg_name.endswith(".config.yaml"):
                cfg_name = cfg_name.split(".")[0] + ".config.yaml"
            files.append((cfg_name, applied_yaml))
            logger.info(f"[BQ Migration API] Pass 2 (Opt-in Execution): Submitting workflow WITH '{cfg_name}'...")

        literals, issues, ok, err_msg = _execute_workflow_pass(client, files, params)
        if not ok:
            logger.error(f"[BQ Migration API] Pass execution failed: {err_msg}")
            return TranslationResponse(
                translated_sql=query_sql, original_sql=query_sql,
                success=False, error_message=err_msg
            )

        raw_translated = literals.get("input.sql")
        if not raw_translated:
            logger.error("[BQ Migration API] Workflow completed but returned no target SQL literal.")
            return TranslationResponse(
                translated_sql=query_sql, original_sql=query_sql,
                success=False, error_message="Migration API returned no target SQL literal."
            )

        translated = strip_leading_ddl(raw_translated) if not params.keep_ddl_in_output else raw_translated

        # Step 3: Dry-Run Byte Delta Validation (S2-3 Fix)
        bytes_before, bytes_after, delta_pct = None, None, None
        if params.dry_run_compare:
            from google.cloud import bigquery
            bq_client = scoped_client or bigquery.Client(project=project)
            
            # Dry run original query
            cfg_orig = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
            try:
                j_before = bq_client.query(query_sql, job_config=cfg_orig)
                bytes_before = j_before.total_bytes_processed
            except Exception as e:
                logger.warning(f"Original query dry-run failed: {e}")

            # Dry run translated query
            cfg_trans = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
            try:
                j_after = bq_client.query(translated, job_config=cfg_trans)
                bytes_after = j_after.total_bytes_processed
            except Exception as e:
                # S2-3 Fix: If translated SQL fails dry-run, report failure loudly!
                logger.error(f"[BQ Migration API] Translated SQL failed dry-run: {e}")
                return TranslationResponse(
                    translated_sql=translated, original_sql=query_sql,
                    bytes_before=bytes_before,
                    issues=issues, applied_config_yaml=applied_yaml,
                    success=False,
                    error_message=f"Translated SQL failed BigQuery dry-run validation: {e}"
                )

            if bytes_before is not None and bytes_after is not None and bytes_before > 0:
                delta_pct = round((bytes_after - bytes_before) / bytes_before * 100.0, 2)

        logger.info(f"◼ [BQ Migration API] Workflow completed successfully! Byte Delta: {delta_pct}%")

        return TranslationResponse(
            translated_sql=translated,
            original_sql=query_sql,
            bytes_before=bytes_before,
            bytes_after=bytes_after,
            bytes_delta_pct=delta_pct,
            issues=issues,
            applied_config_yaml=applied_yaml,
            success=True
        )

    except Exception as e:
        return TranslationResponse(
            translated_sql=params.query, original_sql=params.query,
            success=False, error_message=str(e)
        )
