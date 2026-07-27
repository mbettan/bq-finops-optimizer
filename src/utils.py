import logging
import re
import time
import contextvars
from typing import Optional, List
from fastapi import HTTPException
from google.cloud import bigquery
from pydantic import BaseModel, ConfigDict

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Shared FinOps constants
# ---------------------------------------------------------------------------
# Standard calendar average: 365.25 / 12 = 30.4375.
# Used consistently across all financial projections (HBO, Fluid Scaling, etc.)
DAYS_PER_MONTH = 365.25 / 12  # 30.4375

# ---------------------------------------------------------------------------
# Request correlation ID
# ---------------------------------------------------------------------------
# Set once per request via middleware; automatically injected into every log
# line by RequestIdFilter.  Default "--------" for startup / non-request logs.
request_id_var: contextvars.ContextVar[str] = contextvars.ContextVar(
    "request_id", default="--------"
)


class RequestIdFilter(logging.Filter):
    """Inject the current request_id into every LogRecord so the formatter
    can include %(request_id)s without any per-call changes."""

    def filter(self, record: logging.LogRecord) -> bool:
        record.request_id = request_id_var.get()  # type: ignore[attr-defined]
        return True


def log_endpoint_start(endpoint_name: str, params, _logger=None) -> float:
    """Log a structured summary at the start of every endpoint call. Returns time.time() for elapsed calculation."""
    log = _logger or logger
    project = getattr(params, 'org_project_id', '?')
    region = getattr(params, 'region', '?')
    lookback = getattr(params, 'lookback_days', None)
    focus = getattr(params, 'focus_projects', None) or []
    cap_gb = getattr(params, 'max_bytes_billed_gb', None)
    cap_str = f"{cap_gb} GiB" if cap_gb else "200 GiB (default)"

    scope_str = f"{len(focus)} projects ({', '.join(focus[:3])}{'…' if len(focus) > 3 else ''})" if focus else "full organization"
    lookback_str = f" | lookback={lookback}d" if lookback else ""

    log.info(
        "▶ %s — project=%s | region=%s | scope=%s%s | safety_cap=%s",
        endpoint_name, project, region, scope_str, lookback_str, cap_str
    )
    return time.time()


def log_endpoint_end(endpoint_name: str, start_time: float, _logger=None):
    """Log endpoint completion with elapsed time."""
    log = _logger or logger
    elapsed = time.time() - start_time
    log.info("◼ %s — completed in %.1fs", endpoint_name, elapsed)

_IDENT_RE = re.compile(r"^[a-zA-Z0-9_\-\.\:]+\Z")
_ALIAS_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")

# Cap for UX sanity and validation cost — filtering actually *reduces* result size
# vs. org-wide, so SQL-length and result-set size aren't the concern.
MAX_FOCUS_PROJECTS = 50


class FocusMixin(BaseModel):
    """Mixin providing the optional focus_projects field.
    Inherit alongside existing param classes to add project-scoping support."""
    focus_projects: Optional[List[str]] = None


class OrgParams(BaseModel):
    """Base class for org-only endpoints that do NOT support focus_projects.
    extra='forbid' causes Pydantic to auto-reject any unknown fields (including
    focus_projects) with a 422, making the contract self-enforcing via the
    type system rather than imperative checks."""
    model_config = ConfigDict(extra="forbid")

    org_project_id: Optional[str] = None
    region: str = "region-us"
    max_bytes_billed_gb: Optional[int] = None


class AppliedScope(BaseModel):
    """Attached to analysis responses to make scope self-describing.
    Ensures exported/screenshotted numbers carry their scope context."""
    mode: str  # "focused" | "org"
    projects: Optional[List[str]] = None  # present when mode == "focused"
    total_org_projects: Optional[int] = None  # for Tier 1.5 context


def validate_focus_projects(projects: Optional[List[str]]) -> Optional[List[str]]:
    """Validate, sanitize, and deduplicate a list of focus project IDs.
    Returns None if the list is empty (= org-wide scan)."""
    if not projects:
        return None
    # Order-preserving dedup + trim
    seen = set()
    validated = []
    for p in projects:
        p = p.strip()
        if not p:
            continue
        if p in seen:
            continue
        seen.add(p)
        reject_dummy_project(p)
        _safe_ident(p, "focus_projects entry")
        validated.append(p)
    if not validated:
        return None
    if len(validated) > MAX_FOCUS_PROJECTS:
        raise HTTPException(
            status_code=400,
            detail=f"focus_projects supports at most {MAX_FOCUS_PROJECTS} projects, got {len(validated)}."
        )
    return validated


# Column allow-list — never interpolate caller-provided column names freely
_ALLOWED_FILTER_COLUMNS = {"project_id", "project_name", "catalog_name"}


def build_project_filter(
    focus_projects: Optional[List[str]],
    column: str = "project_id",
    table_alias: Optional[str] = None,
) -> tuple:
    """Returns (sql_clause, query_params).
    Value is parameterized via IN UNNEST(@focus_projects), not interpolated.
    Logs once when the filter is active (centralized observability —
    no per-endpoint logger.info needed).

    Args:
        focus_projects: List of project IDs, or None for org-wide.
        column: Column name (must be in allow-list).
        table_alias: Optional table alias for JOIN contexts (e.g., "j").
                     Validated against _ALIAS_RE.

    Examples:
      None             -> ("", [])
      ["a"]            -> ("AND project_id IN UNNEST(@focus_projects)", [...])
      ["a"], alias="j"  -> ("AND j.project_id IN UNNEST(@focus_projects)", [...])
    """
    if not focus_projects:
        return "", []
    if column not in _ALLOWED_FILTER_COLUMNS:
        raise ValueError(f"Unsupported filter column: {column}")
    qualified_col = column
    if table_alias:
        if not _ALIAS_RE.match(table_alias):
            raise ValueError(f"Invalid table alias: {table_alias!r}")
        qualified_col = f"{table_alias}.{column}"
    logger.info("Focus filter active: %d projects", len(focus_projects))
    param = bigquery.ArrayQueryParameter("focus_projects", "STRING", focus_projects)
    return f"AND {qualified_col} IN UNNEST(@focus_projects)", [param]

def _safe_ident(value: str, name: str) -> str:
    """Validates that a string is a safe GCP identifier (project, dataset, table, etc.)."""
    if not value or not _IDENT_RE.match(value):
        raise HTTPException(status_code=400, detail=f"Invalid {name}: {value!r}")
    return value

def _normalize_region(region: str) -> str:
    """Standardizes BigQuery metadata region formats."""
    region = (region or "").strip()
    if not region:
        return "region-us"
    return region if region.startswith("region-") else f"region-{region}"

def reject_dummy_project(project_id: str):
    """
    Rejects dummy GCP project IDs to prevent querying sandbox placeholders.
    Note: 'mbettan-sandbox' is a placeholder from imported dummy snapshots that 
    can reside in the user's browser localStorage. 'your-project-id' is the 
    default code fallback.
    """
    if not project_id:
        return
    cleaned = project_id.strip()
    if cleaned in ("your-project-id", "mbettan-sandbox"):
        raise HTTPException(
            status_code=400,
            detail=f"The project ID '{project_id}' is a dummy placeholder. Please set a valid GCP Project ID."
        )

def init_bq_client_and_resolve_project(params) -> tuple[bigquery.Client, str]:
    """
    Initializes the BigQuery client and resolves the target project ID.
    If org_project_id is empty or None, it defaults to client.project.
    Validates that the resolved project ID is not empty and matches safety regex.
    """
    org_project_id = getattr(params, "org_project_id", None)
    project_override = org_project_id.strip() if (org_project_id and org_project_id.strip()) else None
    
    # Check parameter for dummy project
    if project_override:
        reject_dummy_project(project_override)
        
    try:
        client = bigquery.Client(project=project_override) if project_override else bigquery.Client()
    except Exception as e:
        logger.error(f"Failed to initialize BigQuery client: {e}")
        raise HTTPException(
            status_code=400,
            detail=f"Failed to initialize BigQuery client: {e}"
        )
        
    resolved_project = project_override if project_override else client.project
    
    if not resolved_project or not resolved_project.strip():
        raise HTTPException(
            status_code=400,
            detail="GCP Project ID must be specified (either configured in Settings, or resolved via environment credentials)."
        )
        
    resolved_project = resolved_project.strip()
    
    # If fallback to ADC project occurred, log loudly to prevent accidental queries on wrong organization
    if not project_override:
        logger.warning(
            "No explicit project set — falling back to ADC default '%s'. "
            "Org-wide queries will scope to THIS project's organization.", resolved_project
        )
        
    reject_dummy_project(resolved_project)
        
    _safe_ident(resolved_project, "project_id")
        
    return client, resolved_project

def handle_endpoint_exception(e: Exception, service_name: str):
    """
    Centralized exception handler for API endpoints.
    Catches GCP-specific exceptions to return structured client-safe error messages
    with appropriate status codes, and raises generic 500s for unexpected errors
    to prevent PII/schema leakage.
    """
    if isinstance(e, HTTPException):
        raise e
        
    from google.api_core import exceptions as gax_exc
    
    if isinstance(e, gax_exc.Forbidden):
        logger.error(f"{service_name} access denied: {e}")
        raise HTTPException(
            status_code=403,
            detail="Access Denied: The service account or user lacks required BigQuery permissions."
        )
    elif isinstance(e, gax_exc.NotFound):
        logger.error(f"{service_name} resource not found: {e}")
        raise HTTPException(
            status_code=404,
            detail="Resource Not Found: Requested project, region, or table was not found."
        )
    elif isinstance(e, gax_exc.BadRequest):
        # Log the real error server-side only — BigQuery BadRequest messages
        # often echo back SQL/schema fragments, which would otherwise leak
        # to the client (and, combined with any injection bug, act as a
        # feedback channel for refining an attack). The request_id (already
        # in every log line via RequestIdFilter) lets support correlate a
        # client report back to the full server-side detail.
        req_id = request_id_var.get()
        logger.error(f"{service_name} bad request [{req_id}]: {e}")
        err_str = str(e)
        # F6: surface a specific message when the bytes-billed safety cap
        # is exceeded so users know to raise max_bytes_billed_gb.
        if "bytesBilledLimitExceeded" in err_str or "exceeds the maximum" in err_str.lower():
            raise HTTPException(
                400,
                f"Query exceeded the bytes-billed safety cap. Raise max_bytes_billed_gb "
                f"or narrow scope with focus_projects / shorter lookback. "
                f"Reference: {req_id} — check server logs for details."
            )
        raise HTTPException(
            400,
            f"BigQuery rejected the request (invalid parameters or malformed query). "
            f"Reference: {req_id} — check server logs for details."
        )
    elif isinstance(e, gax_exc.GoogleAPIError):
        req_id = request_id_var.get()
        logger.error(f"{service_name} BigQuery error [{req_id}]: {e}")
        err_str = str(e)
        detail_msg = f"BigQuery Query Failed (Reference: {req_id}). Check server logs for details."
        if "internal error" in err_str.lower() or "33494873" in err_str:
            detail_msg = (
                f"BigQuery internal engine error occurred after retry attempts (Ref: {req_id}). "
                "This usually indicates missing GCP Organization-level Recommender permissions or a transient BigQuery issue."
            )
        # bytesBilledLimitExceeded often arrives as a 500, not a 400.
        # Detect it here so the user gets an actionable message.
        if "bytesBilledLimitExceeded" in err_str or "exceeds the maximum" in err_str.lower():
            raise HTTPException(
                400,
                f"Query exceeded the bytes-billed safety cap. Raise max_bytes_billed_gb "
                f"or narrow scope with focus_projects / shorter lookback. "
                f"Reference: {req_id} — check server logs for details."
            )
        raise HTTPException(
            status_code=400,
            detail=detail_msg
        )
    else:
        req_id = request_id_var.get()
        logger.exception(f"Unexpected error in {service_name} [{req_id}]")
        raise HTTPException(
            status_code=500,
            detail=f"{service_name} failed; check server logs (Ref: {req_id})."
        )


def run_query_with_retry_limit(
    client: bigquery.Client,
    sql: str,
    job_config: bigquery.QueryJobConfig,
    description: str = "Query",
    max_attempts: int = 5,
    initial_delay: float = 1.0,
    max_delay: float = 10.0,
    fetch_df: bool = False
):
    """
    Executes a BigQuery query with a strict limit of max_attempts (default 5).
    Disables the SDK's default 10-minute retry policy (retry=None), retrying up to
    max_attempts with exponential backoff and explicit per-attempt logging.
    """
    from google.api_core import exceptions as gax_exc

    attempt = 0
    delay = initial_delay

    while True:
        attempt += 1
        try:
            query_job = client.query(sql, job_config=job_config, retry=None, job_retry=None)
            results = query_job.result(retry=None, job_retry=None)
            if fetch_df:
                res_data = results.to_dataframe(create_bqstorage_client=True)
            else:
                res_data = results
            return query_job, res_data
        except Exception as e:
            err_msg = str(e)
            # Permanent errors: no amount of retrying will fix a missing
            # IAM permission, a non-existent resource, or a malformed query.
            if isinstance(e, (gax_exc.Forbidden, gax_exc.NotFound, gax_exc.BadRequest)):
                logger.error(
                    "❌ %s failed with permanent error (no retry): %s",
                    description, err_msg.splitlines()[0]
                )
                raise
            # bytesBilledLimitExceeded comes back as a 500 InternalServerError,
            # not a 400 BadRequest, so isinstance checks miss it. Detect by
            # message — the query will never fit under the cap on retry.
            if "bytesBilledLimitExceeded" in err_msg:
                logger.error(
                    "❌ %s exceeded bytes-billed cap (no retry): %s",
                    description, err_msg.splitlines()[0]
                )
                raise
            if attempt >= max_attempts:
                logger.error(
                    "❌ %s failed permanently after %d/%d attempts. Final error: %s",
                    description, attempt, max_attempts, err_msg
                )
                raise
            
            logger.warning(
                "⚠️ %s attempt %d/%d failed: %s. Retrying in %.1fs...",
                description, attempt, max_attempts, err_msg.splitlines()[0], delay
            )
            time.sleep(delay)
            delay = min(delay * 2.0, max_delay)


# Default safety cap for maximum_bytes_billed (200 GiB).
DEFAULT_MAX_BYTES_BILLED = 200 * 1024**3

def get_max_bytes_billed(params=None) -> int:
    """
    Resolve the maximum_bytes_billed value from an API params object.

    Reads the optional ``max_bytes_billed_gb`` attribute (in GiB) and converts
    it to bytes.  Falls back to :data:`DEFAULT_MAX_BYTES_BILLED` (200 GiB) when
    the attribute is missing, ``None``, or ``0``.

    The value is clamped to the range [1 GiB, 10 TiB] to prevent accidental
    misconfiguration.
    """
    gb = getattr(params, "max_bytes_billed_gb", None) if params else None
    if not gb:
        return DEFAULT_MAX_BYTES_BILLED
    gb = int(gb)
    # Clamp: minimum 1 GiB, maximum 10 TiB (10240 GiB)
    gb = max(1, min(gb, 10240))
    return gb * 1024**3


def run_query_and_log(client: bigquery.Client, sql: str, label: str = "Query",
                      params=None, query_parameters=None, fetch_df: bool = False):
    """Run a BigQuery query with timing, BQ Console URL, and structured logging.

    This is the single, canonical query-execution path used by all modules.

    Args:
        client: An authenticated BigQuery client (project-scoped).
        sql: The SQL string to execute.
        label: A human-readable description for log messages.
        params: An API params object carrying ``max_bytes_billed_gb``.
        query_parameters: Optional list of ``bigquery.ScalarQueryParameter``.
        fetch_df: If True, return a DataFrame via BQ Storage API instead of a
            row iterator.

    Returns:
        A BigQuery row iterator (default) or a pandas DataFrame when
        ``fetch_df=True``.
    """
    max_bytes = get_max_bytes_billed(params)
    job_config = bigquery.QueryJobConfig(
        maximum_bytes_billed=max_bytes,
        query_parameters=query_parameters or []
    )
    logger.debug("%s SQL:\n%s", label, sql)
    logger.info("⏳ %s — submitting query (safety cap: %s GiB)…", label, max_bytes // (1024**3))
    t0 = time.time()
    query_job, results = run_query_with_retry_limit(
        client, sql, job_config, description=label, max_attempts=5, fetch_df=fetch_df
    )
    elapsed = time.time() - t0
    proc = query_job.total_bytes_processed
    billed = query_job.total_bytes_billed
    proc_gib = f"{proc / (1024**3):.2f} GiB" if proc is not None else "N/A"
    bill_gib = f"{billed / (1024**3):.2f} GiB" if billed is not None else "N/A"
    loc = query_job.location or "us"
    bq_url = (
        f"https://console.cloud.google.com/bigquery?project={query_job.project}"
        f"&j=bq:{loc}:{query_job.job_id}&page=queryresults"
    )
    logger.info(
        "✅ %s — %.1fs | Job: %s | Processed: %s | Billed: %s | Cache: %s | %s",
        label, elapsed, query_job.job_id, proc_gib, bill_gib, query_job.cache_hit, bq_url
    )
    return results
