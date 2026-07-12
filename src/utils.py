import logging
import re
import time
import contextvars
from typing import Optional, List
from fastapi import HTTPException
from google.cloud import bigquery
from pydantic import BaseModel

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
        raise HTTPException(
            400,
            f"BigQuery rejected the request (invalid parameters or malformed query). "
            f"Reference: {req_id} — check server logs for details."
        )
    elif isinstance(e, gax_exc.GoogleAPIError):
        logger.error(f"{service_name} BigQuery error: {e}")
        raise HTTPException(
            status_code=400,
            detail="BigQuery Query Failed: BigQuery service returned an error; check server logs."
        )
    else:
        logger.exception(f"Unexpected error in {service_name}")
        raise HTTPException(
            status_code=500,
            detail=f"{service_name} failed; check server logs."
        )

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
