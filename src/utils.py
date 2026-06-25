import logging
import re
from typing import Optional
from fastapi import HTTPException
from google.cloud import bigquery

logger = logging.getLogger(__name__)

_IDENT_RE = re.compile(r"^[a-zA-Z0-9_\-\.\:]+\Z")

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
        logger.error(f"{service_name} bad request: {e}")
        # Surface the real BigQuery error (truncated) to the client
        raise HTTPException(400, f"BigQuery Query Failed: {str(e)[:500]}")
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
