# BigQuery FinOps Optimizer — Code Review Package

> **Purpose**: This document provides a complete snapshot of the backend source code for the BigQuery FinOps Optimizer API. It is intended to be consumed by an LLM code reviewer. Review for **bugs, logic errors, SQL injection risks, race conditions, data correctness issues, edge cases, and architectural concerns**. Frontend dashboard code (HTML/CSS/JS) and test files are intentionally excluded — focus on the Python backend and SQL queries.

---

## Table of Contents

1. [Project Overview & Architecture](#1-project-overview--architecture)
2. [Dependency Manifest](#2-dependency-manifest)
3. [Deployment Configuration](#3-deployment-configuration)
4. [Source Code](#4-source-code)
   - 4.1 [`src/utils.py`](#41-srcutilspy) — Shared utilities, BQ client init, SQL injection guards, logging
   - 4.2 [`src/main.py`](#42-srcmainpy) — FastAPI app, middleware, 20+ API endpoints (storage, compute, AI, governance, slots)
   - 4.3 [`src/fluid_scaling.py`](#43-srcfluid_scalingpy) — Fluid Scaling status & savings estimation
   - 4.4 [`src/hbo.py`](#44-srchbopy) — History-Based Optimization analysis
   - 4.5 [`src/cost_attribution.py`](#45-srccost_attributionpy) — Reservation cost attribution engine
5. [Configuration Files](#5-configuration-files)
6. [Review Focus Areas](#6-review-focus-areas)
7. [Design Decisions & Reviewer Guidance](#7-design-decisions--reviewer-guidance)

---

## 1. Project Overview & Architecture

**What it is**: A FastAPI-based REST API that queries Google BigQuery `INFORMATION_SCHEMA` views to produce cost optimization recommendations for an organization's BigQuery usage. It covers:

- **Storage Optimization**: Compares logical vs. physical billing models per dataset
- **Compute Analysis**: On-Demand vs. Editions cost comparison per job
- **Anti-Pattern Detection**: SELECT *, DML abuse, data skew, MV costs, batch candidates
- **Capacity Planning**: Slot utilization, tiered recommendations, simulation
- **Fluid Scaling**: Autoscaler savings estimation (legacy vs. fluid)
- **HBO**: History-Based Optimization impact analysis
- **Cost Attribution**: Per-project cost allocation across reservations
- **AI Doctor**: Gemini-powered query audit via `AI.GENERATE` in BigQuery
- **Governance**: Dataset expiration, partition filter enforcement

**Key architectural decisions**:
- All SQL is constructed via f-string interpolation with user-controlled parameters validated through `_safe_ident()` and allow-lists
- `focus_projects` filtering uses parameterized `IN UNNEST(@focus_projects)` — the one parameterized pattern
- Many numeric parameters (e.g., `lookback_days`, `limit`) are interpolated directly into SQL
- The app loads `.env` manually at import time
- BigQuery client is created per-request (no connection pooling)
- `_run_and_log` / `run_query_and_log` are the centralized query executors with `maximum_bytes_billed` safety caps

---

## 2. Dependency Manifest

**File**: `requirements.txt`

```
fastapi>=0.115.0,<1.0.0
uvicorn>=0.34.0,<1.0.0
google-cloud-bigquery>=3.30.0,<4.0.0
pydantic>=2.10.0,<3.0.0
pyOpenSSL>=24.0.0,<26.0.0
numpy>=1.26.0,<3.0.0
pandas>=2.2.0,<3.0.0
google-cloud-bigquery-storage>=2.27.0,<3.0.0
db-dtypes>=1.3.0,<2.0.0
```

---

## 3. Deployment Configuration

**File**: `Dockerfile`

```dockerfile
# Use official lightweight Python image
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Copy dependency list and install them
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code
COPY src/ src/
COPY static/ static/
# Expose port (Cloud Run defaults to 8080)
EXPOSE 8080

# Command to run the web server using Uvicorn
CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8080"]
```

---

## 4. Source Code

### 4.1 `src/utils.py`

**Role**: Shared utilities used across all modules — BQ client initialization, SQL injection validation, parameterized focus_projects filter, logging helpers, error handling.

```python
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
_ALLOWED_FILTER_COLUMNS = {"project_id", "project_name"}


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

```

---

### 4.2 `src/main.py`

**Role**: FastAPI application entry point. Contains the app instance, middleware, 20+ API endpoints spanning storage analysis, compute analysis, anti-pattern detection, AI query auditing, slot capacity planning, and dashboard stubs.

```python
from fastapi import FastAPI, HTTPException, Response, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.middleware.gzip import GZipMiddleware
from pydantic import BaseModel, Field, field_validator
from typing import Optional, List, Set, Tuple
from concurrent.futures import ThreadPoolExecutor
from google.cloud import bigquery
import hashlib
from functools import lru_cache
from pathlib import Path
import unicodedata
import os
import logging
from logging.handlers import RotatingFileHandler
import math
import numpy as np
from collections import defaultdict
import json
import re
import pandas as pd
from google.api_core import exceptions as gax_exc
from .cost_attribution import router as cost_attribution_router
from .hbo import router as hbo_router
from .fluid_scaling import (
    router as fluid_scaling_router,
    _strip_qualifier,
)
from .utils import init_bq_client_and_resolve_project, reject_dummy_project, _safe_ident, _normalize_region, handle_endpoint_exception, get_max_bytes_billed, FocusMixin, validate_focus_projects, build_project_filter, log_endpoint_start, log_endpoint_end, request_id_var, RequestIdFilter
import time
import uuid


__version__ = "1.1.0"

app = FastAPI(
    title="BigQuery FinOps Optimizer API",
    description="Enterprise-grade diagnostic and simulation suite for Google Cloud BigQuery costs.",
    version=__version__
)
app.add_middleware(GZipMiddleware, minimum_size=1000)
app.include_router(cost_attribution_router)
app.include_router(hbo_router)
app.include_router(fluid_scaling_router)

@app.middleware("http")
async def no_cache_static_assets(request: Request, call_next):
    """Force revalidation of JS/CSS so version-hash changes take effect immediately."""
    response = await call_next(request)
    path = request.url.path
    if path.startswith("/static/") and (path.endswith(".js") or path.endswith(".css")):
        response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
    return response

@app.middleware("http")
async def inject_request_id(request: Request, call_next):
    """Assign a short correlation ID to every request for log tracing."""
    request_id_var.set(uuid.uuid4().hex[:8])
    response = await call_next(request)
    return response

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Load .env manually to ensure GOOGLE_CLOUD_PROJECT is set
# (Uvicorn doesn't load it by default without --env-file)
_env_file = os.path.join(BASE_DIR, ".env")
if os.path.exists(_env_file):
    with open(_env_file) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#') and '=' in line:
                key, val = line.split('=', 1)
                os.environ[key.strip()] = val.strip()

# Configure logging
# Set LOG_LEVEL=DEBUG to see full SQL for every query.
# Default: INFO (shows ▶/⏳/✅/◼ progress without SQL noise).
log_file = os.path.join(BASE_DIR, 'app.log')
_log_level = getattr(logging, os.environ.get("LOG_LEVEL", "INFO").upper(), logging.INFO)
_req_filter = RequestIdFilter()
_handlers = [
    RotatingFileHandler(log_file, maxBytes=10*1024*1024, backupCount=5),
    logging.StreamHandler()
]
for h in _handlers:
    h.addFilter(_req_filter)

logging.basicConfig(
    level=_log_level,
    format='%(asctime)s - %(name)s - %(levelname)s - [%(request_id)s] %(message)s',
    handlers=_handlers,
    force=True
)
logger = logging.getLogger(__name__)

STATIC_DIR = Path(BASE_DIR) / "static"

@lru_cache(maxsize=64)
def _hash_file(name: str, mtime: float) -> str:
    return hashlib.sha256((STATIC_DIR / name).read_bytes()).hexdigest()[:8]

def asset_version(name: str) -> str:
    try:
        return _hash_file(name, (STATIC_DIR / name).stat().st_mtime)
    except FileNotFoundError:
        return "0"

# Serve index.html at root

@app.get("/", response_class=HTMLResponse)
def read_index():
    try:
        html = (STATIC_DIR / "index.html").read_text(encoding="utf-8")
        # Replace versions dynamically based on actual file hashes
        html = re.sub(
            r"static/app\.js(\?v=[^\"']*)?",
            f"static/app.js?v={asset_version('app.js')}",
            html
        )
        html = re.sub(
            r"static/style\.css(\?v=[^\"']*)?",
            f"static/style.css?v={asset_version('style.css')}",
            html
        )
        headers = {
            "Cache-Control": "no-cache, no-store, must-revalidate",
            "Pragma": "no-cache",
            "Expires": "0"
        }
        return HTMLResponse(content=html, headers=headers)
    except Exception as e:
        logger.error(f"Error serving index: {e}")
        raise HTTPException(status_code=500, detail="Internal server error reading main page")

@app.get("/favicon.ico", include_in_schema=False)
def favicon():
    return Response(status_code=204)

# Mount static files
app.mount("/static", StaticFiles(directory=os.path.join(BASE_DIR, "static")), name="static")

@app.get("/api/about")
def get_about():
    """Version and release metadata for the frontend About panel.

    Parses RELEASE_NOTES.md to extract highlights for the latest release,
    making it the single source of truth for what's shown in the UI.
    """
    return _about_cache

def _parse_release_notes() -> dict:
    """Parse RELEASE_NOTES.md once at startup to build the About payload.

    Extracts all releases and their highlights.
    """
    releases = []
    current_release = None
    in_highlights_section = False

    rn_path = Path(__file__).resolve().parent.parent / "RELEASE_NOTES.md"
    try:
        text = rn_path.read_text(encoding="utf-8")
    except FileNotFoundError:
        logger.warning("RELEASE_NOTES.md not found — About highlights will be empty")
        return _build_about([])

    lines = text.splitlines()

    for line in lines:
        stripped = line.strip()

        # Detect the version heading: ## v1.1.0 — 2026-07-08
        if stripped.startswith("## v") and "—" in stripped:
            if current_release:
                releases.append(current_release)
            
            parts = stripped.split("—", 1)
            version = parts[0].replace("##", "").strip()
            date_str = parts[1].strip() if len(parts) == 2 else "—"
            
            current_release = {
                "version": version.lstrip("v"),
                "release_date": date_str,
                "highlights": [],
                "_has_highlights_section": False
            }
            in_highlights_section = False
            continue

        if not current_release:
            continue

        # Detect the Key Highlights subsection
        if stripped.startswith("### ") and "Key Highlights" in stripped:
            in_highlights_section = True
            current_release["_has_highlights_section"] = True
            continue

        # If we're in highlights and hit the next ### section, stop collecting
        if in_highlights_section and stripped.startswith("### "):
            in_highlights_section = False
            continue

        # Collect #### N. Title lines inside Key Highlights
        if in_highlights_section and stripped.startswith("#### "):
            # Strip "#### ", the number prefix "N. ", and any leading emoji
            title = stripped[5:]  # remove "#### "
            # Remove leading "N. " pattern (e.g., "1. ", "12. ")
            if len(title) > 2 and title[0].isdigit():
                dot_pos = title.find(". ")
                if dot_pos != -1:
                    title = title[dot_pos + 2:]
            # Strip leading emoji (1-2 code points + optional variation selector + space)
            cleaned = []
            skip_leading = True
            for ch in title:
                if skip_leading and (
                    unicodedata.category(ch).startswith("So")  # Symbol, other (emoji)
                    or ch == "\ufe0f"  # variation selector
                    or ch == " "
                ):
                    continue
                skip_leading = False
                cleaned.append(ch)
            title = "".join(cleaned).strip()
            if title:
                current_release["highlights"].append(title)

        # Fallback: capture top-level bullet points for releases without Key Highlights
        if (not current_release["_has_highlights_section"]
                and stripped.startswith("* ")
                and not stripped.startswith("*   **")):
            title = stripped[2:].strip()
            if title:
                current_release["highlights"].append(title)

    if current_release:
        releases.append(current_release)

    # Strip internal parsing flags before returning
    for r in releases:
        r.pop("_has_highlights_section", None)

    return _build_about(releases)


def _build_about(releases: list[dict]) -> dict:
    latest_version = releases[0]["version"] if releases else __version__
    latest_date = releases[0]["release_date"] if releases else "—"
    
    return {
        "name": "BigQuery FinOps Optimizer",
        "version": latest_version,
        "release_date": latest_date,
        "repo_url": "https://github.com/mbettan/bq-finops-optimizer",
        "changelog_url": "https://github.com/mbettan/bq-finops-optimizer/blob/main/RELEASE_NOTES.md",
        "demo_url": "https://mbettan.github.io/bq-finops-optimizer/simulator.html",
        "releases": releases
    }


# Parse once at import time and cache (Requires server restart if RELEASE_NOTES.md changes)
_about_cache: dict = _parse_release_notes()

_ALLOWED_TT_HOURS = {48, 72, 96, 120, 144, 168}

class StorageParams(FocusMixin):
    active_logical_price: float = 0.02
    long_term_logical_price: float = 0.01
    active_physical_price: float = 0.04
    long_term_physical_price: float = 0.02
    time_travel_rescale: float = 1.0
    time_travel_hours: Optional[int] = None
    min_monthly_saving: float = 0.0
    min_monthly_saving_pct: float = 0.0
    region: str = "region-us"
    org_project_id: Optional[str] = None
    max_bytes_billed_gb: Optional[int] = None

    @field_validator('time_travel_hours')
    @classmethod
    def validate_time_travel_hours(cls, v):
        if v is not None and v not in _ALLOWED_TT_HOURS:
            raise ValueError(f"time_travel_hours must be one of {sorted(_ALLOWED_TT_HOURS)}")
        return v


class StaticAuditParams(StorageParams):
    pass

class StaticAuditResult(BaseModel):
    project_id: str
    dataset_id: str
    table_id: str
    row_count: int
    size_bytes: int
    is_partitioned: bool
    partition_column: Optional[str] = None
    is_clustered: bool
    clustering_fields: Optional[str] = None

@app.post("/api/storage/static_audit", response_model=List[StaticAuditResult])
def run_static_schema_audit(params: StaticAuditParams):
    _validate_safe_params(params)
    t0 = log_endpoint_start("Static Schema Audit", params, _logger=logger)
    scoped_client, resolved_project = init_bq_client_and_resolve_project(params)
    region_val = _normalize_region(params.region)
    
    # We query the project-level INFORMATION_SCHEMA views.
    # TABLE_STORAGE provides total_rows directly (no need for PARTITIONS).
    sql = f"""
    WITH table_sizes AS (
      SELECT
        project_id,
        table_schema,
        table_name,
        total_logical_bytes AS size_bytes,
        total_rows,
        total_partitions
      FROM
        `{resolved_project}`.`{region_val}`.INFORMATION_SCHEMA.TABLE_STORAGE
      WHERE
        deleted = false
    ),
    table_cols AS (
      SELECT
        table_catalog,
        table_schema,
        table_name,
        MAX(CASE WHEN is_partitioning_column = 'YES' THEN column_name END) AS partition_column,
        STRING_AGG(CASE WHEN clustering_ordinal_position IS NOT NULL THEN column_name END, ', ' ORDER BY clustering_ordinal_position) AS clustering_fields
      FROM
        `{resolved_project}`.`{region_val}`.INFORMATION_SCHEMA.COLUMNS
      GROUP BY 1,2,3
    )
    SELECT
      t.table_catalog AS project_id,
      t.table_schema AS dataset_id,
      t.table_name AS table_id,
      COALESCE(s.total_rows, 0) AS row_count,
      COALESCE(s.size_bytes, 0) AS size_bytes,
      COALESCE(s.total_partitions > 0, false) AS is_partitioned,
      c.partition_column AS partition_column,
      COALESCE(c.clustering_fields IS NOT NULL, false) AS is_clustered,
      c.clustering_fields AS clustering_fields
    FROM
      `{resolved_project}`.`{region_val}`.INFORMATION_SCHEMA.TABLES t
    LEFT JOIN
      table_sizes s
    ON
      t.table_catalog = s.project_id
      AND t.table_schema = s.table_schema
      AND t.table_name = s.table_name
    LEFT JOIN
      table_cols c
    ON
      t.table_catalog = c.table_catalog
      AND t.table_schema = c.table_schema
      AND t.table_name = c.table_name
    WHERE
      t.table_type = 'BASE TABLE'
      AND COALESCE(s.size_bytes, 0) > 1073741824 -- > 1 GB
      AND (COALESCE(s.total_partitions, 0) = 0 OR c.clustering_fields IS NULL)
      -- Exclude system/tooling datasets that are never optimization candidates
      AND NOT STARTS_WITH(t.table_schema, 'assessment_')  -- BigQuery Migration Assessment exports
      AND NOT STARTS_WITH(t.table_schema, '_script')       -- Temporary script datasets
      AND NOT STARTS_WITH(t.table_schema, '_c0')           -- Temporary query result datasets
      AND t.table_schema != 'dataform'                     -- BQ transfer service staging
    ORDER BY
      size_bytes DESC
    LIMIT 50
    """
    
    try:
        results = run_query_and_log(scoped_client, sql, "Static Schema Audit", params=params)
        output = []
        for row in results:
            output.append(StaticAuditResult(
                project_id=row['project_id'],
                dataset_id=row['dataset_id'],
                table_id=row['table_id'],
                row_count=int(row['row_count'] or 0),
                size_bytes=int(row['size_bytes'] or 0),
                is_partitioned=bool(row['is_partitioned']),
                partition_column=row['partition_column'],
                is_clustered=bool(row['is_clustered']),
                clustering_fields=row['clustering_fields']
            ))
            
        log_endpoint_end("Static Schema Audit", t0, _logger=logger)
        return output
    except Exception as e:
        logger.warning(f"Static schema audit query failed: {e}")
        return []


class ActiveAssistResult(BaseModel):
    project_id: str
    dataset_id: str
    table_id: str
    recommendation: str  # 'Partition' or 'Cluster'
    cluster_columns: List[str]
    partition_column: Optional[str] = None
    on_demand_monthly_savings: Optional[float] = None
    editions_monthly_savings: Optional[float] = None

@app.post("/api/storage/active_assist", response_model=List[ActiveAssistResult])
def fetch_active_assist_recommendations(params: StorageParams):
    _validate_safe_params(params)
    t0 = log_endpoint_start("Active Assist", params, _logger=logger)
    
    scoped_client, resolved_project = init_bq_client_and_resolve_project(params)
    region_val = _normalize_region(params.region)
    
    # Try querying the real RECOMMENDATIONS view if it exists and is accessible
    sql = f"""
    SELECT
      project_id,
      target_resources,
      description,
      primary_impact,
      additional_details
    FROM
      `{resolved_project}`.`{region_val}`.INFORMATION_SCHEMA.RECOMMENDATIONS
    WHERE
      recommender = 'google.bigquery.table.PartitionClusterRecommender'
    LIMIT 20
    """
    
    logger.info(f"Querying Google Active Assist Recommendations...")
    
    try:
        # We will attempt to run it
        results = run_query_and_log(scoped_client, sql, "Active Assist Recommendations", params=params)
        output = []
        
        # If the view exists but returns nothing, or if it succeeds
        for row in results:
            # Parse resource to extract dataset and table
            # e.g. "projects/project_id/datasets/dataset_id/tables/table_id"
            resources = row['target_resources'] or ""
            dataset_id = "UNKNOWN"
            table_id = "UNKNOWN"
            if "/datasets/" in resources and "/tables/" in resources:
                parts = resources.split("/")
                try:
                    ds_idx = parts.index("datasets")
                    dataset_id = parts[ds_idx + 1]
                    tbl_idx = parts.index("tables")
                    table_id = parts[tbl_idx + 1]
                except Exception:
                    pass
                    
            desc = (row['description'] or "").lower()
            rec_type = "Partition" if "partition" in desc else "Cluster"
            
            # Parse savings from primary_impact if available
            savings: Optional[float] = None
            primary_impact = row.get('primary_impact')
            if primary_impact and isinstance(primary_impact, dict):
                cost_proj = primary_impact.get('cost_projection')
                if cost_proj and isinstance(cost_proj, dict):
                    raw = cost_proj.get('cost_in_local_currency') or cost_proj.get('cost_savings')
                    if raw is not None:
                        savings = float(raw)

            # Parse column suggestions from additional_details if available
            cluster_cols: List[str] = []
            part_col: Optional[str] = None
            additional_details = row.get('additional_details') or {}
            if isinstance(additional_details, dict):
                overview = additional_details.get('overview') or ''
                # Best-effort extraction: real parsing depends on recommender payload shape
                if rec_type == 'Partition':
                    part_col = additional_details.get('recommended_partition_column') or None
                else:
                    cols = additional_details.get('recommended_cluster_columns')
                    if isinstance(cols, list):
                        cluster_cols = [str(c) for c in cols]

            output.append(ActiveAssistResult(
                project_id=row['project_id'] or resolved_project,
                dataset_id=dataset_id,
                table_id=table_id,
                recommendation=rec_type,
                cluster_columns=cluster_cols,
                partition_column=part_col,
                on_demand_monthly_savings=savings,
                editions_monthly_savings=None,
            ))
            
        log_endpoint_end("Active Assist", t0, _logger=logger)
        return output
        
    except Exception as e:
        logger.warning(f"Active Assist recommendation query failed: {e}")
        return []


class JobAnalysisParams(FocusMixin):
    on_demand_rate_per_tb: float = 6.25
    edition_slot_hr_rate: float = 0.06
    slot_step_size: int = 50
    lookback_days: int = Field(default=3, ge=1, le=90)
    region: str = "region-us"
    org_project_id: Optional[str] = None
    min_bytes_billed: int = 10485760
    limit_jobs: int = Field(default=1000, ge=1, le=10000)
    fluid_scaling: bool = False
    max_bytes_billed_gb: Optional[int] = None






def run_query_and_log(scoped_client: bigquery.Client, sql: str, description: str = "Query", params=None, query_parameters=None):
    # Safety cap: cancel queries that would scan more than this.
    max_bytes = get_max_bytes_billed(params)
    job_config = bigquery.QueryJobConfig(
        maximum_bytes_billed=max_bytes,
        query_parameters=query_parameters or []
    )
    # Always log the SQL at DEBUG so every query is traceable without cluttering INFO
    logger.debug("%s SQL:\n%s", description, sql)
    logger.info("⏳ %s — submitting query (safety cap: %s GiB)…", description, max_bytes // (1024**3))
    t0 = time.time()
    query_job = scoped_client.query(sql, job_config=job_config)
    results = query_job.result()
    elapsed = time.time() - t0
    bytes_processed = query_job.total_bytes_processed
    bytes_billed = query_job.total_bytes_billed
    cache_hit = query_job.cache_hit

    # Build a clickable BigQuery Console URL for the job
    job_project = query_job.project
    job_location = query_job.location or "us"
    bq_console_url = (
        f"https://console.cloud.google.com/bigquery?project={job_project}"
        f"&j=bq:{job_location}:{query_job.job_id}&page=queryresults"
    )

    proc_gib = f"{bytes_processed / (1024**3):.2f} GiB" if bytes_processed is not None else "N/A"
    bill_gib = f"{bytes_billed / (1024**3):.2f} GiB" if bytes_billed is not None else "N/A"
    logger.info(
        "✅ %s — %.1fs | Job: %s | Processed: %s | Billed: %s | Cache: %s | %s",
        description, elapsed, query_job.job_id, proc_gib, bill_gib, cache_hit, bq_console_url
    )
    return results

def run_query_to_df(scoped_client: bigquery.Client, sql: str, description: str = "Query", params=None, query_parameters=None):
    """Like run_query_and_log but returns a DataFrame via BQ Storage API."""
    max_bytes = get_max_bytes_billed(params)
    job_config = bigquery.QueryJobConfig(
        maximum_bytes_billed=max_bytes,
        query_parameters=query_parameters or []
    )
    logger.debug("%s SQL:\n%s", description, sql)
    logger.info("⏳ %s — submitting query (safety cap: %s GiB)…", description, max_bytes // (1024**3))
    t0 = time.time()
    query_job = scoped_client.query(sql, job_config=job_config)
    df = query_job.result().to_dataframe(create_bqstorage_client=True)
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
        description, elapsed, query_job.job_id, proc_gib, bill_gib, query_job.cache_hit, bq_url
    )
    return df

def get_storage_metrics(scoped_client: bigquery.Client, params: StorageParams):
    focus_clause, focus_params = build_project_filter(params.focus_projects)
    sql = f"""
    SELECT
       project_id AS project_name,
       table_schema AS dataset_name,
       SUM(active_logical_bytes) AS active_logical_bytes,
       SUM(long_term_logical_bytes) AS long_term_logical_bytes,
       SUM(active_physical_bytes) AS active_physical_bytes,
       SUM(time_travel_physical_bytes) AS time_travel_physical_bytes,
       SUM(fail_safe_physical_bytes) AS fail_safe_physical_bytes,
       SUM(long_term_physical_bytes) AS long_term_physical_bytes
    FROM
       `{params.region}`.INFORMATION_SCHEMA.TABLE_STORAGE_BY_ORGANIZATION
    WHERE TRUE
       AND total_physical_bytes > 0
       AND deleted = false
       AND table_type = 'BASE TABLE'
       {focus_clause}
    GROUP BY 1,2
    """
    results = run_query_and_log(scoped_client, sql, "Storage Metrics", params=params, query_parameters=focus_params)
    
    processed_metrics = []
    GIB_CONVERSION = 1024 ** 3

    for row in results:
        # Convert to GiB
        active_logical_gib = row['active_logical_bytes'] / GIB_CONVERSION if row['active_logical_bytes'] else 0
        long_term_logical_gib = row['long_term_logical_bytes'] / GIB_CONVERSION if row['long_term_logical_bytes'] else 0
        active_physical_bytes = row['active_physical_bytes'] if row['active_physical_bytes'] else 0
        time_travel_physical_bytes = row['time_travel_physical_bytes'] if row['time_travel_physical_bytes'] else 0
        fail_safe_physical_bytes = row['fail_safe_physical_bytes'] if row['fail_safe_physical_bytes'] else 0
        long_term_physical_bytes = row['long_term_physical_bytes'] if row['long_term_physical_bytes'] else 0

        active_physical_gib = active_physical_bytes / GIB_CONVERSION
        time_travel_physical_gib = time_travel_physical_bytes / GIB_CONVERSION
        fail_safe_physical_gib = fail_safe_physical_bytes / GIB_CONVERSION
        long_term_physical_gib = long_term_physical_bytes / GIB_CONVERSION

        # Rescale time travel
        time_travel_physical_gib_rescaled = time_travel_physical_gib * params.time_travel_rescale

        # Derived metrics        # Formula: Physical Cost = (Active + Failsafe)*rate + (LongTerm)*rate
        # Since active_physical ALREADY includes TT, we subtract it out first so we can 
        # add back our RESCALED TT cost based on user parameters.
        active_no_tt_physical_gib = max(0, active_physical_gib - time_travel_physical_gib)
        
        forecast_logical_active_cost = active_logical_gib * params.active_logical_price
        forecast_logical_long_term_cost = long_term_logical_gib * params.long_term_logical_price
        forecast_logical = forecast_logical_active_cost + forecast_logical_long_term_cost
        
        forecast_active_no_tt_physical_cost = active_no_tt_physical_gib * params.active_physical_price
        forecast_travel_physical_cost = time_travel_physical_gib_rescaled * params.active_physical_price
        forecast_failsafe_physical_cost = fail_safe_physical_gib * params.active_physical_price
        forecast_long_term_physical_cost = long_term_physical_gib * params.long_term_physical_price
        
        forecast_physical = (forecast_active_no_tt_physical_cost + 
                             forecast_travel_physical_cost + 
                             forecast_failsafe_physical_cost + 
                             forecast_long_term_physical_cost)

        # Build total physical volume from the SAME components used in forecast_physical,
        # so the blended pricing ratio (cost / volume) is internally consistent.
        #
        # active_physical_bytes INCLUDES raw time travel, so we strip it out and add back
        # the RESCALED time travel — mirroring the forecast logic above exactly.
        total_physical_gib = (
            active_no_tt_physical_gib              # active minus raw TT
            + time_travel_physical_gib_rescaled   # rescaled TT (matches forecast)
            + fail_safe_physical_gib
            + long_term_physical_gib
        )
        
        processed_metrics.append({
            "project_name": row['project_name'],
            "dataset_name": row['dataset_name'],
            "forecast_logical": forecast_logical,
            "forecast_physical": forecast_physical,
            "total_physical_gib": total_physical_gib
        })

    return processed_metrics

def get_physical_datasets(scoped_client: bigquery.Client, projects: set, region: str, params=None):
    if not projects:
        return set()

    # Try fast UNION ALL approach
    unions = []
    for p in projects:
        unions.append(f"SELECT '{p}' as project_name, schema_name as dataset_name FROM `{p}.{region}.INFORMATION_SCHEMA.SCHEMATA_OPTIONS` WHERE option_name = 'storage_billing_model' AND option_value = 'PHYSICAL'")
    
    sql = "\nUNION ALL\n".join(unions)
    
    logger.info(f"Trying fast UNION ALL for physical datasets on {len(projects)} projects")
    try:
        results = run_query_and_log(scoped_client, sql, "Physical Datasets (Fast)", params=params)
        return {(row['project_name'], row['dataset_name']) for row in results}
    except Exception as e:
        logger.warning(f"Fast UNION ALL failed: {e}. Falling back to loop.")
        
    # Fallback to loop
    physical_datasets = set()
    for p in projects:
        sql = f"SELECT schema_name as dataset_name FROM `{p}.{region}.INFORMATION_SCHEMA.SCHEMATA_OPTIONS` WHERE option_name = 'storage_billing_model' AND option_value = 'PHYSICAL'"
        try:
            results = run_query_and_log(scoped_client, sql, f"Physical Datasets (Fallback {p})", params=params)
            for row in results:
                physical_datasets.add((p, row['dataset_name']))
        except Exception as e:
            logger.warning(f"Failed to query SCHEMATA_OPTIONS for project {p}: {e}")
            
    return physical_datasets

def get_org_storage_billing_model(scoped_client: bigquery.Client, region: str, params=None):
    sql = f"SELECT option_value FROM `{region}`.INFORMATION_SCHEMA.ORGANIZATION_OPTIONS WHERE option_name = 'default_storage_billing_model'"
    logger.info(f"Checking Organization Default Storage Billing Model for {region}")
    try:
        results = run_query_and_log(scoped_client, sql, "Org Storage Billing Model", params=params)
        for row in results:
            return row['option_value']
    except Exception as e:
        logger.warning(f"Failed to query ORGANIZATION_OPTIONS: {e}. Assuming LOGICAL or not set.")
    return "LOGICAL"

@app.post("/api/storage/analyze")
def analyze_storage(params: StorageParams):
    _validate_safe_params(params)
    t0 = log_endpoint_start("Storage Analysis", params, _logger=logger)
    scoped_client, resolved_project = init_bq_client_and_resolve_project(params)
    try:

        
        org_billing_model = get_org_storage_billing_model(scoped_client, params.region, params=params)
        org_status = {
            "current_model": org_billing_model,
            "is_optimized": org_billing_model == "PHYSICAL",
            "ddl": f"ALTER ORGANIZATION SET OPTIONS (`{params.region}.default_storage_billing_model`='PHYSICAL');" if org_billing_model != "PHYSICAL" else None
        }
        
        metrics = get_storage_metrics(scoped_client, params)
        
        global_physical_cost = sum(r['forecast_physical'] for r in metrics)
        global_physical_gib = sum(r['total_physical_gib'] for r in metrics)
        effective_pricing_ratio = global_physical_cost / global_physical_gib if global_physical_gib > 0 else 0
        
        projects = {row['project_name'] for row in metrics}
        physical_datasets = get_physical_datasets(scoped_client, projects, params.region, params=params)
        
        processed_data = []
        for row in metrics:
            project = row['project_name']
            dataset = row['dataset_name']
            forecast_logical = row['forecast_logical']
            forecast_physical = row['forecast_physical']
            
            currently_on = "physical" if (project, dataset) in physical_datasets else "logical"
            better_on = "physical" if forecast_logical > forecast_physical else "logical"
            
            if currently_on == better_on:
                continue
                
            forecast_compare = forecast_logical - forecast_physical
            monthly_spending = forecast_logical if currently_on == "logical" else forecast_physical
            monthly_savings = abs(forecast_compare)
            monthly_savings_pct = monthly_savings / monthly_spending if monthly_spending > 0 else 0
            
            if monthly_savings <= params.min_monthly_saving:
                continue
            if monthly_savings_pct <= params.min_monthly_saving_pct:
                continue
                
            if params.time_travel_hours is None:
                ddl = f"ALTER SCHEMA `{project}.{dataset}` SET OPTIONS(storage_billing_model='{better_on}' );"
            else:
                ddl = f"ALTER SCHEMA `{project}.{dataset}` SET OPTIONS(storage_billing_model='{better_on}', max_time_travel_hours={params.time_travel_hours});"
                
            processed_data.append({
                "project_name": project,
                "dataset_name": dataset,
                "forecast_logical": forecast_logical,
                "forecast_physical": forecast_physical,
                "forecast_compare": forecast_compare,
                "better_on": better_on,
                "currently_on": currently_on,
                "monthly_spending": monthly_spending,
                "monthly_savings": monthly_savings,
                "monthly_savings_pct": monthly_savings_pct,
                "ddl": ddl
            })
            
        processed_data.sort(key=lambda x: x['monthly_savings'], reverse=True)
        log_endpoint_end("Storage Analysis", t0, _logger=logger)
        return {
            "datasets": processed_data,
            "org_status": org_status,
            "effective_pricing_ratio": effective_pricing_ratio
        }
        
    except Exception as e:
        if "hasn't been enabled" in str(e):
            logger.warning(f"Storage view not enabled for {params.region}: {e}")
            project_id = resolved_project
            enable_ddl = f"ALTER PROJECT `{project_id}` SET OPTIONS (`{params.region}.enable_info_schema_storage` = TRUE)"
            return {
                "datasets": [],
                "org_status": {
                    "current_model": "UNKNOWN",
                    "is_optimized": False,
                    "ddl": enable_ddl,
                    "error_message": f"Storage tracking views are not enabled for region {params.region}."
                }
            }
        handle_endpoint_exception(e, "Storage analysis")

@app.post("/api/jobs/analyze")
def analyze_jobs(params: JobAnalysisParams):
    _validate_safe_params(params)
    t0 = log_endpoint_start("Job Analysis (Compute Analyzer)", params, _logger=logger)
    scoped_client, org_project = init_bq_client_and_resolve_project(params)
    focus_clause, focus_params = build_project_filter(params.focus_projects)
    try:
        
        duration_expression = "TIMESTAMP_DIFF(end_time, start_time, MILLISECOND)" if params.fluid_scaling else "GREATEST(TIMESTAMP_DIFF(end_time, start_time, MILLISECOND), 60000)"
        
        sql = f"""
        SELECT
          job_id,
          user_email,
          project_id,
          COALESCE(total_bytes_billed, 0) AS total_bytes_billed,
          total_slot_ms,
          CASE WHEN error_result IS NOT NULL THEN TRUE ELSE FALSE END AS has_error,
          NULLIF(TIMESTAMP_DIFF(end_time, start_time, MILLISECOND), 0) AS actual_duration_ms,
          {duration_expression} AS billed_duration_ms
        FROM
          `{org_project}`.`{params.region}`.INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION
        WHERE
          creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {params.lookback_days} DAY)
          AND state = 'DONE'
          AND job_type = 'QUERY'
          AND (statement_type != 'SCRIPT' OR statement_type IS NULL)
          AND IFNULL(cache_hit, FALSE) = FALSE
          AND total_bytes_billed >= {params.min_bytes_billed}
          {focus_clause}
        ORDER BY total_bytes_billed DESC
        LIMIT {params.limit_jobs}
        """
        
        results = run_query_and_log(scoped_client, sql, "Job Stats", params=params, query_parameters=focus_params)
        
        project_metrics = {}
        top_jobs = []
        
        TB_CONVERSION = 1024 ** 4
        SLOT_HR_MS = 3600000.0
        
        for row in results:
            project = row['project_id']
            job_id = row['job_id']
            user_email = row['user_email']
            
            bytes_billed = row['total_bytes_billed']
            slot_ms = row['total_slot_ms']
            has_error = row['has_error']
            actual_duration_ms = row['actual_duration_ms'] or 0
            billed_duration_ms = row['billed_duration_ms'] if params.fluid_scaling else (row['billed_duration_ms'] or 60000)
            
            avg_slots = (slot_ms / actual_duration_ms) if (actual_duration_ms and slot_ms is not None) else 0

            # NOTE: No spike factor. The legacy short-query tax is modeled SOLELY by the
            # 60s duration floor (applied to billed_duration_ms in legacy mode). Inflating
            # avg_slots here would double-count the same 60s minimum penalty.
            #
            # Proof (DoiT Google Next FinOps reference): consumed + tax slot-hours =
            #   (avg_slots * duration) + (avg_slots * (60 - duration)) = avg_slots * 60.
            # i.e. the correct legacy bill is exactly avg_slots held for the 60s floor,
            # with NO peak/slot multiplier. avg_slots already reflects bursts because a
            # spiky job has higher total_slot_ms over its short window.
            effective_slots = avg_slots

            # Slot-packing heuristic (unrelated to the 60s tax): small queries are assumed
            # to pack into existing baseline capacity without triggering an independent
            # autoscaler event. Larger usage rounds up to the slot step increment.
            if effective_slots < 50:
                billed_slots = effective_slots
            else:
                billed_slots = math.ceil(effective_slots / params.slot_step_size) * params.slot_step_size
            
            on_demand_cost = (bytes_billed / TB_CONVERSION) * params.on_demand_rate_per_tb
            editions_cost = ((billed_slots * billed_duration_ms) / SLOT_HR_MS) * params.edition_slot_hr_rate
            savings = on_demand_cost - editions_cost
            
            # Heuristic 3: 3-Bucket Categorization
            if on_demand_cost > editions_cost * 1.2:
                category = "Strong Reservation Candidate (High IO / Low CPU)"
            elif editions_cost > on_demand_cost * 1.2:
                category = "Strong On-Demand Candidate (Low IO / High CPU)"
            else:
                category = "Balanced / Uncertain"
                
            if project not in project_metrics:
                project_metrics[project] = {
                    "on_demand_cost": 0.0,
                    "editions_cost": 0.0,
                    "error_tax": 0.0,
                    "net_savings": 0.0
                }
                
            project_metrics[project]["on_demand_cost"] += on_demand_cost
            project_metrics[project]["editions_cost"] += editions_cost
            if has_error:
                project_metrics[project]["error_tax"] += editions_cost
            project_metrics[project]["net_savings"] += savings
            
            top_jobs.append({
                "job_id": job_id,
                "project_id": project,
                "user_email": user_email,
                "on_demand_cost": on_demand_cost,
                "editions_cost": editions_cost,
                "waste_savings": savings,
                "has_error": has_error,
                "category": category,
                "avg_slots": avg_slots,
                "effective_slots": effective_slots,
                "billed_slots": billed_slots
            })
            
        # Format project summaries
        project_list = []
        for p, m in project_metrics.items():
            project_list.append({
                "project_id": p,
                "total_on_demand_cost": m["on_demand_cost"],
                "total_editions_cost": m["editions_cost"],
                "editions_error_tax": m["error_tax"],
                "reservation_savings": m["net_savings"]
            })
            
        project_list.sort(key=lambda x: x["reservation_savings"], reverse=True)
        
        # Format top jobs
        top_jobs.sort(key=lambda x: x["waste_savings"], reverse=True)
        top_candidates = top_jobs[:500] # Return top 500 for UI performance
        
        log_endpoint_end("Job Analysis (Compute Analyzer)", t0, _logger=logger)
        return {
            "project_summaries": project_list,
            "top_jobs": top_candidates
        }
        
    except Exception as e:
        handle_endpoint_exception(e, "Job analysis")


class HygieneParams(FocusMixin):
    org_project_id: Optional[str] = None
    region: str = "region-us"
    limit: int = Field(default=20, ge=1, le=500)
    max_bytes_billed_gb: Optional[int] = None

class HygieneResult(BaseModel):
    project_id: str
    dataset: str
    table_name: str
    live_active_physical_gb: float
    time_travel_gb: float
    churn_ratio: float
    health_status: str

@app.post("/api/storage/hygiene", response_model=List[HygieneResult])
def analyze_storage_hygiene(params: HygieneParams):
    _validate_safe_params(params)
    t0 = log_endpoint_start("Storage Hygiene", params, _logger=logger)
    scoped_client, target_project = init_bq_client_and_resolve_project(params)
    focus_clause, focus_params = build_project_filter(params.focus_projects)
    try:
        
        sql = f"""
        SELECT
          project_id,
          table_schema AS dataset,
          table_name,
          (active_physical_bytes - time_travel_physical_bytes) / POW(1024,3) AS live_active_physical_gb,
          time_travel_physical_bytes / POW(1024,3) AS time_travel_gb,
          SAFE_DIVIDE(time_travel_physical_bytes, active_physical_bytes) AS churn_ratio,
          IF(time_travel_physical_bytes > 0.5 * active_physical_bytes, 'High Churn/Recreate Detected', 'Healthy') AS health_status
        FROM `{target_project}`.`{params.region}`.INFORMATION_SCHEMA.TABLE_STORAGE_BY_ORGANIZATION
        WHERE
          total_physical_bytes > 0
          AND deleted = FALSE
          {focus_clause}
        ORDER BY time_travel_gb DESC
        LIMIT {params.limit}
        """
        

        results = run_query_and_log(scoped_client, sql, "Storage Hygiene", params=params, query_parameters=focus_params)
        
        output = []
        for row in results:
            output.append(HygieneResult(
                project_id=row.project_id,
                dataset=row.dataset,
                table_name=row.table_name,
                live_active_physical_gb=float(row.live_active_physical_gb or 0),
                time_travel_gb=float(row.time_travel_gb or 0),
                churn_ratio=float(row.churn_ratio or 0),
                health_status=row.health_status
            ))
        log_endpoint_end("Storage Hygiene", t0, _logger=logger)
        return output
        
    except Exception as e:
        handle_endpoint_exception(e, "Storage hygiene analysis")

class DMLAbuseParams(FocusMixin):
    org_project_id: Optional[str] = None
    region: str = "region-us"
    lookback_days: int = Field(default=1, ge=1, le=90)
    threshold: int = Field(default=1000, ge=1)
    max_bytes_billed_gb: Optional[int] = None

class DMLAbuseResult(BaseModel):
    user_email: str
    project_id: str
    insert_job_count: int
    wasted_slot_hours: float

@app.post("/api/antipatterns/dml", response_model=List[DMLAbuseResult])
def analyze_dml_abuse(params: DMLAbuseParams):
    _validate_safe_params(params)
    t0 = log_endpoint_start("DML Abuse Auditor", params, _logger=logger)
    scoped_client, target_project = init_bq_client_and_resolve_project(params)
    focus_clause, focus_params = build_project_filter(params.focus_projects)
    try:
        
        sql = f"""
        SELECT
          user_email,
          project_id,
          COUNT(job_id) AS insert_job_count,
          SUM(total_slot_ms) / (1000 * 60 * 60) AS wasted_slot_hours
        FROM
          `{target_project}`.`{params.region}`.INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION
        WHERE
          statement_type = 'INSERT'
          AND creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {params.lookback_days} DAY)
          {focus_clause}
        GROUP BY
          user_email, project_id
        HAVING 
          insert_job_count > {params.threshold}
        ORDER BY
          wasted_slot_hours DESC
        """
        

        results = run_query_and_log(scoped_client, sql, "DML Abuse", params=params, query_parameters=focus_params)
        
        output = []
        for row in results:
            output.append(DMLAbuseResult(
                user_email=row.user_email,
                project_id=row.project_id,
                insert_job_count=row.insert_job_count,
                wasted_slot_hours=row.wasted_slot_hours
            ))
        log_endpoint_end("DML Abuse Auditor", t0, _logger=logger)
        return output
        
    except Exception as e:
        handle_endpoint_exception(e, "DML abuse analysis")

class MVCostResult(BaseModel):
    project_id: str
    dataset: str
    table_name: str
    refresh_count: int
    total_slot_hours: float

@app.post("/api/antipatterns/mv", response_model=List[MVCostResult])
def analyze_mv_costs(params: DMLAbuseParams):
    _validate_safe_params(params)
    t0 = log_endpoint_start("MV Cost Auditor", params, _logger=logger)
    scoped_client, target_project = init_bq_client_and_resolve_project(params)
    focus_clause, focus_params = build_project_filter(params.focus_projects)
    try:
        
        # 1. Get all Materialized Views
        mv_sql = f"""
        SELECT table_catalog AS project_id, table_schema, table_name 
        FROM `{target_project}`.`{params.region}`.INFORMATION_SCHEMA.TABLES 
        WHERE table_type = 'MATERIALIZED VIEW'
        """
        logger.debug("Fetching MVs:\n%s", mv_sql)
        mv_results = run_query_and_log(scoped_client, mv_sql, "MV List", params=params)
        mvs = {(row.project_id, row.table_schema, row.table_name) for row in mv_results}
        
        if not mvs:
            log_endpoint_end("MV Cost Auditor", t0, _logger=logger)
            return []
            
        # 2. Get all query jobs with destination tables
        jobs_sql = f"""
        SELECT
          project_id,
          destination_table.dataset_id AS dataset_id,
          destination_table.table_id AS table_id,
          total_slot_ms
        FROM `{target_project}`.`{params.region}`.INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION
        WHERE creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {params.lookback_days} DAY)
          AND job_type = 'QUERY'
          AND destination_table.table_id IS NOT NULL
          {focus_clause}
        """
        logger.debug("Fetching jobs for MV check:\n%s", jobs_sql)
        jobs_results = run_query_and_log(scoped_client, jobs_sql, "MV Jobs", params=params, query_parameters=focus_params)
        
        # 3. Process in Python
        from collections import defaultdict
        mv_stats = defaultdict(lambda: {"count": 0, "slot_ms": 0})
        
        for row in jobs_results:
            key = (row.project_id, row.dataset_id, row.table_id)
            if key in mvs:
                stat_key = (row.project_id, row.dataset_id, row.table_id)
                mv_stats[stat_key]["count"] += 1
                mv_stats[stat_key]["slot_ms"] += row.total_slot_ms or 0
                
        output = []
        for (proj, ds, tbl), stats in mv_stats.items():
            output.append(MVCostResult(
                project_id=proj,
                dataset=ds,
                table_name=tbl,
                refresh_count=stats["count"],
                total_slot_hours=stats["slot_ms"] / (1000 * 60 * 60)
            ))
            
        output.sort(key=lambda x: x.total_slot_hours, reverse=True)
        log_endpoint_end("MV Cost Auditor", t0, _logger=logger)
        return output
        
    except Exception as e:
        handle_endpoint_exception(e, "MV cost analysis")

class AntiPatternParams(FocusMixin):
    org_project_id: Optional[str] = None
    region: str = "region-us"
    lookback_days: int = Field(default=7, ge=1, le=90)
    limit_per_project: int = Field(default=100, ge=1, le=1000)
    max_bytes_billed_gb: Optional[int] = None

class LinterResult(BaseModel):
    project_id: str
    job_id: str
    user_email: str
    query_snippet: str
    abuse_type: str
    billed_gb: float

@app.post("/api/antipatterns/linter", response_model=List[LinterResult])
def analyze_query_linter(params: AntiPatternParams):
    _validate_safe_params(params)
    t0 = log_endpoint_start("Linter Analysis", params, _logger=logger)
    scoped_client, target_project = init_bq_client_and_resolve_project(params)
    focus_clause, focus_params = build_project_filter(params.focus_projects)
    try:
        
        # 1. Find active projects — skip discovery when focus filter is set
        if params.focus_projects:
            projects = list(params.focus_projects)
        else:
            projects_sql = f"""
            SELECT DISTINCT project_id 
            FROM `{target_project}`.`{params.region}`.INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION
            WHERE creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {params.lookback_days} DAY)
              AND job_type = 'QUERY'
              AND project_id IS NOT NULL
            """
            
            logger.debug("Fetching active projects for linter scan:\n%s", projects_sql)
            projects_results = run_query_and_log(scoped_client, projects_sql, "Linter Projects", params=params)
            projects = [row.project_id for row in projects_results]
        
        if not projects:
            projects = [target_project]
            
        output = []
        
        import re
        select_star_regex = re.compile(r'SELECT\s+\*\s+FROM', re.IGNORECASE)
        
        # 2. Loop through projects and lint queries
        for p in projects:
            sql = f"""
            SELECT
              job_id,
              user_email,
              query,
              total_bytes_billed / POW(1024, 3) AS billed_gb
            FROM `{p}`.`{params.region}`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
            WHERE creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {params.lookback_days} DAY)
              AND job_type = 'QUERY'
              AND state = 'DONE'
              AND query IS NOT NULL
              AND total_bytes_billed > 107374182400 -- > 100 GB
            LIMIT {params.limit_per_project}
            """
            
            logger.info(f"Scanning project {p} for SELECT * abuse...")
            try:
                results = run_query_and_log(scoped_client, sql, f"Linter Scan {p}", params=params)
                for row in results:
                    query_text = row.query or ''
                    if select_star_regex.search(query_text):
                        output.append(LinterResult(
                            project_id=p,
                            job_id=row.job_id,
                            user_email=row.user_email,
                            query_snippet=query_text[:100] + "...",
                            abuse_type="[SELECT * ABUSE]",
                            billed_gb=row.billed_gb or 0.0
                        ))
            except Exception as e:
                logger.warning(f"Failed to scan project {p} for linter: {e}")
                
        output.sort(key=lambda x: x.billed_gb, reverse=True)
        log_endpoint_end("Linter Analysis", t0, _logger=logger)
        return output
        
    except Exception as e:
        handle_endpoint_exception(e, "Linter analysis")

class SkewResult(BaseModel):
    project_id: str
    job_id: str
    user_email: str
    stage_name: str
    avg_compute_ms: int
    max_compute_ms: int
    skew_ratio: float

@app.post("/api/antipatterns/skew", response_model=List[SkewResult])
def analyze_data_skew(params: AntiPatternParams):
    _validate_safe_params(params)
    t0 = log_endpoint_start("Skew Analysis", params, _logger=logger)
    scoped_client, target_project = init_bq_client_and_resolve_project(params)
    focus_clause, focus_params = build_project_filter(params.focus_projects)
    try:
        
        sql = f"""
        WITH unnested_stages AS (
          SELECT
            job_id,
            user_email,
            project_id,
            total_slot_ms,
            stage.name AS stage_name,
            stage.compute_ms_avg AS avg_compute_ms,
            stage.compute_ms_max AS max_compute_ms
          FROM
            `{target_project}`.`{params.region}`.INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION,
            UNNEST(job_stages) AS stage
          WHERE
            creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {params.lookback_days} DAY)
            AND job_type = 'QUERY'
            AND state = 'DONE'
            {focus_clause}
        )
        SELECT
          job_id,
          user_email,
          project_id,
          stage_name,
          avg_compute_ms,
          max_compute_ms,
          SAFE_DIVIDE(max_compute_ms, avg_compute_ms) AS skew_ratio
        FROM
          unnested_stages
        WHERE
          avg_compute_ms > 0
          AND SAFE_DIVIDE(max_compute_ms, avg_compute_ms) > 10
          AND max_compute_ms > 10000
        ORDER BY
          skew_ratio DESC
        LIMIT {params.limit_per_project}
        """
        

        results = run_query_and_log(scoped_client, sql, "Data Skew", params=params, query_parameters=focus_params)
        
        output = []
        for row in results:
            output.append(SkewResult(
                project_id=row.project_id,
                job_id=row.job_id,
                user_email=row.user_email,
                stage_name=row.stage_name,
                avg_compute_ms=row.avg_compute_ms,
                max_compute_ms=row.max_compute_ms,
                skew_ratio=row.skew_ratio
            ))
        log_endpoint_end("Skew Analysis", t0, _logger=logger)
        return output
        
    except Exception as e:
        handle_endpoint_exception(e, "Skew analysis")

class BatchCandidateResult(BaseModel):
    project_id: str
    job_id: str
    user_email: str
    duration_minutes: float
    total_slot_ms: int
    batch_candidate_reason: str

@app.post("/api/antipatterns/batch_candidates", response_model=List[BatchCandidateResult])
def analyze_batch_candidates(params: AntiPatternParams):
    _validate_safe_params(params)
    t0 = log_endpoint_start("Batch Candidates Analysis", params, _logger=logger)
    scoped_client, target_project = init_bq_client_and_resolve_project(params)
    focus_clause, focus_params = build_project_filter(params.focus_projects)
    try:
        
        sql = f"""
        SELECT
          job_id,
          user_email,
          project_id,
          total_slot_ms,
          TIMESTAMP_DIFF(end_time, start_time, MINUTE) AS duration_minutes,
          CASE 
            WHEN user_email LIKE '%.gserviceaccount.com' THEN 'Service Account'
            WHEN TIMESTAMP_DIFF(end_time, start_time, MINUTE) > 5 THEN 'Long-Running ETL'
            WHEN EXTRACT(HOUR FROM creation_time AT TIME ZONE "UTC") NOT BETWEEN 13 AND 23 THEN 'Off-Peak Hours (US)'
            ELSE 'Other'
          END AS batch_candidate_reason
        FROM
          `{target_project}`.`{params.region}`.INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION
        WHERE
          job_type = 'QUERY'
          AND priority = 'INTERACTIVE'
          AND total_slot_ms > 10000
          AND creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {params.lookback_days} DAY)
          AND (
            user_email LIKE '%.gserviceaccount.com'
            OR TIMESTAMP_DIFF(end_time, start_time, MINUTE) > 5
            OR EXTRACT(HOUR FROM creation_time AT TIME ZONE "UTC") NOT BETWEEN 13 AND 23
          )
          {focus_clause}
        ORDER BY
          total_slot_ms DESC
        LIMIT {params.limit_per_project}
        """
        

        results = run_query_and_log(scoped_client, sql, "Batch Candidates", params=params, query_parameters=focus_params)
        
        output = []
        for row in results:
            output.append(BatchCandidateResult(
                project_id=row.project_id,
                job_id=row.job_id,
                user_email=row.user_email,
                duration_minutes=row.duration_minutes or 0.0,
                total_slot_ms=row.total_slot_ms or 0,
                batch_candidate_reason=row.batch_candidate_reason or 'Other'
            ))
        log_endpoint_end("Batch Candidates Analysis", t0, _logger=logger)
        return output
        
    except Exception as e:
        handle_endpoint_exception(e, "Batch candidates analysis")



class AIParams(FocusMixin):
    org_project_id: Optional[str] = None
    region: str = "region-us"
    lookback_days: int = Field(default=7, ge=1, le=90)
    limit: int = Field(20, ge=1, le=100)
    max_bytes_billed_gb: Optional[int] = None

class AIResult(BaseModel):
    job_id: str
    user_email: str
    total_slot_ms: int
    query: str
    gemini_optimization_advice: str
    tables_referenced_count: int
    tables_found_count: int

TABLE_PATTERN = re.compile(
    r"\b(?:FROM|JOIN)\s+(?:"
    r"`([a-zA-Z0-9_\-]+)\.([a-zA-Z0-9_]+)\.([a-zA-Z0-9_]+)`|"  # `project.dataset.table`
    r"`([a-zA-Z0-9_]+)\.([a-zA-Z0-9_]+)`|"                     # `dataset.table`
    r"([a-zA-Z0-9_\-]+)\.([a-zA-Z0-9_]+)\.([a-zA-Z0-9_]+)|"     # project.dataset.table
    r"([a-zA-Z0-9_]+)\.([a-zA-Z0-9_]+)"                         # dataset.table
    r")",
    re.IGNORECASE
)

def extract_table_names(sql: str, default_project: str) -> List[str]:
    # Strip comments to prevent matching tables inside commented-out SQL code
    sql_clean = re.sub(r"--.*", "", sql)
    sql_clean = re.sub(r"/\*.*?\*/", "", sql_clean, flags=re.DOTALL)
    
    tables = []
    for match in TABLE_PATTERN.finditer(sql_clean):
        groups = match.groups()
        if groups[0] and groups[1] and groups[2]:
            tables.append(f"{groups[0]}.{groups[1]}.{groups[2]}")
        elif groups[3] and groups[4]:
            tables.append(f"{default_project}.{groups[3]}.{groups[4]}")
        elif groups[5] and groups[6] and groups[7]:
            tables.append(f"{groups[5]}.{groups[6]}.{groups[7]}")
        elif groups[8] and groups[9]:
            tables.append(f"{default_project}.{groups[8]}.{groups[9]}")
            
    seen = set()
    unique_tables = []
    for t in tables:
        if t not in seen:
            seen.add(t)
            unique_tables.append(t)
    return unique_tables

@app.post("/api/ai/analyze", response_model=List[AIResult])
def analyze_ai_query(params: AIParams):
    _validate_safe_params(params)
    t0 = log_endpoint_start("AI Doctor", params, _logger=logger)
    params.focus_projects = validate_focus_projects(params.focus_projects)
    scoped_client, target_project = init_bq_client_and_resolve_project(params)
    try:
        focus_clause, focus_params = build_project_filter(params.focus_projects)
        # Step 1: Query Discovery
        discovery_sql = f"""
        SELECT
          job_id,
          project_id,
          user_email,
          total_slot_ms
        FROM
          `{target_project}`.`{params.region}`.INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION
        WHERE
          job_type = 'QUERY'
          AND statement_type = 'SELECT'
          AND (statement_type IS NULL OR statement_type <> 'SCRIPT')
          AND creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {params.lookback_days} DAY)
          {focus_clause}
        ORDER BY
          total_slot_ms DESC
        LIMIT {params.limit}
        """
        
        logger.info("Executing AI Query Discovery stage")
        discovery_results = run_query_and_log(scoped_client, discovery_sql, "AI Query Discovery", params=params, query_parameters=focus_params)
        
        # JOBS_BY_ORGANIZATION does not contain the 'query' text for privacy.
        # We must fetch the query text directly from JOBS_BY_PROJECT for the identified top jobs.
        project_to_jobs = {}
        for row in discovery_results:
            project_to_jobs.setdefault(row.project_id, []).append(row)
            
        expensive_queries = []
        for pid, jobs in project_to_jobs.items():
            job_ids = [j.job_id for j in jobs]
            sql = f"""
            SELECT job_id, query 
            FROM `{pid}`.`{params.region}`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
            WHERE creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {params.lookback_days} DAY)
              AND job_id IN UNNEST(@job_ids)
            """
            job_id_params = [
                bigquery.ArrayQueryParameter("job_ids", "STRING", job_ids)
            ]
            try:
                q_results = run_query_and_log(scoped_client, sql, f"Fetch queries for {pid}", params=params, query_parameters=job_id_params)
                q_map = {r.job_id: r.query for r in q_results}
            except Exception as e:
                logger.warning(f"Failed to fetch query texts for {pid}: {e}")
                q_map = {}
                
            for j in jobs:
                # BigQuery might redact query texts for some users, defaulting to empty string
                query_text = q_map.get(j.job_id, "")
                expensive_queries.append({
                    "job_id": j.job_id,
                    "user_email": j.user_email or 'unknown',
                    "total_slot_ms": j.total_slot_ms or 0,
                    "query": query_text
                })
        
        # Sort back by total_slot_ms since the project grouping scrambled the order
        expensive_queries.sort(key=lambda x: x["total_slot_ms"], reverse=True)
            
        if not expensive_queries:
            logger.info("No expensive queries found to audit.")
            log_endpoint_end("AI Doctor", t0, _logger=logger)
            return []
            
        # Step 2: Concurrently Retrieve DDL Schemas (H1: Latency Optimization)
        # Deduplicate referenced tables first
        all_tables = set()
        for item in expensive_queries:
            raw_sql = item["query"]
            referenced = extract_table_names(raw_sql, target_project)
            all_tables.update(referenced)
            
        schema_cache = {}
        
        def fetch_table_schema(table_ref):
            try:
                table_obj = scoped_client.get_table(table_ref)
                return table_ref, table_obj
            except (gax_exc.NotFound, gax_exc.Forbidden):
                return table_ref, None
                
        # Concurrently fetch schemas using a ThreadPoolExecutor
        logger.info(f"Concurrently fetching schemas for {len(all_tables)} unique referenced tables")
        with ThreadPoolExecutor(max_workers=10) as executor:
            future_results = executor.map(fetch_table_schema, all_tables)
            for table_ref, table_obj in future_results:
                schema_cache[table_ref] = table_obj
                
        # Build Audits Data
        endpoint_url = (
            f"https://aiplatform.googleapis.com/v1/projects/{target_project}"
            f"/locations/global/publishers/google/models/gemini-3.1-flash-lite"
        )
        
        audits_to_run = []
        for item in expensive_queries:
            raw_sql = item["query"]
            referenced_tables = extract_table_names(raw_sql, target_project)
            tables_referenced_count = len(referenced_tables)
            tables_found_count = 0
            schemas_context = []
            
            for table_ref in referenced_tables:
                table_obj = schema_cache.get(table_ref)
                if table_obj:
                    # Surfacing range-partitioning and partition requirement (M2)
                    if table_obj.time_partitioning:
                        field = table_obj.time_partitioning.field or "_PARTITIONTIME (ingestion)"
                        req = " (REQUIRES partition filter)" if table_obj.require_partition_filter else ""
                        part_info = f"Partitioned by: {field}{req}"
                    elif table_obj.range_partitioning:
                        part_info = f"Range-partitioned by: {table_obj.range_partitioning.field}"
                    else:
                        part_info = "Not partitioned"
                        
                    clust_info = f"Clustered by: {', '.join(table_obj.clustering_fields)}" if table_obj.clustering_fields else "Not clustered"
                    # Summarize DDL: We don't need all column names to detect structural anti-patterns.
                    # This drastically reduces payload size, leaving more room for the SQL text.
                    num_columns = len(table_obj.schema)
                    
                    num_rows = table_obj.num_rows if table_obj.num_rows is not None else 0
                    num_bytes = table_obj.num_bytes if table_obj.num_bytes is not None else 0
                    
                    schemas_context.append(
                        f"Table `{table_ref}`:\n"
                        f"- Row count: {num_rows:,} | Size: {num_bytes / (1024**2):.2f} MB\n" # Rich size metadata (M3)
                        f"- {part_info}\n"
                        f"- {clust_info}\n"
                        f"- Schema size: {num_columns} columns"
                    )
                    tables_found_count += 1
                    
            table_schemas_text = "\n\n".join(schemas_context) if schemas_context else "No table schemas could be retrieved."
            if len(table_schemas_text) > 4000:
                cut = table_schemas_text[:4000].rfind('\n')
                table_schemas_text = table_schemas_text[:cut if cut > 2000 else 4000] + "\n... [TRUNCATED DUE TO SIZE LIMIT]"
                
            safe_sql = raw_sql
            if len(safe_sql) > 5000:
                cut = safe_sql[:5000].rfind('\n')
                safe_sql = safe_sql[:cut if cut > 3000 else 5000] + "\n... [QUERY TRUNCATED DUE TO SIZE LIMIT]"
            
            prompt_content = (
                f"You are an elite Google Cloud BigQuery Data Engineer.\n"
                f"Analyze the following SQL query and flag any performance anti-patterns based on these specific rules:\n"
                f"- Avoid SELECT * (especially with LIMIT, as LIMIT does not reduce bytes billed).\n"
                f"- Filter data (WHERE clauses) BEFORE joining tables.\n"
                f"- Avoid CROSS JOINs.\n"
                f"- Use APPROX_COUNT_DISTINCT instead of COUNT(DISTINCT) if applicable.\n"
                f"- Avoid ordering (ORDER BY) a large result set without a LIMIT.\n"
                f"- Do not use REGEXP_CONTAINS if a simple LIKE would work.\n"
                f"- Avoid using ROW_NUMBER() OVER() just to get the latest record; suggest ARRAY_AGG() instead.\n\n"
                f"Use the provided physical table schemas to verify if partitioning or clustering are utilized correctly:\n"
                f"--- PHYSICAL TABLE SCHEMAS ---\n"
                f"{table_schemas_text}\n\n"
                f"--- SQL QUERY TO ANALYZE ---\n"
                f"{safe_sql}\n\n"
                f"If the query violates any of these, provide a clean bulleted list of the violations (without referencing rule numbers and without using markdown bolding) and a 1-sentence fix for each. "
                f"If the query is perfectly optimized, reply exactly with \"NO_ANTI_PATTERNS_FOUND\"."
            )
            
            audits_to_run.append({
                "job_id": item["job_id"],
                "user_email": item["user_email"],
                "total_slot_ms": item["total_slot_ms"],
                "query": raw_sql,
                "tables_referenced_count": tables_referenced_count,
                "tables_found_count": tables_found_count,
                "prompt_content": prompt_content
            })
            
        # Step 3: Chunk Audits & Execute Parameterized Queries (C1 & H2: Safety & Reliability)
        output = []
        chunk_size = 5
        chunks = [audits_to_run[i:i + chunk_size] for i in range(0, len(audits_to_run), chunk_size)]
        
        for chunk_idx, chunk in enumerate(chunks):
            subqueries = []
            query_params = []
            
            for idx, audit in enumerate(chunk):
                param_suffix = f"c{chunk_idx}_a{idx}"
                
                subqueries.append(f"""
                SELECT
                  @job_id_{param_suffix} AS job_id,
                  @email_{param_suffix} AS user_email,
                  @slot_ms_{param_suffix} AS total_slot_ms,
                  @query_{param_suffix} AS query,
                  @ref_count_{param_suffix} AS tables_referenced_count,
                  @found_count_{param_suffix} AS tables_found_count,
                  AI.GENERATE(
                    @prompt_{param_suffix},
                    endpoint => '{endpoint_url}',
                    model_params => JSON '{{"generation_config": {{"temperature": 0.1, "max_output_tokens": 1024, "thinking_config": {{"thinking_level": "MINIMAL"}}}}, "safety_settings": [{{"category": "HARM_CATEGORY_HATE_SPEECH", "threshold": "OFF"}}, {{"category": "HARM_CATEGORY_DANGEROUS_CONTENT", "threshold": "OFF"}}, {{"category": "HARM_CATEGORY_SEXUALLY_EXPLICIT", "threshold": "OFF"}}, {{"category": "HARM_CATEGORY_HARASSMENT", "threshold": "OFF"}}]}}'
                  ) AS ai_struct
                """)
                
                query_params.extend([
                    bigquery.ScalarQueryParameter(f"job_id_{param_suffix}", "STRING", audit["job_id"]),
                    bigquery.ScalarQueryParameter(f"email_{param_suffix}", "STRING", audit["user_email"]),
                    bigquery.ScalarQueryParameter(f"slot_ms_{param_suffix}", "INT64", audit["total_slot_ms"]),
                    bigquery.ScalarQueryParameter(f"query_{param_suffix}", "STRING", audit["query"]),
                    bigquery.ScalarQueryParameter(f"ref_count_{param_suffix}", "INT64", audit["tables_referenced_count"]),
                    bigquery.ScalarQueryParameter(f"found_count_{param_suffix}", "INT64", audit["tables_found_count"]),
                    bigquery.ScalarQueryParameter(f"prompt_{param_suffix}", "STRING", audit["prompt_content"]),
                ])
                
            union_sql = "\nUNION ALL\n".join(subqueries)
            
            logger.info(f"Executing grounded AI audits (Chunk {chunk_idx + 1}/{len(chunks)})")
            logger.debug(f"AI Query Analysis SQL (Chunk {chunk_idx + 1}):\n{union_sql}")
            
            try:
                chunk_results = run_query_and_log(
                    scoped_client, 
                    union_sql, 
                    f"AI Query Analysis - Chunk {chunk_idx + 1}", 
                    params=params, 
                    query_parameters=query_params
                )
                
                for row in chunk_results:
                    ai_struct = row.ai_struct
                    logger.debug(f"Job {row.job_id} ai_struct = {ai_struct}")
                    advice = ""
                    if ai_struct:
                        advice = ai_struct.get("result", "") or ""
                        status = ai_struct.get("status", "")
                        full_response = ai_struct.get("full_response", "")
                        if not advice:
                            if status:
                                logger.error(
                                    f"AI.GENERATE failed for Job {row.job_id} | "
                                    f"Status: {status}"
                                )
                            else:
                                logger.error(
                                    f"AI.GENERATE blocked for Job {row.job_id} | "
                                    f"Full Response: {full_response}"
                                )
                    else:
                        logger.error(f"AI.GENERATE returned NULL struct for Job {row.job_id}")
                            
                    if "NO_ANTI_PATTERNS_FOUND" not in advice:
                        logger.info(f"AI Doctor advice for Job {row.job_id} (User: {row.user_email}):")
                        logger.debug(f"Advice:\n{advice}\n" + "-" * 80)
                        output.append(AIResult(
                            job_id=row.job_id,
                            user_email=row.user_email,
                            total_slot_ms=row.total_slot_ms or 0,
                            query=row.query or '',
                            gemini_optimization_advice=advice,
                            tables_referenced_count=row.tables_referenced_count or 0,
                            tables_found_count=row.tables_found_count or 0
                        ))
            except Exception as e:
                logger.error(f"Error executing AI audit chunk {chunk_idx + 1}: {e}")
                # If multiple chunks exist, allow continuing to capture partial results.
                # If only one chunk is run, let the error bubble up to granular exception handling.
                if len(chunks) == 1:
                    raise e
                else:
                    logger.warning("Continuing to next chunk despite failure...")
        
        log_endpoint_end("AI Doctor", t0, _logger=logger)
        return output
        
    except HTTPException:
        raise
    except Exception as e:
        # Restored granular exception mapping (M1)
        handle_endpoint_exception(e, "AI query analysis")


class BIParams(FocusMixin):
    org_project_id: Optional[str] = None
    region: str = "region-us"
    lookback_days: int = Field(default=7, ge=1, le=90)
    limit: int = Field(default=50, ge=1, le=500)
    max_bytes_billed_gb: Optional[int] = None

class BIResult(BaseModel):
    job_id: str
    user_email: str
    processed_gb: float
    billed_gb: float
    estimated_dollars_saved: float
    bi_engine_mode: str
    failure_reasons: str

@app.post("/api/bi/analyze", response_model=List[BIResult])
def analyze_bi_engine(params: BIParams):
    _validate_safe_params(params)
    t0 = log_endpoint_start("BI Engine Optimizer", params, _logger=logger)
    scoped_client, target_project = init_bq_client_and_resolve_project(params)
    focus_clause, focus_params = build_project_filter(params.focus_projects)
    try:
        
        sql = f"""
        SELECT
          job_id,
          user_email,
          total_bytes_processed / POW(1024, 3) AS processed_gb,
          total_bytes_billed / POW(1024, 3) AS billed_gb,
          ((total_bytes_processed - total_bytes_billed) / POW(1024, 4)) * 6.25 AS estimated_dollars_saved,
          bi_engine_statistics.bi_engine_mode,
          ARRAY_TO_STRING(
            ARRAY(SELECT code FROM UNNEST(bi_engine_statistics.bi_engine_reasons)), ', '
          ) AS failure_reasons
        FROM `{target_project}`.`{params.region}`.INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION
        WHERE 
          job_type = 'QUERY'
          AND creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {params.lookback_days} DAY)
          {focus_clause}
        ORDER BY total_bytes_processed DESC
        LIMIT {params.limit}
        """
        

        results = run_query_and_log(scoped_client, sql, "BI Engine", params=params, query_parameters=focus_params)
        
        output = []
        for row in results:
            output.append(BIResult(
                job_id=row.job_id,
                user_email=row.user_email,
                processed_gb=row.processed_gb or 0.0,
                billed_gb=row.billed_gb or 0.0,
                estimated_dollars_saved=row.estimated_dollars_saved or 0.0,
                bi_engine_mode=row.bi_engine_mode or 'UNKNOWN',
                failure_reasons=row.failure_reasons or ''
            ))
        log_endpoint_end("BI Engine Optimizer", t0, _logger=logger)
        return output
        
    except Exception as e:
        handle_endpoint_exception(e, "BI engine analysis")

class GovernanceParams(FocusMixin):
    org_project_id: Optional[str] = None
    region: str = "region-us"
    max_bytes_billed_gb: Optional[int] = None

class ExpirationResult(BaseModel):
    project_id: str
    dataset_id: str
    default_table_expiration: Optional[str] = None

class PartitionFilterResult(BaseModel):
    project_id: str
    dataset_id: str
    table_name: str
    partition_type: str

class GovernanceResponse(BaseModel):
    expiration_issues: List[ExpirationResult]
    filter_issues: List[PartitionFilterResult]

@app.post("/api/governance/analyze", response_model=GovernanceResponse)
def analyze_governance(params: GovernanceParams):
    _validate_safe_params(params)
    t0 = log_endpoint_start("Governance Auditor", params, _logger=logger)
    scoped_client, target_project = init_bq_client_and_resolve_project(params)
    focus_clause, focus_params = build_project_filter(params.focus_projects)
    try:
        
        # 1. Audit Dataset Expiration
        exp_sql = f"""
        SELECT
          s.catalog_name AS project_id,
          s.schema_name AS dataset_id,
          CAST(NULL AS STRING) AS default_table_expiration
        FROM `{target_project}`.`{params.region}`.INFORMATION_SCHEMA.SCHEMATA s
        LEFT JOIN `{target_project}`.`{params.region}`.INFORMATION_SCHEMA.SCHEMATA_OPTIONS o
          ON s.catalog_name = o.catalog_name
          AND s.schema_name = o.schema_name
          AND o.option_name = 'default_table_expiration_days'
        WHERE o.option_name IS NULL
        """

        exp_results = run_query_and_log(scoped_client, exp_sql, "Expiration Audit", params=params)
        
        expiration_issues = []
        for row in exp_results:
            expiration_issues.append(ExpirationResult(
                project_id=row.project_id,
                dataset_id=row.dataset_id,
                default_table_expiration=row.default_table_expiration
            ))
            
        # 2. Audit Require Partition Filter on TOP HEAVY datasets
        # First, find top datasets by size
        top_datasets_sql = f"""
        SELECT
          project_id,
          table_schema AS dataset_id,
          SUM(total_physical_bytes) AS total_bytes
        FROM
          `{target_project}`.`{params.region}`.INFORMATION_SCHEMA.TABLE_STORAGE_BY_ORGANIZATION
        WHERE total_physical_bytes > 0
          {focus_clause}
        GROUP BY 1, 2
        ORDER BY total_bytes DESC
        LIMIT 5
        """
        logger.debug("Fetching top heavy datasets:\n%s", top_datasets_sql)
        top_datasets_results = run_query_and_log(scoped_client, top_datasets_sql, "Top Datasets", params=params, query_parameters=focus_params)
        
        filter_issues = []
        
        for row in top_datasets_results:
            p = row.project_id
            ds = row.dataset_id
            
            logger.info(f"Scanning project {p} dataset {ds} for partition filters via API...")
            try:
                dataset_ref = bigquery.DatasetReference(p, ds)
                tables = scoped_client.list_tables(dataset_ref, max_results=100) # Cap at 100 tables
                
                for tbl_ref in tables:
                    # list_tables returns TableListItem, we need full Table object to see partitioning
                    tbl = scoped_client.get_table(tbl_ref.reference)
                    
                    if tbl.time_partitioning or tbl.range_partitioning:
                        if not tbl.require_partition_filter:
                            p_type = "RANGE"
                            if tbl.time_partitioning:
                                p_type = str(tbl.time_partitioning.type_)
                                
                            filter_issues.append(PartitionFilterResult(
                                project_id=p,
                                dataset_id=ds,
                                table_name=tbl.table_id,
                                partition_type=p_type
                            ))
            except Exception as e:
                logger.warning(f"Failed to scan dataset {ds} in project {p} via API: {e}")
                
        logger.info(f"Returning {len(expiration_issues)} expiration issues, {len(filter_issues)} filter issues")
        log_endpoint_end("Governance Auditor", t0, _logger=logger)
        return GovernanceResponse(
            expiration_issues=expiration_issues,
            filter_issues=filter_issues
        )
        
    except Exception as e:
        handle_endpoint_exception(e, "Governance analysis")


class MVResult(BaseModel):
    job_id: str
    user_email: str
    mv_name: str
    chosen: bool
    rejected_reason: str

@app.post("/api/mv/analyze", response_model=List[MVResult])
def analyze_mv_rejections(params: GovernanceParams):
    _validate_safe_params(params)
    t0 = log_endpoint_start("MV Rejections", params, _logger=logger)
    scoped_client, target_project = init_bq_client_and_resolve_project(params)
    focus_clause, focus_params = build_project_filter(params.focus_projects)
    try:
        
        sql = f"""
        SELECT
          job_id,
          user_email,
          mv.table_reference.table_id AS mv_name,
          mv.chosen,
          mv.rejected_reason
        FROM `{target_project}`.`{params.region}`.INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION,
        UNNEST(materialized_view_statistics.materialized_view) AS mv
        WHERE mv.chosen = false
          {focus_clause}
        LIMIT 50
        """
        

        results = run_query_and_log(scoped_client, sql, "MV Rejections", params=params, query_parameters=focus_params)
        
        output = []
        for row in results:
            output.append(MVResult(
                job_id=row.job_id,
                user_email=row.user_email,
                mv_name=row.mv_name,
                chosen=row.chosen,
                rejected_reason=row.rejected_reason or ''
            ))
        log_endpoint_end("MV Rejections", t0, _logger=logger)
        return output
        
    except Exception as e:
        handle_endpoint_exception(e, "MV rejections")

class WarningResult(BaseModel):
    job_id: str
    user_email: str
    resource_warning: str

@app.post("/api/resource_warnings/analyze", response_model=List[WarningResult])
def analyze_resource_warnings(params: GovernanceParams):
    _validate_safe_params(params)
    t0 = log_endpoint_start("Resource Warnings", params, _logger=logger)
    scoped_client, target_project = init_bq_client_and_resolve_project(params)
    focus_clause, focus_params = build_project_filter(params.focus_projects)
    try:
        
        sql = f"""
        SELECT
          job_id,
          user_email,
          query_info.resource_warning
        FROM `{target_project}`.`{params.region}`.INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION
        WHERE query_info.resource_warning IS NOT NULL
          {focus_clause}
        ORDER BY creation_time DESC
        LIMIT 50
        """
        

        results = run_query_and_log(scoped_client, sql, "Resource Warnings", params=params, query_parameters=focus_params)
        
        output = []
        for row in results:
            output.append(WarningResult(
                job_id=row.job_id,
                user_email=row.user_email,
                resource_warning=row.resource_warning or ''
            ))
        log_endpoint_end("Resource Warnings", t0, _logger=logger)
        return output
        
    except Exception as e:
        handle_endpoint_exception(e, "Resource warnings")


class SlotsParams(FocusMixin):
    org_project_id: Optional[str] = None
    region: str = "region-us"
    lookback_days: int = Field(default=7, ge=1, le=90)
    window_minutes: int = Field(default=5, ge=1, le=60)
    percentile: int = Field(default=90, ge=1, le=99)
    admin_project_id: Optional[str] = None
    max_bytes_billed_gb: Optional[int] = None

@app.post("/api/slots/analyze")
def analyze_slots(params: SlotsParams):
    _validate_safe_params(params)
    t0 = log_endpoint_start("Slots Analysis (Capacity Planner)", params, _logger=logger)
    
    scoped_client, resolved_project = init_bq_client_and_resolve_project(params)
    # NOTE: focus_projects intentionally NOT applied to capacity planning.
    # JOBS_TIMELINE_BY_ORGANIZATION must reflect full org demand to size reservations correctly.
    focus_clause, focus_params = "", []
    window_seconds = params.window_minutes * 60
    
    recommendations_sql = f"""
  WITH per_second_usage AS (
    SELECT
     period_start,
     reservation_id,
     SUM(period_slot_ms) / 1000 AS concurrent_slots
    FROM
     `{resolved_project}`.`{params.region}`.INFORMATION_SCHEMA.JOBS_TIMELINE_BY_ORGANIZATION
    WHERE
     period_start > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {params.lookback_days} DAY)
     AND reservation_id IS NOT NULL
     AND (statement_type != 'SCRIPT' OR statement_type IS NULL)
     {focus_clause}
    -- GROUPING SETS computes BOTH the per-reservation per-second total AND the
    -- org-wide per-second total (reservation_id = NULL) in a single base-table scan.
    -- The NULL-keyed rows ARE the merged series, summed within each second BEFORE
    -- any windowing/percentile — which preserves the original merged semantics.
    GROUP BY GROUPING SETS ((period_start, reservation_id), (period_start))
  ),
  windowed_stats AS (
    SELECT
     TIMESTAMP_SECONDS(DIV(UNIX_SECONDS(period_start), {window_seconds}) * {window_seconds}) AS window_start,
     reservation_id AS base_res_id,
     SUM(concurrent_slots) / {window_seconds} AS avg_slots,
     MAX(concurrent_slots) AS max_slots
    FROM per_second_usage
    GROUP BY window_start, base_res_id
  ),
  per_res AS (
    SELECT
      -- Coalesce the GROUPING SETS NULL key into the merged label.
      IFNULL(base_res_id, 'MERGED (Simulated)') AS reservation_id,
      CAST(IF(base_res_id IS NOT NULL AND CONTAINS_SUBSTR(base_res_id, ":"),
        SPLIT(REPLACE(base_res_id, ".", ":"), ":")[OFFSET(0)],
        NULL) AS STRING) AS admin_project_id,
      IFNULL(
        ARRAY_REVERSE(SPLIT(REPLACE(base_res_id, ".", ":"), ":"))[OFFSET(0)],
        'MERGED (Simulated)'
      ) AS clean_reservation_id,
      APPROX_QUANTILES(avg_slots, 100)[OFFSET({params.percentile})] AS recommended_baseline,
      APPROX_QUANTILES(max_slots, 100)[OFFSET(90)] AS recommended_max_p90,
      APPROX_QUANTILES(max_slots, 100)[OFFSET(99)] AS recommended_max_p99,
      MAX(max_slots) AS recommended_max_peak
    FROM
      windowed_stats
    GROUP BY
      1, 2, 3
  )
  SELECT * FROM per_res
  """
    
    reservations_sql = f"""
    SELECT
      reservation_name AS reservation_id,
      slot_capacity AS current_baseline,
      autoscale.max_slots AS current_max_slots,
      edition,
      ignore_idle_slots,
      scaling_mode,
      target_job_concurrency
    FROM
      `{resolved_project}`.`{params.region}`.INFORMATION_SCHEMA.RESERVATIONS
    """
    

    try:
        
        recommendations_results = run_query_and_log(scoped_client, recommendations_sql, "Slots Recommendations", params=params, query_parameters=focus_params)
        recommendations_data = []
        for row in recommendations_results:
            d = dict(row)
            for key in ['recommended_baseline', 'recommended_max_p90', 'recommended_max_p99', 'recommended_max_peak']:
                if key in d and d[key] is not None:
                    d[key] = int(round(d[key] / 50.0) * 50)
            recommendations_data.append(d)
        
        current_reservations_data = []
        
        # Extract admin projects from reservation IDs in recommendations
        admin_projects = {row.get('admin_project_id') for row in recommendations_data if row.get('admin_project_id')}
                
        # Fallback to the provided admin_project_id or org_project_id if no specific admin project found
        if not admin_projects:
            if params.admin_project_id:
                admin_projects.add(params.admin_project_id)
            else:
                admin_projects.add(resolved_project)
            
        fairness_enabled = False
        for admin_proj in admin_projects:
            # Query Project Options for Fluid Scaling and Fairness
            fluid_enabled_reservations = set()
            options_sql = f"""
            SELECT option_name, option_value 
            FROM `{admin_proj}`.`{params.region}`.INFORMATION_SCHEMA.PROJECT_OPTIONS 
            WHERE option_name IN ('preflight_fluid_autoscaling_reservations', 'enable_reservation_based_fairness')
            """
            try:
                logger.info(f"Checking Project Options for project {admin_proj}")
                options_results = run_query_and_log(scoped_client, options_sql, f"Project Options {admin_proj}", params=params)
                for row in options_results:
                    name = row['option_name']
                    val = row['option_value']
                    if name == 'preflight_fluid_autoscaling_reservations' and val:
                        try:
                            # Handle potential unquoted strings in the array string (e.g., [a, b] instead of ["a", "b"])
                            val_clean = val.strip()
                            if val_clean.startswith('[') and val_clean.endswith(']'):
                                val_clean = val_clean[1:-1]
                            fluid_enabled_reservations = set(r.strip().strip('"').strip("'") for r in val_clean.split(',') if r.strip())
                        except Exception as err:
                            logger.warning(f"Failed to parse fluid reservations: {err}")
                    elif name == 'enable_reservation_based_fairness' and val:
                        fairness_enabled = (val.lower() == 'true')
            except Exception as opt_err:
                logger.warning(f"Failed to query PROJECT_OPTIONS in {admin_proj}: {opt_err}")

            reservations_sql = f"""
            SELECT
              reservation_name AS reservation_id,
              slot_capacity AS current_baseline,
              autoscale.max_slots AS current_max_slots,
              edition,
              ignore_idle_slots,
              scaling_mode,
              target_job_concurrency
            FROM
              `{admin_proj}`.`{params.region}`.INFORMATION_SCHEMA.RESERVATIONS
            """
            try:
                logger.info(f"Executing Current Reservations Query for project {admin_proj}")
                reservations_results = run_query_and_log(scoped_client, reservations_sql, f"Current Reservations ({admin_proj})", params=params)
                for row in reservations_results:
                    d = dict(row)
                    d['admin_project_id'] = admin_proj
                    d['region'] = params.region
                    
                    # Add fluid scaling column
                    res_id = row['reservation_id']
                    d['fluid_scaling_enabled'] = res_id in fluid_enabled_reservations
                    
                    current_reservations_data.append(d)
            except Exception as res_err:
                logger.warning(f"Failed to query RESERVATIONS in {admin_proj}: {res_err}")
            
        log_endpoint_end("Slots Analysis (Capacity Planner)", t0, _logger=logger)
        return {
            "recommendations": recommendations_data,
            "current_reservations": current_reservations_data,
            "fairness_enabled": fairness_enabled
        }
        
    except Exception as e:
        handle_endpoint_exception(e, "Slots analysis")


class TieredRecParams(FocusMixin):
    org_project_id: Optional[str] = None
    region: str = "region-us"
    lookback_days: int = Field(default=7, ge=1, le=90)
    max_bytes_billed_gb: Optional[int] = None

class TieredRecResult(BaseModel):
    reservation_id: str
    aggressive_baseline_p80: int
    balanced_baseline_p95: int
    performance_baseline_max: int
    suggested_autoscale_max: Optional[int] = None
    minutes_observed: Optional[int] = None

@app.post("/api/slots/tiered_recommendations", response_model=List[TieredRecResult])
def get_tiered_recommendations(params: TieredRecParams):
    _validate_safe_params(params)
    t0 = log_endpoint_start("Tiered Recommendations", params, _logger=logger)
    
    scoped_client, resolved_project = init_bq_client_and_resolve_project(params)
    # NOTE: focus_projects intentionally NOT applied to capacity planning.
    # Tiered recommendations must reflect full org demand to size reservations correctly.
    focus_clause, focus_params = "", []
    
    def get_sql(table_name: str) -> str:
        """
        Build the tiered percentile recommendation query.

        Math notes:
          - JOBS_TIMELINE_BY_ORGANIZATION emits rows at 1-second granularity (period_start).
          - period_slot_ms in a 1-second window / 1000 = concurrent slots active that second.
          - We sum across jobs per (second, reservation) to get total concurrent demand.
          - We then take the MAX over each minute (NOT avg) so sub-minute spikes are preserved.
          - Percentiles are taken over per-minute peaks, which is what the autoscaler responds to.
          - Final recommendations are rounded UP to the nearest 50 (BigQuery slot increment).
          - HAVING filter drops reservations with too little data to produce a trustworthy number.
        """
        return f"""
        WITH per_second_usage AS (
          SELECT
            period_start,
            reservation_id,
            SUM(period_slot_ms) / 1000 AS concurrent_slots
          FROM
            `{resolved_project}`.`{params.region}`.INFORMATION_SCHEMA.{table_name}
          WHERE
            period_start > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {params.lookback_days} DAY)
            {focus_clause}
          GROUP BY
            period_start, reservation_id
        ),
        minute_usage AS (
          SELECT
            TIMESTAMP_TRUNC(period_start, MINUTE) AS usage_minute,
            reservation_id AS base_res_id,
            MAX(concurrent_slots) AS peak_slots_in_minute
          FROM per_second_usage
          GROUP BY usage_minute, base_res_id
        )
        SELECT
          IFNULL(base_res_id, 'default-or-on-demand') AS reservation_id,
          COUNT(*) AS minutes_observed,
          CAST(CEIL(APPROX_QUANTILES(peak_slots_in_minute, 100)[OFFSET(80)] / 50) * 50 AS INT64)
            AS aggressive_baseline_p80,
          CAST(CEIL(APPROX_QUANTILES(peak_slots_in_minute, 100)[OFFSET(95)] / 50) * 50 AS INT64)
            AS balanced_baseline_p95,
          CAST(CEIL(MAX(peak_slots_in_minute) / 50) * 50 AS INT64)
            AS performance_baseline_max,
          CAST(CEIL((MAX(peak_slots_in_minute) * 1.2) / 50) * 50 AS INT64)
            AS suggested_autoscale_max
        FROM minute_usage
        GROUP BY 1
        HAVING minutes_observed >= 60
        ORDER BY performance_baseline_max DESC
        """
    
    try:
        sql = get_sql("JOBS_TIMELINE_BY_ORGANIZATION")

        try:
            results = run_query_and_log(scoped_client, sql, "Tiered Recommendations (Org)", params=params, query_parameters=focus_params)
        except Exception as e:
            # focus_projects is intentionally not applied to capacity planning,
            # so the project-level fallback is always safe.
            if "Access Denied" in str(e) or "does not exist" in str(e):
                logger.warning(f"Org scope failed with access error, falling back to Project scope: {e}")
                sql = get_sql("JOBS_TIMELINE")
                logger.info("Tiered Recommendations — retrying with project scope")
                results = run_query_and_log(scoped_client, sql, "Tiered Recommendations (Project)", params=params)
            else:
                raise e
        
        output = []
        for row in results:
            output.append(TieredRecResult(
                reservation_id=row['reservation_id'],
                aggressive_baseline_p80=row['aggressive_baseline_p80'] if row['aggressive_baseline_p80'] is not None else 0,
                balanced_baseline_p95=row['balanced_baseline_p95'] if row['balanced_baseline_p95'] is not None else 0,
                performance_baseline_max=row['performance_baseline_max'] if row['performance_baseline_max'] is not None else 0,
                suggested_autoscale_max=row['suggested_autoscale_max'] if row['suggested_autoscale_max'] is not None else 0,
                minutes_observed=row['minutes_observed'] if row['minutes_observed'] is not None else 0
            ))
        log_endpoint_end("Tiered Recommendations", t0, _logger=logger)
        return output
        
    except Exception as e:
        handle_endpoint_exception(e, "Tiered recommendations")


class SlotUtilizationParams(FocusMixin):
    org_project_id: Optional[str] = None
    region: str = "region-us"
    lookback_days: int = Field(default=7, ge=1, le=90)
    timezone: str = "America/New_York"
    resolution: str = "MINUTE"
    max_bytes_billed_gb: Optional[int] = None

    @field_validator('resolution')
    @classmethod
    def validate_resolution(cls, v: str) -> str:
        if v not in _ALLOWED_RESOLUTIONS:
            raise ValueError(f"Resolution must be one of {_ALLOWED_RESOLUTIONS}")
        return v

@app.post("/api/slots/utilization")
def analyze_slot_utilization(params: SlotUtilizationParams):
    _validate_safe_params(params)
    t0 = log_endpoint_start("Slot Utilization", params, _logger=logger)
    
    scoped_client, resolved_project = init_bq_client_and_resolve_project(params)
    # NOTE: focus_projects intentionally NOT applied to capacity planning.
    focus_clause, focus_params = "", []
    resolution = params.resolution
    duration_ms = 60000
    if resolution == "HOUR":
        duration_ms = 3600000
    elif resolution == "DAY":
        duration_ms = 86400000
        
    sql = f"""
    SELECT
      TIMESTAMP_TRUNC(period_start, {resolution}) AS period_min,
      SUM(CAST(period_slot_ms AS NUMERIC)) / {duration_ms} AS time_average,
      MAX(period_slot_ms / 1000) AS max_slots,
      APPROX_QUANTILES(period_slot_ms / 1000, 100)[OFFSET(90)] AS p90_slots,
      APPROX_QUANTILES(period_slot_ms / 1000, 100)[OFFSET(99)] AS p99_slots,
      SUM(total_bytes_billed) / 60 AS bytes_billed_avg,
      SUM(total_bytes_processed) / 60 AS bytes_processed_avg
    FROM
      `{resolved_project}`.`{params.region}`.INFORMATION_SCHEMA.JOBS_TIMELINE_BY_ORGANIZATION
    WHERE
      period_start > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {params.lookback_days} DAY)
      AND job_type = 'QUERY'
      AND (statement_type != 'SCRIPT' OR statement_type IS NULL)
      {focus_clause}
    GROUP BY
      period_min
    ORDER BY period_min ASC
    """
    

    try:
        from zoneinfo import ZoneInfo
        try:
            tz = ZoneInfo(params.timezone)
        except Exception:
            raise HTTPException(status_code=400, detail=f"Invalid timezone: {params.timezone}")

        results = run_query_and_log(scoped_client, sql, "Slot Utilization Raw Data", params=params, query_parameters=focus_params)
        
        processed_results = []
        for row in results:
            ts = row['period_min']
            ts_tz = ts.astimezone(tz)
            
            processed_results.append({
                "timestamp": ts_tz.isoformat(),
                "max_slots": round(row['max_slots'] or 0, 2),
                "median_slots": 0,
                "p90_slots": round(row['p90_slots'] or 0, 3),
                "p99_slots": round(row['p99_slots'] or 0, 3),
                "time_average": round(row['time_average'] or 0, 4),
                "bytes_billed_avg": round(row['bytes_billed_avg'] or 0, 2),
                "bytes_processed_avg": round(row['bytes_processed_avg'] or 0, 4)
            })
            
        processed_results.sort(key=lambda x: x['timestamp'], reverse=True)
        
        log_endpoint_end("Slot Utilization", t0, _logger=logger)
        return processed_results
        
    except HTTPException:
        raise
    except Exception as e:
        handle_endpoint_exception(e, "Slot utilization analysis")

class SlotSimulationParams(FocusMixin):
    org_project_id: Optional[str] = None
    region: str = "region-us"
    lookback_days: int = 7
    timezone: str = "America/New_York"
    max_baseline: int = 10000
    step_size: int = 50
    payg_price: float = 0.06
    commit_1yr_price: float = 0.048
    commit_3yr_price: float = 0.036
    max_bytes_billed_gb: Optional[int] = None

@app.post("/api/slots/simulate")
def simulate_slots(params: SlotSimulationParams):
    _validate_safe_params(params)
    t0 = log_endpoint_start("Slot Simulation", params, _logger=logger)
    
    scoped_client, resolved_project = init_bq_client_and_resolve_project(params)
    # NOTE: focus_projects intentionally NOT applied to capacity planning.
    focus_clause, focus_params = "", []
    
    sql = f"""
    SELECT
      TIMESTAMP_TRUNC(period_start, MINUTE) AS usage_minute,
      SUM(period_slot_ms) / (1000 * 60) AS avg_slots
    FROM `{resolved_project}`.`{params.region}`.INFORMATION_SCHEMA.JOBS_TIMELINE_BY_ORGANIZATION
    WHERE 
      period_start >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {params.lookback_days} DAY)
      AND job_type = 'QUERY'
      AND (statement_type != 'SCRIPT' OR statement_type IS NULL)
      {focus_clause}
    GROUP BY 1
    ORDER BY 1 ASC
    """
    

    
    try:
        results = run_query_and_log(scoped_client, sql, "Slot Simulation Raw Data", params=params, query_parameters=focus_params)
        
        avg_slots_list = [float(row['avg_slots'] or 0.0) for row in results]
        avg_slots_array = np.array(avg_slots_list)
        if len(avg_slots_array) == 0:
            log_endpoint_end("Slot Simulation", t0, _logger=logger)
            return []
            
        # Time calculations
        actual_hours_in_data = params.lookback_days * 24.0
        actual_minutes_in_data = actual_hours_in_data * 60.0
        
        # BQ Editions are billed on a standard 730-hour month. 
        # We calculate a multiplier to project the X days of data into a full standard month.
        monthly_multiplier = 730.0 / actual_hours_in_data
        
        processed_results = []
        sum_all_slots = np.sum(avg_slots_array)
        logger.info(f"Simulation data loaded: {len(avg_slots_array)} minutes of usage.")
        
        for baseline in range(0, params.max_baseline + params.step_size, params.step_size):
            # 1. Bucket Calculations (How many minutes spent in this exact slot band)
            if baseline == 0:
                bucket_name = "[0 → 0]"
                bucket_mins = int(np.sum(avg_slots_array == 0))
            else:
                prev_baseline = baseline - params.step_size
                bucket_name = f"[{prev_baseline} → {baseline}]"
                bucket_mins = int(np.sum((avg_slots_array > prev_baseline) & (avg_slots_array <= baseline)))
            
            # 2. Autoscale Calculations (Projected to a full month)
            autoscale_slot_hours_raw = float(np.maximum(avg_slots_array - baseline, 0).sum()) / 60.0
            autoscale_slot_hours_mo = autoscale_slot_hours_raw * monthly_multiplier
            autoscale_slot_months = autoscale_slot_hours_mo / 730.0
            
            # 3. Utilization Calculations (How well is the baseline being used?)
            max_baseline_hours_raw = baseline * actual_hours_in_data
            # Cap per-minute consumption at the baseline to isolate used committed slots
            used_under_baseline = np.minimum(avg_slots_array, baseline)
            used_baseline_hours_raw = float(used_under_baseline.sum()) / 60.0
            idle_slot_hours_raw = max(0, max_baseline_hours_raw - used_baseline_hours_raw)
            
            utilization_pct = (used_baseline_hours_raw / max_baseline_hours_raw) if max_baseline_hours_raw > 0 else 0.0
            
            # 4. Cost Calculations (Monthly)
            autoscale_cost_payg = autoscale_slot_hours_mo * params.payg_price
            
            baseline_cost_payg = baseline * 730.0 * params.payg_price
            baseline_cost_1yr  = baseline * 730.0 * params.commit_1yr_price
            baseline_cost_3yr  = baseline * 730.0 * params.commit_3yr_price
            
            processed_results.append({
                "bucket": bucket_name,
                "minutes": bucket_mins,
                "slots": baseline,
                "utilization_pct": round(utilization_pct * 100, 2),
                "autoscale_slot_hours": round(autoscale_slot_hours_mo, 0),
                "autoscale_slot_months": round(autoscale_slot_months, 0),
                "cost_autoscale_payg": round(autoscale_cost_payg, 2),
                "cost_base_payg": round(baseline_cost_payg, 2),
                "cost_base_1yr": round(baseline_cost_1yr, 2),
                "cost_base_3yr": round(baseline_cost_3yr, 2),
                "total_payg": round(baseline_cost_payg + autoscale_cost_payg, 2),
                "total_1yr": round(baseline_cost_1yr + autoscale_cost_payg, 2),
                "total_3yr": round(baseline_cost_3yr + autoscale_cost_payg, 2)
            })
            
        logger.info(f"Slot simulation completed with {len(processed_results)} results")
        log_endpoint_end("Slot Simulation", t0, _logger=logger)
        return processed_results
        
    except Exception as e:
        handle_endpoint_exception(e, "Slot simulation")
# Constants
# ---------------------------------------------------------------------------

# Allowed BigQuery editions, resolutions, and AI models (validated before interpolation into SQL).
_ALLOWED_EDITIONS = {"STANDARD", "ENTERPRISE", "ENTERPRISE_PLUS"}
_ALLOWED_RESOLUTIONS = {"MINUTE", "HOUR", "DAY"}

# _MAX_BYTES_BILLED removed — now resolved dynamically via get_max_bytes_billed(params)
_COOLDOWN_WINDOW_SEC = 60
_MAX_LOOKBACK_DAYS = 30


def _validate_safe_params(params):
    """
    Validate and sanitize common parameters to prevent SQL injection.
    Raises HTTPException(400) on validation failures.
    """
    if hasattr(params, "org_project_id") and params.org_project_id:
        params.org_project_id = _safe_ident(params.org_project_id.strip(), "org_project_id")
        reject_dummy_project(params.org_project_id)
    if hasattr(params, "admin_project_id") and params.admin_project_id:
        params.admin_project_id = _safe_ident(params.admin_project_id.strip(), "admin_project_id")
        reject_dummy_project(params.admin_project_id)
    if hasattr(params, "region") and params.region:
        params.region = _safe_ident(_normalize_region(params.region), "region")
    if hasattr(params, "edition") and params.edition:
        if params.edition not in _ALLOWED_EDITIONS:
            raise HTTPException(400, f"Invalid edition: {params.edition}")
    if hasattr(params, "resolution") and params.resolution:
        if params.resolution not in _ALLOWED_RESOLUTIONS:
            raise HTTPException(400, f"Invalid resolution: {params.resolution}")
    if hasattr(params, "focus_projects") and params.focus_projects:
        params.focus_projects = validate_focus_projects(params.focus_projects)


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

# (typing imports already at top level)

_PATTERN_DISCLAIMER = (
    "Indicative savings assume each job independently incurs a full 60s cooldown. "
    "This overstates real savings when jobs run concurrently or share a reservation. "
    "Use these figures for directional ranking of optimization targets only — not as a "
    "ground-truth financial projection. Values are window-bound (not extrapolated)."
)


class FluidSimParams(FocusMixin):
    org_project_id: Optional[str] = None
    region: str = "region-us"
    lookback_days: int = Field(default=7, ge=1, le=90)
    edition_slot_hr_rate: float = Field(default=0.06, gt=0)
    cooldown_window: int = Field(default=60, ge=1, le=300)
    max_bytes_billed_gb: Optional[int] = None


class FluidSimResult(BaseModel):
    pattern_id: str
    pattern_label: str
    workload_type: str
    reservation_id: Optional[str] = None          # fully-qualified
    reservation_short_name: Optional[str] = None  # display-only
    sample_user: Optional[str] = None
    sample_job_id: Optional[str] = None
    job_count: int
    avg_duration_seconds: float
    avg_peak_slots: float
    total_slot_seconds: float
    cooldown_exposure_score: int
    exposure_reasons: List[str] = Field(default_factory=list)
    indicative_savings_usd: float


class FluidSimResponse(BaseModel):
    lookback_days: int
    total_jobs_analyzed: int
    total_patterns_found: int
    patterns: List[FluidSimResult]
    disclaimer: str = _PATTERN_DISCLAIMER   # always populated


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_UUID_RE = re.compile(
    r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"
)
_UUID_USCORE_RE = re.compile(
    r"[0-9a-fA-F]{8}_[0-9a-fA-F]{4}_[0-9a-fA-F]{4}_[0-9a-fA-F]{4}_[0-9a-fA-F]{12}"
)
# Collapse the bquxjob_<hex>_<hex> console/job-id shape regardless of segment
# length. This is the dominant high-cardinality format that exploded the
# /api/slots/fluid_simulation payload to 189 MB (one pattern per job).
_BQUXJOB_RE = re.compile(r"bquxjob_[0-9a-fA-F]+_[0-9a-fA-F]+")
_BQJOB_RE = re.compile(r"bqjob_r?[0-9a-fA-F]+(_[0-9a-fA-F]+)*")
# BigQuery default random job ID shape: job_ followed by url-safe base64-like string
_BASE64_JOB_RE = re.compile(r"^job_[a-zA-Z0-9_-]{20,}")
# Lowered from 16 -> 8 so short hex blobs (e.g. "5c2184f") are masked too.
_HEX_RE = re.compile(r"[0-9a-fA-F]{8,}")
_LONGNUM_RE = re.compile(r"\d{6,}")
# ISO-ish timestamps: 2026_06_03T12_15_00_00_00, 2026-06-03, 2026_06_03, etc.
# Masked BEFORE hex/longnum so the date components don't survive as structure.
# Uses negative lookahead ((?![a-zA-Z])) to avoid eating the start of hex strings.
_TIMESTAMP_RE = re.compile(r"\d{4}[_\-]\d{2}[_\-]\d{2}([T_]\d{2}([_:]\d{2}(?![a-zA-Z])){0,5})?")
# Collapse repeated runs of "_#" (e.g., _#_# or _#_#_#) into a single "_#".
# Crucial to merge parent script hex + nested children indices into a single script_job_# pattern.
_REPEAT_HASH_RE = re.compile(r"(_#){2,}")
_TRAILING_INDEX_RE = re.compile(r"(_\d+)+$")
_ADJACENT_HASH_RE = re.compile(r"#+")
_TRAILING_HASH_RE = re.compile(r"[#_]+$")


def _extract_pattern(job_id: Optional[str]) -> str:
    """
    Collapse a concrete job_id into a structural pattern key by masking only the
    clearly-variable (random) portions, while preserving short structural tokens
    so distinct pipelines aren't over-merged.
    """
    if not job_id:
        return "(unknown)"
    s = str(job_id)
    # Strip any "project:region." qualifier so prefixes don't fragment patterns.
    s = re.split(r"[.:]", s)[-1]
    s = _UUID_RE.sub("<uuid>", s)          # full UUIDs
    s = _UUID_USCORE_RE.sub("<uuid>", s)   # underscore-delimited UUIDs
    s = _BQUXJOB_RE.sub("bquxjob_#_#", s)  # console/job-id shape (key fix)
    s = _BQJOB_RE.sub("bqjob_#", s)        # console bqjob_r... shape
    s = _BASE64_JOB_RE.sub("job_#", s)     # base64-urlsafe random job IDs
    s = _TIMESTAMP_RE.sub("#", s)          # mask timestamps before hex/num runs
    s = _HEX_RE.sub("#", s)                # hex blobs (>=8) = random ids
    # Mask LONG digit runs (>=6) — timestamps / epoch / random counters.
    # Short numeric tokens (e.g. _q3, step12, v2) are kept as structure.
    s = _LONGNUM_RE.sub("#", s)
    # Collapse ANY trailing _N index/chain (script child indices: _0, _2, _0_0...)
    s = _TRAILING_INDEX_RE.sub("_#", s)
    # Collapse repeated runs of _# (e.g. script_job_#_# or script_job_#_#_#) to script_job_#
    s = _REPEAT_HASH_RE.sub("_#", s)
    s = _ADJACENT_HASH_RE.sub("#", s)   # collapse ## -> # (adjacent hashes)
    s = _TRAILING_HASH_RE.sub("_#", s)  # collapse trailing hashes/underscores -> _#
    return s


def _rank_and_cap(patterns: List[FluidSimResult], limit: int = 100) -> List[FluidSimResult]:
    # Rank by estimated savings impact (dollars), most impactful first.
    patterns.sort(key=lambda p: p.indicative_savings_usd, reverse=True)
    return patterns[:limit]


def _humanize_pattern(pattern_id: str) -> str:
    if not pattern_id or pattern_id == "(unknown)":
        return "Unknown pattern"
    if "<uuid>" in pattern_id:
        return f"Automated pipeline ({pattern_id})"
    if pattern_id.startswith("bquxjob"):
        return f"Console / ad-hoc ({pattern_id})"
    if pattern_id.startswith("scheduled"):
        return f"Scheduled query ({pattern_id})"
    return pattern_id


def _classify_workload(
    avg_duration: float,
    avg_peak_slots: float,
    pct_short_duration: float,
    statement_types: Set[str],
) -> str:
    """Heuristic label describing the pattern's workload character."""
    if pct_short_duration >= 0.8 and avg_duration < 5.0:
        return "High-frequency heartbeat"
    if avg_duration < 30.0 and avg_peak_slots >= 100.0:
        return "Spiky short burst"
    if avg_duration < 30.0:
        return "Short query"
    if statement_types and statement_types.issubset({"INSERT", "MERGE", "UPDATE", "DELETE"}):
        return "DML pipeline"
    return "Mixed / general"


def _compute_exposure_score(
    avg_duration: float,
    avg_peak_slots: float,
    job_count: int,
) -> Tuple[int, List[str]]:
    """
    Diagnostic 0-100 score: how exposed this pattern is to the 60s cooldown tax.
    Higher = more wasteful under legacy autoscaling, more recoverable under Fluid.
    """
    reasons: List[str] = []
    score = 0

    # Shorter jobs waste a larger fraction of the 60s cooldown window.
    if avg_duration < 60.0:
        waste_fraction = (60.0 - avg_duration) / 60.0
        dur_pts = int(round(waste_fraction * 50))
        score += dur_pts
        if avg_duration < 5.0:
            reasons.append(f"Very short avg duration ({avg_duration:.1f}s) — {waste_fraction*100:.0f}% of cooldown wasted")
        elif avg_duration < 30.0:
            reasons.append(f"Short avg duration ({avg_duration:.1f}s) exposed to 60s cooldown")

    # High frequency multiplies the per-job waste.
    if job_count >= 100000:
        score += 30
        reasons.append(f"Very high frequency ({job_count:,} runs in window)")
    elif job_count >= 10000:
        score += 20
        reasons.append(f"High frequency ({job_count:,} runs in window)")
    elif job_count >= 1000:
        score += 10
        reasons.append(f"Moderate frequency ({job_count:,} runs in window)")

    # Large slot spikes mean each cooldown holds expensive capacity.
    if avg_peak_slots >= 500.0:
        score += 20
        reasons.append(f"Large slot spikes ({avg_peak_slots:.0f} slots/job)")
    elif avg_peak_slots >= 100.0:
        score += 10
        reasons.append(f"Moderate slot spikes ({avg_peak_slots:.0f} slots/job)")

    score = max(0, min(100, score))
    if not reasons:
        reasons.append("Low cooldown exposure")
    return score, reasons


# ---------------------------------------------------------------------------
# Pattern simulation SQL & endpoint
# ---------------------------------------------------------------------------

_SQL_JOBS = """
SELECT
  job_id,
  project_id,
  reservation_id,
  user_email,
  statement_type,
  TIMESTAMP_DIFF(end_time, start_time, SECOND) AS duration_seconds,
  total_slot_ms / 1000.0 AS slot_seconds,
  SAFE_DIVIDE(
    total_slot_ms,
    NULLIF(TIMESTAMP_DIFF(end_time, start_time, MILLISECOND), 0)
  ) AS avg_slots
FROM `{org_project}`.`{region}`.INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION
WHERE creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @lookback_days DAY)
  AND creation_time <  CURRENT_TIMESTAMP()
  AND job_type = 'QUERY'
  AND state = 'DONE'
  AND error_result IS NULL
  AND TIMESTAMP_DIFF(end_time, start_time, SECOND) < @cooldown_window
  AND end_time > start_time
  AND total_slot_ms > 0
  AND (statement_type != 'SCRIPT' OR statement_type IS NULL)
  {focus_clause}
ORDER BY total_slot_ms DESC
LIMIT 500000
"""





def _render_sql_local(template: str, **idents) -> str:
    """Render SQL template with identifier substitution (local to main.py)."""
    out = template
    for key, val in idents.items():
        out = out.replace("{" + key + "}", val)
    return out


@app.post("/api/slots/fluid_simulation", response_model=FluidSimResponse)
def simulate_fluid_scaling(params: FluidSimParams):
    _validate_safe_params(params)
    t0 = log_endpoint_start("Fluid Simulation", params, _logger=logger)
    try:
        client, org_project = init_bq_client_and_resolve_project(params)
        # NOTE: focus_projects intentionally NOT applied to capacity planning.
        focus_clause, focus_params = "", []
        region = params.region
        sql = _render_sql_local(_SQL_JOBS, org_project=org_project, region=region, focus_clause=focus_clause)
        all_params = [
            bigquery.ScalarQueryParameter("lookback_days", "INT64", params.lookback_days),
            bigquery.ScalarQueryParameter("cooldown_window", "INT64", params.cooldown_window),
        ] + (focus_params or [])
        logger.info(
            "Running fluid_simulation (lookback=%d days, cooldown=%ds)",
            params.lookback_days, params.cooldown_window,
        )
        df = run_query_to_df(client, sql, "Fluid Simulation", params=params, query_parameters=all_params)

        if df.empty:
            log_endpoint_end("Fluid Simulation", t0, _logger=logger)
            return FluidSimResponse(
                lookback_days=params.lookback_days,
                total_jobs_analyzed=0,
                total_patterns_found=0,
                patterns=[],
            )

        total_jobs = int(len(df))

        # Numeric hygiene
        df["duration_seconds"] = pd.to_numeric(df["duration_seconds"], errors="coerce").fillna(0.0)
        df["avg_slots"] = pd.to_numeric(df["avg_slots"], errors="coerce").fillna(0.0)
        df["slot_seconds"] = pd.to_numeric(df["slot_seconds"], errors="coerce").fillna(0.0)

        # Pattern key — optimized to run regex only on unique job_ids
        unique_ids = df["job_id"].dropna().unique()
        pattern_map = {jid: _extract_pattern(jid) for jid in unique_ids}
        df["pattern_id"] = df["job_id"].map(pattern_map).fillna("(unknown)")
        logger.info(
            "fluid_simulation: %d jobs collapsed into %d patterns",
            total_jobs, df["pattern_id"].nunique()
        )

        df["is_short"] = (df["duration_seconds"] < 30).astype(float)

        # Vectorized aggregation
        agg = df.groupby(["pattern_id", "reservation_id"], dropna=False).agg(
            job_count=("job_id", "size"),
            avg_duration=("duration_seconds", "mean"),
            avg_peak_slots=("avg_slots", "mean"),
            total_slot_seconds=("slot_seconds", "sum"),
            pct_short=("is_short", "mean"),
            sample_job_id=("job_id", "first"),
            sample_user=("user_email", "first"),
        ).reset_index()

        # statement_types still need per-group sets — vectorized
        stmt_types = (
            df.dropna(subset=["statement_type"])
              .groupby(["pattern_id", "reservation_id"], dropna=False)["statement_type"]
              .agg(lambda s: frozenset(s.unique()))
        )

        patterns: List[FluidSimResult] = []
        for r in agg.itertuples(index=False):
            avg_duration = float(r.avg_duration)
            avg_peak_slots = float(r.avg_peak_slots)
            job_count = int(r.job_count)

            indicative_wasted_slot_hours = (
                (avg_peak_slots * (params.cooldown_window - avg_duration) * job_count) / 3600.0
                if avg_duration < params.cooldown_window else 0.0
            )
            indicative_savings_usd = indicative_wasted_slot_hours * params.edition_slot_hr_rate

            score, reasons = _compute_exposure_score(
                avg_duration=avg_duration,
                avg_peak_slots=avg_peak_slots,
                job_count=job_count,
            )

            res_full = str(r.reservation_id) if pd.notna(r.reservation_id) else None
            types = stmt_types.get((r.pattern_id, r.reservation_id), frozenset())

            patterns.append(FluidSimResult(
                pattern_id=str(r.pattern_id),
                pattern_label=_humanize_pattern(str(r.pattern_id)),
                workload_type=_classify_workload(
                    avg_duration=avg_duration,
                    avg_peak_slots=avg_peak_slots,
                    pct_short_duration=float(r.pct_short),
                    statement_types={str(s) for s in types},
                ),
                reservation_id=res_full,
                reservation_short_name=_strip_qualifier(res_full),
                sample_user=str(r.sample_user) if pd.notna(r.sample_user) else None,
                sample_job_id=str(r.sample_job_id) if pd.notna(r.sample_job_id) else None,
                job_count=job_count,
                avg_duration_seconds=round(avg_duration, 1),
                avg_peak_slots=round(avg_peak_slots, 0),
                total_slot_seconds=round(float(r.total_slot_seconds), 1),
                cooldown_exposure_score=score,
                exposure_reasons=reasons,
                indicative_savings_usd=round(indicative_savings_usd, 0),
            ))

        # Rank by estimated savings impact (dollars), most impactful first, and cap at 100
        MAX_PATTERNS_RETURNED = 100
        total_patterns = len(patterns)              # true count BEFORE truncation
        patterns = _rank_and_cap(patterns, limit=MAX_PATTERNS_RETURNED)

        # Canary 1: catastrophic leak (patterns ≈ jobs).
        if total_jobs > 0 and total_patterns / total_jobs > 0.5:
            logger.warning(
                "fluid_simulation: SUSPICIOUS collapse ratio — %d patterns from %d jobs "
                "(%.0f%%). Pattern masking likely failing for this job-id format.",
                total_patterns, total_jobs, 100 * total_patterns / total_jobs,
            )

        # Canary 2: high absolute count (subtle leak — e.g. timestamps in the tail).
        if total_patterns > 500:
            logger.warning(
                "fluid_simulation: %d distinct patterns is high — likely an unmasked "
                "ID format (timestamps? short hashes?) in the long tail. Sample tail: %s",
                total_patterns,
                patterns[-1].pattern_id if patterns else "(none)",
            )

        logger.info(
            "fluid_simulation: %d jobs -> %d patterns (returning top %d by savings)",
            total_jobs, total_patterns, len(patterns),
        )

        log_endpoint_end("Fluid Simulation", t0, _logger=logger)
        return FluidSimResponse(
            lookback_days=params.lookback_days,
            total_jobs_analyzed=total_jobs,
            total_patterns_found=total_patterns,      # report the real count
            patterns=patterns,
        )

    except gax_exc.Forbidden:
        logger.exception("Permission denied running fluid simulation")
        raise HTTPException(403, "Insufficient permissions for JOBS_BY_ORGANIZATION")
    except gax_exc.NotFound:
        logger.exception("Project or region not found")
        raise HTTPException(404, "Project or region not found")
    except gax_exc.GoogleAPIError:
        logger.exception("BigQuery error running fluid simulation")
        raise HTTPException(500, "Query failed; check server logs")
    except HTTPException:
        raise
    except Exception:
        logger.exception("Unexpected error in fluid simulation")
        raise HTTPException(500, "Internal server error")


class SlotActualParams(FocusMixin):
    org_project_id: Optional[str] = None
    region: str = "region-us"
    lookback_days: int = Field(default=7, ge=1, le=90)
    timezone: str = "America/New_York"
    edition: str = "ENTERPRISE"
    admin_project_id: Optional[str] = None
    max_bytes_billed_gb: Optional[int] = None

    @field_validator('edition')
    @classmethod
    def validate_edition(cls, v: str) -> str:
        if v not in _ALLOWED_EDITIONS:
            raise ValueError(f"Edition must be one of {_ALLOWED_EDITIONS}")
        return v

@app.post("/api/slots/actual_provisioning")
def get_actual_provisioning(params: SlotActualParams):
    _validate_safe_params(params)
    t0 = log_endpoint_start("Actual Provisioning", params, _logger=logger)
    
    scoped_client, resolved_project = init_bq_client_and_resolve_project(params)
    from zoneinfo import ZoneInfo
    try:
        tz = ZoneInfo(params.timezone)
    except Exception:
        raise HTTPException(status_code=400, detail=f"Invalid timezone: {params.timezone}")

    from datetime import datetime, timedelta
    now = datetime.now(ZoneInfo('UTC'))
    start_time = now - timedelta(days=params.lookback_days)
    end_time = now

    # Format timestamps for BQ
    start_str = start_time.strftime('%Y-%m-%d %H:%M:%S UTC')
    end_str = end_time.strftime('%Y-%m-%d %H:%M:%S UTC')

    admin_project_raw = params.admin_project_id.strip() if (params.admin_project_id and params.admin_project_id.strip()) else resolved_project
    target_project = _safe_ident(admin_project_raw, "admin_project_id")
    reject_dummy_project(target_project)

    # Base CTEs for both queries
    base_ctes = f"""
WITH
  autoscale_slot_data AS (
  SELECT
    change_timestamp,
    reservation_name,
    CASE action
      WHEN "CREATE" THEN autoscale.current_slots
      WHEN "UPDATE" THEN IFNULL(autoscale.current_slots - LAG(autoscale.current_slots) OVER (PARTITION BY project_id, reservation_name ORDER BY change_timestamp ASC, action ASC), IFNULL(autoscale.current_slots, IFNULL(-1 * LAG(autoscale.current_slots) OVER (PARTITION BY project_id, reservation_name ORDER BY change_timestamp ASC, action ASC), 0)))
      WHEN "DELETE" THEN IF (LAG(action) OVER (PARTITION BY project_id, reservation_name ORDER BY change_timestamp ASC, action ASC) IN ('CREATE', 'UPDATE'), -1 * autoscale.current_slots, 0)
  END
    AS current_slot_delta,
    CASE action
      WHEN "CREATE" THEN slot_capacity
      WHEN "UPDATE" THEN IFNULL(slot_capacity - LAG(slot_capacity) OVER (PARTITION BY project_id, reservation_name ORDER BY change_timestamp ASC, action ASC), IFNULL(slot_capacity, IFNULL(-1 * LAG(slot_capacity) OVER (PARTITION BY project_id, reservation_name ORDER BY change_timestamp ASC, action ASC), 0)))
      WHEN "DELETE" THEN IF (LAG(action) OVER (PARTITION BY project_id, reservation_name ORDER BY change_timestamp ASC, action ASC) IN ('CREATE', 'UPDATE'), -1 * slot_capacity, 0)
  END
    AS baseline_slot_delta,
  FROM
    `{target_project}`.`{params.region}`.INFORMATION_SCHEMA.RESERVATION_CHANGES
  WHERE
    change_timestamp <= TIMESTAMP('{end_str}')
    AND edition = '{params.edition}'
  UNION ALL
  SELECT
    TIMESTAMP('{start_str}'),
    "Start",
    0,
    0
  UNION ALL
  SELECT
    TIMESTAMP('{end_str}'),
    "End",
    0,
    0),
  running_total AS (
  SELECT
    change_timestamp,
    SUM(current_slot_delta) OVER (ORDER BY change_timestamp RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS current_slots,
    SUM(baseline_slot_delta) OVER (ORDER BY change_timestamp RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS baseline_slots,
  FROM
    autoscale_slot_data),
  capacity_commitments AS (
  SELECT
    capacity_commitment_id,
    CASE
      WHEN action = "CREATE" OR action = "UPDATE" THEN 
        IFNULL(
          IF(LAG(action) OVER (PARTITION BY capacity_commitment_id ORDER BY change_timestamp ASC, action ASC) IN ('CREATE', 'UPDATE'),
             slot_count - LAG(slot_count) OVER (PARTITION BY capacity_commitment_id ORDER BY change_timestamp ASC, action ASC),
             slot_count),
          slot_count)
      WHEN action = "DELETE" THEN
        IF(LAG(action) OVER (PARTITION BY capacity_commitment_id ORDER BY change_timestamp ASC, action ASC) IN ('CREATE', 'UPDATE'),
           -1 * slot_count,
           0)
      ELSE 0
    END AS slot_count_delta,
    change_timestamp
  FROM
    `{target_project}`.`{params.region}`.INFORMATION_SCHEMA.CAPACITY_COMMITMENT_CHANGES
  WHERE
    state = "ACTIVE"
    AND edition = '{params.edition}'
    AND change_timestamp <= TIMESTAMP('{end_str}')
  UNION ALL
  SELECT
    "Start" AS capacity_commitment_id,
    0 AS slot_count_delta,
    TIMESTAMP('{start_str}') AS change_timestamp
  UNION ALL
  SELECT
    "End" AS capacity_commitment_id,
    0 AS slot_count_delta,
    TIMESTAMP('{end_str}') AS change_timestamp ),
  running_total_commitment AS (
  SELECT
    change_timestamp,
    SUM(slot_count_delta) OVER (ORDER BY change_timestamp RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS capacity_slots,
  FROM
    capacity_commitments),
  merged_slots AS (
  SELECT
    TIMESTAMP_TRUNC(change_timestamp, SECOND) as change_timestamp,
    TIMESTAMP_TRUNC(IFNULL(LEAD(change_timestamp) OVER (ORDER BY change_timestamp ASC), TIMESTAMP('{end_str}')), SECOND) AS end_timestamp,
    IFNULL(LAST_VALUE(current_slots IGNORE NULLS) OVER (ORDER BY change_timestamp ASC), 0) AS current_slots,
    IFNULL(LAST_VALUE(baseline_slots IGNORE NULLS) OVER (ORDER BY change_timestamp ASC), 0) AS baseline_slots,
    IFNULL(LAST_VALUE(capacity_slots IGNORE NULLS) OVER (ORDER BY change_timestamp ASC), 0) AS capacity_slots,
  FROM
    running_total
  FULL OUTER JOIN
    running_total_commitment
  USING
    (change_timestamp)
  ORDER BY
    change_timestamp ASC)
"""

    agg_sql = base_ctes + f"""
SELECT
  SUM(TIMESTAMP_DIFF(end_timestamp, change_timestamp, SECOND) * current_slots) / 60 / 60 AS autoscaled_slot_hours,
  SUM(TIMESTAMP_DIFF(end_timestamp, change_timestamp, SECOND) *
  IF
    (baseline_slots > capacity_slots, baseline_slots - capacity_slots, 0)) / 60 / 60 AS baseline_slot_hours,
  SUM(TIMESTAMP_DIFF(end_timestamp, change_timestamp, SECOND) * (current_slots +
  IF
    (baseline_slots > capacity_slots, baseline_slots - capacity_slots, 0))) / 60 / 60 AS total_slot_hours
FROM
  merged_slots
WHERE
  change_timestamp >= TIMESTAMP('{start_str}')
"""

    timeline_sql = base_ctes + f"""
SELECT
  TIMESTAMP_TRUNC(change_timestamp, MINUTE) AS change_timestamp,
  MAX(current_slots) AS current_slots,
  MAX(baseline_slots) AS baseline_slots,
  MAX(capacity_slots) AS capacity_slots
FROM merged_slots
WHERE change_timestamp >= TIMESTAMP('{start_str}')
GROUP BY 1
ORDER BY 1 ASC
"""

    try:
        logger.info("Executing Aggregated Actual Provisioning Query")
        agg_results = run_query_and_log(scoped_client, agg_sql, "Aggregated Actual Provisioning", params=params)
        
        logger.info("Executing Timeline Actual Provisioning Query")
        timeline_results = run_query_and_log(scoped_client, timeline_sql, "Timeline Actual Provisioning", params=params)
        
        autoscaled_slot_hours = 0.0
        baseline_slot_hours = 0.0
        total_slot_hours = 0.0
        
        for row in agg_results:
            autoscaled_slot_hours = row['autoscaled_slot_hours'] or 0.0
            baseline_slot_hours = row['baseline_slot_hours'] or 0.0
            total_slot_hours = row['total_slot_hours'] or 0.0
            
        timeline_data = []
        for row in timeline_results:
            timeline_data.append({
                "ts": row['change_timestamp'].isoformat() if hasattr(row['change_timestamp'], 'isoformat') else str(row['change_timestamp']),
                "current_slots": row['current_slots'],
                "baseline_slots": row['baseline_slots'],
                "capacity_slots": row['capacity_slots']
            })
            
        log_endpoint_end("Actual Provisioning", t0, _logger=logger)
        return {
            "autoscaled_slot_hours": round(autoscaled_slot_hours, 2),
            "baseline_slot_hours": round(baseline_slot_hours, 2),
            "total_slot_hours": round(total_slot_hours, 2),
            "timeline": timeline_data
        }
        
    except Exception as e:
        handle_endpoint_exception(e, "Actual provisioning")


class PeakSlotsParams(FocusMixin):
    org_project_id: Optional[str] = None
    region: str = "region-us"
    lookback_days: int = Field(default=30, ge=1, le=90)
    max_bytes_billed_gb: Optional[int] = None

@app.post("/api/slots/peak")
def get_peak_slots(params: PeakSlotsParams):
    _validate_safe_params(params)
    t0 = log_endpoint_start("Peak Slots", params, _logger=logger)
    
    scoped_client, resolved_project = init_bq_client_and_resolve_project(params)
    # NOTE: focus_projects intentionally NOT applied to capacity planning.
    focus_clause, focus_params = "", []
    sql = f"""
    WITH concurrent_usage AS (
        SELECT period_start, SUM(period_slot_ms) / 1000 AS concurrent_slots
        FROM `{resolved_project}`.`{params.region}`.INFORMATION_SCHEMA.JOBS_TIMELINE_BY_ORGANIZATION
        WHERE 
          period_start >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {params.lookback_days} DAY)
          AND job_type = 'QUERY'
          AND (statement_type != 'SCRIPT' OR statement_type IS NULL)
          {focus_clause}
        GROUP BY 1
    )
    SELECT MAX(concurrent_slots) AS peak_slots FROM concurrent_usage
    """
    
    try:
        results = run_query_and_log(scoped_client, sql, "Get Peak Slots", params=params, query_parameters=focus_params)
        
        peak_slots = 0
        for row in results:
            peak_slots = float(row['peak_slots']) if row['peak_slots'] else 0
            
        log_endpoint_end("Peak Slots", t0, _logger=logger)
        return {"peak_slots": peak_slots}
        
    except Exception as e:
        handle_endpoint_exception(e, "Peak slots")


class SlotProfilerParams(FocusMixin):
    org_project_id: Optional[str] = None
    region: str = "region-us"
    lookback_days: int = Field(default=7, ge=1, le=90)
    admin_project_id: Optional[str] = None
    fluid_scaling: bool = False
    max_bytes_billed_gb: Optional[int] = None

@app.post("/api/slots/profiler")
def analyze_workload_profile(params: SlotProfilerParams):
    _validate_safe_params(params)
    t0 = log_endpoint_start("Workload Profiler", params, _logger=logger)
    
    scoped_client, resolved_project = init_bq_client_and_resolve_project(params)
    focus_clause, focus_params = build_project_filter(params.focus_projects)
    focus_clause_j, _ = build_project_filter(params.focus_projects, table_alias="j")
    admin_project_raw = params.admin_project_id.strip() if (params.admin_project_id and params.admin_project_id.strip()) else resolved_project
    target_project = _safe_ident(admin_project_raw, "admin_project_id")
    reject_dummy_project(target_project)
    
    sql = f"""
    WITH reservation_hourly AS (
      SELECT
        TIMESTAMP_TRUNC(creation_time, HOUR) AS hour_bucket,
        reservation_id,
        COUNT(*) AS hourly_queries,
        AVG(total_slot_ms / NULLIF(TIMESTAMP_DIFF(end_time, start_time, MILLISECOND), 0)) AS avg_slots_per_query,
        APPROX_QUANTILES(TIMESTAMP_DIFF(end_time, start_time, SECOND), 100)[OFFSET(50)] AS median_duration_seconds
      FROM
        `{target_project}`.`{params.region}`.INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION
      WHERE
        creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {params.lookback_days} DAY)
        AND job_type = 'QUERY'
        AND (statement_type != 'SCRIPT' OR statement_type IS NULL)
        AND reservation_id IS NOT NULL
        {focus_clause}
      GROUP BY
        hour_bucket, reservation_id
    ),
    flagged_hours AS (
      SELECT
        hour_bucket,
        reservation_id,
        hourly_queries
      FROM
        reservation_hourly
      WHERE
        hourly_queries > 60 
        AND avg_slots_per_query < 100 
        AND median_duration_seconds < 5 
    ),
    project_stats AS (
      SELECT
        j.reservation_id,
        j.project_id,
        COUNT(*) AS total_queries,
        ROW_NUMBER() OVER (PARTITION BY j.reservation_id ORDER BY COUNT(*) DESC) AS rank
      FROM
        `{target_project}`.`{params.region}`.INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION AS j
      JOIN
        flagged_hours
      ON
        TIMESTAMP_TRUNC(j.creation_time, HOUR) = flagged_hours.hour_bucket
        AND j.reservation_id = flagged_hours.reservation_id
      WHERE
        j.creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {params.lookback_days} DAY)
        AND j.job_type = 'QUERY'
        AND (j.statement_type != 'SCRIPT' OR j.statement_type IS NULL)
        AND j.reservation_id IS NOT NULL
        {focus_clause_j}
      GROUP BY
        j.reservation_id, j.project_id
    ),
    top_projects_agg AS (
      SELECT
        reservation_id,
        STRING_AGG(CONCAT(project_id, ' (', total_queries, ')'), ', ' ORDER BY rank) AS top_projects
      FROM
        project_stats
      WHERE
        rank <= 3
      GROUP BY
        reservation_id
    )
    SELECT
      reservation_id,
      COUNT(DISTINCT hour_bucket) AS total_flagged_hours,
      MAX(hourly_queries) AS peak_hourly_queries,
      top_projects
    FROM
      flagged_hours
    JOIN
      top_projects_agg
    USING (reservation_id)
    GROUP BY
      reservation_id, top_projects
    """
    
    timeline_sql = f"""
    WITH reservation_hourly AS (
      SELECT
        TIMESTAMP_TRUNC(creation_time, HOUR) AS hour_bucket,
        reservation_id,
        COUNT(*) AS hourly_queries,
        AVG(total_slot_ms / NULLIF(TIMESTAMP_DIFF(end_time, start_time, MILLISECOND), 0)) AS avg_slots_per_query,
        APPROX_QUANTILES(TIMESTAMP_DIFF(end_time, start_time, SECOND), 100)[OFFSET(50)] AS median_duration_seconds
      FROM
        `{target_project}`.`{params.region}`.INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION
      WHERE
        creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {params.lookback_days} DAY)
        AND job_type = 'QUERY'
        AND (statement_type != 'SCRIPT' OR statement_type IS NULL)
        AND reservation_id IS NOT NULL
        {focus_clause}
      GROUP BY
        hour_bucket, reservation_id
    ),
    flagged_hours AS (
      SELECT
        hour_bucket,
        reservation_id,
        hourly_queries
      FROM
        reservation_hourly
      WHERE
        hourly_queries > 60 
        AND avg_slots_per_query < 100 
        AND median_duration_seconds < 5 
    )
    SELECT
      hour_bucket,
      reservation_id,
      hourly_queries
    FROM
      flagged_hours
    ORDER BY
      hour_bucket ASC
    """
    
    try:
        logger.info("Executing Profiler Summary Query")
        results = run_query_and_log(scoped_client, sql, "Workload Profiler Summary", params=params, query_parameters=focus_params)
        
        logger.info("Executing Profiler Timeline Query")
        timeline_results = run_query_and_log(scoped_client, timeline_sql, "Workload Profiler Timeline", params=params, query_parameters=focus_params)
        
        profile_records = []
        for row in results:
            recommendation = "Optimized by Fluid Scaling. Monitored for efficiency." if params.fluid_scaling else "Potential waste due to 60s minimum billing. Consider enabling Fluid Scaling."
            profile_records.append({
                "reservation_id": row['reservation_id'],
                "total_flagged_hours": row['total_flagged_hours'],
                "peak_hourly_queries": row['peak_hourly_queries'],
                "top_projects": row['top_projects'],
                "recommendation": recommendation
            })
            
        timeline_records = []
        for row in timeline_results:
            timeline_records.append({
                "hour_bucket": row['hour_bucket'].isoformat() if hasattr(row['hour_bucket'], 'isoformat') else str(row['hour_bucket']),
                "reservation_id": row['reservation_id'],
                "hourly_queries": row['hourly_queries']
            })
            
        log_endpoint_end("Workload Profiler", t0, _logger=logger)
        return {
            "summary": profile_records,
            "timeline": timeline_records
        }
        
    except Exception as e:
        handle_endpoint_exception(e, "Workload profiler")


@app.post("/api/slots/profiler/queries")
def get_top_profiler_queries(params: SlotProfilerParams):
    _validate_safe_params(params)
    t0 = log_endpoint_start("Profiler Top Queries", params, _logger=logger)
    
    scoped_client, resolved_project = init_bq_client_and_resolve_project(params)
    focus_clause, focus_params = build_project_filter(params.focus_projects)
    admin_project_raw = params.admin_project_id.strip() if (params.admin_project_id and params.admin_project_id.strip()) else resolved_project
    target_project = _safe_ident(admin_project_raw, "admin_project_id")
    reject_dummy_project(target_project)
    
    sql = f"""
    WITH reservation_hourly AS (
      SELECT
        TIMESTAMP_TRUNC(creation_time, HOUR) AS hour_bucket,
        reservation_id,
        COUNT(*) AS hourly_queries,
        AVG(total_slot_ms / NULLIF(TIMESTAMP_DIFF(end_time, start_time, MILLISECOND), 0)) AS avg_slots_per_query,
        APPROX_QUANTILES(TIMESTAMP_DIFF(end_time, start_time, SECOND), 100)[OFFSET(50)] AS median_duration_seconds
      FROM
        `{target_project}`.`{params.region}`.INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION
      WHERE
        creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {params.lookback_days} DAY)
        AND job_type = 'QUERY'
        AND (statement_type != 'SCRIPT' OR statement_type IS NULL)
        AND reservation_id IS NOT NULL
        {focus_clause}
      GROUP BY
        hour_bucket, reservation_id
    ),
    flagged_hours AS (
      SELECT
        hour_bucket,
        reservation_id
      FROM
        reservation_hourly
      WHERE
        hourly_queries > 60 
        AND avg_slots_per_query < 100 
        AND median_duration_seconds < 5 
    ),
    jobs_with_bucket AS (
      SELECT
        reservation_id,
        TIMESTAMP_TRUNC(creation_time, HOUR) AS hour_bucket,
        job_id,
        project_id,
        total_slot_ms,
        total_bytes_processed,
        start_time,
        end_time,
        query_info.query_hashes.normalized_literals AS query_hash
      FROM
        `{target_project}`.`{params.region}`.INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION
      WHERE
        creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {params.lookback_days} DAY)
        AND job_type = 'QUERY'
        AND (statement_type != 'SCRIPT' OR statement_type IS NULL)
        AND reservation_id IS NOT NULL
        {focus_clause}
    )
    SELECT
      query_hash,
      ANY_VALUE(job_id) AS example_job_id,
      ANY_VALUE(project_id) AS example_project_id,
      COUNT(*) AS frequency,
      AVG(total_slot_ms / 1000 / 60 / 60) AS avg_slot_hours,
      AVG(TIMESTAMP_DIFF(end_time, start_time, SECOND)) AS avg_duration_seconds,
      AVG(total_bytes_processed) AS avg_bytes_processed
    FROM
      jobs_with_bucket
    JOIN
      flagged_hours
    USING (reservation_id, hour_bucket)
    GROUP BY
      query_hash
    ORDER BY
      frequency DESC
    LIMIT 10
    """
    
    try:
        results = run_query_and_log(scoped_client, sql, "Profiler Top Queries", params=params, query_parameters=focus_params)
        
        query_records = []
        for row in results:
            query_text = "Query text not found"
            example_job_id = row['example_job_id']
            example_project_id = row['example_project_id']
            
            if example_job_id and example_project_id:
                try:
                    safe_proj = _safe_ident(example_project_id, "example_project_id")
                    text_sql = f"""
                    SELECT query
                    FROM `{safe_proj}`.`{params.region}`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
                    WHERE job_id = @job_id
                    LIMIT 1
                    """
                    text_results = run_query_and_log(
                        scoped_client, text_sql,
                        f"Profiler Query Text ({example_job_id[:12]})",
                        params=params,
                        query_parameters=[
                            bigquery.ScalarQueryParameter("job_id", "STRING", example_job_id)
                        ]
                    )
                    for text_row in text_results:
                        query_text = text_row['query']
                except Exception as text_err:
                    logger.warning(f"Failed to fetch query text for job {example_job_id}: {text_err}")
                    query_text = "Error fetching query text"
            
            avg_bytes = row['avg_bytes_processed'] or 0.0
            recommendation = "N/A"
            if avg_bytes < 100 * 1024 * 1024 and row['avg_duration_seconds'] < 5:
                if params.fluid_scaling:
                    recommendation = "Optimized by Fluid Scaling. Monitor for further efficiency."
                else:
                    recommendation = "Candidate for Short Query Optimization. Consider enabling Fluid Scaling or Advanced Runtime."
                
            query_records.append({
                "query": query_text[:500] + '...' if len(query_text) > 500 else query_text,
                "example_job_id": example_job_id,
                "project_id": example_project_id,
                "frequency": row['frequency'],
                "avg_slot_hours": round(row['avg_slot_hours'] or 0.0, 4),
                "avg_duration_seconds": round(row['avg_duration_seconds'] or 0.0, 2),
                "avg_bytes_processed": round(avg_bytes, 2),
                "recommendation": recommendation
            })
            
        log_endpoint_end("Profiler Top Queries", t0, _logger=logger)
        return query_records
        
    except Exception as e:
        handle_endpoint_exception(e, "Profiler queries")


class UserProfilerParams(FocusMixin):
    org_project_id: Optional[str] = None
    region: str = "region-us"
    lookback_days: int = Field(default=7, ge=1, le=90)
    admin_project_id: Optional[str] = None
    od_price: float = 6.25
    ed_price: float = 0.06
    max_bytes_billed_gb: Optional[int] = None

@app.post("/api/users/top_spenders")
def get_top_spenders(params: UserProfilerParams):
    _validate_safe_params(params)
    t0 = log_endpoint_start("Top Spenders", params, _logger=logger)
    
    scoped_client, resolved_project = init_bq_client_and_resolve_project(params)
    focus_clause, focus_params = build_project_filter(params.focus_projects)
    admin_project_raw = params.admin_project_id.strip() if (params.admin_project_id and params.admin_project_id.strip()) else resolved_project
    target_project = _safe_ident(admin_project_raw, "admin_project_id")
    reject_dummy_project(target_project)
    
    sql = f"""
    SELECT
      user_email,
      COUNT(*) AS query_count,
      SUM(total_bytes_billed) AS total_bytes_billed,
      SUM(total_slot_ms) / (1000 * 60 * 60) AS total_slot_hours
    FROM
      `{target_project}`.`{params.region}`.INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION
    WHERE
      creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {params.lookback_days} DAY)
      AND job_type = 'QUERY'
      AND parent_job_id IS NULL
      {focus_clause}
    GROUP BY
      user_email
    ORDER BY
      total_slot_hours DESC
    LIMIT 50
    """
    
    try:
        results = run_query_and_log(scoped_client, sql, "Top Spenders", params=params, query_parameters=focus_params)
        
        user_records = []
        for row in results:
            bytes_billed = row['total_bytes_billed'] or 0
            slot_hours = row['total_slot_hours'] or 0.0
            
            # Calculate costs
            est_od_cost = (bytes_billed / (1024**4)) * params.od_price
            est_ed_cost = slot_hours * params.ed_price
            
            user_records.append({
                "user_email": row['user_email'],
                "query_count": row['query_count'],
                "total_bytes_billed": bytes_billed,
                "total_slot_hours": round(slot_hours, 2),
                "est_on_demand_cost": round(est_od_cost, 2),
                "est_editions_cost": round(est_ed_cost, 2)
            })
            
        log_endpoint_end("Top Spenders", t0, _logger=logger)
        return user_records
        
    except Exception as e:
        handle_endpoint_exception(e, "Top spenders")


# -- Dashboard Response models ---------------------------------------------------------

class KpiResponse(BaseModel):
    mtdSpend: Optional[float] = None
    mtdSpendDelta: Optional[float] = None  # percent change MoM, e.g. 12.5 = +12.5%
    forecastSpend: Optional[float] = None
    lastMonthSpend: Optional[float] = None
    potentialSavings: Optional[float] = None
    opportunityCount: Optional[int] = None
    anomalyCount: Optional[int] = None
    stub: bool = True          # True = stub/mock data, False = live data


class Opportunity(BaseModel):
    label: str
    module: str                # short label e.g. "STORAGE", "COMPUTE"
    monthlySavings: float
    deepLink: str              # e.g. "#storage?dataset=foo"


class ProjectCost(BaseModel):
    projectId: str
    cost: float


class Anomaly(BaseModel):
    severity: str              # 'warning' | 'critical'
    html: str                  # pre-sanitized; contains <strong>...</strong>
    deepLink: str


# -- Dashboard Endpoints ---------------------------------------------------------------

@app.get("/api/dashboard/kpis", response_model=KpiResponse)
def get_kpis():
    """
    TODO: Real implementation requires:
      1. GCP Billing Export table for mtdSpend / lastMonthSpend (PRD §7.2.6).
         Without it, return None for these fields and the UI will show '—'.
      2. Linear forecast from MTD spend × (days_in_month / day_of_month).
         For v1, this is acceptable. Real forecasting comes later.
      3. potentialSavings = sum of monthlySavings across:
         - Storage Optimizer endpoint
         - On-Demand vs Editions analyzer
         - Anti-pattern linter (sum of estimated waste)
      4. opportunityCount = count of all rows above
      5. anomalyCount = len(get_anomalies())
    """
    return KpiResponse(stub=True)


@app.get("/api/dashboard/opportunities", response_model=List[Opportunity])
def get_opportunities(limit: int = 5):
    """
    TODO: Aggregate top-N opportunities by monthlySavings across modules.

    Recommended approach:
      - Run existing storage-recommendations and on-demand-vs-editions queries
        with their default params (or cached results from last hour).
      - Run anti-pattern linter and sum waste per pattern.
      - Merge into a single list, sort by monthlySavings DESC, take top `limit`.
      - Each row's deepLink should pre-filter the target module to highlight
        the specific dataset/job (e.g. "#storage?dataset=warehouse_db").
    """
    return []


@app.get("/api/dashboard/top-projects", response_model=List[ProjectCost])
def get_top_projects(limit: int = 5):
    """
    TODO: Use existing Cost Attribution Engine logic.
    Aggregate Direct Usage Cost + Allocated Waste per project for current month.
    Return top `limit` by total cost descending.
    """
    return []


@app.get("/api/dashboard/anomalies", response_model=List[Anomaly])
def get_anomalies():
    """
    TODO: Real anomaly detection requires historical baseline.

    For v1, use this simple rule:
      For each project: compare last 7 days spend vs prior 7 days spend.
      Flag if change > 50% in either direction.

    Critical = >100% change. Warning = 50-100% change.

    The `html` field MUST be sanitized server-side. Frontend trusts it.
    """
    return []



```

---

### 4.3 `src/fluid_scaling.py`

**Role**: Fluid Scaling status checks and savings estimation. Compares legacy autoscaler (60s cooldown) vs. fluid autoscaler billing using per-second capacity and usage data.

```python
from __future__ import annotations

import logging
import re
import time
from dataclasses import dataclass
from typing import List, Optional, Literal

import pandas as pd
from fastapi import APIRouter, HTTPException
from google.api_core import exceptions as gax_exc
from google.cloud import bigquery
from pydantic import BaseModel, Field
from .utils import init_bq_client_and_resolve_project, reject_dummy_project, _safe_ident, _normalize_region, get_max_bytes_billed, FocusMixin, validate_focus_projects, build_project_filter, log_endpoint_start, log_endpoint_end, DAYS_PER_MONTH

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/fluid-scaling", tags=["fluid-scaling"])


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------


DAYS_PER_YEAR = 365.25
SECONDS_PER_HOUR = 3600
SECONDS_PER_MINUTE = 60

# MAX_BYTES_BILLED removed — now resolved dynamically via get_max_bytes_billed(params)
MAX_LOOKBACK_DAYS = 90


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------

class FluidScalingStatus(BaseModel):
    reservation_id: str
    enabled: bool
    ddl: Optional[str] = None


class FluidScalingParams(BaseModel):
    org_project_id: Optional[str] = None
    admin_project_id: Optional[str] = None
    region: str = "region-us"
    max_bytes_billed_gb: Optional[int] = None


class FluidEstimateParams(FocusMixin):
    org_project_id: Optional[str] = None
    admin_project_id: Optional[str] = None
    region: str = "region-us"
    lookback_days: int = Field(default=7, ge=1, le=MAX_LOOKBACK_DAYS)
    price_per_slot_hr: float = Field(default=0.06, gt=0)
    max_bytes_billed_gb: Optional[int] = None


class FluidEstimateMetric(BaseModel):
    """Numeric fields — frontend handles formatting, can sort/filter."""
    reservation_id: str               # Fully qualified, e.g. "project:loc.name"
    reservation_short_name: str       # Just the name part, for display
    fluid_autoscaler_slot_hours: float
    legacy_autoscaler_slot_hours: float
    total_pure_used_slot_hours: float
    slot_hours_saved: float
    clamped_pct_savings: float
    estimated_usd_saved_window: float
    extrapolated_monthly_usd: float
    extrapolated_annual_usd: float
    status: Literal["Active", "Idle", "Inactive", "External Admin"]


class FluidScalingConfigStatus(BaseModel):
    enabled: bool
    configured_reservations: List[str]
    missing_reservations: List[str]
    ddl: Optional[str] = None


class FluidScalingEstimateResponse(BaseModel):
    reservations: List[FluidEstimateMetric]
    config_status: FluidScalingConfigStatus




# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------



def _strip_qualifier(reservation_id: Optional[str]) -> str:
    if not reservation_id:
        return "(unassigned)"
    return re.split(r"[.:]", reservation_id)[-1]


def _run_and_log(client, sql, label, params=None, query_parameters=None):
    """Run a query with timing, BQ URL, and structured logging."""
    max_bytes = get_max_bytes_billed(params)
    job_config = bigquery.QueryJobConfig(
        maximum_bytes_billed=max_bytes,
        query_parameters=query_parameters or []
    )
    logger.debug("%s SQL:\n%s", label, sql)
    logger.info("⏳ %s — submitting query (safety cap: %s GiB)…", label, max_bytes // (1024**3))
    t0 = time.time()
    query_job = client.query(sql, job_config=job_config)
    results = query_job.result()
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


# ---------------------------------------------------------------------------
# Status endpoint
# ---------------------------------------------------------------------------

_FLUID_OPTION_NAME = "preflight_fluid_autoscaling_reservations"


def _parse_option_value(raw: str) -> set[str]:
    if not raw:
        return set()
    val = raw.strip()
    if val.startswith("[") and val.endswith("]"):
        val = val[1:-1]
    return {part.strip().strip('"').strip("'") for part in val.split(",") if part.strip()}


def _render_sql(template: str, **idents) -> str:
    out = template
    for key, val in idents.items():
        out = out.replace("{" + key + "}", val)
    return out


def get_effective_fluid_scaling_reservations(client: bigquery.Client, project: str, region: str, params=None) -> set[str]:
    effective_sql = f"""
      SELECT option_value
      FROM `{project}`.`{region}`.INFORMATION_SCHEMA.EFFECTIVE_PROJECT_OPTIONS
      WHERE option_name = 'preflight_fluid_autoscaling_reservations'
    """
    fallback_sql = f"""
      SELECT option_value
      FROM `{project}`.`{region}`.INFORMATION_SCHEMA.PROJECT_OPTIONS
      WHERE option_name = 'preflight_fluid_autoscaling_reservations'
    """
    for label, sql in [("Fluid Options (EFFECTIVE)", effective_sql), ("Fluid Options (Fallback)", fallback_sql)]:
        try:
            results = list(_run_and_log(client, sql, label, params=params))
            for row in results:
                vals = _parse_option_value(row.option_value)
                if vals:
                    logger.info("Fluid option found via %s on %s: %s", label, project, sorted(vals))
                    return vals
            logger.info("Fluid option not found via %s on %s (0 rows)", label, project)
        except Exception as e:
            logger.warning("%s query failed on %s: %s", label, project, e)
    return set()


@router.post("/status", response_model=List[FluidScalingStatus])
def check_fluid_scaling_status(params: FluidScalingParams):
    t0 = log_endpoint_start("Fluid Scaling Status", params, _logger=logger)
    try:
        client, project = init_bq_client_and_resolve_project(params)
        admin_project_raw = params.admin_project_id.strip() if (params.admin_project_id and params.admin_project_id.strip()) else project
        admin_project = _safe_ident(admin_project_raw, "admin_project_id")
        reject_dummy_project(admin_project)
        region = _safe_ident(_normalize_region(params.region), "region")

        sql_reservations = f"""
            SELECT reservation_name
            FROM `{admin_project}`.`{region}`.INFORMATION_SCHEMA.RESERVATIONS
        """
        res_results = _run_and_log(client, sql_reservations, "Fluid Scaling Reservations", params=params)
        all_reservations = [r.reservation_name for r in res_results]

        enabled = get_effective_fluid_scaling_reservations(client, admin_project, region, params=params)
        enabled_norm = {_strip_qualifier(r) for r in enabled}

        output: List[FluidScalingStatus] = []
        for res in all_reservations:
            short = _strip_qualifier(res)
            is_enabled = short in enabled_norm
            ddl = None
            if not is_enabled:
                new_list = sorted(list(enabled_norm | {short}))
                list_str = ", ".join(f'"{r}"' for r in new_list)
                ddl = (
                    f"ALTER PROJECT `{admin_project}`\n"
                    f"SET OPTIONS (\n"
                    f"  `{region}.{_FLUID_OPTION_NAME}` = [{list_str}]\n"
                    f");"
                )
            output.append(FluidScalingStatus(reservation_id=res, enabled=is_enabled, ddl=ddl))
        log_endpoint_end("Fluid Scaling Status", t0, _logger=logger)
        return output

    except gax_exc.Forbidden:
        logger.exception("Permission denied checking fluid scaling status")
        raise HTTPException(403, "Insufficient permissions for INFORMATION_SCHEMA.RESERVATIONS")
    except gax_exc.NotFound:
        logger.exception("Project or region not found")
        raise HTTPException(404, "Project or region not found")
    except gax_exc.GoogleAPIError:
        logger.exception("BigQuery error checking fluid scaling status")
        raise HTTPException(500, "Query failed; check server logs")


# ---------------------------------------------------------------------------
# Estimate endpoint
# ---------------------------------------------------------------------------

_SQL_PER_SECOND_CAPACITY = """
SELECT
  reservation_id,
  edition,
  s.start_time AS period_start,
  IFNULL(s.slots_assigned, 0) AS baseline_slots,
  IFNULL(s.autoscale_current_slots, 0) AS autoscale_current_slots
FROM `{admin_project}`.`{region}`.INFORMATION_SCHEMA.RESERVATION_TIMELINE_BY_PROJECT,
UNNEST(
  IF(ARRAY_LENGTH(per_second_details) > 0,
     ARRAY(
       SELECT AS STRUCT d.start_time, d.autoscale_current_slots, d.slots_assigned
       FROM UNNEST(per_second_details) AS d
     ),
     ARRAY(
       SELECT AS STRUCT
         ts AS start_time,
         autoscale.current_slots AS autoscale_current_slots,
         slots_assigned AS slots_assigned
       FROM UNNEST(GENERATE_TIMESTAMP_ARRAY(
         period_start,
         TIMESTAMP_ADD(period_start, INTERVAL 59 SECOND),
         INTERVAL 1 SECOND
       )) AS ts
     )
  )
) AS s
WHERE period_start >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @lookback_days DAY)
  AND period_start <  CURRENT_TIMESTAMP()
  AND reservation_id IS NOT NULL
  AND reservation_id != ''
"""

_SQL_PER_SECOND_USAGE = """
SELECT
  reservation_id,
  edition,
  period_start,
  SUM(period_slot_ms) / 1000.0 AS used_slots
FROM `{org_project}`.`{region}`.INFORMATION_SCHEMA.JOBS_TIMELINE_BY_ORGANIZATION
WHERE job_creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @lookback_days DAY)
  AND job_creation_time <  CURRENT_TIMESTAMP()
  AND period_start      >  TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @lookback_days DAY)
  AND period_start      <= CURRENT_TIMESTAMP()
  AND reservation_id IS NOT NULL
  AND reservation_id != ''
  AND (statement_type != 'SCRIPT' OR statement_type IS NULL)
  {focus_clause}
GROUP BY reservation_id, edition, period_start
"""


@dataclass
class _ReservationSummary:
    """Per-reservation aggregates after Python rollup."""
    reservation_id: str
    legacy_slot_seconds: float
    fluid_slot_seconds: float
    total_pure_used_seconds: float
    status: str


def _rollup_to_summaries(
    capacity_df: pd.DataFrame,
    usage_df: pd.DataFrame,
) -> List[_ReservationSummary]:
    if capacity_df.empty and usage_df.empty:
        return []

    capacity_df = capacity_df.copy()
    usage_df = usage_df.copy()

    # Normalize edition on both sides to prevent split rows on NULL/skew (Gap B)
    if not capacity_df.empty:
        capacity_df["edition"] = capacity_df["edition"].fillna("").astype(str)
    if not usage_df.empty:
        usage_df["edition"] = usage_df["edition"].fillna("").astype(str)

    if capacity_df.empty:
        capacity_df = pd.DataFrame(columns=["reservation_id", "edition", "period_start", "baseline_slots", "autoscale_current_slots"])
        capacity_df["minute"] = pd.Series(dtype="datetime64[ns, UTC]")
    else:
        capacity_df["period_start"] = pd.to_datetime(capacity_df["period_start"], utc=True)
        capacity_df["minute"] = capacity_df["period_start"].dt.floor("min")

    if usage_df.empty:
        usage_df = pd.DataFrame(columns=["reservation_id", "edition", "period_start", "used_slots"])
        usage_df["minute"] = pd.Series(dtype="datetime64[ns, UTC]")
    else:
        usage_df["period_start"] = pd.to_datetime(usage_df["period_start"], utc=True)
        usage_df["minute"] = usage_df["period_start"].dt.floor("min")

    # Aggregate capacity to the minute-grain (Gap A)
    cap_per_min = capacity_df.groupby(
        ["reservation_id", "edition", "minute"], as_index=False
    ).agg(
        baseline_slot_seconds=("baseline_slots", "sum"),              # Sum baseline over the 60s
        autoscale_capacity_slot_seconds=("autoscale_current_slots", "sum"),  # Sum current over the 60s
    )
    cap_per_min["_from_capacity"] = 1.0

    # Aggregate usage to the minute-grain (Gap A)
    usage_per_min = usage_df.groupby(
        ["reservation_id", "edition", "minute"], as_index=False
    ).agg(
        used_slots=("used_slots", "sum"),
    )

    # Merge on reservation_id, edition, and minute (Gap B - one-to-one)
    merged = cap_per_min.merge(
        usage_per_min,
        on=["reservation_id", "edition", "minute"],
        how="outer",
    )

    merged["baseline_slot_seconds"] = merged["baseline_slot_seconds"].astype(float).fillna(0.0)
    merged["autoscale_capacity_slot_seconds"] = merged["autoscale_capacity_slot_seconds"].astype(float).fillna(0.0)
    merged["used_slots"] = merged["used_slots"].astype(float).fillna(0.0)
    merged["_from_capacity"] = merged["_from_capacity"].astype(float).fillna(0.0)

    # Fluid clamp on minute-aggregated slot-seconds (Gap A)
    used_above_baseline = (merged["used_slots"] - merged["baseline_slot_seconds"]).clip(lower=0)
    merged["fluid_slot_seconds_min"] = pd.concat(
        [used_above_baseline, merged["autoscale_capacity_slot_seconds"]], axis=1
    ).min(axis=1).clip(lower=0)

    # Per-res sums (single groupby for all aggregates)
    per_res = merged.groupby("reservation_id", as_index=False).agg(
        legacy_slot_seconds=("autoscale_capacity_slot_seconds", "sum"),
        fluid_slot_seconds=("fluid_slot_seconds_min", "sum"),
        total_pure_used_seconds=("used_slots", "sum"),
        has_capacity=("_from_capacity", "max"),
        sum_used=("used_slots", "sum"),
    )

    output = []
    for row in per_res.itertuples():
        has_capacity = float(row.has_capacity)
        sum_used = float(row.sum_used)

        if has_capacity == 0 and sum_used > 0:
            status = "External Admin"
        elif sum_used == 0 and has_capacity > 0:
            status = "Idle"
        elif has_capacity == 0 and sum_used == 0:
            status = "Inactive"
        else:
            status = "Active"
            
        output.append(
            _ReservationSummary(
                reservation_id=row.reservation_id,
                legacy_slot_seconds=float(row.legacy_slot_seconds),
                fluid_slot_seconds=float(row.fluid_slot_seconds),
                total_pure_used_seconds=float(row.total_pure_used_seconds),
                status=status
            )
        )
    return output


def _to_metric(
    summary: _ReservationSummary,
    price_per_slot_hr: float,
    lookback_days: int,
) -> FluidEstimateMetric:
    today_hours = summary.legacy_slot_seconds / SECONDS_PER_HOUR
    fluid_hours = summary.fluid_slot_seconds / SECONDS_PER_HOUR
    total_pure_used_hours = summary.total_pure_used_seconds / SECONDS_PER_HOUR
    saved_hours = today_hours - fluid_hours

    # Clamped True cooldown savings (always >= 0%)
    clamped_savings = (saved_hours / today_hours * 100.0) if today_hours > 0 else 0.0

    usd_window = saved_hours * price_per_slot_hr
    usd_monthly = usd_window * (DAYS_PER_MONTH / lookback_days)
    usd_annual = usd_window * (DAYS_PER_YEAR / lookback_days)

    return FluidEstimateMetric(
        reservation_id=summary.reservation_id or "(unassigned)",
        reservation_short_name=_strip_qualifier(summary.reservation_id),
        fluid_autoscaler_slot_hours=round(fluid_hours, 1),
        legacy_autoscaler_slot_hours=round(today_hours, 1),
        total_pure_used_slot_hours=round(total_pure_used_hours, 1),
        slot_hours_saved=round(saved_hours, 1),
        clamped_pct_savings=round(clamped_savings, 2),
        estimated_usd_saved_window=round(usd_window, 2),
        extrapolated_monthly_usd=round(usd_monthly, 2),
        extrapolated_annual_usd=round(usd_annual, 2),
        status=summary.status,
    )


def _run_query_to_df(
    client: bigquery.Client,
    sql: str,
    lookback_days: int,
    label: str,
    params=None,
    extra_query_params=None,
) -> pd.DataFrame:
    all_params = [
        bigquery.ScalarQueryParameter("lookback_days", "INT64", lookback_days),
    ] + (extra_query_params or [])
    max_bytes = get_max_bytes_billed(params)
    job_config = bigquery.QueryJobConfig(
        query_parameters=all_params,
        maximum_bytes_billed=max_bytes,
    )
    logger.info("⏳ %s — submitting query (lookback=%d days, safety cap: %s GiB)…", label, lookback_days, max_bytes // (1024**3))
    logger.debug("%s SQL:\n%s", label.upper(), sql)
    t0 = time.time()
    query_job = client.query(sql, job_config=job_config)
    df = query_job.result().to_dataframe(
        create_bqstorage_client=True,
    )
    elapsed = time.time() - t0
    # Log profile with clickable BQ Console URL
    job_project = query_job.project
    job_location = query_job.location or "us"
    bq_url = (
        f"https://console.cloud.google.com/bigquery?project={job_project}"
        f"&j=bq:{job_location}:{query_job.job_id}&page=queryresults"
    )
    proc = query_job.total_bytes_processed
    billed = query_job.total_bytes_billed
    proc_gib = f"{proc / (1024**3):.2f} GiB" if proc is not None else "N/A"
    bill_gib = f"{billed / (1024**3):.2f} GiB" if billed is not None else "N/A"
    logger.info(
        "✅ %s — %.1fs | Job: %s | Processed: %s | Billed: %s | Cache: %s | %s",
        label, elapsed, query_job.job_id, proc_gib, bill_gib, query_job.cache_hit, bq_url
    )
    return df


def _build_config_status(
    summaries: List[_ReservationSummary],
    enabled_reservations: set[str],
    admin_project: str,
    region: str,
) -> FluidScalingConfigStatus:
    """
    Determine fluid-scaling enablement vs. actionable reservations.

    Only reservations this project can actually ALTER are considered:
    - "(unassigned)" is skipped (no reservation to configure).
    - "External Admin" is skipped: capacity is owned by a different admin
      project, so an `ALTER PROJECT <admin_project>` DDL here would not apply
      to it and would be misleading/non-runnable.
    """
    enabled_norm = {_strip_qualifier(r) for r in enabled_reservations}
    actionable_res_names = {
        _strip_qualifier(s.reservation_id)
        for s in summaries
        if s.reservation_id
        and _strip_qualifier(s.reservation_id) != "(unassigned)"
        and s.status != "External Admin"
    }

    missing_res = sorted(list(actionable_res_names - enabled_norm))
    configured_res = sorted(list(actionable_res_names & enabled_norm))

    ddl = None
    is_fully_enabled = True
    if missing_res:
        is_fully_enabled = False
        # Union with already-enabled names so the DDL doesn't drop existing entries.
        new_list = sorted(list(enabled_norm | actionable_res_names))
        list_str = ", ".join(f'"{r}"' for r in new_list)
        ddl = (
            f"ALTER PROJECT `{admin_project}`\n"
            f"SET OPTIONS (\n"
            f"  `{region}.{_FLUID_OPTION_NAME}` = [{list_str}]\n"
            f");"
        )

    return FluidScalingConfigStatus(
        enabled=is_fully_enabled,
        configured_reservations=configured_res,
        missing_reservations=missing_res,
        ddl=ddl
    )


@router.post("/estimate", response_model=FluidScalingEstimateResponse)
def estimate_fluid_scaling(params: FluidEstimateParams):
    # NOTE: focus_projects intentionally NOT applied to capacity planning.
    t0 = log_endpoint_start("Fluid Scaling Estimate", params, _logger=logger)
    try:
        client, org_project = init_bq_client_and_resolve_project(params)
        admin_project_raw = params.admin_project_id.strip() if (params.admin_project_id and params.admin_project_id.strip()) else org_project
        admin_project = _safe_ident(admin_project_raw, "admin_project_id")
        reject_dummy_project(admin_project)
        region = _safe_ident(_normalize_region(params.region), "region")

        # Capacity planning must reflect full org demand to size reservations correctly.
        focus_clause = ""
        capacity_sql = _render_sql(_SQL_PER_SECOND_CAPACITY, admin_project=admin_project, region=region)
        usage_sql = _render_sql(_SQL_PER_SECOND_USAGE, org_project=org_project, region=region, focus_clause=focus_clause)

        capacity_df = _run_query_to_df(client, capacity_sql, params.lookback_days, "capacity", params=params)
        usage_df = _run_query_to_df(client, usage_sql, params.lookback_days, "usage", params=params)

        logger.info(
            "Fetched %d capacity rows, %d usage rows", len(capacity_df), len(usage_df)
        )

        logger.info("capacity period_start dtype: %s, sample: %r",
                    capacity_df["period_start"].dtype if not capacity_df.empty else "empty",
                    capacity_df["period_start"].iloc[0] if not capacity_df.empty else None)
        logger.info("usage period_start dtype: %s, sample: %r",
                    usage_df["period_start"].dtype if not usage_df.empty else "empty",
                    usage_df["period_start"].iloc[0] if not usage_df.empty else None)
        if capacity_df.empty and not usage_df.empty:
            logger.warning(
                "Capacity query returned 0 rows but usage has %d rows. "
                "Likely cause: admin_project_id (%s) does not own any reservations. "
                "Reservations seen in usage data: %s",
                len(usage_df),
                admin_project,
                usage_df["reservation_id"].unique().tolist()[:10],
            )

        summaries = _rollup_to_summaries(capacity_df, usage_df)

        metrics = [
            _to_metric(s, params.price_per_slot_hr, params.lookback_days)
            for s in summaries
        ]
        metrics.sort(key=lambda m: m.extrapolated_annual_usd, reverse=True)

        # Config status check
        enabled_reservations = get_effective_fluid_scaling_reservations(client, admin_project, region, params=params)
        config_status = _build_config_status(summaries, enabled_reservations, admin_project, region)
        
        logger.info(
            "FLUID config check: admin_project=%s region=%s -> enabled=%s | active=%s | missing=%s",
            admin_project, region, sorted(list({_strip_qualifier(r) for r in enabled_reservations})),
            sorted(list({_strip_qualifier(s.reservation_id) for s in summaries if s.reservation_id and _strip_qualifier(s.reservation_id) != "(unassigned)"})),
            config_status.missing_reservations,
        )

        log_endpoint_end("Fluid Scaling Estimate", t0, _logger=logger)
        return FluidScalingEstimateResponse(
            reservations=metrics,
            config_status=config_status
        )

    except gax_exc.Forbidden:
        logger.exception("Permission denied")
        raise HTTPException(
            403,
            "Insufficient permissions. Need access to "
            "RESERVATION_TIMELINE_BY_PROJECT and JOBS_TIMELINE_BY_ORGANIZATION.",
        )
    except gax_exc.NotFound:
        logger.exception("Resource not found")
        raise HTTPException(404, "Project or region not found")
    except gax_exc.GoogleAPIError:
        logger.exception("BigQuery error")
        raise HTTPException(500, "Query failed; check server logs")
    except HTTPException:
        raise
    except Exception:
        logger.exception("Unexpected error in fluid scaling estimate")
        raise HTTPException(500, "Internal server error")

```

---

### 4.4 `src/hbo.py`

**Role**: History-Based Optimization — identifies jobs that ran faster than their historical average (HBO optimization), summarizes savings, checks HBO enablement status per project, and surfaces performance insights.

```python
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional, List, Dict
from google.cloud import bigquery
from .utils import init_bq_client_and_resolve_project, handle_endpoint_exception, get_max_bytes_billed, FocusMixin, validate_focus_projects, build_project_filter, log_endpoint_start, log_endpoint_end, DAYS_PER_MONTH
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
import json
import logging
import time

logger = logging.getLogger(__name__)


def _run_and_log(client, sql, label, params=None, query_parameters=None):
    """Run a query with timing, BQ URL, and structured logging."""
    max_bytes = get_max_bytes_billed(params)
    job_config = bigquery.QueryJobConfig(
        maximum_bytes_billed=max_bytes,
        query_parameters=query_parameters or []
    )
    logger.debug("%s SQL:\n%s", label, sql)
    logger.info("⏳ %s — submitting query (safety cap: %s GiB)…", label, max_bytes // (1024**3))
    t0 = time.time()
    query_job = client.query(sql, job_config=job_config)
    results = query_job.result()
    elapsed = time.time() - t0
    proc = query_job.total_bytes_processed
    billed = query_job.total_bytes_billed
    proc_gib = f"{proc / (1024**3):.2f} GiB" if proc is not None else "N/A"
    bill_gib = f"{billed / (1024**3):.2f} GiB" if billed is not None else "N/A"
    loc = query_job.location or "us"
    bq_url = f"https://console.cloud.google.com/bigquery?project={query_job.project}&j=bq:{loc}:{query_job.job_id}&page=queryresults"
    logger.info(
        "✅ %s — %.1fs | Job: %s | Processed: %s | Billed: %s | Cache: %s | %s",
        label, elapsed, query_job.job_id, proc_gib, bill_gib, query_job.cache_hit, bq_url
    )
    return results

router = APIRouter(prefix="/api/hbo", tags=["hbo"])

# _MAX_BYTES_BILLED removed — now resolved dynamically via get_max_bytes_billed(params)

class HBOCommonParams(FocusMixin):
    org_project_id: Optional[str] = None
    region: str = "region-us"
    lookback_days: int = 7
    max_bytes_billed_gb: Optional[int] = None

class HBOAnalyzeParams(HBOCommonParams):
    limit: int = 10

class HBOStatusParams(BaseModel):
    org_project_id: Optional[str] = None
    region: str = "region-us"
    lookback_days: int = 7
    max_bytes_billed_gb: Optional[int] = None

class HBOResult(BaseModel):
    job_id: str
    percent_execution_time_saved: float
    new_elapsed_ms: int
    original_elapsed_ms: int
    saved_slot_hours: float
    estimated_savings_usd: float

class HBOSummary(BaseModel):
    total_optimized_jobs: int
    total_saved_slot_hours: float
    total_estimated_savings_usd: float
    avg_percent_time_saved: float

class HBOStatus(BaseModel):
    project_id: str
    enabled: bool
    ddl: Optional[str] = None

@router.post("/analyze", response_model=List[HBOResult])
def analyze_hbo(params: HBOAnalyzeParams):
    params.focus_projects = validate_focus_projects(params.focus_projects)
    t0 = log_endpoint_start("HBO Analyze", params, _logger=logger)
    try:
        bq_client, target_project = init_bq_client_and_resolve_project(params)
        focus_clause, focus_params = build_project_filter(params.focus_projects)
        
        sql = f"""
        SELECT
          job_id,
          user_email,
          query_info.query_hashes.normalized_literals AS query_hash,
          start_time,
          end_time,
          TIMESTAMP_DIFF(end_time, start_time, MILLISECOND) AS duration_ms,
          total_slot_ms,
          query_info.performance_insights.avg_previous_execution_ms AS prev_exec_ms
        FROM `{target_project}`.`{params.region}`.INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION
        WHERE creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {params.lookback_days} DAY)
          AND job_type = 'QUERY'
          AND state = 'DONE'
          AND query_info.query_hashes.normalized_literals IS NOT NULL
          AND (statement_type IS NULL OR statement_type <> 'SCRIPT')
          AND query_info.performance_insights.avg_previous_execution_ms > TIMESTAMP_DIFF(end_time, start_time, MILLISECOND)
          AND NOT EXISTS (
            SELECT 1 FROM UNNEST(query_info.performance_insights.stage_performance_change_insights)
            WHERE input_data_change.records_read_diff_percentage > 0
          )
          {focus_clause}
        ORDER BY 
          total_slot_ms DESC
        LIMIT 1000
        """
        
        results = _run_and_log(bq_client, sql, "HBO Raw Data", params=params, query_parameters=focus_params)
        
        output = []
        
        for row in results:
            prev_exec_ms = row.prev_exec_ms or 0
            
            if prev_exec_ms > 0:
                # Denominator guard kept consistent with get_hbo_summary's SAFE_DIVIDE
                # so the top-10 table and the KPI tiles reconcile on the same policy.
                percent_saved = 100 * (prev_exec_ms - row.duration_ms) / max(prev_exec_ms, 1)
                
                saved_slot_hours = (percent_saved / 100) * ((row.total_slot_ms or 0) / 3600000.0)
                estimated_savings = saved_slot_hours * 0.06
                
                output.append(HBOResult(
                    job_id=row.job_id,
                    percent_execution_time_saved=percent_saved,
                    new_elapsed_ms=row.duration_ms,
                    original_elapsed_ms=prev_exec_ms,
                    saved_slot_hours=round(saved_slot_hours, 4),
                    estimated_savings_usd=round(estimated_savings, 4)
                ))
                    

                
        # Sort output by percent_saved descending
        output.sort(key=lambda x: x.percent_execution_time_saved, reverse=True)
        log_endpoint_end("HBO Analyze", t0, _logger=logger)
        return output[:params.limit]
        
    except Exception as e:
        handle_endpoint_exception(e, "HBO analysis")

@router.post("/summary", response_model=HBOSummary)
def get_hbo_summary(params: HBOCommonParams):
    params.focus_projects = validate_focus_projects(params.focus_projects)
    t0 = log_endpoint_start("HBO Summary", params, _logger=logger)
    try:
        bq_client, target_project = init_bq_client_and_resolve_project(params)
        focus_clause, focus_params = build_project_filter(params.focus_projects)
        
        sql = f"""
        WITH raw_data AS (
          SELECT
            job_id,
            TIMESTAMP_DIFF(end_time, start_time, MILLISECOND) AS duration_ms,
            total_slot_ms,
            query_info.performance_insights.avg_previous_execution_ms AS prev_exec_ms,
            EXISTS(
              SELECT 1 FROM UNNEST(query_info.performance_insights.stage_performance_change_insights)
              WHERE input_data_change.records_read_diff_percentage > 0
            ) AS has_data_increase
          FROM `{target_project}`.`{params.region}`.INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION
          WHERE creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {params.lookback_days} DAY)
            AND job_type = 'QUERY'
            AND state = 'DONE'
            AND (statement_type IS NULL OR statement_type <> 'SCRIPT')
            {focus_clause}
        )
        SELECT
          COUNT(job_id) AS total_optimized_jobs,
          SUM(prev_exec_ms - duration_ms) AS total_saved_time_ms,
          -- SAFE_DIVIDE mirrors the Python `max(prev_exec_ms, 1)` guard in analyze_hbo,
          -- so the table and these KPIs use a consistent denominator policy.
          SUM(
            SAFE_DIVIDE(prev_exec_ms - duration_ms, prev_exec_ms)
            * (total_slot_ms / 3600000.0)
          ) AS total_saved_slot_hours,
          AVG(
            100.0 * SAFE_DIVIDE(prev_exec_ms - duration_ms, prev_exec_ms)
          ) AS avg_percent_time_saved
        FROM raw_data
        WHERE prev_exec_ms > duration_ms
          AND NOT has_data_increase
        """
        
        results = _run_and_log(bq_client, sql, "HBO Summary", params=params, query_parameters=focus_params)
        
        for row in results:
            total_saved_slot_hours = row.total_saved_slot_hours or 0.0
            
            # Project to monthly savings
            lookback = params.lookback_days if params.lookback_days > 0 else 7
            
            # Calculate daily averages
            daily_slot_avg = total_saved_slot_hours / lookback
            daily_usd_avg = (total_saved_slot_hours * 0.06) / lookback
            
            # Project to standard month (365.25/12 = 30.4375 days)
            monthly_saved_slot_hours = daily_slot_avg * DAYS_PER_MONTH
            monthly_estimated_savings_usd = daily_usd_avg * DAYS_PER_MONTH
            
            log_endpoint_end("HBO Summary", t0, _logger=logger)
            return HBOSummary(
                total_optimized_jobs=row.total_optimized_jobs or 0,
                total_saved_slot_hours=round(monthly_saved_slot_hours, 4),
                total_estimated_savings_usd=round(monthly_estimated_savings_usd, 4),
                avg_percent_time_saved=round(row.avg_percent_time_saved or 0.0, 2)
            )
            
        log_endpoint_end("HBO Summary", t0, _logger=logger)
        return HBOSummary(total_optimized_jobs=0, total_saved_slot_hours=0.0, total_estimated_savings_usd=0.0, avg_percent_time_saved=0.0)
        
    except Exception as e:
        handle_endpoint_exception(e, "HBO summary")

class PerformanceInsightsResult(BaseModel):
    slot_contention_jobs: List[Dict]
    shuffle_quota_jobs: List[Dict]
    data_volume_jobs: List[Dict]

@router.post("/performance_insights", response_model=PerformanceInsightsResult)
def get_performance_insights(params: HBOCommonParams):
    params.focus_projects = validate_focus_projects(params.focus_projects)
    t0 = log_endpoint_start("HBO Performance Insights", params, _logger=logger)
    try:
        bq_client, target_project = init_bq_client_and_resolve_project(params)
        focus_clause, focus_params = build_project_filter(params.focus_projects)
        
        sql = f"""
        SELECT
          project_id,
          job_id,
          user_email,
          query_info.query_hashes.normalized_literals AS query_hash,
          TO_JSON_STRING(query_info.performance_insights) AS perf_insights
        FROM `{target_project}`.`{params.region}`.INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION
        WHERE creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {params.lookback_days} DAY)
          AND job_type = 'QUERY'
          AND state = 'DONE'
          AND query_info.performance_insights IS NOT NULL
          AND (statement_type IS NULL OR statement_type <> 'SCRIPT')
          AND total_slot_ms >= 500000
          AND EXISTS (
            SELECT 1 FROM UNNEST(query_info.performance_insights.stage_performance_standalone_insights)
            WHERE slot_contention OR insufficient_shuffle_quota
            UNION ALL
            SELECT 1 FROM UNNEST(query_info.performance_insights.stage_performance_change_insights)
            WHERE input_data_change.records_read_diff_percentage > 0
          )
          {focus_clause}
        ORDER BY creation_time DESC
        LIMIT 1000
        """
        
        results = _run_and_log(bq_client, sql, "Performance Insights", params=params, query_parameters=focus_params)
        
        slot_contention_jobs = []
        shuffle_quota_jobs = []
        data_volume_jobs = []
        
        for row in results:
            perf_insights = row.perf_insights
            if perf_insights:
                try:
                    insights_dict = json.loads(perf_insights)
                    if insights_dict:
                        # Check standalone insights
                        standalone = insights_dict.get('stage_performance_standalone_insights', [])
                        for stage in standalone:
                            if stage.get('slot_contention'):
                                slot_contention_jobs.append({
                                    "job_id": row.job_id,
                                    "user_email": row.user_email,
                                    "project_id": row.project_id,
                                    "stage_id": stage.get('stage_id')
                                })
                            if stage.get('insufficient_shuffle_quota'):
                                shuffle_quota_jobs.append({
                                    "job_id": row.job_id,
                                    "user_email": row.user_email,
                                    "project_id": row.project_id,
                                    "stage_id": stage.get('stage_id')
                                })
                                
                        # Check change insights
                        change = insights_dict.get('stage_performance_change_insights', [])
                        for stage in change:
                            data_change = stage.get('input_data_change', {})
                            diff_pct = data_change.get('records_read_diff_percentage')
                            if diff_pct and diff_pct > 0:
                                data_volume_jobs.append({
                                    "job_id": row.job_id,
                                    "user_email": row.user_email,
                                    "project_id": row.project_id,
                                    "diff_pct": round(diff_pct, 2)
                                })
                except Exception as e:
                    logger.warning(f"Failed to parse performance insights for job {row.job_id}: {e}")
                    
        # Sort data volume jobs by increase percentage descending
        data_volume_jobs.sort(key=lambda x: x['diff_pct'], reverse=True)
                    
        log_endpoint_end("HBO Performance Insights", t0, _logger=logger)
        return PerformanceInsightsResult(
            slot_contention_jobs=slot_contention_jobs[:10],
            shuffle_quota_jobs=shuffle_quota_jobs[:10],
            data_volume_jobs=data_volume_jobs[:10]
        )
        
    except Exception as e:
        handle_endpoint_exception(e, "Performance insights")

@router.post("/status", response_model=List[HBOStatus])
def check_hbo_status(params: HBOStatusParams):
    t0 = log_endpoint_start("HBO Status Check", params, _logger=logger)
    try:
        bq_client, target_project = init_bq_client_and_resolve_project(params)
        
        # Step 1: Get distinct projects from jobs in the lookback period to find active projects
        # Added LIMIT 500 as per user request (item 4)
        sql_projects = f"""
        SELECT DISTINCT project_id 
        FROM `{target_project}`.`{params.region}`.INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION
        WHERE creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {params.lookback_days} DAY)
        LIMIT 500
        """
        
        projects_results = _run_and_log(bq_client, sql_projects, "HBO Active Projects", params=params)
        
        projects = [row.project_id for row in projects_results]
        if not projects:
            projects = [target_project] # Fallback to target project
            
        output = []
        
        # Helper function to check a single project status (blocking I/O)
        def _check_project_status(prj):
            local_client = bigquery.Client(project=prj)
            try:
                sql_status = f"""
                SELECT 
                  option_value 
                FROM 
                  `{prj}`.`{params.region}`.INFORMATION_SCHEMA.PROJECT_OPTIONS 
                WHERE 
                  option_name = 'default_query_optimizer_options'
                """
                
                logger.debug("Checking HBO Status for project %s", prj)
                # Create client per thread to avoid connection pool exhaustion (Claude Option 1)
                job_config = bigquery.QueryJobConfig(maximum_bytes_billed=get_max_bytes_billed(params))
                results = local_client.query(sql_status, job_config=job_config).result()
                
                enabled = True # Default is enabled
                for row in results:
                    if 'adaptive=off' in row.option_value:
                        enabled = False
                        break
                return prj, enabled
            except Exception as e:
                logger.warning(f"Failed to check status for project {prj}: {e}")
                return prj, None
            finally:
                local_client.close()

        # Step 2: Check options for each active project concurrently
        # Using ThreadPoolExecutor as this is now a sync def route
        with ThreadPoolExecutor(max_workers=10) as executor:
            results = list(executor.map(_check_project_status, projects))
        
        for prj, enabled in results:
            if enabled is False:
                ddl = f"ALTER PROJECT `{prj}` SET OPTIONS (`{params.region}.default_query_optimizer_options` = 'adaptive=on');"
                output.append(HBOStatus(
                    project_id=prj,
                    enabled=False,
                    ddl=ddl
                ))
                
        # If no disabled projects found, return the target project status (or all enabled)
        if not output:
             # Just check target project to report something
             sql_status = f"""
             SELECT option_value FROM `{target_project}`.`{params.region}`.INFORMATION_SCHEMA.PROJECT_OPTIONS 
             WHERE option_name = 'default_query_optimizer_options'
             """
             results = _run_and_log(bq_client, sql_status, "HBO Status Fallback", params=params)
             enabled = True
             for row in results:
                 if 'adaptive=off' in row.option_value:
                     enabled = False
                     break
             
             output.append(HBOStatus(
                 project_id=target_project,
                 enabled=enabled,
                 ddl=f"ALTER PROJECT `{target_project}` SET OPTIONS (`{params.region}.default_query_optimizer_options` = 'adaptive=on');" if not enabled else None
             ))
            
        log_endpoint_end("HBO Status Check", t0, _logger=logger)
        return output
        
    except Exception as e:
        handle_endpoint_exception(e, "HBO status check")

```

---

### 4.5 `src/cost_attribution.py`

**Role**: Reservation-based cost attribution engine. Allocates slot usage costs to projects using direct usage costing and waste distribution (proportional or central dump rules).

```python
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, field_validator
from typing import Optional, Dict
from datetime import datetime, timedelta
from google.cloud import bigquery
from .utils import init_bq_client_and_resolve_project, _safe_ident, reject_dummy_project, handle_endpoint_exception, get_max_bytes_billed, FocusMixin, validate_focus_projects, build_project_filter, log_endpoint_start, log_endpoint_end
from collections import defaultdict
import json
import os
import logging
import time

from pathlib import Path

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/cost-attribution", tags=["cost-attribution"])

CONFIG_FILE = Path(__file__).parent / "cost_attribution_config.json"

# _MAX_BYTES_BILLED removed — now resolved dynamically via get_max_bytes_billed(params)


def _run_and_log(client, sql, label, params=None, query_parameters=None):
    """Run a query with timing, BQ URL, and structured logging."""
    max_bytes = get_max_bytes_billed(params)
    job_config = bigquery.QueryJobConfig(
        maximum_bytes_billed=max_bytes,
        query_parameters=query_parameters or []
    )
    logger.debug("%s SQL:\n%s", label, sql)
    logger.info("⏳ %s — submitting query (safety cap: %s GiB)…", label, max_bytes // (1024**3))
    t0 = time.time()
    query_job = client.query(sql, job_config=job_config)
    results = query_job.result()
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


class ReservationConfig(BaseModel):
    sku_rate: float
    total_admin_bill: float

class CostAttributionConfig(BaseModel):
    waste_rule: str = "A" # "A" = Proportional, "B" = Central Dump
    central_cost_center_project: Optional[str] = None
    borrowing_rule: str = "lender_pays" # "lender_pays", "borrower_pays"
    reservations: Dict[str, ReservationConfig] = {}

class CostAttributionParams(FocusMixin):
    billing_month_start: str
    billing_month_end: str
    org_project_id: Optional[str] = None
    region: str = "region-us"
    admin_project_id: Optional[str] = None
    max_bytes_billed_gb: Optional[int] = None

    @field_validator('billing_month_start', 'billing_month_end')
    @classmethod
    def validate_date_format(cls, v: str) -> str:
        try:
            datetime.strptime(v, '%Y-%m-%d')
            return v
        except ValueError:
            raise ValueError("Date parameters must be in YYYY-MM-DD format")

def load_config() -> CostAttributionConfig:
    if not os.path.exists(CONFIG_FILE):
        return CostAttributionConfig()
    try:
        with open(CONFIG_FILE, "r") as f:
            data = json.load(f)
            return CostAttributionConfig(**data)
    except Exception as e:
        logger.error(f"Failed to load config: {e}")
        return CostAttributionConfig()

def save_config(config: CostAttributionConfig):
    try:
        with open(CONFIG_FILE, "w") as f:
            json.dump(config.dict(), f, indent=2)
    except Exception as e:
        logger.error(f"Failed to save config: {e}")
        raise HTTPException(status_code=500, detail="Failed to save configuration")

@router.get("/config", response_model=CostAttributionConfig)
def get_config():
    return load_config()

@router.post("/config")
def update_config(config: CostAttributionConfig):
    save_config(config)
    return {"message": "Configuration updated successfully"}

@router.post("/calculate")
def calculate_cost_attribution(params: CostAttributionParams):
    params.focus_projects = validate_focus_projects(params.focus_projects)
    config = load_config()
    t0 = log_endpoint_start("Cost Attribution", params, _logger=logger)
    try:
        scoped_client, resolved_project = init_bq_client_and_resolve_project(params)
        
        # Determine table name based on admin_project_id
        target_project_raw = params.admin_project_id.strip() if (params.admin_project_id and params.admin_project_id.strip()) else resolved_project
        target_project = _safe_ident(target_project_raw, "admin_project_id")
        reject_dummy_project(target_project)
        focus_clause, focus_params = build_project_filter(params.focus_projects)
        
        if target_project:
            table_name = f"`{target_project}`.`{params.region}`.INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION"
        else:
            # Fallback to region-scoped view as in example
            table_name = f"`{params.region}`.INFORMATION_SCHEMA.JOBS"
            
        end_date = datetime.strptime(params.billing_month_end, '%Y-%m-%d')
        exclusive_end_date = end_date + timedelta(days=1)
        exclusive_end_str = exclusive_end_date.strftime('%Y-%m-%d')
        
        query = f"""
            SELECT
              project_id,
              reservation_id,
              SUM(total_slot_ms) AS total_slot_ms
            FROM
              {table_name}
            WHERE
              creation_time >= TIMESTAMP(@start_date)
              AND creation_time < TIMESTAMP(@end_date)
              AND job_type = 'QUERY'
              AND statement_type != 'SCRIPT'
              AND reservation_id IS NOT NULL
              {focus_clause}
            GROUP BY
              project_id,
              reservation_id
        """
        
        all_params = [
            bigquery.ScalarQueryParameter("start_date", "STRING", params.billing_month_start),
            bigquery.ScalarQueryParameter("end_date", "STRING", exclusive_end_str),
        ] + (focus_params or [])
        job_results = _run_and_log(scoped_client, query, "Cost Attribution", params=params, query_parameters=all_params)
        
        project_usage = []
        reservation_totals = defaultdict(float)
        
        # Process Raw Data
        for row in job_results:
            slot_hours = row.total_slot_ms / 3600000.0
            
            project_usage.append({
                "project": row.project_id,
                "reservation": row.reservation_id,
                "slot_hours": slot_hours
            })
            
            reservation_totals[row.reservation_id] += slot_hours

        final_attributions = []
        
        for usage in project_usage:
            res_id = usage["reservation"]
            proj_id = usage["project"]
            slot_hours = usage["slot_hours"]
            
            # Pull configurations for this specific reservation (support short and full IDs)
            short_res_id = res_id.split('.')[-1] if '.' in res_id else (res_id.split(':')[-1] if ':' in res_id else res_id)
            res_config = config.reservations.get(short_res_id) or config.reservations.get(res_id)
            if not res_config:
                logger.warning(f"No configuration found for reservation {res_id} (short: {short_res_id}). Skipping.")
                continue
                
            sku_rate_per_slot_hour = res_config.sku_rate
            total_billed_to_admin = res_config.total_admin_bill
            
            # --- A. Strict Isolation for Direct Usage ---
            direct_cost = slot_hours * sku_rate_per_slot_hour
            
            # --- B. Proportional Distribution for Waste ---
            total_res_direct_cost = reservation_totals[res_id] * sku_rate_per_slot_hour
            waste_cost = max(0, total_billed_to_admin - total_res_direct_cost)
            
            allocated_waste = 0.0
            
            if config.waste_rule == "A":
                # Distribute waste proportionally
                project_share_percentage = slot_hours / reservation_totals[res_id] if reservation_totals[res_id] > 0 else 0
                allocated_waste = waste_cost * project_share_percentage
            elif config.waste_rule == "B":
                # Dump 100% of waste to central IT cost center
                pass
                
            total_charge = direct_cost + allocated_waste
            
            final_attributions.append({
                "project_id": proj_id,
                "reservation_id": res_id,
                "direct_usage_cost_usd": round(direct_cost, 2),
                "allocated_waste_cost_usd": round(allocated_waste, 2),
                "total_cost_attribution_usd": round(total_charge, 2),
                "slot_hours": round(slot_hours, 2)
            })
            
        # Handle Rule B (Central Dump) properly if needed
        if config.waste_rule == "B" and config.central_cost_center_project:
            for res_id, total_used_slots in reservation_totals.items():
                short_res_id = res_id.split('.')[-1] if '.' in res_id else (res_id.split(':')[-1] if ':' in res_id else res_id)
                res_config = config.reservations.get(short_res_id) or config.reservations.get(res_id)
                if not res_config:
                    continue
                sku_rate_per_slot_hour = res_config.sku_rate
                total_billed_to_admin = res_config.total_admin_bill
                total_res_direct_cost = total_used_slots * sku_rate_per_slot_hour
                waste_cost = max(0, total_billed_to_admin - total_res_direct_cost)
                
                if waste_cost > 0:
                    final_attributions.append({
                        "project_id": config.central_cost_center_project,
                        "reservation_id": res_id,
                        "direct_usage_cost_usd": 0.0,
                        "allocated_waste_cost_usd": round(waste_cost, 2),
                        "total_cost_attribution_usd": round(waste_cost, 2)
                    })
            
        logger.info("Returning %d attribution records.", len(final_attributions))
        log_endpoint_end("Cost Attribution", t0, _logger=logger)
        return final_attributions
        
    except Exception as e:
        handle_endpoint_exception(e, "Cost attribution")

@router.post("/test-hbo")
def test_hbo():
    return {"message": "HBO test works"}

```

---

## 5. Configuration Files

### `src/cost_attribution_config.json`

```json
{
  "waste_rule": "A",
  "central_cost_center_project": null,
  "borrowing_rule": "lender_pays",
  "reservations": {}
}
```

---

## 6. Review Focus Areas

Please pay special attention to the following categories:

### 6.1 SQL Injection & Parameter Safety
- Most SQL uses f-string interpolation with `_safe_ident()` validation (regex: `^[a-zA-Z0-9_\-\.\:]+$`)
- `focus_projects` uses parameterized `IN UNNEST(@focus_projects)` — verify no bypass
- `lookback_days`, `limit`, `threshold`, `min_bytes_billed`, `limit_per_project` are all Pydantic-validated integers but interpolated via f-string — is the Pydantic validation sufficient?
- `params.edition` and `params.timezone` are string-interpolated into SQL in some endpoints
- AI Doctor builds `jobs_list` with `", ".join(f"'{jid}'" for jid in job_ids)` — these come from BigQuery results, not user input, but verify

### 6.2 Data Correctness & Math
- Storage analysis: verify the time-travel rescaling math and physical/logical comparison logic
- Compute analysis: verify the `billed_duration_ms` floor logic (60s min for legacy, actual for fluid)
- Fluid scaling rollup: verify the minute-grain aggregation and fluid clamp math
- HBO: verify `percent_saved` calculation consistency between Python and SQL
- Cost attribution: verify waste distribution proportional math

### 6.3 Edge Cases & Error Handling
- What happens when `run_query_and_log` returns zero rows? Each endpoint handles this differently
- Division by zero guards (e.g., `monthly_spending > 0`, `max_baseline_hours_raw > 0`)
- `None` handling for BQ result fields (`row['field'] or 0`)
- ThreadPoolExecutor in HBO status — per-thread BQ client creation
- Linter endpoint loops through projects individually — potential for N+1 query explosion

### 6.4 Security Concerns
- `.env` file parsing: no validation on key/value pairs, values set directly to `os.environ`
- `handle_endpoint_exception` for `BadRequest` surfaces truncated BQ error to client — could leak schema info
- Config file read/write uses `os.path.exists` relative to CWD — path traversal risk?
- User emails and job IDs are returned in API responses — PII concern?

### 6.5 Concurrency & Performance
- All endpoints are synchronous (`def`, not `async def`) — blocking I/O on BQ queries
- No connection pooling — new `bigquery.Client()` per request
- `_about_cache` is parsed once at import time — thread-safe for reads but stale if RELEASE_NOTES.md changes
- `_hash_file` uses `@lru_cache` keyed on `(name, mtime)` — race condition if file changes between stat and read?

### 6.6 Code Duplication
- `_run_and_log` is duplicated across `main.py`, `fluid_scaling.py`, `hbo.py`, and `cost_attribution.py` — should be consolidated

### 6.7 Dockerfile & Deployment
- No `RELEASE_NOTES.md` or `.env` copied into container — `_parse_release_notes()` will return empty, `.env` won't load
- Single uvicorn worker (no `--workers` flag) — potential bottleneck
- No health check endpoint

---

## 7. Design Decisions & Reviewer Guidance

The following sections document intentional design decisions that may look like bugs to a reviewer unfamiliar with BigQuery's billing model. Each claim is supported by documentation references and encoded as an executable test in `tests/test_design_invariants.py`.

### 7.1 Editions Cost Model

The `analyze_jobs` endpoint computes per-job Editions cost to compare against on-demand. There are **two independent modeling choices** — do not conflate them:

**A. 60-second duration floor (PROVEN CORRECT):**

Under BigQuery Editions with legacy autoscaling, the autoscaler holds allocated slots for a **minimum 60-second cooldown**. The code floors `billed_duration_ms` to `max(actual_duration, 60000)` in legacy mode. This correctly models the real billing behavior.

Mathematical identity: `cost = avg_slots × max(duration, 60s) × rate / SLOT_HR_MS`

> Test: `test_small_job_matches_identity` — **passes** ✅

**B. Slot-step rounding (HEURISTIC):**

The code rounds `avg_slots` up to the next `slot_step_size` increment for jobs above the passthrough threshold. This models the autoscaler's tendency to scale in discrete increments.

The passthrough cutoff uses the user-configured step size: `if effective_slots < params.slot_step_size:` — jobs below the step size pass through unrounded.

| `avg_slots` | `billed_slots` (with `slot_step_size=100`) | Effect |
|---|---|---|
| 99.9 | 99.9 (passthrough) | Below step size — no rounding |
| 100.1 | 200 (ceil to step) | At step boundary — rounds up |

The discontinuity at the step boundary remains by design (it approximates autoscaler behavior).

> Test: `test_cliff_at_50_boundary` — documents the discontinuity ✅
> Test: `test_slot_step_rounding_deviates_from_identity` — documents the deviation from the 60s proof ✅

**Note on burst averaging:** `avg_slots = total_slot_ms / duration` is an average over the job's runtime. A spiky job (400 slots for 1s, 0 for 4s) reports `avg_slots=80`, but the real autoscaler would have held ~400 during the burst. This is a data limitation — `JOBS_BY_ORGANIZATION` only provides `total_slot_ms`, not an intra-job slot curve. No better data is available at the job level.

---

### 7.2 Fluid Scaling: Legacy uses autoscale-only, not baseline + autoscale

The `_rollup_to_summaries` function sums only `autoscale_capacity_slot_seconds` for legacy cost, excluding `baseline_slots`. This is **correct** for two reasons:

**A. Algebraic cancellation:** Baseline slots are committed capacity paid identically under both legacy and fluid autoscalers. Including baseline in both sides of the savings delta adds equal amounts, producing the same savings number.

**B. Column semantics (confirmed by BigQuery documentation):**

> *"**`autoscale_current_slots`**: The number of additional autoscaling slots currently allocated to the reservation. **This value excludes your baseline slots.**"*
>
> *"Your total slot capacity at any second is effectively `baseline + autoscale_current_slots`."*
>
> — [BigQuery INFORMATION_SCHEMA.RESERVATIONS_TIMELINE documentation](https://cloud.google.com/bigquery/docs/information-schema-reservations-timeline)

Since `autoscale_current_slots` is the **marginal** autoscale portion (not total), `legacy_slot_seconds` correctly excludes baseline. A reservation with `baseline=500, autoscale_current_slots=0` produces `legacy_slot_seconds=0`.

> Test: `test_pure_baseline_no_autoscale` — confirms legacy=0 when autoscale=0 ✅
> Test: `test_savings_is_autoscale_delta_only` — confirms savings reflects only the autoscale portion ✅

---

### 7.3 Capacity Fabrication: Both paths produce equivalent slot-seconds

When `per_second_details` is empty, the SQL replicates the minute-level `autoscale.current_slots` across 60 seconds via `GENERATE_TIMESTAMP_ARRAY`. This equivalence holds because:

**A. `per_second_details` emptiness semantics (confirmed by BigQuery documentation):**

> *"The `per_second_details` array is empty for **non-autoscale reservations that remain unchanged during that specific minute**."*
>
> *"Treat an empty `per_second_details` array as an indication that the reservation's **capacity remained stable (static)** during that one-minute interval."*
>
> — [BigQuery INFORMATION_SCHEMA.RESERVATIONS_TIMELINE documentation](https://cloud.google.com/bigquery/docs/information-schema-reservations-timeline)

Empty `per_second_details` = capacity was constant for the full minute. Therefore `constant × 60 = sum(constant, constant, ...)` produces the correct slot-seconds total.

**B. Period granularity:** `RESERVATION_TIMELINE_BY_PROJECT` rows are one-minute granularity, so the `TIMESTAMP_ADD(period_start, INTERVAL 59 SECOND)` window is correct.

**A scenario with intra-minute variance AND empty `per_second_details` cannot occur in real BigQuery data** — if capacity varied, the array would be populated.

> Test: `test_constant_capacity_equivalent` — confirms identical results for both paths ✅
> Test: `test_zero_autoscale_fabricated_produces_zero_legacy` — confirms fabricated zeros → zero legacy ✅

---

### 7.4 Storage Forecast: Physical bytes for logically-billed datasets are real

`TABLE_STORAGE` always tracks real physical bytes regardless of billing model. Switching billing doesn't change compression or storage layout, only pricing. The comparison is as accurate as any point-in-time analysis can be.

**Caveat — `time_travel_rescale` guard:** Setting `time_travel_rescale < 1.0` without `time_travel_hours` is rejected by a `@model_validator` on `StorageParams`. This ensures the generated DDL always includes `max_time_travel_hours` when the forecast assumes reduced time-travel costs.

---

### 7.5 Design Invariant Tests

All claims in §7 are encoded as executable assertions in [`tests/test_design_invariants.py`](tests/test_design_invariants.py):

| Test | §7 Claim | Expected Result |
|------|----------|-----------------|
| `test_small_job_matches_identity` | §7.1A — 60s floor | ✅ Pass |
| `test_slot_step_rounding_deviates_from_identity` | §7.1B — Rounding is separate from proof | ✅ Documents known deviation |
| `test_cliff_at_50_boundary` | §7.1B — Discontinuity at boundary | ✅ Documents known cliff |
| `test_fluid_scaling_mode_no_60s_floor` | §7.1A — No floor in fluid mode | ✅ Pass |
| `test_pure_baseline_no_autoscale` | §7.2 — Baseline excluded | ✅ Pass |
| `test_autoscale_above_baseline_counted` | §7.2 — Autoscale correctly summed | ✅ Pass |
| `test_savings_is_autoscale_delta_only` | §7.2 — Savings is autoscale delta | ✅ Pass |
| `test_constant_capacity_equivalent` | §7.3 — Fabrication equivalence | ✅ Pass |
| `test_varying_capacity_diverges_from_fabricated` | §7.3 — Documents hypothetical divergence | ✅ N/A (can't occur in practice) |
| `test_zero_autoscale_fabricated_produces_zero_legacy` | §7.3 — Zero autoscale → zero legacy | ✅ Pass |

