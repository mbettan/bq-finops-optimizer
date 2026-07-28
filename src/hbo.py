from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from typing import Optional, List, Dict
from google.cloud import bigquery
from .utils import init_bq_client_and_resolve_project, handle_endpoint_exception, get_max_bytes_billed, FocusMixin, validate_focus_projects, build_project_filter, log_endpoint_start, log_endpoint_end, _safe_ident, _normalize_region, DAYS_PER_MONTH, request_id_var, run_query_with_retry_limit, run_query_and_log as _run_and_log
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
import json
import logging
import time

logger = logging.getLogger(__name__)



router = APIRouter(prefix="/api/hbo", tags=["hbo"])

# _MAX_BYTES_BILLED removed — now resolved dynamically via get_max_bytes_billed(params)

MAX_LOOKBACK_DAYS = 90

class HBOCommonParams(FocusMixin):
    org_project_id: Optional[str] = None
    region: str = "region-us"
    lookback_days: int = Field(default=7, ge=1, le=MAX_LOOKBACK_DAYS)
    # F11: Parameterize slot-hour price — hardcoded $0.06 is the standard
    # Editions PAYG rate; committed-use pricing is lower.
    price_per_slot_hr: float = Field(default=0.06, gt=0, le=1.0)
    max_bytes_billed_gb: Optional[int] = None

class HBOAnalyzeParams(HBOCommonParams):
    # F13: Bound limit to prevent unbounded result sets
    limit: int = Field(default=10, ge=1, le=1000)

class HBOStatusParams(HBOCommonParams):
    pass

class HBOResult(BaseModel):
    job_id: str
    project_id: str = ""
    creation_time: Optional[str] = None
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
    time_base: str = "monthly_projected"  # slot_hours and USD are monthly-projected; job count is raw lookback window

class HBOStatus(BaseModel):
    project_id: str
    # None = the check itself failed (permission error, transient BigQuery
    # error, etc.) — distinct from a real "enabled" reading. See `error`.
    enabled: Optional[bool] = None
    ddl: Optional[str] = None
    error: Optional[str] = None

@router.post("/analyze", response_model=List[HBOResult])
def analyze_hbo(params: HBOAnalyzeParams):
    params.focus_projects = validate_focus_projects(params.focus_projects)
    t0 = log_endpoint_start("HBO Analyze", params, _logger=logger)
    try:
        bq_client, target_project = init_bq_client_and_resolve_project(params)
        region = _safe_ident(_normalize_region(params.region), "region")
        focus_clause, focus_params = build_project_filter(params.focus_projects)

        sql = f"""
        SELECT
          job_id,
          project_id,
          creation_time,
          user_email,
          query_info.query_hashes.normalized_literals AS query_hash,
          start_time,
          end_time,
          TIMESTAMP_DIFF(end_time, start_time, MILLISECOND) AS duration_ms,
          total_slot_ms,
          query_info.performance_insights.avg_previous_execution_ms AS prev_exec_ms
        FROM `{target_project}`.`{region}`.INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION
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
                # M1: Proportionality assumption — if the query ran N× faster,
                # the pre-optimization run consumed N× more slot-time.
                # saved_slots = post_slots × (prev_ms − dur_ms) / dur_ms
                duration_ms = max(row.duration_ms, 1)  # guard div-by-zero
                saved_slot_hours = ((row.total_slot_ms or 0) / 3600000.0) * (prev_exec_ms - duration_ms) / duration_ms
                estimated_savings = saved_slot_hours * params.price_per_slot_hr

                output.append(HBOResult(
                    job_id=row.job_id,
                    project_id=row.project_id or "",
                    creation_time=row.creation_time.isoformat() if row.creation_time else None,
                    percent_execution_time_saved=round(100 * (prev_exec_ms - duration_ms) / max(prev_exec_ms, 1), 2),
                    new_elapsed_ms=row.duration_ms,
                    original_elapsed_ms=prev_exec_ms,
                    saved_slot_hours=round(saved_slot_hours, 4),
                    estimated_savings_usd=round(estimated_savings, 4),
                ))
                    

                
        # F12: Sort by saved_slot_hours (actual savings impact) instead of
        # percent_saved (which biased toward tiny jobs with high % improvement)
        output.sort(key=lambda x: x.saved_slot_hours, reverse=True)
        log_endpoint_end("HBO Analyze", t0, _logger=logger)
        return output[:params.limit]
        
    except Exception as e:
        handle_endpoint_exception(e, "HBO analysis")


# ---------------------------------------------------------------------------
# Optimization details enrichment — per-project JOBS_BY_PROJECT lookups
# ---------------------------------------------------------------------------

# key -> (category, tooltip). Descriptions sourced from BQ documentation.
# Categories: "hbo" = history-based (requires prior runs),
#             "engine" = engine-level (applied at runtime).
OPTIMIZATION_CATALOG: Dict[str, tuple] = {
    "join_pushdown": ("hbo",
        "Executes selective joins earlier in the pipeline to minimize data processed."),
    "semi_join_reduction": ("hbo",
        "Replicates highly selective joins as semi-joins to reduce data shuffled."),
    "join_commutation": ("hbo",
        "Swaps the left and right sides of joins for more efficient memory and CPU use."),
    "parallelism_adjustment": ("hbo",
        "Dynamically alters stage parallelism to reduce wall-clock latency."),
    "enhanced_vectorization": ("engine",
        "Leverages hardware-specific vectorized execution blocks."),
    "short_query_optimization": ("engine",
        "Fast-tracks simpler queries for ultra-low-latency execution."),
}


class JobRef(BaseModel):
    project_id: str
    job_id: str
    creation_time: str


class OptimizationBadge(BaseModel):
    key: str            # raw key from BigQuery, e.g. "semi_join_reduction"
    label: str          # humanized, e.g. "Semi-Join Reduction"
    category: str       # "hbo" | "engine" | "unknown"
    description: str    # tooltip copy


class JobOptimizations(BaseModel):
    project_id: str
    job_id: str
    # [] means "checked, none applied". None means "could not determine".
    # These are NOT the same — see §1.4 on listAll redaction.
    optimizations: Optional[List[OptimizationBadge]] = None


class OptimizationCoverage(BaseModel):
    source: str = "project_enrichment"   # always enrichment (no org allowlist)
    requested_job_count: int
    resolved_job_count: int
    enriched_projects: List[str]
    inaccessible_projects: List[Dict[str, str]]  # {"project_id": "...", "reason": "..."}


class HBOOptimizationsResult(BaseModel):
    jobs: List[JobOptimizations]
    coverage: OptimizationCoverage


class HBOOptimizationsParams(HBOCommonParams):
    # (project_id, job_id, creation_time) triples from /analyze
    jobs: List[JobRef] = Field(..., max_length=50)


def _humanize(key: str) -> str:
    """Convert snake_case key to Title Case label."""
    return key.replace("_", " ").title()


def _badge(key: str) -> OptimizationBadge:
    """Build a badge from a raw optimization key."""
    entry = OPTIMIZATION_CATALOG.get(key)
    if entry:
        category, description = entry
        return OptimizationBadge(
            key=key, label=_humanize(key),
            category=category, description=description)
    # Unknown keys render, they don't disappear.
    return OptimizationBadge(
        key=key, label=f"Optimization: {_humanize(key)}",
        category="unknown",
        description="Optimization type not yet catalogued.")


def _parse_optimization_keys(raw_json: Optional[str]) -> List[str]:
    """Extract optimization key names from optimization_details JSON.

    Tolerates the shapes we know are plausible:
      {"optimizations": [{"semi_join_reduction": {...}}, ...]}
      {"optimizations": [{"type": "semi_join_reduction", ...}, ...]}
      {"optimizations": ["semi_join_reduction", ...]}
      {"semi_join_reduction": {...}}          (bare object, no wrapper)
    Returns [] for null/empty/unparseable — the caller decides whether that
    means "none applied" or "undetermined".
    """
    if not raw_json:
        return []
    try:
        doc = json.loads(raw_json)
    except (ValueError, TypeError):
        logger.warning("Unparseable optimization_details payload")
        return []
    if not isinstance(doc, dict):
        return []

    entries = doc.get("optimizations", doc)
    keys: List[str] = []

    if isinstance(entries, dict):
        keys = list(entries.keys())
    elif isinstance(entries, list):
        for item in entries:
            if isinstance(item, str):
                keys.append(item)
            elif isinstance(item, dict):
                # {"type": "x"} takes precedence over {"x": {...}}
                t = item.get("type") or item.get("optimization_type")
                if isinstance(t, str):
                    keys.append(t)
                else:
                    keys.extend(item.keys())

    # order-preserving dedup
    seen: set = set()
    out: List[str] = []
    for k in keys:
        if isinstance(k, str) and k and k not in seen:
            seen.add(k)
            out.append(k)
    return out


def _enrich_from_projects(
    region: str,
    by_project: Dict[str, List[JobRef]],
    params,
) -> list:
    """One BigQuery job PER PROJECT — never a UNION ALL across projects.

    See CODEREVIEW.md §9.7: a job is atomic w.r.t. permissions, so one
    403 in a UNION ALL blanks every project including the readable ones.
    """
    req_id = request_id_var.get()

    def _one(entry):
        prj, refs = entry
        token = request_id_var.set(req_id)
        try:
            # H9: construct INSIDE the try. bigquery.Client() raising here
            # must not escape the worker and kill executor.map() for all
            # projects.
            with bigquery.Client(project=prj) as cli:
                safe_prj = _safe_ident(prj, "enrich_project_id")
                sql = f"""
                SELECT
                  job_id,
                  TO_JSON_STRING(query_info.optimization_details) AS opt_json
                FROM `{safe_prj}`.`{region}`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
                WHERE creation_time BETWEEN @lo AND @hi
                  AND job_id IN UNNEST(@job_ids)
                """
                from datetime import datetime
                timestamps = []
                for r in refs:
                    try:
                        timestamps.append(datetime.fromisoformat(r.creation_time.replace("Z", "+00:00")))
                    except (ValueError, AttributeError):
                        pass
                if not timestamps:
                    return prj, {}, None

                qp = [
                    bigquery.ScalarQueryParameter("lo", "TIMESTAMP", min(timestamps)),
                    bigquery.ScalarQueryParameter("hi", "TIMESTAMP", max(timestamps)),
                    bigquery.ArrayQueryParameter("job_ids", "STRING",
                        [r.job_id for r in refs]),
                ]
                rows = _run_and_log(cli, sql, f"HBO Enrich ({prj})",
                                    params=params, query_parameters=qp)
                return prj, {r.job_id: r.opt_json for r in rows}, None
        except Exception as e:
            msg = str(e)
            first = msg.splitlines()[0] if msg else repr(e)   # P12: guard empty str
            logger.warning("HBO enrichment failed for project %s: %s", prj, first)
            return prj, {}, first
        finally:
            request_id_var.reset(token)

    with ThreadPoolExecutor(max_workers=10) as ex:
        return list(ex.map(_one, by_project.items()))


@router.post("/optimizations", response_model=HBOOptimizationsResult)
def get_optimizations(params: HBOOptimizationsParams):
    """Enrich HBO jobs with specific optimization types from JOBS_BY_PROJECT.

    optimization_details is only available in JOBS_BY_PROJECT (not
    JOBS_BY_ORGANIZATION), so we fan out one query per project.
    """
    params.focus_projects = validate_focus_projects(params.focus_projects)
    t0 = log_endpoint_start("HBO Optimizations", params, _logger=logger)
    try:
        region = _safe_ident(_normalize_region(params.region), "region")

        # Group requested jobs by project_id
        by_project: Dict[str, List[JobRef]] = defaultdict(list)
        for ref in params.jobs:
            if ref.project_id and ref.job_id and ref.creation_time:
                by_project[ref.project_id].append(ref)

        if not by_project:
            log_endpoint_end("HBO Optimizations", t0, _logger=logger)
            return HBOOptimizationsResult(
                jobs=[],
                coverage=OptimizationCoverage(
                    requested_job_count=len(params.jobs),
                    resolved_job_count=0,
                    enriched_projects=[],
                    inaccessible_projects=[],
                ),
            )

        # Fan out — one BQ job per project, never UNION ALL
        enrichment_results = _enrich_from_projects(region, by_project, params)

        # Merge results
        project_rows: Dict[str, Dict[str, str]] = {}   # project -> {job_id -> opt_json}
        enriched_projects: List[str] = []
        inaccessible: List[Dict[str, str]] = []

        for prj, rows_dict, error in enrichment_results:
            if error:
                inaccessible.append({"project_id": prj, "reason": error})
            else:
                enriched_projects.append(prj)
                project_rows[prj] = rows_dict

        failed_projects = {item["project_id"] for item in inaccessible}

        jobs_out: List[JobOptimizations] = []
        resolved = 0
        for ref in params.jobs:
            if ref.project_id in failed_projects:
                # Undetermined — project query failed
                jobs_out.append(JobOptimizations(
                    project_id=ref.project_id, job_id=ref.job_id,
                    optimizations=None))
            elif ref.project_id in project_rows:
                opt_json = project_rows[ref.project_id].get(ref.job_id)
                if opt_json is None:
                    # Row absent — listAll redaction or job aged out
                    jobs_out.append(JobOptimizations(
                        project_id=ref.project_id, job_id=ref.job_id,
                        optimizations=None))
                else:
                    parsed = _parse_optimization_keys(opt_json)
                    badges = [_badge(k) for k in parsed]
                    jobs_out.append(JobOptimizations(
                        project_id=ref.project_id, job_id=ref.job_id,
                        optimizations=badges))
                    resolved += 1
            else:
                # Project wasn't in the fan-out (shouldn't happen)
                jobs_out.append(JobOptimizations(
                    project_id=ref.project_id, job_id=ref.job_id,
                    optimizations=None))

        log_endpoint_end("HBO Optimizations", t0, _logger=logger)
        return HBOOptimizationsResult(
            jobs=jobs_out,
            coverage=OptimizationCoverage(
                requested_job_count=len(params.jobs),
                resolved_job_count=resolved,
                enriched_projects=enriched_projects,
                inaccessible_projects=inaccessible,
            ),
        )

    except Exception as e:
        handle_endpoint_exception(e, "HBO optimizations")

@router.post("/summary", response_model=HBOSummary)
def get_hbo_summary(params: HBOCommonParams):
    params.focus_projects = validate_focus_projects(params.focus_projects)
    t0 = log_endpoint_start("HBO Summary", params, _logger=logger)
    try:
        bq_client, target_project = init_bq_client_and_resolve_project(params)
        region = _safe_ident(_normalize_region(params.region), "region")
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
          FROM `{target_project}`.`{region}`.INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION
          WHERE creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {params.lookback_days} DAY)
            AND job_type = 'QUERY'
            AND state = 'DONE'
            AND (statement_type IS NULL OR statement_type <> 'SCRIPT')
            AND query_info.query_hashes.normalized_literals IS NOT NULL
            {focus_clause}
        )
        SELECT
          COUNT(job_id) AS total_optimized_jobs,
          SUM(prev_exec_ms - duration_ms) AS total_saved_time_ms,
          -- M1: Proportionality — saved slots = post_slots × (prev − dur) / dur.
          -- Divides by duration_ms (post-optimisation) to estimate the implied
          -- original slot consumption, matching the Python analyze_hbo formula.
          SUM(
            SAFE_DIVIDE(prev_exec_ms - duration_ms, duration_ms)
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
            daily_usd_avg = (total_saved_slot_hours * params.price_per_slot_hr) / lookback
            
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
        region = _safe_ident(_normalize_region(params.region), "region")
        focus_clause, focus_params = build_project_filter(params.focus_projects)

        sql = f"""
        SELECT
          project_id,
          job_id,
          user_email,
          query_info.query_hashes.normalized_literals AS query_hash,
          TO_JSON_STRING(query_info.performance_insights) AS perf_insights
        FROM `{target_project}`.`{region}`.INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION
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
    params.focus_projects = validate_focus_projects(params.focus_projects)
    t0 = log_endpoint_start("HBO Status Check", params, _logger=logger)
    try:
        bq_client, target_project = init_bq_client_and_resolve_project(params)
        region = _safe_ident(_normalize_region(params.region), "region")

        if params.focus_projects:
            # Focus mode: use the explicitly provided projects
            projects = [_safe_ident(p, "hbo_focus_project_id") for p in params.focus_projects]
        else:
            # Org mode: discover active projects from jobs in the lookback period
            sql_projects = f"""
            SELECT DISTINCT project_id
            FROM `{target_project}`.`{region}`.INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION
            WHERE creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {params.lookback_days} DAY)
            ORDER BY project_id
            LIMIT 501
            """
            projects_results = _run_and_log(bq_client, sql_projects, "HBO Active Projects", params=params)
            projects = [row.project_id for row in projects_results]
            truncated = len(projects) > 500
            if truncated:
                projects = projects[:500]
            projects = [_safe_ident(p, "hbo_active_project_id") for p in projects if p]
        if not projects:
            projects = [_safe_ident(target_project, "hbo_target_project")]
            
        output = []
        
        # Helper function to check a single project status (blocking I/O)
        def _check_project_status(prj):
            try:
                with bigquery.Client(project=prj) as local_client:
                    sql_status = f"""
                    SELECT
                      option_value
                    FROM
                      `{prj}`.`{region}`.INFORMATION_SCHEMA.PROJECT_OPTIONS
                    WHERE
                      option_name = 'default_query_optimizer_options'
                    """
                    
                    logger.debug("Checking HBO Status for project %s", prj)
                    job_config = bigquery.QueryJobConfig(maximum_bytes_billed=get_max_bytes_billed(params))
                    _, results = run_query_with_retry_limit(local_client, sql_status, job_config, description=f"HBO Status ({prj})", max_attempts=3)
                    
                    enabled = True # Default is enabled
                    for row in results:
                        if 'adaptive=off' in row.option_value:
                            enabled = False
                            break
                    return prj, enabled
            except Exception as e:
                logger.warning(f"Failed to check status for project {prj}: {e}")
                return prj, None

        # Step 2: Check options for each active project concurrently
        # Using ThreadPoolExecutor as this is now a sync def route
        req_id = request_id_var.get()
        def check_with_ctx(prj):
            token = request_id_var.set(req_id)
            try:
                return _check_project_status(prj)
            finally:
                request_id_var.reset(token)
            
        with ThreadPoolExecutor(max_workers=10) as executor:
            results = list(executor.map(check_with_ctx, projects))

        for prj, enabled in results:
            if enabled is False:
                ddl = f"ALTER PROJECT `{prj}` SET OPTIONS (`{region}.default_query_optimizer_options` = 'adaptive=on');"
                output.append(HBOStatus(
                    project_id=prj,
                    enabled=False,
                    ddl=ddl
                ))
            elif enabled is None:
                # The check itself failed (permission error, transient BigQuery
                # error, etc.) — report this distinctly rather than silently
                # dropping it, which previously made a failed check
                # indistinguishable from "already enabled".
                output.append(HBOStatus(
                    project_id=prj,
                    enabled=None,
                    error="Could not determine HBO status for this project (permission or query error) — check server logs.",
                ))
            else:
                # M5: Always emit a row for enabled=True projects so the user
                # sees confirmation for their requested scope. Previously,
                # healthy projects were silently dropped, and the fallback
                # could emit a row for target_project (admin project) which
                # may not be in the focus list.
                output.append(HBOStatus(
                    project_id=prj,
                    enabled=True,
                ))

        # M5: Only use the fallback for org mode with no projects discovered.
        # In focus mode, all requested projects are already checked above.
        if not output and not params.focus_projects:
             # Just check target project to report something
             sql_status = f"""
             SELECT option_value FROM `{target_project}`.`{region}`.INFORMATION_SCHEMA.PROJECT_OPTIONS
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
                 ddl=f"ALTER PROJECT `{target_project}` SET OPTIONS (`{region}.default_query_optimizer_options` = 'adaptive=on');" if not enabled else None
             ))
            
        log_endpoint_end("HBO Status Check", t0, _logger=logger)
        return output
        
    except Exception as e:
        handle_endpoint_exception(e, "HBO status check")
