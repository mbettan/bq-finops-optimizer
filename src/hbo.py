from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from typing import Optional, List, Dict
from google.cloud import bigquery
from .utils import init_bq_client_and_resolve_project, handle_endpoint_exception, get_max_bytes_billed, FocusMixin, validate_focus_projects, build_project_filter, log_endpoint_start, log_endpoint_end, _safe_ident, _normalize_region, DAYS_PER_MONTH, request_id_var, run_query_with_retry_limit
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
    query_job, results = run_query_with_retry_limit(client, sql, job_config, description=label, max_attempts=5)
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

MAX_LOOKBACK_DAYS = 90

class HBOCommonParams(FocusMixin):
    org_project_id: Optional[str] = None
    region: str = "region-us"
    lookback_days: int = Field(default=7, ge=1, le=MAX_LOOKBACK_DAYS)
    max_bytes_billed_gb: Optional[int] = None

class HBOAnalyzeParams(HBOCommonParams):
    limit: int = 10

class HBOStatusParams(HBOCommonParams):
    pass

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
            LIMIT 500
            """
            projects_results = _run_and_log(bq_client, sql_projects, "HBO Active Projects", params=params)
            projects = [row.project_id for row in projects_results]
            projects = [_safe_ident(p, "hbo_active_project_id") for p in projects if p]
        if not projects:
            projects = [_safe_ident(target_project, "hbo_target_project")]
            
        output = []
        
        # Helper function to check a single project status (blocking I/O)
        def _check_project_status(prj):
            local_client = bigquery.Client(project=prj)
            try:
                sql_status = f"""
                SELECT
                  option_value
                FROM
                  `{prj}`.`{region}`.INFORMATION_SCHEMA.PROJECT_OPTIONS
                WHERE
                  option_name = 'default_query_optimizer_options'
                """
                
                logger.debug("Checking HBO Status for project %s", prj)
                # Create client per thread to avoid connection pool exhaustion (Claude Option 1)
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
            finally:
                local_client.close()

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

        # If no disabled projects and no failed checks, return the target project status (or all enabled)
        if not output:
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
