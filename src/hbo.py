from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional, List, Dict
from google.cloud import bigquery
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
import json
import logging

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/hbo", tags=["hbo"])

# Safety cap: cancel queries that would scan more than this.
_MAX_BYTES_BILLED = 200 * 1024**3  # 200 GiB

class HBOCommonParams(BaseModel):
    org_project_id: str
    region: str = "region-us"
    lookback_days: int = 7

class HBOAnalyzeParams(HBOCommonParams):
    limit: int = 10

class HBOStatusParams(BaseModel):
    org_project_id: str
    region: str = "region-us"
    lookback_days: int = 7

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
    try:
        bq_client = bigquery.Client(project=params.org_project_id)
        target_project = params.org_project_id
        
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
        ORDER BY 
          total_slot_ms DESC
        LIMIT 1000
        """
        
        logger.info(f"Executing HBO Raw Data Query (Org Scope):\n{sql}")
        job_config = bigquery.QueryJobConfig(maximum_bytes_billed=_MAX_BYTES_BILLED)
        results = bq_client.query(sql, job_config=job_config).result()
        
        output = []
        
        for row in results:
            prev_exec_ms = row.prev_exec_ms or 0
            
            if prev_exec_ms > 0:
                # Use max to avoid division by zero as suggested by Claude
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
        return output[:params.limit]
        
    except Exception as e:
        logger.error(f"HBO analysis failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/summary", response_model=HBOSummary)
def get_hbo_summary(params: HBOCommonParams):
    try:
        bq_client = bigquery.Client(project=params.org_project_id)
        target_project = params.org_project_id
        
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
        )
        SELECT
          COUNT(job_id) AS total_optimized_jobs,
          SUM(prev_exec_ms - duration_ms) AS total_saved_time_ms,
          SUM(((prev_exec_ms - duration_ms) / prev_exec_ms) * (total_slot_ms / 3600000.0)) AS total_saved_slot_hours,
          AVG(100.0 * (prev_exec_ms - duration_ms) / prev_exec_ms) AS avg_percent_time_saved
        FROM raw_data
        WHERE prev_exec_ms > duration_ms
          AND NOT has_data_increase
        """
        
        logger.info(f"Executing HBO Summary Query:\n{sql}")
        job_config = bigquery.QueryJobConfig(maximum_bytes_billed=_MAX_BYTES_BILLED)
        results = bq_client.query(sql, job_config=job_config).result()
        
        for row in results:
            total_saved_slot_hours = row.total_saved_slot_hours or 0.0
            
            # Project to monthly savings
            lookback = params.lookback_days if params.lookback_days > 0 else 7
            
            # Calculate daily averages
            daily_slot_avg = total_saved_slot_hours / lookback
            daily_usd_avg = (total_saved_slot_hours * 0.06) / lookback
            
            # Project to standard month (30.41 days)
            monthly_saved_slot_hours = daily_slot_avg * 30.41
            monthly_estimated_savings_usd = daily_usd_avg * 30.41
            
            return HBOSummary(
                total_optimized_jobs=row.total_optimized_jobs or 0,
                total_saved_slot_hours=round(monthly_saved_slot_hours, 4),
                total_estimated_savings_usd=round(monthly_estimated_savings_usd, 4),
                avg_percent_time_saved=round(row.avg_percent_time_saved or 0.0, 2)
            )
            
        return HBOSummary(total_optimized_jobs=0, total_saved_slot_hours=0.0, total_estimated_savings_usd=0.0, avg_percent_time_saved=0.0)
        
    except Exception as e:
        logger.error(f"HBO summary failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

class PerformanceInsightsResult(BaseModel):
    slot_contention_jobs: List[Dict]
    shuffle_quota_jobs: List[Dict]
    data_volume_jobs: List[Dict]

@router.post("/performance_insights", response_model=PerformanceInsightsResult)
def get_performance_insights(params: HBOCommonParams):
    try:
        bq_client = bigquery.Client(project=params.org_project_id)
        target_project = params.org_project_id
        
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
        ORDER BY creation_time DESC
        LIMIT 1000
        """
        
        logger.info(f"Executing Performance Insights Query:\n{sql}")
        job_config = bigquery.QueryJobConfig(maximum_bytes_billed=_MAX_BYTES_BILLED)
        results = bq_client.query(sql, job_config=job_config).result()
        
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
                    
        return PerformanceInsightsResult(
            slot_contention_jobs=slot_contention_jobs[:10],
            shuffle_quota_jobs=shuffle_quota_jobs[:10],
            data_volume_jobs=data_volume_jobs[:10]
        )
        
    except Exception as e:
        logger.error(f"Performance insights failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/status", response_model=List[HBOStatus])
def check_hbo_status(params: HBOStatusParams):
    try:
        bq_client = bigquery.Client(project=params.org_project_id)
        target_project = params.org_project_id
        
        # Step 1: Get distinct projects from jobs in the lookback period to find active projects
        # Added LIMIT 500 as per user request (item 4)
        sql_projects = f"""
        SELECT DISTINCT project_id 
        FROM `{target_project}`.`{params.region}`.INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION
        WHERE creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {params.lookback_days} DAY)
        LIMIT 500
        """
        
        logger.info(f"Getting active projects from org jobs:\n{sql_projects}")
        job_config = bigquery.QueryJobConfig(maximum_bytes_billed=_MAX_BYTES_BILLED)
        projects_results = bq_client.query(sql_projects, job_config=job_config).result()
        
        projects = [row.project_id for row in projects_results]
        if not projects:
            projects = [target_project] # Fallback to target project
            
        output = []
        
        # Helper function to check a single project status (blocking I/O)
        def _check_project_status(prj):
            local_client = bigquery.Client(project=params.org_project_id)
            try:
                sql_status = f"""
                SELECT 
                  option_value 
                FROM 
                  `{prj}`.`{params.region}`.INFORMATION_SCHEMA.PROJECT_OPTIONS 
                WHERE 
                  option_name = 'default_query_optimizer_options'
                """
                
                logger.info(f"Checking HBO Status for project {prj}:\n{sql_status}")
                # Create client per thread to avoid connection pool exhaustion (Claude Option 1)
                job_config = bigquery.QueryJobConfig(maximum_bytes_billed=_MAX_BYTES_BILLED)
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
             job_config = bigquery.QueryJobConfig(maximum_bytes_billed=_MAX_BYTES_BILLED)
             results = bq_client.query(sql_status, job_config=job_config).result()
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
            
        return output
        
    except Exception as e:
        logger.error(f"HBO status check failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))
