from fastapi import FastAPI, HTTPException, Response
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel
from typing import Optional
from google.cloud import bigquery
import os
import logging
import math
import numpy as np

app = FastAPI()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Configure logging
log_file = os.path.join(BASE_DIR, 'app.log')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Serve index.html at root
@app.get("/")
async def read_index():
    headers = {
        "Cache-Control": "no-cache, no-store, must-revalidate",
        "Pragma": "no-cache",
        "Expires": "0"
    }
    return FileResponse(os.path.join(BASE_DIR, 'static', 'index.html'), headers=headers)

@app.get("/favicon.ico", include_in_schema=False)
async def favicon():
    return Response(status_code=204)

# Mount static files
app.mount("/static", StaticFiles(directory=os.path.join(BASE_DIR, "static")), name="static")

class StorageParams(BaseModel):
    active_logical_price: float = 0.02
    long_term_logical_price: float = 0.01
    active_physical_price: float = 0.04
    long_term_physical_price: float = 0.02
    time_travel_rescale: float = 1.0
    time_travel_hours: Optional[float] = None
    min_monthly_saving: float = 0.0
    min_monthly_saving_pct: float = 0.0
    region: str = "region-us"
    org_project_id: Optional[str] = None

class JobAnalysisParams(BaseModel):
    on_demand_rate_per_tb: float = 6.25
    edition_slot_hr_rate: float = 0.06
    slot_step_size: int = 50
    lookback_days: int = 3
    region: str = "region-us"
    org_project_id: Optional[str] = None
    min_bytes_billed: int = 10485760
    limit_jobs: int = 1000




# Initialize BigQuery client
try:
    client = bigquery.Client()
except Exception as e:
    logger.error(f"Failed to initialize BigQuery client: {e}")
    client = None

def run_query_and_log(scoped_client: bigquery.Client, sql: str, description: str = "Query"):
    query_job = scoped_client.query(sql)
    results = query_job.result()
    bytes_processed = query_job.total_bytes_processed
    bytes_billed = query_job.total_bytes_billed
    cache_hit = query_job.cache_hit
    
    logger.info(f"{description} Profile - Job ID: {query_job.job_id}")
    if bytes_processed is not None:
         logger.info(f"{description} Profile - Bytes Processed: {bytes_processed} ({bytes_processed / (1024**3):.2f} GiB)")
    if bytes_billed is not None:
         logger.info(f"{description} Profile - Bytes Billed: {bytes_billed} ({bytes_billed / (1024**3):.2f} GiB)")
    logger.info(f"{description} Profile - Cache Hit: {cache_hit}")
    return results

def get_storage_metrics(scoped_client: bigquery.Client, params: StorageParams):
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
    GROUP BY 1,2
    """
    logger.info(f"SQL QUERY:\n{sql}")
    results = run_query_and_log(scoped_client, sql, "Storage Metrics")
    
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

        # Derived metrics
        active_no_tt_no_fs_physical_gib = active_physical_gib - time_travel_physical_gib

        # Calculate Costs
        forecast_active_logical_cost = active_logical_gib * params.active_logical_price
        forecast_long_term_logical_cost = long_term_logical_gib * params.long_term_logical_price
        
        forecast_active_no_tt_no_fs_physical_cost = active_no_tt_no_fs_physical_gib * params.active_physical_price
        forecast_travel_physical_cost = time_travel_physical_gib_rescaled * params.active_physical_price
        forecast_failsafe_physical_cost = fail_safe_physical_gib * params.active_physical_price
        forecast_long_term_physical_cost = long_term_physical_gib * params.long_term_physical_price

        # Totals
        forecast_logical = forecast_active_logical_cost + forecast_long_term_logical_cost
        forecast_physical = (forecast_active_no_tt_no_fs_physical_cost + 
                             forecast_travel_physical_cost + 
                             forecast_failsafe_physical_cost + 
                             forecast_long_term_physical_cost)

        processed_metrics.append({
            "project_name": row['project_name'],
            "dataset_name": row['dataset_name'],
            "forecast_logical": forecast_logical,
            "forecast_physical": forecast_physical
        })

    return processed_metrics

def get_physical_datasets(scoped_client: bigquery.Client, projects: set, region: str):
    if not projects:
        return set()

    # Try fast UNION ALL approach
    unions = []
    for p in projects:
        unions.append(f"SELECT '{p}' as project_name, schema_name as dataset_name FROM `{p}.{region}.INFORMATION_SCHEMA.SCHEMATA_OPTIONS` WHERE option_name = 'storage_billing_model' AND option_value = 'PHYSICAL'")
    
    sql = "\nUNION ALL\n".join(unions)
    
    logger.info(f"Trying fast UNION ALL for physical datasets on {len(projects)} projects")
    logger.info(f"SQL QUERY (Fast Path):\n{sql}")
    try:
        results = run_query_and_log(scoped_client, sql, "Physical Datasets (Fast)")
        return {(row['project_name'], row['dataset_name']) for row in results}
    except Exception as e:
        logger.warning(f"Fast UNION ALL failed: {e}. Falling back to loop.")
        
    # Fallback to loop
    physical_datasets = set()
    for p in projects:
        sql = f"SELECT schema_name as dataset_name FROM `{p}.{region}.INFORMATION_SCHEMA.SCHEMATA_OPTIONS` WHERE option_name = 'storage_billing_model' AND option_value = 'PHYSICAL'"
        logger.info(f"SQL QUERY (Fallback Loop):\n{sql}")
        try:
            results = run_query_and_log(scoped_client, sql, f"Physical Datasets (Fallback {p})")
            for row in results:
                physical_datasets.add((p, row['dataset_name']))
        except Exception as e:
            logger.warning(f"Failed to query SCHEMATA_OPTIONS for project {p}: {e}")
            
    return physical_datasets

def get_org_storage_billing_model(scoped_client: bigquery.Client, region: str):
    sql = f"SELECT option_value FROM `{region}`.INFORMATION_SCHEMA.ORGANIZATION_OPTIONS WHERE option_name = 'default_storage_billing_model'"
    logger.info(f"Checking Organization Default Storage Billing Model for {region}")
    logger.info(f"SQL QUERY:\n{sql}")
    try:
        results = run_query_and_log(scoped_client, sql, "Org Storage Billing Model")
        for row in results:
            return row['option_value']
    except Exception as e:
        logger.warning(f"Failed to query ORGANIZATION_OPTIONS: {e}. Assuming LOGICAL or not set.")
    return "LOGICAL"

@app.post("/api/storage/analyze")
async def analyze_storage(params: StorageParams):
    if params.org_project_id:
        params.org_project_id = params.org_project_id.strip()
    logger.info(f"Storage Analysis Request: region={params.region}, org_project_id={params.org_project_id}")
    
    try:
        scoped_client = bigquery.Client(project=params.org_project_id) if params.org_project_id else bigquery.Client()
        
        org_billing_model = get_org_storage_billing_model(scoped_client, params.region)
        org_status = {
            "current_model": org_billing_model,
            "is_optimized": org_billing_model == "PHYSICAL",
            "ddl": f"ALTER ORGANIZATION SET OPTIONS (`{params.region}.default_storage_billing_model`='PHYSICAL');" if org_billing_model != "PHYSICAL" else None
        }
        
        metrics = get_storage_metrics(scoped_client, params)
        projects = {row['project_name'] for row in metrics}
        physical_datasets = get_physical_datasets(scoped_client, projects, params.region)
        
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
        return {
            "datasets": processed_data,
            "org_status": org_status
        }
        
    except Exception as e:
        if "hasn't been enabled" in str(e):
            logger.warning(f"Storage view not enabled for {params.region}: {e}")
            project_id = params.org_project_id if params.org_project_id else "your-project-id"
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
        logger.error(f"Storage analysis failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/jobs/analyze")
async def analyze_jobs(params: JobAnalysisParams):
    if params.org_project_id:
        params.org_project_id = params.org_project_id.strip()
    logger.info(f"Job Analysis Request: region={params.region}, org_project_id={params.org_project_id}")
    
    try:
        scoped_client = bigquery.Client(project=params.org_project_id) if params.org_project_id else bigquery.Client()
        
        org_project = params.org_project_id if params.org_project_id else "your-project-id"
        
        sql = f"""
        SELECT
          job_id,
          user_email,
          project_id,
          COALESCE(total_bytes_billed, 0) AS total_bytes_billed,
          total_slot_ms,
          CASE WHEN error_result IS NOT NULL THEN TRUE ELSE FALSE END AS has_error,
          NULLIF(TIMESTAMP_DIFF(end_time, start_time, MILLISECOND), 0) AS actual_duration_ms,
          GREATEST(TIMESTAMP_DIFF(end_time, start_time, MILLISECOND), 60000) AS billed_duration_ms
        FROM
          `{org_project}`.`{params.region}`.INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION
        WHERE
          creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {params.lookback_days}*24 HOUR)
          AND state = 'DONE'
          AND job_type = 'QUERY'
          AND IFNULL(cache_hit, FALSE) = FALSE
          AND total_bytes_billed >= {params.min_bytes_billed}
        ORDER BY total_bytes_billed DESC
        LIMIT {params.limit_jobs}
        """
        
        logger.info(f"Job Analyzer SQL QUERY:\n{sql}")
        results = run_query_and_log(scoped_client, sql, "Job Stats")
        
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
            billed_duration_ms = row['billed_duration_ms'] or 60000
            
            avg_slots = (slot_ms / actual_duration_ms) if (actual_duration_ms and slot_ms is not None) else 0
            
            # Heuristic 1: Spike Factor for short jobs (Peak Approximation)
            spike_factor = 1.0
            if actual_duration_ms < 60000:
                # Scales from 3.0 at 0ms to 1.0 at 60s
                spike_factor = 1.0 + 2.0 * (1.0 - (actual_duration_ms / 60000.0))
            
            effective_slots = avg_slots * spike_factor
            
            # Heuristic 2: Slot Sharing Discount for small queries
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
        
        return {
            "project_summaries": project_list,
            "top_jobs": top_candidates
        }
        
    except Exception as e:
        logger.error(f"Job analysis failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


class SlotsParams(BaseModel):
    org_project_id: str
    region: str = "region-us"
    lookback_days: int = 7
    window_minutes: int = 5
    percentile: int = 90

@app.post("/api/slots/analyze")
async def analyze_slots(params: SlotsParams):
    logger.info(f"Slots Analysis Request: org_project={params.org_project_id}, region={params.region}, window={params.window_minutes}m, P{params.percentile}")
    
    window_seconds = params.window_minutes * 60
    
    recommendations_sql = f"""
    WITH per_second_usage AS (
        SELECT
          period_start,
          reservation_id,
          SUM(period_slot_ms) / 1000 AS concurrent_slots
        FROM
          `{params.org_project_id}`.`{params.region}`.INFORMATION_SCHEMA.JOBS_TIMELINE_BY_ORGANIZATION
        WHERE 
          period_start > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {params.lookback_days} DAY)
          AND reservation_id IS NOT NULL
        GROUP BY
          period_start, reservation_id
    ),
    windowed_stats AS (
        SELECT
          TIMESTAMP_SECONDS(DIV(UNIX_SECONDS(period_start), {window_seconds}) * {window_seconds}) AS window_start,
          reservation_id,
          SUM(concurrent_slots) / {window_seconds} AS avg_slots,
          MAX(concurrent_slots) AS max_slots
        FROM per_second_usage
        GROUP BY window_start, reservation_id
    ),
    per_res AS (
        SELECT 
            reservation_id,
            APPROX_QUANTILES(avg_slots, 100)[OFFSET({params.percentile})] AS recommended_baseline,
            APPROX_QUANTILES(max_slots, 100)[OFFSET(90)] AS recommended_max_p90,
            APPROX_QUANTILES(max_slots, 100)[OFFSET(99)] AS recommended_max_p99,
            MAX(max_slots) AS recommended_max_peak
        FROM 
            windowed_stats
        GROUP BY 
            reservation_id
    ),
    merged_per_second AS (
        SELECT
          period_start,
          SUM(concurrent_slots) AS concurrent_slots
        FROM
          per_second_usage
        GROUP BY
          period_start
    ),
    merged_windowed AS (
        SELECT
          TIMESTAMP_SECONDS(DIV(UNIX_SECONDS(period_start), {window_seconds}) * {window_seconds}) AS window_start,
          SUM(concurrent_slots) / {window_seconds} AS avg_slots,
          MAX(concurrent_slots) AS max_slots
        FROM merged_per_second
        GROUP BY window_start
    ),
    merged_res AS (
        SELECT 
            'MERGED (Simulated)' AS reservation_id,
            APPROX_QUANTILES(avg_slots, 100)[OFFSET({params.percentile})] AS recommended_baseline,
            APPROX_QUANTILES(max_slots, 100)[OFFSET(90)] AS recommended_max_p90,
            APPROX_QUANTILES(max_slots, 100)[OFFSET(99)] AS recommended_max_p99,
            MAX(max_slots) AS recommended_max_peak
        FROM 
            merged_windowed
    )
    SELECT * FROM per_res
    UNION ALL
    SELECT * FROM merged_res
    """
    
    reservations_sql = f"""
    SELECT
      reservation_name AS reservation_id,
      slot_capacity AS current_baseline,
      autoscale.max_slots AS current_max_slots,
      edition
    FROM
      `{params.org_project_id}`.`{params.region}`.INFORMATION_SCHEMA.RESERVATIONS
    """
    
    logger.info(f"Executing Slots Recommendations Query")
    logger.info(f"SQL QUERY (Recommendations):\n{recommendations_sql}")
    
    try:
        scoped_client = bigquery.Client(project=params.org_project_id)
        
        recommendations_results = run_query_and_log(scoped_client, recommendations_sql, "Slots Recommendations")
        recommendations_data = []
        for row in recommendations_results:
            d = dict(row)
            for key in ['recommended_baseline', 'recommended_max_p90', 'recommended_max_p99', 'recommended_max_peak']:
                if key in d and d[key] is not None:
                    d[key] = int(round(d[key] / 50.0) * 50)
            recommendations_data.append(d)
        
        current_reservations_data = []
        
        # Extract admin projects from reservation IDs in recommendations
        admin_projects = set()
        for row in recommendations_data:
            res_id = row.get('reservation_id')
            if res_id and ':' in res_id:
                # Format is usually project_id:region.reservation_name
                parts = res_id.split(':')
                admin_projects.add(parts[0])
                
        # Fallback to the provided org_project_id if no specific admin project found
        if not admin_projects:
            admin_projects.add(params.org_project_id)
            
        for admin_proj in admin_projects:
            reservations_sql = f"""
            SELECT
              reservation_name AS reservation_id,
              slot_capacity AS current_baseline,
              autoscale.max_slots AS current_max_slots,
              edition
            FROM
              `{admin_proj}`.`{params.region}`.INFORMATION_SCHEMA.RESERVATIONS
            """
            try:
                logger.info(f"Executing Current Reservations Query for project {admin_proj}")
                logger.info(f"SQL QUERY (Reservations):\n{reservations_sql}")
                reservations_results = run_query_and_log(scoped_client, reservations_sql, f"Current Reservations ({admin_proj})")
                for row in reservations_results:
                    d = dict(row)
                    d['admin_project_id'] = admin_proj
                    d['region'] = params.region
                    current_reservations_data.append(d)
            except Exception as res_err:
                logger.warning(f"Failed to query RESERVATIONS in {admin_proj}: {res_err}")
            
        return {
            "recommendations": recommendations_data,
            "current_reservations": current_reservations_data
        }
        
    except Exception as e:
        logger.error(f"Slots analysis failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


class SlotUtilizationParams(BaseModel):
    org_project_id: str
    region: str = "region-us"
    lookback_days: int = 7
    timezone: str = "America/New_York"

@app.post("/api/slots/utilization")
async def analyze_slot_utilization(params: SlotUtilizationParams):
    logger.info(f"Slot Utilization Request: org_project={params.org_project_id}, region={params.region}, lookback={params.lookback_days}d")
    
    sql = f"""
    SELECT
      TIMESTAMP_TRUNC(period_start, MINUTE) AS period_min,
      SUM(period_slot_ms) / 1000 / 60 AS time_average,
      MAX(period_slot_ms / 1000) AS max_slots,
      APPROX_QUANTILES(period_slot_ms / 1000, 100)[OFFSET(90)] AS p90_slots,
      APPROX_QUANTILES(period_slot_ms / 1000, 100)[OFFSET(99)] AS p99_slots,
      SUM(total_bytes_billed) / 60 AS bytes_billed_avg,
      SUM(total_bytes_processed) / 60 AS bytes_processed_avg
    FROM
      `{params.org_project_id}`.`{params.region}`.INFORMATION_SCHEMA.JOBS_TIMELINE_BY_ORGANIZATION
    WHERE
      period_start > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {params.lookback_days} DAY)
      AND job_type = 'QUERY'
      AND (statement_type <> 'SCRIPT' AND statement_type IS NOT NULL)
    GROUP BY
      period_min
    ORDER BY period_min ASC
    """
    
    logger.info(f"Executing Slot Utilization Query")
    logger.info(f"SQL QUERY:\n{sql}")
    
    try:
        from zoneinfo import ZoneInfo
        try:
            tz = ZoneInfo(params.timezone)
        except Exception:
            raise HTTPException(status_code=400, detail=f"Invalid timezone: {params.timezone}")

        scoped_client = bigquery.Client(project=params.org_project_id)
        results = run_query_and_log(scoped_client, sql, "Slot Utilization Raw Data")
        
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
        
        return processed_results
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Slot utilization analysis failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

class SlotSimulationParams(BaseModel):
    org_project_id: str
    region: str = "region-us"
    lookback_days: int = 7
    timezone: str = "America/New_York"
    max_baseline: int = 10000
    step_size: int = 50
    payg_price: float = 0.06
    commit_1yr_price: float = 0.048
    commit_3yr_price: float = 0.036

@app.post("/api/slots/simulate")
async def simulate_slots(params: SlotSimulationParams):
    logger.info(f"Slot Simulation Request: org_project={params.org_project_id}, region={params.region}, lookback={params.lookback_days}d")
    
    sql = f"""
    SELECT
      TIMESTAMP_TRUNC(period_start, MINUTE) AS usage_minute,
      SUM(period_slot_ms) / (1000 * 60) AS avg_slots
    FROM `{params.org_project_id}`.`{params.region}`.INFORMATION_SCHEMA.JOBS_TIMELINE_BY_ORGANIZATION
    WHERE 
      period_start >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {params.lookback_days} DAY)
      AND job_type = 'QUERY'
      AND (statement_type <> 'SCRIPT' AND statement_type IS NOT NULL)
    GROUP BY 1
    ORDER BY 1 ASC
    """
    
    logger.info(f"Executing Slot Simulation Raw Data Query")
    
    try:
        scoped_client = bigquery.Client(project=params.org_project_id)
        results = run_query_and_log(scoped_client, sql, "Slot Simulation Raw Data")
        
        avg_slots_list = [float(row['avg_slots'] or 0.0) for row in results]
        avg_slots_array = np.array(avg_slots_list)
        if len(avg_slots_array) == 0:
            return []
            
        # Time calculations
        actual_hours_in_data = params.lookback_days * 24.0
        actual_minutes_in_data = actual_hours_in_data * 60.0
        
        # BQ Editions are billed on a standard 730-hour month. 
        # We calculate a multiplier to project the X days of data into a full 30.41-day month.
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
            idle_slot_hours_raw = autoscale_slot_hours_raw - (sum_all_slots - (actual_minutes_in_data * baseline)) / 60.0
            idle_slot_hours_raw = max(0, idle_slot_hours_raw)
            
            used_baseline_hours_raw = max_baseline_hours_raw - idle_slot_hours_raw
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
        return processed_results
        
    except Exception as e:
        logger.error(f"Slot simulation failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

class PeakSlotsParams(BaseModel):
    org_project_id: str
    region: str = "region-us"
    lookback_days: int = 30

@app.post("/api/slots/peak")
async def get_peak_slots(params: PeakSlotsParams):
    sql = f"""
    WITH concurrent_usage AS (
        SELECT period_start, SUM(period_slot_ms) / 1000 AS concurrent_slots
        FROM `{params.org_project_id}`.`{params.region}`.INFORMATION_SCHEMA.JOBS_TIMELINE_BY_ORGANIZATION
        WHERE 
          period_start >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {params.lookback_days} DAY)
          AND job_type = 'QUERY'
          AND (statement_type <> 'SCRIPT' AND statement_type IS NOT NULL)
        GROUP BY 1
    )
    SELECT MAX(concurrent_slots) AS peak_slots FROM concurrent_usage
    """
    
    try:
        scoped_client = bigquery.Client(project=params.org_project_id)
        results = run_query_and_log(scoped_client, sql, "Get Peak Slots")
        
        peak_slots = 0
        for row in results:
            peak_slots = float(row['peak_slots']) if row['peak_slots'] else 0
            
        return {"peak_slots": peak_slots}
        
    except Exception as e:
        logger.error(f"Failed to get peak slots: {e}")
        raise HTTPException(status_code=500, detail=str(e))


