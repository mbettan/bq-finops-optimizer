from fastapi import FastAPI, HTTPException, Response
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel
from typing import Optional
from google.cloud import bigquery
import os
import logging
import math

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





