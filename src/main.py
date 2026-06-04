from fastapi import FastAPI, HTTPException, Response
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, HTMLResponse
from pydantic import BaseModel, Field
from typing import Optional, List
from google.cloud import bigquery
import hashlib
from functools import lru_cache
from pathlib import Path
import os
import logging
import math
import numpy as np
from collections import defaultdict
import json
import re
import pandas as pd
from google.api_core import exceptions as gax_exc
from cost_attribution import router as cost_attribution_router
from hbo import router as hbo_router
from fluid_scaling import (
    router as fluid_scaling_router,
    _safe_ident,
    _normalize_region,
    _strip_qualifier,
)

app = FastAPI()
app.include_router(cost_attribution_router)
app.include_router(hbo_router)
app.include_router(fluid_scaling_router)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

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
async def read_index():
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

@app.get("/simulator", response_class=HTMLResponse)
async def read_simulator():
    try:
        html = (STATIC_DIR / "simulator.html").read_text(encoding="utf-8")
        import json
        from pathlib import Path
        dummy_file = Path(BASE_DIR) / "docs" / "finops-snapshot_dummy.json"
        if dummy_file.exists():
            data = json.loads(dummy_file.read_text(encoding="utf-8"))
            dummy_json_str = json.dumps(data.get("data", {}))
            html = html.replace("__SIMULATOR_JSON_DATA_PLACEHOLDER__", dummy_json_str)
        else:
            html = html.replace("__SIMULATOR_JSON_DATA_PLACEHOLDER__", "{}")
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
        logger.error(f"Error serving simulator: {e}")
        raise HTTPException(status_code=500, detail="Internal server error reading simulator page")

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
    fluid_scaling: bool = False




# Initialize BigQuery client
try:
    client = bigquery.Client()
except Exception as e:
    logger.error(f"Failed to initialize BigQuery client: {e}")
    client = None

def run_query_and_log(scoped_client: bigquery.Client, sql: str, description: str = "Query"):
    # Safety cap: cancel queries that would scan more than this.
    job_config = bigquery.QueryJobConfig(maximum_bytes_billed=200 * 1024**3)  # 200 GiB
    query_job = scoped_client.query(sql, job_config=job_config)
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

        total_physical_gib = (active_physical_bytes + time_travel_physical_bytes + fail_safe_physical_bytes + long_term_physical_bytes) / GIB_CONVERSION
        
        processed_metrics.append({
            "project_name": row['project_name'],
            "dataset_name": row['dataset_name'],
            "forecast_logical": forecast_logical,
            "forecast_physical": forecast_physical,
            "total_physical_gib": total_physical_gib
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
    _validate_safe_params(params)
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
        
        global_physical_cost = sum(r['forecast_physical'] for r in metrics)
        global_physical_gib = sum(r['total_physical_gib'] for r in metrics)
        effective_pricing_ratio = global_physical_cost / global_physical_gib if global_physical_gib > 0 else 0
        
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
            "org_status": org_status,
            "effective_pricing_ratio": effective_pricing_ratio
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
    _validate_safe_params(params)
    logger.info(f"Job Analysis Request: region={params.region}, org_project_id={params.org_project_id}")
    
    try:
        scoped_client = bigquery.Client(project=params.org_project_id) if params.org_project_id else bigquery.Client()
        
        org_project = params.org_project_id if params.org_project_id else "your-project-id"
        
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
          creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {params.lookback_days}*24 HOUR)
          AND state = 'DONE'
          AND job_type = 'QUERY'
          AND (statement_type != 'SCRIPT' OR statement_type IS NULL)
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
            billed_duration_ms = row['billed_duration_ms'] if params.fluid_scaling else (row['billed_duration_ms'] or 60000)
            
            avg_slots = (slot_ms / actual_duration_ms) if (actual_duration_ms and slot_ms is not None) else 0
            
            # Heuristic 1: Spike Factor for short jobs (Peak Approximation)
            spike_factor = 1.0
            if not params.fluid_scaling and actual_duration_ms < 60000:
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


class HygieneParams(BaseModel):
    org_project_id: Optional[str] = None
    region: str = "region-us"
    limit: int = 20

class HygieneResult(BaseModel):
    dataset: str
    table_name: str
    live_table_gb: float
    time_travel_gb: float
    health_status: str

@app.post("/api/storage/hygiene", response_model=List[HygieneResult])
async def analyze_storage_hygiene(params: HygieneParams):
    _validate_safe_params(params)
    try:
        scoped_client = bigquery.Client(project=params.org_project_id) if params.org_project_id else bigquery.Client()
        
        target_project = params.org_project_id if params.org_project_id else "your-project-id"
        
        sql = f"""
        SELECT
          table_schema AS dataset,
          table_name,
          active_logical_bytes / POW(1024,3) AS live_table_gb,
          time_travel_physical_bytes / POW(1024,3) AS time_travel_gb,
          IF(time_travel_physical_bytes > active_logical_bytes, 'High Churn/Recreate Detected', 'Healthy') as health_status
        FROM `{target_project}`.`{params.region}`.INFORMATION_SCHEMA.TABLE_STORAGE_BY_ORGANIZATION
        WHERE total_physical_bytes > 0
        ORDER BY time_travel_gb DESC
        LIMIT {params.limit}
        """
        
        logger.info(f"Executing Storage Hygiene Query:\n{sql}")
        results = scoped_client.query(sql).result()
        
        output = []
        for row in results:
            output.append(HygieneResult(
                dataset=row.dataset,
                table_name=row.table_name,
                live_table_gb=row.live_table_gb,
                time_travel_gb=row.time_travel_gb,
                health_status=row.health_status
            ))
        return output
        
    except Exception as e:
        logger.error(f"Storage hygiene analysis failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

class DMLAbuseParams(BaseModel):
    org_project_id: Optional[str] = None
    region: str = "region-us"
    lookback_days: int = 1
    threshold: int = 1000

class DMLAbuseResult(BaseModel):
    user_email: str
    project_id: str
    insert_job_count: int
    wasted_slot_hours: float

@app.post("/api/antipatterns/dml", response_model=List[DMLAbuseResult])
async def analyze_dml_abuse(params: DMLAbuseParams):
    _validate_safe_params(params)
    try:
        scoped_client = bigquery.Client(project=params.org_project_id) if params.org_project_id else bigquery.Client()
        
        target_project = params.org_project_id if params.org_project_id else "your-project-id"
        
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
        GROUP BY
          user_email, project_id
        HAVING 
          insert_job_count > {params.threshold}
        ORDER BY
          wasted_slot_hours DESC
        """
        
        logger.info(f"Executing DML Abuse Query:\n{sql}")
        results = scoped_client.query(sql).result()
        
        output = []
        for row in results:
            output.append(DMLAbuseResult(
                user_email=row.user_email,
                project_id=row.project_id,
                insert_job_count=row.insert_job_count,
                wasted_slot_hours=row.wasted_slot_hours
            ))
        return output
        
    except Exception as e:
        logger.error(f"DML abuse analysis failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

class MVCostResult(BaseModel):
    project_id: str
    dataset: str
    table_name: str
    refresh_count: int
    total_slot_hours: float

@app.post("/api/antipatterns/mv", response_model=List[MVCostResult])
async def analyze_mv_costs(params: DMLAbuseParams):
    _validate_safe_params(params)
    try:
        scoped_client = bigquery.Client(project=params.org_project_id) if params.org_project_id else bigquery.Client()
        
        target_project = params.org_project_id if params.org_project_id else "your-project-id"
        
        # 1. Get all Materialized Views
        mv_sql = f"""
        SELECT table_schema, table_name 
        FROM `{target_project}`.`{params.region}`.INFORMATION_SCHEMA.TABLES 
        WHERE table_type = 'MATERIALIZED VIEW'
        """
        logger.info(f"Fetching MVs:\n{mv_sql}")
        mv_results = scoped_client.query(mv_sql).result()
        mvs = {(row.table_schema, row.table_name) for row in mv_results}
        
        if not mvs:
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
        """
        logger.info(f"Fetching jobs for MV check:\n{jobs_sql}")
        jobs_results = scoped_client.query(jobs_sql).result()
        
        # 3. Process in Python
        from collections import defaultdict
        mv_stats = defaultdict(lambda: {"count": 0, "slot_ms": 0})
        
        for row in jobs_results:
            key = (row.dataset_id, row.table_id)
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
        return output
        
    except Exception as e:
        logger.error(f"MV cost analysis failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

class AntiPatternParams(BaseModel):
    org_project_id: Optional[str] = None
    region: str = "region-us"
    lookback_days: int = 7
    limit_per_project: int = 100

class LinterResult(BaseModel):
    project_id: str
    job_id: str
    user_email: str
    query_snippet: str
    abuse_type: str
    billed_gb: float

@app.post("/api/antipatterns/linter", response_model=List[LinterResult])
async def analyze_query_linter(params: AntiPatternParams):
    _validate_safe_params(params)
    try:
        scoped_client = bigquery.Client(project=params.org_project_id) if params.org_project_id else bigquery.Client()
        
        target_project = params.org_project_id if params.org_project_id else "your-project-id"
        
        # 1. Find active projects
        projects_sql = f"""
        SELECT DISTINCT project_id 
        FROM `{target_project}`.`{params.region}`.INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION
        WHERE creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {params.lookback_days} DAY)
          AND job_type = 'QUERY'
          AND project_id IS NOT NULL
        """
        
        logger.info(f"Fetching active projects for linter scan:\n{projects_sql}")
        projects_results = scoped_client.query(projects_sql).result()
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
                results = scoped_client.query(sql).result()
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
        return output
        
    except Exception as e:
        logger.error(f"Query linter failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

class SkewResult(BaseModel):
    project_id: str
    job_id: str
    user_email: str
    stage_name: str
    avg_compute_ms: int
    max_compute_ms: int
    skew_ratio: float

@app.post("/api/antipatterns/skew", response_model=List[SkewResult])
async def analyze_data_skew(params: AntiPatternParams):
    _validate_safe_params(params)
    try:
        scoped_client = bigquery.Client(project=params.org_project_id) if params.org_project_id else bigquery.Client()
        
        target_project = params.org_project_id if params.org_project_id else "your-project-id"
        
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
        
        logger.info(f"Executing Data Skew Query:\n{sql}")
        results = scoped_client.query(sql).result()
        
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
        return output
        
    except Exception as e:
        logger.error(f"Data skew analysis failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

class BatchCandidateResult(BaseModel):
    project_id: str
    job_id: str
    user_email: str
    duration_minutes: float
    total_slot_ms: int
    batch_candidate_reason: str

@app.post("/api/antipatterns/batch_candidates", response_model=List[BatchCandidateResult])
async def analyze_batch_candidates(params: AntiPatternParams):
    _validate_safe_params(params)
    try:
        scoped_client = bigquery.Client(project=params.org_project_id) if params.org_project_id else bigquery.Client()
        
        target_project = params.org_project_id if params.org_project_id else "your-project-id"
        
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
        ORDER BY
          total_slot_ms DESC
        LIMIT {params.limit_per_project}
        """
        
        logger.info(f"Executing Batch Candidate Query:\n{sql}")
        results = scoped_client.query(sql).result()
        
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
        return output
        
    except Exception as e:
        logger.error(f"Batch candidate analysis failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

class AIParams(BaseModel):
    org_project_id: Optional[str] = None
    region: str = "region-us"
    model_name: str
    limit: int = 20

class AIResult(BaseModel):
    job_id: str
    user_email: str
    total_slot_ms: int
    gemini_optimization_advice: str

@app.post("/api/ai/analyze", response_model=List[AIResult])
async def analyze_ai_query(params: AIParams):
    _validate_safe_params(params)
    try:
        scoped_client = bigquery.Client(project=params.org_project_id) if params.org_project_id else bigquery.Client()
        
        target_project = params.org_project_id if params.org_project_id else "your-project-id"
        
        sql = f"""
        WITH expensive_queries AS (
          SELECT
            job_id,
            user_email,
            total_slot_ms,
            query
          FROM
            `{target_project}`.`{params.region}`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
          WHERE
            job_type = 'QUERY'
            AND statement_type = 'SELECT'
            AND creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
          ORDER BY
            total_slot_ms DESC
          LIMIT {params.limit}
        )

        SELECT
          job_id,
          user_email,
          total_slot_ms,
          result AS gemini_optimization_advice
        FROM AI.GENERATE_TEXT(
          MODEL `{params.model_name}`,
          (
            SELECT
              job_id,
              user_email,
              total_slot_ms,
              CONCAT(
                'You are an elite Google Cloud BigQuery Data Engineer. ',
                'Analyze the following SQL query and flag any performance anti-patterns based on these specific rules:\\n',
                '- Avoid SELECT * (especially with LIMIT, as LIMIT does not reduce bytes billed).\\n',
                '- Filter data (WHERE clauses) BEFORE joining tables.\\n',
                '- Avoid CROSS JOINs.\\n',
                '- Use APPROX_COUNT_DISTINCT instead of COUNT(DISTINCT) if applicable.\\n',
                '- Avoid ordering (ORDER BY) a large result set without a LIMIT.\\n',
                '- Do not use REGEXP_CONTAINS if a simple LIKE would work.\\n',
                '- Avoid using ROW_NUMBER() OVER() just to get the latest record; suggest ARRAY_AGG() instead.\\n\\n',
                'If the query violates any of these, provide a clean bulleted list of the violations (without referencing rule numbers and without using markdown bolding) and a 1-sentence fix for each. If the query is perfectly optimized, reply exactly with "NO_ANTI_PATTERNS_FOUND".\\n\\n',
                'SQL Query to Analyze:\\n',
                query
              ) AS prompt
            FROM expensive_queries
          ),
          STRUCT(
            0.1 AS temperature,
            300 AS max_output_tokens
          )
        );
        """
        
        logger.info(f"Executing AI Query Analysis using model {params.model_name}...")
        results = scoped_client.query(sql).result()
        
        output = []
        for row in results:
            advice = row.gemini_optimization_advice or ''
            if "NO_ANTI_PATTERNS_FOUND" not in advice:
                output.append(AIResult(
                    job_id=row.job_id,
                    user_email=row.user_email,
                    total_slot_ms=row.total_slot_ms or 0,
                    gemini_optimization_advice=advice
                ))
        return output
        
    except Exception as e:
        logger.error(f"AI query analysis failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


class BIParams(BaseModel):
    org_project_id: Optional[str] = None
    region: str = "region-us"
    lookback_days: int = 7
    limit: int = 50

class BIResult(BaseModel):
    job_id: str
    user_email: str
    processed_gb: float
    billed_gb: float
    estimated_dollars_saved: float
    bi_engine_mode: str
    failure_reasons: str

@app.post("/api/bi/analyze", response_model=List[BIResult])
async def analyze_bi_engine(params: BIParams):
    _validate_safe_params(params)
    try:
        scoped_client = bigquery.Client(project=params.org_project_id) if params.org_project_id else bigquery.Client()
        
        target_project = params.org_project_id if params.org_project_id else "your-project-id"
        
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
        ORDER BY total_bytes_processed DESC
        LIMIT {params.limit}
        """
        
        logger.info(f"Executing BI Engine Query:\n{sql}")
        results = scoped_client.query(sql).result()
        
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
        return output
        
    except Exception as e:
        logger.error(f"BI engine analysis failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

class GovernanceParams(BaseModel):
    org_project_id: Optional[str] = None
    region: str = "region-us"

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
async def analyze_governance(params: GovernanceParams):
    _validate_safe_params(params)
    try:
        scoped_client = bigquery.Client(project=params.org_project_id) if params.org_project_id else bigquery.Client()
        
        target_project = params.org_project_id if params.org_project_id else "your-project-id"
        
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
        logger.info(f"Executing Expiration Audit Query:\n{exp_sql}")
        exp_results = scoped_client.query(exp_sql).result()
        
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
        GROUP BY 1, 2
        ORDER BY total_bytes DESC
        LIMIT 5
        """
        logger.info(f"Fetching top heavy datasets:\n{top_datasets_sql}")
        top_datasets_results = scoped_client.query(top_datasets_sql).result()
        
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
        return GovernanceResponse(
            expiration_issues=expiration_issues,
            filter_issues=filter_issues
        )
        
    except Exception as e:
        logger.error(f"Governance analysis failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


class MVResult(BaseModel):
    job_id: str
    user_email: str
    mv_name: str
    chosen: bool
    rejected_reason: str

@app.post("/api/mv/analyze", response_model=List[MVResult])
async def analyze_mv_rejections(params: GovernanceParams):
    _validate_safe_params(params)
    try:
        scoped_client = bigquery.Client(project=params.org_project_id) if params.org_project_id else bigquery.Client()
        target_project = params.org_project_id if params.org_project_id else "your-project-id"
        
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
        LIMIT 50
        """
        
        logger.info(f"Executing MV Rejection Query:\\n{sql}")
        results = scoped_client.query(sql).result()
        
        output = []
        for row in results:
            output.append(MVResult(
                job_id=row.job_id,
                user_email=row.user_email,
                mv_name=row.mv_name,
                chosen=row.chosen,
                rejected_reason=row.rejected_reason or ''
            ))
        return output
        
    except Exception as e:
        logger.error(f"MV rejection analysis failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

class WarningResult(BaseModel):
    job_id: str
    user_email: str
    resource_warning: str

@app.post("/api/resource_warnings/analyze", response_model=List[WarningResult])
async def analyze_resource_warnings(params: GovernanceParams):
    _validate_safe_params(params)
    try:
        scoped_client = bigquery.Client(project=params.org_project_id) if params.org_project_id else bigquery.Client()
        target_project = params.org_project_id if params.org_project_id else "your-project-id"
        
        sql = f"""
        SELECT
          job_id,
          user_email,
          query_info.resource_warning
        FROM `{target_project}`.`{params.region}`.INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION
        WHERE query_info.resource_warning IS NOT NULL
        ORDER BY creation_time DESC
        LIMIT 50
        """
        
        logger.info(f"Executing Resource Warning Query:\\n{sql}")
        results = scoped_client.query(sql).result()
        
        output = []
        for row in results:
            output.append(WarningResult(
                job_id=row.job_id,
                user_email=row.user_email,
                resource_warning=row.resource_warning or ''
            ))
        return output
        
    except Exception as e:
        logger.error(f"Resource warning analysis failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


class SlotsParams(BaseModel):
    org_project_id: str
    region: str = "region-us"
    lookback_days: int = 7
    window_minutes: int = 5
    percentile: int = 90
    admin_project_id: Optional[str] = None

@app.post("/api/slots/analyze")
async def analyze_slots(params: SlotsParams):
    _validate_safe_params(params)
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
            CAST(IF(CONTAINS_SUBSTR(reservation_id, ":"), 
               SPLIT(REPLACE(reservation_id, ".", ":"), ":")[OFFSET(0)], 
               NULL) AS STRING) AS admin_project_id,
            ARRAY_REVERSE(SPLIT(REPLACE(reservation_id, ".", ":"), ":"))[OFFSET(0)] AS clean_reservation_id,
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
            CAST(NULL AS STRING) AS admin_project_id,
            'MERGED (Simulated)' AS clean_reservation_id,
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
      edition,
      ignore_idle_slots,
      scaling_mode,
      target_job_concurrency
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
        admin_projects = {row.get('admin_project_id') for row in recommendations_data if row.get('admin_project_id')}
                
        # Fallback to the provided admin_project_id or org_project_id if no specific admin project found
        if not admin_projects:
            if params.admin_project_id:
                admin_projects.add(params.admin_project_id)
            else:
                admin_projects.add(params.org_project_id)
            
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
                options_results = scoped_client.query(options_sql).result()
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
                logger.info(f"SQL QUERY (Reservations):\n{reservations_sql}")
                reservations_results = run_query_and_log(scoped_client, reservations_sql, f"Current Reservations ({admin_proj})")
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
            
        return {
            "recommendations": recommendations_data,
            "current_reservations": current_reservations_data,
            "fairness_enabled": fairness_enabled
        }
        
    except Exception as e:
        logger.error(f"Slots analysis failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


class TieredRecParams(BaseModel):
    org_project_id: str
    region: str = "region-us"
    lookback_days: int = 7

class TieredRecResult(BaseModel):
    reservation_id: str
    aggressive_baseline_p80: int
    balanced_baseline_p95: int
    performance_baseline_max: int
    suggested_autoscale_max: Optional[int] = None
    minutes_observed: Optional[int] = None

@app.post("/api/slots/tiered_recommendations", response_model=List[TieredRecResult])
async def get_tiered_recommendations(params: TieredRecParams):
    _validate_safe_params(params)
    logger.info(f"Tiered Recommendations Request: org_project={params.org_project_id}, region={params.region}, lookback={params.lookback_days}d")
    
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
            `{params.org_project_id}`.`{params.region}`.INFORMATION_SCHEMA.{table_name}
          WHERE
            period_start > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {params.lookback_days} DAY)
          GROUP BY
            period_start, reservation_id
        ),
        minute_usage AS (
          SELECT
            TIMESTAMP_TRUNC(period_start, MINUTE) AS usage_minute,
            reservation_id,
            MAX(concurrent_slots) AS peak_slots_in_minute
          FROM per_second_usage
          GROUP BY usage_minute, reservation_id
        )
        SELECT
          IFNULL(reservation_id, 'default-or-on-demand') AS reservation_id,
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
        scoped_client = bigquery.Client(project=params.org_project_id)
        sql = get_sql("JOBS_TIMELINE_BY_ORGANIZATION")
        logger.info(f"Executing Tiered Recommendations Query (Org Scope):\\n{sql}")
        try:
            results = run_query_and_log(scoped_client, sql, "Tiered Recommendations (Org)")
        except Exception as e:
            if "Access Denied" in str(e) or "does not exist" in str(e):
                logger.warning(f"Org scope failed with access error, falling back to Project scope: {e}")
                sql = get_sql("JOBS_TIMELINE")
                logger.info(f"Executing Tiered Recommendations Query (Project Scope):\\n{sql}")
                results = run_query_and_log(scoped_client, sql, "Tiered Recommendations (Project)")
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
        return output
        
    except Exception as e:
        logger.error(f"Tiered recommendations failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


class SlotUtilizationParams(BaseModel):
    org_project_id: str
    region: str = "region-us"
    lookback_days: int = 7
    timezone: str = "America/New_York"
    resolution: str = "MINUTE"

@app.post("/api/slots/utilization")
async def analyze_slot_utilization(params: SlotUtilizationParams):
    _validate_safe_params(params)
    logger.info(f"Slot Utilization Request: org_project={params.org_project_id}, region={params.region}, lookback={params.lookback_days}d")
    
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
      `{params.org_project_id}`.`{params.region}`.INFORMATION_SCHEMA.JOBS_TIMELINE_BY_ORGANIZATION
    WHERE
      period_start > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {params.lookback_days} DAY)
      AND job_type = 'QUERY'
      AND (statement_type != 'SCRIPT' OR statement_type IS NULL)
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
    _validate_safe_params(params)
    logger.info(f"Slot Simulation Request: org_project={params.org_project_id}, region={params.region}, lookback={params.lookback_days}d")
    
    sql = f"""
    SELECT
      TIMESTAMP_TRUNC(period_start, MINUTE) AS usage_minute,
      SUM(period_slot_ms) / (1000 * 60) AS avg_slots
    FROM `{params.org_project_id}`.`{params.region}`.INFORMATION_SCHEMA.JOBS_TIMELINE_BY_ORGANIZATION
    WHERE 
      period_start >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {params.lookback_days} DAY)
      AND job_type = 'QUERY'
      AND (statement_type != 'SCRIPT' OR statement_type IS NULL)
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
# Constants
# ---------------------------------------------------------------------------

# Allowed BigQuery editions, resolutions, and AI models (validated before interpolation into SQL).
_ALLOWED_EDITIONS = {"STANDARD", "ENTERPRISE", "ENTERPRISE_PLUS"}
_ALLOWED_RESOLUTIONS = {"MINUTE", "HOUR", "DAY"}
_MODEL_NAME_RE = re.compile(r"^[A-Za-z0-9_\-]+\.[A-Za-z0-9_\-]+\.[A-Za-z0-9_\-]+\Z")

_MAX_BYTES_BILLED = 200 * 1024**3  # 200 GiB safety cap
_COOLDOWN_WINDOW_SEC = 60
_MAX_LOOKBACK_DAYS = 30


def _validate_safe_params(params):
    """
    Validate and sanitize common parameters to prevent SQL injection.
    Raises HTTPException(400) on validation failures.
    """
    if hasattr(params, "org_project_id") and params.org_project_id:
        params.org_project_id = _safe_ident(params.org_project_id.strip(), "org_project_id")
    if hasattr(params, "admin_project_id") and params.admin_project_id:
        params.admin_project_id = _safe_ident(params.admin_project_id.strip(), "admin_project_id")
    if hasattr(params, "region") and params.region:
        params.region = _safe_ident(_normalize_region(params.region), "region")
    if hasattr(params, "edition") and params.edition:
        if params.edition not in _ALLOWED_EDITIONS:
            raise HTTPException(400, f"Invalid edition: {params.edition}")
    if hasattr(params, "resolution") and params.resolution:
        if params.resolution not in _ALLOWED_RESOLUTIONS:
            raise HTTPException(400, f"Invalid resolution: {params.resolution}")
    if hasattr(params, "model_name") and params.model_name:
        if not _MODEL_NAME_RE.match(params.model_name):
            raise HTTPException(400, "Invalid model_name; expected project.dataset.model")


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

import uuid
from typing import List, Optional, Set, Tuple

_PATTERN_DISCLAIMER = (
    "Indicative savings assume each job independently incurs a full 60s cooldown. "
    "This overstates real savings when jobs run concurrently or share a reservation. "
    "Use these figures for directional ranking of optimization targets only — not as a "
    "ground-truth financial projection. Values are window-bound (not extrapolated)."
)


class FluidSimParams(BaseModel):
    org_project_id: str
    region: str = "region-us"
    lookback_days: int = Field(default=7, ge=1, le=90)
    edition_slot_hr_rate: float = Field(default=0.06, gt=0)
    cooldown_window: int = Field(default=60, ge=1, le=300)


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
    s = re.sub(r"(_\d+)+$", "_#", s)
    # Collapse repeated runs of _# (e.g. script_job_#_# or script_job_#_#_#) to script_job_#
    s = _REPEAT_HASH_RE.sub("_#", s)
    s = re.sub(r"#+", "#", s)          # collapse ## -> # (adjacent hashes)
    s = re.sub(r"[#_]+$", "_#", s)     # collapse trailing hashes/underscores -> _#
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
ORDER BY total_slot_ms DESC
LIMIT 500000
"""


def _render_sql(template: str, **idents) -> str:
    out = template
    for key, val in idents.items():
        out = out.replace("{" + key + "}", val)
    return out


@app.post("/api/slots/fluid_simulation", response_model=FluidSimResponse)
async def simulate_fluid_scaling(params: FluidSimParams):
    _validate_safe_params(params)
    org_project = params.org_project_id
    region = params.region
    sql = _render_sql(_SQL_JOBS, org_project=org_project, region=region)

    try:
        client = bigquery.Client(project=org_project)
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("lookback_days", "INT64", params.lookback_days),
                bigquery.ScalarQueryParameter("cooldown_window", "INT64", params.cooldown_window),
            ],
            maximum_bytes_billed=200 * 1024**3,
        )
        logger.info(
            "Running fluid_simulation (lookback=%d days, cooldown=%ds)",
            params.lookback_days, params.cooldown_window,
        )
        logger.debug("JOBS SQL:\n%s", sql)

        df = client.query(sql, job_config=job_config).result().to_dataframe(
            create_bqstorage_client=True,
        )

        if df.empty:
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
    except Exception:
        logger.exception("Unexpected error in fluid simulation")
        raise HTTPException(500, "Internal server error")


class SlotActualParams(BaseModel):
    org_project_id: str
    region: str = "region-us"
    lookback_days: int = 7
    timezone: str = "America/New_York"
    edition: str = "ENTERPRISE"
    admin_project_id: Optional[str] = None

@app.post("/api/slots/actual_provisioning")
async def get_actual_provisioning(params: SlotActualParams):
    _validate_safe_params(params)
    logger.info(f"Slot Actual Provisioning Request: org_project={params.org_project_id}, region={params.region}, lookback={params.lookback_days}d")
    
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

    target_project = params.admin_project_id if params.admin_project_id else params.org_project_id

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
    AND edition = "{params.edition}"
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
    AND edition = "{params.edition}"
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
SELECT * FROM merged_slots WHERE change_timestamp >= TIMESTAMP('{start_str}')
"""

    try:
        scoped_client = bigquery.Client(project=params.org_project_id)
        
        logger.info("Executing Aggregated Actual Provisioning Query")
        agg_results = run_query_and_log(scoped_client, agg_sql, "Aggregated Actual Provisioning")
        
        logger.info("Executing Timeline Actual Provisioning Query")
        timeline_results = run_query_and_log(scoped_client, timeline_sql, "Timeline Actual Provisioning")
        
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
            
        return {
            "autoscaled_slot_hours": round(autoscaled_slot_hours, 2),
            "baseline_slot_hours": round(baseline_slot_hours, 2),
            "total_slot_hours": round(total_slot_hours, 2),
            "timeline": timeline_data
        }
        
    except Exception as e:
        logger.error(f"Actual provisioning analysis failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


class PeakSlotsParams(BaseModel):
    org_project_id: str
    region: str = "region-us"
    lookback_days: int = 30

@app.post("/api/slots/peak")
async def get_peak_slots(params: PeakSlotsParams):
    _validate_safe_params(params)
    sql = f"""
    WITH concurrent_usage AS (
        SELECT period_start, SUM(period_slot_ms) / 1000 AS concurrent_slots
        FROM `{params.org_project_id}`.`{params.region}`.INFORMATION_SCHEMA.JOBS_TIMELINE_BY_ORGANIZATION
        WHERE 
          period_start >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {params.lookback_days} DAY)
          AND job_type = 'QUERY'
          AND (statement_type != 'SCRIPT' OR statement_type IS NULL)
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


class SlotProfilerParams(BaseModel):
    org_project_id: str
    region: str = "region-us"
    lookback_days: int = 7
    admin_project_id: Optional[str] = None
    fluid_scaling: bool = False

@app.post("/api/slots/profiler")
async def analyze_workload_profile(params: SlotProfilerParams):
    _validate_safe_params(params)
    logger.info(f"Slot Profiler Request: org_project={params.org_project_id}, region={params.region}, lookback={params.lookback_days}d")
    
    target_project = params.admin_project_id if params.admin_project_id else params.org_project_id
    
    sql = f"""
    WITH hourly_profile AS (
      SELECT
        TIMESTAMP_TRUNC(creation_time, HOUR) AS hour_bucket,
        reservation_id,
        project_id,
        COUNT(*) AS query_count,
        AVG(total_slot_ms / NULLIF(TIMESTAMP_DIFF(end_time, start_time, MILLISECOND), 0)) AS avg_slots_per_query,
        APPROX_QUANTILES(TIMESTAMP_DIFF(end_time, start_time, SECOND), 100)[OFFSET(50)] AS median_duration_seconds
      FROM
        `{target_project}`.`{params.region}`.INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION
      WHERE
        creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {params.lookback_days} DAY)
        AND job_type = 'QUERY'
        AND (statement_type != 'SCRIPT' OR statement_type IS NULL)
        AND reservation_id IS NOT NULL
      GROUP BY
        hour_bucket, reservation_id, project_id
    ),
    flagged_hours AS (
      SELECT
        hour_bucket,
        reservation_id,
        SUM(query_count) AS hourly_queries
      FROM
        hourly_profile
      GROUP BY
        hour_bucket, reservation_id
      HAVING
        hourly_queries > 60 
        AND AVG(avg_slots_per_query) < 100 
        AND AVG(median_duration_seconds) < 5 
    ),
    project_stats AS (
      SELECT
        reservation_id,
        project_id,
        SUM(query_count) AS total_queries,
        ROW_NUMBER() OVER (PARTITION BY reservation_id ORDER BY SUM(query_count) DESC) AS rank
      FROM
        hourly_profile
      JOIN
        flagged_hours
      USING (hour_bucket, reservation_id)
      GROUP BY
        reservation_id, project_id
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
    WITH hourly_profile AS (
      SELECT
        TIMESTAMP_TRUNC(creation_time, HOUR) AS hour_bucket,
        reservation_id,
        project_id,
        COUNT(*) AS query_count,
        AVG(total_slot_ms / NULLIF(TIMESTAMP_DIFF(end_time, start_time, MILLISECOND), 0)) AS avg_slots_per_query,
        APPROX_QUANTILES(TIMESTAMP_DIFF(end_time, start_time, SECOND), 100)[OFFSET(50)] AS median_duration_seconds
      FROM
        `{target_project}`.`{params.region}`.INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION
      WHERE
        creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {params.lookback_days} DAY)
        AND job_type = 'QUERY'
        AND (statement_type != 'SCRIPT' OR statement_type IS NULL)
        AND reservation_id IS NOT NULL
      GROUP BY
        hour_bucket, reservation_id, project_id
    ),
    flagged_hours AS (
      SELECT
        hour_bucket,
        reservation_id,
        SUM(query_count) AS hourly_queries
      FROM
        hourly_profile
      GROUP BY
        hour_bucket, reservation_id
      HAVING
        hourly_queries > 60 
        AND AVG(avg_slots_per_query) < 100 
        AND AVG(median_duration_seconds) < 5 
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
        scoped_client = bigquery.Client(project=params.org_project_id)
        
        logger.info("Executing Profiler Summary Query")
        results = run_query_and_log(scoped_client, sql, "Workload Profiler Summary")
        
        logger.info("Executing Profiler Timeline Query")
        timeline_results = run_query_and_log(scoped_client, timeline_sql, "Workload Profiler Timeline")
        
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
            
        return {
            "summary": profile_records,
            "timeline": timeline_records
        }
        
    except Exception as e:
        logger.error(f"Workload profiler failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/slots/profiler/queries")
async def get_top_profiler_queries(params: SlotProfilerParams):
    _validate_safe_params(params)
    logger.info(f"Slot Profiler Queries Request: org_project={params.org_project_id}, region={params.region}, lookback={params.lookback_days}d")
    
    target_project = params.admin_project_id if params.admin_project_id else params.org_project_id
    
    sql = f"""
    WITH hourly_profile AS (
      SELECT
        TIMESTAMP_TRUNC(creation_time, HOUR) AS hour_bucket,
        reservation_id,
        project_id,
        COUNT(*) AS query_count,
        AVG(total_slot_ms / NULLIF(TIMESTAMP_DIFF(end_time, start_time, MILLISECOND), 0)) AS avg_slots_per_query,
        APPROX_QUANTILES(TIMESTAMP_DIFF(end_time, start_time, SECOND), 100)[OFFSET(50)] AS median_duration_seconds
      FROM
        `{target_project}`.`{params.region}`.INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION
      WHERE
        creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {params.lookback_days} DAY)
        AND job_type = 'QUERY'
        AND (statement_type != 'SCRIPT' OR statement_type IS NULL)
        AND reservation_id IS NOT NULL
      GROUP BY
        hour_bucket, reservation_id, project_id
    ),
    flagged_hours AS (
      SELECT
        hour_bucket,
        reservation_id
      FROM
        hourly_profile
      GROUP BY
        hour_bucket, reservation_id
      HAVING
        SUM(query_count) > 60 
        AND AVG(avg_slots_per_query) < 100 
        AND AVG(median_duration_seconds) < 5 
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
        scoped_client = bigquery.Client(project=params.org_project_id)
        logger.info(f"Profiler Top Queries SQL:\n{sql}")
        results = run_query_and_log(scoped_client, sql, "Profiler Top Queries")
        
        query_records = []
        for row in results:
            query_text = "Query text not found"
            example_job_id = row['example_job_id']
            example_project_id = row['example_project_id']
            
            if example_job_id and example_project_id:
                # Query JOBS_BY_PROJECT to get the query text
                text_sql = f"""
                SELECT query
                FROM `{example_project_id}`.`{params.region}`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
                WHERE job_id = '{example_job_id}'
                LIMIT 1
                """
                try:
                    logger.info(f"Fetching query text for job {example_job_id} in project {example_project_id}")
                    text_results = scoped_client.query(text_sql).result()
                    for text_row in text_results:
                        query_text = text_row['query']
                except Exception as text_err:
                    logger.warning(f"Failed to fetch query text for job {example_job_id}: {text_err}")
                    query_text = f"Error fetching query text: {text_err}"
            
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
            
        return query_records
        
    except Exception as e:
        logger.error(f"Failed to get profiler queries: {e}")
        raise HTTPException(status_code=500, detail=str(e))


class UserProfilerParams(BaseModel):
    org_project_id: str
    region: str = "region-us"
    lookback_days: int = 7
    admin_project_id: Optional[str] = None
    od_price: float = 6.25
    ed_price: float = 0.06

@app.post("/api/users/top_spenders")
async def get_top_spenders(params: UserProfilerParams):
    _validate_safe_params(params)
    logger.info(f"Top Spenders Request: org_project={params.org_project_id}, region={params.region}, lookback={params.lookback_days}d")
    
    target_project = params.admin_project_id if params.admin_project_id else params.org_project_id
    
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
    GROUP BY
      user_email
    ORDER BY
      total_bytes_billed DESC
    LIMIT 50
    """
    
    try:
        scoped_client = bigquery.Client(project=params.org_project_id)
        logger.info(f"Top Spenders SQL:\n{sql}")
        results = run_query_and_log(scoped_client, sql, "Top Spenders")
        
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
            
        return user_records
        
    except Exception as e:
        logger.error(f"Failed to get top spenders: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# -- Dashboard Response models ---------------------------------------------------------

class KpiResponse(BaseModel):
    mtdSpend: float
    mtdSpendDelta: float       # percent change MoM, e.g. 12.5 = +12.5%
    forecastSpend: float
    lastMonthSpend: float
    potentialSavings: float
    opportunityCount: int
    anomalyCount: int


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
async def get_kpis():
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

    For now: return realistic-looking stub data.
    """
    return KpiResponse(
        mtdSpend=42310.00,
        mtdSpendDelta=12.5,
        forecastSpend=58200.00,
        lastMonthSpend=51400.00,
        potentialSavings=12400.00,
        opportunityCount=47,
        anomalyCount=3,
    )


@app.get("/api/dashboard/opportunities", response_model=List[Opportunity])
async def get_opportunities(limit: int = 5):
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
    return [
        Opportunity(label="Switch warehouse_db to physical storage",
                    module="STORAGE", monthlySavings=4200.00,
                    deepLink="#storage?dataset=warehouse_db"),
        Opportunity(label="Move project-analytics-prod to On-Demand",
                    module="COMPUTE", monthlySavings=3100.00,
                    deepLink="#compute?project=project-analytics-prod"),
        Opportunity(label="Reduce Time Travel on events_db to 2 days",
                    module="STORAGE", monthlySavings=2800.00,
                    deepLink="#storage-hygiene?dataset=events_db"),
        Opportunity(label="Right-size reservation 'analytics-pool'",
                    module="CAPACITY", monthlySavings=1400.00,
                    deepLink="#capacity?reservation=analytics-pool"),
        Opportunity(label="Rewrite top SELECT * queries by user@example.com",
                    module="QUERY QUALITY", monthlySavings=900.00,
                    deepLink="#linter?user=user@example.com"),
    ][:limit]


@app.get("/api/dashboard/top-projects", response_model=List[ProjectCost])
async def get_top_projects(limit: int = 5):
    """
    TODO: Use existing Cost Attribution Engine logic.
    Aggregate Direct Usage Cost + Allocated Waste per project for current month.
    Return top `limit` by total cost descending.
    """
    return [
        ProjectCost(projectId="data-warehouse-prod", cost=18400.00),
        ProjectCost(projectId="ml-training-prod",    cost=12900.00),
        ProjectCost(projectId="analytics-prod",      cost=6300.00),
        ProjectCost(projectId="reporting-prod",      cost=3100.00),
        ProjectCost(projectId="dev-sandbox",         cost=1600.00),
    ][:limit]


@app.get("/api/dashboard/anomalies", response_model=List[Anomaly])
async def get_anomalies():
    """
    TODO: Real anomaly detection requires historical baseline.

    For v1, use this simple rule:
      For each project: compare last 7 days spend vs prior 7 days spend.
      Flag if change > 50% in either direction.

    Critical = >100% change. Warning = 50-100% change.

    The `html` field MUST be sanitized server-side. Frontend trusts it.
    """
    return [
        Anomaly(
            severity="critical",
            html="Project <strong>data-warehouse-prod</strong> spend +340% on Nov 14",
            deepLink="#cost-attribution?project=data-warehouse-prod"
        ),
        Anomaly(
            severity="warning",
            html="Reservation <strong>analytics-pool</strong> idle 80% over last 7 days",
            deepLink="#capacity?reservation=analytics-pool"
        ),
        Anomaly(
            severity="warning",
            html="User <strong>etl@svc.gserviceaccount.com</strong> ran 12 SELECT * queries (&gt;100GB)",
            deepLink="#linter?user=etl@svc.gserviceaccount.com"
        ),
    ]


