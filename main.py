from fastapi import FastAPI, HTTPException, Response
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel
from google.cloud import bigquery
import os
import logging

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
    return FileResponse(os.path.join(BASE_DIR, 'static', 'index.html'))

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
    time_travel_hours: float | None = None
    min_monthly_saving: float = 0.0
    min_monthly_saving_pct: float = 0.0
    region: str = "region-us"
    org_project_id: str | None = None




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




