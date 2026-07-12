from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, field_validator
from typing import Optional, Dict
from datetime import datetime, timedelta
from google.cloud import bigquery
from .utils import init_bq_client_and_resolve_project, _safe_ident, _normalize_region, reject_dummy_project, handle_endpoint_exception, get_max_bytes_billed, FocusMixin, validate_focus_projects, build_project_filter, log_endpoint_start, log_endpoint_end
from collections import defaultdict
import json
import os
import logging
import time

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/cost-attribution", tags=["cost-attribution"])

CONFIG_FILE = "cost_attribution_config.json"

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
    """Load the saved config, or defaults if none has been saved yet.

    A missing file is a legitimate initial state (returns defaults). A file
    that exists but fails to parse/validate is a real problem — callers must
    handle that explicitly rather than have it silently masked as defaults,
    which would make every reservation appear "unconfigured" with no
    indication that the stored config was actually lost/corrupted.
    """
    if not os.path.exists(CONFIG_FILE):
        return CostAttributionConfig()
    with open(CONFIG_FILE, "r") as f:
        data = json.load(f)
    return CostAttributionConfig(**data)

def save_config(config: CostAttributionConfig):
    try:
        with open(CONFIG_FILE, "w") as f:
            json.dump(config.dict(), f, indent=2)
    except Exception as e:
        logger.error(f"Failed to save config: {e}")
        raise HTTPException(status_code=500, detail="Failed to save configuration")

@router.get("/config", response_model=CostAttributionConfig)
def get_config():
    try:
        return load_config()
    except Exception as e:
        logger.error(f"Failed to load cost attribution config: {e}")
        raise HTTPException(status_code=500, detail="Failed to load cost attribution configuration; check server logs.")

@router.post("/config")
def update_config(config: CostAttributionConfig):
    save_config(config)
    return {"message": "Configuration updated successfully"}

@router.post("/calculate")
def calculate_cost_attribution(params: CostAttributionParams):
    params.focus_projects = validate_focus_projects(params.focus_projects)
    t0 = log_endpoint_start("Cost Attribution", params, _logger=logger)
    try:
        config = load_config()
        scoped_client, resolved_project = init_bq_client_and_resolve_project(params)
        
        # Determine table name based on admin_project_id
        target_project_raw = params.admin_project_id.strip() if (params.admin_project_id and params.admin_project_id.strip()) else resolved_project
        target_project = _safe_ident(target_project_raw, "admin_project_id")
        reject_dummy_project(target_project)
        region = _safe_ident(_normalize_region(params.region), "region")
        focus_clause, focus_params = build_project_filter(params.focus_projects)

        if target_project:
            table_name = f"`{target_project}`.`{region}`.INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION"
        else:
            # Fallback to region-scoped view as in example
            table_name = f"`{region}`.INFORMATION_SCHEMA.JOBS"
            
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
