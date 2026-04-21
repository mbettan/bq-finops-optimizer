from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional, Dict
from google.cloud import bigquery
from collections import defaultdict
import json
import os
import logging

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/cost-attribution", tags=["cost-attribution"])

CONFIG_FILE = "cost_attribution_config.json"

class ReservationConfig(BaseModel):
    sku_rate: float
    total_admin_bill: float

class CostAttributionConfig(BaseModel):
    waste_rule: str = "A" # "A" = Proportional, "B" = Central Dump
    central_cost_center_project: Optional[str] = None
    borrowing_rule: str = "lender_pays" # "lender_pays", "borrower_pays"
    reservations: Dict[str, ReservationConfig] = {}

class CostAttributionParams(BaseModel):
    billing_month_start: str
    billing_month_end: str
    org_project_id: Optional[str] = None
    region: str = "region-us"
    admin_project_id: Optional[str] = None

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
async def get_config():
    return load_config()

@router.post("/config")
async def update_config(config: CostAttributionConfig):
    save_config(config)
    return {"message": "Configuration updated successfully"}

@router.post("/calculate")
async def calculate_cost_attribution(params: CostAttributionParams):
    config = load_config()
    
    try:
        # Use org project if provided, else default client
        scoped_client = bigquery.Client(project=params.org_project_id) if params.org_project_id else bigquery.Client()
        
        # Determine table name based on admin_project_id
        target_project = params.admin_project_id if params.admin_project_id else params.org_project_id
        
        if target_project:
            table_name = f"`{target_project}`.`{params.region}`.INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION"
        else:
            # Fallback to region-scoped view as in example
            table_name = f"`{params.region}`.INFORMATION_SCHEMA.JOBS"
            
        from datetime import datetime, timedelta
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
              creation_time >= TIMESTAMP('{params.billing_month_start}')
              AND creation_time < TIMESTAMP('{exclusive_end_str}')
              AND job_type = 'QUERY'
              AND statement_type != 'SCRIPT'
              AND reservation_id IS NOT NULL
            GROUP BY
              project_id,
              reservation_id
        """
        
        logger.info(f"Executing Cost Attribution Query:\n{query}")
        job_results = scoped_client.query(query).result()
        
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
                "total_cost_attribution_usd": round(total_charge, 2)
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
            
        logger.info(f"Returning {len(final_attributions)} attribution records.")
        return final_attributions
        
    except Exception as e:
        logger.error(f"Cost attribution calculation failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))
