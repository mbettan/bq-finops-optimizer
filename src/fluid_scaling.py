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
from .utils import (
    init_bq_client_and_resolve_project,
    reject_dummy_project,
    _safe_ident,
    _normalize_region,
    get_max_bytes_billed,
    FocusMixin,
    OrgParams,
    validate_focus_projects,
    build_project_filter,
    log_endpoint_start,
    log_endpoint_end,
    DAYS_PER_MONTH,
    run_query_and_log as _run_and_log,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/fluid-scaling", tags=["fluid-scaling"])


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------


DAYS_PER_YEAR = 365.25
SECONDS_PER_HOUR = 3600

# Per-edition slot pricing (USD per slot-hour).  [H2]
# Source: https://cloud.google.com/bigquery/pricing#editions-pricing
# The user-supplied `price_per_slot_hr` in FluidEstimateParams serves as the
# fallback for any edition string not in this map.
EDITION_RATES: dict[str, float] = {
    "STANDARD":         0.04,
    "ENTERPRISE":       0.06,
    "ENTERPRISE_PLUS":  0.10,
}

# MAX_BYTES_BILLED removed — now resolved dynamically via get_max_bytes_billed(params)
MAX_LOOKBACK_DAYS = 90


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------

class FluidScalingStatus(BaseModel):
    reservation_id: str
    enabled: bool
    ddl: Optional[str] = None


class FluidScalingParams(OrgParams):
    org_project_id: Optional[str] = None
    admin_project_id: Optional[str] = None
    region: str = "region-us"
    max_bytes_billed_gb: Optional[int] = None


class FluidEstimateParams(OrgParams):
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
    enabled: Optional[bool] = None
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
    # Tracks whether the *last completed* attempt was a failure. If every
    # attempt raises, we genuinely don't know the configured state and must
    # not report "no reservations configured" — that would generate a
    # misleading "please enable fluid scaling" DDL suggestion for
    # reservations that may already be enabled. A later successful query
    # (even a confirmed 0-row result) supersedes an earlier failure.
    last_error = None
    for label, sql in [("Fluid Options (EFFECTIVE)", effective_sql), ("Fluid Options (Fallback)", fallback_sql)]:
        try:
            results = list(_run_and_log(client, sql, label, params=params))
            last_error = None
            for row in results:
                vals = _parse_option_value(row.option_value)
                if vals:
                    logger.info("Fluid option found via %s on %s: %s", label, project, sorted(vals))
                    return vals
            logger.info("Fluid option not found via %s on %s (0 rows)", label, project)
        except Exception as e:
            logger.warning("%s query failed on %s: %s", label, project, e)
            last_error = e
    if last_error is not None:
        raise last_error
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
                new_list_safe = [_safe_ident(r, "reservation_name") for r in new_list]
                list_str = ", ".join(f'"{r}"' for r in new_list_safe)
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
    except HTTPException:
        raise
    except Exception:
        logger.exception("Unexpected error checking fluid scaling status")
        raise HTTPException(500, "Unexpected error; check server logs")


# ---------------------------------------------------------------------------
# Estimate endpoint
# ---------------------------------------------------------------------------

# F18: Aggregates all the way down to ONE ROW PER RESERVATION inside BigQuery
# rather than returning per-second rows to the client. A 90-day lookback would
# otherwise materialize ~7.8M rows per reservation into a pandas DataFrame —
# enough to OOM a memory-capped Cloud Run instance. Summing bounded per-second
# values here is mathematically identical to summing the same rows client-side.
_SQL_UNIFIED_FLUID_SCALING = """
WITH capacity_per_sec AS (
  SELECT
    reservation_id,
    edition,
    s.start_time AS period_start,
    IFNULL(s.slots_assigned, 0) AS baseline_slots,
    IFNULL(s.autoscale_current_slots, 0) AS current_slots,
    IFNULL(s.borrowed_slots, 0) AS borrowed_slots
  FROM `{admin_project}`.`{region}`.INFORMATION_SCHEMA.RESERVATION_TIMELINE_BY_PROJECT AS rt,
  UNNEST(
    IF(ARRAY_LENGTH(per_second_details) > 0,
       ARRAY(
         SELECT AS STRUCT d.start_time, d.autoscale_current_slots, d.slots_assigned, d.borrowed_slots
         FROM UNNEST(per_second_details) AS d
       ),
       ARRAY(
         SELECT AS STRUCT
           ts AS start_time,
           autoscale.current_slots AS autoscale_current_slots,
           slots_assigned AS slots_assigned,
           -- F9: RESERVATION_TIMELINE_BY_PROJECT exposes borrowed_slots only
           -- inside per_second_details. When that array is empty we fabricate a
           -- per-second series from the minute row, and borrowed slots are
           -- genuinely unrecoverable at this grain.
           --
           -- 0 is the conservative choice: it means borrowed capacity is
           -- attributed to the fluid autoscaler, which INFLATES
           -- fluid_slot_seconds and therefore UNDERSTATES slot_hours_saved.
           -- We prefer understating a savings claim to overstating one.
           0 AS borrowed_slots
         FROM UNNEST(GENERATE_TIMESTAMP_ARRAY(
           rt.period_start,
           (TIMESTAMP_ADD(rt.period_start, INTERVAL 59 SECOND)),
           INTERVAL 1 SECOND
         )) AS ts
       )
    )
  ) AS s
  WHERE rt.period_start >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @lookback_days DAY)
    AND rt.period_start <  CURRENT_TIMESTAMP()
    AND reservation_id IS NOT NULL
    AND reservation_id != ''
),
usage_per_sec AS (
  SELECT
    reservation_id,
    period_start,
    SUM(period_slot_ms) / 1000.0 AS used_slots
  FROM `{org_project}`.`{region}`.INFORMATION_SCHEMA.JOBS_TIMELINE_BY_ORGANIZATION
  -- job_creation_time must have a wide slack (7 days) to ensure jobs that started BEFORE the
  -- lookback window but continued running INTO the window are not dropped by partition pruning.
  WHERE job_creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL (@lookback_days + 7) DAY)
    AND job_creation_time <  CURRENT_TIMESTAMP()
  -- period_start must be tight and strictly aligned with capacity_per_sec to prevent orphaned
  -- rows in the FULL OUTER JOIN. (Included at @start, excluded exactly at CURRENT_TIMESTAMP).
    AND period_start      >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @lookback_days DAY)
    AND period_start      <  CURRENT_TIMESTAMP()
    AND reservation_id IS NOT NULL
    AND reservation_id != ''
    AND (statement_type != 'SCRIPT' OR statement_type IS NULL)
    {focus_clause}
  GROUP BY reservation_id, period_start
),
joined_per_sec AS (
  SELECT
    COALESCE(c.reservation_id, u.reservation_id) AS reservation_id,
    c.edition,
    c.current_slots,
    c.baseline_slots,
    c.borrowed_slots,
    u.used_slots,
    IF(c.period_start IS NOT NULL, 1, 0) AS _from_capacity
  FROM capacity_per_sec c
  FULL OUTER JOIN usage_per_sec u
    ON c.reservation_id = u.reservation_id
   AND c.period_start = u.period_start
)
SELECT
  reservation_id,
  MAX(edition) AS edition,
  SUM(IFNULL(current_slots, 0)) AS legacy_slot_seconds,
  -- Core FinOps Mathematical Engine for Fluid Scaling:
  -- 1. `used_slots - borrowed_slots - baseline_slots` isolates the pure autoscaling usage.
  -- 2. `GREATEST(..., 0)` safely floors negative values (e.g. if usage is entirely within the baseline).
  -- 3. `LEAST(..., current_slots)` imposes a strict cap, ensuring billed usage never mathematically 
  --    exceeds the provisioned capacity ceiling for that exact second.
  -- Summing these bounded seconds accurately reconstructs the exact billable autoscaler capacity.
  SUM(LEAST(GREATEST(IFNULL(used_slots, 0) - IFNULL(borrowed_slots, 0) - IFNULL(baseline_slots, 0), 0), IFNULL(current_slots, 0))) AS fluid_slot_seconds,
  SUM(IFNULL(used_slots, 0)) AS total_pure_used_seconds,
  MAX(_from_capacity) AS has_capacity
FROM joined_per_sec
GROUP BY reservation_id
"""


@dataclass
class _ReservationSummary:
    """Per-reservation aggregates after Python rollup."""
    reservation_id: str
    edition: Optional[str]
    legacy_slot_seconds: float
    fluid_slot_seconds: float
    total_pure_used_seconds: float
    status: str


def _process_unified_results(df: pd.DataFrame) -> List[_ReservationSummary]:
    if df.empty:
        return []

    output = []
    for row in df.itertuples():
        has_capacity = float(row.has_capacity) if pd.notnull(row.has_capacity) else 0.0
        used_secs = float(row.total_pure_used_seconds) if pd.notnull(row.total_pure_used_seconds) else 0.0

        if has_capacity == 0 and used_secs > 0:
            status = "External Admin"
        elif used_secs == 0 and has_capacity > 0:
            status = "Idle"
        elif has_capacity == 0 and used_secs == 0:
            status = "Inactive"
        else:
            status = "Active"
            
        output.append(
            _ReservationSummary(
                reservation_id=row.reservation_id,
                edition=str(row.edition).upper().strip() if pd.notnull(getattr(row, 'edition', None)) else None,
                legacy_slot_seconds=float(row.legacy_slot_seconds) if pd.notnull(row.legacy_slot_seconds) else 0.0,
                fluid_slot_seconds=float(row.fluid_slot_seconds) if pd.notnull(row.fluid_slot_seconds) else 0.0,
                total_pure_used_seconds=float(row.total_pure_used_seconds) if pd.notnull(row.total_pure_used_seconds) else 0.0,
                status=status
            )
        )
    return output


def _to_metric(
    summary: _ReservationSummary,
    price_per_slot_hr: float,
    lookback_days: int,
) -> FluidEstimateMetric:
    # H2: Use edition-specific rate; fall back to user-supplied override.
    effective_rate = EDITION_RATES.get(summary.edition, price_per_slot_hr) if summary.edition else price_per_slot_hr

    today_hours = summary.legacy_slot_seconds / SECONDS_PER_HOUR
    fluid_hours = summary.fluid_slot_seconds / SECONDS_PER_HOUR
    total_pure_used_hours = summary.total_pure_used_seconds / SECONDS_PER_HOUR
    saved_hours = max(0.0, today_hours - fluid_hours)

    # Clamped True cooldown savings (always >= 0%)
    clamped_savings = min(max((saved_hours / today_hours * 100.0), 0.0), 100.0) if today_hours > 0 else 0.0

    usd_window = saved_hours * effective_rate
    # M10: Guard against zero lookback_days from internal callers (HTTP is safe via ge=1).
    safe_lookback = max(lookback_days, 1)
    usd_monthly = usd_window * (DAYS_PER_MONTH / safe_lookback)
    usd_annual = usd_window * (DAYS_PER_YEAR / safe_lookback)

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
    return _run_and_log(client, sql, label, params=params,
                        query_parameters=all_params, fetch_df=True)


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
    # No actionable reservations found — don't claim "fully enabled" when we
    # have no data to base that on (e.g. wrong admin_project_id or region).
    if not actionable_res_names:
        is_fully_enabled = None
    elif missing_res:
        is_fully_enabled = False
        # Union with already-enabled names so the DDL doesn't drop existing entries.
        new_list = sorted(list(enabled_norm | actionable_res_names))
        new_list_safe = [_safe_ident(r, "reservation_name") for r in new_list]
        list_str = ", ".join(f'"{r}"' for r in new_list_safe)
        ddl = (
            f"ALTER PROJECT `{admin_project}`\n"
            f"SET OPTIONS (\n"
            f"  `{region}.{_FLUID_OPTION_NAME}` = [{list_str}]\n"
            f");"
        )
    else:
        is_fully_enabled = True

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


        unified_sql = _render_sql(_SQL_UNIFIED_FLUID_SCALING, admin_project=admin_project, org_project=org_project, region=region, focus_clause="")
        df = _run_query_to_df(client, unified_sql, params.lookback_days, "fluid_scaling_unified", params=params)

        logger.info("Fetched %d unified rows", len(df))

        if df.empty:
            logger.warning("Unified query returned 0 rows. Likely cause: admin_project_id (%s) does not own any reservations.", admin_project)

        summaries = _process_unified_results(df)

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
