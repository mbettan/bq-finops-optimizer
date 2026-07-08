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
from .utils import init_bq_client_and_resolve_project, reject_dummy_project, _safe_ident, _normalize_region, get_max_bytes_billed, FocusMixin, validate_focus_projects, build_project_filter, log_endpoint_start, log_endpoint_end

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/fluid-scaling", tags=["fluid-scaling"])


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------


DAYS_PER_MONTH = 30.44
DAYS_PER_YEAR = 365.25
SECONDS_PER_HOUR = 3600
SECONDS_PER_MINUTE = 60

# MAX_BYTES_BILLED removed — now resolved dynamically via get_max_bytes_billed(params)
MAX_LOOKBACK_DAYS = 90


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------

class FluidScalingStatus(BaseModel):
    reservation_id: str
    enabled: bool
    ddl: Optional[str] = None


class FluidScalingParams(BaseModel):
    org_project_id: Optional[str] = None
    admin_project_id: Optional[str] = None
    region: str = "region-us"
    max_bytes_billed_gb: Optional[int] = None


class FluidEstimateParams(FocusMixin):
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
    enabled: bool
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
    for label, sql in [("Fluid Options (EFFECTIVE)", effective_sql), ("Fluid Options (Fallback)", fallback_sql)]:
        try:
            results = list(_run_and_log(client, sql, label, params=params))
            for row in results:
                vals = _parse_option_value(row.option_value)
                if vals:
                    logger.info("Fluid option found via %s on %s: %s", label, project, sorted(vals))
                    return vals
            logger.info("Fluid option not found via %s on %s (0 rows)", label, project)
        except Exception as e:
            logger.warning("%s query failed on %s: %s", label, project, e)
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
                list_str = ", ".join(f'"{r}"' for r in new_list)
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


# ---------------------------------------------------------------------------
# Estimate endpoint
# ---------------------------------------------------------------------------

_SQL_PER_SECOND_CAPACITY = """
SELECT
  reservation_id,
  edition,
  s.start_time AS period_start,
  IFNULL(s.slots_assigned, 0) AS baseline_slots,
  IFNULL(s.autoscale_current_slots, 0) AS autoscale_current_slots
FROM `{admin_project}`.`{region}`.INFORMATION_SCHEMA.RESERVATION_TIMELINE_BY_PROJECT,
UNNEST(
  IF(ARRAY_LENGTH(per_second_details) > 0,
     ARRAY(
       SELECT AS STRUCT d.start_time, d.autoscale_current_slots, d.slots_assigned
       FROM UNNEST(per_second_details) AS d
     ),
     ARRAY(
       SELECT AS STRUCT
         ts AS start_time,
         autoscale.current_slots AS autoscale_current_slots,
         slots_assigned AS slots_assigned
       FROM UNNEST(GENERATE_TIMESTAMP_ARRAY(
         period_start,
         TIMESTAMP_ADD(period_start, INTERVAL 59 SECOND),
         INTERVAL 1 SECOND
       )) AS ts
     )
  )
) AS s
WHERE period_start >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @lookback_days DAY)
  AND period_start <  CURRENT_TIMESTAMP()
  AND reservation_id IS NOT NULL
  AND reservation_id != ''
"""

_SQL_PER_SECOND_USAGE = """
SELECT
  reservation_id,
  edition,
  period_start,
  SUM(period_slot_ms) / 1000.0 AS used_slots
FROM `{org_project}`.`{region}`.INFORMATION_SCHEMA.JOBS_TIMELINE_BY_ORGANIZATION
WHERE job_creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @lookback_days DAY)
  AND job_creation_time <  CURRENT_TIMESTAMP()
  AND period_start      >  TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @lookback_days DAY)
  AND period_start      <= CURRENT_TIMESTAMP()
  AND reservation_id IS NOT NULL
  AND reservation_id != ''
  AND (statement_type != 'SCRIPT' OR statement_type IS NULL)
  {focus_clause}
GROUP BY reservation_id, edition, period_start
"""


@dataclass
class _ReservationSummary:
    """Per-reservation aggregates after Python rollup."""
    reservation_id: str
    legacy_slot_seconds: float
    fluid_slot_seconds: float
    total_pure_used_seconds: float
    status: str


def _rollup_to_summaries(
    capacity_df: pd.DataFrame,
    usage_df: pd.DataFrame,
) -> List[_ReservationSummary]:
    if capacity_df.empty and usage_df.empty:
        return []

    capacity_df = capacity_df.copy()
    usage_df = usage_df.copy()

    # Normalize edition on both sides to prevent split rows on NULL/skew (Gap B)
    if not capacity_df.empty:
        capacity_df["edition"] = capacity_df["edition"].fillna("").astype(str)
    if not usage_df.empty:
        usage_df["edition"] = usage_df["edition"].fillna("").astype(str)

    if capacity_df.empty:
        capacity_df = pd.DataFrame(columns=["reservation_id", "edition", "period_start", "baseline_slots", "autoscale_current_slots"])
        capacity_df["minute"] = pd.Series(dtype="datetime64[ns, UTC]")
    else:
        capacity_df["period_start"] = pd.to_datetime(capacity_df["period_start"], utc=True)
        capacity_df["minute"] = capacity_df["period_start"].dt.floor("min")

    if usage_df.empty:
        usage_df = pd.DataFrame(columns=["reservation_id", "edition", "period_start", "used_slots"])
        usage_df["minute"] = pd.Series(dtype="datetime64[ns, UTC]")
    else:
        usage_df["period_start"] = pd.to_datetime(usage_df["period_start"], utc=True)
        usage_df["minute"] = usage_df["period_start"].dt.floor("min")

    # Aggregate capacity to the minute-grain (Gap A)
    cap_per_min = capacity_df.groupby(
        ["reservation_id", "edition", "minute"], as_index=False
    ).agg(
        baseline_slot_seconds=("baseline_slots", "sum"),              # Sum baseline over the 60s
        autoscale_capacity_slot_seconds=("autoscale_current_slots", "sum"),  # Sum current over the 60s
    )
    cap_per_min["_from_capacity"] = 1.0

    # Aggregate usage to the minute-grain (Gap A)
    usage_per_min = usage_df.groupby(
        ["reservation_id", "edition", "minute"], as_index=False
    ).agg(
        used_slots=("used_slots", "sum"),
    )

    # Merge on reservation_id, edition, and minute (Gap B - one-to-one)
    merged = cap_per_min.merge(
        usage_per_min,
        on=["reservation_id", "edition", "minute"],
        how="outer",
    )

    merged["baseline_slot_seconds"] = merged["baseline_slot_seconds"].astype(float).fillna(0.0)
    merged["autoscale_capacity_slot_seconds"] = merged["autoscale_capacity_slot_seconds"].astype(float).fillna(0.0)
    merged["used_slots"] = merged["used_slots"].astype(float).fillna(0.0)
    merged["_from_capacity"] = merged["_from_capacity"].astype(float).fillna(0.0)

    # Fluid clamp on minute-aggregated slot-seconds (Gap A)
    used_above_baseline = (merged["used_slots"] - merged["baseline_slot_seconds"]).clip(lower=0)
    merged["fluid_slot_seconds_min"] = pd.concat(
        [used_above_baseline, merged["autoscale_capacity_slot_seconds"]], axis=1
    ).min(axis=1).clip(lower=0)

    # Per-res sums
    per_res = merged.groupby("reservation_id", as_index=False).agg(
        legacy_slot_seconds=("autoscale_capacity_slot_seconds", "sum"),
        fluid_slot_seconds=("fluid_slot_seconds_min", "sum"),
        total_pure_used_seconds=("used_slots", "sum"),
    )

    # Calculate status criteria using capacity presence indicator (Gap C)
    res_sums = merged.groupby("reservation_id").agg(
        has_capacity=("_from_capacity", "max"),
        sum_used=("used_slots", "sum")
    )

    output = []
    for row in per_res.itertuples():
        # Robust scalar extraction (avoids Series-truthiness ValueError)
        has_capacity = float(res_sums.at[row.reservation_id, "has_capacity"])
        sum_used = float(res_sums.at[row.reservation_id, "sum_used"])

        if has_capacity == 0 and sum_used > 0:
            status = "External Admin"
        elif sum_used == 0 and has_capacity > 0:
            status = "Idle"
        elif has_capacity == 0 and sum_used == 0:
            status = "Inactive"
        else:
            status = "Active"
            
        output.append(
            _ReservationSummary(
                reservation_id=row.reservation_id,
                legacy_slot_seconds=float(row.legacy_slot_seconds),
                fluid_slot_seconds=float(row.fluid_slot_seconds),
                total_pure_used_seconds=float(row.total_pure_used_seconds),
                status=status
            )
        )
    return output


def _to_metric(
    summary: _ReservationSummary,
    price_per_slot_hr: float,
    lookback_days: int,
) -> FluidEstimateMetric:
    today_hours = summary.legacy_slot_seconds / SECONDS_PER_HOUR
    fluid_hours = summary.fluid_slot_seconds / SECONDS_PER_HOUR
    total_pure_used_hours = summary.total_pure_used_seconds / SECONDS_PER_HOUR
    saved_hours = today_hours - fluid_hours

    # Clamped True cooldown savings (always >= 0%)
    clamped_savings = (saved_hours / today_hours * 100.0) if today_hours > 0 else 0.0

    usd_window = saved_hours * price_per_slot_hr
    usd_monthly = usd_window * (DAYS_PER_MONTH / lookback_days)
    usd_annual = usd_window * (DAYS_PER_YEAR / lookback_days)

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
    max_bytes = get_max_bytes_billed(params)
    job_config = bigquery.QueryJobConfig(
        query_parameters=all_params,
        maximum_bytes_billed=max_bytes,
    )
    logger.info("⏳ %s — submitting query (lookback=%d days, safety cap: %s GiB)…", label, lookback_days, max_bytes // (1024**3))
    logger.debug("%s SQL:\n%s", label.upper(), sql)
    t0 = time.time()
    query_job = client.query(sql, job_config=job_config)
    df = query_job.result().to_dataframe(
        create_bqstorage_client=True,
    )
    elapsed = time.time() - t0
    # Log profile with clickable BQ Console URL
    job_project = query_job.project
    job_location = query_job.location or "us"
    bq_url = (
        f"https://console.cloud.google.com/bigquery?project={job_project}"
        f"&j=bq:{job_location}:{query_job.job_id}&page=queryresults"
    )
    proc = query_job.total_bytes_processed
    billed = query_job.total_bytes_billed
    proc_gib = f"{proc / (1024**3):.2f} GiB" if proc is not None else "N/A"
    bill_gib = f"{billed / (1024**3):.2f} GiB" if billed is not None else "N/A"
    logger.info(
        "✅ %s — %.1fs | Job: %s | Processed: %s | Billed: %s | Cache: %s | %s",
        label, elapsed, query_job.job_id, proc_gib, bill_gib, query_job.cache_hit, bq_url
    )
    return df


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
    is_fully_enabled = True
    if missing_res:
        is_fully_enabled = False
        # Union with already-enabled names so the DDL doesn't drop existing entries.
        new_list = sorted(list(enabled_norm | actionable_res_names))
        list_str = ", ".join(f'"{r}"' for r in new_list)
        ddl = (
            f"ALTER PROJECT `{admin_project}`\n"
            f"SET OPTIONS (\n"
            f"  `{region}.{_FLUID_OPTION_NAME}` = [{list_str}]\n"
            f");"
        )

    return FluidScalingConfigStatus(
        enabled=is_fully_enabled,
        configured_reservations=configured_res,
        missing_reservations=missing_res,
        ddl=ddl
    )


@router.post("/estimate", response_model=FluidScalingEstimateResponse)
def estimate_fluid_scaling(params: FluidEstimateParams):
    params.focus_projects = validate_focus_projects(params.focus_projects)
    t0 = log_endpoint_start("Fluid Scaling Estimate", params, _logger=logger)
    try:
        client, org_project = init_bq_client_and_resolve_project(params)
        admin_project_raw = params.admin_project_id.strip() if (params.admin_project_id and params.admin_project_id.strip()) else org_project
        admin_project = _safe_ident(admin_project_raw, "admin_project_id")
        reject_dummy_project(admin_project)
        region = _safe_ident(_normalize_region(params.region), "region")

        focus_clause, focus_params = build_project_filter(params.focus_projects)
        capacity_sql = _render_sql(_SQL_PER_SECOND_CAPACITY, admin_project=admin_project, region=region)
        usage_sql = _render_sql(_SQL_PER_SECOND_USAGE, org_project=org_project, region=region, focus_clause=focus_clause)

        capacity_df = _run_query_to_df(client, capacity_sql, params.lookback_days, "capacity", params=params)
        usage_df = _run_query_to_df(client, usage_sql, params.lookback_days, "usage", params=params, extra_query_params=focus_params)

        logger.info(
            "Fetched %d capacity rows, %d usage rows", len(capacity_df), len(usage_df)
        )

        logger.info("capacity period_start dtype: %s, sample: %r",
                    capacity_df["period_start"].dtype if not capacity_df.empty else "empty",
                    capacity_df["period_start"].iloc[0] if not capacity_df.empty else None)
        logger.info("usage period_start dtype: %s, sample: %r",
                    usage_df["period_start"].dtype if not usage_df.empty else "empty",
                    usage_df["period_start"].iloc[0] if not usage_df.empty else None)
        if capacity_df.empty and not usage_df.empty:
            logger.warning(
                "Capacity query returned 0 rows but usage has %d rows. "
                "Likely cause: admin_project_id (%s) does not own any reservations. "
                "Reservations seen in usage data: %s",
                len(usage_df),
                admin_project,
                usage_df["reservation_id"].unique().tolist()[:10],
            )

        summaries = _rollup_to_summaries(capacity_df, usage_df)

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
