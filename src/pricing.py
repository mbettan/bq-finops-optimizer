"""Dynamic regional pricing from BigQuery Cloud Billing SKU data.

This is a **leaf module** — it imports nothing from ``src.*``, so any
module in the package can import from it without circular dependencies.

The SKU JSON file is parsed **once** at import time and cached in a
module-level dict.  Lookups are O(1) dict access with zero I/O at
request time.

Price source: ``src/data/bigquery_skus.json``  (refreshed ~yearly).
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Query

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# 1.  RegionPricing dataclass
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class RegionPricing:
    """Immutable container for a single region's resolved prices."""

    on_demand_usd_per_tib: float       # OnDemandAnalysis — $/TiB
    active_logical_gib_mo: float       # ActiveStorage — $/GiB/mo
    long_term_logical_gib_mo: float    # LongTermStorage — $/GiB/mo
    active_physical_gib_mo: float      # PhysicalStorage (Active) — $/GiB/mo
    long_term_physical_gib_mo: float   # PhysicalStorage (Long-Term) — $/GiB/mo
    editions_slot_hr_rate: float = 0.06  # Not region-dependent; global constant


# US multi-region baseline — used as fallback for unknown regions.
_US_DEFAULTS = RegionPricing(
    on_demand_usd_per_tib=6.25,
    active_logical_gib_mo=0.02,
    long_term_logical_gib_mo=0.01,
    active_physical_gib_mo=0.04,
    long_term_physical_gib_mo=0.02,
)

# ---------------------------------------------------------------------------
# 2.  SKU JSON parsing
# ---------------------------------------------------------------------------

# Multi-region alias: the app uses "eu" but the SKU file uses "europe".
_REGION_ALIASES: dict[str, str] = {
    "eu": "europe",
}


def _extract_paid_price(sku: dict[str, Any]) -> float:
    """Extract the first paid-tier unit price from a SKU's tiered rates.

    Skips the free tier (startUsageAmount == 0 with $0 price) and returns
    the first paid tier. If only one tier exists, returns that.
    """
    rates = sku["pricingInfo"][0]["pricingExpression"]["tieredRates"]
    for rate in rates:
        up = rate["unitPrice"]
        price = int(up["units"]) + up["nanos"] / 1_000_000_000
        if price > 0:
            return price
    # Fallback: return the first rate's price
    up = rates[0]["unitPrice"]
    return int(up["units"]) + up["nanos"] / 1_000_000_000


def _parse_sku_file(path: Path) -> dict[str, RegionPricing]:
    """Parse the SKU JSON and build a region → RegionPricing lookup."""

    with open(path, encoding="utf-8") as f:
        skus: list[dict[str, Any]] = json.load(f)

    # Intermediate accumulator: region → { field_name: price }
    acc: dict[str, dict[str, float]] = {}

    for sku in skus:
        regions = sku.get("serviceRegions", [])
        if not regions:
            continue
        group = sku["category"]["resourceGroup"]
        description = sku.get("description", "")

        # Determine which pricing field this SKU populates.
        field: str | None = None
        if group == "OnDemandAnalysis":
            field = "on_demand_usd_per_tib"
        elif group == "ActiveStorage":
            field = "active_logical_gib_mo"
        elif group == "LongTermStorage":
            field = "long_term_logical_gib_mo"
        elif group == "PhysicalStorage":
            if "Active Physical" in description:
                field = "active_physical_gib_mo"
            elif "Long-Term Physical" in description:
                field = "long_term_physical_gib_mo"

        if field is None:
            continue  # SKU not relevant (Network, Streaming, etc.)

        price = _extract_paid_price(sku)

        for region in regions:
            if region not in acc:
                acc[region] = {}
            acc[region][field] = price

    # Build immutable RegionPricing objects, filling gaps from US defaults.
    result: dict[str, RegionPricing] = {}
    us_fields = {
        "on_demand_usd_per_tib": _US_DEFAULTS.on_demand_usd_per_tib,
        "active_logical_gib_mo": _US_DEFAULTS.active_logical_gib_mo,
        "long_term_logical_gib_mo": _US_DEFAULTS.long_term_logical_gib_mo,
        "active_physical_gib_mo": _US_DEFAULTS.active_physical_gib_mo,
        "long_term_physical_gib_mo": _US_DEFAULTS.long_term_physical_gib_mo,
    }

    for region, fields in acc.items():
        merged = {**us_fields, **fields}
        result[region] = RegionPricing(**merged)

    return result


# ---------------------------------------------------------------------------
# 3.  Module-level cache — parsed once at import time
# ---------------------------------------------------------------------------

_SKU_FILE = Path(os.environ.get(
    "BQ_SKU_FILE",
    str(Path(__file__).parent / "data" / "bigquery_skus.json"),
))

try:
    _PRICING_CACHE: dict[str, RegionPricing] = _parse_sku_file(_SKU_FILE)
    logger.info("Loaded BigQuery pricing for %d regions from %s", len(_PRICING_CACHE), _SKU_FILE)
except Exception:
    logger.exception("Failed to load SKU pricing from %s — falling back to US defaults", _SKU_FILE)
    _PRICING_CACHE = {}


# ---------------------------------------------------------------------------
# 4.  Public API
# ---------------------------------------------------------------------------

def _bare_region(value: str) -> str:
    """Normalise to bare region form: 'region-europe-west1' → 'europe-west1'."""
    v = (value or "").strip().lower()
    if v.startswith("region-"):
        v = v[len("region-"):]
    return v


def get_pricing(region: str) -> RegionPricing:
    """Resolve pricing for a region.

    Accepts any format: ``'region-us'``, ``'us'``, ``'US'``,
    ``'region-europe-west1'``, etc.

    Falls back to US multi-region defaults for unknown regions (with a
    logged warning).
    """
    bare = _bare_region(region)

    # Apply alias (e.g. "eu" → "europe").
    bare = _REGION_ALIASES.get(bare, bare)

    rp = _PRICING_CACHE.get(bare)
    if rp is not None:
        return rp

    logger.warning(
        "No SKU pricing found for region '%s' — falling back to US defaults.",
        region,
    )
    return _US_DEFAULTS


def get_all_regions() -> list[str]:
    """Return a sorted list of all regions with pricing data."""
    return sorted(_PRICING_CACHE.keys())


# ---------------------------------------------------------------------------
# 5.  FastAPI endpoint
# ---------------------------------------------------------------------------

router = APIRouter(tags=["pricing"])


@router.get("/api/pricing")
def get_pricing_endpoint(region: str = Query(default="region-us")):
    """Return resolved pricing for the given region."""
    rp = get_pricing(region)
    bare = _bare_region(region)
    bare = _REGION_ALIASES.get(bare, bare)
    is_fallback = bare not in _PRICING_CACHE
    return {
        "region": bare,
        "on_demand_usd_per_tib": rp.on_demand_usd_per_tib,
        "active_logical_gib_mo": rp.active_logical_gib_mo,
        "long_term_logical_gib_mo": rp.long_term_logical_gib_mo,
        "active_physical_gib_mo": rp.active_physical_gib_mo,
        "long_term_physical_gib_mo": rp.long_term_physical_gib_mo,
        "editions_slot_hr_rate": rp.editions_slot_hr_rate,
        "source": "us_default_fallback" if is_fallback else "sku_file",
    }
