"""Shared constants for the BQ FinOps Optimizer.

This is a leaf module — it imports nothing from ``src.*``, which means
any module in the package (including ``main.py`` and
``report_generator.py``) can import from it without introducing a
circular dependency.
"""


__version__ = "1.4.4"

# ---------------------------------------------------------------------------
# Legacy pricing sentinels (US-baseline defaults)
# ---------------------------------------------------------------------------
# Actual per-region pricing is resolved dynamically by ``src.pricing``.
# These sentinels are retained ONLY as Pydantic field defaults so that
# model_validators can detect "user didn't override" vs "user sent an
# explicit value".  Do NOT use them in cost formulas.
_US_ON_DEMAND_USD_PER_TIB: float = 6.25
_US_EDITIONS_SLOT_HR_RATE: float = 0.06

# ---------------------------------------------------------------------------
# Canonical BigQuery region registry
# ---------------------------------------------------------------------------
# Canonical form is PREFIXED ("region-europe-west1") because that is what the
# UI <select> emits and what every Pydantic model defaults to. The BigQuery
# job `location` argument and the dataset qualifier both want the BARE form
# ("europe-west1"), so always convert with bare_region() rather than doing
# ad-hoc .replace("region-", "") string surgery.
#
# supports_org_views: whether the region exposes organization-scoped
# INFORMATION_SCHEMA views (JOBS_BY_ORGANIZATION, TABLE_STORAGE_BY_ORGANIZATION,
# RESERVATIONS, ...). Regions marked False are still listed — absence from the
# dropdown reads as an unsupported product, whereas a precise error does not —
# but the UI annotates them and the error taxonomy explains the 404.
#
# (canonical_value, label, group, supports_org_views)
# Note on supports_org_views:
# (a) These flags are an empirical/best-effort assertion; Google does not publish
#     a machine-readable list of which regions support org-scoped INFORMATION_SCHEMA.
# (b) Last reviewed: 2024-05-15 (or recent date).
# (c) A wrong `False` produces a misleading error message ("This region does not expose..."),
#     while a wrong `True` merely produces the raw upstream error. Therefore, `True` is
#     the safe default for uncertainty (which supports_org_views() uses for unknown regions).
BQ_REGIONS: tuple[tuple[str, str, str, bool], ...] = (
    ("region-us", "US (multi-region)", "Multi-region", True),
    ("region-eu", "EU (multi-region)", "Multi-region", True),

    ("region-europe-west1", "europe-west1 (Belgium)", "Europe", True),
    ("region-europe-west2", "europe-west2 (London)", "Europe", True),
    ("region-europe-west3", "europe-west3 (Frankfurt)", "Europe", True),
    ("region-europe-west4", "europe-west4 (Netherlands)", "Europe", True),
    ("region-europe-west6", "europe-west6 (Zurich)", "Europe", True),
    ("region-europe-west8", "europe-west8 (Milan)", "Europe", True),
    ("region-europe-west9", "europe-west9 (Paris)", "Europe", True),
    ("region-europe-west10", "europe-west10 (Berlin)", "Europe", False),
    ("region-europe-west12", "europe-west12 (Turin)", "Europe", False),
    ("region-europe-north1", "europe-north1 (Finland)", "Europe", True),
    ("region-europe-central2", "europe-central2 (Warsaw)", "Europe", True),
    ("region-europe-southwest1", "europe-southwest1 (Madrid)", "Europe", True),

    ("region-us-central1", "us-central1 (Iowa)", "Americas", True),
    ("region-us-east1", "us-east1 (South Carolina)", "Americas", True),
    ("region-us-east4", "us-east4 (N. Virginia)", "Americas", True),
    ("region-us-east5", "us-east5 (Columbus)", "Americas", True),
    ("region-us-south1", "us-south1 (Dallas)", "Americas", True),
    ("region-us-west1", "us-west1 (Oregon)", "Americas", True),
    ("region-us-west2", "us-west2 (Los Angeles)", "Americas", True),
    ("region-us-west3", "us-west3 (Salt Lake City)", "Americas", True),
    ("region-us-west4", "us-west4 (Las Vegas)", "Americas", True),
    ("region-northamerica-northeast1", "northamerica-northeast1 (Montréal)", "Americas", True),
    ("region-northamerica-northeast2", "northamerica-northeast2 (Toronto)", "Americas", True),
    ("region-southamerica-east1", "southamerica-east1 (São Paulo)", "Americas", True),
    ("region-southamerica-west1", "southamerica-west1 (Santiago)", "Americas", True),

    ("region-asia-east1", "asia-east1 (Taiwan)", "Asia-Pacific", True),
    ("region-asia-east2", "asia-east2 (Hong Kong)", "Asia-Pacific", True),
    ("region-asia-northeast1", "asia-northeast1 (Tokyo)", "Asia-Pacific", True),
    ("region-asia-northeast2", "asia-northeast2 (Osaka)", "Asia-Pacific", True),
    ("region-asia-northeast3", "asia-northeast3 (Seoul)", "Asia-Pacific", True),
    ("region-asia-south1", "asia-south1 (Mumbai)", "Asia-Pacific", True),
    ("region-asia-south2", "asia-south2 (Delhi)", "Asia-Pacific", True),
    ("region-asia-southeast1", "asia-southeast1 (Singapore)", "Asia-Pacific", True),
    ("region-asia-southeast2", "asia-southeast2 (Jakarta)", "Asia-Pacific", True),
    ("region-australia-southeast1", "australia-southeast1 (Sydney)", "Asia-Pacific", True),
    ("region-australia-southeast2", "australia-southeast2 (Melbourne)", "Asia-Pacific", True),

    ("region-me-central1", "me-central1 (Doha)", "Middle East", False),
    ("region-me-central2", "me-central2 (Dammam)", "Middle East", False),
    ("region-me-west1", "me-west1 (Tel Aviv)", "Middle East", True),

    ("region-africa-south1", "africa-south1 (Johannesburg)", "Africa", False),
)

# Preserves the declaration order above when rendering <optgroup> elements.
BQ_REGION_GROUPS: tuple[str, ...] = (
    "Multi-region", "Europe", "Americas", "Asia-Pacific", "Middle East", "Africa",
)

_VALID_REGIONS = frozenset(r[0] for r in BQ_REGIONS)
_NO_ORG_VIEW_REGIONS = frozenset(r[0] for r in BQ_REGIONS if not r[3])

DEFAULT_REGION = "region-us"


def normalize_region(value: str) -> str:
    """Return the canonical prefixed form of a region.

    Accepts either convention so callers never have to care:
        'us' | 'region-us' | 'REGION-US' | ' Region-US ' -> 'region-us'

    An empty or missing value falls back to DEFAULT_REGION. Unknown regions are
    returned normalized rather than rejected, so a user's exotic/custom region
    still round-trips; use is_known_region() when validation is required.
    """
    v = (value or "").strip().lower()
    if not v:
        return DEFAULT_REGION
    return v if v.startswith("region-") else f"region-{v}"


def bare_region(value: str) -> str:
    """Return the form BigQuery wants for a job `location`.

    'region-europe-west1' -> 'europe-west1'
    """
    normalized = normalize_region(value)
    return normalized[len("region-"):] if normalized.startswith("region-") else normalized


def is_known_region(value: str) -> bool:
    """True if the region appears in BQ_REGIONS (accepts either convention)."""
    return normalize_region(value) in _VALID_REGIONS


def supports_org_views(value: str) -> bool:
    """False for regions known not to expose org-scoped INFORMATION_SCHEMA.

    Unknown/custom regions return True: we cannot prove they are unsupported,
    so we let the query run and surface the real error if it fails.
    """
    return normalize_region(value) not in _NO_ORG_VIEW_REGIONS

