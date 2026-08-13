"""Shared constants for the BQ FinOps Optimizer.

This is a leaf module — it imports nothing from ``src.*``, which means
any module in the package (including ``main.py`` and
``report_generator.py``) can import from it without introducing a
circular dependency.
"""

import os

__version__ = "1.4.3"

# Centralized on-demand pricing — single source of truth [R-pricing]
ON_DEMAND_USD_PER_TB = float(os.environ.get("BQ_ON_DEMAND_USD_PER_TB", "6.25"))
EDITIONS_SLOT_HR_RATE = float(os.environ.get("BQ_EDITIONS_SLOT_HR_RATE", "0.06"))
