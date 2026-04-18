"""Dataclasses and constants for load disaggregation."""

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

# Known load profiles for the household.
# Sorted by range width (max_watts - min_watts) ascending so that the tightest
# profiles are matched first during overlap detection; this prevents the wide
# ev_charging range from shadowing hot_water_boost.
KNOWN_LOADS: list[dict] = [
    {
        "name": "hot_water_boost",
        # Midea RSJ-15/190RDN3-E boost element: 2780W rated
        "min_watts": 2500,
        "max_watts": 3000,
        "tolerance": 250,
        "min_duration_min": 15,
    },
    {
        "name": "hot_water_heatpump",
        # Midea RSJ-15/190RDN3-E heat pump: 1500W rated, 800–1400W real-world
        "min_watts": 700,
        "max_watts": 1600,
        "tolerance": 300,
        "min_duration_min": 45,
    },
    {
        "name": "ev_charging",
        # Covers 10A outlet (~2.4kW) through Wall Connector (~7.2kW)
        "min_watts": 1800,
        "max_watts": 8000,
        "tolerance": 500,
        "min_duration_min": 20,
    },
]


@dataclass
class LoadEvent:
    """A detected or manually-entered load event."""

    id: int | None  # None before DB insert
    started_at: datetime
    ended_at: datetime | None
    load_name: (
        str  # 'ev_charging' | 'hot_water_heatpump' | 'hot_water_boost' | 'unknown'
    )
    avg_watts: int | None
    kwh: Decimal | None
    confidence: Decimal | None  # 0.000–1.000
    source: str  # 'inferred' | 'tesla_api' | 'manual'
