"""Dataclasses for the tariff engine.

These types are pure data — no DB access, no side effects.  The engine
(``tariffs/engine.py``) is responsible for fetching rows from the database
and converting them into these objects before handing off to strategies.

Sign convention
---------------
``SolarReading.grid_w``:
    Positive  → importing from grid (costs money).
    Negative  → exporting to grid (earns FiT credit or wholesale credit).
"""

from dataclasses import dataclass, field
from datetime import date, datetime, time
from decimal import Decimal


@dataclass
class SolarReading:
    """A single 5-minute inverter snapshot, as stored in ``solar_readings``."""

    ts: datetime  # UTC timestamp
    solar_w: int  # PV generation in watts
    grid_w: int  # Grid power in watts (+import / -export)
    load_w: int  # House consumption in watts
    daily_yield_kwh: float | None = None
    total_yield_kwh: float | None = None
    inverter_temp_c: float | None = None


@dataclass
class TariffPlan:
    """A tariff plan row from ``tariff_plans``."""

    id: int
    name: str
    plan_type: str  # 'single_rate'|'tou'|'demand'|'wholesale'
    supply_charge_daily_cents: Decimal
    valid_from: date
    retailer: str | None = None
    feed_in_tariff_cents: Decimal | None = None
    valid_to: date | None = None
    notes: str | None = None


@dataclass
class TariffRate:
    """A rate row from ``tariff_rates``."""

    id: int
    plan_id: int
    rate_type: str  # 'flat'|'peak'|'shoulder'|'offpeak'|'demand'
    cents_per_kwh: Decimal | None = None  # None for demand-charge rows
    cents_per_kw: Decimal | None = None  # None for energy-charge rows
    window_start: time | None = None  # None = applies all times
    window_end: time | None = None  # None = applies all times
    days_of_week: list[int] | None = None  # 0=Mon…6=Sun; None = all days
    months: list[int] | None = None  # 1–12; None = all months
    demand_window_start: time | None = None
    demand_window_end: time | None = None


@dataclass
class PeriodResult:
    """Energy and cost for a single billing period bucket."""

    kwh: Decimal
    cents: Decimal


@dataclass
class BillResult:
    """Complete breakdown of a modelled electricity bill."""

    total_cents: Decimal
    supply_charge_cents: Decimal
    import_charge_cents: Decimal
    export_credit_cents: Decimal  # always non-negative (a saving)
    demand_charge_cents: Decimal  # zero for non-demand plans
    # Maps period name (e.g. 'peak', 'offpeak', 'flat') to energy/cost
    period_breakdown: dict[str, PeriodResult] = field(default_factory=dict)
