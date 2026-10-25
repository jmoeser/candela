"""Base protocol and shared helpers for tariff strategies."""

from datetime import datetime
from decimal import Decimal
from typing import Protocol

from candela.collector.aemo import AemoPrice
from candela.tariffs.models import BillResult, SolarReading, TariffPlan, TariffRate

# All readings are produced by the 5-minute poller.
_INTERVAL_HOURS: float = 5 / 60


class TariffStrategy(Protocol):
    """Protocol that all tariff strategies implement."""

    def compute(
        self,
        readings: list[SolarReading],
        plan: TariffPlan,
        rates: list[TariffRate],
        *,
        aemo_prices: list[AemoPrice] | None = None,
    ) -> BillResult: ...


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def supply_charge_cents(plan: TariffPlan, readings: list[SolarReading]) -> Decimal:
    """Return the supply charge for the date range spanned by *readings*."""
    if not readings:
        return Decimal(0)
    dates = {r.ts.date() for r in readings}
    days = (max(dates) - min(dates)).days + 1
    return plan.supply_charge_daily_cents * days


def reading_import_kwh(reading: SolarReading) -> Decimal:
    """kWh imported from grid for a single 5-minute reading."""
    return (
        Decimal(str(max(reading.grid_w, 0)))
        * Decimal(str(_INTERVAL_HOURS))
        / Decimal(1000)
    )


def reading_export_kwh(reading: SolarReading) -> Decimal:
    """kWh exported to grid for a single 5-minute reading."""
    return (
        Decimal(str(max(-reading.grid_w, 0)))
        * Decimal(str(_INTERVAL_HOURS))
        / Decimal(1000)
    )


def rate_applies(rate: TariffRate, ts: datetime) -> bool:
    """Return True if *rate* applies at *ts* based on its time/day/month filters."""
    t = ts.time()
    in_window = (
        rate.window_start is None
        or rate.window_end is None
        or rate.window_start <= t < rate.window_end
    )
    in_days = rate.days_of_week is None or ts.weekday() in rate.days_of_week
    in_months = rate.months is None or ts.month in rate.months
    return in_window and in_days and in_months


# Priority ordering for TOU period matching (lower = higher priority).
_RATE_PRIORITY: dict[str, int] = {"peak": 0, "shoulder": 1, "offpeak": 2, "flat": 3}


def match_rate(ts: datetime, rates: list[TariffRate]) -> TariffRate | None:
    """Return the highest-priority rate that applies at *ts*, or None."""
    candidates = [r for r in rates if rate_applies(r, ts)]
    if not candidates:
        return None
    return min(candidates, key=lambda r: _RATE_PRIORITY.get(r.rate_type, 99))


def block_start_for(ts: datetime) -> datetime:
    """Return the start of the 30-minute aligned clock block containing *ts*.

    Used for demand charge calculations (Energex 30-minute block definition).
    """
    return ts.replace(minute=(ts.minute // 30) * 30, second=0, microsecond=0)
