"""Base protocol and shared helpers for tariff strategies."""

from datetime import datetime, time
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
        return Decimal("0")
    dates = {r.ts.date() for r in readings}
    days = (max(dates) - min(dates)).days + 1
    return plan.supply_charge_daily_cents * days


def reading_import_kwh(reading: SolarReading) -> Decimal:
    """kWh imported from grid for a single 5-minute reading."""
    return Decimal(str(max(reading.grid_w, 0))) * Decimal(str(_INTERVAL_HOURS)) / Decimal("1000")


def reading_export_kwh(reading: SolarReading) -> Decimal:
    """kWh exported to grid for a single 5-minute reading."""
    return Decimal(str(max(-reading.grid_w, 0))) * Decimal(str(_INTERVAL_HOURS)) / Decimal("1000")


def rate_applies(rate: TariffRate, ts: datetime) -> bool:
    """Return True if *rate* applies at *ts* based on its time/day/month filters."""
    t = ts.time()

    if rate.window_start is not None and rate.window_end is not None:
        if not (rate.window_start <= t < rate.window_end):
            return False

    if rate.days_of_week is not None:
        if ts.weekday() not in rate.days_of_week:
            return False

    if rate.months is not None:
        if ts.month not in rate.months:
            return False

    return True


# Priority ordering for TOU period matching (lower = higher priority).
_RATE_PRIORITY: dict[str, int] = {"peak": 0, "shoulder": 1, "offpeak": 2, "flat": 3}


def match_rate(ts: datetime, rates: list[TariffRate]) -> TariffRate | None:
    """Return the highest-priority rate that applies at *ts*, or None."""
    candidates = [r for r in rates if rate_applies(r, ts)]
    if not candidates:
        return None
    return min(candidates, key=lambda r: _RATE_PRIORITY.get(r.rate_type, 99))


def block_start_for(ts: datetime) -> datetime:
    """Return the start of the 30-minute aligned clock block containing *ts*."""
    return ts.replace(minute=(ts.minute // 30) * 30, second=0, microsecond=0)
