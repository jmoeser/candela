"""Demand tariff strategy.

TOU energy charges as per ``TOUStrategy``, plus a demand charge based on the
single highest 30-minute block kW recorded during the demand window.

Energex demand calculation
--------------------------
Per Energex's definition, the demand for a 30-minute interval is:

    block_kwh = sum(max(grid_w, 0) * INTERVAL_HOURS / 1000
                    for each reading in the block)
    block_demand_kw = block_kwh * 2   # kWh / 0.5 h = average kW

The demand charge for the billing period is:

    max(block_demand_kw across all blocks in the demand window) * cents_per_kw
"""

from collections import defaultdict
from decimal import Decimal

from candela.collector.aemo import AemoPrice
from candela.tariffs.models import BillResult, PeriodResult, SolarReading, TariffPlan, TariffRate
from candela.tariffs.strategies.base import (
    _INTERVAL_HOURS,
    block_start_for,
    match_rate,
    rate_applies,
    reading_export_kwh,
    reading_import_kwh,
    supply_charge_cents,
)


class DemandStrategy:
    """Compute a bill under a TOU-plus-demand plan."""

    def compute(
        self,
        readings: list[SolarReading],
        plan: TariffPlan,
        rates: list[TariffRate],
        *,
        aemo_prices: list[AemoPrice] | None = None,
    ) -> BillResult:
        demand_rate = next((r for r in rates if r.rate_type == "demand"), None)
        energy_rates = [r for r in rates if r.rate_type != "demand"]

        # --- TOU energy charges (same logic as TOUStrategy) ---
        kwh_by_period: dict[str, Decimal] = defaultdict(Decimal)
        export_kwh = Decimal("0")

        for reading in readings:
            imp = reading_import_kwh(reading)
            exp = reading_export_kwh(reading)

            if imp > 0:
                matched = match_rate(reading.ts, energy_rates)
                period = matched.rate_type if matched else "offpeak"
                kwh_by_period[period] += imp

            if exp > 0:
                export_kwh += exp

        rate_by_type: dict[str, TariffRate] = {r.rate_type: r for r in energy_rates}
        period_breakdown: dict[str, PeriodResult] = {}
        total_import_cents = Decimal("0")

        for period, kwh in kwh_by_period.items():
            rate = rate_by_type.get(period)
            cpkwh = rate.cents_per_kwh if rate and rate.cents_per_kwh else Decimal("0")
            cents = kwh * cpkwh
            period_breakdown[period] = PeriodResult(kwh=kwh, cents=cents)
            total_import_cents += cents

        # --- Demand charge ---
        demand_charge = Decimal("0")

        if demand_rate and demand_rate.cents_per_kw:
            # Group readings in the demand window into 30-minute blocks
            block_kwh: dict[object, Decimal] = defaultdict(Decimal)

            for reading in readings:
                # Check if reading falls within demand window
                if not _in_demand_window(reading, demand_rate):
                    continue
                imp_kwh = Decimal(str(max(reading.grid_w, 0))) * Decimal(str(_INTERVAL_HOURS)) / Decimal("1000")
                key = block_start_for(reading.ts)
                block_kwh[key] += imp_kwh

            if block_kwh:
                # demand_kw = block_kwh * 2  (= kWh / 0.5 h)
                max_demand_kw = max(kwh * Decimal("2") for kwh in block_kwh.values())
                demand_charge = max_demand_kw * demand_rate.cents_per_kw

        export_cents = export_kwh * (plan.feed_in_tariff_cents or Decimal("0"))
        sc = supply_charge_cents(plan, readings)

        return BillResult(
            total_cents=sc + total_import_cents + demand_charge - export_cents,
            supply_charge_cents=sc,
            import_charge_cents=total_import_cents,
            export_credit_cents=export_cents,
            demand_charge_cents=demand_charge,
            period_breakdown=period_breakdown,
        )


def _in_demand_window(reading: SolarReading, demand_rate: TariffRate) -> bool:
    """Return True if *reading* falls within the demand measurement window."""
    t = reading.ts.time()
    ws = demand_rate.demand_window_start
    we = demand_rate.demand_window_end
    if ws is None or we is None:
        return True  # no window defined → all times qualify
    return ws <= t < we
