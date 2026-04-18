"""Time-of-use (TOU) tariff strategy.

Buckets each import reading into the highest-priority matching rate period
(peak > shoulder > offpeak). Export credit is always based on the plan's
flat feed-in tariff, not the period rate.
"""

from collections import defaultdict
from decimal import Decimal

from candela.collector.aemo import AemoPrice
from candela.tariffs.models import (
    BillResult,
    PeriodResult,
    SolarReading,
    TariffPlan,
    TariffRate,
)
from candela.tariffs.strategies.base import (
    match_rate,
    reading_export_kwh,
    reading_import_kwh,
    supply_charge_cents,
)


class TOUStrategy:
    """Compute a bill under a time-of-use plan."""

    def compute(
        self,
        readings: list[SolarReading],
        plan: TariffPlan,
        rates: list[TariffRate],
        *,
        aemo_prices: list[AemoPrice] | None = None,
    ) -> BillResult:
        # Only energy rates contribute to TOU buckets (exclude demand rows)
        energy_rates = [r for r in rates if r.rate_type != "demand"]

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

        # Build rate lookup by type for cost calculation
        rate_by_type: dict[str, TariffRate] = {r.rate_type: r for r in energy_rates}

        period_breakdown: dict[str, PeriodResult] = {}
        total_import_cents = Decimal("0")

        for period, kwh in kwh_by_period.items():
            rate = rate_by_type.get(period)
            cpkwh = rate.cents_per_kwh if rate and rate.cents_per_kwh else Decimal("0")
            cents = kwh * cpkwh
            period_breakdown[period] = PeriodResult(kwh=kwh, cents=cents)
            total_import_cents += cents

        export_cents = export_kwh * (plan.feed_in_tariff_cents or Decimal("0"))
        sc = supply_charge_cents(plan, readings)

        return BillResult(
            total_cents=sc + total_import_cents - export_cents,
            supply_charge_cents=sc,
            import_charge_cents=total_import_cents,
            export_credit_cents=export_cents,
            demand_charge_cents=Decimal("0"),
            period_breakdown=period_breakdown,
        )
