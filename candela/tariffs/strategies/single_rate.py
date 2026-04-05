"""Single-rate tariff strategy.

Charges a flat rate per kWh imported, a flat feed-in tariff per kWh exported,
and a daily supply charge.
"""

from decimal import Decimal

from candela.collector.aemo import AemoPrice
from candela.tariffs.models import BillResult, PeriodResult, SolarReading, TariffPlan, TariffRate
from candela.tariffs.strategies.base import (
    reading_export_kwh,
    reading_import_kwh,
    supply_charge_cents,
)


class SingleRateStrategy:
    """Compute a bill under a flat single-rate plan."""

    def compute(
        self,
        readings: list[SolarReading],
        plan: TariffPlan,
        rates: list[TariffRate],
        *,
        aemo_prices: list[AemoPrice] | None = None,
    ) -> BillResult:
        flat_rate = next((r for r in rates if r.rate_type == "flat"), None)
        cents_per_kwh = flat_rate.cents_per_kwh if flat_rate else Decimal("0")

        import_kwh = Decimal("0")
        export_kwh = Decimal("0")
        for reading in readings:
            import_kwh += reading_import_kwh(reading)
            export_kwh += reading_export_kwh(reading)

        sc = supply_charge_cents(plan, readings)
        import_cents = import_kwh * (cents_per_kwh or Decimal("0"))
        export_cents = export_kwh * (plan.feed_in_tariff_cents or Decimal("0"))

        period_breakdown: dict[str, PeriodResult] = {}
        if import_kwh > 0 or export_kwh > 0:
            period_breakdown["flat"] = PeriodResult(kwh=import_kwh, cents=import_cents)

        return BillResult(
            total_cents=sc + import_cents - export_cents,
            supply_charge_cents=sc,
            import_charge_cents=import_cents,
            export_credit_cents=export_cents,
            demand_charge_cents=Decimal("0"),
            period_breakdown=period_breakdown,
        )
