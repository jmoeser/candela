"""Wholesale (AEMO) tariff strategy.

Models electricity cost using live AEMO spot prices as a proxy for
Amber Electric pricing.

Pricing formulae
----------------
Import cost per reading:
    kwh = max(grid_w, 0) * INTERVAL_HOURS / 1000
    rate_c_per_kwh = rrp_per_mwh / 10 + wholesale_adder_cents_per_kwh
    cost_cents = kwh * rate_c_per_kwh

Export credit per reading:
    kwh = max(-grid_w, 0) * INTERVAL_HOURS / 1000
    credit_rate = rrp_per_mwh * 0.7 / 10   (0.7 = retailer discount factor)
    credit_cents = kwh * credit_rate        (can be negative during negative prices)

Unit conversion:  rrp_per_mwh ($/MWh) → c/kWh:
    $/MWh × (1 MWh / 1000 kWh) × (100 c / $1) = $/MWh / 10

AEMO price lookup:
    Each reading is matched to the 5-minute clock block that contains it.
    Readings with no matching block contribute zero energy cost.
"""

from datetime import datetime
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
    _INTERVAL_HOURS,
    supply_charge_cents,
)

_MWH_TO_CENTS_PER_KWH = Decimal("10")  # divide rrp by this


class WholesaleStrategy:
    """Compute a bill against AEMO wholesale spot prices."""

    def __init__(
        self, wholesale_adder_cents_per_kwh: Decimal = Decimal("18.0")
    ) -> None:
        self._adder = wholesale_adder_cents_per_kwh

    def compute(
        self,
        readings: list[SolarReading],
        plan: TariffPlan,
        rates: list[TariffRate],
        *,
        aemo_prices: list[AemoPrice] | None = None,
    ) -> BillResult:
        # Build a fast lookup: interval_start (5-min) → rrp (Decimal)
        price_lookup: dict[datetime, Decimal] = {}
        for p in aemo_prices or []:
            price_lookup[p.interval_start] = Decimal(str(p.rrp_per_mwh))

        total_import_cents = Decimal("0")
        total_export_cents = Decimal("0")
        import_kwh = Decimal("0")
        export_kwh = Decimal("0")

        for reading in readings:
            block = reading.ts.replace(
                minute=(reading.ts.minute // 5) * 5, second=0, microsecond=0
            )
            rrp = price_lookup.get(block)
            if rrp is None:
                continue  # no price data → skip this reading

            kwh_import = (
                Decimal(str(max(reading.grid_w, 0)))
                * Decimal(str(_INTERVAL_HOURS))
                / Decimal("1000")
            )
            kwh_export = (
                Decimal(str(max(-reading.grid_w, 0)))
                * Decimal(str(_INTERVAL_HOURS))
                / Decimal("1000")
            )

            if kwh_import > 0:
                rate_c = rrp / _MWH_TO_CENTS_PER_KWH + self._adder
                total_import_cents += kwh_import * rate_c
                import_kwh += kwh_import

            if kwh_export > 0:
                credit_rate_c = rrp * Decimal("0.7") / _MWH_TO_CENTS_PER_KWH
                total_export_cents += kwh_export * credit_rate_c
                export_kwh += kwh_export

        sc = supply_charge_cents(plan, readings)

        period_breakdown: dict[str, PeriodResult] = {}
        if import_kwh > 0 or total_import_cents != 0:
            period_breakdown["wholesale"] = PeriodResult(
                kwh=import_kwh, cents=total_import_cents
            )

        return BillResult(
            total_cents=sc + total_import_cents - total_export_cents,
            supply_charge_cents=sc,
            import_charge_cents=total_import_cents,
            export_credit_cents=total_export_cents,
            demand_charge_cents=Decimal("0"),
            period_breakdown=period_breakdown,
        )
