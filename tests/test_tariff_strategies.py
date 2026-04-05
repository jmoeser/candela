"""Tests for tariff strategy implementations.

All tests are pure (no DB) — strategies receive Python dataclasses only.

Interval assumption
-------------------
All synthetic readings use 5-minute intervals (the system standard).
One 5-minute reading of X watts contributes X * (5/60) / 1000 kWh.

Demand calculation
------------------
Demand per 30-min block = block_kwh * 2 (i.e. kWh / 0.5 h = average kW).
"""

from datetime import UTC, date, datetime, time
from decimal import Decimal

import pytest

from candela.collector.aemo import AemoPrice
from candela.tariffs.models import (
    BillResult,
    SolarReading,
    TariffPlan,
    TariffRate,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_INTERVAL_HOURS = 5 / 60  # 5-minute readings


def _reading(ts: datetime, grid_w: int, solar_w: int = 0, load_w: int = 0) -> SolarReading:
    return SolarReading(ts=ts, solar_w=solar_w, grid_w=grid_w, load_w=load_w)


def _plan(
    plan_type: str,
    supply_cents: float = 100.0,
    fit_cents: float | None = None,
) -> TariffPlan:
    return TariffPlan(
        id=1,
        name="Test Plan",
        plan_type=plan_type,
        supply_charge_daily_cents=Decimal(str(supply_cents)),
        valid_from=date(2026, 1, 1),
        feed_in_tariff_cents=Decimal(str(fit_cents)) if fit_cents is not None else None,
    )


def _rate(
    rate_type: str,
    cents_per_kwh: float | None = None,
    cents_per_kw: float | None = None,
    window_start: time | None = None,
    window_end: time | None = None,
    days_of_week: list[int] | None = None,
    months: list[int] | None = None,
    demand_window_start: time | None = None,
    demand_window_end: time | None = None,
) -> TariffRate:
    return TariffRate(
        id=1,
        plan_id=1,
        rate_type=rate_type,
        cents_per_kwh=Decimal(str(cents_per_kwh)) if cents_per_kwh is not None else None,
        cents_per_kw=Decimal(str(cents_per_kw)) if cents_per_kw is not None else None,
        window_start=window_start,
        window_end=window_end,
        days_of_week=days_of_week,
        months=months,
        demand_window_start=demand_window_start,
        demand_window_end=demand_window_end,
    )


def _ts(hour: int, minute: int = 0, day: int = 1) -> datetime:
    """UTC datetime on 2026-01-{day} at {hour}:{minute}."""
    return datetime(2026, 1, day, hour, minute, 0, tzinfo=UTC)


# ---------------------------------------------------------------------------
# Single-rate strategy
# ---------------------------------------------------------------------------


def test_single_rate_import_only() -> None:
    """Import-only: 12 readings × 1000 W → 1 kWh → 30 c import + 100 c supply."""
    from candela.tariffs.strategies.single_rate import SingleRateStrategy

    # 12 readings of 5 min each = 1 hour at 1000 W = 1 kWh
    readings = [_reading(_ts(0, i * 5), grid_w=1000) for i in range(12)]
    plan = _plan("single_rate", supply_cents=100.0)
    rates = [_rate("flat", cents_per_kwh=30.0)]

    result = SingleRateStrategy().compute(readings, plan, rates)

    expected_import_kwh = Decimal("1000") * Decimal("12") * Decimal(str(_INTERVAL_HOURS)) / Decimal("1000")
    expected_import_cents = expected_import_kwh * Decimal("30")

    assert result.supply_charge_cents == Decimal("100")
    assert float(result.import_charge_cents) == pytest.approx(float(expected_import_cents), rel=1e-6)
    assert result.export_credit_cents == Decimal("0")
    assert result.demand_charge_cents == Decimal("0")
    assert float(result.total_cents) == pytest.approx(
        float(result.supply_charge_cents + result.import_charge_cents - result.export_credit_cents),
        rel=1e-6,
    )
    assert "flat" in result.period_breakdown


def test_single_rate_export_credit() -> None:
    """Export-only: 12 readings × -500 W → 0.5 kWh export → 5 c FiT credit."""
    from candela.tariffs.strategies.single_rate import SingleRateStrategy

    readings = [_reading(_ts(12, i * 5), grid_w=-500) for i in range(12)]
    plan = _plan("single_rate", supply_cents=100.0, fit_cents=10.0)
    rates = [_rate("flat", cents_per_kwh=30.0)]

    result = SingleRateStrategy().compute(readings, plan, rates)

    export_kwh = Decimal("500") * Decimal("12") * Decimal(str(_INTERVAL_HOURS)) / Decimal("1000")
    expected_credit = export_kwh * Decimal("10")

    assert result.import_charge_cents == Decimal("0")
    assert float(result.export_credit_cents) == pytest.approx(float(expected_credit), rel=1e-6)
    assert float(result.total_cents) == pytest.approx(
        float(Decimal("100") - expected_credit), rel=1e-6
    )


def test_single_rate_mixed_import_export() -> None:
    """Mixed readings: import and export accounted for separately."""
    from candela.tariffs.strategies.single_rate import SingleRateStrategy

    import_readings = [_reading(_ts(18, i * 5), grid_w=2000) for i in range(6)]
    export_readings = [_reading(_ts(12, i * 5), grid_w=-1000) for i in range(6)]
    plan = _plan("single_rate", supply_cents=0.0, fit_cents=5.0)
    rates = [_rate("flat", cents_per_kwh=20.0)]

    result = SingleRateStrategy().compute(import_readings + export_readings, plan, rates)

    assert result.import_charge_cents > 0
    assert result.export_credit_cents > 0
    assert result.import_charge_cents != result.export_credit_cents


def test_single_rate_no_fit_means_no_export_credit() -> None:
    """When FiT is None, export generates zero credit."""
    from candela.tariffs.strategies.single_rate import SingleRateStrategy

    readings = [_reading(_ts(12, 0), grid_w=-1000)]
    plan = _plan("single_rate", supply_cents=0.0, fit_cents=None)
    rates = [_rate("flat", cents_per_kwh=30.0)]

    result = SingleRateStrategy().compute(readings, plan, rates)

    assert result.export_credit_cents == Decimal("0")


def test_single_rate_supply_spans_days() -> None:
    """Supply charge is multiplied by the number of unique calendar days covered."""
    from candela.tariffs.strategies.single_rate import SingleRateStrategy

    readings = [
        _reading(_ts(0, 0, day=1), grid_w=0),
        _reading(_ts(0, 0, day=2), grid_w=0),
        _reading(_ts(0, 0, day=3), grid_w=0),
    ]
    plan = _plan("single_rate", supply_cents=100.0)
    rates = [_rate("flat", cents_per_kwh=30.0)]

    result = SingleRateStrategy().compute(readings, plan, rates)

    assert result.supply_charge_cents == Decimal("300")  # 3 days × 100 c


# ---------------------------------------------------------------------------
# TOU strategy
# ---------------------------------------------------------------------------


def test_tou_peak_window() -> None:
    """A reading at 4:30 pm (peak window 16:00–21:00) uses the peak rate."""
    from candela.tariffs.strategies.tou import TOUStrategy

    readings = [_reading(_ts(16, 30), grid_w=1000)]
    plan = _plan("tou", supply_cents=0.0)
    rates = [
        _rate("peak", cents_per_kwh=40.0, window_start=time(16, 0), window_end=time(21, 0)),
        _rate("offpeak", cents_per_kwh=15.0),
    ]

    result = TOUStrategy().compute(readings, plan, rates)

    assert "peak" in result.period_breakdown
    assert result.period_breakdown["peak"].kwh > 0
    assert result.period_breakdown["peak"].cents > 0
    # Should not also appear in offpeak
    assert result.period_breakdown.get("offpeak", None) is None or \
        result.period_breakdown["offpeak"].kwh == Decimal("0")


def test_tou_offpeak_window() -> None:
    """A reading at 2 am (outside any windowed rate) falls to offpeak."""
    from candela.tariffs.strategies.tou import TOUStrategy

    readings = [_reading(_ts(2, 0), grid_w=500)]
    plan = _plan("tou", supply_cents=0.0)
    rates = [
        _rate("peak", cents_per_kwh=40.0, window_start=time(16, 0), window_end=time(21, 0)),
        _rate("shoulder", cents_per_kwh=28.0, window_start=time(7, 0), window_end=time(16, 0)),
        _rate("offpeak", cents_per_kwh=15.0),
    ]

    result = TOUStrategy().compute(readings, plan, rates)

    assert result.period_breakdown["offpeak"].kwh > 0
    assert result.period_breakdown.get("peak") is None or \
        result.period_breakdown["peak"].kwh == Decimal("0")


def test_tou_shoulder_window() -> None:
    """A reading at 10 am falls into the shoulder window (7:00–16:00)."""
    from candela.tariffs.strategies.tou import TOUStrategy

    readings = [_reading(_ts(10, 0), grid_w=2000)]
    plan = _plan("tou", supply_cents=0.0)
    rates = [
        _rate("peak", cents_per_kwh=40.0, window_start=time(16, 0), window_end=time(21, 0)),
        _rate("shoulder", cents_per_kwh=28.0, window_start=time(7, 0), window_end=time(16, 0)),
        _rate("offpeak", cents_per_kwh=15.0),
    ]

    result = TOUStrategy().compute(readings, plan, rates)

    assert result.period_breakdown["shoulder"].kwh > 0
    assert result.period_breakdown.get("peak") is None or \
        result.period_breakdown["peak"].kwh == Decimal("0")


def test_tou_mixed_periods() -> None:
    """Readings across three periods each accumulate in the correct bucket."""
    from candela.tariffs.strategies.tou import TOUStrategy

    readings = [
        _reading(_ts(3, 0), grid_w=500),    # offpeak
        _reading(_ts(9, 0), grid_w=1000),   # shoulder
        _reading(_ts(17, 0), grid_w=2000),  # peak
    ]
    plan = _plan("tou", supply_cents=0.0)
    rates = [
        _rate("peak", cents_per_kwh=40.0, window_start=time(16, 0), window_end=time(21, 0)),
        _rate("shoulder", cents_per_kwh=28.0, window_start=time(7, 0), window_end=time(16, 0)),
        _rate("offpeak", cents_per_kwh=15.0),
    ]

    result = TOUStrategy().compute(readings, plan, rates)

    assert result.period_breakdown["peak"].kwh > 0
    assert result.period_breakdown["shoulder"].kwh > 0
    assert result.period_breakdown["offpeak"].kwh > 0

    # Peak rate is highest → peak bucket has highest cost per kWh
    peak_cpkwh = result.period_breakdown["peak"].cents / result.period_breakdown["peak"].kwh
    offpeak_cpkwh = result.period_breakdown["offpeak"].cents / result.period_breakdown["offpeak"].kwh
    assert peak_cpkwh > offpeak_cpkwh


def test_tou_days_of_week_filter() -> None:
    """A reading on Wednesday is excluded from a Mon–Fri-only rate."""
    from candela.tariffs.strategies.tou import TOUStrategy

    # 2026-01-07 is a Wednesday (weekday 2)
    wednesday_ts = datetime(2026, 1, 7, 17, 0, 0, tzinfo=UTC)
    readings = [_reading(wednesday_ts, grid_w=1000)]
    plan = _plan("tou", supply_cents=0.0)
    rates = [
        # Peak only on Mon–Fri (0–4)
        _rate(
            "peak",
            cents_per_kwh=40.0,
            window_start=time(16, 0),
            window_end=time(21, 0),
            days_of_week=[0, 1, 3, 4],  # Mon, Tue, Thu, Fri — NOT Wednesday
        ),
        _rate("offpeak", cents_per_kwh=15.0),
    ]

    result = TOUStrategy().compute(readings, plan, rates)

    # Wednesday is not in the peak rate's days_of_week, so falls to offpeak
    assert result.period_breakdown.get("peak") is None or \
        result.period_breakdown["peak"].kwh == Decimal("0")
    assert result.period_breakdown["offpeak"].kwh > 0


def test_tou_export_uses_fit() -> None:
    """TOU export credit uses the plan FiT rate, not the period rate."""
    from candela.tariffs.strategies.tou import TOUStrategy

    readings = [_reading(_ts(12, 0), grid_w=-1000)]
    plan = _plan("tou", supply_cents=0.0, fit_cents=5.0)
    rates = [
        _rate("shoulder", cents_per_kwh=28.0, window_start=time(7, 0), window_end=time(16, 0)),
        _rate("offpeak", cents_per_kwh=15.0),
    ]

    result = TOUStrategy().compute(readings, plan, rates)

    assert result.export_credit_cents > 0
    assert result.import_charge_cents == Decimal("0")


# ---------------------------------------------------------------------------
# Demand strategy
# ---------------------------------------------------------------------------


def test_demand_basic_demand_charge() -> None:
    """Peak 30-min block at 4000 W → demand = 4 kW × rate = demand_charge_cents."""
    from candela.tariffs.strategies.demand import DemandStrategy

    # 16:00–16:30 block: 6 readings × 4000 W
    # block_kwh = 4000 * 6 * (5/60) / 1000 = 2.0 kWh
    # block_demand_kw = 2.0 * 2 = 4.0 kW
    peak_block = [_reading(_ts(16, i * 5), grid_w=4000) for i in range(6)]
    # Some readings outside demand window
    outside = [_reading(_ts(10, i * 5), grid_w=1000) for i in range(6)]

    plan = _plan("demand", supply_cents=0.0)
    rates = [
        _rate("peak", cents_per_kwh=40.0, window_start=time(16, 0), window_end=time(21, 0)),
        _rate("offpeak", cents_per_kwh=15.0),
        _rate(
            "demand",
            cents_per_kw=50.0,
            demand_window_start=time(16, 0),
            demand_window_end=time(21, 0),
        ),
    ]

    result = DemandStrategy().compute(peak_block + outside, plan, rates)

    assert float(result.demand_charge_cents) == pytest.approx(200.0, rel=1e-4)  # 4 kW × 50 c/kW


def test_demand_uses_max_block() -> None:
    """The demand charge is based on the single highest 30-min block."""
    from candela.tariffs.strategies.demand import DemandStrategy

    # Block 1 (16:00–16:30): 3000 W → 3 kW demand
    block1 = [_reading(_ts(16, i * 5), grid_w=3000) for i in range(6)]
    # Block 2 (16:30–17:00): 5000 W → 5 kW demand
    block2 = [_reading(_ts(16, 30 + i * 5), grid_w=5000) for i in range(6)]

    plan = _plan("demand", supply_cents=0.0)
    rates = [
        _rate("peak", cents_per_kwh=40.0, window_start=time(16, 0), window_end=time(21, 0)),
        _rate("offpeak", cents_per_kwh=15.0),
        _rate(
            "demand",
            cents_per_kw=100.0,
            demand_window_start=time(16, 0),
            demand_window_end=time(21, 0),
        ),
    ]

    result = DemandStrategy().compute(block1 + block2, plan, rates)

    # Max block is block2: 5 kW × 100 c/kW = 500 c
    assert float(result.demand_charge_cents) == pytest.approx(500.0, rel=1e-4)


def test_demand_spike_outside_window_ignored() -> None:
    """A large spike outside the demand window does not inflate the demand charge."""
    from candela.tariffs.strategies.demand import DemandStrategy

    # 10:00 block — outside 16:00–21:00 demand window
    outside_spike = [_reading(_ts(10, i * 5), grid_w=10000) for i in range(6)]
    # Normal 17:00 block
    normal_peak = [_reading(_ts(17, i * 5), grid_w=1000) for i in range(6)]

    plan = _plan("demand", supply_cents=0.0)
    rates = [
        _rate("peak", cents_per_kwh=40.0, window_start=time(16, 0), window_end=time(21, 0)),
        _rate("offpeak", cents_per_kwh=15.0),
        _rate(
            "demand",
            cents_per_kw=100.0,
            demand_window_start=time(16, 0),
            demand_window_end=time(21, 0),
        ),
    ]

    result = DemandStrategy().compute(outside_spike + normal_peak, plan, rates)

    # Demand based only on the 17:00 block: 1 kW × 100 = 100 c
    assert float(result.demand_charge_cents) == pytest.approx(100.0, rel=1e-4)


def test_demand_zero_when_no_readings_in_window() -> None:
    """If no readings fall in the demand window, demand charge is zero."""
    from candela.tariffs.strategies.demand import DemandStrategy

    readings = [_reading(_ts(3, 0), grid_w=2000)]
    plan = _plan("demand", supply_cents=0.0)
    rates = [
        _rate("offpeak", cents_per_kwh=15.0),
        _rate(
            "demand",
            cents_per_kw=100.0,
            demand_window_start=time(16, 0),
            demand_window_end=time(21, 0),
        ),
    ]

    result = DemandStrategy().compute(readings, plan, rates)

    assert result.demand_charge_cents == Decimal("0")


# ---------------------------------------------------------------------------
# Wholesale strategy
# ---------------------------------------------------------------------------


def _aemo_price(
    hour: int,
    minute: int,
    rrp: float,
    region: str = "QLD1",
) -> AemoPrice:
    """30-minute AEMO price interval starting at {hour}:{minute} UTC on 2026-01-01."""
    from datetime import timedelta
    start = datetime(2026, 1, 1, hour, minute, 0, tzinfo=UTC)
    return AemoPrice(
        interval_start=start,
        interval_end=start + timedelta(minutes=30),
        rrp_per_mwh=rrp,
        region=region,
    )


def test_wholesale_import_positive_price() -> None:
    """Import at a positive AEMO price is charged at (rrp/10 + adder) c/kWh."""
    from candela.tariffs.strategies.wholesale import WholesaleStrategy

    # 1 reading at 16:00 → falls in the 16:00–16:30 AEMO block
    reading = _reading(_ts(16, 0), grid_w=1000)
    plan = _plan("wholesale", supply_cents=0.0)
    rates = []
    aemo_prices = [_aemo_price(16, 0, rrp=100.0)]
    adder = Decimal("18.0")

    result = WholesaleStrategy(wholesale_adder_cents_per_kwh=adder).compute(
        [reading], plan, rates, aemo_prices=aemo_prices
    )

    kwh = Decimal("1000") * Decimal(str(_INTERVAL_HOURS)) / Decimal("1000")
    rate_cents_per_kwh = Decimal("100") / Decimal("10") + adder  # rrp/10 + adder
    expected_import = kwh * rate_cents_per_kwh

    assert float(result.import_charge_cents) == pytest.approx(float(expected_import), rel=1e-6)
    assert result.export_credit_cents == Decimal("0")


def test_wholesale_export_positive_price() -> None:
    """Export at a positive AEMO price earns credit at (rrp × 0.7 / 10) c/kWh."""
    from candela.tariffs.strategies.wholesale import WholesaleStrategy

    reading = _reading(_ts(12, 0), grid_w=-1000)
    plan = _plan("wholesale", supply_cents=0.0)
    rates = []
    aemo_prices = [_aemo_price(12, 0, rrp=80.0)]
    adder = Decimal("18.0")

    result = WholesaleStrategy(wholesale_adder_cents_per_kwh=adder).compute(
        [reading], plan, rates, aemo_prices=aemo_prices
    )

    kwh = Decimal("1000") * Decimal(str(_INTERVAL_HOURS)) / Decimal("1000")
    credit_rate = Decimal("80") * Decimal("0.7") / Decimal("10")
    expected_credit = kwh * credit_rate

    assert result.import_charge_cents == Decimal("0")
    assert float(result.export_credit_cents) == pytest.approx(float(expected_credit), rel=1e-6)


def test_wholesale_negative_price_export() -> None:
    """During a negative price, exporting earns negative credit (a cost)."""
    from candela.tariffs.strategies.wholesale import WholesaleStrategy

    reading = _reading(_ts(3, 0), grid_w=-2000)
    plan = _plan("wholesale", supply_cents=0.0)
    rates = []
    aemo_prices = [_aemo_price(3, 0, rrp=-50.0)]  # negative spot price
    adder = Decimal("18.0")

    result = WholesaleStrategy(wholesale_adder_cents_per_kwh=adder).compute(
        [reading], plan, rates, aemo_prices=aemo_prices
    )

    # credit_rate = -50 × 0.7 / 10 = -3.5 c/kWh → negative credit
    # export_credit_cents is the absolute credit; negative here means it costs money
    assert float(result.export_credit_cents) < 0


def test_wholesale_negative_price_import() -> None:
    """During a negative price, importing can result in negative import cost."""
    from candela.tariffs.strategies.wholesale import WholesaleStrategy

    reading = _reading(_ts(3, 0), grid_w=1000)
    plan = _plan("wholesale", supply_cents=0.0)
    rates = []
    aemo_prices = [_aemo_price(3, 0, rrp=-200.0)]  # very negative
    adder = Decimal("18.0")

    result = WholesaleStrategy(wholesale_adder_cents_per_kwh=adder).compute(
        [reading], plan, rates, aemo_prices=aemo_prices
    )

    # rate = -200/10 + 18 = -20 + 18 = -2 c/kWh (negative: grid pays you to import)
    assert float(result.import_charge_cents) < 0


def test_wholesale_reading_with_no_aemo_price_skipped() -> None:
    """Readings without a matching AEMO interval contribute zero energy cost."""
    from candela.tariffs.strategies.wholesale import WholesaleStrategy

    reading = _reading(_ts(14, 0), grid_w=3000)
    plan = _plan("wholesale", supply_cents=0.0)
    rates = []
    # No AEMO price for 14:00 interval
    aemo_prices = [_aemo_price(16, 0, rrp=100.0)]
    adder = Decimal("18.0")

    result = WholesaleStrategy(wholesale_adder_cents_per_kwh=adder).compute(
        [reading], plan, rates, aemo_prices=aemo_prices
    )

    assert result.import_charge_cents == Decimal("0")


def test_wholesale_supply_charge_applied() -> None:
    """Supply charge is included in the wholesale bill total."""
    from candela.tariffs.strategies.wholesale import WholesaleStrategy

    readings = [_reading(_ts(0, 0, day=1), grid_w=0), _reading(_ts(0, 0, day=2), grid_w=0)]
    plan = _plan("wholesale", supply_cents=150.0)
    rates = []
    aemo_prices = []
    adder = Decimal("18.0")

    result = WholesaleStrategy(wholesale_adder_cents_per_kwh=adder).compute(
        readings, plan, rates, aemo_prices=aemo_prices
    )

    assert result.supply_charge_cents == Decimal("300")  # 2 days × 150 c
