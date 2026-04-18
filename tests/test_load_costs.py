"""Unit tests for candela.tariffs.load_costs."""

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from candela.collector.aemo import AemoPrice
from candela.tariffs.load_costs import (
    LoadEvent,
    compute_load_event_cost,
    summarise_load_costs,
)
from candela.tariffs.models import TariffPlan, TariffRate

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

_VALID_FROM = datetime.fromisoformat("2026-01-01").date()


def _plan(plan_type: str, fit_cents: float = 5.0) -> TariffPlan:
    return TariffPlan(
        id=1,
        name="Test Plan",
        plan_type=plan_type,
        supply_charge_daily_cents=Decimal("100"),
        valid_from=_VALID_FROM,
        feed_in_tariff_cents=Decimal(str(fit_cents)),
    )


def _flat_rate(cents: float) -> TariffRate:
    return TariffRate(
        id=1, plan_id=1, rate_type="flat", cents_per_kwh=Decimal(str(cents))
    )


def _tou_rates() -> list[TariffRate]:
    from datetime import time

    return [
        TariffRate(
            id=2,
            plan_id=1,
            rate_type="peak",
            cents_per_kwh=Decimal("40"),
            window_start=time(7, 0),
            window_end=time(21, 0),
        ),
        TariffRate(
            id=3,
            plan_id=1,
            rate_type="offpeak",
            cents_per_kwh=Decimal("15"),
            window_start=time(21, 0),
            window_end=time(7, 0),
        ),
    ]


def _ev_event(
    start: str,
    end: str,
    kwh: float = 10.0,
    load_name: str = "ev_charging",
) -> LoadEvent:
    return LoadEvent(
        load_name=load_name,
        started_at=datetime.fromisoformat(start),
        ended_at=datetime.fromisoformat(end),
        kwh=kwh,
    )


# ---------------------------------------------------------------------------
# compute_load_event_cost — single_rate
# ---------------------------------------------------------------------------


def test_single_rate_basic() -> None:
    """Single-rate plan: cost = kwh × rate."""
    event = _ev_event("2026-04-07T10:00:00+00:00", "2026-04-07T12:00:00+00:00", kwh=5.0)
    plan = _plan("single_rate")
    rates = [_flat_rate(30.0)]

    cost = compute_load_event_cost(event, plan, rates)
    assert cost == pytest.approx(5.0 * 30.0)


def test_single_rate_no_flat_rate_returns_none() -> None:
    event = _ev_event("2026-04-07T10:00:00+00:00", "2026-04-07T12:00:00+00:00")
    plan = _plan("single_rate")
    cost = compute_load_event_cost(event, plan, [])
    assert cost is None


def test_missing_kwh_returns_none() -> None:
    event = LoadEvent(
        load_name="ev_charging",
        started_at=datetime.fromisoformat("2026-04-07T10:00:00+00:00"),
        ended_at=datetime.fromisoformat("2026-04-07T12:00:00+00:00"),
        kwh=None,
    )
    cost = compute_load_event_cost(event, _plan("single_rate"), [_flat_rate(30)])
    assert cost is None


def test_missing_ended_at_returns_none() -> None:
    event = LoadEvent(
        load_name="ev_charging",
        started_at=datetime.fromisoformat("2026-04-07T10:00:00+00:00"),
        ended_at=None,
        kwh=5.0,
    )
    cost = compute_load_event_cost(event, _plan("single_rate"), [_flat_rate(30)])
    assert cost is None


def test_demand_plan_computes_energy_cost() -> None:
    """Demand plans have TOU energy rates — compute energy cost, skip demand charge."""
    event = _ev_event("2026-04-07T10:00:00+00:00", "2026-04-07T12:00:00+00:00", kwh=2.0)
    plan = _plan("demand")
    # Supply a peak rate that covers 10am–12pm
    from datetime import time

    rates = [
        TariffRate(
            id=1,
            plan_id=1,
            rate_type="peak",
            cents_per_kwh=Decimal("40"),
            window_start=time(7, 0),
            window_end=time(21, 0),
        ),
        TariffRate(
            id=2,
            plan_id=1,
            rate_type="demand",
            cents_per_kw=Decimal("56"),
            demand_window_start=time(16, 0),
            demand_window_end=time(21, 0),
        ),
    ]
    cost = compute_load_event_cost(event, plan, rates)
    # 2 kWh × 40 c/kWh = 80c (demand charge not included)
    assert cost == pytest.approx(80.0, rel=1e-2)


# ---------------------------------------------------------------------------
# compute_load_event_cost — TOU
# ---------------------------------------------------------------------------


def test_tou_all_peak() -> None:
    """Event entirely in peak hours → all kWh charged at peak rate."""
    # Peak window is 07:00–21:00 UTC; event is 10:00–11:00 UTC (1 h, all peak)
    event = _ev_event("2026-04-07T10:00:00+00:00", "2026-04-07T11:00:00+00:00", kwh=2.0)
    plan = _plan("tou")
    cost = compute_load_event_cost(event, plan, _tou_rates())
    # All 2 kWh at 40 c/kWh = 80c
    assert cost == pytest.approx(80.0, rel=1e-2)


def test_tou_all_offpeak() -> None:
    """Event entirely in offpeak hours → all kWh charged at offpeak rate."""
    # Offpeak is 21:00–07:00 UTC; event is 22:00–23:00 UTC
    event = _ev_event("2026-04-07T22:00:00+00:00", "2026-04-07T23:00:00+00:00", kwh=3.0)
    plan = _plan("tou")
    cost = compute_load_event_cost(event, plan, _tou_rates())
    # All 3 kWh at 15 c/kWh = 45c
    assert cost == pytest.approx(45.0, rel=1e-2)


def test_tou_spans_rate_boundary() -> None:
    """Event crossing peak/offpeak boundary prorates kWh to each period."""
    # Event 20:00–22:00 UTC: first hour peak (20:00–21:00), second hour offpeak (21:00–22:00)
    event = _ev_event("2026-04-07T20:00:00+00:00", "2026-04-07T22:00:00+00:00", kwh=4.0)
    plan = _plan("tou")
    cost = compute_load_event_cost(event, plan, _tou_rates())
    # ~2 kWh peak @ 40c + ~2 kWh offpeak @ 15c = 80 + 30 = 110c  (±5% for slot rounding)
    assert cost == pytest.approx(110.0, rel=0.05)


# ---------------------------------------------------------------------------
# compute_load_event_cost — wholesale
# ---------------------------------------------------------------------------


def test_wholesale_with_aemo_prices() -> None:
    """Wholesale plan applies AEMO price + adder for each 5-min slot."""
    event = _ev_event("2026-04-07T10:00:00+00:00", "2026-04-07T10:30:00+00:00", kwh=1.0)
    plan = _plan("wholesale")
    # AEMO price 100 $/MWh = 10 c/kWh; adder = 18 c/kWh → 28 c/kWh per slot
    # Provide one price record per 5-minute slot in the window
    from datetime import timedelta

    base = datetime(2026, 4, 7, 10, 0, 0, tzinfo=UTC)
    aemo_prices = [
        AemoPrice(
            interval_start=base + timedelta(minutes=5 * i),
            interval_end=base + timedelta(minutes=5 * (i + 1)),
            rrp_per_mwh=100.0,
            region="QLD1",
        )
        for i in range(6)
    ]
    cost = compute_load_event_cost(event, plan, [], aemo_prices=aemo_prices)
    # 1 kWh × 28 c/kWh = 28c
    assert cost == pytest.approx(28.0, rel=1e-2)


# ---------------------------------------------------------------------------
# summarise_load_costs
# ---------------------------------------------------------------------------


def test_summarise_groups_by_load_name() -> None:
    events = [
        _ev_event(
            "2026-04-07T10:00:00+00:00",
            "2026-04-07T11:00:00+00:00",
            kwh=2.0,
            load_name="ev_charging",
        ),
        _ev_event(
            "2026-04-07T12:00:00+00:00",
            "2026-04-07T13:00:00+00:00",
            kwh=1.0,
            load_name="ev_charging",
        ),
        _ev_event(
            "2026-04-07T14:00:00+00:00",
            "2026-04-07T14:30:00+00:00",
            kwh=0.5,
            load_name="hot_water_heatpump",
        ),
    ]
    plan = _plan("single_rate")
    rates = [_flat_rate(30.0)]
    summaries = summarise_load_costs(events, plan, rates)

    by_name = {s.load_name: s for s in summaries}
    assert set(by_name) == {"ev_charging", "hot_water_heatpump"}

    ev = by_name["ev_charging"]
    assert ev.event_count == 2
    assert ev.kwh == pytest.approx(3.0)
    assert ev.cost_cents == pytest.approx(90.0)  # 3 kWh × 30c

    hw = by_name["hot_water_heatpump"]
    assert hw.event_count == 1
    assert hw.kwh == pytest.approx(0.5)
    assert hw.cost_cents == pytest.approx(15.0)  # 0.5 kWh × 30c


def test_summarise_demand_plan_computes_energy_cost() -> None:
    """summarise_load_costs returns a cost (not None) for demand plans."""
    from datetime import time

    events = [
        _ev_event("2026-04-07T10:00:00+00:00", "2026-04-07T11:00:00+00:00", kwh=2.0)
    ]
    plan = _plan("demand")
    rates = [
        TariffRate(
            id=1,
            plan_id=1,
            rate_type="peak",
            cents_per_kwh=Decimal("40"),
            window_start=time(7, 0),
            window_end=time(21, 0),
        ),
    ]
    summaries = summarise_load_costs(events, plan, rates)
    assert len(summaries) == 1
    assert summaries[0].cost_cents == pytest.approx(80.0, rel=1e-2)


def test_summarise_empty_events() -> None:
    summaries = summarise_load_costs([], _plan("single_rate"), [_flat_rate(30)])
    assert summaries == []
