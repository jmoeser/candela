"""Compute the variable energy cost of load events against a tariff plan.

The cost is the *import* energy charge only — supply charge is excluded because
it's a fixed daily cost that can't be attributed to individual appliances.
Demand charges are also excluded: they're based on the monthly peak kW, which
can't be cleanly split across individual events.

Only events with confidence >= 0.7 are considered "high-confidence" for cost
summaries (see summarise_load_costs filter in callers).

Algorithm for TOU / wholesale plans
------------------------------------
The event window [started_at, ended_at] is walked in 5-minute steps (matching
the inverter polling granularity).  Each slot's kWh contribution is assumed
constant (even draw), so kWh/slot = total_kwh / n_slots.  The applicable rate
is found via match_rate() for TOU, or from the AEMO price lookup for wholesale.
"""

from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING

from candela.collector.aemo import AemoPrice
from candela.tariffs.models import TariffPlan, TariffRate
from candela.tariffs.strategies.base import match_rate

if TYPE_CHECKING:
    from candela.db import Database

_WHOLESALE_ADDER = Decimal("18.0")  # c/kWh network / retail adder for wholesale plans


# ---------------------------------------------------------------------------
# Data types
# ---------------------------------------------------------------------------


@dataclass
class LoadEvent:
    """Minimal load event data needed for cost computation."""

    load_name: str
    started_at: datetime
    ended_at: datetime | None
    kwh: float | None


@dataclass
class LoadCostSummary:
    """Aggregate cost for a single load type over a date range."""

    load_name: str
    event_count: int
    kwh: float
    cost_cents: float | None  # None = energy cost not computable (missing data)


# ---------------------------------------------------------------------------
# Per-event computation
# ---------------------------------------------------------------------------


def compute_load_event_cost(
    event: LoadEvent,
    plan: TariffPlan,
    rates: list[TariffRate],
    aemo_prices: list[AemoPrice] | None = None,
) -> float | None:
    """Return variable energy cost in cents for one load event, or None.

    Returns None when:
    - The event has no kWh or no ended_at (incomplete data)

    Demand plans have a TOU energy component that *can* be attributed per-load;
    only the demand charge itself (c/kW peak) cannot.  We compute the energy
    cost and omit the demand charge — the same exclusion that applies to all
    plan types (supply charge is also always excluded).
    """
    if not event.kwh or event.kwh <= 0:
        return None
    if not event.ended_at:
        return None

    duration_s = (event.ended_at - event.started_at).total_seconds()
    if duration_s <= 0:
        return None

    kwh = Decimal(str(event.kwh))

    if plan.plan_type == "single_rate":
        flat = next(
            (r for r in rates if r.rate_type == "flat" and r.cents_per_kwh is not None),
            None,
        )
        if flat is None:
            return None
        return float(kwh * flat.cents_per_kwh)  # type: ignore[operator]

    # demand plans share TOU energy rates — compute energy cost, skip demand charge
    if plan.plan_type in ("tou", "demand", "wholesale"):
        energy_rates = [r for r in rates if r.rate_type != "demand"]

        # Build AEMO lookup: interval_start (tz-aware UTC) → rrp_per_mwh
        aemo_lookup: dict[datetime, float] = {}
        if plan.plan_type == "wholesale" and aemo_prices:
            for p in aemo_prices:
                aemo_lookup[p.interval_start] = p.rrp_per_mwh

        # Walk event window in 5-minute steps
        step = timedelta(minutes=5)
        slots: list[datetime] = []
        t = event.started_at
        while t < event.ended_at:
            slots.append(t)
            t += step

        if not slots:
            return None

        kwh_per_slot = kwh / Decimal(str(len(slots)))
        total_cents = Decimal("0")
        # Precompute fallback for slots that don't match any rate window (mirrors
        # TOUStrategy which defaults to "offpeak" when match_rate returns None).
        rate_by_type = {r.rate_type: r for r in energy_rates}
        fallback_rate = rate_by_type.get("offpeak") or rate_by_type.get("flat")

        for slot_t in slots:
            if plan.plan_type in ("tou", "demand"):
                matched = match_rate(slot_t, energy_rates) or fallback_rate
                if matched and matched.cents_per_kwh is not None:
                    total_cents += kwh_per_slot * matched.cents_per_kwh
            else:  # wholesale
                aligned = slot_t.replace(
                    minute=(slot_t.minute // 5) * 5, second=0, microsecond=0
                )
                rrp = aemo_lookup.get(aligned, 0.0)
                cents_per_kwh = Decimal(str(rrp / 10)) + _WHOLESALE_ADDER
                total_cents += kwh_per_slot * cents_per_kwh

        return float(total_cents)

    return None


# ---------------------------------------------------------------------------
# Aggregation
# ---------------------------------------------------------------------------


def summarise_load_costs(
    events: list[LoadEvent],
    plan: TariffPlan,
    rates: list[TariffRate],
    aemo_prices: list[AemoPrice] | None = None,
) -> list[LoadCostSummary]:
    """Aggregate load event costs by load_name.

    Events with un-computable costs (e.g. demand plans, missing data) are
    counted and their kWh summed, but cost_cents is set to None for that group.
    """
    by_name: dict[str, dict] = defaultdict(
        lambda: {"count": 0, "kwh": 0.0, "cost_cents": 0.0, "computable": True}
    )

    for event in events:
        cost = compute_load_event_cost(event, plan, rates, aemo_prices)
        entry = by_name[event.load_name]
        entry["count"] += 1
        entry["kwh"] += event.kwh or 0.0
        if cost is None:
            entry["computable"] = False
        else:
            entry["cost_cents"] += cost

    return [
        LoadCostSummary(
            load_name=name,
            event_count=entry["count"],
            kwh=entry["kwh"],
            cost_cents=entry["cost_cents"] if entry["computable"] else None,
        )
        for name, entry in sorted(by_name.items())
    ]


# ---------------------------------------------------------------------------
# Async helper for routes
# ---------------------------------------------------------------------------


async def load_costs_for_plan(
    events: list[LoadEvent],
    plan_id: int,
    date_from: date,
    date_to: date,
    db: "Database",
) -> list[LoadCostSummary]:
    """Fetch plan / rates / AEMO prices from *db* then compute load costs."""
    from candela.tariffs.engine import fetch_plan, fetch_rates, fetch_aemo_prices

    plan = await fetch_plan(plan_id, db)
    rates = await fetch_rates(plan_id, db)
    aemo_prices = None
    if plan.plan_type == "wholesale":
        aemo_prices = await fetch_aemo_prices(date_from, date_to, db)

    return summarise_load_costs(events, plan, rates, aemo_prices)
