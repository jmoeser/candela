"""Tariff engine — compute_bill() entry point.

Fetches all necessary data from the database, selects the correct strategy,
and delegates computation.  All I/O is isolated here; strategies are pure.
"""

import json
import logging
from datetime import UTC, date, datetime, time
from decimal import Decimal
from typing import Any

from candela.collector.aemo import AemoPrice
from candela.db import Database
from candela.tariffs.models import BillResult, SolarReading, TariffPlan, TariffRate
from candela.tariffs.strategies.base import TariffStrategy
from candela.tariffs.strategies.demand import DemandStrategy
from candela.tariffs.strategies.single_rate import SingleRateStrategy
from candela.tariffs.strategies.tou import TOUStrategy
from candela.tariffs.strategies.wholesale import WholesaleStrategy

logger = logging.getLogger(__name__)

_STRATEGIES: dict[str, TariffStrategy] = {
    "single_rate": SingleRateStrategy(),
    "tou": TOUStrategy(),
    "demand": DemandStrategy(),
}


async def compute_bill(
    plan_id: int,
    date_from: date,
    date_to: date,
    db: Database,
    *,
    wholesale_adder: Decimal | None = None,
) -> BillResult:
    """Compute the electricity bill for *plan_id* over [date_from, date_to].

    Parameters
    ----------
    plan_id:
        Primary key of the ``tariff_plans`` row.
    date_from, date_to:
        Inclusive date range.  All ``solar_readings`` with ``ts`` on or
        between these dates (UTC) are included.
    db:
        Active database connection.
    wholesale_adder:
        Override for the wholesale adder (c/kWh).  When ``None``, the
        default from ``WholesaleStrategy`` is used.

    Raises
    ------
    ValueError
        If *plan_id* does not exist in ``tariff_plans``.
    """
    plan = await _fetch_plan(plan_id, db)
    rates = await _fetch_rates(plan_id, db)
    readings = await _fetch_readings(date_from, date_to, db)

    logger.info(
        "compute_bill plan=%d type=%s readings=%d range=%s–%s",
        plan_id,
        plan.plan_type,
        len(readings),
        date_from,
        date_to,
    )

    if plan.plan_type == "wholesale":
        aemo_prices = await _fetch_aemo_prices(date_from, date_to, db)
        adder = wholesale_adder if wholesale_adder is not None else Decimal("18.0")
        ws_strategy = WholesaleStrategy(wholesale_adder_cents_per_kwh=adder)
        return ws_strategy.compute(readings, plan, rates, aemo_prices=aemo_prices)

    strategy: TariffStrategy | None = _STRATEGIES.get(plan.plan_type)
    if strategy is None:
        raise ValueError(f"Unknown plan type: {plan.plan_type!r}")

    return strategy.compute(readings, plan, rates)


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------


async def _fetch_plan(plan_id: int, db: Database) -> TariffPlan:
    row = await db.fetchrow("SELECT * FROM tariff_plans WHERE id = ?", plan_id)
    if row is None:
        raise ValueError(f"Tariff plan {plan_id} not found")

    return TariffPlan(
        id=int(row["id"]),
        name=str(row["name"]),
        plan_type=str(row["plan_type"]),
        supply_charge_daily_cents=Decimal(str(row["supply_charge_daily_cents"])),
        valid_from=date.fromisoformat(str(row["valid_from"])),
        retailer=str(row["retailer"]) if row["retailer"] else None,
        feed_in_tariff_cents=(
            Decimal(str(row["feed_in_tariff_cents"]))
            if row["feed_in_tariff_cents"] is not None
            else None
        ),
        valid_to=(
            date.fromisoformat(str(row["valid_to"])) if row["valid_to"] else None
        ),
        notes=str(row["notes"]) if row["notes"] else None,
    )


async def _fetch_rates(plan_id: int, db: Database) -> list[TariffRate]:
    rows = await db.fetch(
        "SELECT * FROM tariff_rates WHERE plan_id = ? ORDER BY id", plan_id
    )
    return [_row_to_rate(r) for r in rows]


def _row_to_rate(row: Any) -> TariffRate:
    return TariffRate(
        id=int(row["id"]),
        plan_id=int(row["plan_id"]),
        rate_type=str(row["rate_type"]),
        cents_per_kwh=(
            Decimal(str(row["cents_per_kwh"]))
            if row["cents_per_kwh"] is not None
            else None
        ),
        cents_per_kw=(
            Decimal(str(row["cents_per_kw"]))
            if row["cents_per_kw"] is not None
            else None
        ),
        window_start=_parse_time(row["window_start"]),
        window_end=_parse_time(row["window_end"]),
        days_of_week=(json.loads(row["days_of_week"]) if row["days_of_week"] else None),
        months=json.loads(row["months"]) if row["months"] else None,
        demand_window_start=_parse_time(row["demand_window_start"]),
        demand_window_end=_parse_time(row["demand_window_end"]),
    )


async def _fetch_readings(
    date_from: date, date_to: date, db: Database
) -> list[SolarReading]:
    # Use ISO8601 string bounds that cover the full days in UTC
    ts_from = datetime(
        date_from.year, date_from.month, date_from.day, tzinfo=UTC
    ).isoformat()
    ts_to = datetime(
        date_to.year, date_to.month, date_to.day, 23, 59, 59, tzinfo=UTC
    ).isoformat()

    rows = await db.fetch(
        "SELECT * FROM solar_readings WHERE ts >= ? AND ts <= ? ORDER BY ts",
        ts_from,
        ts_to,
    )
    return [_row_to_reading(r) for r in rows]


def _row_to_reading(row: Any) -> SolarReading:
    ts_raw = str(row["ts"])
    # Handle both "Z" suffix and "+00:00" offset
    if ts_raw.endswith("Z"):
        ts_raw = ts_raw[:-1] + "+00:00"
    ts = datetime.fromisoformat(ts_raw)
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=UTC)

    return SolarReading(
        ts=ts,
        solar_w=int(row["solar_w"]),
        grid_w=int(row["grid_w"]),
        load_w=int(row["load_w"]),
        daily_yield_kwh=float(row["daily_yield_kwh"])
        if row["daily_yield_kwh"] is not None
        else None,
        total_yield_kwh=float(row["total_yield_kwh"])
        if row["total_yield_kwh"] is not None
        else None,
        inverter_temp_c=float(row["inverter_temp_c"])
        if row["inverter_temp_c"] is not None
        else None,
    )


async def _fetch_aemo_prices(
    date_from: date, date_to: date, db: Database
) -> list[AemoPrice]:
    from datetime import timedelta

    ts_from = datetime(
        date_from.year, date_from.month, date_from.day, tzinfo=UTC
    ).isoformat()
    # Extend to_date by 1 day to capture the final block ending after midnight
    ts_to_dt = datetime(
        date_to.year, date_to.month, date_to.day, tzinfo=UTC
    ) + timedelta(days=1)
    ts_to = ts_to_dt.isoformat()

    rows = await db.fetch(
        """
        SELECT interval_start, interval_end, rrp_per_mwh, region
        FROM aemo_trading_prices
        WHERE interval_start >= ? AND interval_start < ?
        ORDER BY interval_start
        """,
        ts_from,
        ts_to,
    )
    return [_row_to_aemo_price(r) for r in rows]


def _row_to_aemo_price(row: Any) -> AemoPrice:
    def _parse_ts(raw: str) -> datetime:
        if raw.endswith("Z"):
            raw = raw[:-1] + "+00:00"
        dt = datetime.fromisoformat(raw)
        return dt if dt.tzinfo else dt.replace(tzinfo=UTC)

    return AemoPrice(
        interval_start=_parse_ts(str(row["interval_start"])),
        interval_end=_parse_ts(str(row["interval_end"])),
        rrp_per_mwh=float(row["rrp_per_mwh"]),
        region=str(row["region"]),
    )


# ---------------------------------------------------------------------------
# Public helpers (thin wrappers around private fetch functions)
# ---------------------------------------------------------------------------


async def fetch_plan(plan_id: int, db: Database) -> TariffPlan:
    """Fetch a TariffPlan from the database. Raises ValueError if not found."""
    return await _fetch_plan(plan_id, db)


async def fetch_rates(plan_id: int, db: Database) -> list[TariffRate]:
    """Fetch all TariffRate rows for *plan_id*."""
    return await _fetch_rates(plan_id, db)


async def fetch_aemo_prices(
    date_from: date, date_to: date, db: Database
) -> list[AemoPrice]:
    """Fetch AEMO trading prices for the given date range."""
    return await _fetch_aemo_prices(date_from, date_to, db)


def _parse_time(value: Any) -> time | None:
    if value is None:
        return None
    s = str(value)
    if not s:
        return None
    # Accept "HH:MM:SS" or "HH:MM"
    try:
        return time.fromisoformat(s)
    except ValueError:
        logger.warning("Could not parse time value: %r", s)
        return None
