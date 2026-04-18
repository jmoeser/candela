"""Tests for tariffs/engine.py and tariffs/seed.py.

All tests use an in-memory SQLite database with the Phase 2 schema applied
manually (no Alembic).  The engine's ``compute_bill`` is the primary surface
under test; seed tests verify that known plans are inserted idempotently.
"""

from datetime import date
from decimal import Decimal

import pytest

from candela.db import Database


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


async def _make_db() -> Database:
    """In-memory SQLite database with Phase 2 tables."""
    db = Database("sqlite+aiosqlite:///:memory:")
    await db.connect()

    await db.execute(
        """
        CREATE TABLE solar_readings (
            ts TEXT NOT NULL PRIMARY KEY,
            solar_w INTEGER NOT NULL,
            grid_w INTEGER NOT NULL,
            load_w INTEGER NOT NULL,
            daily_yield_kwh REAL,
            total_yield_kwh REAL,
            inverter_temp_c REAL
        )
        """
    )
    await db.execute(
        """
        CREATE TABLE tariff_plans (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            retailer TEXT,
            plan_type TEXT NOT NULL,
            supply_charge_daily_cents NUMERIC NOT NULL,
            feed_in_tariff_cents NUMERIC,
            valid_from TEXT NOT NULL,
            valid_to TEXT,
            notes TEXT
        )
        """
    )
    await db.execute(
        """
        CREATE TABLE tariff_rates (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            plan_id INTEGER NOT NULL REFERENCES tariff_plans(id),
            rate_type TEXT NOT NULL,
            cents_per_kwh NUMERIC,
            cents_per_kw NUMERIC,
            window_start TEXT,
            window_end TEXT,
            days_of_week TEXT,
            months TEXT,
            demand_window_start TEXT,
            demand_window_end TEXT
        )
        """
    )
    await db.execute(
        """
        CREATE TABLE aemo_trading_prices (
            interval_start TEXT NOT NULL,
            interval_end TEXT NOT NULL,
            rrp_per_mwh NUMERIC NOT NULL,
            region TEXT NOT NULL DEFAULT 'QLD1',
            PRIMARY KEY (interval_start, region)
        )
        """
    )
    return db


async def _insert_plan(
    db: Database,
    *,
    name: str = "Test Plan",
    plan_type: str = "single_rate",
    supply_cents: float = 100.0,
    fit_cents: float | None = None,
) -> int:
    await db.execute(
        """
        INSERT INTO tariff_plans
            (name, plan_type, supply_charge_daily_cents, feed_in_tariff_cents, valid_from)
        VALUES (?, ?, ?, ?, ?)
        """,
        name,
        plan_type,
        supply_cents,
        fit_cents,
        "2025-01-01",
    )
    plan_id = await db.fetchval("SELECT id FROM tariff_plans WHERE name = ?", name)
    assert plan_id is not None
    return int(plan_id)


async def _insert_rate(
    db: Database,
    plan_id: int,
    *,
    rate_type: str,
    cents_per_kwh: float | None = None,
    cents_per_kw: float | None = None,
    window_start: str | None = None,
    window_end: str | None = None,
    demand_window_start: str | None = None,
    demand_window_end: str | None = None,
) -> None:
    await db.execute(
        """
        INSERT INTO tariff_rates
            (plan_id, rate_type, cents_per_kwh, cents_per_kw,
             window_start, window_end,
             demand_window_start, demand_window_end)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        plan_id,
        rate_type,
        cents_per_kwh,
        cents_per_kw,
        window_start,
        window_end,
        demand_window_start,
        demand_window_end,
    )


async def _insert_reading(
    db: Database,
    ts: str,
    *,
    grid_w: int = 0,
    solar_w: int = 0,
    load_w: int = 0,
) -> None:
    await db.execute(
        """
        INSERT INTO solar_readings (ts, solar_w, grid_w, load_w)
        VALUES (?, ?, ?, ?)
        """,
        ts,
        solar_w,
        grid_w,
        load_w,
    )


# ---------------------------------------------------------------------------
# compute_bill — single rate
# ---------------------------------------------------------------------------


async def test_compute_bill_single_rate() -> None:
    """compute_bill returns a BillResult with correct import charge."""
    from candela.tariffs.engine import compute_bill

    db = await _make_db()
    plan_id = await _insert_plan(db, plan_type="single_rate", supply_cents=0.0)
    await _insert_rate(db, plan_id, rate_type="flat", cents_per_kwh=30.0)

    # 12 readings of 1000 W = 1 kWh → 30 c
    for i in range(12):
        ts = f"2026-01-01T00:{i * 5:02d}:00Z"
        await _insert_reading(db, ts, grid_w=1000)

    result = await compute_bill(
        plan_id,
        date(2026, 1, 1),
        date(2026, 1, 1),
        db,
    )

    assert float(result.import_charge_cents) == pytest.approx(30.0, rel=1e-3)
    assert float(result.demand_charge_cents) == 0.0


async def test_compute_bill_unknown_plan_raises() -> None:
    """compute_bill raises ValueError when the plan does not exist."""
    from candela.tariffs.engine import compute_bill

    db = await _make_db()
    with pytest.raises(ValueError, match="plan"):
        await compute_bill(9999, date(2026, 1, 1), date(2026, 1, 1), db)


async def test_compute_bill_no_readings_returns_supply_only() -> None:
    """compute_bill with no readings in range returns supply charge only."""
    from candela.tariffs.engine import compute_bill

    db = await _make_db()
    plan_id = await _insert_plan(db, plan_type="single_rate", supply_cents=100.0)
    await _insert_rate(db, plan_id, rate_type="flat", cents_per_kwh=30.0)

    result = await compute_bill(
        plan_id,
        date(2026, 1, 1),
        date(2026, 1, 1),
        db,
    )

    assert result.import_charge_cents == Decimal("0")
    assert result.export_credit_cents == Decimal("0")


async def test_compute_bill_tou() -> None:
    """compute_bill dispatches to TOU strategy; peak reading costs more."""
    from candela.tariffs.engine import compute_bill

    db = await _make_db()
    plan_id = await _insert_plan(db, plan_type="tou", supply_cents=0.0)
    await _insert_rate(
        db,
        plan_id,
        rate_type="peak",
        cents_per_kwh=40.0,
        window_start="16:00:00",
        window_end="21:00:00",
    )
    await _insert_rate(db, plan_id, rate_type="offpeak", cents_per_kwh=15.0)

    # One peak reading at 17:00 and one offpeak reading at 02:00
    await _insert_reading(db, "2026-01-01T17:00:00Z", grid_w=1000)
    await _insert_reading(db, "2026-01-01T02:00:00Z", grid_w=1000)

    result = await compute_bill(plan_id, date(2026, 1, 1), date(2026, 1, 1), db)

    # Peak period should appear with a higher rate
    assert "peak" in result.period_breakdown
    peak_cpkwh = (
        result.period_breakdown["peak"].cents / result.period_breakdown["peak"].kwh
    )
    offpeak_cpkwh = (
        result.period_breakdown["offpeak"].cents
        / result.period_breakdown["offpeak"].kwh
    )
    assert peak_cpkwh > offpeak_cpkwh


async def test_compute_bill_demand() -> None:
    """compute_bill dispatches to demand strategy; demand charge is non-zero."""
    from candela.tariffs.engine import compute_bill

    db = await _make_db()
    plan_id = await _insert_plan(db, plan_type="demand", supply_cents=0.0)
    await _insert_rate(
        db,
        plan_id,
        rate_type="peak",
        cents_per_kwh=40.0,
        window_start="16:00:00",
        window_end="21:00:00",
    )
    await _insert_rate(db, plan_id, rate_type="offpeak", cents_per_kwh=15.0)
    await _insert_rate(
        db,
        plan_id,
        rate_type="demand",
        cents_per_kw=50.0,
        demand_window_start="16:00:00",
        demand_window_end="21:00:00",
    )

    # Full 30-min block at 4000 W in peak window
    for i in range(6):
        ts = f"2026-01-01T16:{i * 5:02d}:00Z"
        await _insert_reading(db, ts, grid_w=4000)

    result = await compute_bill(plan_id, date(2026, 1, 1), date(2026, 1, 1), db)

    assert float(result.demand_charge_cents) == pytest.approx(200.0, rel=1e-3)


async def test_compute_bill_wholesale() -> None:
    """compute_bill dispatches to wholesale strategy with AEMO price lookup."""
    from candela.tariffs.engine import compute_bill

    db = await _make_db()
    plan_id = await _insert_plan(db, plan_type="wholesale", supply_cents=0.0)

    # Insert AEMO price for the 16:00 block
    await db.execute(
        """
        INSERT INTO aemo_trading_prices (interval_start, interval_end, rrp_per_mwh, region)
        VALUES (?, ?, ?, ?)
        """,
        "2026-01-01T16:00:00Z",
        "2026-01-01T16:30:00Z",
        100.0,
        "QLD1",
    )

    await _insert_reading(db, "2026-01-01T16:00:00Z", grid_w=1000)

    result = await compute_bill(plan_id, date(2026, 1, 1), date(2026, 1, 1), db)

    # kWh = 1000 * (5/60) / 1000 = 0.0833...
    # rate = 100/10 + 18 = 28 c/kWh
    kwh = 1000 * (5 / 60) / 1000
    expected = kwh * 28
    assert float(result.import_charge_cents) == pytest.approx(expected, rel=1e-3)


# ---------------------------------------------------------------------------
# seed
# ---------------------------------------------------------------------------


async def test_seed_creates_four_plans() -> None:
    """seed_plans inserts exactly four tariff plans."""
    from candela.tariffs.seed import seed_plans

    db = await _make_db()
    await seed_plans(db)

    count = await db.fetchval("SELECT COUNT(*) FROM tariff_plans")
    assert count == 4


async def test_seed_creates_rates_for_each_plan() -> None:
    """seed_plans inserts at least one rate for non-wholesale plans.

    The wholesale plan uses AEMO spot prices rather than rate rows, so it
    intentionally has zero tariff_rates entries.
    """
    from candela.tariffs.seed import seed_plans

    db = await _make_db()
    await seed_plans(db)

    plans = await db.fetch(
        "SELECT id, plan_type FROM tariff_plans WHERE plan_type != 'wholesale'"
    )
    for plan in plans:
        rate_count = await db.fetchval(
            "SELECT COUNT(*) FROM tariff_rates WHERE plan_id = ?", plan["id"]
        )
        assert rate_count >= 1, (
            f"Plan {plan['id']} (type={plan['plan_type']}) has no rates"
        )


async def test_seed_idempotent() -> None:
    """Running seed_plans twice does not create duplicate plans."""
    from candela.tariffs.seed import seed_plans

    db = await _make_db()
    await seed_plans(db)
    await seed_plans(db)

    count = await db.fetchval("SELECT COUNT(*) FROM tariff_plans")
    assert count == 4


async def test_seed_plan_types() -> None:
    """seed_plans creates exactly one plan of each expected type."""
    from candela.tariffs.seed import seed_plans

    db = await _make_db()
    await seed_plans(db)

    for plan_type in ("single_rate", "tou", "demand", "wholesale"):
        count = await db.fetchval(
            "SELECT COUNT(*) FROM tariff_plans WHERE plan_type = ?", plan_type
        )
        assert count == 1, f"Expected 1 plan of type '{plan_type}', got {count}"
