"""Shared fixtures for Candela API and web tests.

All tests that need a database or HTTP client should use these fixtures.
The ``db`` fixture provides an in-memory SQLite database with all tables;
the ``client`` fixture wraps the FastAPI app with ``get_db`` overridden.
"""

import os

# Must be set before any app code is imported so pydantic-settings can read them.
os.environ.setdefault("DATABASE_URL", "sqlite+aiosqlite:///:memory:")
os.environ.setdefault("SECRET_KEY", "test-secret-key-not-for-production")
os.environ.setdefault("AUTH_USERNAME", "testuser")
os.environ.setdefault("AUTH_PASSWORD", "testpass")
os.environ.setdefault("ISOLARCLOUD_APP_KEY", "test-key")
os.environ.setdefault("ISOLARCLOUD_USERNAME", "test@example.com")
os.environ.setdefault("ISOLARCLOUD_PASSWORD", "test-password")

import pytest
from httpx import ASGITransport, AsyncClient

from candela.auth import require_api_key, require_auth
from candela.db import Database
from candela.main import app, get_db

# ---------------------------------------------------------------------------
# Full schema for all three phases
# ---------------------------------------------------------------------------

_CREATE_TABLES: list[str] = [
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
    """,
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
    """,
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
    """,
    """
    CREATE TABLE aemo_trading_prices (
        interval_start TEXT NOT NULL,
        interval_end TEXT NOT NULL,
        rrp_per_mwh NUMERIC NOT NULL,
        region TEXT NOT NULL DEFAULT 'QLD1',
        PRIMARY KEY (interval_start, region)
    )
    """,
    """
    CREATE TABLE load_events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        started_at TEXT NOT NULL,
        ended_at TEXT,
        load_name TEXT NOT NULL,
        avg_watts INTEGER,
        kwh NUMERIC,
        confidence NUMERIC,
        source TEXT NOT NULL
    )
    """,
]


async def make_test_db() -> Database:
    """Create an in-memory SQLite database with all Candela tables."""
    db = Database("sqlite+aiosqlite:///:memory:")
    await db.connect()
    for sql in _CREATE_TABLES:
        await db.execute(sql)
    return db


@pytest.fixture
async def db() -> Database:  # type: ignore[misc]
    """In-memory SQLite database with all tables, disconnected after the test."""
    test_db = await make_test_db()
    yield test_db
    await test_db.disconnect()


@pytest.fixture
async def client(db: Database):  # type: ignore[misc]
    """AsyncClient for the FastAPI app with ``get_db`` overridden to the test DB.

    Auth dependencies are bypassed so tests don't need to log in.
    """
    app.dependency_overrides[get_db] = lambda: db
    app.dependency_overrides[require_auth] = lambda: None
    app.dependency_overrides[require_api_key] = lambda: None
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as ac:
        yield ac
    app.dependency_overrides.clear()
