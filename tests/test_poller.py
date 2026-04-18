"""Tests for the iSolarCloud polling loop (collector/poller.py)."""

import logging
from datetime import UTC, datetime
from unittest.mock import AsyncMock

import pytest

from candela.collector.isolarcloud import InverterReading
from candela.collector.poller import poll_once
from candela.db import Database


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_reading(
    solar_w: int = 3500,
    grid_w: int = -1200,
    load_w: int = 2300,
    ts: datetime | None = None,
) -> InverterReading:
    return InverterReading(
        ts=ts or datetime(2026, 1, 15, 6, 0, 0, tzinfo=UTC),
        solar_w=solar_w,
        grid_w=grid_w,
        load_w=load_w,
        daily_yield_kwh=18.5,
        total_yield_kwh=1234.6,
        inverter_temp_c=42.3,
    )


async def _make_db() -> Database:
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
    return db


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


async def test_poll_once_inserts_reading() -> None:
    db = await _make_db()
    client = AsyncMock()
    client.fetch_current_reading.return_value = _make_reading()

    await poll_once(client, db)

    row = await db.fetchrow(
        "SELECT * FROM solar_readings WHERE ts = ?", "2026-01-15T06:00:00Z"
    )
    assert row is not None
    assert row["solar_w"] == 3500
    assert row["grid_w"] == -1200
    assert row["load_w"] == 2300
    assert row["daily_yield_kwh"] == pytest.approx(18.5)
    await db.disconnect()


async def test_poll_once_upserts_on_duplicate_ts() -> None:
    db = await _make_db()
    client = AsyncMock()

    client.fetch_current_reading.return_value = _make_reading(solar_w=3500)
    await poll_once(client, db)

    client.fetch_current_reading.return_value = _make_reading(solar_w=9999)
    await poll_once(client, db)

    rows = await db.fetch("SELECT * FROM solar_readings")
    assert len(rows) == 1
    assert rows[0]["solar_w"] == 9999
    await db.disconnect()


async def test_poll_once_handles_api_error_gracefully(
    caplog: pytest.LogCaptureFixture,
) -> None:
    db = await _make_db()
    client = AsyncMock()
    client.fetch_current_reading.side_effect = Exception("network error")

    with caplog.at_level(logging.ERROR, logger="candela.collector.poller"):
        await poll_once(client, db)  # must not raise

    error_records = [r for r in caplog.records if r.levelno == logging.ERROR]
    assert any("network error" in r.message or r.exc_info for r in error_records)

    rows = await db.fetch("SELECT * FROM solar_readings")
    assert rows == []
    await db.disconnect()
