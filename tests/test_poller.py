"""Tests for the inverter polling loop (collector/poller.py)."""

from datetime import UTC, datetime
from unittest.mock import AsyncMock, patch

import pytest

from candela.collector.inverter import InverterReading
from candela.collector.poller import poll_once
from candela.db import Database


def _make_reading(
    solar_w: int = 3500,
    grid_w: int = 1200,
    load_w: int = 4700,
) -> InverterReading:
    return InverterReading(
        ts=datetime(2026, 1, 15, 6, 0, 0, tzinfo=UTC),
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


async def test_poll_once_inserts_reading_on_success() -> None:
    db = await _make_db()
    failures = [0]
    reading = _make_reading()

    mock_client = AsyncMock()
    mock_client.read.return_value = reading

    await poll_once(mock_client, db, failures)

    row = await db.fetchrow("SELECT * FROM solar_readings WHERE ts = ?", "2026-01-15T06:00:00Z")
    assert row is not None
    assert row["solar_w"] == 3500
    assert row["grid_w"] == 1200
    assert row["load_w"] == 4700
    assert row["daily_yield_kwh"] == pytest.approx(18.5)
    assert failures[0] == 0

    await db.disconnect()


async def test_poll_once_upserts_on_duplicate_ts() -> None:
    db = await _make_db()
    failures = [0]

    mock_client = AsyncMock()
    mock_client.read.return_value = _make_reading(solar_w=3500)
    await poll_once(mock_client, db, failures)

    mock_client.read.return_value = _make_reading(solar_w=9999)
    await poll_once(mock_client, db, failures)

    rows = await db.fetch("SELECT * FROM solar_readings")
    assert len(rows) == 1
    assert rows[0]["solar_w"] == 9999

    await db.disconnect()


async def test_poll_once_skips_gracefully_on_none(caplog: pytest.LogCaptureFixture) -> None:
    db = await _make_db()
    failures = [0]

    mock_client = AsyncMock()
    mock_client.read.return_value = None

    import logging
    with caplog.at_level(logging.WARNING, logger="candela.collector.poller"):
        await poll_once(mock_client, db, failures)

    assert failures[0] == 1
    rows = await db.fetch("SELECT * FROM solar_readings")
    assert rows == []

    await db.disconnect()


async def test_poll_once_logs_error_after_three_consecutive_failures(
    caplog: pytest.LogCaptureFixture,
) -> None:
    db = await _make_db()
    failures = [0]

    mock_client = AsyncMock()
    mock_client.read.return_value = None

    import logging
    with caplog.at_level(logging.ERROR, logger="candela.collector.poller"):
        await poll_once(mock_client, db, failures)
        await poll_once(mock_client, db, failures)
        await poll_once(mock_client, db, failures)

    assert failures[0] == 3
    error_records = [r for r in caplog.records if r.levelno == logging.ERROR]
    assert len(error_records) >= 1
    assert "consecutive" in error_records[0].message.lower()

    await db.disconnect()


async def test_poll_once_resets_failure_count_on_success() -> None:
    db = await _make_db()
    failures = [2]  # already had 2 failures

    mock_client = AsyncMock()
    mock_client.read.return_value = _make_reading()

    await poll_once(mock_client, db, failures)

    assert failures[0] == 0

    await db.disconnect()
