"""Tests for GET /api/v1/summary/daily and /api/v1/summary/monthly."""

import pytest
from httpx import AsyncClient

from candela.db import Database

_INTERVAL_H = 5 / 60  # 5-minute reading → kWh factor


async def test_daily_summary_empty_range(client: AsyncClient) -> None:
    """Returns an empty days list when no readings exist."""
    resp = await client.get(
        "/api/v1/summary/daily",
        params={"from": "2026-01-01", "to": "2026-01-07"},
    )
    assert resp.status_code == 200
    assert resp.json()["days"] == []


async def test_daily_summary_aggregates_solar_and_export(
    client: AsyncClient, db: Database
) -> None:
    """Aggregates solar generation and export correctly for a single day."""
    # 12 readings: 2000W solar, -500W grid (exporting), 1500W load
    for i in range(12):
        await db.execute(
            "INSERT INTO solar_readings (ts, solar_w, grid_w, load_w) VALUES (?, ?, ?, ?)",
            f"2026-01-15T{i:02d}:00:00+00:00",
            2000,
            -500,
            1500,
        )

    resp = await client.get(
        "/api/v1/summary/daily",
        params={"from": "2026-01-15", "to": "2026-01-15"},
    )
    assert resp.status_code == 200
    days = resp.json()["days"]
    assert len(days) == 1
    day = days[0]
    assert day["date"] == "2026-01-15"
    assert day["solar_kwh"] == pytest.approx(12 * 2000 * _INTERVAL_H / 1000, rel=1e-2)
    assert day["export_kwh"] == pytest.approx(12 * 500 * _INTERVAL_H / 1000, rel=1e-2)
    assert day["import_kwh"] == pytest.approx(0.0, abs=0.01)


async def test_daily_summary_aggregates_import(
    client: AsyncClient, db: Database
) -> None:
    """Import kWh correctly summed for readings with positive grid_w."""
    for i in range(6):
        await db.execute(
            "INSERT INTO solar_readings (ts, solar_w, grid_w, load_w) VALUES (?, ?, ?, ?)",
            f"2026-01-20T{i:02d}:00:00+00:00",
            0,
            1000,
            1000,
        )

    resp = await client.get(
        "/api/v1/summary/daily",
        params={"from": "2026-01-20", "to": "2026-01-20"},
    )
    days = resp.json()["days"]
    assert days[0]["import_kwh"] == pytest.approx(
        6 * 1000 * _INTERVAL_H / 1000, rel=1e-2
    )


async def test_daily_summary_spans_multiple_days(
    client: AsyncClient, db: Database
) -> None:
    """Returns one entry per day when readings span multiple days."""
    for day in [1, 2, 3]:
        await db.execute(
            "INSERT INTO solar_readings (ts, solar_w, grid_w, load_w) VALUES (?, ?, ?, ?)",
            f"2026-02-0{day}T12:00:00+00:00",
            1000,
            0,
            800,
        )

    resp = await client.get(
        "/api/v1/summary/daily",
        params={"from": "2026-02-01", "to": "2026-02-03"},
    )
    days = resp.json()["days"]
    assert len(days) == 3
    assert [d["date"] for d in days] == ["2026-02-01", "2026-02-02", "2026-02-03"]


async def test_monthly_summary_no_readings(client: AsyncClient) -> None:
    """Monthly summary returns zeros when no data exists for the month."""
    resp = await client.get("/api/v1/summary/monthly", params={"month": "2026-01"})
    assert resp.status_code == 200
    data = resp.json()
    assert data["month"] == "2026-01"
    assert data["solar_kwh"] == pytest.approx(0.0)
    assert data["import_kwh"] == pytest.approx(0.0)
    assert data["export_kwh"] == pytest.approx(0.0)


async def test_monthly_summary_aggregates_correctly(
    client: AsyncClient, db: Database
) -> None:
    """Monthly totals are the sum of all readings in the calendar month."""
    # 3 readings: 2000W solar, -300W export
    for day in [1, 10, 20]:
        await db.execute(
            "INSERT INTO solar_readings (ts, solar_w, grid_w, load_w) VALUES (?, ?, ?, ?)",
            f"2026-03-{day:02d}T12:00:00+00:00",
            2000,
            -300,
            1500,
        )

    resp = await client.get("/api/v1/summary/monthly", params={"month": "2026-03"})
    assert resp.status_code == 200
    data = resp.json()
    assert data["solar_kwh"] == pytest.approx(3 * 2000 * _INTERVAL_H / 1000, rel=1e-2)
    assert data["export_kwh"] == pytest.approx(3 * 300 * _INTERVAL_H / 1000, rel=1e-2)
    assert data["import_kwh"] == pytest.approx(0.0, abs=0.01)


async def test_monthly_summary_invalid_month(client: AsyncClient) -> None:
    """Returns 422 for a malformed month string."""
    resp = await client.get("/api/v1/summary/monthly", params={"month": "notadate"})
    assert resp.status_code == 422
