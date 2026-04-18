"""Tests for GET /api/v1/readings and GET /api/v1/readings/latest."""

import pytest
from httpx import AsyncClient

from candela.db import Database


async def test_readings_latest_none_when_empty(client: AsyncClient) -> None:
    """Returns null (JSON) when no readings exist."""
    resp = await client.get("/api/v1/readings/latest")
    assert resp.status_code == 200
    assert resp.json() is None


async def test_readings_latest_returns_most_recent(
    client: AsyncClient, db: Database
) -> None:
    """Returns the reading with the highest ts."""
    for ts, solar, grid in [
        ("2026-01-01T10:00:00+00:00", 2000, 0),
        ("2026-01-01T10:05:00+00:00", 2100, -100),
    ]:
        await db.execute(
            "INSERT INTO solar_readings (ts, solar_w, grid_w, load_w) VALUES (?, ?, ?, ?)",
            ts,
            solar,
            grid,
            1500,
        )

    resp = await client.get("/api/v1/readings/latest")
    assert resp.status_code == 200
    data = resp.json()
    assert data is not None
    assert data["solar_w"] == 2100
    assert data["grid_w"] == -100


async def test_readings_range_returns_filtered_list(
    client: AsyncClient, db: Database
) -> None:
    """Readings filtered by from/to date range."""
    for i in range(5):
        await db.execute(
            "INSERT INTO solar_readings (ts, solar_w, grid_w, load_w) VALUES (?, ?, ?, ?)",
            f"2026-01-0{i + 1}T00:00:00+00:00",
            1000,
            100,
            900,
        )

    resp = await client.get(
        "/api/v1/readings",
        params={"from": "2026-01-02", "to": "2026-01-04"},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["count"] == 3
    assert len(data["readings"]) == 3


async def test_readings_range_empty_when_no_data(client: AsyncClient) -> None:
    """Returns empty list with count=0 when no readings in range."""
    resp = await client.get(
        "/api/v1/readings",
        params={"from": "2026-01-01", "to": "2026-01-31"},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["count"] == 0
    assert data["readings"] == []


async def test_readings_includes_optional_fields(
    client: AsyncClient, db: Database
) -> None:
    """Reading response includes optional sensor fields when present."""
    await db.execute(
        """
        INSERT INTO solar_readings
            (ts, solar_w, grid_w, load_w, daily_yield_kwh, inverter_temp_c)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        "2026-01-01T12:00:00+00:00",
        3000,
        -500,
        1500,
        12.5,
        42.3,
    )

    resp = await client.get("/api/v1/readings/latest")
    assert resp.status_code == 200
    data = resp.json()
    assert data["daily_yield_kwh"] == pytest.approx(12.5)
    assert data["inverter_temp_c"] == pytest.approx(42.3)
