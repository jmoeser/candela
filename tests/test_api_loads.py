"""Tests for /api/v1/loads endpoints (list, create, patch)."""

import pytest
from httpx import AsyncClient

from candela.db import Database


async def _insert_event(
    db: Database, day: int = 15, load_name: str = "ev_charging"
) -> None:
    await db.execute(
        """INSERT INTO load_events
               (started_at, ended_at, load_name, avg_watts, kwh, confidence, source)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        f"2026-01-{day:02d}T14:00:00+00:00",
        f"2026-01-{day:02d}T16:00:00+00:00",
        load_name,
        7200,
        14.4,
        0.75,
        "inferred",
    )


async def test_list_loads_empty(client: AsyncClient) -> None:
    """Returns empty list with count=0 when no events exist."""
    resp = await client.get("/api/v1/loads")
    assert resp.status_code == 200
    data = resp.json()
    assert data["events"] == []
    assert data["count"] == 0


async def test_create_manual_load_event(client: AsyncClient) -> None:
    """POST /api/v1/loads creates a manual event with source='manual'."""
    payload = {
        "started_at": "2026-01-15T14:00:00+00:00",
        "ended_at": "2026-01-15T16:30:00+00:00",
        "load_name": "ev_charging",
        "avg_watts": 7200,
        "kwh": 18.0,
    }
    resp = await client.post("/api/v1/loads", json=payload)
    assert resp.status_code == 201
    data = resp.json()
    assert data["id"] > 0
    assert data["load_name"] == "ev_charging"
    assert data["source"] == "manual"
    assert data["kwh"] == pytest.approx(18.0)


async def test_create_minimal_load_event(client: AsyncClient) -> None:
    """POST with only required fields succeeds."""
    payload = {
        "started_at": "2026-01-15T14:00:00+00:00",
        "load_name": "hot_water_heatpump",
    }
    resp = await client.post("/api/v1/loads", json=payload)
    assert resp.status_code == 201
    data = resp.json()
    assert data["load_name"] == "hot_water_heatpump"
    assert data["ended_at"] is None
    assert data["source"] == "manual"


async def test_list_loads_returns_created_event(client: AsyncClient) -> None:
    """Created event appears in subsequent GET /api/v1/loads."""
    payload = {
        "started_at": "2026-01-15T14:00:00+00:00",
        "load_name": "ev_charging",
    }
    await client.post("/api/v1/loads", json=payload)
    resp = await client.get("/api/v1/loads")
    assert resp.status_code == 200
    data = resp.json()
    assert data["count"] == 1
    assert data["events"][0]["load_name"] == "ev_charging"


async def test_patch_load_event_confirm(client: AsyncClient, db: Database) -> None:
    """PATCH confirms an inferred event by setting confidence=1.0 and source='manual'."""
    await _insert_event(db)
    event_id = await db.fetchval("SELECT id FROM load_events LIMIT 1")

    resp = await client.patch(
        f"/api/v1/loads/{event_id}",
        json={"confidence": 1.0, "source": "manual"},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["confidence"] == pytest.approx(1.0)
    assert data["source"] == "manual"


async def test_patch_load_event_reject(client: AsyncClient, db: Database) -> None:
    """PATCH rejects an event by setting confidence=0.0."""
    await _insert_event(db, load_name="hot_water_heatpump")
    event_id = await db.fetchval("SELECT id FROM load_events LIMIT 1")

    resp = await client.patch(
        f"/api/v1/loads/{event_id}",
        json={"confidence": 0.0, "source": "manual"},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["confidence"] == pytest.approx(0.0)
    assert data["source"] == "manual"


async def test_patch_load_event_not_found(client: AsyncClient) -> None:
    """PATCH for a non-existent event returns 404."""
    resp = await client.patch(
        "/api/v1/loads/9999",
        json={"confidence": 1.0, "source": "manual"},
    )
    assert resp.status_code == 404


async def test_loads_filtered_by_date_range(client: AsyncClient, db: Database) -> None:
    """Events outside the from/to range are excluded."""
    for day in [5, 15, 25]:
        await _insert_event(db, day=day)

    resp = await client.get(
        "/api/v1/loads",
        params={"from": "2026-01-13T00:00:00+00:00", "to": "2026-01-17T23:59:59+00:00"},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["count"] == 1
    assert data["events"][0]["started_at"].startswith("2026-01-15")


async def test_wholesale_prices_empty(client: AsyncClient) -> None:
    """GET /api/v1/wholesale/prices returns empty list when no AEMO data."""
    resp = await client.get(
        "/api/v1/wholesale/prices",
        params={"from": "2026-01-01", "to": "2026-01-31"},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["prices"] == []


async def test_wholesale_prices_returns_filtered_range(
    client: AsyncClient, db: Database
) -> None:
    """AEMO prices are filtered to the requested date range."""
    for day in [1, 15, 31]:
        await db.execute(
            """INSERT INTO aemo_trading_prices
                   (interval_start, interval_end, rrp_per_mwh, region)
               VALUES (?, ?, ?, ?)""",
            f"2026-01-{day:02d}T00:00:00+00:00",
            f"2026-01-{day:02d}T00:30:00+00:00",
            100.0,
            "QLD1",
        )

    resp = await client.get(
        "/api/v1/wholesale/prices",
        params={"from": "2026-01-10", "to": "2026-01-20"},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["count"] == 1
    assert data["prices"][0]["rrp_per_mwh"] == pytest.approx(100.0)
