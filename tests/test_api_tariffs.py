"""Tests for /api/v1/plans and /api/v1/compare endpoints."""

import pytest
from httpx import AsyncClient

from candela.db import Database

_PLAN_PAYLOAD = {
    "name": "Test Single Rate",
    "plan_type": "single_rate",
    "supply_charge_daily_cents": 120.5,
    "valid_from": "2026-01-01",
}


async def test_list_plans_empty(client: AsyncClient) -> None:
    """GET /api/v1/plans returns [] when no plans exist."""
    resp = await client.get("/api/v1/plans")
    assert resp.status_code == 200
    assert resp.json() == []


async def test_create_plan(client: AsyncClient) -> None:
    """POST /api/v1/plans creates a plan and returns 201 with the plan data."""
    resp = await client.post("/api/v1/plans", json=_PLAN_PAYLOAD)
    assert resp.status_code == 201
    data = resp.json()
    assert data["id"] > 0
    assert data["name"] == "Test Single Rate"
    assert data["plan_type"] == "single_rate"
    assert data["supply_charge_daily_cents"] == pytest.approx(120.5)
    assert data["valid_from"] == "2026-01-01"


async def test_list_plans_returns_created_plan(client: AsyncClient) -> None:
    """Created plan appears in subsequent GET /api/v1/plans."""
    await client.post("/api/v1/plans", json=_PLAN_PAYLOAD)
    resp = await client.get("/api/v1/plans")
    assert resp.status_code == 200
    plans = resp.json()
    assert len(plans) == 1
    assert plans[0]["name"] == "Test Single Rate"


async def test_create_plan_with_optional_fields(client: AsyncClient) -> None:
    """Optional fields (retailer, FiT, notes) are stored and returned."""
    payload = {
        "name": "Engie Full",
        "retailer": "Engie",
        "plan_type": "single_rate",
        "supply_charge_daily_cents": 115.0,
        "feed_in_tariff_cents": 5.0,
        "valid_from": "2026-01-01",
        "notes": "current plan",
    }
    resp = await client.post("/api/v1/plans", json=payload)
    assert resp.status_code == 201
    data = resp.json()
    assert data["retailer"] == "Engie"
    assert data["feed_in_tariff_cents"] == pytest.approx(5.0)
    assert data["notes"] == "current plan"


async def test_update_plan(client: AsyncClient) -> None:
    """PUT /api/v1/plans/{id} updates the plan and returns updated data."""
    create_resp = await client.post("/api/v1/plans", json=_PLAN_PAYLOAD)
    plan_id = create_resp.json()["id"]

    updated = {
        **_PLAN_PAYLOAD,
        "name": "Updated Name",
        "supply_charge_daily_cents": 130.0,
    }
    resp = await client.put(f"/api/v1/plans/{plan_id}", json=updated)
    assert resp.status_code == 200
    data = resp.json()
    assert data["name"] == "Updated Name"
    assert data["supply_charge_daily_cents"] == pytest.approx(130.0)


async def test_update_plan_not_found(client: AsyncClient) -> None:
    """PUT for a non-existent plan returns 404."""
    resp = await client.put("/api/v1/plans/9999", json=_PLAN_PAYLOAD)
    assert resp.status_code == 404


async def test_compare_empty_when_no_plan_ids(client: AsyncClient) -> None:
    """GET /api/v1/compare with no plan_ids returns []."""
    resp = await client.get(
        "/api/v1/compare",
        params={"from": "2026-01-01", "to": "2026-01-31"},
    )
    assert resp.status_code == 200
    assert resp.json() == []


async def test_compare_single_rate_plan(client: AsyncClient, db: Database) -> None:
    """Compare returns a BillResult for a single-rate plan."""
    await db.execute(
        """INSERT INTO tariff_plans
               (name, plan_type, supply_charge_daily_cents, feed_in_tariff_cents, valid_from)
           VALUES (?, ?, ?, ?, ?)""",
        "Flat Plan",
        "single_rate",
        0.0,
        None,
        "2026-01-01",
    )
    plan_id = await db.fetchval("SELECT id FROM tariff_plans WHERE name = 'Flat Plan'")
    await db.execute(
        "INSERT INTO tariff_rates (plan_id, rate_type, cents_per_kwh) VALUES (?, ?, ?)",
        plan_id,
        "flat",
        30.0,
    )
    # 1 reading: 1000W for 5 min → 1/12 kWh → 2.5c
    await db.execute(
        "INSERT INTO solar_readings (ts, solar_w, grid_w, load_w) VALUES (?, ?, ?, ?)",
        "2026-01-15T00:00:00+00:00",
        0,
        1000,
        1000,
    )

    resp = await client.get(
        "/api/v1/compare",
        params={"plan_ids": str(plan_id), "from": "2026-01-15", "to": "2026-01-15"},
    )
    assert resp.status_code == 200
    results = resp.json()
    assert len(results) == 1
    assert results[0]["plan_id"] == plan_id
    expected_cents = 1000 * (5 / 60) / 1000 * 30.0
    assert results[0]["import_charge_cents"] == pytest.approx(expected_cents, rel=1e-2)


async def test_compare_skips_unknown_plan_id(client: AsyncClient) -> None:
    """Unknown plan IDs are silently skipped in compare results."""
    resp = await client.get(
        "/api/v1/compare",
        params={"plan_ids": "9999", "from": "2026-01-01", "to": "2026-01-31"},
    )
    assert resp.status_code == 200
    assert resp.json() == []


async def test_compare_multiple_plans(client: AsyncClient, db: Database) -> None:
    """Compare returns one result per valid plan ID."""
    for name, rate in [("Plan A", 25.0), ("Plan B", 35.0)]:
        await db.execute(
            """INSERT INTO tariff_plans
                   (name, plan_type, supply_charge_daily_cents, valid_from)
               VALUES (?, ?, ?, ?)""",
            name,
            "single_rate",
            0.0,
            "2026-01-01",
        )
        pid = await db.fetchval("SELECT id FROM tariff_plans WHERE name = ?", name)
        await db.execute(
            "INSERT INTO tariff_rates (plan_id, rate_type, cents_per_kwh) VALUES (?, ?, ?)",
            pid,
            "flat",
            rate,
        )

    ids = ",".join(
        str(r["id"]) for r in await db.fetch("SELECT id FROM tariff_plans ORDER BY id")
    )
    resp = await client.get(
        "/api/v1/compare",
        params={"plan_ids": ids, "from": "2026-01-01", "to": "2026-01-01"},
    )
    assert resp.status_code == 200
    assert len(resp.json()) == 2


# ---------------------------------------------------------------------------
# Current plan endpoints
# ---------------------------------------------------------------------------


async def test_get_current_plan_none(client: AsyncClient) -> None:
    """GET /api/v1/plans/current returns null when no plan is set."""
    resp = await client.get("/api/v1/plans/current")
    assert resp.status_code == 200
    assert resp.json() is None


async def test_set_current_plan(client: AsyncClient) -> None:
    """POST /api/v1/plans/{id}/set-current sets the plan and returns it."""
    create_resp = await client.post("/api/v1/plans", json=_PLAN_PAYLOAD)
    plan_id = create_resp.json()["id"]

    resp = await client.post(
        f"/api/v1/plans/{plan_id}/set-current",
        json={"active_from": "2026-04-01"},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["plan_id"] == plan_id
    assert data["active_from"] == "2026-04-01"
    assert data["active_to"] is None


async def test_get_current_plan_after_set(client: AsyncClient) -> None:
    """GET /api/v1/plans/current returns the plan after it has been set."""
    create_resp = await client.post("/api/v1/plans", json=_PLAN_PAYLOAD)
    plan_id = create_resp.json()["id"]

    await client.post(
        f"/api/v1/plans/{plan_id}/set-current",
        json={"active_from": "2026-04-01"},
    )
    resp = await client.get("/api/v1/plans/current")
    assert resp.status_code == 200
    data = resp.json()
    assert data["plan_id"] == plan_id
    assert data["plan_name"] == "Test Single Rate"


async def test_set_current_plan_closes_previous(client: AsyncClient) -> None:
    """Setting a new current plan closes the previous open period."""
    payload_a = {**_PLAN_PAYLOAD, "name": "Plan A"}
    payload_b = {**_PLAN_PAYLOAD, "name": "Plan B"}

    id_a = (await client.post("/api/v1/plans", json=payload_a)).json()["id"]
    id_b = (await client.post("/api/v1/plans", json=payload_b)).json()["id"]

    await client.post(
        f"/api/v1/plans/{id_a}/set-current", json={"active_from": "2026-01-01"}
    )
    await client.post(
        f"/api/v1/plans/{id_b}/set-current", json={"active_from": "2026-04-01"}
    )

    resp = await client.get("/api/v1/plans/current")
    data = resp.json()
    assert data["plan_id"] == id_b
    assert data["active_from"] == "2026-04-01"


async def test_set_current_plan_not_found(client: AsyncClient) -> None:
    """POST set-current for a non-existent plan returns 404."""
    resp = await client.post(
        "/api/v1/plans/9999/set-current",
        json={"active_from": "2026-04-01"},
    )
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# Delete plan
# ---------------------------------------------------------------------------


async def test_delete_plan(client: AsyncClient) -> None:
    """DELETE /api/v1/plans/{id} removes a non-current, non-wholesale plan."""
    create_resp = await client.post("/api/v1/plans", json=_PLAN_PAYLOAD)
    plan_id = create_resp.json()["id"]

    resp = await client.delete(f"/api/v1/plans/{plan_id}")
    assert resp.status_code == 204

    list_resp = await client.get("/api/v1/plans")
    assert list_resp.json() == []


async def test_delete_plan_not_found(client: AsyncClient) -> None:
    """DELETE for a non-existent plan returns 404."""
    resp = await client.delete("/api/v1/plans/9999")
    assert resp.status_code == 404


async def test_delete_current_plan_rejected(client: AsyncClient) -> None:
    """DELETE the currently active plan returns 409."""
    create_resp = await client.post("/api/v1/plans", json=_PLAN_PAYLOAD)
    plan_id = create_resp.json()["id"]
    await client.post(
        f"/api/v1/plans/{plan_id}/set-current", json={"active_from": "2026-04-01"}
    )

    resp = await client.delete(f"/api/v1/plans/{plan_id}")
    assert resp.status_code == 409


async def test_delete_wholesale_plan_rejected(client: AsyncClient) -> None:
    """DELETE a wholesale plan returns 409."""
    payload = {**_PLAN_PAYLOAD, "plan_type": "wholesale"}
    create_resp = await client.post("/api/v1/plans", json=payload)
    plan_id = create_resp.json()["id"]

    resp = await client.delete(f"/api/v1/plans/{plan_id}")
    assert resp.status_code == 409


async def test_delete_removes_rates(client: AsyncClient, db: Database) -> None:
    """Deleting a plan also removes its associated rates."""
    create_resp = await client.post("/api/v1/plans", json=_PLAN_PAYLOAD)
    plan_id = create_resp.json()["id"]
    await db.execute(
        "INSERT INTO tariff_rates (plan_id, rate_type, cents_per_kwh) VALUES (?, ?, ?)",
        plan_id,
        "flat",
        30.0,
    )

    await client.delete(f"/api/v1/plans/{plan_id}")

    rate_count = await db.fetchval(
        "SELECT COUNT(*) FROM tariff_rates WHERE plan_id = ?", plan_id
    )
    assert rate_count == 0


# ---------------------------------------------------------------------------
# Create plan with rates
# ---------------------------------------------------------------------------


async def test_create_single_rate_plan_with_rate(
    client: AsyncClient, db: Database
) -> None:
    """POST /api/v1/plans with a flat rate inserts the rate into tariff_rates."""
    payload = {
        **_PLAN_PAYLOAD,
        "rates": [{"rate_type": "flat", "cents_per_kwh": 28.5}],
    }
    resp = await client.post("/api/v1/plans", json=payload)
    assert resp.status_code == 201
    plan_id = resp.json()["id"]

    rate = await db.fetchrow("SELECT * FROM tariff_rates WHERE plan_id = ?", plan_id)
    assert rate is not None
    assert float(rate["cents_per_kwh"]) == pytest.approx(28.5)
    assert rate["rate_type"] == "flat"


async def test_create_tou_plan_with_rates(client: AsyncClient, db: Database) -> None:
    """POST /api/v1/plans with TOU rates inserts all rate rows with time windows."""
    payload = {
        **_PLAN_PAYLOAD,
        "plan_type": "tou",
        "rates": [
            {
                "rate_type": "peak",
                "cents_per_kwh": 45.0,
                "window_start": "07:00",
                "window_end": "23:00",
            },
            {
                "rate_type": "offpeak",
                "cents_per_kwh": 18.0,
                "window_start": "23:00",
                "window_end": "07:00",
            },
        ],
    }
    resp = await client.post("/api/v1/plans", json=payload)
    assert resp.status_code == 201
    plan_id = resp.json()["id"]

    rates = await db.fetch(
        "SELECT rate_type, cents_per_kwh, window_start, window_end FROM tariff_rates WHERE plan_id = ? ORDER BY id",
        plan_id,
    )
    assert len(rates) == 2
    assert rates[0]["rate_type"] == "peak"
    assert float(rates[0]["cents_per_kwh"]) == pytest.approx(45.0)
    assert rates[0]["window_start"] == "07:00"
    assert rates[1]["rate_type"] == "offpeak"


async def test_create_demand_plan_with_rates(client: AsyncClient, db: Database) -> None:
    """POST /api/v1/plans with demand rates inserts TOU energy rows plus a demand row."""
    payload = {
        **_PLAN_PAYLOAD,
        "plan_type": "demand",
        "rates": [
            {
                "rate_type": "peak",
                "cents_per_kwh": 45.0,
                "window_start": "07:00",
                "window_end": "23:00",
            },
            {
                "rate_type": "offpeak",
                "cents_per_kwh": 18.0,
                "window_start": "23:00",
                "window_end": "07:00",
            },
            {
                "rate_type": "demand",
                "cents_per_kw": 53.0,
                "demand_window_start": "07:00",
                "demand_window_end": "23:00",
            },
        ],
    }
    resp = await client.post("/api/v1/plans", json=payload)
    assert resp.status_code == 201
    plan_id = resp.json()["id"]

    rates = await db.fetch(
        "SELECT rate_type, cents_per_kwh, cents_per_kw, demand_window_start, demand_window_end "
        "FROM tariff_rates WHERE plan_id = ? ORDER BY id",
        plan_id,
    )
    assert len(rates) == 3
    demand_row = next(r for r in rates if r["rate_type"] == "demand")
    assert float(demand_row["cents_per_kw"]) == pytest.approx(53.0)
    assert demand_row["demand_window_start"] == "07:00"
    assert demand_row["demand_window_end"] == "23:00"
    assert demand_row["cents_per_kwh"] is None
