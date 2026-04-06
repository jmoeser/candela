"""Tests for Jinja2-rendered page routes: /, /history, /compare, /plans, /loads."""

from httpx import AsyncClient

from candela.db import Database


async def test_dashboard_returns_200(client: AsyncClient) -> None:
    resp = await client.get("/")
    assert resp.status_code == 200
    assert b"<html" in resp.content.lower()


async def test_history_returns_200(client: AsyncClient) -> None:
    resp = await client.get("/history")
    assert resp.status_code == 200
    assert b"<html" in resp.content.lower()


async def test_compare_page_returns_200(client: AsyncClient) -> None:
    resp = await client.get("/compare")
    assert resp.status_code == 200
    assert b"<html" in resp.content.lower()


async def test_plans_page_returns_200(client: AsyncClient) -> None:
    resp = await client.get("/plans")
    assert resp.status_code == 200
    assert b"<html" in resp.content.lower()


async def test_loads_page_returns_200(client: AsyncClient) -> None:
    resp = await client.get("/loads")
    assert resp.status_code == 200
    assert b"<html" in resp.content.lower()


async def test_status_partial_returns_html(client: AsyncClient) -> None:
    resp = await client.get("/partials/status")
    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith("text/html")


async def test_status_partial_with_reading(client: AsyncClient, db: Database) -> None:
    """Status partial shows solar/grid/load values when data is present."""
    await db.execute(
        "INSERT INTO solar_readings (ts, solar_w, grid_w, load_w) VALUES (?, ?, ?, ?)",
        "2026-01-01T12:00:00+00:00",
        3000,
        -500,
        2000,
    )
    resp = await client.get("/partials/status")
    assert resp.status_code == 200
    assert b"3000" in resp.content or b"3,000" in resp.content


async def test_today_summary_partial_returns_html(client: AsyncClient) -> None:
    resp = await client.get("/partials/today-summary")
    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith("text/html")


async def test_wholesale_price_partial_returns_html(client: AsyncClient) -> None:
    resp = await client.get("/partials/wholesale-price")
    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith("text/html")


async def test_daily_chart_partial_returns_html(client: AsyncClient) -> None:
    resp = await client.get(
        "/partials/daily-chart",
        params={"from": "2026-01-01", "to": "2026-01-07"},
    )
    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith("text/html")


async def test_compare_results_partial_empty(client: AsyncClient) -> None:
    """Partial returns 200 with empty results when no plans selected."""
    resp = await client.get(
        "/partials/compare-results",
        params={"from": "2026-01-01", "to": "2026-01-31"},
    )
    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith("text/html")


async def test_compare_results_partial_with_plan(
    client: AsyncClient, db: Database
) -> None:
    """Partial renders comparison results when a valid plan_ids is provided."""
    await db.execute(
        """INSERT INTO tariff_plans
               (name, plan_type, supply_charge_daily_cents, valid_from)
           VALUES (?, ?, ?, ?)""",
        "Test Plan",
        "single_rate",
        0.0,
        "2026-01-01",
    )
    plan_id = await db.fetchval("SELECT id FROM tariff_plans WHERE name = 'Test Plan'")
    await db.execute(
        "INSERT INTO tariff_rates (plan_id, rate_type, cents_per_kwh) VALUES (?, ?, ?)",
        plan_id,
        "flat",
        30.0,
    )

    resp = await client.get(
        "/partials/compare-results",
        params={"plan_ids": str(plan_id), "from": "2026-01-01", "to": "2026-01-31"},
    )
    assert resp.status_code == 200
    assert b"Test Plan" in resp.content
