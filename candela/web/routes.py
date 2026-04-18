"""Jinja2-rendered page routes and HTMX partial routes.

Full pages:  /  /history  /compare  /plans  /loads
Partials:    /partials/status
             /partials/today-summary
             /partials/wholesale-price
             /partials/daily-chart
             /partials/compare-results
"""

import logging
import secrets
from collections import defaultdict
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Annotated, cast

from datetime import date as date_type

from fastapi import APIRouter, Depends, Form, Query
from fastapi.requests import Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from candela.auth import require_auth
from candela.config import get_settings
from candela.db import Database
from candela.main import get_db
from candela.tariffs.strategies.base import TariffStrategy

logger = logging.getLogger(__name__)

router = APIRouter()
_protected = APIRouter()
templates = Jinja2Templates(directory=Path(__file__).parent / "templates")


def _fmt_ts(raw: str | datetime | None) -> str:
    """Format a DB timestamp string (or datetime) as e.g. '13 Apr, 2:30 pm'."""
    if raw is None:
        return "ongoing"
    if isinstance(raw, str):
        raw = _parse_ts(raw)
    time_part = raw.strftime("%I:%M %p").lstrip("0").lower()
    return f"{raw.day} {raw.strftime('%b')}, {time_part}"


templates.env.filters["fmt_ts"] = _fmt_ts

_INTERVAL_H = 5 / 60


# ---------------------------------------------------------------------------
# Auth routes (public)
# ---------------------------------------------------------------------------


@router.get("/login", response_class=HTMLResponse)
async def login_page(request: Request) -> HTMLResponse:
    if request.session.get("authenticated"):
        return RedirectResponse("/", status_code=303)  # type: ignore[return-value]
    return templates.TemplateResponse(request=request, name="login.html", context={})


@router.post("/login", response_model=None)
async def login_submit(
    request: Request,
    username: Annotated[str, Form()],
    password: Annotated[str, Form()],
) -> RedirectResponse | HTMLResponse:
    s = get_settings()
    username_ok = secrets.compare_digest(username, s.auth_username)
    password_ok = secrets.compare_digest(password, s.auth_password)
    if username_ok and password_ok:
        request.session["authenticated"] = True
        return RedirectResponse("/", status_code=303)
    return templates.TemplateResponse(
        request=request,
        name="login.html",
        context={"error": "Invalid username or password"},
        status_code=200,
    )


@router.post("/logout")
async def logout(request: Request) -> RedirectResponse:
    request.session.clear()
    return RedirectResponse("/login", status_code=303)


def _parse_ts(raw: str) -> datetime:
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    dt = datetime.fromisoformat(raw)
    return dt if dt.tzinfo else dt.replace(tzinfo=UTC)


# ---------------------------------------------------------------------------
# Full pages
# ---------------------------------------------------------------------------


@_protected.get("/", response_class=HTMLResponse)
async def dashboard(
    request: Request, db: Annotated[Database, Depends(get_db)]
) -> HTMLResponse:
    """Dashboard: current status, today's summary, wholesale price strip."""
    plans = await db.fetch("SELECT id, name FROM tariff_plans ORDER BY id LIMIT 10")
    return templates.TemplateResponse(
        request=request, name="dashboard.html", context={"plans": list(plans)}
    )


@_protected.get("/history", response_class=HTMLResponse)
async def history(
    request: Request, db: Annotated[Database, Depends(get_db)]
) -> HTMLResponse:
    """History page: date-range chart + load event timeline."""
    plans = await db.fetch("SELECT id, name FROM tariff_plans ORDER BY id")
    return templates.TemplateResponse(
        request=request, name="history.html", context={"plans": list(plans)}
    )


@_protected.get("/compare", response_class=HTMLResponse)
async def compare(
    request: Request, db: Annotated[Database, Depends(get_db)]
) -> HTMLResponse:
    """Plan comparison page."""
    plans = await db.fetch("SELECT id, name, plan_type FROM tariff_plans ORDER BY id")
    return templates.TemplateResponse(
        request=request, name="compare.html", context={"plans": list(plans)}
    )


@_protected.post("/plans/{plan_id}/set-current")
async def set_plan_current(
    plan_id: int,
    request: Request,
    db: Annotated[Database, Depends(get_db)],
    active_from: Annotated[date_type, Form()],
) -> RedirectResponse:
    """Handle the 'Set as current' form on the plans page."""
    prev_day = (active_from - timedelta(days=1)).isoformat()
    await db.execute(
        "UPDATE current_plan_periods SET active_to = ? WHERE active_to IS NULL",
        prev_day,
    )
    await db.execute(
        "INSERT INTO current_plan_periods (plan_id, active_from, active_to) VALUES (?, ?, NULL)",
        plan_id,
        active_from.isoformat(),
    )
    return RedirectResponse("/plans", status_code=303)


@_protected.get("/plans", response_class=HTMLResponse)
async def plans_page(
    request: Request, db: Annotated[Database, Depends(get_db)]
) -> HTMLResponse:
    """Plan management: list and create/edit forms."""
    plans = await db.fetch("SELECT * FROM tariff_plans ORDER BY id")
    current_period = await db.fetchrow(
        """
        SELECT cpp.plan_id, tp.name AS plan_name, cpp.active_from
        FROM current_plan_periods cpp
        JOIN tariff_plans tp ON tp.id = cpp.plan_id
        WHERE cpp.active_to IS NULL
        ORDER BY cpp.active_from DESC
        LIMIT 1
        """
    )
    today = date.today().isoformat()
    return templates.TemplateResponse(
        request=request,
        name="plans.html",
        context={
            "plans": list(plans),
            "current_period": dict(current_period) if current_period else None,
            "today": today,
        },
    )


@_protected.get("/loads", response_class=HTMLResponse)
async def loads_page(
    request: Request,
    db: Annotated[Database, Depends(get_db)],
    page: int = 1,
) -> HTMLResponse:
    """Load events page: paginated table with confirm/reject actions."""
    from candela.tariffs.load_costs import LoadEvent, load_costs_for_plan

    per_page = 50
    offset = (page - 1) * per_page
    events = await db.fetch(
        "SELECT * FROM load_events ORDER BY started_at DESC LIMIT ? OFFSET ?",
        per_page,
        offset,
    )
    total = await db.fetchval("SELECT COUNT(*) FROM load_events") or 0

    # Monthly load cost summary using the currently active plan
    current_period = await db.fetchrow(
        """
        SELECT cpp.plan_id, tp.name AS plan_name
        FROM current_plan_periods cpp
        JOIN tariff_plans tp ON tp.id = cpp.plan_id
        WHERE cpp.active_to IS NULL
        ORDER BY cpp.active_from DESC
        LIMIT 1
        """
    )

    load_summary = None
    current_plan_name = None
    month_label = date.today().strftime("%B %Y")

    if current_period:
        current_plan_name = str(current_period["plan_name"])
        plan_id = int(current_period["plan_id"])

        today = date.today()
        month_from = today.replace(day=1)
        ts_from = datetime(month_from.year, month_from.month, 1, tzinfo=UTC).isoformat()
        ts_to = datetime(
            today.year, today.month, today.day, 23, 59, 59, tzinfo=UTC
        ).isoformat()

        load_rows = await db.fetch(
            """
            SELECT load_name, started_at, ended_at, kwh
            FROM load_events
            WHERE started_at >= ? AND started_at <= ?
              AND confidence >= 0.7
              AND kwh IS NOT NULL
              AND ended_at IS NOT NULL
            ORDER BY started_at
            """,
            ts_from,
            ts_to,
        )

        if load_rows:
            qualifying = [
                LoadEvent(
                    load_name=str(r["load_name"]),
                    started_at=_parse_ts(str(r["started_at"])),
                    ended_at=_parse_ts(str(r["ended_at"])),
                    kwh=float(r["kwh"]),
                )
                for r in load_rows
            ]
            load_summary = await load_costs_for_plan(
                qualifying, plan_id, month_from, today, db
            )

    return templates.TemplateResponse(
        request=request,
        name="loads.html",
        context={
            "events": list(events),
            "total": int(total),
            "page": page,
            "per_page": per_page,
            "total_pages": max(1, (int(total) + per_page - 1) // per_page),
            "load_summary": load_summary,
            "current_plan_name": current_plan_name,
            "month_label": month_label,
        },
    )


# ---------------------------------------------------------------------------
# HTMX partials
# ---------------------------------------------------------------------------


@_protected.get("/partials/status", response_class=HTMLResponse)
async def partial_status(
    request: Request, db: Annotated[Database, Depends(get_db)]
) -> HTMLResponse:
    """Live status panel: latest inverter reading. Refreshes every 60s via HTMX."""
    row = await db.fetchrow("SELECT * FROM solar_readings ORDER BY ts DESC LIMIT 1")
    reading = None
    if row is not None:
        reading = {
            "ts": _parse_ts(str(row["ts"])),
            "solar_w": int(row["solar_w"]),
            "grid_w": int(row["grid_w"]),
            "load_w": int(row["load_w"]),
        }
    return templates.TemplateResponse(
        request=request,
        name="partials/status.html",
        context={"reading": reading},
    )


@_protected.get("/partials/today-summary", response_class=HTMLResponse)
async def partial_today_summary(
    request: Request, db: Annotated[Database, Depends(get_db)]
) -> HTMLResponse:
    """Today's aggregate kWh summary, cost, and solar savings."""
    from decimal import Decimal

    from candela.tariffs.engine import (
        compute_bill,
        fetch_aemo_prices,
        fetch_plan,
        fetch_rates,
    )
    from candela.tariffs.models import BillResult, SolarReading
    from candela.tariffs.strategies.demand import DemandStrategy
    from candela.tariffs.strategies.single_rate import SingleRateStrategy
    from candela.tariffs.strategies.tou import TOUStrategy
    from candela.tariffs.strategies.wholesale import WholesaleStrategy

    today = date.today()
    ts_from = datetime(today.year, today.month, today.day, tzinfo=UTC).isoformat()
    ts_to = datetime(
        today.year, today.month, today.day, 23, 59, 59, tzinfo=UTC
    ).isoformat()
    rows = await db.fetch(
        "SELECT ts, solar_w, grid_w, load_w FROM solar_readings WHERE ts >= ? AND ts <= ?",
        ts_from,
        ts_to,
    )
    solar_kwh = sum(int(r["solar_w"]) * _INTERVAL_H / 1000 for r in rows)
    import_kwh = sum(max(int(r["grid_w"]), 0) * _INTERVAL_H / 1000 for r in rows)
    export_kwh = sum(max(-int(r["grid_w"]), 0) * _INTERVAL_H / 1000 for r in rows)

    cost_data = None
    if rows:
        current_period = await db.fetchrow(
            """
            SELECT cpp.plan_id, tp.name AS plan_name, tp.plan_type
            FROM current_plan_periods cpp
            JOIN tariff_plans tp ON tp.id = cpp.plan_id
            WHERE cpp.active_to IS NULL
            ORDER BY cpp.active_from DESC
            LIMIT 1
            """
        )
        if current_period:
            plan_id = int(current_period["plan_id"])
            try:
                bill = await compute_bill(plan_id, today, today, db)
                plan = await fetch_plan(plan_id, db)
                rates = await fetch_rates(plan_id, db)

                # Synthetic readings where solar=0 and all load comes from the grid
                no_solar_readings = [
                    SolarReading(
                        ts=_parse_ts(str(r["ts"])),
                        solar_w=0,
                        grid_w=max(int(r["load_w"]), 0),
                        load_w=int(r["load_w"]),
                    )
                    for r in rows
                ]

                no_solar_bill: BillResult | None
                if plan.plan_type == "wholesale":
                    aemo_prices = await fetch_aemo_prices(today, today, db)
                    no_solar_bill = WholesaleStrategy(
                        wholesale_adder_cents_per_kwh=Decimal("18.0")
                    ).compute(no_solar_readings, plan, rates, aemo_prices=aemo_prices)
                else:
                    _strategies: dict[str, TariffStrategy] = {
                        "single_rate": SingleRateStrategy(),
                        "tou": TOUStrategy(),
                        "demand": DemandStrategy(),
                    }
                    strategy = _strategies.get(plan.plan_type)
                    no_solar_bill = (
                        strategy.compute(no_solar_readings, plan, rates)
                        if strategy
                        else None
                    )

                savings_dollars = None
                if no_solar_bill is not None:
                    savings_dollars = (
                        float(no_solar_bill.total_cents - bill.total_cents) / 100
                    )

                cost_data = {
                    "plan_name": str(current_period["plan_name"]),
                    "total_dollars": float(bill.total_cents) / 100,
                    "solar_savings_dollars": savings_dollars,
                }
            except Exception:
                logger.warning("Failed to compute today's cost", exc_info=True)

    return templates.TemplateResponse(
        request=request,
        name="partials/today_summary.html",
        context={
            "solar_kwh": round(solar_kwh, 2),
            "import_kwh": round(import_kwh, 2),
            "export_kwh": round(export_kwh, 2),
            "cost_data": cost_data,
        },
    )


@_protected.get("/partials/wholesale-price", response_class=HTMLResponse)
async def partial_wholesale_price(
    request: Request, db: Annotated[Database, Depends(get_db)]
) -> HTMLResponse:
    """Most recent AEMO QLD1 spot price, colour-coded by level."""
    row = await db.fetchrow(
        "SELECT * FROM aemo_trading_prices ORDER BY interval_start DESC LIMIT 1"
    )
    price_info = None
    if row is not None:
        rrp = float(row["rrp_per_mwh"])
        cents_kwh = rrp / 10  # $/MWh → c/kWh
        if cents_kwh < 5:
            colour = "green"
        elif cents_kwh < 20:
            colour = "amber"
        else:
            colour = "red"
        price_info = {
            "rrp_per_mwh": rrp,
            "cents_kwh": round(cents_kwh, 2),
            "colour": colour,
            "interval_start": _parse_ts(str(row["interval_start"])),
        }
    return templates.TemplateResponse(
        request=request,
        name="partials/wholesale_price.html",
        context={"price": price_info},
    )


@_protected.get("/partials/daily-chart", response_class=HTMLResponse)
async def partial_daily_chart(
    request: Request,
    db: Annotated[Database, Depends(get_db)],
    from_: Annotated[date | None, Query(alias="from")] = None,
    to: date | None = None,
) -> HTMLResponse:
    """Daily bar chart data (solar / import / export kWh per day)."""
    if from_ is None:
        from_ = date.today() - timedelta(days=29)
    if to is None:
        to = date.today()

    ts_from = datetime(from_.year, from_.month, from_.day, tzinfo=UTC).isoformat()
    ts_to = datetime(to.year, to.month, to.day, 23, 59, 59, tzinfo=UTC).isoformat()
    rows = await db.fetch(
        "SELECT ts, solar_w, grid_w FROM solar_readings WHERE ts >= ? AND ts <= ? ORDER BY ts",
        ts_from,
        ts_to,
    )

    day_solar: dict[str, float] = defaultdict(float)
    day_import: dict[str, float] = defaultdict(float)
    day_export: dict[str, float] = defaultdict(float)

    for row in rows:
        d = _parse_ts(str(row["ts"])).date().isoformat()
        day_solar[d] += int(row["solar_w"]) * _INTERVAL_H / 1000
        grid_w = int(row["grid_w"])
        if grid_w > 0:
            day_import[d] += grid_w * _INTERVAL_H / 1000
        else:
            day_export[d] += -grid_w * _INTERVAL_H / 1000

    labels = sorted(set(day_solar) | set(day_import) | set(day_export))

    # Also fetch load events in the same range for the timeline
    load_rows = await db.fetch(
        """
        SELECT id, started_at, ended_at, load_name, source, confidence
        FROM load_events
        WHERE started_at >= ? AND started_at <= ?
        ORDER BY started_at
        """,
        ts_from,
        ts_to,
    )
    load_events = [
        {
            "id": int(r["id"]),
            "started_at": _parse_ts(str(r["started_at"])),
            "ended_at": _parse_ts(str(r["ended_at"])) if r["ended_at"] else None,
            "load_name": str(r["load_name"]),
            "source": str(r["source"]),
            "confidence": float(r["confidence"])
            if r["confidence"] is not None
            else None,
        }
        for r in load_rows
    ]

    return templates.TemplateResponse(
        request=request,
        name="partials/daily_chart.html",
        context={
            "labels": labels,
            "solar": [round(day_solar.get(d, 0), 2) for d in labels],
            "import_": [round(day_import.get(d, 0), 2) for d in labels],
            "export": [round(day_export.get(d, 0), 2) for d in labels],
            "load_events": load_events,
            "from_": from_,
            "to": to,
        },
    )


@_protected.get("/partials/compare-results", response_class=HTMLResponse)
async def partial_compare_results(
    request: Request,
    db: Annotated[Database, Depends(get_db)],
    from_: Annotated[date | None, Query(alias="from")] = None,
    to: date | None = None,
    plan_ids: str | None = Query(None),
) -> HTMLResponse:
    """Side-by-side cost cards for selected plans."""
    from candela.tariffs.engine import compute_bill
    from candela.tariffs.load_costs import LoadEvent, load_costs_for_plan

    if from_ is None:
        today = date.today()
        from_ = today.replace(day=1)
    if to is None:
        to = date.today()

    # Fetch high-confidence load events in the date range (used for all plans)
    ts_from = datetime(from_.year, from_.month, from_.day, tzinfo=UTC).isoformat()
    ts_to = datetime(to.year, to.month, to.day, 23, 59, 59, tzinfo=UTC).isoformat()
    load_rows = await db.fetch(
        """
        SELECT load_name, started_at, ended_at, kwh
        FROM load_events
        WHERE started_at >= ? AND started_at <= ?
          AND confidence >= 0.7
          AND kwh IS NOT NULL
          AND ended_at IS NOT NULL
        ORDER BY started_at
        """,
        ts_from,
        ts_to,
    )
    qualifying_events = [
        LoadEvent(
            load_name=str(r["load_name"]),
            started_at=_parse_ts(str(r["started_at"])),
            ended_at=_parse_ts(str(r["ended_at"])),
            kwh=float(r["kwh"]),
        )
        for r in load_rows
    ]

    results = []
    load_costs_by_plan: dict[int, list] = {}

    if plan_ids:
        ids = [int(x.strip()) for x in plan_ids.split(",") if x.strip()]
        for plan_id in ids:
            try:
                bill = await compute_bill(plan_id, from_, to, db)
            except ValueError:
                logger.warning("Plan %d not found during compare partial", plan_id)
                continue

            plan_row = await db.fetchrow(
                "SELECT name, plan_type FROM tariff_plans WHERE id = ?", plan_id
            )
            if plan_row is None:
                continue

            results.append(
                {
                    "plan_id": plan_id,
                    "plan_name": str(plan_row["name"]),
                    "plan_type": str(plan_row["plan_type"]),
                    "total_cents": float(bill.total_cents),
                    "supply_charge_cents": float(bill.supply_charge_cents),
                    "import_charge_cents": float(bill.import_charge_cents),
                    "export_credit_cents": float(bill.export_credit_cents),
                    "demand_charge_cents": float(bill.demand_charge_cents),
                    "period_breakdown": {
                        k: {"kwh": float(v.kwh), "cents": float(v.cents)}
                        for k, v in bill.period_breakdown.items()
                    },
                }
            )

            if qualifying_events:
                try:
                    load_costs_by_plan[plan_id] = await load_costs_for_plan(
                        qualifying_events, plan_id, from_, to, db
                    )
                except ValueError:
                    pass

    # Sort by total cost ascending so cheapest is first
    results.sort(key=lambda r: cast(float, r["total_cents"]))

    return templates.TemplateResponse(
        request=request,
        name="partials/compare_results.html",
        context={
            "results": results,
            "from_": from_,
            "to": to,
            "load_costs_by_plan": load_costs_by_plan,
        },
    )


# Include all protected routes (registered above) into the public router,
# with require_auth applied. Must be at the end of the file so that all
# @_protected routes are already registered before include_router is called.
router.include_router(_protected, dependencies=[Depends(require_auth)])
