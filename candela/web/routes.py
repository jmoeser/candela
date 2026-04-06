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
from typing import Annotated

from fastapi import APIRouter, Depends, Form, Query
from fastapi.requests import Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from candela.auth import require_auth
from candela.config import get_settings
from candela.db import Database
from candela.main import get_db

logger = logging.getLogger(__name__)

router = APIRouter()
_protected = APIRouter()
templates = Jinja2Templates(directory=Path(__file__).parent / "templates")

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


@_protected.get("/plans", response_class=HTMLResponse)
async def plans_page(
    request: Request, db: Annotated[Database, Depends(get_db)]
) -> HTMLResponse:
    """Plan management: list and create/edit forms."""
    plans = await db.fetch("SELECT * FROM tariff_plans ORDER BY id")
    return templates.TemplateResponse(
        request=request, name="plans.html", context={"plans": list(plans)}
    )


@_protected.get("/loads", response_class=HTMLResponse)
async def loads_page(
    request: Request,
    db: Annotated[Database, Depends(get_db)],
    page: int = 1,
) -> HTMLResponse:
    """Load events page: paginated table with confirm/reject actions."""
    per_page = 50
    offset = (page - 1) * per_page
    events = await db.fetch(
        "SELECT * FROM load_events ORDER BY started_at DESC LIMIT ? OFFSET ?",
        per_page,
        offset,
    )
    total = await db.fetchval("SELECT COUNT(*) FROM load_events") or 0
    return templates.TemplateResponse(
        request=request,
        name="loads.html",
        context={
            "events": list(events),
            "total": int(total),
            "page": page,
            "per_page": per_page,
            "total_pages": max(1, (int(total) + per_page - 1) // per_page),
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
    """Today's aggregate kWh summary."""
    today = date.today()
    ts_from = datetime(today.year, today.month, today.day, tzinfo=UTC).isoformat()
    ts_to = datetime(
        today.year, today.month, today.day, 23, 59, 59, tzinfo=UTC
    ).isoformat()
    rows = await db.fetch(
        "SELECT solar_w, grid_w FROM solar_readings WHERE ts >= ? AND ts <= ?",
        ts_from,
        ts_to,
    )
    solar_kwh = sum(int(r["solar_w"]) * _INTERVAL_H / 1000 for r in rows)
    import_kwh = sum(max(int(r["grid_w"]), 0) * _INTERVAL_H / 1000 for r in rows)
    export_kwh = sum(max(-int(r["grid_w"]), 0) * _INTERVAL_H / 1000 for r in rows)
    return templates.TemplateResponse(
        request=request,
        name="partials/today_summary.html",
        context={
            "solar_kwh": round(solar_kwh, 2),
            "import_kwh": round(import_kwh, 2),
            "export_kwh": round(export_kwh, 2),
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

    if from_ is None:
        today = date.today()
        from_ = today.replace(day=1)
    if to is None:
        to = date.today()

    results = []
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

    # Sort by total cost ascending so cheapest is first
    results.sort(key=lambda r: r["total_cents"])

    return templates.TemplateResponse(
        request=request,
        name="partials/compare_results.html",
        context={"results": results, "from_": from_, "to": to},
    )


# Include all protected routes (registered above) into the public router,
# with require_auth applied. Must be at the end of the file so that all
# @_protected routes are already registered before include_router is called.
router.include_router(_protected, dependencies=[Depends(require_auth)])
