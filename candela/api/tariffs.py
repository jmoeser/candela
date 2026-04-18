"""JSON API routes for tariff plans and bill comparison.

GET    /api/v1/plans                       list all plans
POST   /api/v1/plans                       create a plan (with optional rates)
PUT    /api/v1/plans/{id}                  update a plan (soft-delete via valid_to)
DELETE /api/v1/plans/{id}                  delete a plan (not current or wholesale)
GET    /api/v1/compare?plan_ids=&from=&to= compute bills for multiple plans side-by-side
"""

import json
import logging
from datetime import date, timedelta
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel

from candela.auth import require_api_key
from candela.db import Database
from candela.main import get_db
from candela.tariffs.engine import compute_bill

logger = logging.getLogger(__name__)

router = APIRouter(dependencies=[Depends(require_api_key)])


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------


class PlanOut(BaseModel):
    id: int
    name: str
    retailer: str | None
    plan_type: str
    supply_charge_daily_cents: float
    feed_in_tariff_cents: float | None
    valid_from: date
    valid_to: date | None
    notes: str | None


class RateIn(BaseModel):
    rate_type: str
    cents_per_kwh: float | None = None
    cents_per_kw: float | None = None
    window_start: str | None = None  # "HH:MM"
    window_end: str | None = None
    days_of_week: list[int] | None = None
    months: list[int] | None = None
    demand_window_start: str | None = None  # "HH:MM"
    demand_window_end: str | None = None


class PlanIn(BaseModel):
    name: str
    retailer: str | None = None
    plan_type: str
    supply_charge_daily_cents: float
    feed_in_tariff_cents: float | None = None
    valid_from: date
    valid_to: date | None = None
    notes: str | None = None
    rates: list[RateIn] = []


class CurrentPlanPeriodOut(BaseModel):
    plan_id: int
    plan_name: str
    active_from: date
    active_to: date | None


class SetCurrentIn(BaseModel):
    active_from: date


class PeriodBreakdownItem(BaseModel):
    kwh: float
    cents: float


class CompareResult(BaseModel):
    plan_id: int
    plan_name: str
    plan_type: str
    total_cents: float
    supply_charge_cents: float
    import_charge_cents: float
    export_credit_cents: float
    demand_charge_cents: float
    period_breakdown: dict[str, PeriodBreakdownItem]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _row_to_plan_out(row: object) -> PlanOut:
    return PlanOut(
        id=int(row["id"]),  # type: ignore[index]
        name=str(row["name"]),  # type: ignore[index]
        retailer=str(row["retailer"]) if row["retailer"] else None,  # type: ignore[index]
        plan_type=str(row["plan_type"]),  # type: ignore[index]
        supply_charge_daily_cents=float(row["supply_charge_daily_cents"]),  # type: ignore[index]
        feed_in_tariff_cents=(
            float(row["feed_in_tariff_cents"])  # type: ignore[index]
            if row["feed_in_tariff_cents"] is not None  # type: ignore[index]
            else None
        ),
        valid_from=date.fromisoformat(str(row["valid_from"])),  # type: ignore[index]
        valid_to=(
            date.fromisoformat(str(row["valid_to"]))  # type: ignore[index]
            if row["valid_to"]  # type: ignore[index]
            else None
        ),
        notes=str(row["notes"]) if row["notes"] else None,  # type: ignore[index]
    )


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------


@router.get("/plans", response_model=list[PlanOut])
async def list_plans(db: Annotated[Database, Depends(get_db)]) -> list[PlanOut]:
    """Return all tariff plans ordered by id."""
    rows = await db.fetch("SELECT * FROM tariff_plans ORDER BY id")
    return [_row_to_plan_out(r) for r in rows]


@router.post("/plans", response_model=PlanOut, status_code=201)
async def create_plan(
    plan: PlanIn, db: Annotated[Database, Depends(get_db)]
) -> PlanOut:
    """Create a new tariff plan, optionally with rates."""
    await db.execute(
        """
        INSERT INTO tariff_plans
            (name, retailer, plan_type, supply_charge_daily_cents,
             feed_in_tariff_cents, valid_from, valid_to, notes)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        plan.name,
        plan.retailer,
        plan.plan_type,
        plan.supply_charge_daily_cents,
        plan.feed_in_tariff_cents,
        plan.valid_from.isoformat(),
        plan.valid_to.isoformat() if plan.valid_to else None,
        plan.notes,
    )
    row = await db.fetchrow(
        "SELECT * FROM tariff_plans WHERE name = ? ORDER BY id DESC LIMIT 1",
        plan.name,
    )
    assert row is not None
    plan_id = int(row["id"])  # type: ignore[index]

    for rate in plan.rates:
        await db.execute(
            """
            INSERT INTO tariff_rates
                (plan_id, rate_type, cents_per_kwh, cents_per_kw,
                 window_start, window_end, days_of_week, months,
                 demand_window_start, demand_window_end)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            plan_id,
            rate.rate_type,
            rate.cents_per_kwh,
            rate.cents_per_kw,
            rate.window_start,
            rate.window_end,
            json.dumps(rate.days_of_week) if rate.days_of_week is not None else None,
            json.dumps(rate.months) if rate.months is not None else None,
            rate.demand_window_start,
            rate.demand_window_end,
        )

    return _row_to_plan_out(row)


@router.delete("/plans/{plan_id}", status_code=204)
async def delete_plan(
    plan_id: int,
    db: Annotated[Database, Depends(get_db)],
) -> None:
    """Delete a tariff plan.

    Rejected if the plan is currently active or has ``plan_type = 'wholesale'``.
    Also deletes associated rates and any historical current-plan-period records.
    """
    existing = await db.fetchrow(
        "SELECT id, plan_type FROM tariff_plans WHERE id = ?", plan_id
    )
    if existing is None:
        raise HTTPException(status_code=404, detail=f"Plan {plan_id} not found")

    if str(existing["plan_type"]) == "wholesale":
        raise HTTPException(status_code=409, detail="Wholesale plans cannot be deleted")

    active = await db.fetchrow(
        "SELECT id FROM current_plan_periods WHERE plan_id = ? AND active_to IS NULL",
        plan_id,
    )
    if active is not None:
        raise HTTPException(
            status_code=409, detail="Cannot delete the currently active plan"
        )

    await db.execute("DELETE FROM tariff_rates WHERE plan_id = ?", plan_id)
    await db.execute("DELETE FROM current_plan_periods WHERE plan_id = ?", plan_id)
    await db.execute("DELETE FROM tariff_plans WHERE id = ?", plan_id)


@router.get("/plans/current", response_model=CurrentPlanPeriodOut | None)
async def get_current_plan(
    db: Annotated[Database, Depends(get_db)],
) -> CurrentPlanPeriodOut | None:
    """Return the currently active plan period, or null if none is set."""
    row = await db.fetchrow(
        """
        SELECT cpp.plan_id, tp.name AS plan_name, cpp.active_from, cpp.active_to
        FROM current_plan_periods cpp
        JOIN tariff_plans tp ON tp.id = cpp.plan_id
        WHERE cpp.active_to IS NULL
        ORDER BY cpp.active_from DESC
        LIMIT 1
        """
    )
    if row is None:
        return None
    return CurrentPlanPeriodOut(
        plan_id=int(row["plan_id"]),
        plan_name=str(row["plan_name"]),
        active_from=date.fromisoformat(str(row["active_from"])),
        active_to=None,
    )


@router.post("/plans/{plan_id}/set-current", response_model=CurrentPlanPeriodOut)
async def set_current_plan(
    plan_id: int,
    body: SetCurrentIn,
    db: Annotated[Database, Depends(get_db)],
) -> CurrentPlanPeriodOut:
    """Set a plan as the currently active plan.

    Closes any open period by setting its ``active_to`` to ``active_from - 1 day``,
    then opens a new period for the given plan starting from ``active_from``.
    """
    existing = await db.fetchrow("SELECT id FROM tariff_plans WHERE id = ?", plan_id)
    if existing is None:
        raise HTTPException(status_code=404, detail=f"Plan {plan_id} not found")

    prev_day = (body.active_from - timedelta(days=1)).isoformat()
    await db.execute(
        "UPDATE current_plan_periods SET active_to = ? WHERE active_to IS NULL",
        prev_day,
    )
    await db.execute(
        "INSERT INTO current_plan_periods (plan_id, active_from, active_to) VALUES (?, ?, NULL)",
        plan_id,
        body.active_from.isoformat(),
    )

    plan_row = await db.fetchrow("SELECT name FROM tariff_plans WHERE id = ?", plan_id)
    assert plan_row is not None
    return CurrentPlanPeriodOut(
        plan_id=plan_id,
        plan_name=str(plan_row["name"]),
        active_from=body.active_from,
        active_to=None,
    )


@router.put("/plans/{plan_id}", response_model=PlanOut)
async def update_plan(
    plan_id: int,
    plan: PlanIn,
    db: Annotated[Database, Depends(get_db)],
) -> PlanOut:
    """Update an existing tariff plan.

    Use ``valid_to`` to soft-delete (preserve historical bill calculations).
    """
    existing = await db.fetchrow("SELECT id FROM tariff_plans WHERE id = ?", plan_id)
    if existing is None:
        raise HTTPException(status_code=404, detail=f"Plan {plan_id} not found")

    await db.execute(
        """
        UPDATE tariff_plans
        SET name = ?, retailer = ?, plan_type = ?,
            supply_charge_daily_cents = ?, feed_in_tariff_cents = ?,
            valid_from = ?, valid_to = ?, notes = ?
        WHERE id = ?
        """,
        plan.name,
        plan.retailer,
        plan.plan_type,
        plan.supply_charge_daily_cents,
        plan.feed_in_tariff_cents,
        plan.valid_from.isoformat(),
        plan.valid_to.isoformat() if plan.valid_to else None,
        plan.notes,
        plan_id,
    )
    row = await db.fetchrow("SELECT * FROM tariff_plans WHERE id = ?", plan_id)
    assert row is not None
    return _row_to_plan_out(row)


@router.get("/compare", response_model=list[CompareResult])
async def compare_plans(
    db: Annotated[Database, Depends(get_db)],
    from_: Annotated[date, Query(alias="from")],
    to: date,
    plan_ids: str | None = Query(None, description="Comma-separated plan IDs"),
) -> list[CompareResult]:
    """Compute electricity bills for multiple plans over the same date range.

    Unknown plan IDs are silently skipped so the UI can pass a fixed set of IDs
    without failing when plans are deleted.
    """
    if not plan_ids:
        return []

    ids = [int(x.strip()) for x in plan_ids.split(",") if x.strip()]
    if not ids:
        return []

    results: list[CompareResult] = []
    for plan_id in ids:
        try:
            bill = await compute_bill(plan_id, from_, to, db)
        except ValueError:
            logger.warning(
                "Plan %d not found or unsupported — skipping compare", plan_id
            )
            continue

        plan_row = await db.fetchrow(
            "SELECT name, plan_type FROM tariff_plans WHERE id = ?", plan_id
        )
        if plan_row is None:
            continue

        results.append(
            CompareResult(
                plan_id=plan_id,
                plan_name=str(plan_row["name"]),
                plan_type=str(plan_row["plan_type"]),
                total_cents=float(bill.total_cents),
                supply_charge_cents=float(bill.supply_charge_cents),
                import_charge_cents=float(bill.import_charge_cents),
                export_credit_cents=float(bill.export_credit_cents),
                demand_charge_cents=float(bill.demand_charge_cents),
                period_breakdown={
                    k: PeriodBreakdownItem(kwh=float(v.kwh), cents=float(v.cents))
                    for k, v in bill.period_breakdown.items()
                },
            )
        )

    return results
