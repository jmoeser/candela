"""JSON API routes for tariff plans and bill comparison.

GET  /api/v1/plans                       list all plans
POST /api/v1/plans                       create a plan
PUT  /api/v1/plans/{id}                  update a plan (soft-delete via valid_to)
GET  /api/v1/compare?plan_ids=&from=&to= compute bills for multiple plans side-by-side
"""

import logging
from datetime import date
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


class PlanIn(BaseModel):
    name: str
    retailer: str | None = None
    plan_type: str
    supply_charge_daily_cents: float
    feed_in_tariff_cents: float | None = None
    valid_from: date
    valid_to: date | None = None
    notes: str | None = None


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
    """Create a new tariff plan."""
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
    return _row_to_plan_out(row)


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
