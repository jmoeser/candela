"""JSON API routes for daily and monthly summaries.

GET  /api/v1/summary/daily?from=&to=     daily aggregates (solar, import, export kWh)
GET  /api/v1/summary/monthly?month=      calendar-month totals
"""

import logging
from calendar import monthrange
from collections import defaultdict
from datetime import UTC, date, datetime
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel

from candela.auth import require_api_key
from candela.db import Database
from candela.main import get_db

logger = logging.getLogger(__name__)

router = APIRouter(dependencies=[Depends(require_api_key)])

_INTERVAL_H = 5 / 60  # 5-minute reading → hours


class DailySummaryItem(BaseModel):
    date: date
    solar_kwh: float
    import_kwh: float
    export_kwh: float
    self_consumption_pct: float | None


class DailySummaryResponse(BaseModel):
    days: list[DailySummaryItem]


class MonthlySummaryResponse(BaseModel):
    month: str
    import_kwh: float
    export_kwh: float
    solar_kwh: float
    estimated_cost_cents: float | None
    plan_id: int | None
    plan_name: str | None


def _reading_kwh(watts: int) -> float:
    return watts * _INTERVAL_H / 1000


def _parse_ts(raw: str) -> datetime:
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    dt = datetime.fromisoformat(raw)
    return dt if dt.tzinfo else dt.replace(tzinfo=UTC)


@router.get("/summary/daily", response_model=DailySummaryResponse)
async def get_daily_summary(
    db: Annotated[Database, Depends(get_db)],
    from_: Annotated[date | None, Query(alias="from")] = None,
    to: date | None = None,
) -> DailySummaryResponse:
    """Return daily aggregate kWh for solar generation, grid import, and grid export."""
    conditions: list[str] = []
    params: list[object] = []

    if from_ is not None:
        conditions.append("ts >= ?")
        params.append(
            datetime(from_.year, from_.month, from_.day, tzinfo=UTC).isoformat()
        )
    if to is not None:
        conditions.append("ts <= ?")
        params.append(
            datetime(to.year, to.month, to.day, 23, 59, 59, tzinfo=UTC).isoformat()
        )

    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    rows = await db.fetch(
        f"SELECT ts, solar_w, grid_w FROM solar_readings {where} ORDER BY ts",
        *params,
    )

    day_solar: dict[str, float] = defaultdict(float)
    day_import: dict[str, float] = defaultdict(float)
    day_export: dict[str, float] = defaultdict(float)

    for row in rows:
        d = _parse_ts(str(row["ts"])).date().isoformat()
        day_solar[d] += _reading_kwh(int(row["solar_w"]))
        grid_w = int(row["grid_w"])
        if grid_w > 0:
            day_import[d] += _reading_kwh(grid_w)
        else:
            day_export[d] += _reading_kwh(-grid_w)

    all_days = sorted(set(day_solar) | set(day_import) | set(day_export))
    items: list[DailySummaryItem] = []
    for d in all_days:
        solar = day_solar.get(d, 0.0)
        imp = day_import.get(d, 0.0)
        exp = day_export.get(d, 0.0)
        pct = (solar - exp) / solar * 100 if solar > 0 else None
        items.append(
            DailySummaryItem(
                date=date.fromisoformat(d),
                solar_kwh=round(solar, 3),
                import_kwh=round(imp, 3),
                export_kwh=round(exp, 3),
                self_consumption_pct=round(pct, 1) if pct is not None else None,
            )
        )

    return DailySummaryResponse(days=items)


@router.get("/summary/monthly", response_model=MonthlySummaryResponse)
async def get_monthly_summary(
    db: Annotated[Database, Depends(get_db)],
    month: str = Query(..., description="Calendar month in YYYY-MM format"),
) -> MonthlySummaryResponse:
    """Return totals for a full calendar month.

    Also used by the future Glow-worm integration endpoint.
    """
    try:
        year, mon = int(month[:4]), int(month[5:7])
        if not (1 <= mon <= 12):
            raise ValueError
    except ValueError, IndexError:
        raise HTTPException(status_code=422, detail="month must be YYYY-MM")

    _, last_day = monthrange(year, mon)
    ts_from = datetime(year, mon, 1, tzinfo=UTC).isoformat()
    ts_to = datetime(year, mon, last_day, 23, 59, 59, tzinfo=UTC).isoformat()

    rows = await db.fetch(
        "SELECT solar_w, grid_w FROM solar_readings WHERE ts >= ? AND ts <= ?",
        ts_from,
        ts_to,
    )

    solar_kwh = 0.0
    import_kwh = 0.0
    export_kwh = 0.0
    for row in rows:
        solar_kwh += _reading_kwh(int(row["solar_w"]))
        grid_w = int(row["grid_w"])
        if grid_w > 0:
            import_kwh += _reading_kwh(grid_w)
        else:
            export_kwh += _reading_kwh(-grid_w)

    return MonthlySummaryResponse(
        month=month,
        import_kwh=round(import_kwh, 3),
        export_kwh=round(export_kwh, 3),
        solar_kwh=round(solar_kwh, 3),
        estimated_cost_cents=None,
        plan_id=None,
        plan_name=None,
    )
