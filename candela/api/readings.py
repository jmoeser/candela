"""JSON API routes for solar readings.

GET  /api/v1/readings?from=&to=&limit=&offset=   paginated readings
GET  /api/v1/readings/latest                      most recent reading (or null)
"""

import logging
from datetime import UTC, date, datetime
from typing import Annotated

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel

from candela.auth import require_api_key
from candela.db import Database
from candela.main import get_db

logger = logging.getLogger(__name__)

router = APIRouter(dependencies=[Depends(require_api_key)])


class ReadingOut(BaseModel):
    ts: datetime
    solar_w: int
    grid_w: int
    load_w: int
    daily_yield_kwh: float | None = None
    total_yield_kwh: float | None = None
    inverter_temp_c: float | None = None


class ReadingsResponse(BaseModel):
    readings: list[ReadingOut]
    count: int


def _row_to_reading(row: object) -> ReadingOut:
    ts_raw = str(row["ts"])  # type: ignore[index]
    if ts_raw.endswith("Z"):
        ts_raw = ts_raw[:-1] + "+00:00"
    ts = datetime.fromisoformat(ts_raw)
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=UTC)
    return ReadingOut(
        ts=ts,
        solar_w=int(row["solar_w"]),  # type: ignore[index]
        grid_w=int(row["grid_w"]),  # type: ignore[index]
        load_w=int(row["load_w"]),  # type: ignore[index]
        daily_yield_kwh=(
            float(row["daily_yield_kwh"])  # type: ignore[index]
            if row["daily_yield_kwh"] is not None  # type: ignore[index]
            else None
        ),
        total_yield_kwh=(
            float(row["total_yield_kwh"])  # type: ignore[index]
            if row["total_yield_kwh"] is not None  # type: ignore[index]
            else None
        ),
        inverter_temp_c=(
            float(row["inverter_temp_c"])  # type: ignore[index]
            if row["inverter_temp_c"] is not None  # type: ignore[index]
            else None
        ),
    )


@router.get("/readings/latest", response_model=ReadingOut | None)
async def get_latest_reading(
    db: Annotated[Database, Depends(get_db)],
) -> ReadingOut | None:
    """Return the most recent solar reading, or null if none exist."""
    row = await db.fetchrow("SELECT * FROM solar_readings ORDER BY ts DESC LIMIT 1")
    return _row_to_reading(row) if row is not None else None


@router.get("/readings", response_model=ReadingsResponse)
async def get_readings(
    db: Annotated[Database, Depends(get_db)],
    from_: Annotated[date | None, Query(alias="from")] = None,
    to: date | None = None,
    limit: int = 288,
    offset: int = 0,
) -> ReadingsResponse:
    """Return a paginated list of solar readings, optionally filtered by date range."""
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
        f"SELECT * FROM solar_readings {where} ORDER BY ts DESC LIMIT ? OFFSET ?",
        *params,
        limit,
        offset,
    )
    readings = [_row_to_reading(r) for r in rows]
    return ReadingsResponse(readings=readings, count=len(readings))
