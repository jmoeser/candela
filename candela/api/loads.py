"""JSON API routes for load events and AEMO wholesale prices.

GET   /api/v1/loads?from=&to=&limit=&offset=   list load events
POST  /api/v1/loads                             create a manual load event
PATCH /api/v1/loads/{id}                        confirm / reject / update
GET   /api/v1/wholesale/prices?from=&to=        AEMO price data
"""

import logging
from datetime import UTC, date, datetime
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel

from candela.auth import require_api_key
from candela.db import Database
from candela.main import get_db

logger = logging.getLogger(__name__)

router = APIRouter(dependencies=[Depends(require_api_key)])


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------


class LoadEventOut(BaseModel):
    id: int
    started_at: datetime
    ended_at: datetime | None
    load_name: str
    avg_watts: int | None
    kwh: float | None
    confidence: float | None
    source: str


class LoadsResponse(BaseModel):
    events: list[LoadEventOut]
    count: int


class LoadEventIn(BaseModel):
    started_at: datetime
    ended_at: datetime | None = None
    load_name: str
    avg_watts: int | None = None
    kwh: float | None = None
    confidence: float | None = None


class LoadEventPatch(BaseModel):
    source: str | None = None
    ended_at: datetime | None = None
    avg_watts: int | None = None
    kwh: float | None = None
    confidence: float | None = None


class WholesalePriceOut(BaseModel):
    interval_start: datetime
    interval_end: datetime
    rrp_per_mwh: float
    region: str


class WholesalePricesResponse(BaseModel):
    prices: list[WholesalePriceOut]
    count: int


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _parse_ts(raw: str) -> datetime:
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    dt = datetime.fromisoformat(raw)
    return dt if dt.tzinfo else dt.replace(tzinfo=UTC)


def _row_to_event(row: object) -> LoadEventOut:
    return LoadEventOut(
        id=int(row["id"]),  # type: ignore[index]
        started_at=_parse_ts(str(row["started_at"])),  # type: ignore[index]
        ended_at=(
            _parse_ts(str(row["ended_at"]))  # type: ignore[index]
            if row["ended_at"]  # type: ignore[index]
            else None
        ),
        load_name=str(row["load_name"]),  # type: ignore[index]
        avg_watts=(
            int(row["avg_watts"])  # type: ignore[index]
            if row["avg_watts"] is not None  # type: ignore[index]
            else None
        ),
        kwh=(
            float(row["kwh"])  # type: ignore[index]
            if row["kwh"] is not None  # type: ignore[index]
            else None
        ),
        confidence=(
            float(row["confidence"])  # type: ignore[index]
            if row["confidence"] is not None  # type: ignore[index]
            else None
        ),
        source=str(row["source"]),  # type: ignore[index]
    )


# ---------------------------------------------------------------------------
# Load event routes
# ---------------------------------------------------------------------------


@router.get("/loads", response_model=LoadsResponse)
async def list_loads(
    db: Annotated[Database, Depends(get_db)],
    from_: Annotated[datetime | None, Query(alias="from")] = None,
    to: datetime | None = None,
    limit: int = 50,
    offset: int = 0,
) -> LoadsResponse:
    """Return a paginated list of load events, optionally filtered by start time."""
    conditions: list[str] = []
    params: list[object] = []

    if from_ is not None:
        conditions.append("started_at >= ?")
        params.append(from_.isoformat())
    if to is not None:
        conditions.append("started_at <= ?")
        params.append(to.isoformat())

    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    rows = await db.fetch(
        f"SELECT * FROM load_events {where} ORDER BY started_at DESC LIMIT ? OFFSET ?",
        *params,
        limit,
        offset,
    )
    events = [_row_to_event(r) for r in rows]
    return LoadsResponse(events=events, count=len(events))


@router.post("/loads", response_model=LoadEventOut, status_code=201)
async def create_load_event(
    event: LoadEventIn, db: Annotated[Database, Depends(get_db)]
) -> LoadEventOut:
    """Create a manual load event (source is always 'manual')."""
    await db.execute(
        """
        INSERT INTO load_events
            (started_at, ended_at, load_name, avg_watts, kwh, confidence, source)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        event.started_at.isoformat(),
        event.ended_at.isoformat() if event.ended_at else None,
        event.load_name,
        event.avg_watts,
        event.kwh,
        event.confidence,
        "manual",
    )
    row = await db.fetchrow(
        """
        SELECT * FROM load_events
        WHERE started_at = ? AND load_name = ?
        ORDER BY id DESC LIMIT 1
        """,
        event.started_at.isoformat(),
        event.load_name,
    )
    assert row is not None
    return _row_to_event(row)


@router.patch("/loads/{event_id}", response_model=LoadEventOut)
async def update_load_event(
    event_id: int,
    patch: LoadEventPatch,
    db: Annotated[Database, Depends(get_db)],
) -> LoadEventOut:
    """Confirm, reject, or update a load event.

    Typically called with ``{"source": "manual", "confidence": 1.0}`` to confirm
    or ``{"source": "manual", "confidence": 0.0}`` to reject.
    """
    existing = await db.fetchrow("SELECT id FROM load_events WHERE id = ?", event_id)
    if existing is None:
        raise HTTPException(status_code=404, detail=f"Load event {event_id} not found")

    updates: list[str] = []
    params: list[object] = []

    if patch.source is not None:
        updates.append("source = ?")
        params.append(patch.source)
    if patch.confidence is not None:
        updates.append("confidence = ?")
        params.append(patch.confidence)
    if patch.ended_at is not None:
        updates.append("ended_at = ?")
        params.append(patch.ended_at.isoformat())
    if patch.avg_watts is not None:
        updates.append("avg_watts = ?")
        params.append(patch.avg_watts)
    if patch.kwh is not None:
        updates.append("kwh = ?")
        params.append(patch.kwh)

    if updates:
        params.append(event_id)
        await db.execute(
            f"UPDATE load_events SET {', '.join(updates)} WHERE id = ?",
            *params,
        )

    row = await db.fetchrow("SELECT * FROM load_events WHERE id = ?", event_id)
    assert row is not None
    return _row_to_event(row)


# ---------------------------------------------------------------------------
# Wholesale prices route
# ---------------------------------------------------------------------------


@router.get("/wholesale/prices", response_model=WholesalePricesResponse)
async def get_wholesale_prices(
    db: Annotated[Database, Depends(get_db)],
    from_: Annotated[date | None, Query(alias="from")] = None,
    to: date | None = None,
) -> WholesalePricesResponse:
    """Return AEMO QLD1 wholesale prices for the given date range."""
    conditions: list[str] = []
    params: list[object] = []

    if from_ is not None:
        conditions.append("interval_start >= ?")
        params.append(
            datetime(from_.year, from_.month, from_.day, tzinfo=UTC).isoformat()
        )
    if to is not None:
        conditions.append("interval_start <= ?")
        params.append(
            datetime(to.year, to.month, to.day, 23, 59, 59, tzinfo=UTC).isoformat()
        )

    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    rows = await db.fetch(
        f"""
        SELECT interval_start, interval_end, rrp_per_mwh, region
        FROM aemo_trading_prices
        {where}
        ORDER BY interval_start
        """,
        *params,
    )
    prices = [
        WholesalePriceOut(
            interval_start=_parse_ts(str(r["interval_start"])),
            interval_end=_parse_ts(str(r["interval_end"])),
            rrp_per_mwh=float(r["rrp_per_mwh"]),
            region=str(r["region"]),
        )
        for r in rows
    ]
    return WholesalePricesResponse(prices=prices, count=len(prices))
