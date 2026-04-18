"""Load disaggregation detector.

Implements a sliding-window, profile-matching algorithm that identifies EV
charging and hot water heat pump events within the ``load_w`` time series.

Algorithm overview
------------------
For each reading (in time order):

1. Compute ``net_load = load_w - baseline``, where *baseline* is the median
   ``load_w`` during overnight hours (11 pm – 6 am).
2. Greedily assign the net load to known profiles, tightest-range-first.  The
   tightest-first ordering ensures that ``hot_water_boost`` (narrow range) is
   not eclipsed by ``ev_charging`` (wide range) at similar wattages.
3. Track which profiles are currently active.  When a profile goes inactive,
   close the event.  If it lasted at least ``min_duration_min``, record it.

Overlap handling
----------------
After the first matching profile consumes its expected midpoint wattage from
the net load, any remaining is checked against smaller profiles in turn.
This lets EV + hot water events be detected simultaneously.
"""

import logging
import statistics
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

from candela.db import Database
from candela.disaggregation.models import KNOWN_LOADS, LoadEvent
from candela.tariffs.models import SolarReading

logger = logging.getLogger(__name__)

INTERVAL_MINUTES = 5

# Profiles sorted by range width (max - min) ascending: tightest first.
_PROFILES: list[dict] = sorted(
    KNOWN_LOADS, key=lambda p: p["max_watts"] - p["min_watts"]
)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def detect_events(readings: list[SolarReading]) -> list[LoadEvent]:
    """Detect load events in *readings* and return completed ``LoadEvent`` objects.

    This is a pure function — no database access.  Readings need not be
    pre-sorted; this function sorts by timestamp internally.
    """
    if not readings:
        return []

    sorted_readings = sorted(readings, key=lambda r: r.ts)
    baseline = _compute_baseline(sorted_readings)

    # active: profile_name → {"started_at": datetime, "readings": list[SolarReading]}
    active: dict[str, dict] = {}
    completed: list[LoadEvent] = []

    for reading in sorted_readings:
        net_load = max(float(reading.load_w) - baseline, 0.0)
        current = set(_active_profiles(net_load))

        # Close events that are no longer active at this reading
        for name in list(active.keys()):
            if name not in current:
                _maybe_record_event(active.pop(name), reading.ts, name, completed)

        # Extend running events or start new ones
        for name in current:
            if name not in active:
                active[name] = {"started_at": reading.ts, "readings": [reading]}
            else:
                active[name]["readings"].append(reading)

    # Close any events still active at the end of the window
    if sorted_readings:
        last_ts = sorted_readings[-1].ts
        for name, state in list(active.items()):
            _maybe_record_event(state, last_ts, name, completed)

    return completed


async def run_detection(target_date: date, db: Database) -> list[LoadEvent]:
    """Detect events for *target_date* and upsert results to ``load_events``.

    Designed to run as a daily scheduled job over the prior day's readings.
    Re-running for the same date is safe: events are only inserted once
    (matched on ``started_at`` + ``load_name`` + ``source='inferred'``).
    """
    ts_from = datetime(
        target_date.year, target_date.month, target_date.day, tzinfo=UTC
    ).isoformat()
    ts_to = (
        datetime(target_date.year, target_date.month, target_date.day, tzinfo=UTC)
        + timedelta(days=1)
    ).isoformat()

    rows = await db.fetch(
        "SELECT ts, solar_w, grid_w, load_w FROM solar_readings "
        "WHERE ts >= ? AND ts < ? ORDER BY ts",
        ts_from,
        ts_to,
    )
    readings = [_row_to_reading(r) for r in rows]
    events = detect_events(readings)

    for event in events:
        await _upsert_event(event, db)

    logger.info(
        "run_detection date=%s readings=%d events=%d",
        target_date,
        len(readings),
        len(events),
    )
    return events


# ---------------------------------------------------------------------------
# Private helpers — detection logic
# ---------------------------------------------------------------------------


def _compute_baseline(readings: list[SolarReading]) -> float:
    """Return the median ``load_w`` during overnight hours (11 pm – 6 am).

    Falls back to the overall median when no overnight readings are present.
    """
    overnight = [r.load_w for r in readings if r.ts.hour >= 23 or r.ts.hour < 6]
    values = overnight if overnight else [r.load_w for r in readings]
    return float(statistics.median(values)) if values else 0.0


def _active_profiles(net_load: float) -> list[str]:
    """Return profile names that explain *net_load*, using greedy tightest-first matching.

    After a profile is matched, its expected midpoint wattage is subtracted
    from the remaining load and the remaining is checked against further
    profiles.  This allows EV + hot water to be detected simultaneously.
    """
    active: list[str] = []
    remaining = net_load
    changed = True
    while changed and remaining > 0:
        changed = False
        for profile in _PROFILES:
            name = profile["name"]
            if name in active:
                continue
            lo = float(profile["min_watts"] - profile["tolerance"])
            hi = float(profile["max_watts"] + profile["tolerance"])
            if lo <= remaining <= hi:
                active.append(name)
                midpoint = (profile["min_watts"] + profile["max_watts"]) / 2.0
                remaining -= midpoint
                changed = True
                break  # restart with updated remaining
    return active


def _maybe_record_event(
    state: dict,
    ended_at: datetime,
    name: str,
    completed: list[LoadEvent],
) -> None:
    """Append a ``LoadEvent`` to *completed* if the event meets min_duration."""
    profile = next(p for p in KNOWN_LOADS if p["name"] == name)
    event_readings: list[SolarReading] = state["readings"]
    duration_min = len(event_readings) * INTERVAL_MINUTES
    if duration_min < profile["min_duration_min"]:
        return

    avg_w = int(sum(r.load_w for r in event_readings) / len(event_readings))
    kwh_val = sum(r.load_w * INTERVAL_MINUTES / 60.0 / 1000.0 for r in event_readings)
    completed.append(
        LoadEvent(
            id=None,
            started_at=state["started_at"],
            ended_at=ended_at,
            load_name=name,
            avg_watts=avg_w,
            kwh=Decimal(str(round(kwh_val, 3))),
            confidence=Decimal("0.7"),
            source="inferred",
        )
    )


# ---------------------------------------------------------------------------
# Private helpers — DB
# ---------------------------------------------------------------------------


def _row_to_reading(row: object) -> SolarReading:
    ts_raw = str(row["ts"])  # type: ignore[index]
    if ts_raw.endswith("Z"):
        ts_raw = ts_raw[:-1] + "+00:00"
    ts = datetime.fromisoformat(ts_raw)
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=UTC)
    return SolarReading(
        ts=ts,
        solar_w=int(row["solar_w"]),  # type: ignore[index]
        grid_w=int(row["grid_w"]),  # type: ignore[index]
        load_w=int(row["load_w"]),  # type: ignore[index]
    )


async def _upsert_event(event: LoadEvent, db: Database) -> None:
    """Insert *event* only if no inferred event with the same start/name exists."""
    existing = await db.fetchval(
        "SELECT id FROM load_events "
        "WHERE started_at = ? AND load_name = ? AND source = 'inferred'",
        event.started_at.isoformat(),
        event.load_name,
    )
    if existing is not None:
        return

    await db.execute(
        """
        INSERT INTO load_events
            (started_at, ended_at, load_name, avg_watts, kwh, confidence, source)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        event.started_at.isoformat(),
        event.ended_at.isoformat() if event.ended_at is not None else None,
        event.load_name,
        event.avg_watts,
        str(event.kwh) if event.kwh is not None else None,
        str(event.confidence) if event.confidence is not None else None,
        event.source,
    )
