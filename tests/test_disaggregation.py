"""Tests for load disaggregation — detector and reconciler.

Detection tests are pure (no DB).  Reconciler DB tests use in-memory SQLite.

Interval assumption
-------------------
All synthetic readings use 5-minute intervals (the system standard).
"""

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from candela.db import Database
from candela.tariffs.models import SolarReading


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _reading(ts: datetime, load_w: int, grid_w: int = 0, solar_w: int = 0) -> SolarReading:
    return SolarReading(ts=ts, solar_w=solar_w, grid_w=grid_w, load_w=load_w)


def _ts(hour: int, minute: int = 0, day: int = 1) -> datetime:
    return datetime(2026, 1, day, hour, minute, 0, tzinfo=UTC)


def _make_readings(
    start: datetime,
    load_w: int,
    count: int,
    interval_min: int = 5,
) -> list[SolarReading]:
    """Generate *count* readings spaced *interval_min* apart starting at *start*."""
    return [
        _reading(start + timedelta(minutes=i * interval_min), load_w)
        for i in range(count)
    ]


def _overnight_baseline(load_w: int = 300, count: int = 24) -> list[SolarReading]:
    """Return overnight readings at *load_w* to establish the baseline."""
    return _make_readings(_ts(2, 0), load_w=load_w, count=count)


async def _make_db() -> Database:
    """In-memory SQLite with the load_events table."""
    db = Database("sqlite+aiosqlite:///:memory:")
    await db.connect()
    await db.execute(
        """
        CREATE TABLE load_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            started_at TEXT NOT NULL,
            ended_at TEXT,
            load_name TEXT NOT NULL,
            avg_watts INTEGER,
            kwh NUMERIC,
            confidence NUMERIC,
            source TEXT NOT NULL
        )
        """
    )
    return db


async def _make_full_db() -> Database:
    """In-memory SQLite with both solar_readings and load_events tables."""
    db = await _make_db()
    await db.execute(
        """
        CREATE TABLE solar_readings (
            ts TEXT NOT NULL PRIMARY KEY,
            solar_w INTEGER NOT NULL,
            grid_w INTEGER NOT NULL,
            load_w INTEGER NOT NULL,
            daily_yield_kwh REAL,
            total_yield_kwh REAL,
            inverter_temp_c REAL
        )
        """
    )
    return db


async def _insert_solar_reading(db: Database, ts: str, load_w: int) -> None:
    await db.execute(
        "INSERT INTO solar_readings (ts, solar_w, grid_w, load_w) VALUES (?, ?, ?, ?)",
        ts, 0, load_w, load_w,
    )


# ---------------------------------------------------------------------------
# Detector — basic single-load detection
# ---------------------------------------------------------------------------


def test_detect_ev_charging_event() -> None:
    """A sustained 5000W load for 25 min (5 readings) is detected as ev_charging."""
    from candela.disaggregation.detector import detect_events

    # Overnight baseline at 300W (hour=2, which is < 6)
    baseline = _overnight_baseline()
    # EV charging: 5 readings × 5 min = 25 min >= 20 min threshold
    ev = _make_readings(_ts(14, 0), load_w=5000, count=5)
    end = [_reading(_ts(14, 25), load_w=300)]

    events = detect_events(baseline + ev + end)

    ev_events = [e for e in events if e.load_name == "ev_charging"]
    assert len(ev_events) == 1
    assert ev_events[0].source == "inferred"
    assert ev_events[0].kwh is not None and ev_events[0].kwh > 0
    assert ev_events[0].avg_watts is not None and ev_events[0].avg_watts > 0


def test_ev_below_min_duration_not_detected() -> None:
    """An EV load lasting only 15 min (< 20 min threshold) is not recorded."""
    from candela.disaggregation.detector import detect_events

    baseline = _overnight_baseline()
    # 3 readings × 5 min = 15 min < 20 min minimum
    short_ev = _make_readings(_ts(14, 0), load_w=5000, count=3)
    end = [_reading(_ts(14, 15), load_w=300)]

    events = detect_events(baseline + short_ev + end)

    assert not any(e.load_name == "ev_charging" for e in events)


def test_detect_hot_water_heatpump_event() -> None:
    """A sustained 1100W load for 55 min is detected as hot_water_heatpump."""
    from candela.disaggregation.detector import detect_events

    baseline = _overnight_baseline()
    # 11 readings × 5 min = 55 min >= 45 min minimum
    hw = _make_readings(_ts(6, 0), load_w=1100, count=11)
    end = [_reading(_ts(6, 55), load_w=300)]

    events = detect_events(baseline + hw + end)

    hw_events = [e for e in events if e.load_name == "hot_water_heatpump"]
    assert len(hw_events) == 1
    assert hw_events[0].source == "inferred"


def test_hot_water_heatpump_below_min_duration_not_detected() -> None:
    """Hot water heatpump load lasting only 40 min (< 45 min) is not recorded."""
    from candela.disaggregation.detector import detect_events

    baseline = _overnight_baseline()
    # 8 readings × 5 min = 40 min < 45 min
    short_hw = _make_readings(_ts(6, 0), load_w=1100, count=8)
    end = [_reading(_ts(6, 40), load_w=300)]

    events = detect_events(baseline + short_hw + end)

    assert not any(e.load_name == "hot_water_heatpump" for e in events)


def test_detect_hot_water_boost_event() -> None:
    """A 2700W load for 20 min (4 readings) is detected as hot_water_boost.

    2700W is within hot_water_boost range [2250W, 3250W] (2500±250+3000±250)
    and is NOT mis-classified as ev_charging because the tighter boost profile
    is checked first.
    """
    from candela.disaggregation.detector import detect_events

    baseline = _overnight_baseline()
    # 4 readings × 5 min = 20 min >= 15 min minimum
    boost = _make_readings(_ts(14, 0), load_w=2700, count=4)
    end = [_reading(_ts(14, 20), load_w=300)]

    events = detect_events(baseline + boost + end)

    boost_events = [e for e in events if e.load_name == "hot_water_boost"]
    assert len(boost_events) == 1
    # Must not also appear as EV
    assert not any(e.load_name == "ev_charging" for e in events)


def test_detect_ev_kwh_calculated() -> None:
    """Detected EV event has kwh = load_w × duration / 1000 (rounded to 3dp)."""
    from candela.disaggregation.detector import detect_events

    baseline = _overnight_baseline()
    # 6 readings × 5 min × 6000W = 6000 * (30/60) / 1000 = 3.000 kWh
    ev = _make_readings(_ts(14, 0), load_w=6000, count=6)
    end = [_reading(_ts(14, 30), load_w=300)]

    events = detect_events(baseline + ev + end)

    ev_events = [e for e in events if e.load_name == "ev_charging"]
    assert len(ev_events) == 1
    assert float(ev_events[0].kwh) == pytest.approx(3.0, rel=1e-3)


def test_detect_no_events_at_baseline() -> None:
    """Readings at baseline level produce no events."""
    from candela.disaggregation.detector import detect_events

    readings = _make_readings(_ts(0, 0), load_w=300, count=48)
    assert detect_events(readings) == []


def test_detect_empty_readings() -> None:
    """An empty readings list returns an empty events list."""
    from candela.disaggregation.detector import detect_events

    assert detect_events([]) == []


# ---------------------------------------------------------------------------
# Detector — overlap: EV + hot water simultaneously
# ---------------------------------------------------------------------------


def test_detect_ev_and_hot_water_overlap() -> None:
    """EV + hot water running simultaneously are both detected.

    Scenario
    --------
    - Phase 1 (30 min):  EV only at 5000W
    - Phase 2 (60 min):  EV + hot water heatpump at 6200W (5000 + 1200 above baseline)
    - Phase 3 (10 min):  EV only at 5000W
    - End:               back to baseline

    Overlap resolution
    ------------------
    net_load during phase 2 = 6200 - 300 = 5900W.
    Specificity-first matching: hot_water_boost [2250, 3250]? No.
    hot_water_heatpump [400, 1900]? No.  ev_charging [1300, 8500]? Yes.
    Subtract EV midpoint (4900W): remaining = 1000W.
    hot_water_heatpump [400, 1900]? Yes → both detected.
    """
    from candela.disaggregation.detector import detect_events

    baseline = _overnight_baseline()

    # Phase 1: EV only (6 readings = 30 min)
    ev_only = _make_readings(_ts(14, 0), load_w=5000, count=6)
    # Phase 2: EV + hot water heatpump (12 readings = 60 min)
    combined = _make_readings(_ts(14, 30), load_w=6200, count=12)
    # Phase 3: EV only (2 readings = 10 min)
    ev_tail = _make_readings(_ts(15, 30), load_w=5000, count=2)
    # End
    end = [_reading(_ts(15, 40), load_w=300)]

    events = detect_events(baseline + ev_only + combined + ev_tail + end)

    ev_events = [e for e in events if e.load_name == "ev_charging"]
    hw_events = [e for e in events if e.load_name == "hot_water_heatpump"]

    assert len(ev_events) == 1, f"Expected 1 EV event, got {len(ev_events)}"
    assert len(hw_events) == 1, f"Expected 1 HW event, got {len(hw_events)}"

    # EV event spans the whole charging session; HW only the overlap window
    ev_dur = (ev_events[0].ended_at - ev_events[0].started_at).total_seconds()
    hw_dur = (hw_events[0].ended_at - hw_events[0].started_at).total_seconds()
    assert ev_dur > hw_dur


# ---------------------------------------------------------------------------
# Reconciler — confidence scoring (pure, no DB)
# ---------------------------------------------------------------------------


def test_reconciler_typical_ev_event_high_confidence() -> None:
    """An EV event at typical charging time with typical duration scores > 0.5."""
    from candela.disaggregation.models import LoadEvent
    from candela.disaggregation.reconciler import score_confidence

    event = LoadEvent(
        id=None,
        started_at=_ts(18, 0),   # 6pm — typical EV charging time
        ended_at=_ts(20, 0),     # 2 hours — typical session
        load_name="ev_charging",
        avg_watts=5000,
        kwh=Decimal("10.0"),
        confidence=Decimal("0.7"),
        source="inferred",
    )
    assert float(score_confidence(event)) > 0.5


def test_reconciler_atypical_ev_event_lower_confidence() -> None:
    """An EV event at 3am with short duration scores lower than a typical event."""
    from candela.disaggregation.models import LoadEvent
    from candela.disaggregation.reconciler import score_confidence

    unusual = LoadEvent(
        id=None,
        started_at=_ts(3, 0),
        ended_at=_ts(3, 25),     # 25 min — below 30 min typical
        load_name="ev_charging",
        avg_watts=5000,
        kwh=Decimal("2.1"),
        confidence=Decimal("0.7"),
        source="inferred",
    )
    typical = LoadEvent(
        id=None,
        started_at=_ts(18, 0),
        ended_at=_ts(20, 0),
        load_name="ev_charging",
        avg_watts=5000,
        kwh=Decimal("10.0"),
        confidence=Decimal("0.7"),
        source="inferred",
    )
    assert float(score_confidence(unusual)) < float(score_confidence(typical))


def test_reconciler_hot_water_morning_high_confidence() -> None:
    """Hot water heatpump in the morning scores > 0.5."""
    from candela.disaggregation.models import LoadEvent
    from candela.disaggregation.reconciler import score_confidence

    event = LoadEvent(
        id=None,
        started_at=_ts(6, 0),
        ended_at=_ts(8, 0),      # 2 hours
        load_name="hot_water_heatpump",
        avg_watts=1100,
        kwh=Decimal("2.2"),
        confidence=Decimal("0.7"),
        source="inferred",
    )
    assert float(score_confidence(event)) > 0.5


def test_reconciler_history_boosts_confidence() -> None:
    """Confirmed history at similar times increases the confidence score."""
    from candela.disaggregation.models import LoadEvent
    from candela.disaggregation.reconciler import score_confidence

    event = LoadEvent(
        id=None,
        started_at=_ts(18, 0),
        ended_at=_ts(20, 0),
        load_name="ev_charging",
        avg_watts=5000,
        kwh=Decimal("10.0"),
        confidence=Decimal("0.7"),
        source="inferred",
    )
    # Manually confirmed history at a similar hour on a different day
    history = [
        LoadEvent(
            id=1,
            started_at=_ts(18, 30, day=2),
            ended_at=_ts(20, 30, day=2),
            load_name="ev_charging",
            avg_watts=5000,
            kwh=Decimal("10.0"),
            confidence=Decimal("1.0"),
            source="manual",
        )
    ]
    assert float(score_confidence(event, history=history)) > float(score_confidence(event))


def test_reconciler_score_bounded() -> None:
    """score_confidence always returns a value in [0.0, 1.0]."""
    from candela.disaggregation.models import LoadEvent
    from candela.disaggregation.reconciler import score_confidence

    # Build a history that would push score well above 1.0 without clamping
    event = LoadEvent(
        id=None,
        started_at=_ts(18, 0),
        ended_at=_ts(22, 0),
        load_name="ev_charging",
        avg_watts=5000,
        kwh=Decimal("20.0"),
        confidence=Decimal("0.7"),
        source="inferred",
    )
    history = [
        LoadEvent(
            id=i,
            started_at=_ts(18, 0, day=i + 2),
            ended_at=_ts(22, 0, day=i + 2),
            load_name="ev_charging",
            avg_watts=5000,
            kwh=Decimal("20.0"),
            confidence=Decimal("1.0"),
            source="manual",
        )
        for i in range(10)
    ]
    score = float(score_confidence(event, history=history))
    assert 0.0 <= score <= 1.0


# ---------------------------------------------------------------------------
# Reconciler — DB (confirm / reject)
# ---------------------------------------------------------------------------


async def _insert_event(db: Database, *, load_name: str = "ev_charging") -> int:
    await db.execute(
        """
        INSERT INTO load_events
            (started_at, ended_at, load_name, avg_watts, kwh, confidence, source)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        "2026-01-01T18:00:00+00:00",
        "2026-01-01T20:00:00+00:00",
        load_name,
        5000,
        "10.000",
        "0.700",
        "inferred",
    )
    row_id = await db.fetchval(
        "SELECT id FROM load_events WHERE load_name = ? ORDER BY id DESC LIMIT 1",
        load_name,
    )
    assert row_id is not None
    return int(row_id)


async def test_confirm_event_sets_manual_and_full_confidence() -> None:
    """confirm_event sets source='manual' and confidence=1.0."""
    from candela.disaggregation.reconciler import confirm_event

    db = await _make_db()
    event_id = await _insert_event(db)
    await confirm_event(event_id, db)

    row = await db.fetchrow(
        "SELECT source, confidence FROM load_events WHERE id = ?", event_id
    )
    assert row is not None
    assert row["source"] == "manual"
    assert float(row["confidence"]) == pytest.approx(1.0)


async def test_reject_event_sets_manual_and_zero_confidence() -> None:
    """reject_event sets source='manual' and confidence=0.0."""
    from candela.disaggregation.reconciler import reject_event

    db = await _make_db()
    event_id = await _insert_event(db)
    await reject_event(event_id, db)

    row = await db.fetchrow(
        "SELECT source, confidence FROM load_events WHERE id = ?", event_id
    )
    assert row is not None
    assert row["source"] == "manual"
    assert float(row["confidence"]) == pytest.approx(0.0)


async def test_confirm_does_not_affect_other_events() -> None:
    """confirm_event only updates the targeted event, not others in the table."""
    from candela.disaggregation.reconciler import confirm_event

    db = await _make_db()
    id1 = await _insert_event(db, load_name="ev_charging")
    id2 = await _insert_event(db, load_name="hot_water_heatpump")

    await confirm_event(id1, db)

    row2 = await db.fetchrow("SELECT source FROM load_events WHERE id = ?", id2)
    assert row2 is not None
    assert row2["source"] == "inferred"  # unchanged


# ---------------------------------------------------------------------------
# run_detection — DB integration
# ---------------------------------------------------------------------------


async def test_run_detection_writes_to_db() -> None:
    """run_detection fetches readings, detects events, and writes to load_events."""
    from datetime import date

    from candela.disaggregation.detector import run_detection

    db = await _make_full_db()

    # Overnight baseline (hour=0, which is < 6 → used for baseline)
    for i in range(12):
        ts = f"2026-01-01T00:{i * 5:02d}:00+00:00"
        await _insert_solar_reading(db, ts, load_w=300)

    # EV charging: 5 readings × 5 min = 25 min >= 20 min
    for i in range(5):
        ts = f"2026-01-01T14:{i * 5:02d}:00+00:00"
        await _insert_solar_reading(db, ts, load_w=5000)

    # Return to baseline
    await _insert_solar_reading(db, "2026-01-01T14:25:00+00:00", load_w=300)

    events = await run_detection(date(2026, 1, 1), db)

    assert len(events) >= 1
    assert any(e.load_name == "ev_charging" for e in events)

    count = await db.fetchval(
        "SELECT COUNT(*) FROM load_events WHERE load_name = 'ev_charging'"
    )
    assert int(count) >= 1


async def test_run_detection_idempotent() -> None:
    """Running detection twice for the same date does not create duplicate events."""
    from datetime import date

    from candela.disaggregation.detector import run_detection

    db = await _make_full_db()

    for i in range(12):
        ts = f"2026-01-01T00:{i * 5:02d}:00+00:00"
        await _insert_solar_reading(db, ts, load_w=300)

    for i in range(5):
        ts = f"2026-01-01T14:{i * 5:02d}:00+00:00"
        await _insert_solar_reading(db, ts, load_w=5000)

    await _insert_solar_reading(db, "2026-01-01T14:25:00+00:00", load_w=300)

    await run_detection(date(2026, 1, 1), db)
    count_after_first = await db.fetchval("SELECT COUNT(*) FROM load_events")

    await run_detection(date(2026, 1, 1), db)
    count_after_second = await db.fetchval("SELECT COUNT(*) FROM load_events")

    assert int(count_after_second) == int(count_after_first)
