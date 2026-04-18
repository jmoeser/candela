"""Load event reconciler.

Provides two services:

1. **Confidence scoring** (pure, no DB) — scores inferred events based on
   duration plausibility, time-of-day consistency, and confirmed history.

2. **Manual confirmation / rejection** (DB) — users can confirm or reject
   detected events via the UI, which sets ``source='manual'`` and anchors
   ``confidence`` to 1.0 or 0.0.
"""

import logging
from decimal import Decimal

from candela.db import Database
from candela.disaggregation.models import LoadEvent

logger = logging.getLogger(__name__)

# Typical EV charging window (UTC hours; in Brisbane UTC+10, 16–23 UTC = 2am–9am AEST
# which is less typical, but this is a proxy and can be tuned after data collection)
_EV_TYPICAL_HOURS: frozenset[int] = frozenset(range(16, 24))  # 4pm–midnight UTC

# Typical hot water operation windows
_HW_MORNING_HOURS: frozenset[int] = frozenset(range(5, 10))  # 5am–9am UTC
_HW_AFTERNOON_HOURS: frozenset[int] = frozenset(range(14, 19))  # 2pm–6pm UTC

# Typical event durations in minutes
_EV_TYPICAL_MIN_DURATION = 30
_EV_TYPICAL_MAX_DURATION = 480  # 8 hours
_HW_TYPICAL_MIN_DURATION = 45
_HW_TYPICAL_MAX_DURATION = 180  # 3 hours


# ---------------------------------------------------------------------------
# Confidence scoring
# ---------------------------------------------------------------------------


def score_confidence(
    event: LoadEvent,
    history: list[LoadEvent] | None = None,
) -> Decimal:
    """Return a confidence score in [0.0, 1.0] for *event*.

    Scoring components
    ------------------
    - Base score: 0.5
    - Duration in typical range: +0.2
    - Starts at a typical time of day: +0.1
    - Confirmed history at similar hour: +0.15

    The result is clamped to [0.0, 1.0].
    """
    score = 0.5

    duration_min = 0.0
    if event.ended_at is not None:
        duration_min = (event.ended_at - event.started_at).total_seconds() / 60.0

    if event.load_name == "ev_charging":
        if _EV_TYPICAL_MIN_DURATION <= duration_min <= _EV_TYPICAL_MAX_DURATION:
            score += 0.2
        if event.started_at.hour in _EV_TYPICAL_HOURS:
            score += 0.1

    elif event.load_name in ("hot_water_heatpump", "hot_water_boost"):
        if _HW_TYPICAL_MIN_DURATION <= duration_min <= _HW_TYPICAL_MAX_DURATION:
            score += 0.2
        if (
            event.started_at.hour in _HW_MORNING_HOURS
            or event.started_at.hour in _HW_AFTERNOON_HOURS
        ):
            score += 0.1

    if history:
        confirmed = [
            h
            for h in history
            if h.load_name == event.load_name
            and h.source == "manual"
            and h.confidence is not None
            and float(h.confidence) >= 0.8
            and abs(h.started_at.hour - event.started_at.hour) <= 2
        ]
        if confirmed:
            score += 0.15

    return Decimal(str(min(round(score, 3), 1.0)))


# ---------------------------------------------------------------------------
# Manual confirmation / rejection
# ---------------------------------------------------------------------------


async def confirm_event(event_id: int, db: Database) -> None:
    """Mark *event_id* as manually confirmed.

    Sets ``source='manual'`` and ``confidence=1.0``.
    """
    await db.execute(
        "UPDATE load_events SET source = 'manual', confidence = 1.0 WHERE id = ?",
        event_id,
    )
    logger.info("confirm_event id=%d", event_id)


async def reject_event(event_id: int, db: Database) -> None:
    """Mark *event_id* as manually rejected.

    Sets ``source='manual'`` and ``confidence=0.0``.
    """
    await db.execute(
        "UPDATE load_events SET source = 'manual', confidence = 0.0 WHERE id = ?",
        event_id,
    )
    logger.info("reject_event id=%d", event_id)
