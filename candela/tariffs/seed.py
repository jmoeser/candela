"""Seed known tariff plans into the database.

Run once on first startup (or when adding new plans).  The function is
idempotent — plans are identified by name and skipped if already present.

Rates are approximate as at July 2025.  Update ``supply_charge_daily_cents``
and the rate values from your actual bill before using for cost comparison.

Plans seeded
------------
1. Engie Single Rate    — flat 30.48 c/kWh, 121.77 c/day supply, 5.0 c FiT
2. Energex TOU          — peak/shoulder/offpeak, 5.0 c FiT
3. Energex TOU + Demand — as above + 56.10 c/kW demand charge (4–9 pm)
4. AEMO Wholesale       — spot price proxy; adder configured via settings
"""

import json
import logging
from datetime import date

from candela.db import Database

logger = logging.getLogger(__name__)

_VALID_FROM = date(2025, 7, 1)

# ---------------------------------------------------------------------------
# Plan definitions
# ---------------------------------------------------------------------------

_PLANS: list[dict] = [
    {
        "name": "Engie Single Rate",
        "retailer": "Engie",
        "plan_type": "single_rate",
        "supply_charge_daily_cents": 121.77,
        "feed_in_tariff_cents": 5.0,
        "valid_from": _VALID_FROM,
        "notes": "Update from your actual bill. Rates approximate as at Jul 2025.",
        "rates": [
            {"rate_type": "flat", "cents_per_kwh": 30.48},
        ],
    },
    {
        "name": "Energex TOU (Tariff 12)",
        "retailer": "Energex",
        "plan_type": "tou",
        "supply_charge_daily_cents": 121.77,
        "feed_in_tariff_cents": 5.0,
        "valid_from": _VALID_FROM,
        "notes": (
            "Peak 4–9 pm all days; shoulder 7 am–4 pm + 9 pm–11 pm; "
            "offpeak 11 pm–7 am. Rates approximate as at Jul 2025."
        ),
        "rates": [
            {
                "rate_type": "peak",
                "cents_per_kwh": 45.18,
                "window_start": "16:00:00",
                "window_end": "21:00:00",
            },
            {
                "rate_type": "shoulder",
                "cents_per_kwh": 27.55,
                "window_start": "07:00:00",
                "window_end": "16:00:00",
            },
            {
                "rate_type": "shoulder",
                "cents_per_kwh": 27.55,
                "window_start": "21:00:00",
                "window_end": "23:00:00",
            },
            {
                "rate_type": "offpeak",
                "cents_per_kwh": 17.32,
            },
        ],
    },
    {
        "name": "Energex TOU + Demand",
        "retailer": "Energex",
        "plan_type": "demand",
        "supply_charge_daily_cents": 121.77,
        "feed_in_tariff_cents": 5.0,
        "valid_from": _VALID_FROM,
        "notes": (
            "TOU energy + demand charge on peak 4–9 pm window. "
            "Demand rate approximate as at Jul 2025. "
            "Check energex.com.au for current Tariff 22 rates."
        ),
        "rates": [
            {
                "rate_type": "peak",
                "cents_per_kwh": 45.18,
                "window_start": "16:00:00",
                "window_end": "21:00:00",
            },
            {
                "rate_type": "shoulder",
                "cents_per_kwh": 27.55,
                "window_start": "07:00:00",
                "window_end": "16:00:00",
            },
            {
                "rate_type": "shoulder",
                "cents_per_kwh": 27.55,
                "window_start": "21:00:00",
                "window_end": "23:00:00",
            },
            {
                "rate_type": "offpeak",
                "cents_per_kwh": 17.32,
            },
            {
                "rate_type": "demand",
                "cents_per_kw": 56.10,
                "demand_window_start": "16:00:00",
                "demand_window_end": "21:00:00",
            },
        ],
    },
    {
        "name": "AEMO Wholesale (QLD1)",
        "retailer": None,
        "plan_type": "wholesale",
        "supply_charge_daily_cents": 121.77,
        "feed_in_tariff_cents": None,
        "valid_from": _VALID_FROM,
        "notes": (
            "Proxy for Amber Electric: spot price + network/retail adder "
            "(WHOLESALE_ADDER_CENTS_KWH, default 18 c/kWh). "
            "Export credit = spot × 0.7 / 10 c/kWh."
        ),
        "rates": [],  # wholesale strategy uses AEMO prices, not rate rows
    },
]


async def seed_plans(db: Database) -> None:
    """Insert seed plans and their rates, skipping any that already exist by name."""
    for plan in _PLANS:
        existing = await db.fetchval(
            "SELECT id FROM tariff_plans WHERE name = ?", plan["name"]
        )
        if existing is not None:
            logger.debug("Tariff plan %r already exists — skipping", plan["name"])
            continue

        await db.execute(
            """
            INSERT INTO tariff_plans
                (name, retailer, plan_type, supply_charge_daily_cents,
                 feed_in_tariff_cents, valid_from, notes)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            plan["name"],
            plan.get("retailer"),
            plan["plan_type"],
            plan["supply_charge_daily_cents"],
            plan.get("feed_in_tariff_cents"),
            plan["valid_from"].isoformat(),
            plan.get("notes"),
        )

        plan_id = await db.fetchval(
            "SELECT id FROM tariff_plans WHERE name = ?", plan["name"]
        )

        for rate in plan.get("rates", []):
            await db.execute(
                """
                INSERT INTO tariff_rates
                    (plan_id, rate_type, cents_per_kwh, cents_per_kw,
                     window_start, window_end,
                     days_of_week, months,
                     demand_window_start, demand_window_end)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                plan_id,
                rate["rate_type"],
                rate.get("cents_per_kwh"),
                rate.get("cents_per_kw"),
                rate.get("window_start"),
                rate.get("window_end"),
                json.dumps(rate["days_of_week"]) if "days_of_week" in rate else None,
                json.dumps(rate["months"]) if "months" in rate else None,
                rate.get("demand_window_start"),
                rate.get("demand_window_end"),
            )

        logger.info("Seeded tariff plan %r (id=%s)", plan["name"], plan_id)
