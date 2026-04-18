"""iSolarCloud polling loop.

Runs as the entrypoint for the ``candela-collector`` container. Uses
APScheduler to fire ``poll_once`` every ``ISOLARCLOUD_POLL_INTERVAL_SECONDS``
seconds (default 300 / 5 minutes).

iSolarCloud data note
---------------------
Data from iSolarCloud refreshes approximately every 5 minutes. The poller
calls ``fetch_current_reading()`` on each tick to get the latest real-time
snapshot and upserts it. Using ``ON CONFLICT DO UPDATE`` means re-fetching
the same timestamp is idempotent.

Poll rate
---------
Keep at 300 seconds. Polling more frequently than the iSolarCloud data
refresh rate wastes API calls and risks rate limiting.
"""

import asyncio
import logging
from datetime import datetime, timezone

from apscheduler.schedulers.asyncio import AsyncIOScheduler

from candela.collector.aemo import fetch_month, store_prices
from candela.collector.isolarcloud import ISolarCloudClient, InverterReading
from candela.config import get_settings
from candela.db import Database

logger = logging.getLogger(__name__)


async def poll_once(client: ISolarCloudClient, db: Database) -> None:
    """Fetch the current real-time reading from iSolarCloud and upsert to solar_readings.

    Parameters
    ----------
    client:
        Configured ``ISolarCloudClient`` instance.
    db:
        Connected ``Database`` instance.
    """
    try:
        reading = await client.fetch_current_reading()
    except Exception:
        logger.error("Failed to fetch current reading from iSolarCloud", exc_info=True)
        return

    await _upsert_reading(db, reading)
    logger.info(
        "Polled iSolarCloud: solar=%dW grid=%+dW load=%dW ts=%s",
        reading.solar_w,
        reading.grid_w,
        reading.load_w,
        reading.ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
    )


async def _upsert_reading(db: Database, reading: InverterReading) -> None:
    ts = reading.ts.strftime("%Y-%m-%dT%H:%M:%SZ")
    await db.execute(
        """
        INSERT INTO solar_readings
            (ts, solar_w, grid_w, load_w, daily_yield_kwh, total_yield_kwh, inverter_temp_c)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (ts) DO UPDATE SET
            solar_w = excluded.solar_w,
            grid_w = excluded.grid_w,
            load_w = excluded.load_w,
            daily_yield_kwh = excluded.daily_yield_kwh,
            total_yield_kwh = excluded.total_yield_kwh,
            inverter_temp_c = excluded.inverter_temp_c
        """,
        ts,
        reading.solar_w,
        reading.grid_w,
        reading.load_w,
        reading.daily_yield_kwh,
        reading.total_yield_kwh,
        reading.inverter_temp_c,
    )


async def fetch_aemo_prices(db: Database, region: str) -> None:
    """Fetch current month's AEMO prices and upsert into aemo_trading_prices."""
    today = datetime.now(tz=timezone.utc)
    try:
        prices = await fetch_month(year=today.year, month=today.month, region=region)
        await store_prices(prices, db)
    except Exception:
        logger.error("Failed to fetch AEMO prices", exc_info=True)


async def _run() -> None:
    settings = get_settings()
    db = Database(settings.database_url)
    await db.connect()

    client = ISolarCloudClient(
        app_key=settings.isolarcloud_app_key,
        access_key=settings.isolarcloud_access_key,
        username=settings.isolarcloud_username,
        password=settings.isolarcloud_password,
        base_url=settings.isolarcloud_base_url,
    )

    scheduler = AsyncIOScheduler()
    scheduler.add_job(
        poll_once,
        "interval",
        seconds=settings.isolarcloud_poll_interval_seconds,
        args=[client, db],
    )
    scheduler.add_job(
        fetch_aemo_prices,
        "interval",
        hours=24,
        args=[db, settings.aemo_region],
        next_run_time=datetime.now(tz=timezone.utc),  # run immediately on startup
    )
    scheduler.start()
    logger.info(
        "Poller started (interval=%ds)",
        settings.isolarcloud_poll_interval_seconds,
    )

    try:
        while True:
            await asyncio.sleep(3600)
    except KeyboardInterrupt, SystemExit:
        pass
    finally:
        scheduler.shutdown(wait=False)
        await db.disconnect()
        logger.info("Poller stopped")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    asyncio.run(_run())
