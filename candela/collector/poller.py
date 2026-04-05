"""Inverter polling loop.

Runs as the entrypoint for the ``candela-collector`` container. Uses
APScheduler to fire ``poll_once`` every ``INVERTER_POLL_INTERVAL_SECONDS``
seconds (default 300 / 5 minutes).

WiNet-S2 stability note
-----------------------
Do not poll faster than 30 seconds. The built-in WiNet-S2 module on the
SG5.0RS-ADA is known to drop offline under aggressive polling. The 300s
default is conservative and appropriate for this application.
"""

import asyncio
import logging

from apscheduler.schedulers.asyncio import AsyncIOScheduler

from candela.collector.inverter import SungrowClient
from candela.config import get_settings
from candela.db import Database

logger = logging.getLogger(__name__)

_CONSECUTIVE_FAILURE_THRESHOLD = 3


async def poll_once(
    client: SungrowClient,
    db: Database,
    consecutive_failures: list[int],
) -> None:
    """Fetch one reading and upsert it into solar_readings.

    Parameters
    ----------
    client:
        Configured ``SungrowClient`` instance.
    db:
        Connected ``Database`` instance.
    consecutive_failures:
        Single-element mutable list used as a shared counter across calls
        (avoids class state while remaining easily patchable in tests).
    """
    reading = await client.read()

    if reading is None:
        consecutive_failures[0] += 1
        count = consecutive_failures[0]
        if count >= _CONSECUTIVE_FAILURE_THRESHOLD:
            logger.error(
                "Inverter unreachable: %d consecutive poll failures",
                count,
            )
        else:
            logger.warning(
                "Inverter poll returned no data (consecutive failures: %d)",
                count,
            )
        return

    consecutive_failures[0] = 0
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
    logger.info(
        "Polled inverter: solar=%dW grid=%+dW load=%dW ts=%s",
        reading.solar_w,
        reading.grid_w,
        reading.load_w,
        ts,
    )


async def _run() -> None:
    settings = get_settings()
    db = Database(settings.database_url)
    await db.connect()

    client = SungrowClient(settings.inverter_host)
    consecutive_failures: list[int] = [0]

    scheduler = AsyncIOScheduler()
    scheduler.add_job(
        poll_once,
        "interval",
        seconds=settings.inverter_poll_interval_seconds,
        args=[client, db, consecutive_failures],
    )
    scheduler.start()
    logger.info(
        "Poller started (interval=%ds, host=%s)",
        settings.inverter_poll_interval_seconds,
        settings.inverter_host,
    )

    try:
        while True:
            await asyncio.sleep(3600)
    except (KeyboardInterrupt, SystemExit):
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
