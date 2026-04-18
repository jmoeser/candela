"""AEMO wholesale price fetcher.

Downloads AEMO price-and-demand CSV files for a given month and region,
parses the 5-minute interval spot prices, and stores them in
``aemo_trading_prices``.

AEMO data format
----------------
Files are published at:

    https://www.aemo.com.au/aemo/data/nem/priceanddemand/
      PRICE_AND_DEMAND_{year}{month:02d}_{region}.csv

Each file is a plain CSV with a header row and columns:
    REGION, SETTLEMENTDATE, TOTALDEMAND, RRP, PERIODTYPE

Filter logic:
- Only keep rows where ``PERIODTYPE == "TRADE"``
- ``SETTLEMENTDATE`` is the **end** of the 5-minute interval; subtract
  5 minutes to get ``interval_start``
- ``RRP`` is the spot price in $/MWh (can be negative)
- Date format is ``YYYY/MM/DD HH:MM:SS``

Usage
-----
    # Fetch and store current month's AEMO data (run daily via scheduler)
    uv run python -m candela.collector.aemo
"""

import asyncio
import csv
import io
import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import httpx

from candela.config import get_settings
from candela.db import Database

logger = logging.getLogger(__name__)

_AEMO_BASE = (
    "https://www.aemo.com.au/aemo/data/nem/priceanddemand/"
    "PRICE_AND_DEMAND_{year}{month:02d}_{region}.csv"
)

_SETTLEMENT_FMT = "%Y/%m/%d %H:%M:%S"
_FIVE_MIN = timedelta(minutes=5)


@dataclass
class AemoPrice:
    interval_start: datetime  # UTC, start of the 5-min trading interval
    interval_end: datetime  # UTC, end of the 5-min trading interval
    rrp_per_mwh: float  # Regional reference price in $/MWh (may be negative)
    region: str  # NEM region identifier, e.g. "QLD1"


def parse_price_demand_csv(
    stream: io.TextIOBase, *, region: str = "QLD1"
) -> list[AemoPrice]:
    """Parse an AEMO PRICE_AND_DEMAND CSV stream and return price records.

    Only ``TRADE`` rows are returned. ``SETTLEMENTDATE`` (interval end) is
    shifted back 5 minutes to produce ``interval_start``.

    Parameters
    ----------
    stream:
        Text stream of the CSV file content.
    region:
        NEM region to filter on (default ``"QLD1"``).
    """
    prices: list[AemoPrice] = []

    reader = csv.DictReader(stream)
    for row in reader:
        if row.get("PERIODTYPE", "").strip() != "TRADE":
            continue
        if row.get("REGION", "").strip() != region:
            continue

        try:
            settlement_dt = datetime.strptime(
                row["SETTLEMENTDATE"].strip(), _SETTLEMENT_FMT
            ).replace(tzinfo=UTC)
            rrp = float(row["RRP"].strip())
        except (ValueError, KeyError) as exc:
            logger.warning("Skipping malformed AEMO row: %s — %s", row, exc)
            continue

        prices.append(
            AemoPrice(
                interval_start=settlement_dt - _FIVE_MIN,
                interval_end=settlement_dt,
                rrp_per_mwh=rrp,
                region=region,
            )
        )

    return prices


async def fetch_month(
    *, year: int, month: int, region: str = "QLD1"
) -> list[AemoPrice]:
    """Download and parse the PRICE_AND_DEMAND file for a given month.

    Parameters
    ----------
    year, month:
        The calendar month to fetch.
    region:
        NEM region to filter on.

    Raises
    ------
    httpx.HTTPStatusError
        If the AEMO server returns a non-2xx response.
    """
    url = _AEMO_BASE.format(year=year, month=month, region=region)
    logger.info("Fetching AEMO PRICE_AND_DEMAND: %s", url)

    async with httpx.AsyncClient(timeout=60.0) as client:
        response = await client.get(url)
        response.raise_for_status()

    prices = parse_price_demand_csv(io.StringIO(response.text), region=region)
    logger.info(
        "Parsed %d %s price records for %d-%02d", len(prices), region, year, month
    )
    return prices


async def store_prices(prices: list[AemoPrice], db: Database) -> int:
    """Upsert a list of ``AemoPrice`` records into ``aemo_trading_prices``.

    Returns the number of records processed.
    """
    for price in prices:
        start = price.interval_start.strftime("%Y-%m-%dT%H:%M:%SZ")
        end = price.interval_end.strftime("%Y-%m-%dT%H:%M:%SZ")
        await db.execute(
            """
            INSERT INTO aemo_trading_prices (interval_start, interval_end, rrp_per_mwh, region)
            VALUES (?, ?, ?, ?)
            ON CONFLICT (interval_start, region) DO UPDATE SET
                interval_end = excluded.interval_end,
                rrp_per_mwh  = excluded.rrp_per_mwh
            """,
            start,
            end,
            price.rrp_per_mwh,
            price.region,
        )

    logger.info("Stored %d AEMO price records", len(prices))
    return len(prices)


async def _main() -> None:
    from datetime import date

    settings = get_settings()
    db = Database(settings.database_url)
    await db.connect()

    try:
        today = date.today()
        prices = await fetch_month(
            year=today.year, month=today.month, region=settings.aemo_region
        )
        await store_prices(prices, db)
    finally:
        await db.disconnect()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    asyncio.run(_main())
