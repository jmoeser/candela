"""AEMO wholesale price fetcher.

Downloads AEMO TRADINGPRICE CSV zip files for a given month and region,
parses the 30-minute interval spot prices, and stores them in
``aemo_trading_prices``.

AEMO data format
----------------
Files are published at:

    https://www.nemweb.com.au/Data_Archive/Wholesale_Electricity/MMSDM/
      {year}/MMSDM_{year}_{month:02d}/MMSDM_Historical_Data_SQLLoader/DATA/
      PUBLIC_DVD_TRADINGPRICE_{year}{month:02d}010000.zip

Each zip contains a single CSV with multiple interleaved record types:
- ``C`` rows: comments/metadata — skip
- ``I`` rows: column headers — use to find field positions
- ``D`` rows: data records — only these carry price data

Filter logic:
- Only process rows where column index 0 == ``'D'``
- Only keep rows where ``REGIONID == region`` (default ``"QLD1"``)
- ``SETTLEMENTDATE`` is the **end** of the 30-minute interval; subtract
  30 minutes to get ``interval_start``
- ``RRP`` is the spot price in $/MWh (can be negative)

Usage
-----
    # Fetch and store previous month's AEMO data (run daily via scheduler)
    uv run python -m candela.collector.aemo
"""

import asyncio
import csv
import io
import logging
import zipfile
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import httpx

from candela.config import get_settings
from candela.db import Database

logger = logging.getLogger(__name__)

_AEMO_BASE = (
    "https://www.nemweb.com.au/Data_Archive/Wholesale_Electricity/MMSDM/"
    "{year}/MMSDM_{year}_{month:02d}/MMSDM_Historical_Data_SQLLoader/DATA/"
    "PUBLIC_DVD_TRADINGPRICE_{year}{month:02d}010000.zip"
)

_SETTLEMENT_FMT = "%Y-%m-%d %H:%M:%S"
_HALF_HOUR = timedelta(minutes=30)


@dataclass
class AemoPrice:
    interval_start: datetime  # UTC, start of the 30-min trading interval
    interval_end: datetime    # UTC, end of the 30-min trading interval
    rrp_per_mwh: float        # Regional reference price in $/MWh (may be negative)
    region: str               # NEM region identifier, e.g. "QLD1"


def parse_tradingprice_csv(stream: io.TextIOBase, *, region: str = "QLD1") -> list[AemoPrice]:
    """Parse an AEMO TRADINGPRICE CSV stream and return filtered price records.

    Only ``D`` (data) rows matching ``region`` are returned. The
    ``SETTLEMENTDATE`` (interval end) is shifted back 30 minutes to produce
    ``interval_start``.

    Parameters
    ----------
    stream:
        Text stream of the extracted CSV file content.
    region:
        NEM region to filter on (default ``"QLD1"``).
    """
    prices: list[AemoPrice] = []
    headers: list[str] | None = None
    settlement_idx: int | None = None
    regionid_idx: int | None = None
    rrp_idx: int | None = None

    reader = csv.reader(stream)
    for row in reader:
        if not row:
            continue

        row_type = row[0].strip()

        if row_type == "I":
            # Header row — locate the columns we need
            headers = [h.strip() for h in row]
            try:
                settlement_idx = headers.index("SETTLEMENTDATE")
                regionid_idx = headers.index("REGIONID")
                rrp_idx = headers.index("RRP")
            except ValueError as exc:
                logger.warning("AEMO CSV header missing expected column: %s", exc)
                return []
            continue

        if row_type != "D":
            continue

        if headers is None or settlement_idx is None:
            continue

        if row[regionid_idx].strip() != region:
            continue

        try:
            settlement_dt = datetime.strptime(
                row[settlement_idx].strip(), _SETTLEMENT_FMT
            ).replace(tzinfo=UTC)
            rrp = float(row[rrp_idx].strip())
        except (ValueError, IndexError) as exc:
            logger.warning("Skipping malformed AEMO row: %s — %s", row, exc)
            continue

        prices.append(
            AemoPrice(
                interval_start=settlement_dt - _HALF_HOUR,
                interval_end=settlement_dt,
                rrp_per_mwh=rrp,
                region=region,
            )
        )

    return prices


async def fetch_month(*, year: int, month: int, region: str = "QLD1") -> list[AemoPrice]:
    """Download and parse the TRADINGPRICE file for a given month.

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
    url = _AEMO_BASE.format(year=year, month=month)
    logger.info("Fetching AEMO TRADINGPRICE: %s", url)

    async with httpx.AsyncClient(timeout=60.0) as client:
        response = await client.get(url)
        response.raise_for_status()

    with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
        csv_name = next(n for n in zf.namelist() if n.upper().endswith(".CSV"))
        with zf.open(csv_name) as f:
            text = f.read().decode("utf-8", errors="replace")

    prices = parse_tradingprice_csv(io.StringIO(text), region=region)
    logger.info("Parsed %d %s price records for %d-%02d", len(prices), region, year, month)
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
        # Fetch previous month (AEMO publishes historical files with a short lag)
        if today.month == 1:
            year, month = today.year - 1, 12
        else:
            year, month = today.year, today.month - 1

        prices = await fetch_month(year=year, month=month, region=settings.aemo_region)
        await store_prices(prices, db)
    finally:
        await db.disconnect()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    asyncio.run(_main())
