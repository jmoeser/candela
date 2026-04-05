"""iSolarCloud CSV backfill importer.

Imports historical data from an iSolarCloud CSV export into ``solar_readings``.

Expected CSV format
-------------------
The iSolarCloud "Plant Details" export (5-minute resolution) should contain
the following columns (exact header names matter):

    Time,Generation Power(kW),Grid Power(kW),Load Power(kW),
    Daily Yield(kWh),Total Yield(kWh),Internal Temperature(℃)

- ``Time``: local timestamp as ``YYYY-MM-DD HH:MM`` — treated as **UTC**
  (iSolarCloud stores data in the configured plant timezone; adjust your
  system clock or export settings so they match UTC before running this).
- ``Generation Power(kW)``: PV output in kW → stored as watts.
- ``Grid Power(kW)``: positive = import, negative = export; in kW → watts.
- ``Load Power(kW)``: house consumption in kW → watts.
- Optional columns: ``Daily Yield(kWh)``, ``Total Yield(kWh)``,
  ``Internal Temperature(℃)`` — empty values are stored as NULL.

Usage
-----
    uv run python -m candela.collector.backfill --file export.csv
"""

import argparse
import asyncio
import csv
import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TextIO

from candela.config import get_settings
from candela.db import Database

logger = logging.getLogger(__name__)

_TIME_COL = "Time"
_SOLAR_COL = "Generation Power(kW)"
_GRID_COL = "Grid Power(kW)"
_LOAD_COL = "Load Power(kW)"
_DAILY_YIELD_COL = "Daily Yield(kWh)"
_TOTAL_YIELD_COL = "Total Yield(kWh)"
_TEMP_COL = "Internal Temperature(℃)"

_REQUIRED_COLS = (_TIME_COL, _SOLAR_COL, _GRID_COL, _LOAD_COL)


@dataclass
class BackfillRow:
    ts: datetime
    solar_w: int
    grid_w: int
    load_w: int
    daily_yield_kwh: float | None
    total_yield_kwh: float | None
    inverter_temp_c: float | None


def _kw_to_w(value: str) -> int:
    return round(float(value) * 1000.0)


def _opt_float(value: str) -> float | None:
    stripped = value.strip()
    if not stripped:
        return None
    try:
        return float(stripped)
    except ValueError:
        return None


def parse_csv(stream: TextIO) -> list[BackfillRow]:
    """Parse an iSolarCloud CSV stream into a list of ``BackfillRow`` objects.

    Parameters
    ----------
    stream:
        Any readable text stream (file, ``io.StringIO``, etc.).

    Raises
    ------
    ValueError
        If a required column is missing from the header.
    """
    reader = csv.DictReader(stream)
    if reader.fieldnames is None:
        return []

    for col in _REQUIRED_COLS:
        if col not in reader.fieldnames:
            raise ValueError(
                f"Required column '{col}' not found in CSV. "
                f"Found columns: {list(reader.fieldnames)}"
            )

    rows: list[BackfillRow] = []
    for lineno, record in enumerate(reader, start=2):
        # Skip blank lines (DictReader may yield empty rows)
        if not any(v.strip() for v in record.values()):
            continue

        try:
            ts = datetime.strptime(record[_TIME_COL].strip(), "%Y-%m-%d %H:%M").replace(
                tzinfo=UTC
            )
            row = BackfillRow(
                ts=ts,
                solar_w=_kw_to_w(record[_SOLAR_COL]),
                grid_w=_kw_to_w(record[_GRID_COL]),
                load_w=_kw_to_w(record[_LOAD_COL]),
                daily_yield_kwh=_opt_float(record.get(_DAILY_YIELD_COL, "")),
                total_yield_kwh=_opt_float(record.get(_TOTAL_YIELD_COL, "")),
                inverter_temp_c=_opt_float(record.get(_TEMP_COL, "")),
            )
            rows.append(row)
        except (ValueError, KeyError) as exc:
            logger.warning("Skipping malformed row at line %d: %s", lineno, exc)

    return rows


async def import_csv(rows: list[BackfillRow], db: Database) -> int:
    """Upsert a list of ``BackfillRow`` objects into ``solar_readings``.

    Returns the number of rows processed.
    """
    for row in rows:
        ts = row.ts.strftime("%Y-%m-%dT%H:%M:%SZ")
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
            row.solar_w,
            row.grid_w,
            row.load_w,
            row.daily_yield_kwh,
            row.total_yield_kwh,
            row.inverter_temp_c,
        )

    logger.info("Imported %d readings from CSV", len(rows))
    return len(rows)


async def _main(file_path: str) -> None:
    settings = get_settings()
    db = Database(settings.database_url)
    await db.connect()

    try:
        with open(file_path, encoding="utf-8") as f:
            rows = parse_csv(f)

        if not rows:
            logger.warning("No rows parsed from %s", file_path)
            return

        count = await import_csv(rows, db)
        logger.info("Backfill complete: %d readings imported from %s", count, file_path)
    finally:
        await db.disconnect()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    parser = argparse.ArgumentParser(description="Import iSolarCloud CSV into solar_readings")
    parser.add_argument("--file", required=True, help="Path to iSolarCloud CSV export")
    args = parser.parse_args()
    asyncio.run(_main(args.file))
