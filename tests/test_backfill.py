"""Tests for the iSolarCloud CSV backfill importer (collector/backfill.py)."""

import io
import textwrap
from datetime import UTC, datetime

import pytest

from candela.collector.backfill import BackfillRow, parse_csv, import_csv
from candela.db import Database


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _csv(*lines: str) -> io.TextIOWrapper:
    """Build an in-memory text stream from the given lines."""
    content = "\n".join(lines) + "\n"
    return io.StringIO(content)


# ---------------------------------------------------------------------------
# parse_csv tests
# ---------------------------------------------------------------------------

SAMPLE_CSV = textwrap.dedent("""\
    Time,Generation Power(kW),Grid Power(kW),Load Power(kW),Daily Yield(kWh),Total Yield(kWh),Internal Temperature(℃)
    2025-01-15 06:00,3.500,1.200,4.700,18.5,1234.6,42.3
    2025-01-15 06:05,3.200,-0.800,2.400,18.9,1234.9,41.8
    2025-01-15 06:10,0.000,0.000,0.400,,1235.1,
""")


def test_parse_csv_returns_rows() -> None:
    rows = parse_csv(io.StringIO(SAMPLE_CSV))
    assert len(rows) == 3


def test_parse_csv_power_converted_to_watts() -> None:
    rows = parse_csv(io.StringIO(SAMPLE_CSV))
    assert rows[0].solar_w == 3500
    assert rows[0].grid_w == 1200
    assert rows[0].load_w == 4700


def test_parse_csv_negative_grid_power() -> None:
    rows = parse_csv(io.StringIO(SAMPLE_CSV))
    # second row: grid=-0.800 kW → -800 W (exporting)
    assert rows[1].grid_w == -800


def test_parse_csv_timestamps_are_utc() -> None:
    rows = parse_csv(io.StringIO(SAMPLE_CSV))
    assert rows[0].ts == datetime(2025, 1, 15, 6, 0, 0, tzinfo=UTC)
    assert rows[1].ts == datetime(2025, 1, 15, 6, 5, 0, tzinfo=UTC)


def test_parse_csv_optional_fields() -> None:
    rows = parse_csv(io.StringIO(SAMPLE_CSV))
    assert rows[0].daily_yield_kwh == pytest.approx(18.5)
    assert rows[0].total_yield_kwh == pytest.approx(1234.6)
    assert rows[0].inverter_temp_c == pytest.approx(42.3)


def test_parse_csv_empty_optional_fields_become_none() -> None:
    rows = parse_csv(io.StringIO(SAMPLE_CSV))
    # third row has empty daily_yield_kwh and inverter_temp_c
    assert rows[2].daily_yield_kwh is None
    assert rows[2].inverter_temp_c is None


def test_parse_csv_skips_header_only_file() -> None:
    rows = parse_csv(
        io.StringIO("Time,Generation Power(kW),Grid Power(kW),Load Power(kW)\n")
    )
    assert rows == []


def test_parse_csv_skips_blank_lines() -> None:
    csv_with_blanks = (
        "Time,Generation Power(kW),Grid Power(kW),Load Power(kW)\n"
        "\n"
        "2025-01-15 06:00,3.500,1.200,4.700\n"
        "\n"
    )
    rows = parse_csv(io.StringIO(csv_with_blanks))
    assert len(rows) == 1


def test_parse_csv_missing_required_column_raises() -> None:
    bad_csv = "Time,Grid Power(kW),Load Power(kW)\n2025-01-15 06:00,1.2,4.7\n"
    with pytest.raises(ValueError, match="Generation Power"):
        parse_csv(io.StringIO(bad_csv))


# ---------------------------------------------------------------------------
# import_csv tests
# ---------------------------------------------------------------------------

async def _make_db() -> Database:
    db = Database("sqlite+aiosqlite:///:memory:")
    await db.connect()
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


async def test_import_csv_writes_rows_to_db() -> None:
    db = await _make_db()
    rows = parse_csv(io.StringIO(SAMPLE_CSV))
    count = await import_csv(rows, db)

    assert count == 3
    stored = await db.fetch("SELECT * FROM solar_readings ORDER BY ts")
    assert len(stored) == 3
    assert stored[0]["solar_w"] == 3500
    assert stored[1]["grid_w"] == -800

    await db.disconnect()


async def test_import_csv_upserts_duplicates() -> None:
    db = await _make_db()
    rows = parse_csv(io.StringIO(SAMPLE_CSV))

    await import_csv(rows, db)
    # import again with modified solar_w for the first row
    modified = [rows[0].__class__(
        ts=rows[0].ts,
        solar_w=9999,
        grid_w=rows[0].grid_w,
        load_w=rows[0].load_w,
        daily_yield_kwh=rows[0].daily_yield_kwh,
        total_yield_kwh=rows[0].total_yield_kwh,
        inverter_temp_c=rows[0].inverter_temp_c,
    )] + rows[1:]
    await import_csv(modified, db)

    stored = await db.fetch("SELECT * FROM solar_readings ORDER BY ts")
    assert len(stored) == 3
    assert stored[0]["solar_w"] == 9999

    await db.disconnect()


async def test_import_csv_returns_zero_for_empty_list() -> None:
    db = await _make_db()
    count = await import_csv([], db)
    assert count == 0
    await db.disconnect()
