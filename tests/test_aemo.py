"""Tests for the AEMO wholesale price fetcher (collector/aemo.py)."""

import io
import textwrap
import zipfile
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from candela.collector.aemo import AemoPrice, parse_tradingprice_csv, fetch_month, store_prices
from candela.db import Database


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

# Real AEMO TRADINGPRICE CSV format:
# Row types: C (comment), I (header), D (data), others (ignored)
# Only 'D' rows with REGIONID == 'QLD1' are imported.
SAMPLE_CSV = textwrap.dedent("""\
    C,NEMP.WORLD,...
    I,TRADING,TRADINGPRICE,1,SETTLEMENTDATE,RUNNO,REGIONID,PERIODID,RRP,RAISE6SECRRP,RAISE60SECRRP,RAISE5MINRRP,RAISEREGRRP,LOWER6SECRRP,LOWER60SECRRP,LOWER5MINRRP,LOWERREGRRP,INVALIDFLAG,LASTCHANGED,RAISE1SECRRP,LOWER1SECRRP
    D,TRADING,TRADINGPRICE,1,2025-01-15 00:30:00,1,QLD1,1,45.25,0,0,0,0,0,0,0,0,,,0,0
    D,TRADING,TRADINGPRICE,1,2025-01-15 00:30:00,1,NSW1,1,43.12,0,0,0,0,0,0,0,0,,,0,0
    D,TRADING,TRADINGPRICE,1,2025-01-15 01:00:00,1,QLD1,2,52.80,0,0,0,0,0,0,0,0,,,0,0
    D,TRADING,TRADINGPRICE,1,2025-01-15 01:00:00,1,SA1,2,61.00,0,0,0,0,0,0,0,0,,,0,0
    D,TRADING,TRADINGPRICE,1,2025-01-15 01:30:00,1,QLD1,3,-12.50,0,0,0,0,0,0,0,0,,,0,0
""")


def _make_zip(csv_content: str) -> bytes:
    """Wrap CSV content in a zip file, as AEMO distributes."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("PUBLIC_DVD_TRADINGPRICE_202501010000.CSV", csv_content)
    return buf.getvalue()


# ---------------------------------------------------------------------------
# parse_tradingprice_csv
# ---------------------------------------------------------------------------

def test_parse_filters_qld1_only() -> None:
    prices = parse_tradingprice_csv(io.StringIO(SAMPLE_CSV), region="QLD1")
    assert all(p.region == "QLD1" for p in prices)
    assert len(prices) == 3  # QLD1 rows only


def test_parse_excludes_other_regions() -> None:
    prices = parse_tradingprice_csv(io.StringIO(SAMPLE_CSV), region="QLD1")
    regions = {p.region for p in prices}
    assert regions == {"QLD1"}


def test_parse_settlement_date_shifted_back_30_min() -> None:
    prices = parse_tradingprice_csv(io.StringIO(SAMPLE_CSV), region="QLD1")
    # SETTLEMENTDATE 2025-01-15 00:30:00 → interval_start 2025-01-15 00:00:00
    assert prices[0].interval_start == datetime(2025, 1, 15, 0, 0, 0, tzinfo=UTC)
    assert prices[0].interval_end == datetime(2025, 1, 15, 0, 30, 0, tzinfo=UTC)


def test_parse_interval_end_equals_settlement_date() -> None:
    prices = parse_tradingprice_csv(io.StringIO(SAMPLE_CSV), region="QLD1")
    assert prices[1].interval_end == datetime(2025, 1, 15, 1, 0, 0, tzinfo=UTC)


def test_parse_rrp_correct() -> None:
    prices = parse_tradingprice_csv(io.StringIO(SAMPLE_CSV), region="QLD1")
    assert prices[0].rrp_per_mwh == pytest.approx(45.25)
    assert prices[1].rrp_per_mwh == pytest.approx(52.80)


def test_parse_negative_price() -> None:
    prices = parse_tradingprice_csv(io.StringIO(SAMPLE_CSV), region="QLD1")
    # third QLD1 row has RRP = -12.50
    assert prices[2].rrp_per_mwh == pytest.approx(-12.50)


def test_parse_skips_non_data_rows() -> None:
    csv_with_extras = "C,comment line\n" + SAMPLE_CSV
    prices = parse_tradingprice_csv(io.StringIO(csv_with_extras), region="QLD1")
    assert len(prices) == 3  # same result


def test_parse_empty_csv_returns_empty_list() -> None:
    prices = parse_tradingprice_csv(io.StringIO(""), region="QLD1")
    assert prices == []


# ---------------------------------------------------------------------------
# fetch_month
# ---------------------------------------------------------------------------

async def test_fetch_month_downloads_and_parses_zip() -> None:
    zip_bytes = _make_zip(SAMPLE_CSV)

    mock_response = MagicMock()
    mock_response.raise_for_status = MagicMock()
    mock_response.content = zip_bytes

    mock_client = AsyncMock()
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)
    mock_client.get = AsyncMock(return_value=mock_response)

    with patch("candela.collector.aemo.httpx.AsyncClient", return_value=mock_client):
        prices = await fetch_month(year=2025, month=1, region="QLD1")

    assert len(prices) == 3
    assert prices[0].region == "QLD1"


async def test_fetch_month_raises_on_http_error() -> None:
    mock_response = MagicMock()
    mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
        "404", request=MagicMock(), response=MagicMock()
    )

    mock_client = AsyncMock()
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)
    mock_client.get = AsyncMock(return_value=mock_response)

    with patch("candela.collector.aemo.httpx.AsyncClient", return_value=mock_client):
        with pytest.raises(httpx.HTTPStatusError):
            await fetch_month(year=2025, month=1, region="QLD1")


# ---------------------------------------------------------------------------
# store_prices
# ---------------------------------------------------------------------------

async def _make_db() -> Database:
    db = Database("sqlite+aiosqlite:///:memory:")
    await db.connect()
    await db.execute(
        """
        CREATE TABLE aemo_trading_prices (
            interval_start TEXT NOT NULL,
            interval_end   TEXT NOT NULL,
            rrp_per_mwh    REAL NOT NULL,
            region         TEXT NOT NULL DEFAULT 'QLD1',
            PRIMARY KEY (interval_start, region)
        )
        """
    )
    return db


async def test_store_prices_inserts_rows() -> None:
    db = await _make_db()
    prices = parse_tradingprice_csv(io.StringIO(SAMPLE_CSV), region="QLD1")
    count = await store_prices(prices, db)

    assert count == 3
    rows = await db.fetch("SELECT * FROM aemo_trading_prices ORDER BY interval_start")
    assert len(rows) == 3
    assert rows[0]["rrp_per_mwh"] == pytest.approx(45.25)

    await db.disconnect()


async def test_store_prices_upserts_on_conflict() -> None:
    db = await _make_db()
    prices = parse_tradingprice_csv(io.StringIO(SAMPLE_CSV), region="QLD1")
    await store_prices(prices, db)

    # Re-store with modified price
    modified = [AemoPrice(
        interval_start=prices[0].interval_start,
        interval_end=prices[0].interval_end,
        rrp_per_mwh=999.99,
        region=prices[0].region,
    )] + prices[1:]
    await store_prices(modified, db)

    rows = await db.fetch("SELECT * FROM aemo_trading_prices ORDER BY interval_start")
    assert len(rows) == 3
    assert rows[0]["rrp_per_mwh"] == pytest.approx(999.99)

    await db.disconnect()


async def test_store_prices_returns_zero_for_empty() -> None:
    db = await _make_db()
    count = await store_prices([], db)
    assert count == 0
    await db.disconnect()
