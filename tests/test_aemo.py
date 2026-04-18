"""Tests for the AEMO wholesale price fetcher (collector/aemo.py)."""

import io
import textwrap
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from candela.collector.aemo import (
    AemoPrice,
    parse_price_demand_csv,
    fetch_month,
    store_prices,
)
from candela.db import Database


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

SAMPLE_CSV = textwrap.dedent("""\
    REGION,SETTLEMENTDATE,TOTALDEMAND,RRP,PERIODTYPE
    QLD1,2026/04/01 00:05:00,6172.5,52.75,TRADE
    QLD1,2026/04/01 00:10:00,6172.31,43.12,TRADE
    NSW1,2026/04/01 00:05:00,8000.0,55.00,TRADE
    QLD1,2026/04/01 00:15:00,6156.16,-12.50,TRADE
    QLD1,2026/04/01 00:20:00,6100.0,60.00,FORECAST
""")


# ---------------------------------------------------------------------------
# parse_price_demand_csv
# ---------------------------------------------------------------------------


def test_parse_filters_qld1_only() -> None:
    prices = parse_price_demand_csv(io.StringIO(SAMPLE_CSV), region="QLD1")
    assert all(p.region == "QLD1" for p in prices)
    assert len(prices) == 3  # 3 QLD1 TRADE rows


def test_parse_excludes_other_regions() -> None:
    prices = parse_price_demand_csv(io.StringIO(SAMPLE_CSV), region="QLD1")
    assert all(p.region == "QLD1" for p in prices)


def test_parse_excludes_non_trade_rows() -> None:
    prices = parse_price_demand_csv(io.StringIO(SAMPLE_CSV), region="QLD1")
    # FORECAST row for QLD1 should be excluded
    assert len(prices) == 3


def test_parse_settlement_date_shifted_back_5_min() -> None:
    prices = parse_price_demand_csv(io.StringIO(SAMPLE_CSV), region="QLD1")
    # SETTLEMENTDATE 2026/04/01 00:05:00 → interval_start 2026/04/01 00:00:00
    assert prices[0].interval_start == datetime(2026, 4, 1, 0, 0, 0, tzinfo=UTC)
    assert prices[0].interval_end == datetime(2026, 4, 1, 0, 5, 0, tzinfo=UTC)


def test_parse_rrp_correct() -> None:
    prices = parse_price_demand_csv(io.StringIO(SAMPLE_CSV), region="QLD1")
    assert prices[0].rrp_per_mwh == pytest.approx(52.75)
    assert prices[1].rrp_per_mwh == pytest.approx(43.12)


def test_parse_negative_price() -> None:
    prices = parse_price_demand_csv(io.StringIO(SAMPLE_CSV), region="QLD1")
    assert prices[2].rrp_per_mwh == pytest.approx(-12.50)


def test_parse_empty_csv_returns_empty_list() -> None:
    prices = parse_price_demand_csv(io.StringIO(""), region="QLD1")
    assert prices == []


def test_parse_header_only_returns_empty_list() -> None:
    prices = parse_price_demand_csv(
        io.StringIO("REGION,SETTLEMENTDATE,TOTALDEMAND,RRP,PERIODTYPE\n"),
        region="QLD1",
    )
    assert prices == []


# ---------------------------------------------------------------------------
# fetch_month
# ---------------------------------------------------------------------------


async def test_fetch_month_downloads_and_parses_csv() -> None:
    mock_response = MagicMock()
    mock_response.raise_for_status = MagicMock()
    mock_response.text = SAMPLE_CSV

    mock_client = AsyncMock()
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)
    mock_client.get = AsyncMock(return_value=mock_response)

    with patch("candela.collector.aemo.httpx.AsyncClient", return_value=mock_client):
        prices = await fetch_month(year=2026, month=4, region="QLD1")

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
            await fetch_month(year=2026, month=4, region="QLD1")


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
    prices = parse_price_demand_csv(io.StringIO(SAMPLE_CSV), region="QLD1")
    count = await store_prices(prices, db)

    assert count == 3
    rows = await db.fetch("SELECT * FROM aemo_trading_prices ORDER BY interval_start")
    assert len(rows) == 3
    assert rows[0]["rrp_per_mwh"] == pytest.approx(52.75)

    await db.disconnect()


async def test_store_prices_upserts_on_conflict() -> None:
    db = await _make_db()
    prices = parse_price_demand_csv(io.StringIO(SAMPLE_CSV), region="QLD1")
    await store_prices(prices, db)

    modified = [
        AemoPrice(
            interval_start=prices[0].interval_start,
            interval_end=prices[0].interval_end,
            rrp_per_mwh=999.99,
            region=prices[0].region,
        )
    ] + prices[1:]
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
