"""Tests for the Sungrow inverter wrapper (collector/inverter.py)."""

from datetime import UTC
from unittest.mock import AsyncMock, patch

import pytest
from sungrow_websocket import InverterItem

from candela.collector.inverter import InverterReading, SungrowClient


def _item(value: str, unit: str = "W") -> InverterItem:
    return InverterItem(name="", desc="", value=value, unit=unit)


FULL_DATA: dict[str, InverterItem] = {
    "total_dc_power": _item("3500", "W"),
    "meter_active_power": _item("1200", "W"),
    "total_load_active_power": _item("4700", "W"),
    "daily_power_yields": _item("18.5", "kWh"),
    "total_active_generation": _item("1234.6", "kWh"),
    "internal_temperature": _item("42.3", "℃"),
}


async def test_read_returns_reading_on_success() -> None:
    client = SungrowClient("192.168.1.100")
    with patch.object(client._ws, "get_data_async", new=AsyncMock(return_value=FULL_DATA)):
        reading = await client.read()

    assert reading is not None
    assert isinstance(reading, InverterReading)
    assert reading.solar_w == 3500
    assert reading.grid_w == 1200
    assert reading.load_w == 4700
    assert reading.daily_yield_kwh == pytest.approx(18.5)
    assert reading.total_yield_kwh == pytest.approx(1234.6)
    assert reading.inverter_temp_c == pytest.approx(42.3)
    assert reading.ts.tzinfo is UTC


async def test_read_converts_kw_to_watts() -> None:
    data = {
        "total_dc_power": _item("3.500", "kW"),
        "meter_active_power": _item("-1.200", "kW"),
        "total_load_active_power": _item("2.300", "kW"),
    }
    client = SungrowClient("192.168.1.100")
    with patch.object(client._ws, "get_data_async", new=AsyncMock(return_value=data)):
        reading = await client.read()

    assert reading is not None
    assert reading.solar_w == 3500
    assert reading.grid_w == -1200
    assert reading.load_w == 2300


async def test_read_returns_none_on_connection_error() -> None:
    client = SungrowClient("192.168.1.100")
    with patch.object(
        client._ws,
        "get_data_async",
        new=AsyncMock(side_effect=OSError("connection refused")),
    ):
        reading = await client.read()

    assert reading is None


async def test_read_returns_none_on_empty_data() -> None:
    client = SungrowClient("192.168.1.100")
    with patch.object(client._ws, "get_data_async", new=AsyncMock(return_value={})):
        reading = await client.read()

    assert reading is None


async def test_read_returns_none_on_missing_required_fields() -> None:
    # grid power missing
    data = {
        "total_dc_power": _item("3500"),
        "total_load_active_power": _item("4700"),
    }
    client = SungrowClient("192.168.1.100")
    with patch.object(client._ws, "get_data_async", new=AsyncMock(return_value=data)):
        reading = await client.read()

    assert reading is None


async def test_optional_fields_are_none_when_absent() -> None:
    data = {
        "total_dc_power": _item("3500"),
        "meter_active_power": _item("1200"),
        "total_load_active_power": _item("4700"),
    }
    client = SungrowClient("192.168.1.100")
    with patch.object(client._ws, "get_data_async", new=AsyncMock(return_value=data)):
        reading = await client.read()

    assert reading is not None
    assert reading.daily_yield_kwh is None
    assert reading.total_yield_kwh is None
    assert reading.inverter_temp_c is None
