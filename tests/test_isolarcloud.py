"""Tests for the iSolarCloud HTTP API client (collector/isolarcloud.py)."""

from datetime import UTC, date, datetime

import httpx
import pytest

from candela.collector.isolarcloud import (
    ISolarCloudClient,
    ISolarCloudError,
    InverterReading,
    _parse_interval_data,
    _parse_realtime_reading,
    parse_isolarcloud_ts,
)


# ---------------------------------------------------------------------------
# Mock transport helpers
# ---------------------------------------------------------------------------


def _build_transport(
    *,
    login_code: str = "1",
    stations: list | None = None,
    interval_data: dict | None = None,
    interval_code: str = "1",
) -> httpx.MockTransport:
    """Build an httpx.MockTransport with canned iSolarCloud responses."""
    if stations is None:
        stations = [{"ps_id": "123456"}]
    if interval_data is None:
        interval_data = {
            "83067": [{"time": "20250601080000", "value": "2500"}],
            "83052": [{"time": "20250601080000", "value": "400"}],
            "83549": [{"time": "20250601080000", "value": "-2100"}],
        }

    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if "login" in url:
            return httpx.Response(
                200,
                json={
                    "result_code": login_code,
                    "result_data": {"token": "test-token", "token_id": "abc"},
                },
            )
        if "getPowerStationList" in url:
            return httpx.Response(
                200,
                json={
                    "result_code": "1",
                    "result_data": {"pageList": stations},
                },
            )
        if "queryMutiPointDataList" in url:
            return httpx.Response(
                200,
                json={
                    "result_code": interval_code,
                    "result_data": interval_data,
                },
            )
        return httpx.Response(404, json={})

    return httpx.MockTransport(handler)


def _make_client(
    transport: httpx.MockTransport | None = None,
) -> ISolarCloudClient:
    return ISolarCloudClient(
        app_key="test-key",
        access_key="test-access-key",
        username="user@example.com",
        password="password",
        base_url="https://augateway.isolarcloud.com",
        transport=transport or _build_transport(),
    )


# ---------------------------------------------------------------------------
# parse_isolarcloud_ts
# ---------------------------------------------------------------------------


def test_parse_isolarcloud_ts_converts_to_utc() -> None:
    # 08:00 AEST (UTC+10) → 22:00 UTC the previous day
    ts = parse_isolarcloud_ts("20250601080000")
    assert ts == datetime(2025, 5, 31, 22, 0, 0, tzinfo=UTC)


def test_parse_isolarcloud_ts_midnight() -> None:
    # 00:00 AEST → 14:00 UTC previous day
    ts = parse_isolarcloud_ts("20250601000000")
    assert ts == datetime(2025, 5, 31, 14, 0, 0, tzinfo=UTC)


def test_parse_isolarcloud_ts_returns_utc_tzinfo() -> None:
    ts = parse_isolarcloud_ts("20250601120000")
    assert ts.tzinfo is UTC


# ---------------------------------------------------------------------------
# authenticate
# ---------------------------------------------------------------------------


async def test_authenticate_caches_token() -> None:
    client = _make_client()
    await client.authenticate()
    assert client._token == "test-token"
    assert client._token_acquired is not None


async def test_authenticate_skips_when_token_is_fresh() -> None:
    client = _make_client()
    await client.authenticate()
    first_acquired = client._token_acquired
    await client.authenticate()
    # No re-login; acquired timestamp unchanged
    assert client._token_acquired == first_acquired


async def test_authenticate_refreshes_stale_token() -> None:
    client = _make_client()
    await client.authenticate()
    # Force token to appear 23+ hours old
    client._token_acquired = datetime(2020, 1, 1, tzinfo=UTC)
    await client.authenticate()
    assert client._token_acquired > datetime(2020, 1, 2, tzinfo=UTC)


async def test_authenticate_raises_on_api_error() -> None:
    transport = _build_transport(login_code="invalid_user")
    client = _make_client(transport)
    with pytest.raises(ISolarCloudError, match="API error"):
        await client.authenticate()


# ---------------------------------------------------------------------------
# get_ps_id
# ---------------------------------------------------------------------------


async def test_get_ps_id_returns_plant_id() -> None:
    client = _make_client()
    ps_id = await client.get_ps_id()
    assert ps_id == "123456"


async def test_get_ps_id_caches_result() -> None:
    station_call_count = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal station_call_count
        url = str(request.url)
        if "login" in url:
            return httpx.Response(
                200,
                json={
                    "result_code": "1",
                    "result_data": {"token": "t", "token_id": "a"},
                },
            )
        if "getPowerStationList" in url:
            station_call_count += 1
            return httpx.Response(
                200,
                json={
                    "result_code": "1",
                    "result_data": {"pageList": [{"ps_id": "999"}]},
                },
            )
        return httpx.Response(404, json={})

    client = ISolarCloudClient(
        "k", "ak", "u", "p", "https://host", transport=httpx.MockTransport(handler)
    )
    await client.get_ps_id()
    await client.get_ps_id()
    assert station_call_count == 1


async def test_get_ps_id_raises_when_no_stations() -> None:
    transport = _build_transport(stations=[])
    client = _make_client(transport)
    with pytest.raises(ISolarCloudError, match="No power stations"):
        await client.get_ps_id()


# ---------------------------------------------------------------------------
# fetch_interval_data
# ---------------------------------------------------------------------------


async def test_fetch_interval_data_returns_readings() -> None:
    client = _make_client()
    readings = await client.fetch_interval_data(date(2025, 6, 1))
    assert len(readings) == 1
    r = readings[0]
    assert isinstance(r, InverterReading)
    assert r.solar_w == 2500
    assert r.grid_w == -2100
    assert r.load_w == 400


async def test_fetch_interval_data_timestamp_converted_to_utc() -> None:
    client = _make_client()
    readings = await client.fetch_interval_data(date(2025, 6, 1))
    # 20250601080000 AEST = 2025-05-31T22:00:00 UTC
    assert readings[0].ts == datetime(2025, 5, 31, 22, 0, 0, tzinfo=UTC)


async def test_fetch_interval_data_multiple_intervals_sorted() -> None:
    data = {
        "83067": [
            {"time": "20250601080000", "value": "2500"},
            {"time": "20250601080500", "value": "2600"},
        ],
        "83052": [
            {"time": "20250601080000", "value": "400"},
            {"time": "20250601080500", "value": "450"},
        ],
        "83549": [
            {"time": "20250601080000", "value": "-2100"},
            {"time": "20250601080500", "value": "-2150"},
        ],
    }
    transport = _build_transport(interval_data=data)
    client = _make_client(transport)
    readings = await client.fetch_interval_data(date(2025, 6, 1))
    assert len(readings) == 2
    assert readings[0].solar_w == 2500
    assert readings[1].solar_w == 2600


async def test_fetch_interval_data_optional_fields_present() -> None:
    data = {
        "83067": [{"time": "20250601080000", "value": "2500"}],
        "83052": [{"time": "20250601080000", "value": "400"}],
        "83549": [{"time": "20250601080000", "value": "-2100"}],
        "83022": [{"time": "20250601080000", "value": "18500"}],  # 18.5 kWh in Wh
        "83024": [{"time": "20250601080000", "value": "1234600"}],  # 1234.6 kWh in Wh
        "83016": [{"time": "20250601080000", "value": "42.3"}],
    }
    transport = _build_transport(interval_data=data)
    client = _make_client(transport)
    readings = await client.fetch_interval_data(date(2025, 6, 1))
    r = readings[0]
    assert r.daily_yield_kwh == pytest.approx(18.5)
    assert r.total_yield_kwh == pytest.approx(1234.6)
    assert r.inverter_temp_c == pytest.approx(42.3)


async def test_fetch_interval_data_optional_fields_absent() -> None:
    client = _make_client()  # default data has no optional fields
    readings = await client.fetch_interval_data(date(2025, 6, 1))
    assert readings[0].daily_yield_kwh is None
    assert readings[0].total_yield_kwh is None
    assert readings[0].inverter_temp_c is None


async def test_fetch_interval_data_empty_response() -> None:
    transport = _build_transport(interval_data={})
    client = _make_client(transport)
    readings = await client.fetch_interval_data(date(2025, 6, 1))
    assert readings == []


async def test_fetch_interval_data_api_error_raises() -> None:
    transport = _build_transport(interval_code="limit_exceeded")
    client = _make_client(transport)
    with pytest.raises(ISolarCloudError):
        await client.fetch_interval_data(date(2025, 6, 1))


async def test_fetch_interval_data_skips_intervals_with_missing_grid_or_load() -> None:
    # solar has 2 timestamps, but load/grid only have 1 — second interval is skipped
    data = {
        "83067": [
            {"time": "20250601080000", "value": "2500"},
            {"time": "20250601080500", "value": "2600"},
        ],
        "83052": [{"time": "20250601080000", "value": "400"}],
        "83549": [{"time": "20250601080000", "value": "-2100"}],
    }
    transport = _build_transport(interval_data=data)
    client = _make_client(transport)
    readings = await client.fetch_interval_data(date(2025, 6, 1))
    assert len(readings) == 1


# ---------------------------------------------------------------------------
# _parse_interval_data (unit tests for the pure parsing function)
# ---------------------------------------------------------------------------


def test_parse_interval_data_basic() -> None:
    data = {
        "83067": [{"time": "20250601080000", "value": "1000"}],
        "83052": [{"time": "20250601080000", "value": "200"}],
        "83549": [{"time": "20250601080000", "value": "-800"}],
    }
    readings = _parse_interval_data(data)
    assert len(readings) == 1
    assert readings[0].solar_w == 1000
    assert readings[0].load_w == 200
    assert readings[0].grid_w == -800


def test_parse_interval_data_temperature() -> None:
    data = {
        "83067": [{"time": "20250601080000", "value": "1000"}],
        "83052": [{"time": "20250601080000", "value": "200"}],
        "83549": [{"time": "20250601080000", "value": "0"}],
        "83016": [{"time": "20250601080000", "value": "35.0"}],
    }
    readings = _parse_interval_data(data)
    assert readings[0].inverter_temp_c == pytest.approx(35.0)


def test_parse_interval_data_empty_input() -> None:
    assert _parse_interval_data({}) == []


# ---------------------------------------------------------------------------
# _parse_realtime_reading
# ---------------------------------------------------------------------------


def _make_rt_response(device_time: str = "20260410080000", **points: str) -> dict:
    device_point = {
        "device_time": device_time,
        **{f"p{k}": v for k, v in points.items()},
    }
    return {"device_point_list": [{"device_point": device_point}]}


def test_parse_realtime_reading_daytime() -> None:
    plant = _make_rt_response(**{"83022": "14000.0", "83024": "9606300.0"})
    inverter = _make_rt_response(**{"24": "3500.0", "4": "42.5"})
    meter = _make_rt_response(**{"8018": "-2800.0"})
    r = _parse_realtime_reading(plant, inverter, meter)
    assert r.solar_w == 3500
    assert r.grid_w == -2800
    assert r.load_w == 700  # 3500 - 2800
    assert r.daily_yield_kwh == pytest.approx(14.0)
    assert r.total_yield_kwh == pytest.approx(9606.3)
    assert r.inverter_temp_c == pytest.approx(42.5)
    assert r.ts == datetime(2026, 4, 9, 22, 0, 0, tzinfo=UTC)  # 08:00 AEST


def test_parse_realtime_reading_night() -> None:
    # At night inverter is idle — solar defaults to 0, load == grid import
    plant = _make_rt_response(**{"83022": "28000.0", "83024": "9606300.0"})
    meter = _make_rt_response(**{"8018": "253.0"})
    r = _parse_realtime_reading(plant, None, meter)
    assert r.solar_w == 0
    assert r.grid_w == 253
    assert r.load_w == 253
    assert r.inverter_temp_c is None


def test_parse_realtime_reading_no_meter() -> None:
    plant = _make_rt_response()
    inverter = _make_rt_response(**{"24": "1000.0"})
    r = _parse_realtime_reading(plant, inverter, None)
    assert r.solar_w == 1000
    assert r.grid_w == 0
    assert r.load_w == 1000


def test_parse_realtime_reading_uses_meter_timestamp() -> None:
    plant = _make_rt_response()
    inverter = _make_rt_response(**{"24": "1000.0"})
    meter = _make_rt_response(device_time="20260410090000", **{"8018": "100.0"})
    r = _parse_realtime_reading(plant, inverter, meter)
    assert r.ts == datetime(2026, 4, 9, 23, 0, 0, tzinfo=UTC)  # 09:00 AEST


async def test_fetch_current_reading_integration() -> None:
    """Full client round-trip using mock transport."""
    import json

    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        payload = json.loads(request.read())

        if "login" in url:
            return httpx.Response(
                200, json={"result_code": "1", "result_data": {"token": "t"}}
            )
        if "getPowerStationList" in url:
            return httpx.Response(
                200,
                json={
                    "result_code": "1",
                    "result_data": {"pageList": [{"ps_id": "1555280"}]},
                },
            )
        if "getPowerStationDetail" in url:
            return httpx.Response(
                200,
                json={"result_code": "1", "result_data": {"ps_key": "1555280_11_0_0"}},
            )
        if "getDeviceList" in url:
            return httpx.Response(
                200,
                json={
                    "result_code": "1",
                    "result_data": {
                        "pageList": [
                            {"device_type": 1, "ps_key": "1555280_1_1_1"},
                            {"device_type": 7, "ps_key": "1555280_7_1_1"},
                        ]
                    },
                },
            )
        if "getDeviceRealTimeData" in url:
            ps_keys = payload.get("ps_key_list", [])
            if "1555280_11_0_0" in ps_keys:
                return httpx.Response(
                    200,
                    json={
                        "result_code": "1",
                        "result_data": {
                            "device_point_list": [
                                {
                                    "device_point": {
                                        "device_time": "20260410080000",
                                        "p83022": "14000.0",
                                        "p83024": "9606300.0",
                                    }
                                }
                            ]
                        },
                    },
                )
            if "1555280_1_1_1" in ps_keys:
                return httpx.Response(
                    200,
                    json={
                        "result_code": "1",
                        "result_data": {
                            "device_point_list": [
                                {
                                    "device_point": {
                                        "device_time": "20260410080000",
                                        "p24": "3500.0",
                                        "p4": "42.5",
                                    }
                                }
                            ]
                        },
                    },
                )
            if "1555280_7_1_1" in ps_keys:
                return httpx.Response(
                    200,
                    json={
                        "result_code": "1",
                        "result_data": {
                            "device_point_list": [
                                {
                                    "device_point": {
                                        "device_time": "20260410080000",
                                        "p8018": "-2800.0",
                                    }
                                }
                            ]
                        },
                    },
                )
        return httpx.Response(404, json={})

    client = ISolarCloudClient(
        "key",
        "access",
        "user@example.com",
        "pass",
        "https://host",
        transport=httpx.MockTransport(handler),
    )
    r = await client.fetch_current_reading()
    assert r.solar_w == 3500
    assert r.grid_w == -2800
    assert r.load_w == 700
    assert r.daily_yield_kwh == pytest.approx(14.0)
    assert r.inverter_temp_c == pytest.approx(42.5)
