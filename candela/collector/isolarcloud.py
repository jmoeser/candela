"""iSolarCloud HTTP API client.

Authenticates with the iSolarCloud API, discovers the plant ID, and fetches
5-minute interval data as typed ``InverterReading`` dataclasses.

Sign convention
---------------
``grid_w`` follows the project convention: positive = importing from grid,
negative = exporting to grid. Meter point 8018 (Meter Active Power) is
assumed to follow the same sign convention — verify during a solar-export
period to confirm negative values appear when exporting.

Timezone
--------
iSolarCloud timestamps are in local time (Australia/Brisbane, AEST = UTC+10).
All timestamps are converted to UTC before being returned.
"""

import asyncio
import hashlib
import logging
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from zoneinfo import ZoneInfo

import httpx

logger = logging.getLogger(__name__)

BRISBANE = ZoneInfo("Australia/Brisbane")

# ---------------------------------------------------------------------------
# Point ID constants
# ---------------------------------------------------------------------------
# Plant-level (device type 11)
_POINT_SOLAR_W = "83067"  # Total Active Power of PV (W) — not reliable; use inverter
_POINT_LOAD_W = "83052"  # Load Power (W)
_POINT_GRID_W = "83549"  # Grid Active Power (W)
_POINT_DAILY_WH = "83022"  # Daily Yield of Plant (Wh)
_POINT_TOTAL_WH = "83024"  # Plant Total Yield (Wh)
_POINT_TEMP = "83016"  # Plant Ambient Temperature (°C)

# Inverter-level (device type 1)
_INV_POINT_SOLAR_W = "24"  # Total Active Power / AC output (W)
_INV_POINT_TEMP = "4"  # Internal Air Temperature (°C)

# Meter-level (device type 7)
_METER_POINT_GRID_W = "8018"  # Meter Active Power (W, positive=import, negative=export)

_ALL_POINT_IDS = [
    _POINT_SOLAR_W,
    _POINT_LOAD_W,
    _POINT_GRID_W,
    _POINT_DAILY_WH,
    _POINT_TOTAL_WH,
    _POINT_TEMP,
]

# Token lifetime: iSolarCloud tokens expire after ~24 hours. Refresh
# proactively when the token is 23+ hours old.
_TOKEN_REFRESH_AGE = timedelta(hours=23)


# ---------------------------------------------------------------------------
# Public types
# ---------------------------------------------------------------------------


@dataclass
class InverterReading:
    """A single timestamped interval reading from iSolarCloud."""

    ts: datetime  # UTC timestamp
    solar_w: int  # PV generation in watts
    grid_w: int  # Grid power in watts (+import / -export)
    load_w: int  # House consumption in watts
    daily_yield_kwh: float | None  # Generation today in kWh
    total_yield_kwh: float | None  # Lifetime generation in kWh
    inverter_temp_c: float | None  # Inverter board temperature in °C


class ISolarCloudError(Exception):
    """Raised when the iSolarCloud API returns a non-success result code."""


# ---------------------------------------------------------------------------
# Timestamp parsing
# ---------------------------------------------------------------------------


def parse_isolarcloud_ts(ts_str: str) -> datetime:
    """Parse an iSolarCloud timestamp string to a UTC-aware datetime.

    iSolarCloud uses local time (AEST, UTC+10). Format: ``YYYYMMDDHHmmss``.
    """
    naive = datetime.strptime(ts_str, "%Y%m%d%H%M%S")
    local = naive.replace(tzinfo=BRISBANE)
    return local.astimezone(UTC)


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------


class ISolarCloudClient:
    """Async client for the iSolarCloud HTTP API.

    Parameters
    ----------
    app_key:
        Application key from the iSolarCloud developer portal.
    username:
        iSolarCloud account email address.
    password:
        iSolarCloud account password (plaintext; MD5'd at authentication time).
    base_url:
        API base URL. Australian accounts use
        ``https://augateway.isolarcloud.com``.
    transport:
        Optional ``httpx`` transport. Pass an ``httpx.MockTransport`` in tests.
    """

    def __init__(
        self,
        app_key: str,
        access_key: str,
        username: str,
        password: str,
        base_url: str,
        *,
        transport: httpx.AsyncBaseTransport | httpx.MockTransport | None = None,
    ) -> None:
        self.app_key = app_key
        self.access_key = access_key
        self.username = username
        self._password_md5 = hashlib.md5(password.encode()).hexdigest()
        self.base_url = base_url.rstrip("/")
        self._token: str | None = None
        self._token_acquired: datetime | None = None
        self._ps_id: str | None = None
        self._ps_key: str | None = None
        self._device_ps_keys: dict[int, str] | None = None  # keyed by device_type
        self._transport = transport

    def _make_http_client(self) -> httpx.AsyncClient:
        headers: dict[str, str] = {
            "x-access-key": self.access_key,
            "Content-Type": "application/json;charset=UTF-8",
        }
        if self._token:
            headers["Authorization"] = f"Bearer {self._token}"
        kwargs: dict = {"base_url": self.base_url, "timeout": 30.0, "headers": headers}
        if self._transport is not None:
            kwargs["transport"] = self._transport
        return httpx.AsyncClient(**kwargs)

    async def _post(self, endpoint: str, payload: dict) -> dict:
        """POST to an iSolarCloud endpoint and return ``result_data``.

        Raises
        ------
        ISolarCloudError
            If the API returns a ``result_code`` other than ``"1"``.
        httpx.HTTPStatusError
            If the HTTP response is a 4xx/5xx.
        """
        if self._token:
            payload = {"token": self._token, **payload}
        async with self._make_http_client() as http:
            response = await http.post(endpoint, json=payload)
            response.raise_for_status()

        body = response.json()
        if str(body.get("result_code")) != "1":
            raise ISolarCloudError(
                f"API error on {endpoint}: "
                f"code={body.get('result_code')!r} "
                f"msg={body.get('result_msg', '')!r}"
            )
        return body.get("result_data") or {}

    def _token_needs_refresh(self) -> bool:
        if self._token is None or self._token_acquired is None:
            return True
        return datetime.now(UTC) - self._token_acquired >= _TOKEN_REFRESH_AGE

    async def authenticate(self) -> None:
        """Fetch and cache an auth token. Skips if the current token is fresh."""
        if not self._token_needs_refresh():
            return
        logger.info("Authenticating with iSolarCloud as %s", self.username)
        data = await self._post(
            "/openapi/login",
            {
                "appkey": self.app_key,
                # "sys_code": "900",
                "login_type": "1",
                "user_account": self.username,
                "user_password": self._password_md5,
            },
        )
        self._token = data["token"]
        self._token_acquired = datetime.now(UTC)
        logger.info("iSolarCloud authentication successful")

    async def get_ps_id(self) -> str:
        """Return the plant (power station) ID, fetching it once and caching."""
        if self._ps_id is not None:
            return self._ps_id
        await self.authenticate()
        data = await self._post(
            "/openapi/getPowerStationList",
            {
                "appkey": self.app_key,
                "curPage": "1",
                "size": "10",
            },
        )
        stations = data.get("pageList") or data.get("list") or []
        if not stations:
            raise ISolarCloudError("No power stations found for this account")
        self._ps_id = str(stations[0]["ps_id"])
        logger.info("Discovered plant ID: %s", self._ps_id)
        return self._ps_id

    async def get_ps_key(self) -> str:
        """Return the plant ps_key, fetching it once and caching."""
        if self._ps_key is not None:
            return self._ps_key
        ps_id = await self.get_ps_id()
        data = await self._post(
            "/openapi/getPowerStationDetail",
            {"appkey": self.app_key, "ps_id": ps_id},
        )
        self._ps_key = str(data["ps_key"])
        logger.info("Discovered plant ps_key: %s", self._ps_key)
        return self._ps_key

    async def _ensure_devices_discovered(self) -> None:
        """Fetch device list once and cache ps_keys by device_type."""
        if self._device_ps_keys is not None:
            return
        ps_id = await self.get_ps_id()
        data = await self._post(
            "/openapi/getDeviceList",
            {"appkey": self.app_key, "ps_id": ps_id, "curPage": "1", "size": "20"},
        )
        self._device_ps_keys = {}
        for device in data.get("pageList") or []:
            dtype = device.get("device_type")
            ps_key = device.get("ps_key")
            if dtype is not None and ps_key:
                self._device_ps_keys[int(dtype)] = str(ps_key)
                logger.info("Discovered device type=%d ps_key=%s", dtype, ps_key)

    async def get_device_ps_key(self, device_type: int) -> str | None:
        """Return the ps_key for the first device of ``device_type``, or ``None``."""
        await self._ensure_devices_discovered()
        return (self._device_ps_keys or {}).get(device_type)

    async def fetch_current_reading(self) -> InverterReading:
        """Fetch a single real-time reading from the inverter, meter, and plant.

        - Inverter (type 1): solar AC power, temperature
        - Meter (type 7): grid power
        - Plant (type 11): daily/total yield

        Load power is derived as ``solar_w + grid_w``. All three requests are
        issued concurrently.
        """
        ps_key = await self.get_ps_key()
        inverter_ps_key = await self.get_device_ps_key(1)
        meter_ps_key = await self.get_device_ps_key(7)

        coros = [
            self._post(
                "/openapi/getDeviceRealTimeData",
                {
                    "appkey": self.app_key,
                    "ps_key_list": [ps_key],
                    "point_id_list": [_POINT_DAILY_WH, _POINT_TOTAL_WH],
                    "device_type": 11,
                },
            ),
            self._post(
                "/openapi/getDeviceRealTimeData",
                {
                    "appkey": self.app_key,
                    "ps_key_list": [inverter_ps_key],
                    "point_id_list": [_INV_POINT_SOLAR_W, _INV_POINT_TEMP],
                    "device_type": 1,
                },
            )
            if inverter_ps_key
            else None,
            self._post(
                "/openapi/getDeviceRealTimeData",
                {
                    "appkey": self.app_key,
                    "ps_key_list": [meter_ps_key],
                    "point_id_list": [_METER_POINT_GRID_W],
                    "device_type": 7,
                },
            )
            if meter_ps_key
            else None,
        ]

        plant_data, inverter_data, meter_data = await asyncio.gather(
            *[c if c is not None else asyncio.sleep(0) for c in coros]
        )
        # asyncio.sleep(0) returns None, which is the right sentinel
        if coros[1] is None:
            inverter_data = None
        if coros[2] is None:
            meter_data = None

        assert plant_data is not None  # coros[0] is always a real coroutine
        return _parse_realtime_reading(plant_data, inverter_data, meter_data)

    async def fetch_interval_data(self, day: date) -> list[InverterReading]:
        """Fetch 5-minute interval data for a single calendar day.

        Parameters
        ----------
        day:
            The date to fetch (local AEST date).

        Returns
        -------
        list[InverterReading]
            Readings sorted by timestamp ascending. May be empty if no data is
            available yet for that day (e.g. asking for today very early).
        """
        await self.authenticate()
        ps_id = await self.get_ps_id()

        start = day.strftime("%Y%m%d") + "000000"
        end = day.strftime("%Y%m%d") + "235959"

        data = await self._post(
            "/v1/commonService/queryMutiPointDataList",
            {
                "appkey": self.app_key,
                "ps_id": ps_id,
                "points": ",".join(_ALL_POINT_IDS),
                "minute_interval": "5",
                "start_time_stamp": start,
                "end_time_stamp": end,
                "device_type": "1",
            },
        )
        return _parse_interval_data(data)


# ---------------------------------------------------------------------------
# Internal parsing helpers
# ---------------------------------------------------------------------------


def _parse_realtime_reading(
    plant_data: dict,
    inverter_data: dict | None,
    meter_data: dict | None,
) -> InverterReading:
    """Parse ``getDeviceRealTimeData`` responses into a single ``InverterReading``.

    - ``plant_data``: plant-level device (type 11) — yields
    - ``inverter_data``: inverter device (type 1) — solar AC power, temperature
    - ``meter_data``: smart meter (type 7) — grid power

    Point values are keyed as ``p{id}`` on ``device_point``. Missing power
    points (e.g. at night) default to 0.
    """

    def _device_point(data: dict | None) -> dict:
        if data and data.get("device_point_list"):
            return data["device_point_list"][0].get("device_point", {})
        return {}

    plant_point = _device_point(plant_data)
    inv_point = _device_point(inverter_data)
    meter_point = _device_point(meter_data)

    ts_str = (
        meter_point.get("device_time")
        or inv_point.get("device_time")
        or plant_point.get("device_time")
    )
    ts_utc = parse_isolarcloud_ts(ts_str) if ts_str else datetime.now(UTC)

    solar_w = round(_opt_float(inv_point.get(f"p{_INV_POINT_SOLAR_W}")) or 0.0)
    grid_w = round(_opt_float(meter_point.get(f"p{_METER_POINT_GRID_W}")) or 0.0)
    load_w = solar_w + grid_w

    daily_wh = _opt_float(plant_point.get(f"p{_POINT_DAILY_WH}"))
    total_wh = _opt_float(plant_point.get(f"p{_POINT_TOTAL_WH}"))

    return InverterReading(
        ts=ts_utc,
        solar_w=solar_w,
        grid_w=grid_w,
        load_w=load_w,
        daily_yield_kwh=daily_wh / 1000.0 if daily_wh is not None else None,
        total_yield_kwh=total_wh / 1000.0 if total_wh is not None else None,
        inverter_temp_c=_opt_float(inv_point.get(f"p{_INV_POINT_TEMP}")),
    )


def _parse_interval_data(data: dict) -> list[InverterReading]:
    """Parse a point data result_data dict into readings."""
    solar_map = _index_by_time(data.get(_POINT_SOLAR_W) or [])
    load_map = _index_by_time(data.get(_POINT_LOAD_W) or [])
    grid_map = _index_by_time(data.get(_POINT_GRID_W) or [])
    daily_map = _index_by_time(data.get(_POINT_DAILY_WH) or [])
    total_map = _index_by_time(data.get(_POINT_TOTAL_WH) or [])
    temp_map = _index_by_time(data.get(_POINT_TEMP) or [])

    readings: list[InverterReading] = []
    for ts_str in sorted(solar_map):
        if ts_str not in load_map or ts_str not in grid_map:
            logger.debug("Skipping interval %s: missing load or grid point", ts_str)
            continue

        try:
            solar_w = round(float(solar_map[ts_str]))
            load_w = round(float(load_map[ts_str]))
            grid_w = round(float(grid_map[ts_str]))
        except ValueError, TypeError:
            logger.warning("Cannot parse power values at %s — skipping", ts_str)
            continue

        raw_temp = _opt_float(temp_map.get(ts_str))

        try:
            ts_utc = parse_isolarcloud_ts(ts_str)
        except ValueError:
            logger.warning("Cannot parse timestamp %r — skipping", ts_str)
            continue

        daily_wh = _opt_float(daily_map.get(ts_str))
        total_wh = _opt_float(total_map.get(ts_str))

        readings.append(
            InverterReading(
                ts=ts_utc,
                solar_w=solar_w,
                grid_w=grid_w,
                load_w=load_w,
                daily_yield_kwh=daily_wh / 1000.0 if daily_wh is not None else None,
                total_yield_kwh=total_wh / 1000.0 if total_wh is not None else None,
                inverter_temp_c=raw_temp,
            )
        )

    return readings


def _index_by_time(entries: list[dict]) -> dict[str, str]:
    return {e["time"]: e["value"] for e in entries if "time" in e and "value" in e}


def _opt_float(value: str | None) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except ValueError, TypeError:
        return None
