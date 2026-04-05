"""Sungrow WiNet-S2 inverter client.

Thin wrapper around the ``sungrow-websocket`` library that returns a typed
``InverterReading`` dataclass. Handles the inverter being offline overnight by
returning ``None`` rather than raising.

Field mapping
-------------
The sungrow-websocket library returns data keyed by the I18N_COMMON_ name
stripped of its prefix and lowercased. Field names below are those observed on
the SG5.0RS-ADA with WiNet-S2 firmware. If your unit returns different keys,
adjust the ``_*_KEY`` constants at the top of this module.

Sign convention
---------------
``grid_w`` follows the project convention: positive = importing from grid,
negative = exporting to grid. This matches the sign from ``meter_active_power``
on the SG5.0RS-ADA; verify against your own firmware if you see reversed signs.
"""

import logging
from dataclasses import dataclass
from datetime import UTC, datetime

from sungrow_websocket import InverterItem, SungrowWebsocket

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Field name constants — adjust if your inverter firmware uses different keys
# ---------------------------------------------------------------------------
_SOLAR_KEY = "total_dc_power"            # PV generation (W or kW)
_GRID_KEY = "meter_active_power"         # Grid power: +import / -export
_LOAD_KEY = "total_load_active_power"    # House consumption
_DAILY_YIELD_KEY = "daily_power_yields"  # Today's yield (kWh)
_TOTAL_YIELD_KEY = "total_active_generation"  # Lifetime yield (kWh)
_TEMP_KEY = "internal_temperature"       # Inverter temperature (℃)

_REQUIRED_KEYS: tuple[str, ...] = (_SOLAR_KEY, _GRID_KEY, _LOAD_KEY)


@dataclass
class InverterReading:
    """A single timestamped snapshot from the inverter."""

    ts: datetime            # UTC timestamp of when the reading was taken
    solar_w: int            # PV generation in watts
    grid_w: int             # Grid power in watts (+import / -export)
    load_w: int             # House consumption in watts
    daily_yield_kwh: float | None   # Generation today in kWh
    total_yield_kwh: float | None   # Lifetime generation in kWh
    inverter_temp_c: float | None   # Inverter board temperature in °C


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _to_watts(item: InverterItem) -> int | None:
    """Parse a power InverterItem to whole watts, converting kW if needed."""
    try:
        value = float(item.value)
    except (ValueError, TypeError):
        return None
    if item.unit == "kW":
        value *= 1000.0
    return round(value)


def _to_float(item: InverterItem) -> float | None:
    """Parse an InverterItem value to float, returning None on failure."""
    try:
        return float(item.value)
    except (ValueError, TypeError):
        return None


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------

class SungrowClient:
    """Async client for the Sungrow WiNet-S2 websocket API.

    Parameters
    ----------
    host:
        LAN IP address of the WiNet-S2 module.
    """

    def __init__(self, host: str) -> None:
        self._ws = SungrowWebsocket(host)

    async def read(self) -> InverterReading | None:
        """Fetch the current inverter state.

        Returns ``None`` if the inverter is offline or if required fields are
        missing (e.g. at night when the inverter has shut down).
        Never raises.
        """
        try:
            data = await self._ws.get_data_async()
        except Exception:
            logger.warning("Inverter connection failed", exc_info=True)
            return None

        if not data:
            logger.warning("Inverter returned empty data")
            return None

        missing = [k for k in _REQUIRED_KEYS if k not in data]
        if missing:
            logger.warning("Inverter data missing required fields: %s", missing)
            return None

        solar_w = _to_watts(data[_SOLAR_KEY])
        grid_w = _to_watts(data[_GRID_KEY])
        load_w = _to_watts(data[_LOAD_KEY])

        if solar_w is None or grid_w is None or load_w is None:
            logger.warning("Could not parse required inverter power fields")
            return None

        return InverterReading(
            ts=datetime.now(UTC),
            solar_w=solar_w,
            grid_w=grid_w,
            load_w=load_w,
            daily_yield_kwh=_to_float(data[_DAILY_YIELD_KEY]) if _DAILY_YIELD_KEY in data else None,
            total_yield_kwh=_to_float(data[_TOTAL_YIELD_KEY]) if _TOTAL_YIELD_KEY in data else None,
            inverter_temp_c=_to_float(data[_TEMP_KEY]) if _TEMP_KEY in data else None,
        )
