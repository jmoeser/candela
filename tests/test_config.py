"""Tests for candela.config — pydantic-settings loading."""

import os
import pytest
from unittest.mock import patch


def test_config_defaults() -> None:
    """Settings with only required vars should use documented defaults."""
    env = {
        "DATABASE_URL": "sqlite+aiosqlite:///./candela.db",
        "INVERTER_HOST": "192.168.1.100",
    }
    with patch.dict(os.environ, env, clear=True):
        from candela.config import Settings
        s = Settings()
    assert s.database_url == "sqlite+aiosqlite:///./candela.db"
    assert s.inverter_host == "192.168.1.100"
    assert s.inverter_poll_interval_seconds == 300
    assert s.aemo_region == "QLD1"
    assert s.wholesale_adder_cents_kwh == 18.0


def test_config_overrides() -> None:
    """Env vars override all defaults."""
    env = {
        "DATABASE_URL": "postgresql+asyncpg://user:pass@localhost/candela",
        "INVERTER_HOST": "10.0.0.50",
        "INVERTER_POLL_INTERVAL_SECONDS": "60",
        "AEMO_REGION": "NSW1",
        "WHOLESALE_ADDER_CENTS_KWH": "22.5",
    }
    with patch.dict(os.environ, env, clear=True):
        from candela.config import Settings
        s = Settings()
    assert s.database_url == "postgresql+asyncpg://user:pass@localhost/candela"
    assert s.inverter_host == "10.0.0.50"
    assert s.inverter_poll_interval_seconds == 60
    assert s.aemo_region == "NSW1"
    assert s.wholesale_adder_cents_kwh == 22.5


def test_config_missing_required_vars() -> None:
    """Missing required vars should raise a ValidationError."""
    from pydantic import ValidationError
    with patch.dict(os.environ, {}, clear=True):
        from candela.config import Settings
        with pytest.raises(ValidationError):
            Settings()


def test_config_missing_inverter_host() -> None:
    """DATABASE_URL present but INVERTER_HOST missing should raise."""
    from pydantic import ValidationError
    env = {"DATABASE_URL": "sqlite+aiosqlite:///./candela.db"}
    with patch.dict(os.environ, env, clear=True):
        from candela.config import Settings
        with pytest.raises(ValidationError):
            Settings()
