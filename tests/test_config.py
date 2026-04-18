"""Tests for candela.config — pydantic-settings loading."""

import os
import pytest
from unittest.mock import patch

_REQUIRED = {
    "DATABASE_URL": "sqlite+aiosqlite:///./candela.db",
    "SECRET_KEY": "test-secret-key",
    "AUTH_USERNAME": "admin",
    "AUTH_PASSWORD": "hunter2",
    "ISOLARCLOUD_APP_KEY": "myappkey",
    "ISOLARCLOUD_ACCESS_KEY": "myaccesskey",
    "ISOLARCLOUD_USERNAME": "user@example.com",
    "ISOLARCLOUD_PASSWORD": "secret",
}


def test_config_defaults() -> None:
    """Settings with only required vars should use documented defaults."""
    with patch.dict(os.environ, _REQUIRED, clear=True):
        from candela.config import Settings

        s = Settings(_env_file=None)
    assert s.database_url == "sqlite+aiosqlite:///./candela.db"
    assert s.secret_key == "test-secret-key"
    assert s.auth_username == "admin"
    assert s.auth_password == "hunter2"
    assert s.api_key is None
    assert s.isolarcloud_app_key == "myappkey"
    assert s.isolarcloud_username == "user@example.com"
    assert s.isolarcloud_password == "secret"
    assert s.isolarcloud_base_url == "https://augateway.isolarcloud.com"
    assert s.isolarcloud_poll_interval_seconds == 300
    assert s.aemo_region == "QLD1"
    assert s.wholesale_adder_cents_kwh == 18.0


def test_config_overrides() -> None:
    """Env vars override all defaults."""
    env = {
        **_REQUIRED,
        "ISOLARCLOUD_BASE_URL": "https://custom.isolarcloud.com",
        "ISOLARCLOUD_POLL_INTERVAL_SECONDS": "60",
        "AEMO_REGION": "NSW1",
        "WHOLESALE_ADDER_CENTS_KWH": "22.5",
    }
    with patch.dict(os.environ, env, clear=True):
        from candela.config import Settings

        s = Settings(_env_file=None)
    assert s.isolarcloud_base_url == "https://custom.isolarcloud.com"
    assert s.isolarcloud_poll_interval_seconds == 60
    assert s.aemo_region == "NSW1"
    assert s.wholesale_adder_cents_kwh == 22.5


def test_config_missing_required_vars() -> None:
    """Missing required vars should raise a ValidationError."""
    from pydantic import ValidationError

    with patch.dict(os.environ, {}, clear=True):
        from candela.config import Settings

        with pytest.raises(ValidationError):
            Settings(_env_file=None)


def test_config_missing_isolarcloud_credentials() -> None:
    """DATABASE_URL present but iSolarCloud credentials missing should raise."""
    from pydantic import ValidationError

    env = {"DATABASE_URL": "sqlite+aiosqlite:///./candela.db"}
    with patch.dict(os.environ, env, clear=True):
        from candela.config import Settings

        with pytest.raises(ValidationError):
            Settings(_env_file=None)
