"""Application configuration via pydantic-settings.

All settings are read from environment variables. Copy .env.example to .env
(or .env.dev for local development) and populate accordingly.
"""

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    database_url: str
    secret_key: str
    auth_username: str
    auth_password: str
    api_key: str | None = None
    isolarcloud_app_key: str
    isolarcloud_username: str
    isolarcloud_password: str  # stored plaintext in env, MD5'd at auth time
    isolarcloud_base_url: str = "https://augateway.isolarcloud.com"
    isolarcloud_poll_interval_seconds: int = 300
    aemo_region: str = "QLD1"
    wholesale_adder_cents_kwh: float = 18.0

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}


def get_settings() -> "Settings":
    """Return the application settings. Reads from environment on each call.

    In application code, cache via ``functools.lru_cache`` on the call site if
    you need a singleton. Tests can call ``Settings()`` directly with a patched
    environment without affecting other tests.
    """
    return Settings()
