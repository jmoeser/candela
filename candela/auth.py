"""Authentication dependencies for web and API routes."""

import secrets

from fastapi import HTTPException, Request

from candela.config import get_settings


class LoginRequired(Exception):
    """Raised by web route dependencies when the session is not authenticated."""


async def require_auth(request: Request) -> None:
    """Redirect to /login if the session is not authenticated."""
    if not request.session.get("authenticated"):
        raise LoginRequired()


async def require_api_key(request: Request) -> None:
    """Require a matching X-Api-Key header if an API key is configured.

    When ``API_KEY`` is not set in the environment, all requests are allowed.
    """
    configured_key = get_settings().api_key
    if configured_key is None:
        return
    provided = request.headers.get("X-Api-Key", "")
    if not secrets.compare_digest(provided, configured_key):
        raise HTTPException(status_code=401, detail="Invalid or missing API key")
