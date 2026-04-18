"""Candela FastAPI application entry point."""

import logging
from contextlib import asynccontextmanager
from pathlib import Path
from typing import AsyncGenerator

from fastapi import FastAPI, Request
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles
from starlette.middleware.sessions import SessionMiddleware

from candela.auth import LoginRequired
from candela.config import get_settings
from candela.db import Database

logger = logging.getLogger(__name__)

_db: Database | None = None

_STATIC_DIR = Path(__file__).parent / "web" / "static"


def get_db() -> Database:
    """Return the application database instance."""
    if _db is None:
        raise RuntimeError("Database not initialised — lifespan not running?")
    return _db


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Connect to the database on startup; disconnect on shutdown."""
    global _db
    s = get_settings()
    _db = Database(s.database_url)
    await _db.connect()
    logger.info("Database connected (%s)", s.database_url.split("://")[0])
    try:
        yield
    finally:
        await _db.disconnect()
        _db = None
        logger.info("Database disconnected")


app = FastAPI(title="Candela", lifespan=lifespan)

_settings = get_settings()
app.add_middleware(
    SessionMiddleware,
    secret_key=_settings.secret_key,
    max_age=14 * 24 * 3600,
    same_site="lax",
    https_only=False,
)


@app.exception_handler(LoginRequired)
async def login_required_handler(
    request: Request, exc: LoginRequired
) -> RedirectResponse:
    return RedirectResponse("/login", status_code=303)


# Static files (CSS, JS assets)
app.mount("/static", StaticFiles(directory=_STATIC_DIR), name="static")

# ---------------------------------------------------------------------------
# Routers
# ---------------------------------------------------------------------------

from candela.api import readings as _readings  # noqa: E402
from candela.api import summary as _summary  # noqa: E402
from candela.api import tariffs as _tariffs  # noqa: E402
from candela.api import loads as _loads  # noqa: E402
from candela.web import routes as _web  # noqa: E402

app.include_router(_readings.router, prefix="/api/v1", tags=["readings"])  # type: ignore[has-type]
app.include_router(_summary.router, prefix="/api/v1", tags=["summary"])  # type: ignore[has-type]
app.include_router(_tariffs.router, prefix="/api/v1", tags=["tariffs"])  # type: ignore[has-type]
app.include_router(_loads.router, prefix="/api/v1", tags=["loads"])  # type: ignore[has-type]
app.include_router(_web.router, tags=["web"])  # type: ignore[has-type]


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}
