"""Candela FastAPI application entry point."""

import logging
from contextlib import asynccontextmanager
from typing import AsyncGenerator

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from candela.config import get_settings
from candela.db import Database

logger = logging.getLogger(__name__)

_db: Database | None = None


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


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}
