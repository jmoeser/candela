"""Database abstraction layer.

Detects the backend from DATABASE_URL and provides a unified async interface
over both asyncpg (PostgreSQL, production) and aiosqlite (SQLite, development).

Usage
-----
    from candela.db import Database

    db = Database(settings.database_url)
    await db.connect()
    row = await db.fetchrow("SELECT * FROM solar_readings WHERE ts = ?", ts)
    await db.disconnect()

Parameter style
---------------
Always use ``?`` positional placeholders in SQL statements. The Database class
translates ``?`` to ``$1, $2, ...`` for asyncpg automatically.

SQL compatibility
-----------------
Keep SQL portable between SQLite and PostgreSQL:
- Use CURRENT_TIMESTAMP, not NOW()
- Avoid RETURNING clauses where possible
- Use INTEGER (not SERIAL/BIGSERIAL) for integer PKs in migrations; Alembic
  handles autoincrement portably
- Use sa.DateTime(timezone=True) in Alembic models — rendered as TIMESTAMPTZ
  on Postgres and TEXT on SQLite
"""

import logging
import re
from typing import Any

import aiosqlite

logger = logging.getLogger(__name__)


def detect_backend(database_url: str) -> str:
    """Return ``'sqlite'`` or ``'postgres'`` from a DATABASE_URL string.

    Raises ``ValueError`` for unsupported schemes.
    """
    if database_url.startswith("sqlite"):
        return "sqlite"
    if database_url.startswith("postgresql"):
        return "postgres"
    raise ValueError(f"Unsupported DATABASE_URL scheme: {database_url!r}")


def _sqlite_path(database_url: str) -> str:
    """Extract the filesystem path from a ``sqlite+aiosqlite://`` URL."""
    # sqlite+aiosqlite:///./candela.db  → ./candela.db
    # sqlite+aiosqlite:////abs/path.db  → /abs/path.db
    # SQLAlchemy convention: sqlite:///relative → 3 slashes strip to "relative"
    #                        sqlite:////abs     → 4 slashes strip to "/abs"
    match = re.match(r"sqlite\+aiosqlite:///(.+)", database_url)
    if not match:
        raise ValueError(f"Cannot parse SQLite path from URL: {database_url!r}")
    return match.group(1)


def _pg_dsn(database_url: str) -> str:
    """Strip the ``+asyncpg`` driver suffix so asyncpg accepts the DSN."""
    return database_url.replace("postgresql+asyncpg://", "postgresql://")


class _Row(dict):
    """Dict subclass that also supports attribute-style access."""

    def __getattr__(self, name: str) -> Any:
        try:
            return self[name]
        except KeyError:
            raise AttributeError(name) from None


def _to_row(mapping: Any) -> _Row:
    return _Row(mapping)


class Database:
    """Unified async database interface for SQLite and PostgreSQL.

    Parameters
    ----------
    url:
        DATABASE_URL string. Determines which backend driver is used.
    """

    def __init__(self, url: str) -> None:
        self._url = url
        self._backend = detect_backend(url)
        self._sqlite_conn: aiosqlite.Connection | None = None
        self._pg_pool: Any = None  # asyncpg.Pool, imported lazily

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def connect(self) -> None:
        """Open the database connection / pool."""
        if self._backend == "sqlite":
            path = _sqlite_path(self._url)
            self._sqlite_conn = await aiosqlite.connect(path)
            self._sqlite_conn.row_factory = aiosqlite.Row
            # Enable WAL for better concurrency
            await self._sqlite_conn.execute("PRAGMA journal_mode=WAL")
            await self._sqlite_conn.execute("PRAGMA foreign_keys=ON")
            logger.debug("SQLite connection opened: %s", path)
        else:
            import asyncpg  # noqa: PLC0415 — lazy import avoids hard dep in dev

            self._pg_pool = await asyncpg.create_pool(_pg_dsn(self._url))
            logger.debug("asyncpg pool opened")

    async def disconnect(self) -> None:
        """Close the database connection / pool."""
        if self._backend == "sqlite" and self._sqlite_conn is not None:
            await self._sqlite_conn.close()
            self._sqlite_conn = None
            logger.debug("SQLite connection closed")
        elif self._backend == "postgres" and self._pg_pool is not None:
            await self._pg_pool.close()
            self._pg_pool = None
            logger.debug("asyncpg pool closed")

    # ------------------------------------------------------------------
    # Query helpers
    # ------------------------------------------------------------------

    def _pg_params(
        self, sql: str, args: tuple[Any, ...]
    ) -> tuple[str, tuple[Any, ...]]:
        """Replace ``?`` placeholders with ``$1, $2, ...`` for asyncpg."""
        idx = 0

        def replace(_: re.Match) -> str:  # type: ignore[type-arg]
            nonlocal idx
            idx += 1
            return f"${idx}"

        return re.sub(r"\?", replace, sql), args

    async def execute(self, sql: str, *args: Any) -> None:
        """Execute a statement that returns no rows (INSERT, UPDATE, CREATE…)."""
        if self._backend == "sqlite":
            assert self._sqlite_conn is not None
            await self._sqlite_conn.execute(sql, args)
            await self._sqlite_conn.commit()
        else:
            assert self._pg_pool is not None
            pg_sql, pg_args = self._pg_params(sql, args)
            async with self._pg_pool.acquire() as conn:
                await conn.execute(pg_sql, *pg_args)

    async def fetch(self, sql: str, *args: Any) -> list[_Row]:
        """Return all matching rows as a list of ``_Row`` dicts."""
        if self._backend == "sqlite":
            assert self._sqlite_conn is not None
            async with self._sqlite_conn.execute(sql, args) as cursor:
                rows = await cursor.fetchall()
            return [_Row(dict(row)) for row in rows]
        else:
            assert self._pg_pool is not None
            pg_sql, pg_args = self._pg_params(sql, args)
            async with self._pg_pool.acquire() as conn:
                rows = await conn.fetch(pg_sql, *pg_args)
            return [_to_row(dict(r)) for r in rows]

    async def fetchrow(self, sql: str, *args: Any) -> _Row | None:
        """Return the first matching row, or ``None`` if no rows match."""
        if self._backend == "sqlite":
            assert self._sqlite_conn is not None
            async with self._sqlite_conn.execute(sql, args) as cursor:
                row = await cursor.fetchone()
            return _Row(dict(row)) if row is not None else None
        else:
            assert self._pg_pool is not None
            pg_sql, pg_args = self._pg_params(sql, args)
            async with self._pg_pool.acquire() as conn:
                row = await conn.fetchrow(pg_sql, *pg_args)
            return _to_row(dict(row)) if row is not None else None

    async def fetchval(self, sql: str, *args: Any) -> Any:
        """Return the first column of the first matching row, or ``None``."""
        row = await self.fetchrow(sql, *args)
        if row is None:
            return None
        return next(iter(row.values()))
