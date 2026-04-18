"""Tests for candela.db — database abstraction layer."""

import pytest


def test_detect_sqlite_backend() -> None:
    """SQLite URL should be detected as the sqlite backend."""
    from candela.db import detect_backend

    assert detect_backend("sqlite+aiosqlite:///./candela.db") == "sqlite"


def test_detect_postgres_backend() -> None:
    """PostgreSQL URL should be detected as the postgres backend."""
    from candela.db import detect_backend

    assert (
        detect_backend("postgresql+asyncpg://user:pass@localhost/candela") == "postgres"
    )


def test_detect_unknown_backend_raises() -> None:
    """Unknown URL scheme should raise a ValueError."""
    from candela.db import detect_backend

    with pytest.raises(ValueError, match="Unsupported"):
        detect_backend("mysql+aiomysql://user:pass@localhost/db")


@pytest.mark.asyncio
async def test_sqlite_pool_acquire_and_execute() -> None:
    """Can acquire a connection from a SQLite pool and execute a query."""
    from candela.db import Database

    db = Database("sqlite+aiosqlite:///./test_candela_tmp.db")
    await db.connect()
    try:
        result = await db.fetchval("SELECT 42")
        assert result == 42
    finally:
        await db.disconnect()
        import os as _os

        _os.unlink("test_candela_tmp.db")


@pytest.mark.asyncio
async def test_sqlite_execute_and_fetch() -> None:
    """Can create a table, insert a row, and fetch it back."""
    import os as _os
    from candela.db import Database

    db = Database("sqlite+aiosqlite:///./test_candela_crud.db")
    await db.connect()
    try:
        await db.execute(
            "CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY, val TEXT NOT NULL)"
        )
        await db.execute("INSERT INTO t (id, val) VALUES (?, ?)", 1, "hello")
        row = await db.fetchrow("SELECT id, val FROM t WHERE id = ?", 1)
        assert row is not None
        assert row["val"] == "hello"

        rows = await db.fetch("SELECT id, val FROM t")
        assert len(rows) == 1
        assert rows[0]["id"] == 1
    finally:
        await db.disconnect()
        _os.unlink("test_candela_crud.db")


@pytest.mark.asyncio
async def test_sqlite_fetchval_no_rows() -> None:
    """fetchval returns None when no rows match."""
    import os as _os
    from candela.db import Database

    db = Database("sqlite+aiosqlite:///./test_candela_empty.db")
    await db.connect()
    try:
        await db.execute("CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY)")
        result = await db.fetchval("SELECT id FROM t WHERE id = ?", 999)
        assert result is None
    finally:
        await db.disconnect()
        _os.unlink("test_candela_empty.db")
