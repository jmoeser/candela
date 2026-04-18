"""Alembic migration environment.

Reads DATABASE_URL from the environment and configures the correct SQLAlchemy
dialect automatically:
- postgresql+asyncpg://  → asyncpg (production)
- sqlite+aiosqlite://    → aiosqlite (development)

Migrations use render_as_batch=True so they work on both SQLite and PostgreSQL.
"""

import os
from logging.config import fileConfig

from sqlalchemy import engine_from_config, pool

from alembic import context

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Read DATABASE_URL from environment.  Alembic uses synchronous SQLAlchemy
# drivers for migrations; translate async driver names to their sync equivalents.
_raw_url = os.environ.get("DATABASE_URL", "")
if not _raw_url:
    raise RuntimeError(
        "DATABASE_URL environment variable is not set. "
        "Copy .env.example to .env and set DATABASE_URL before running migrations."
    )

# Translate async driver names → sync equivalents for Alembic's synchronous engine.
_sync_url = _raw_url.replace("postgresql+asyncpg://", "postgresql+psycopg2://").replace(
    "sqlite+aiosqlite://", "sqlite://"
)

config.set_main_option("sqlalchemy.url", _sync_url)

target_metadata = None  # candela uses raw SQL migrations, not ORM metadata


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode (no live DB connection required)."""
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        render_as_batch=True,
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode."""
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            render_as_batch=True,
        )
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
