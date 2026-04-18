# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Before marking a task complete

Always run these before reporting done:

```bash
uv run pytest
uv run mypy candela/
```

## Commands

```bash
uv sync                                        # install all deps (including dev)
uv run uvicorn candela.main:app --reload       # run dev server
uv run pytest                                  # run all tests
uv run pytest tests/test_db.py::test_name -v  # run a single test
uv run coverage run -m pytest && uv run coverage report  # tests with coverage
DATABASE_URL=... uv run alembic upgrade head   # run migrations
DATABASE_URL=... uv run alembic revision --autogenerate -m "description"  # new migration
```

## Architecture

Candela is a solar analytics app: it polls the iSolarCloud API every 5 minutes, models electricity costs against multiple tariff structures, and does load disaggregation (EV charging, hot water heat pump detection). Deployed on Fedora CoreOS via Podman Quadlet as two separate systemd container units — web server and collector poller.

### Database abstraction (`candela/db.py`)

Single `Database` class works over both SQLite (dev, `aiosqlite`) and PostgreSQL (prod, `asyncpg`). Key rules:
- **Always use `?` placeholders** in SQL — `db.py` translates to `$1, $2, ...` for asyncpg
- No ORMs, no SQLAlchemy ORM layer — raw SQL only
- Write portable SQL: no `SERIAL` (use `INTEGER` autoincrement), no `RETURNING`, no `NOW()` (use `CURRENT_TIMESTAMP`), use `sa.DateTime(timezone=True)` in Alembic migrations
- `_Row` supports both `row['col']` and `row.col` access

### Configuration (`candela/config.py`)

Pydantic Settings; all config from environment variables or `.env` file. `get_settings()` returns a fresh instance (not a singleton) so tests can patch it. Required vars: `DATABASE_URL`, `SECRET_KEY`, `AUTH_USERNAME`, `AUTH_PASSWORD`, `ISOLARCLOUD_APP_KEY`, `ISOLARCLOUD_ACCESS_KEY`, `ISOLARCLOUD_USERNAME`, `ISOLARCLOUD_PASSWORD`.

### App lifecycle (`candela/main.py`)

`_db` is a module-level singleton initialised in the FastAPI lifespan context manager. Use `get_db()` to access it in routes and dependencies.

### Module layout

- `candela/collector/` — iSolarCloud API polling (APScheduler), AEMO wholesale fetcher
- `candela/tariffs/strategies/` — pluggable tariff calculation (single rate, TOU, demand, wholesale)
- `candela/disaggregation/` — EV and hot water heat pump event detection
- `candela/api/` — JSON API routers
- `candela/web/templates/` + `candela/web/static/` — Jinja2 + HTMX (no frontend build)
- `migrations/versions/` — Alembic migration scripts

### Testing

`asyncio_mode = "auto"` is set in `pyproject.toml`, so async tests work without decorators. Tests use an in-memory or temp SQLite database — never a real Postgres instance.

### Deployment

Two Podman Quadlet units in `quadlet/`: `candela-web.container` (runs `alembic upgrade head` then uvicorn) and `candela-collector.container` (runs `python -m candela.collector.poller`, depends on web). Both read env from `/etc/candela/candela.env`.
