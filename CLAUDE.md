# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Before marking a task complete

Always run these before reporting done:

```bash
uv run pytest
uv run pyrefly check candela/ tests/
```

## Commands

```bash
uv sync                                        # install all deps (including dev)
uv run uvicorn candela.main:app --reload       # run dev server
uv run pytest                                  # run all tests
uv run pytest tests/test_db.py::test_name -v  # run a single test
uv run coverage run -m pytest && uv run coverage report  # tests with coverage
uv run ruff check .                            # lint
uv run ruff format --check .                   # format check
uv run pyrefly check candela/ tests/                           # type check
uv run prek install                            # install git hooks
uv run prek run --all-files                    # run git hooks
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

## Git hooks (prek)

Hooks run on `git commit` via [prek](https://prek.j178.dev/) (`prek.toml`). Install with `uv run prek install`. Run manually with `uv run prek run --all-files`.

- **ruff**: lint with auto-fix, then format.
- **pytest** and **pyrefly**: full test suite and type check (`language = system`, so they use the project venv).
- **pre-commit-hooks**: merge conflict markers, large files (>500KB), EOF newlines, trailing whitespace.

## Dependency updates

Hosted [Mend Renovate](https://github.com/apps/renovate); config is `renovate.json5`. Dependabot version updates are not used; keep the GitHub dependency graph and Dependabot alerts.

- One non-major PR on the 1st and 15th (`Australia/Brisbane`). Majors are separate PRs in the same window. Nothing automerges.
- Python dependencies are exact pins (`==`) so `uv lock` cannot jump past the version Renovate selected (`rangeStrategy: "pin"`).
- uv toolchain (`aqua.yaml` `astral-sh/uv`, GHCR `ghcr.io/astral-sh/uv` image, CI `setup-uv` `version:`) is always one isolated `uv` PR (all update types, including 0.x minors and 1.x).
- Python `3.14` → `3.15` is a **minor**, isolated as `python runtime` and Dashboard-gated. Do not merge until `requires-python` and `[tool.pyrefly] python-version` move in the same commit.
- `minimumReleaseAge: "7 days"` where the registry publishes timestamps. The GitHub release of `astral-sh/uv` is the clock for the uv group (GHCR is timestamp-optional on that docker member only).
- Pending updates live on the Dependency Dashboard issue. Dashboard “Run now” starts a job; it does **not** skip the schedule for new PRs.
