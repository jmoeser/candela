# Candela

A self-hosted solar analytics app for Australian households. Polls the iSolarCloud API every 5 minutes, tracks electricity costs against multiple tariff structures, and detects high-draw loads like EV charging and hot water heat pumps.

## Features

- Real-time solar generation, grid import/export, and home load monitoring
- Cost modelling across multiple tariff plans (single rate, time-of-use, demand, wholesale spot)
- Load disaggregation — EV charging and heat pump event detection
- AEMO wholesale price tracking
- Web UI with live-updating dashboard (HTMX, no JS build step)
- JSON API for external integrations

## Requirements

- Python 3.14+
- SQLite (development) or PostgreSQL (production)
- iSolarCloud account with API credentials

## Quick Start

```bash
cp .env.example .env        # fill in required variables
uv sync                     # install dependencies
DATABASE_URL=sqlite+aiosqlite:///./candela.db uv run alembic upgrade head
uv run uvicorn candela.main:app --reload    # web server on :8000
uv run python -m candela.collector.poller  # collector in a second terminal
```

## Configuration

All configuration is via environment variables (or a `.env` file).

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `DATABASE_URL` | Yes | — | `sqlite+aiosqlite:///./dev.db` or `postgresql+asyncpg://...` |
| `SECRET_KEY` | Yes | — | Random secret for session cookies |
| `AUTH_USERNAME` | Yes | — | Web UI login username |
| `AUTH_PASSWORD` | Yes | — | Web UI login password |
| `API_KEY` | No | — | Optional bearer token for API access |
| `ISOLARCLOUD_APP_KEY` | Yes | — | iSolarCloud API app key |
| `ISOLARCLOUD_ACCESS_KEY` | Yes | — | iSolarCloud API access key |
| `ISOLARCLOUD_USERNAME` | Yes | — | iSolarCloud account username |
| `ISOLARCLOUD_PASSWORD` | Yes | — | iSolarCloud account password (MD5'd at auth time) |
| `ISOLARCLOUD_BASE_URL` | No | `https://augateway.isolarcloud.com` | iSolarCloud API endpoint |
| `ISOLARCLOUD_POLL_INTERVAL_SECONDS` | No | `300` | Polling frequency (minimum 300) |
| `AEMO_REGION` | No | `QLD1` | AEMO region code for wholesale prices |
| `WHOLESALE_ADDER_CENTS_KWH` | No | `18.0` | Network + retail markup on top of wholesale spot price |

## Development

```bash
uv run pytest                                         # run all tests
uv run pytest tests/test_aemo.py::test_name -v       # single test
uv run coverage run -m pytest && uv run coverage report
DATABASE_URL=... uv run alembic revision --autogenerate -m "description"
```

### Architecture

```
candela/
├── api/             JSON API routers (readings, summary, tariffs, loads)
├── collector/       iSolarCloud poller + AEMO price fetcher (APScheduler)
├── disaggregation/  EV and heat pump event detection
├── tariffs/         Cost calculation engine + pluggable tariff strategies
├── web/             Jinja2 templates + HTMX partials + static assets
├── db.py            Async database abstraction (SQLite + PostgreSQL)
├── config.py        Pydantic Settings
└── main.py          FastAPI app + lifespan
```

The database layer uses raw SQL with `?` placeholders (translated to `$1, $2, ...` for asyncpg). No ORM.

## Deployment

Candela is packaged as a container image (see `Containerfile`) and runs as two separate services:

- **web** — runs `alembic upgrade head` then starts Uvicorn on port 8000
- **collector** — runs the polling loop; should start after the web service

```bash
# Build
docker build -t candela .

# Web server (pass config via --env-file or -e flags)
docker run -d --name candela-web -p 8000:8000 --env-file candela.env candela \
  sh -c "uv run alembic upgrade head && uv run uvicorn candela.main:app --host 0.0.0.0 --port 8000"

# Collector
docker run -d --name candela-collector --env-file candela.env candela \
  uv run python -m candela.collector.poller
```

Example Podman Quadlet unit files for running under systemd are in `quadlet/`.

## License

MIT
