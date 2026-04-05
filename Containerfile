FROM python:3.14-slim

WORKDIR /app

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Copy dependency manifests first for layer caching
COPY pyproject.toml uv.lock* ./

# Install production dependencies only (no dev extras)
RUN uv sync --frozen --no-dev

# Copy application source
COPY candela/ ./candela/
COPY migrations/ ./migrations/
COPY alembic.ini ./

# Both Quadlet units use this image; CMD is overridden per unit.
# Default: run the web server.
CMD ["uv", "run", "uvicorn", "candela.main:app", "--host", "0.0.0.0", "--port", "8000"]
