FROM python:3.14-slim@sha256:51dafde81dbdb6ebde285137a295cf18a47ca95234fe388a343719cb97305b3d

WORKDIR /app

# Install uv. Tag and digest stay in lockstep with aqua.yaml and CI setup-uv.
COPY --from=ghcr.io/astral-sh/uv:0.12.17@sha256:10787c682e4184e4f290de1171fd4703dc63de99221f10fe1c99002ce7fa9acc /uv /usr/local/bin/uv

# Copy dependency manifests first for layer caching
COPY pyproject.toml uv.lock ./

# Install production dependencies only (no dev extras)
RUN uv sync --frozen --no-dev

# Copy application source
COPY candela/ ./candela/
COPY migrations/ ./migrations/
COPY alembic.ini ./
COPY entrypoint.sh ./
RUN chmod +x entrypoint.sh

# Run Alembic migrations before starting any process.
ENTRYPOINT ["./entrypoint.sh"]

# Both Quadlet units use this image; CMD is overridden per unit.
# Default: run the web server.
CMD ["uv", "run", "uvicorn", "candela.main:app", "--host", "0.0.0.0", "--port", "8000"]
