FROM python:3.14-slim@sha256:0514bc76712d64f8bb48adcb95b1ab0d88c69fee95d62fe4c18ee948bee9b1e1

# pip vendors its own certifi and ignores PIP_CERT. Fail the build when the
# base image still ships a pip older than the emSign CA fix (26.2.2).
RUN python -c "import pip; v=tuple(int(p) for p in pip.__version__.split('.')[:3]); assert v>=(26,2,2), pip.__version__"

WORKDIR /app

# Install uv. Tag and digest stay in lockstep with aqua.yaml and CI setup-uv.
COPY --from=ghcr.io/astral-sh/uv:0.13.7@sha256:96f40d3904aad8d0bd010539001d36c8e2188b981e1334471a90c4a1d93bb490 /uv /usr/local/bin/uv

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
