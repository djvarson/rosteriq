# RosterIQ Production Dockerfile
# Simplified for flat repo structure (source files at repo root)

# ============================================================================
# Stage 1: Builder
# ============================================================================
FROM python:3.11-slim as builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
        && rm -rf /var/lib/apt/lists/*

RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip setuptools wheel && \
    pip install --no-cache-dir -r requirements.txt

# ============================================================================
# Stage 2: Runtime
# ============================================================================
FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 \
    curl \
    libgomp1 \
    && rm -rf /var/lib/apt/lists/*
# libgomp1: OpenMP runtime required by xgboost's shared library. Without it,
# `import xgboost` fails at dlopen (the import guard now degrades gracefully,
# but then forecasting silently loses the XGBoost model).

RUN useradd -m -u 1000 appuser

COPY --from=builder /opt/venv /opt/venv

WORKDIR /app

ENV PATH="/opt/venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH="/app" \
    ENVIRONMENT=production

# Copy entire repo as the rosteriq package (repo root = package root)
COPY --chown=appuser:appuser . ./rosteriq/
# Copy pyproject.toml to app root
COPY --chown=appuser:appuser pyproject.toml .

RUN chown -R appuser:appuser /app

USER appuser

EXPOSE 8000

# Bind to $PORT when the platform injects one (Railway/Render/Heroku),
# defaulting to 8000. The healthcheck uses the same value.
HEALTHCHECK --interval=30s --timeout=5s --retries=3 --start-period=10s \
    CMD curl -f "http://localhost:${PORT:-8000}/health" || exit 1

CMD ["sh", "-c", "python -m rosteriq.migrations.run_migrations --run && exec uvicorn rosteriq.api:app --host 0.0.0.0 --port ${PORT:-8000} --workers 2"]
