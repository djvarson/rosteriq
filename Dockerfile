# RosterIQ Production Dockerfile
# Multi-stage build for optimized image size and security

# ============================================================================
# Stage 1: Builder
# ============================================================================
FROM python:3.11-slim as builder

# Install build dependencies needed for some packages (psycopg2, cryptography)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Create virtual environment in builder
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip setuptools wheel && \
    pip install --no-cache-dir -r requirements.txt

# ============================================================================
# Stage 2: Runtime
# ============================================================================
FROM python:3.11-slim

# Install only runtime dependencies (libpq for psycopg2)
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user for security
RUN useradd -m -u 1000 appuser

# Copy virtual environment from builder
COPY --from=builder /opt/venv /opt/venv

# Set working directory
WORKDIR /app

# Set Python environment variables
ENV PATH="/opt/venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH="/app" \
    ENVIRONMENT=production

# Copy application code
COPY --chown=appuser:appuser pyproject.toml .
COPY --chown=appuser:appuser RosterIQ/ ./rosteriq/
COPY --chown=appuser:appuser tests/ ./tests/

# Install the application in production mode
RUN pip install --no-cache-dir -e . && \
    chown -R appuser:appuser /app

# Syntax check all Python files at build time
RUN python -m py_compile $(find /app/rosteriq -name '*.py') && \
    echo "✓ All Python files syntax valid"

# Switch to non-root user
USER appuser

# Expose port
EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=5s --retries=3 --start-period=10s \
    CMD curl -f http://localhost:8000/health || exit 1

# Default command: run migrations then start uvicorn
CMD sh -c "python -m rosteriq.migrations.run_migrations --run && uvicorn rosteriq.api:app --host 0.0.0.0 --port 8000 --workers 2"
