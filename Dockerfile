# Production Dockerfile for RosterIQ API
# Multi-stage build for minimal production image

FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies required for Python packages
# gcc: Required for compiling some Python packages
# libpq-dev: Required for psycopg2 PostgreSQL adapter
# curl: Required for health checks
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better Docker layer caching
# This layer only rebuilds if requirements change
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
# rosteriq package contains the core API and business logic
COPY rosteriq/ ./rosteriq/
# dashboard contains the frontend assets
COPY dashboard/ ./dashboard/

# Expose the port the API listens on
EXPOSE 8000

# Health check: Verify the API is responding
# Interval: Check every 30s, timeout: 10s, start grace: 40s, max retries: 3
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

# Run the application with uvicorn
# host 0.0.0.0 ensures the server listens on all network interfaces
# port 8000 is the standard port
# workers: Single worker for starter deployments (scale horizontally)
CMD ["uvicorn", "rosteriq.api:app", "--host", "0.0.0.0", "--port", "8000"]
