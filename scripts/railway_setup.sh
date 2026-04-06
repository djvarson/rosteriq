#!/bin/bash
# RosterIQ Railway.app Deployment Setup Script
# Initialises database, seeds demo data, and starts the FastAPI server
# This is the startup script that Railway will execute

set -e  # Exit on any error
set -u  # Exit if undefined variable is used

# Color output for better readability
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'  # No Color

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

log_step() {
    echo -e "\n${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}\n"
}

log_step "RosterIQ Railway.app Deployment Setup"

# ==================== Environment Validation ====================
log_step "Step 1: Validating Environment Variables"

required_vars=(
    "DATABASE_URL"
    "RIQ_JWT_SECRET"
    "PORT"
)

missing_vars=()
for var in "${required_vars[@]}"; do
    if [ -z "${!var:-}" ]; then
        missing_vars+=("$var")
    fi
done

if [ ${#missing_vars[@]} -gt 0 ]; then
    log_error "Missing required environment variables: ${missing_vars[*]}"
    log_error "Please ensure the following are set in Railway dashboard:"
    for var in "${missing_vars[@]}"; do
        echo "  - $var"
    done
    exit 1
fi

log_info "DATABASE_URL is set"
log_info "RIQ_JWT_SECRET is set"
log_info "PORT=${PORT}"

# ==================== Database Readiness ====================
log_step "Step 2: Waiting for Database Connection"

# Extract database URL components (basic parsing)
DB_URL="${DATABASE_URL}"

# Wait for database to be ready with exponential backoff
max_attempts=60
attempt=1
wait_time=1

log_info "Attempting to connect to PostgreSQL..."

while [ $attempt -le $max_attempts ]; do
    if psql "$DB_URL" -c "SELECT 1" > /dev/null 2>&1; then
        log_info "Database is ready (attempt $attempt)"
        break
    fi

    if [ $attempt -eq 1 ] || [ $((attempt % 10)) -eq 0 ]; then
        log_warn "Database not ready yet. Attempt $attempt/$max_attempts. Waiting ${wait_time}s..."
    fi

    sleep "$wait_time"
    ((attempt++))

    # Increase wait time for subsequent attempts (backoff)
    if [ $wait_time -lt 5 ]; then
        ((wait_time++))
    fi
done

if [ $attempt -gt $max_attempts ]; then
    log_error "Could not connect to database after $max_attempts attempts ($(($max_attempts * 5))s)"
    log_error "Check DATABASE_URL configuration in Railway dashboard"
    exit 1
fi

log_info "Successfully connected to database"

# ==================== Database Initialization ====================
log_step "Step 3: Initializing Database Schema"

# Check if schema.sql exists
if [ -f "schema.sql" ]; then
    log_info "Running database migrations from schema.sql..."

    if psql "$DB_URL" -f schema.sql > /dev/null 2>&1; then
        log_info "Database schema initialized successfully"
    else
        log_warn "Schema migration completed (some statements may have been idempotent)"
    fi
else
    log_warn "schema.sql not found - skipping schema migrations"
    log_info "Ensure your database has the required schema"
fi

# ==================== Data Seeding ====================
log_step "Step 4: Seeding Demo Data"

# Check if venues table exists and has data
if psql "$DB_URL" -t -c "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name='venues')" 2>/dev/null | grep -q "t"; then
    venue_count=$(psql "$DB_URL" -t -c "SELECT COUNT(*) FROM venues;" 2>/dev/null || echo "0")

    if [ "$venue_count" -eq 0 ]; then
        log_info "Database is empty. Running demo seed script..."

        if [ -f "scripts/seed_demo.py" ]; then
            if python scripts/seed_demo.py; then
                log_info "Demo data seeded successfully"
                log_info "Demo user: demo@rosteriq.local / DemoPass123!"
                log_info "Demo venue: The Royal Oak (Fitzroy, VIC)"
            else
                log_warn "Demo seed script failed - database may not be ready"
                log_info "You can seed data manually later"
            fi
        else
            log_warn "seed_demo.py not found - skipping demo data seeding"
        fi
    else
        log_info "Database already contains data ($venue_count venue(s))"
        log_info "Skipping seed to preserve existing data"
    fi
else
    log_warn "venues table does not exist - schema may not be initialized"
fi

# ==================== Application Startup ====================
log_step "Step 5: Starting RosterIQ API Server"

log_info "Starting uvicorn on 0.0.0.0:${PORT}"
log_info "Health check: http://localhost:${PORT}/health"
log_info "API docs: http://localhost:${PORT}/docs"
log_info "Press Ctrl+C to stop the server"

echo ""
log_info "Server starting..."
echo ""

# Start the uvicorn server
# Railway will inject $PORT environment variable
exec uvicorn rosteriq.api:app \
    --host 0.0.0.0 \
    --port "${PORT}" \
    --log-level info
