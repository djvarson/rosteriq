#!/bin/bash
# RosterIQ Production Deployment Script
# Handles database migrations, data seeding, and server startup

set -e  # Exit on any error
set -u  # Exit if undefined variable is used

# Color output for better readability
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'  # No Color

echo -e "${GREEN}=== RosterIQ Deployment Script ===${NC}"

# ==================== Validation ====================
echo -e "${YELLOW}Checking required environment variables...${NC}"

required_vars=(
    "DATABASE_URL"
    "RIQ_JWT_SECRET"
)

for var in "${required_vars[@]}"; do
    if [ -z "${!var:-}" ]; then
        echo -e "${RED}Error: Required environment variable '$var' is not set${NC}"
        exit 1
    fi
done

echo -e "${GREEN}All required variables are set${NC}"

# ==================== Database Setup ====================
echo -e "${YELLOW}Setting up database...${NC}"

# Extract database connection info from DATABASE_URL
# Format: postgresql://username:password@host:port/database
DB_URL=$DATABASE_URL

# Wait for database to be ready (retry logic)
echo "Waiting for PostgreSQL to be ready..."
max_attempts=30
attempt=1

while [ $attempt -le $max_attempts ]; do
    if psql "$DB_URL" -c "SELECT 1" > /dev/null 2>&1; then
        echo -e "${GREEN}Database is ready${NC}"
        break
    fi
    echo "Attempt $attempt/$max_attempts - waiting for database..."
    sleep 2
    ((attempt++))
done

if [ $attempt -gt $max_attempts ]; then
    echo -e "${RED}Error: Could not connect to database after $max_attempts attempts${NC}"
    exit 1
fi

# Run database migrations if schema.sql exists
if [ -f "schema.sql" ]; then
    echo -e "${YELLOW}Running database migrations...${NC}"
    psql "$DB_URL" -f schema.sql
    echo -e "${GREEN}Migrations completed${NC}"
else
    echo -e "${YELLOW}Note: schema.sql not found, skipping migrations${NC}"
fi

# ==================== Data Seeding ====================
echo -e "${YELLOW}Checking if database needs seeding...${NC}"

# Check if venues table has data
venue_count=$(psql "$DB_URL" -t -c "SELECT COUNT(*) FROM venues;" 2>/dev/null || echo "0")

if [ "$venue_count" -eq 0 ]; then
    echo -e "${YELLOW}Database is empty. Running demo seed...${NC}"

    # Check if seed script exists
    if [ -f "scripts/seed_demo.py" ]; then
        python scripts/seed_demo.py
        echo -e "${GREEN}Demo data seeded successfully${NC}"
    else
        echo -e "${YELLOW}Note: seed_demo.py not found, skipping demo data${NC}"
    fi
else
    echo -e "${GREEN}Database already contains data (venues: $venue_count), skipping seed${NC}"
fi

# ==================== Start Application ====================
echo -e "${YELLOW}Starting RosterIQ API server...${NC}"
echo -e "${GREEN}Server is now running on port 8000${NC}"
echo -e "${GREEN}Health check available at: http://localhost:8000/health${NC}"
echo -e "${GREEN}API documentation at: http://localhost:8000/docs${NC}"

# Start the uvicorn server
uvicorn rosteriq.api:app --host 0.0.0.0 --port 8000

