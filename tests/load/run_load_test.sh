#!/bin/bash
#
# RosterIQ Load Test Runner
#
# Usage:
#   ./run_load_test.sh [smoke|load|stress|spike|endurance]
#   ./run_load_test.sh smoke
#   ./run_load_test.sh load --host http://production.example.com
#
# Environment variables:
#   TARGET_HOST        API base URL (default: http://localhost:8000)
#   TANDA_WEBHOOK_SECRET  Webhook signing secret
#   LOCUST_WORKERS     Number of worker processes (default: 1)

set -e

# Default values
PROFILE="${1:-smoke}"
TARGET_HOST="${TARGET_HOST:-http://localhost:8000}"
LOCUST_WORKERS="${LOCUST_WORKERS:-1}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPORT_DIR="${SCRIPT_DIR}/reports"

# Parse command line arguments
LOCUST_ARGS=""
for arg in "$@"; do
    case "$arg" in
        smoke|load|stress|spike|endurance)
            PROFILE="$arg"
            ;;
        --host=*)
            TARGET_HOST="${arg#--host=}"
            ;;
        --host)
            TARGET_HOST="$2"
            shift
            ;;
        *)
            if [[ ! "$arg" =~ ^(smoke|load|stress|spike|endurance)$ ]]; then
                LOCUST_ARGS="$LOCUST_ARGS $arg"
            fi
            ;;
    esac
done

# Validate profile
case "$PROFILE" in
    smoke|load|stress|spike|endurance)
        ;;
    *)
        echo "Usage: $0 [smoke|load|stress|spike|endurance] [options]"
        echo ""
        echo "Available profiles:"
        echo "  smoke       - Quick sanity check (5 users, 30s)"
        echo "  load        - Moderate load (50 users, 5min)"
        echo "  stress      - High stress (100 users, 10min)"
        echo "  spike       - Rapid spike (200 users peak, 5min)"
        echo "  endurance   - Long running (50 users, 30min)"
        echo ""
        echo "Options:"
        echo "  --host URL  - Target host (default: http://localhost:8000)"
        exit 1
        ;;
esac

# Create reports directory
mkdir -p "$REPORT_DIR"

# Get profile settings
case "$PROFILE" in
    smoke)
        NUM_USERS=5
        SPAWN_RATE=1
        DURATION=30
        ;;
    load)
        NUM_USERS=50
        SPAWN_RATE=10
        DURATION=300
        ;;
    stress)
        NUM_USERS=100
        SPAWN_RATE=20
        DURATION=600
        ;;
    spike)
        NUM_USERS=200
        SPAWN_RATE=50
        DURATION=300
        ;;
    endurance)
        NUM_USERS=50
        SPAWN_RATE=5
        DURATION=1800
        ;;
esac

# Generate report filename with timestamp
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
REPORT_FILE="${REPORT_DIR}/rosteriq_${PROFILE}_${TIMESTAMP}.html"
CSV_FILE="${REPORT_DIR}/rosteriq_${PROFILE}_${TIMESTAMP}.csv"

echo "======================================================================"
echo "RosterIQ Load Test: $PROFILE"
echo "======================================================================"
echo "Target Host:      $TARGET_HOST"
echo "Users:            $NUM_USERS"
echo "Spawn Rate:       $SPAWN_RATE users/sec"
echo "Duration:         $DURATION seconds"
echo "Report:           $REPORT_FILE"
echo "======================================================================"
echo ""

# Run locust
locust \
    -f "${SCRIPT_DIR}/locustfile.py" \
    --host="$TARGET_HOST" \
    --users="$NUM_USERS" \
    --spawn-rate="$SPAWN_RATE" \
    --run-time="${DURATION}s" \
    --headless \
    --csv="$CSV_FILE" \
    --html="$REPORT_FILE" \
    --loglevel=INFO \
    $LOCUST_ARGS

EXIT_CODE=$?

echo ""
echo "======================================================================"
echo "Test Complete"
echo "======================================================================"
echo "Report:     $REPORT_FILE"
echo "CSV Data:   $CSV_FILE"
echo "======================================================================"
echo ""

exit $EXIT_CODE
