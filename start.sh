#!/usr/bin/env bash
# ============================================================================
# RosterIQ — One-command startup
# ============================================================================
#
# Usage:
#   ./start.sh              # Start everything (Docker Compose)
#   ./start.sh dev          # Local dev mode (no Docker, in-memory store)
#   ./start.sh stop         # Stop all containers
#   ./start.sh logs         # Tail container logs
#   ./start.sh demo         # Run CLI demo
#   ./start.sh test         # Run full test suite
#
# Prerequisites:
#   Docker mode:  Docker + Docker Compose
#   Dev mode:     Python 3.11+, pip install -e ".[dev]"
# ============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m'

banner() {
    echo -e "${BLUE}"
    echo "  ╔══════════════════════════════════════╗"
    echo "  ║         RosterIQ v0.1.0              ║"
    echo "  ║   AI-Powered Rostering for Hospo     ║"
    echo "  ╚══════════════════════════════════════╝"
    echo -e "${NC}"
}

case "${1:-up}" in

    up|start)
        banner
        echo -e "${GREEN}Starting RosterIQ stack...${NC}"
        echo ""

        if ! command -v docker &> /dev/null; then
            echo -e "${RED}Docker not found. Install Docker Desktop first:${NC}"
            echo "  https://docs.docker.com/get-docker/"
            exit 1
        fi

        docker compose up --build -d

        echo ""
        echo -e "${GREEN}✅ RosterIQ is running!${NC}"
        echo ""
        echo -e "  Dashboard:   ${BLUE}http://localhost:3000${NC}"
        echo -e "  API:         ${BLUE}http://localhost:8000${NC}"
        echo -e "  API docs:    ${BLUE}http://localhost:8000/docs${NC}"
        echo -e "  PostgreSQL:  ${BLUE}localhost:5432${NC} (rosteriq/rosteriq_dev_2026)"
        echo ""
        echo -e "  ${YELLOW}Tip: Run ./start.sh logs to see container output${NC}"
        echo ""

        # Auto-seed demo data
        echo -e "${BLUE}Seeding demo data...${NC}"
        sleep 3
        curl -s -X POST http://localhost:8000/demo/load | python3 -m json.tool 2>/dev/null || echo "(API still starting — refresh dashboard in a few seconds)"
        echo ""
        ;;

    dev)
        banner
        echo -e "${GREEN}Starting local dev server (in-memory, no Docker)...${NC}"
        echo ""

        if ! python3 -c "import rosteriq" 2>/dev/null; then
            echo -e "${YELLOW}Installing rosteriq in dev mode...${NC}"
            pip install -e ".[dev]" || pip install -e .
        fi

        echo -e "  Dashboard:  Open ${BLUE}dashboard/index.html${NC} in your browser"
        echo -e "  API:        ${BLUE}http://localhost:8000${NC}"
        echo -e "  API docs:   ${BLUE}http://localhost:8000/docs${NC}"
        echo ""

        python3 -m uvicorn rosteriq.api:app --reload --host 0.0.0.0 --port 8000
        ;;

    stop)
        echo -e "${YELLOW}Stopping RosterIQ...${NC}"
        docker compose down
        echo -e "${GREEN}✅ Stopped.${NC}"
        ;;

    logs)
        docker compose logs -f --tail=50
        ;;

    demo)
        banner
        python3 -m rosteriq demo
        ;;

    test|tests)
        banner
        echo -e "${BLUE}Running test suite...${NC}"
        echo ""
        python3 -m pytest tests/ -v --tb=short
        ;;

    status)
        docker compose ps
        ;;

    *)
        echo "Usage: ./start.sh [up|dev|stop|logs|demo|test|status]"
        exit 1
        ;;
esac
