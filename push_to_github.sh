#!/bin/bash
# ============================================================
# RosterIQ - Push to GitHub
# Run this script from inside the rosteriq-deploy folder
# ============================================================

set -e

REPO_URL="https://github.com/djvarson-commits/rosteriq.git"

echo "============================================"
echo "  RosterIQ - Push to GitHub"
echo "============================================"
echo ""

# Check we're in the right directory
if [ ! -d "rosteriq" ] || [ ! -f "requirements.txt" ]; then
    echo "ERROR: Run this script from the rosteriq-deploy folder."
    echo "  cd /path/to/rosteriq-deploy"
    echo "  bash push_to_github.sh"
    exit 1
fi

# Initialise git if needed
if [ ! -d ".git" ]; then
    echo "[1/5] Initialising git repository..."
    git init
else
    echo "[1/5] Git already initialised."
fi

# Set branch to main
echo "[2/5] Setting branch to main..."
git branch -M main

# Add remote (skip if already set)
if git remote get-url origin &>/dev/null; then
    echo "[3/5] Remote 'origin' already set."
else
    echo "[3/5] Adding remote origin..."
    git remote add origin "$REPO_URL"
fi

# Stage all files
echo "[4/5] Staging files..."
git add -A
git status --short

# Commit
echo "[5/5] Committing..."
git commit -m "Initial commit: RosterIQ AI rostering platform

- Core modules: roster engine, award engine, shift swap, reports
- POS adapters: SwiftPOS, Lightspeed, Square + aggregator
- Tanda integration with OAuth 2.0
- Auth system with JWT + API keys
- Feed runner with scheduled data collection
- CI/CD pipeline for Railway deployment
- 294+ tests across all modules"

# Push
echo ""
echo "Pushing to GitHub..."
git push -u origin main

echo ""
echo "============================================"
echo "  Done! Your code is live at:"
echo "  https://github.com/djvarson-commits/rosteriq"
echo "============================================"
