#!/bin/bash
# RosterIQ — Quick Setup & Test
# Run: chmod +x setup.sh && ./setup.sh

set -e

echo "Installing dependencies..."
pip install -e ".[dev]"

echo ""
echo "Running test suite..."
python -m pytest tests/ -v --tb=short

echo ""
echo "Done! All modules ready."
echo "Run: python -m rosteriq demo"
