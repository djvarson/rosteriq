#!/bin/bash
# RosterIQ Deploy Installer
# Copies all deployment files into your RosterIQ project folder

ROSTERIQ_DIR="$HOME/Library/Mobile Documents/com~apple~CloudDocs/Projects/RosterIQ"

if [ ! -d "$ROSTERIQ_DIR" ]; then
    echo "ERROR: RosterIQ folder not found at:"
    echo "  $ROSTERIQ_DIR"
    echo "Please check the path and try again."
    exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "Installing RosterIQ deployment files..."
echo "From: $SCRIPT_DIR"
echo "To:   $ROSTERIQ_DIR"
echo ""

# Copy dashboard files
mkdir -p "$ROSTERIQ_DIR/dashboard"
cp "$SCRIPT_DIR/dashboard/index.html" "$ROSTERIQ_DIR/dashboard/index.html"
cp "$SCRIPT_DIR/dashboard/onboarding.html" "$ROSTERIQ_DIR/dashboard/onboarding.html"
echo "  [OK] dashboard/index.html"
echo "  [OK] dashboard/onboarding.html"

# Copy Python modules into rosteriq package
mkdir -p "$ROSTERIQ_DIR/rosteriq"
cp "$SCRIPT_DIR/rosteriq/auth.py" "$ROSTERIQ_DIR/rosteriq/auth.py"
cp "$SCRIPT_DIR/rosteriq/feed_runner.py" "$ROSTERIQ_DIR/rosteriq/feed_runner.py"
echo "  [OK] rosteriq/auth.py"
echo "  [OK] rosteriq/feed_runner.py"

# Copy deployment scripts
mkdir -p "$ROSTERIQ_DIR/scripts"
cp "$SCRIPT_DIR/scripts/deploy.sh" "$ROSTERIQ_DIR/scripts/deploy.sh"
cp "$SCRIPT_DIR/scripts/seed_demo.py" "$ROSTERIQ_DIR/scripts/seed_demo.py"
chmod +x "$ROSTERIQ_DIR/scripts/deploy.sh"
echo "  [OK] scripts/deploy.sh"
echo "  [OK] scripts/seed_demo.py"

# Copy infrastructure files to project root
cp "$SCRIPT_DIR/Dockerfile" "$ROSTERIQ_DIR/Dockerfile"
cp "$SCRIPT_DIR/docker-compose.yml" "$ROSTERIQ_DIR/docker-compose.yml"
cp "$SCRIPT_DIR/requirements.txt" "$ROSTERIQ_DIR/requirements.txt"
cp "$SCRIPT_DIR/.env.example" "$ROSTERIQ_DIR/.env.example"
cp "$SCRIPT_DIR/railway.toml" "$ROSTERIQ_DIR/railway.toml"
cp "$SCRIPT_DIR/render.yaml" "$ROSTERIQ_DIR/render.yaml"
cp "$SCRIPT_DIR/DEPLOYMENT.md" "$ROSTERIQ_DIR/DEPLOYMENT.md"
cp "$SCRIPT_DIR/MANIFEST.md" "$ROSTERIQ_DIR/MANIFEST.md"
echo "  [OK] Dockerfile"
echo "  [OK] docker-compose.yml"
echo "  [OK] requirements.txt"
echo "  [OK] .env.example"
echo "  [OK] railway.toml"
echo "  [OK] render.yaml"
echo "  [OK] DEPLOYMENT.md"
echo "  [OK] MANIFEST.md"

echo ""
echo "Done! 14 files installed into your RosterIQ project."
echo "Next steps:"
echo "  1. cp .env.example .env  (then fill in your secrets)"
echo "  2. docker-compose up     (to run locally)"
