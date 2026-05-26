#!/bin/bash
# WorkYa Dashboard - Auto-update script
# Runs every Monday at 7am via cron

set -e

REPO_DIR="/Users/contratista/workya-dashboard"
MONGO_URL="mongodb+srv://bluesolutions2025_db_user:Nexia2026secure@cluster0.bwtemn9.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
DB_NAME="workya_db"
LOG_FILE="$REPO_DIR/scripts/update.log"

echo "=== $(date) - Iniciando actualización ===" >> "$LOG_FILE"

cd "$REPO_DIR"

# Pull latest changes
git pull origin main >> "$LOG_FILE" 2>&1

# Run the generator
export MONGO_URL="$MONGO_URL"
export DB_NAME="$DB_NAME"
export DASHBOARD_PATH="index.html"

/usr/bin/python3 "$REPO_DIR/scripts/generate_dashboard.py" >> "$LOG_FILE" 2>&1

# Commit and push if changed
git config user.name "WorkYa Bot"
git config user.email "blue.solutions2025@gmail.com"

if ! git diff --quiet index.html; then
    git add index.html
    git commit -m "Auto-update dashboard $(date '+%Y-%m-%d')"
    git push origin main >> "$LOG_FILE" 2>&1
    echo "✓ Dashboard actualizado y publicado." >> "$LOG_FILE"
else
    echo "Sin cambios en los datos." >> "$LOG_FILE"
fi

echo "=== Listo ===" >> "$LOG_FILE"
