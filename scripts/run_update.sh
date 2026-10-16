#!/bin/bash
# WorkYa Dashboard - Auto-update script
# Runs every Monday at 7am via launchd (Tuesday is a fallback in case Monday fails)
#
# CONFIGURACIÓN DE CREDENCIALES:
# Las credenciales se leen desde variables de entorno, NO hardcodeadas aquí.
# Para ejecución local: copiar .env.example → .env y completar MONGO_URL.
# Para GitHub Actions: configurar el secreto MONGO_URL en el repositorio.

REPO_DIR="/Users/contratista/workya-dashboard"
LOG_FILE="$REPO_DIR/scripts/update.log"

echo "=== $(date) - Iniciando actualización ===" >> "$LOG_FILE"

# Cargar variables de entorno desde .env si existe (ejecución local)
if [ -f "$REPO_DIR/.env" ]; then
    set -a
    # shellcheck disable=SC1091
    source "$REPO_DIR/.env"
    set +a
fi

# Aceptar MONGODB_URI (preferido) o MONGO_URL (compatibilidad)
# Si solo está MONGODB_URI, exponerlo también como MONGO_URL para generate_dashboard.py
if [ -n "$MONGODB_URI" ] && [ -z "$MONGO_URL" ]; then
    export MONGO_URL="$MONGODB_URI"
fi

# Validar que al menos uno esté configurado — NUNCA imprimir el valor
if [ -z "$MONGO_URL" ] && [ -z "$MONGODB_URI" ]; then
    echo "ERROR: Ni MONGODB_URI ni MONGO_URL están configurados." >> "$LOG_FILE"
    echo "  → Copiar .env.example a .env y completar MONGODB_URI." >> "$LOG_FILE"
    echo "  → En GitHub Actions: configurar el secreto MONGO_URL." >> "$LOG_FILE"
    echo "=== Listo con error ===" >> "$LOG_FILE"
    exit 1
fi

DB_NAME="${DB_NAME:-workya_db}"

# Wait for network connectivity (up to 3 minutes)
for i in $(seq 1 6); do
    if curl -s --max-time 5 https://github.com > /dev/null 2>&1; then
        break
    fi
    echo "Sin red, esperando 30s... ($i/6)" >> "$LOG_FILE"
    sleep 30
done

if ! curl -s --max-time 5 https://github.com > /dev/null 2>&1; then
    echo "ERROR: Sin acceso a internet después de 3 min. Abortando." >> "$LOG_FILE"
    echo "=== Listo ===" >> "$LOG_FILE"
    exit 1
fi

set -e

cd "$REPO_DIR"

# Pull latest changes
git pull origin main >> "$LOG_FILE" 2>&1

# Run the generator
# MONGO_URL ya está en el entorno (desde .env o variable del sistema)
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
