#!/bin/bash

# Script para formatear código

set -e

GREEN='\033[0;32m'
NC='\033[0m'

log() {
    echo -e "${GREEN}[$(date +'%Y-%m-%d %H:%M:%S')] $1${NC}"
}

log "🎨 Formateando código..."

# Verificar que uv esté instalado
if ! command -v uv &> /dev/null; then
    echo "❌ uv no está instalado. Instálalo con: curl -LsSf https://astral.sh/uv/install.sh | sh"
    exit 1
fi

# Activar entorno virtual
source .venv/bin/activate

log "Ejecutando black..."
uv run black app/ tests/

log "Ejecutando isort..."
uv run isort app/ tests/

log "✅ Código formateado!"