#!/bin/bash

# Script para ejecutar tests

set -e

# Colores para output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

log() {
    echo -e "${GREEN}[$(date +'%Y-%m-%d %H:%M:%S')] $1${NC}"
}

error() {
    echo -e "${RED}[ERROR] $1${NC}"
}

warn() {
    echo -e "${YELLOW}[WARN] $1${NC}"
}

log "🧪 Ejecutando tests de la API..."

# Verificar que uv esté instalado
if ! command -v uv &> /dev/null; then
    error "uv no está instalado. Instálalo con: curl -LsSf https://astral.sh/uv/install.sh | sh"
    exit 1
fi

# Verificar que el entorno virtual existe
if [ ! -d ".venv" ]; then
    log "Creando entorno virtual..."
    uv venv
fi

# Activar entorno virtual
source .venv/bin/activate

# Instalar dependencias de desarrollo
log "Instalando dependencias de desarrollo..."
uv pip install -e ".[dev]"

# Ejecutar tests
log "Ejecutando pytest..."
uv run pytest tests/ -v --tb=short

# Ejecutar linting
log "Ejecutando linting..."
uv run black --check app/ tests/
uv run isort --check-only app/ tests/

log "✅ Todos los tests completados!"