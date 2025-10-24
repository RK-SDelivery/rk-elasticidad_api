#!/bin/bash

# Script para ejecutar la aplicación en desarrollo

set -e

echo "🚀 Iniciando servidor de desarrollo..."

# Verificar si el entorno virtual existe
if [ ! -d ".venv" ]; then
    echo "❌ Entorno virtual no encontrado. Ejecuta primero: ./init.sh"
    exit 1
fi

# Activar entorno virtual
source .venv/bin/activate

# Verificar si .env existe
if [ ! -f ".env" ]; then
    echo "⚠️ Archivo .env no encontrado. Usando configuración por defecto."
fi

# Ejecutar aplicación
echo "🌐 Servidor iniciando en http://localhost:8000"
echo "📖 Documentación disponible en http://localhost:8000/docs"
echo "🛑 Presiona Ctrl+C para detener el servidor"

uv run uvicorn app.main:app --reload --host 0.0.0.0 --port 8000