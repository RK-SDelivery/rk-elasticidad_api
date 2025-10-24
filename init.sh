#!/bin/bash

# Script de inicialización del proyecto Elasticidad API

set -e

echo "🚀 Inicializando proyecto Elasticidad API..."

# Verificar si uv está instalado
if ! command -v uv &> /dev/null; then
    echo "❌ uv no está instalado. Instalando..."
    curl -LsSf https://astral.sh/uv/install.sh | sh
    source ~/.bashrc
fi

echo "✅ uv encontrado"

# Crear entorno virtual
echo "📦 Creando entorno virtual..."
uv venv

# Activar entorno virtual
echo "🔧 Activando entorno virtual..."
source .venv/bin/activate

# Instalar dependencias
echo "📚 Instalando dependencias..."
uv pip install -e .

# Instalar dependencias de desarrollo
echo "🛠️ Instalando dependencias de desarrollo..."
uv pip install -e ".[dev]"

# Crear archivo .env si no existe
if [ ! -f .env ]; then
    echo "⚙️ Creando archivo .env..."
    cp .env.example .env
    echo "📝 Archivo .env creado. Por favor, configura las variables necesarias."
fi

# Hacer scripts ejecutables
echo "🔐 Configurando permisos de scripts..."
chmod +x app/scripts/*.py

echo "✨ ¡Proyecto inicializado exitosamente!"
echo ""
echo "📋 Próximos pasos:"
echo "1. Activar el entorno virtual: source .venv/bin/activate"
echo "2. Configurar variables en .env"
echo "3. Ejecutar la aplicación: uv run uvicorn app.main:app --reload"
echo ""
echo "🌐 La API estará disponible en: http://localhost:8000"
echo "📖 Documentación automática en: http://localhost:8000/docs"