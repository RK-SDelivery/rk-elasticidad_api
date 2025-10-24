# Elasticidad API

API desarrollada con FastAPI para ejecutar flujos de procesamiento en entornos DEV y PRD.

## Características

- ✅ FastAPI con endpoints para DEV y PRD
- ✅ Ejecución configurable de flujos (scripts Python y stored procedures)
- ✅ Gestión de paquetes con uv
- ✅ Preparado para despliegue en Cloud Run
- ✅ Logging estructurado con Loguru
- ✅ Validación de datos con Pydantic

## Estructura del Proyecto

```
elasticidad_api/
├── app/
│   ├── __init__.py
│   ├── main.py              # Aplicación principal FastAPI
│   ├── config.py            # Configuración
│   ├── models/              # Modelos Pydantic
│   ├── services/            # Lógica de negocio
│   ├── utils/              # Utilidades
│   └── scripts/            # Scripts de Python
├── tests/                  # Tests
├── .env.example           # Variables de entorno
├── Dockerfile             # Para Cloud Run
├── cloudbuild.yaml        # Cloud Build
└── pyproject.toml         # Configuración uv
```

## Instalación

1. Instalar uv (si no lo tienes):
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

2. Crear entorno virtual e instalar dependencias:
```bash
uv venv
source .venv/bin/activate  # En Windows: .venv\Scripts\activate
uv pip install -e .
```

3. Para desarrollo:
```bash
uv pip install -e ".[dev]"
```

## Configuración

Copia `.env.example` a `.env` y configura las variables:

```bash
cp .env.example .env
```

## Uso

### Desarrollo local
```bash
uv run uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

### Ejecutar flujos

#### Endpoint DEV
```bash
POST /dev/execute
{
  "flow": [
    {"step": 1, "type": "script", "name": "suavizado.py"},
    {"step": 2, "type": "procedure", "name": "optimizacion"}
  ]
}
```

#### Endpoint PRD
```bash
POST /prd/execute
{
  "flow": [
    {"step": 1, "type": "script", "name": "suavizado.py"},
    {"step": 2, "type": "procedure", "name": "optimizacion"}
  ]
}
```

## Despliegue en Cloud Run

```bash
# Construir y desplegar
gcloud run deploy elasticidad-api \
  --source . \
  --platform managed \
  --region us-central1 \
  --allow-unauthenticated
```

## Testing

```bash
uv run pytest
```

## Formato de código

```bash
uv run black .
uv run isort .
```