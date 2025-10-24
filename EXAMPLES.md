# Ejemplos de Uso de la API

## Estructura de Flujos

Los flujos se definen como una lista de pasos secuenciales:

```json
{
  "flow": [
    {
      "step": 1,
      "type": "script",
      "name": "suavizado.py",
      "parameters": {
        "factor": 0.5,
        "input": "data.csv"
      },
      "timeout": 300
    },
    {
      "step": 2,
      "type": "procedure",
      "name": "optimizacion_stored_proc",
      "parameters": {
        "algorithm": "genetic",
        "iterations": 100
      }
    }
  ],
  "metadata": {
    "description": "Flujo de suavizado y optimización",
    "created_by": "usuario@empresa.com"
  }
}
```

## Ejemplos de Requests

### 1. Flujo DEV Simple

```bash
curl -X POST "https://tu-api-url/dev/execute" \
  -H "Content-Type: application/json" \
  -d '{
    "flow": [
      {
        "step": 1,
        "type": "script",
        "name": "suavizado.py"
      }
    ]
  }'
```

### 2. Flujo PRD Completo

```bash
curl -X POST "https://tu-api-url/prd/execute" \
  -H "Content-Type: application/json" \
  -d '{
    "flow": [
      {
        "step": 1,
        "type": "script",
        "name": "suavizado.py",
        "parameters": {
          "factor": 0.8,
          "input": "/data/production_data.csv",
          "output": "/data/smoothed_data.csv"
        }
      },
      {
        "step": 2,
        "type": "procedure",
        "name": "optimizacion_v2"
      }
    ],
    "metadata": {
      "process_id": "PROC-2024-001",
      "department": "analytics"
    }
  }'
```

### 3. Validar Flujo

```bash
curl -X GET "https://tu-api-url/flows/validate" \
  -H "Content-Type: application/json" \
  -d '[
    {
      "step": 1,
      "type": "script",
      "name": "suavizado.py"
    },
    {
      "step": 2,
      "type": "procedure",
      "name": "optimizacion"
    }
  ]'
```

## Respuesta de la API

```json
{
  "flow_id": "uuid-del-flujo",
  "environment": "dev",
  "status": "success",
  "start_time": "2024-10-24T10:00:00Z",
  "end_time": "2024-10-24T10:05:30Z",
  "duration_seconds": 330.5,
  "total_steps": 2,
  "successful_steps": 2,
  "failed_steps": 0,
  "results": [
    {
      "step": 1,
      "type": "script",
      "name": "suavizado.py",
      "status": "success",
      "start_time": "2024-10-24T10:00:00Z",
      "end_time": "2024-10-24T10:02:00Z",
      "duration_seconds": 120.0,
      "output": "Procesamiento completado: 1000 registros procesados"
    },
    {
      "step": 2,
      "type": "procedure",
      "name": "optimizacion",
      "status": "success",
      "start_time": "2024-10-24T10:02:00Z",
      "end_time": "2024-10-24T10:05:30Z",
      "duration_seconds": 210.5,
      "output": "Optimización completada con valor óptimo: 42.123"
    }
  ]
}
```

## Tipos de Pasos Soportados

### 1. Scripts (`"type": "script"`)
- Ejecuta archivos Python desde `app/scripts/`
- Parámetros se pasan como argumentos de línea de comandos
- Ejemplo: `python suavizado.py --factor 0.5 --input data.csv`

### 2. Stored Procedures (`"type": "procedure"`)
- Ejecuta procedimientos almacenados en la base de datos
- Requiere configuración de `DATABASE_URL`
- Parámetros se pasan al procedimiento

## Códigos de Estado

- **success**: Todos los pasos ejecutados exitosamente
- **partial_success**: Algunos pasos fallaron, pero otros se completaron
- **error**: Todos los pasos fallaron o error crítico

## Health Checks

```bash
# Verificar que la API está funcionando
curl https://tu-api-url/health

# Información general de la API
curl https://tu-api-url/
```

## Documentación Interactiva

Una vez desplegada, la documentación interactiva está disponible en:
- `https://tu-api-url/docs` (Swagger UI)
- `https://tu-api-url/redoc` (ReDoc)