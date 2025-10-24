"""
Aplicación principal FastAPI para la API de Elasticidad
"""

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from loguru import logger
import sys

from .config import settings
from .models.flow import (
    FlowRequest,
    FlowResponse,
    FlowValidationRequest,
    FlowValidationResponse,
)
from .services.flow_executor import FlowExecutor

# Configurar logging
logger.remove()
logger.add(
    sys.stdout,
    format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}",
    level=settings.LOG_LEVEL,
)

app = FastAPI(
    title=settings.APP_NAME,
    version=settings.APP_VERSION,
    description="API para ejecutar flujos de procesamiento en entornos DEV y PRD",
)

# Configurar CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # En producción, especificar dominios específicos
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Inicializar executor
flow_executor = FlowExecutor()


@app.get("/")
async def root():
    """Endpoint de salud de la API"""
    return {
        "message": f"Bienvenido a {settings.APP_NAME}",
        "version": settings.APP_VERSION,
        "status": "healthy",
        "environments": {
            "dev": settings.DEV_ENABLED,
            "prd": settings.PRD_ENABLED,
        },
    }


@app.get("/health")
async def health_check():
    """Endpoint de health check para Cloud Run"""
    return {"status": "healthy", "version": settings.APP_VERSION}


@app.post("/dev/execute", response_model=FlowResponse)
async def execute_dev_flow(flow_request: FlowRequest):
    """
    Ejecuta un flujo en el entorno de desarrollo

    Args:
        flow_request: Configuración del flujo a ejecutar

    Returns:
        FlowResponse: Resultado de la ejecución
    """
    if not settings.DEV_ENABLED:
        raise HTTPException(status_code=503, detail="El entorno DEV no está habilitado")

    logger.info(f"Ejecutando flujo DEV con {len(flow_request.flow)} pasos")

    try:
        result = await flow_executor.execute_flow(flow_request.flow, environment="dev")
        logger.info("Flujo DEV ejecutado exitosamente")
        return result
    except Exception as e:
        logger.error(f"Error ejecutando flujo DEV: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error en ejecución: {str(e)}")


@app.post("/prd/execute", response_model=FlowResponse)
async def execute_prd_flow(flow_request: FlowRequest):
    """
    Ejecuta un flujo en el entorno de producción

    Args:
        flow_request: Configuración del flujo a ejecutar

    Returns:
        FlowResponse: Resultado de la ejecución
    """
    if not settings.PRD_ENABLED:
        raise HTTPException(status_code=503, detail="El entorno PRD no está habilitado")

    logger.info(f"Ejecutando flujo PRD con {len(flow_request.flow)} pasos")

    try:
        result = await flow_executor.execute_flow(flow_request.flow, environment="prd")
        logger.info("Flujo PRD ejecutado exitosamente")
        return result
    except Exception as e:
        logger.error(f"Error ejecutando flujo PRD: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error en ejecución: {str(e)}")


@app.post("/flows/validate", response_model=FlowValidationResponse)
async def validate_flow_syntax(flow_request: FlowValidationRequest):
    """
    Valida la sintaxis de un flujo sin ejecutarlo

    Args:
        flow_request: Solicitud con la lista de pasos del flujo

    Returns:
        FlowValidationResponse: Resultado de la validación
    """
    try:
        validation_result = flow_executor.validate_flow(flow_request.flow)
        return FlowValidationResponse(
            valid=validation_result["valid"],
            errors=validation_result.get("errors", []),
            warnings=validation_result.get("warnings", []),
        )
    except Exception as e:
        logger.error(f"Error validando flujo: {str(e)}")
        raise HTTPException(status_code=400, detail=f"Error en validación: {str(e)}")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=8000,
        reload=settings.DEBUG,
        log_level=settings.LOG_LEVEL.lower(),
    )
