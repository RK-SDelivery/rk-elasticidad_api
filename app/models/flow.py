"""
Modelos Pydantic para la API
"""

from datetime import datetime
from enum import Enum
from typing import List, Optional, Any, Dict
from pydantic import BaseModel, Field, field_validator


class StepType(str, Enum):
    """Tipos de pasos disponibles en el flujo"""

    SCRIPT = "script"
    PROCEDURE = "procedure"
    CALL_PROCEDURE = "call_procedure"  # Alias para procedure


class FlowStep(BaseModel):
    """Modelo para un paso individual del flujo"""

    step: int = Field(..., description="Número de paso en el flujo", ge=1)
    type: StepType = Field(..., description="Tipo de paso a ejecutar")
    name: str = Field(..., description="Nombre del script o procedimiento")
    parameters: Optional[Dict[str, Any]] = Field(
        default=None, description="Parámetros opcionales para el paso"
    )
    timeout: Optional[int] = Field(
        default=None, description="Timeout específico para este paso en segundos"
    )

    @field_validator("name")
    @classmethod
    def validate_name(cls, v):
        """Valida que el nombre no esté vacío"""
        if not v or not v.strip():
            raise ValueError("El nombre del paso no puede estar vacío")
        return v.strip()

    @field_validator("type")
    @classmethod
    def normalize_type(cls, v):
        """Normaliza el tipo de paso"""
        if v == "call_procedure":
            return StepType.PROCEDURE
        return v


class FlowRequest(BaseModel):
    """Modelo para la solicitud de ejecución de flujo"""

    flow: List[FlowStep] = Field(..., description="Lista de pasos del flujo")
    metadata: Optional[Dict[str, Any]] = Field(
        default=None, description="Metadatos adicionales para el flujo"
    )

    @field_validator("flow")
    @classmethod
    def validate_flow_steps(cls, v):
        """Valida que los pasos del flujo sean consecutivos"""
        if not v:
            raise ValueError("El flujo debe tener al menos un paso")

        steps = [step.step for step in v]
        steps.sort()

        # Verificar que los pasos sean consecutivos empezando desde 1
        expected_steps = list(range(1, len(steps) + 1))
        if steps != expected_steps:
            raise ValueError(
                f"Los pasos del flujo deben ser consecutivos empezando desde 1. "
                f"Esperado: {expected_steps}, Recibido: {steps}"
            )

        return v


class StepResult(BaseModel):
    """Resultado de la ejecución de un paso"""

    step: int
    type: StepType
    name: str
    status: str = Field(..., description="success, error, skipped")
    start_time: datetime
    end_time: Optional[datetime] = None
    duration_seconds: Optional[float] = None
    output: Optional[str] = None
    error: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None


class FlowResponse(BaseModel):
    """Respuesta de la ejecución del flujo"""

    flow_id: str = Field(..., description="ID único del flujo ejecutado")
    environment: str = Field(..., description="Entorno donde se ejecutó (dev/prd)")
    status: str = Field(..., description="success, partial_success, error")
    start_time: datetime
    end_time: Optional[datetime] = None
    duration_seconds: Optional[float] = None
    total_steps: int
    successful_steps: int
    failed_steps: int
    results: List[StepResult] = Field(..., description="Resultados de cada paso")
    error_summary: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None
