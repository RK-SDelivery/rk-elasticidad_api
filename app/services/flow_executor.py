"""
Ejecutor de flujos - Maneja la ejecución de scripts y procedimientos
"""

import asyncio
import subprocess
import uuid
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional

from loguru import logger

from ..config import settings
from ..models.flow import FlowStep, FlowResponse, StepResult, StepType


class FlowExecutor:
    """Clase para ejecutar flujos de pasos configurables"""

    def __init__(self):
        self.scripts_path = Path(settings.SCRIPTS_PATH)
        self.scripts_path.mkdir(exist_ok=True, parents=True)

    async def execute_flow(
        self, flow_steps: List[FlowStep], environment: str
    ) -> FlowResponse:
        """
        Ejecuta un flujo completo de pasos

        Args:
            flow_steps: Lista de pasos a ejecutar
            environment: Entorno de ejecución (dev/prd)

        Returns:
            FlowResponse: Resultado de la ejecución
        """
        flow_id = str(uuid.uuid4())
        start_time = datetime.now()

        logger.info(f"Iniciando flujo {flow_id} en entorno {environment}")

        results: List[StepResult] = []
        successful_steps = 0
        failed_steps = 0

        # Ordenar pasos por número de paso
        sorted_steps = sorted(flow_steps, key=lambda x: x.step)

        for step in sorted_steps:
            logger.info(f"Ejecutando paso {step.step}: {step.name} ({step.type})")

            step_result = await self._execute_step(step, environment)
            results.append(step_result)

            if step_result.status == "success":
                successful_steps += 1
            elif step_result.status == "error":
                failed_steps += 1
                # Si un paso falla, decidir si continuar o parar
                # Por ahora, continuamos ejecutando los siguientes pasos
                logger.warning(f"Paso {step.step} falló, pero continuando con el flujo")

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        # Determinar estado general del flujo
        if failed_steps == 0:
            status = "success"
        elif successful_steps > 0:
            status = "partial_success"
        else:
            status = "error"

        # Crear resumen de errores si los hay
        error_summary = None
        if failed_steps > 0:
            errors = [r.error for r in results if r.error]
            error_summary = f"{failed_steps} pasos fallaron: {'; '.join(errors)}"

        logger.info(f"Flujo {flow_id} completado con estado: {status}")

        return FlowResponse(
            flow_id=flow_id,
            environment=environment,
            status=status,
            start_time=start_time,
            end_time=end_time,
            duration_seconds=duration,
            total_steps=len(flow_steps),
            successful_steps=successful_steps,
            failed_steps=failed_steps,
            results=results,
            error_summary=error_summary,
        )

    async def _execute_step(self, step: FlowStep, environment: str) -> StepResult:
        """
        Ejecuta un paso individual del flujo

        Args:
            step: Paso a ejecutar
            environment: Entorno de ejecución

        Returns:
            StepResult: Resultado del paso
        """
        start_time = datetime.now()

        step_result = StepResult(
            step=step.step,
            type=step.type,
            name=step.name,
            status="error",
            start_time=start_time,
        )

        try:
            if step.type == StepType.SCRIPT:
                output = await self._execute_script(step, environment)
                step_result.output = output
                step_result.status = "success"

            elif step.type == StepType.PROCEDURE:
                output = await self._execute_procedure(step, environment)
                step_result.output = output
                step_result.status = "success"

            else:
                raise ValueError(f"Tipo de paso no soportado: {step.type}")

        except Exception as e:
            logger.error(f"Error ejecutando paso {step.step}: {str(e)}")
            step_result.error = str(e)
            step_result.status = "error"

        end_time = datetime.now()
        step_result.end_time = end_time
        step_result.duration_seconds = (end_time - start_time).total_seconds()

        return step_result

    async def _execute_script(self, step: FlowStep, environment: str) -> str:
        """
        Ejecuta un script de Python

        Args:
            step: Paso con información del script
            environment: Entorno de ejecución

        Returns:
            str: Output del script
        """
        script_path = self.scripts_path / environment / step.name

        if not script_path.exists():
            raise FileNotFoundError(f"Script no encontrado: {script_path}")

        # Preparar comando
        cmd = ["python", str(script_path)]

        # Agregar parámetros si los hay
        if step.parameters:
            for key, value in step.parameters.items():
                cmd.extend([f"--{key}", str(value)])

        # Configurar timeout
        timeout = step.timeout or settings.SCRIPT_TIMEOUT

        logger.debug(f"Ejecutando comando: {' '.join(cmd)}")

        # Ejecutar script
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env={**dict(asyncio.subprocess.os.environ), "ENVIRONMENT": environment},
        )

        try:
            stdout, stderr = await asyncio.wait_for(
                process.communicate(), timeout=timeout
            )
        except asyncio.TimeoutError:
            process.kill()
            raise TimeoutError(f"Script {step.name} excedió el timeout de {timeout}s")

        if process.returncode != 0:
            error_msg = stderr.decode() if stderr else "Error desconocido"
            raise RuntimeError(
                f"Script falló con código {process.returncode}: {error_msg}"
            )

        return stdout.decode()

    async def _execute_procedure(self, step: FlowStep, environment: str) -> str:
        """
        Ejecuta un stored procedure

        Args:
            step: Paso con información del procedimiento
            environment: Entorno de ejecución

        Returns:
            str: Resultado del procedimiento
        """
        # Por ahora, simulamos la ejecución de un stored procedure
        # En una implementación real, aquí harías la conexión a la base de datos

        logger.info(f"Ejecutando stored procedure: {step.name} en {environment}")

        # Simular tiempo de ejecución
        await asyncio.sleep(1)

        # Simular resultado exitoso
        return f"Stored procedure {step.name} ejecutado exitosamente en {environment}"

    def validate_flow(self, flow_steps: List[dict]) -> Dict[str, Any]:
        """
        Valida la sintaxis de un flujo sin ejecutarlo

        Args:
            flow_steps: Lista de diccionarios con los pasos del flujo

        Returns:
            dict: Resultado de la validación
        """
        errors = []
        warnings = []

        try:
            # Intentar crear objetos FlowStep para validación
            validated_steps = []
            for step_dict in flow_steps:
                try:
                    step = FlowStep(**step_dict)
                    validated_steps.append(step)
                except Exception as e:
                    errors.append(f"Paso {step_dict.get('step', '?')}: {str(e)}")

            if not errors:
                # Validar que los scripts existen (solo advertencias)
                for step in validated_steps:
                    if step.type == StepType.SCRIPT:
                        script_path = self.scripts_path / step.name
                        if not script_path.exists():
                            warnings.append(
                                f"Script {step.name} no encontrado en {script_path}"
                            )

        except Exception as e:
            errors.append(f"Error general en validación: {str(e)}")

        return {"valid": len(errors) == 0, "errors": errors, "warnings": warnings}
