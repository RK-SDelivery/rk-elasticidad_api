"""
Servicio para ejecutar consultas y procedimientos en BigQuery
"""

import asyncio
from typing import Dict, Any, Optional, List
from google.cloud import bigquery
from google.oauth2 import service_account
from loguru import logger

from ..config import settings


class BigQueryService:
    """Servicio para interactuar con BigQuery"""

    def __init__(self):
        self.client = None
        self._initialize_client()

    def _initialize_client(self):
        """Inicializa el cliente de BigQuery"""
        try:
            if settings.GOOGLE_APPLICATION_CREDENTIALS:
                # Usar service account key file
                credentials = service_account.Credentials.from_service_account_file(
                    settings.GOOGLE_APPLICATION_CREDENTIALS
                )
                self.client = bigquery.Client(
                    credentials=credentials,
                    project=settings.GOOGLE_CLOUD_PROJECT,
                    location=settings.BIGQUERY_LOCATION,
                )
            else:
                # Usar Application Default Credentials (para Cloud Run)
                self.client = bigquery.Client(
                    project=settings.GOOGLE_CLOUD_PROJECT,
                    location=settings.BIGQUERY_LOCATION,
                )

            logger.info("Cliente BigQuery inicializado correctamente")

        except Exception as e:
            logger.error(f"Error inicializando cliente BigQuery: {e}")
            self.client = None

    async def execute_procedure(
        self,
        procedure_name: str,
        environment: str,
        parameters: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Ejecuta un stored procedure en BigQuery

        Args:
            procedure_name: Nombre del procedimiento
            environment: Entorno (dev/prd)
            parameters: Parámetros del procedimiento

        Returns:
            Dict con el resultado de la ejecución
        """
        if not self.client:
            raise RuntimeError("Cliente BigQuery no inicializado")

        dataset = "staging"

        # Construir nombre completo del procedimiento
        full_procedure_name = (
            f"`onus-{environment}-proy-retail-elastici.{dataset}.{procedure_name}`"
        )

        # Construir query CALL
        if parameters:
            # Construir parámetros para la llamada
            param_list = []
            for key, value in parameters.items():
                if isinstance(value, str):
                    param_list.append(f"'{value}'")
                else:
                    param_list.append(str(value))
            params_str = ", ".join(param_list)
            query = f"CALL {full_procedure_name}({params_str});"
        else:
            query = f"CALL {full_procedure_name}();"

        logger.info(f"Ejecutando procedimiento BigQuery: {query}")

        try:
            # Ejecutar en un thread pool para no bloquear el event loop
            job = await asyncio.get_event_loop().run_in_executor(
                None, self._execute_query_sync, query
            )

            # Procesar resultados
            result = {
                "procedure_name": procedure_name,
                "environment": environment,
                "dataset": dataset,
                "query": query,
                "job_id": job.job_id,
                "state": job.state,
                "rows_affected": job.num_dml_affected_rows,
                "execution_time_ms": job.ended - job.started
                if job.ended and job.started
                else None,
            }

            # Si hay resultados, incluirlos
            if job.result().total_rows > 0:
                rows = list(job.result())
                result["results"] = [dict(row) for row in rows]
                result["total_rows"] = len(rows)
            else:
                result["results"] = []
                result["total_rows"] = 0

            logger.info(f"Procedimiento ejecutado exitosamente. Job ID: {job.job_id}")
            return result

        except Exception as e:
            logger.error(f"Error ejecutando procedimiento {procedure_name}: {str(e)}")
            raise RuntimeError(f"Error en BigQuery: {str(e)}")

    def _execute_query_sync(self, query: str) -> bigquery.QueryJob:
        """
        Ejecuta una consulta de forma síncrona

        Args:
            query: Query SQL a ejecutar

        Returns:
            QueryJob con el resultado
        """
        job_config = bigquery.QueryJobConfig()
        job_config.use_legacy_sql = False

        # Configurar timeout
        job_config.job_timeout_ms = settings.PROCEDURE_TIMEOUT * 1000

        query_job = self.client.query(query, job_config=job_config)

        # Esperar a que termine
        query_job.result()

        return query_job

    async def validate_procedure_exists(
        self, procedure_name: str, environment: str
    ) -> bool:
        """
        Valida si un procedimiento existe en BigQuery

        Args:
            procedure_name: Nombre del procedimiento
            environment: Entorno (dev/prd)

        Returns:
            True si existe, False si no
        """
        if not self.client:
            return False

        try:
            dataset = "staging"

            # Consultar información del procedimiento
            query = f"""
            SELECT routine_name 
            FROM `onus-{environment}-proy-retail-elastici.{dataset}.INFORMATION_SCHEMA.ROUTINES`
            WHERE routine_name = '{procedure_name}'
            """

            job = await asyncio.get_event_loop().run_in_executor(
                None, self._execute_query_sync, query
            )

            return job.result().total_rows > 0

        except Exception as e:
            logger.warning(
                f"No se pudo validar procedimiento {procedure_name}: {str(e)}"
            )
            return False

    async def list_procedures(self, environment: str) -> List[str]:
        """
        Lista todos los procedimientos disponibles en un dataset

        Args:
            environment: Entorno (dev/prd)

        Returns:
            Lista de nombres de procedimientos
        """
        if not self.client:
            return []

        try:
            dataset = "staging"

            query = f"""
            SELECT routine_name 
            FROM `onus-{environment}-proy-retail-elastici.{dataset}.INFORMATION_SCHEMA.ROUTINES`
            WHERE routine_type = 'PROCEDURE'
            ORDER BY routine_name
            """

            job = await asyncio.get_event_loop().run_in_executor(
                None, self._execute_query_sync, query
            )

            return [row.routine_name for row in job.result()]

        except Exception as e:
            logger.error(f"Error listando procedimientos: {str(e)}")
            return []


# Instancia global del servicio
bigquery_service = BigQueryService()
