"""
Utilidades para la base de datos
"""

import asyncio
from typing import Optional, Dict, Any
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from loguru import logger

from ..config import settings


class DatabaseManager:
    """Gestor de conexiones a la base de datos"""

    def __init__(self):
        self.engine = None
        self.session_factory = None

    async def initialize(self):
        """Inicializa la conexión a la base de datos"""
        if not settings.DATABASE_URL:
            logger.warning("DATABASE_URL no configurada, funcionalidad de BD limitada")
            return

        try:
            self.engine = create_async_engine(
                settings.DATABASE_URL,
                echo=settings.DEBUG,
                pool_pre_ping=True,
            )

            self.session_factory = sessionmaker(
                self.engine,
                class_=AsyncSession,
                expire_on_commit=False,
            )

            logger.info("Conexión a base de datos inicializada")

        except Exception as e:
            logger.error(f"Error inicializando base de datos: {e}")
            raise

    async def get_session(self) -> AsyncSession:
        """Obtiene una sesión de base de datos"""
        if not self.session_factory:
            raise RuntimeError("Base de datos no inicializada")
        return self.session_factory()

    async def execute_procedure(
        self, procedure_name: str, parameters: Optional[Dict[str, Any]] = None
    ) -> Any:
        """
        Ejecuta un stored procedure

        Args:
            procedure_name: Nombre del procedimiento
            parameters: Parámetros del procedimiento

        Returns:
            Resultado del procedimiento
        """
        if not self.engine:
            raise RuntimeError("Base de datos no inicializada")

        async with self.get_session() as session:
            try:
                # Construir llamada al procedimiento
                params_str = ""
                if parameters:
                    param_list = [f":{key}" for key in parameters.keys()]
                    params_str = ", ".join(param_list)

                query = f"CALL {procedure_name}({params_str})"

                logger.info(f"Ejecutando procedimiento: {query}")

                result = await session.execute(query, parameters or {})
                await session.commit()

                return result.fetchall()

            except Exception as e:
                await session.rollback()
                logger.error(f"Error ejecutando procedimiento {procedure_name}: {e}")
                raise

    async def close(self):
        """Cierra las conexiones de base de datos"""
        if self.engine:
            await self.engine.dispose()
            logger.info("Conexiones de base de datos cerradas")


# Instancia global del gestor de base de datos
db_manager = DatabaseManager()
