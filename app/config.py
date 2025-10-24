"""
Configuración de la aplicación
"""

from typing import Optional
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Configuración de la aplicación usando Pydantic Settings"""

    # Información de la aplicación
    APP_NAME: str = "Elasticidad API"
    APP_VERSION: str = "0.1.0"
    DEBUG: bool = False
    LOG_LEVEL: str = "INFO"

    # Base de datos
    DATABASE_URL: Optional[str] = None

    # Configuración de entornos
    DEV_ENABLED: bool = True
    PRD_ENABLED: bool = True

    # Rutas
    SCRIPTS_PATH: str = "./app/scripts"

    # Cloud Run / GCP
    GOOGLE_CLOUD_PROJECT: Optional[str] = None
    GOOGLE_APPLICATION_CREDENTIALS: Optional[str] = None

    # API Keys
    API_KEY: Optional[str] = None

    # Timeouts (en segundos)
    SCRIPT_TIMEOUT: int = 300  # 5 minutos
    PROCEDURE_TIMEOUT: int = 600  # 10 minutos

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


# Instancia global de configuración
settings = Settings()
