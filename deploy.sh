#!/bin/bash

# Script de despliegue para Google Cloud Platform
# Despliega la API de Elasticidad en Cloud Run

set -e

# Configuración por defecto
PROJECT_ID=""
REGION="us-central1"
SERVICE_NAME="elasticidad-api"
ENV_FILE=".env"

# Colores para output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Función para mostrar ayuda
show_help() {
    echo "Uso: $0 [opciones]"
    echo ""
    echo "Opciones:"
    echo "  -p, --project PROJECT_ID    ID del proyecto de GCP (requerido)"
    echo "  -r, --region REGION         Región de despliegue (default: us-central1)"
    echo "  -s, --service SERVICE_NAME  Nombre del servicio (default: elasticidad-api)"
    echo "  -e, --env ENV_FILE          Archivo de variables de entorno (default: .env)"
    echo "  -h, --help                  Mostrar esta ayuda"
    echo ""
    echo "Ejemplo:"
    echo "  $0 -p mi-proyecto-gcp -r us-central1"
}

# Función para logging
log() {
    echo -e "${GREEN}[$(date +'%Y-%m-%d %H:%M:%S')] $1${NC}"
}

warn() {
    echo -e "${YELLOW}[WARN] $1${NC}"
}

error() {
    echo -e "${RED}[ERROR] $1${NC}"
    exit 1
}

# Parsear argumentos
while [[ $# -gt 0 ]]; do
    case $1 in
        -p|--project)
            PROJECT_ID="$2"
            shift 2
            ;;
        -r|--region)
            REGION="$2"
            shift 2
            ;;
        -s|--service)
            SERVICE_NAME="$2"
            shift 2
            ;;
        -e|--env)
            ENV_FILE="$2"
            shift 2
            ;;
        -h|--help)
            show_help
            exit 0
            ;;
        *)
            error "Opción desconocida: $1"
            ;;
    esac
done

# Usar proyecto configurado en gcloud si no se especifica
if [ -z "$PROJECT_ID" ]; then
    PROJECT_ID=$(gcloud config get-value project 2>/dev/null)
    if [ -z "$PROJECT_ID" ]; then
        error "PROJECT_ID es requerido. Usa -p o --project para especificarlo o configura: gcloud config set project TU_PROJECT_ID"
    fi
fi

log "Iniciando despliegue en Google Cloud Platform"
log "Proyecto: $PROJECT_ID"
log "Región: $REGION"
log "Servicio: $SERVICE_NAME"

# Verificar que gcloud esté instalado
if ! command -v gcloud &> /dev/null; then
    error "gcloud CLI no está instalado. Instálalo desde: https://cloud.google.com/sdk/docs/install"
fi

# Verificar autenticación
if ! gcloud auth list --filter=status:ACTIVE --format="value(account)" | grep -q .; then
    error "No estás autenticado en gcloud. Ejecuta: gcloud auth login"
fi

# Configurar proyecto
log "Configurando proyecto GCP..."
gcloud config set project $PROJECT_ID

# Habilitar APIs necesarias
log "Habilitando APIs de GCP..."
gcloud services enable cloudbuild.googleapis.com
gcloud services enable run.googleapis.com
gcloud services enable containerregistry.googleapis.com

# Preparar variables de entorno para Cloud Run
ENV_VARS=""
if [ -f "$ENV_FILE" ]; then
    log "Cargando variables de entorno desde $ENV_FILE..."
    
    # Leer variables del archivo .env y convertirlas al formato de gcloud
    while IFS='=' read -r key value; do
        # Saltar líneas vacías y comentarios
        [[ $key =~ ^[[:space:]]*# ]] && continue
        [[ -z $key ]] && continue
        
        # Remover espacios y comillas
        key=$(echo $key | xargs)
        value=$(echo $value | xargs | sed 's/^["'\'']//' | sed 's/["'\'']$//')
        
        if [ ! -z "$key" ] && [ ! -z "$value" ]; then
            if [ -z "$ENV_VARS" ]; then
                ENV_VARS="$key=$value"
            else
                ENV_VARS="$ENV_VARS,$key=$value"
            fi
        fi
    done < <(grep -v '^#' $ENV_FILE | grep '=' || true)
else
    warn "Archivo $ENV_FILE no encontrado. Desplegando sin variables de entorno personalizadas."
fi

# Desplegar usando Cloud Build (método recomendado)
log "Desplegando usando Cloud Build..."

# Actualizar cloudbuild.yaml con parámetros dinámicos
sed -i.bak "s/elasticidad-api/$SERVICE_NAME/g" cloudbuild.yaml
sed -i.bak "s/us-central1/$REGION/g" cloudbuild.yaml

# Ejecutar Cloud Build
gcloud builds submit --config cloudbuild.yaml .

# Restaurar cloudbuild.yaml original
mv cloudbuild.yaml.bak cloudbuild.yaml

# Si hay variables de entorno, actualizarlas
if [ ! -z "$ENV_VARS" ]; then
    log "Actualizando variables de entorno..."
    gcloud run services update $SERVICE_NAME \
        --region $REGION \
        --set-env-vars $ENV_VARS
fi

# Obtener URL del servicio
SERVICE_URL=$(gcloud run services describe $SERVICE_NAME --region $REGION --format 'value(status.url)')

log "¡Despliegue completado exitosamente!"
log "URL del servicio: $SERVICE_URL"
log ""
log "Prueba tu API:"
log "  curl $SERVICE_URL"
log "  curl $SERVICE_URL/health"
log ""
log "Documentación interactiva:"
log "  $SERVICE_URL/docs"
log ""
log "Para ver logs:"
log "  gcloud logs tail --service $SERVICE_NAME"