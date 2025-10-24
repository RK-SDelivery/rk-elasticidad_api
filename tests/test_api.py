"""
Tests para la API de elasticidad
"""

import pytest
from fastapi.testclient import TestClient
from app.main import app

client = TestClient(app)


def test_root_endpoint():
    """Test del endpoint raíz"""
    response = client.get("/")
    assert response.status_code == 200
    data = response.json()
    assert "message" in data
    assert "version" in data
    assert "status" in data
    assert data["status"] == "healthy"


def test_health_check():
    """Test del endpoint de health check"""
    response = client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "healthy"


def test_dev_execute_flow():
    """Test del endpoint de ejecución en DEV"""
    flow_data = {
        "flow": [
            {"step": 1, "type": "script", "name": "suavizado.py"},
            {"step": 2, "type": "procedure", "name": "optimizacion"},
        ]
    }

    response = client.post("/dev/execute", json=flow_data)
    assert response.status_code == 200

    data = response.json()
    assert "flow_id" in data
    assert data["environment"] == "dev"
    assert data["total_steps"] == 2
    assert "results" in data


def test_prd_execute_flow():
    """Test del endpoint de ejecución en PRD"""
    flow_data = {"flow": [{"step": 1, "type": "script", "name": "suavizado.py"}]}

    response = client.post("/prd/execute", json=flow_data)
    assert response.status_code == 200

    data = response.json()
    assert "flow_id" in data
    assert data["environment"] == "prd"
    assert data["total_steps"] == 1


def test_invalid_flow():
    """Test con flujo inválido"""
    invalid_flow = {
        "flow": [
            {"step": 2, "type": "script", "name": "test.py"}  # Paso no consecutivo
        ]
    }

    response = client.post("/dev/execute", json=invalid_flow)
    assert response.status_code == 422  # Validation error


def test_empty_flow():
    """Test con flujo vacío"""
    empty_flow = {"flow": []}

    response = client.post("/dev/execute", json=empty_flow)
    assert response.status_code == 422  # Validation error


def test_flow_validation():
    """Test del endpoint de validación"""
    flow_data = [
        {"step": 1, "type": "script", "name": "suavizado.py"},
        {"step": 2, "type": "procedure", "name": "optimizacion"},
    ]

    response = client.get("/flows/validate", params={"flow": flow_data})
    assert response.status_code == 200

    data = response.json()
    assert "valid" in data
    assert "errors" in data
    assert "warnings" in data
