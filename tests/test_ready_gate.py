"""
/ready is the Railway deploy gate: a build that would lose venue data must
never be promoted.

Pins:
* dev/test (no explicit ENVIRONMENT=production): memory store is fine,
  /ready is 200 when the DB pings
* ENVIRONMENT=production + memory store -> 503 (a prod deploy that silently
  fell back to RAM storage must be refused by the healthcheck)
* ENVIRONMENT=production + missing JWT_SECRET -> 503
* checks are named in the payload so the failure is diagnosable from curl
"""

from fastapi.testclient import TestClient

from rosteriq.api import app


def test_ready_ok_in_dev(monkeypatch):
    monkeypatch.delenv("ENVIRONMENT", raising=False)
    c = TestClient(app)
    r = c.get("/ready")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["checks"]["database"] is True
    assert "durable_store" in body["checks"] and "jwt_secret" in body["checks"]


def test_ready_refuses_memory_store_in_production(monkeypatch):
    monkeypatch.setenv("ENVIRONMENT", "production")
    c = TestClient(app)
    r = c.get("/ready")   # test store IS MemoryStore -> not durable
    assert r.status_code == 503, r.text
    assert r.json()["checks"]["durable_store"] is False


def test_ready_refuses_missing_jwt_secret_in_production(monkeypatch):
    monkeypatch.setenv("ENVIRONMENT", "production")
    monkeypatch.delenv("JWT_SECRET", raising=False)
    c = TestClient(app)
    r = c.get("/ready")
    assert r.status_code == 503
    assert r.json()["checks"]["jwt_secret"] is False
