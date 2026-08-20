"""
The in-process test runner must not be reachable on a real deployment.

POST /api/v1/admin/run-tests calls TestRunner.run_all(), which executes the
whole pytest suite ON THE EVENT LOOP against whatever database the app is
pointed at. On production that means pytest fixtures — venues, employees,
rosters — written into a customer's data while every other request stalls.

Two independent guards, both pinned here:
  1. platform-owner only (the previous check was a stub that passed everyone)
  2. an ENVIRONMENT gate that fails CLOSED — ENVIRONMENT is not set on Railway,
     so "disabled when production" would have failed open exactly where it
     matters. It asks "is this provably a dev box?" instead.
"""

import uuid

import pytest
from fastapi.testclient import TestClient

from rosteriq.api import app
from rosteriq.database import get_db
from rosteriq.routes.test_report import test_runner_enabled


PW = "Passw0rd!234"


def _user(c, role):
    email = f"{role}_{uuid.uuid4().hex[:8]}@x.com"
    assert c.post("/api/auth/register",
                  json={"email": email, "password": PW, "name": "U"}).status_code in (200, 201)
    db = get_db()
    rec = db.get_user_by_email(email)
    rec["role"] = role
    rec["venue_ids"] = []
    db.save_user(rec)
    r = c.post("/api/auth/login", json={"email": email, "password": PW})
    b = r.json()
    tok = b.get("access_token") or b.get("tokens", {}).get("access_token")
    return {"Authorization": f"Bearer {tok}"}


# ---------------------------------------------------------------------------
# the environment gate
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("env,database_url,expected", [
    ("development", None, True),
    ("dev", None, True),
    ("test", "postgres://x", True),          # explicit test env: allowed
    ("production", None, False),
    ("staging", None, False),
    ("PRODUCTION", "postgres://x", False),   # case-insensitive
    (None, None, True),                      # unset + no DB = a dev box
    (None, "postgres://x", False),           # unset + a real DB = FAIL CLOSED
])
def test_runner_gate_fails_closed(monkeypatch, env, database_url, expected):
    """The case that matters: ENVIRONMENT unset with a real DATABASE_URL —
    which is Railway today — must be DISABLED."""
    monkeypatch.delenv("ENVIRONMENT", raising=False)
    monkeypatch.delenv("DATABASE_URL", raising=False)
    if env is not None:
        monkeypatch.setenv("ENVIRONMENT", env)
    if database_url is not None:
        monkeypatch.setenv("DATABASE_URL", database_url)
    assert test_runner_enabled() is expected


# ---------------------------------------------------------------------------
# the route
# ---------------------------------------------------------------------------

def test_run_tests_is_refused_on_a_production_like_deployment(monkeypatch):
    monkeypatch.setenv("ENVIRONMENT", "production")
    c = TestClient(app)
    owner = _user(c, "owner")
    r = c.post("/api/v1/admin/run-tests", json={"verbose": False}, headers=owner)
    assert r.status_code == 403, r.text
    assert "disabled on this deployment" in r.text


def test_run_tests_is_refused_when_environment_is_unset_but_a_database_is_configured(monkeypatch):
    """Railway's exact shape today."""
    monkeypatch.delenv("ENVIRONMENT", raising=False)
    monkeypatch.setenv("DATABASE_URL", "postgres://example/db")
    c = TestClient(app)
    owner = _user(c, "owner")
    assert c.post("/api/v1/admin/run-tests", json={"verbose": False},
                  headers=owner).status_code == 403


def test_only_the_platform_owner_reaches_these_endpoints(monkeypatch):
    """The old check was a stub that let every signed-in user through — a
    venue's casual staff could read internal paths and start a run."""
    monkeypatch.setenv("ENVIRONMENT", "development")
    c = TestClient(app)
    for role in ("staff", "manager"):
        h = _user(c, role)
        assert c.get("/api/v1/admin/test-report", headers=h).status_code == 403, role
        assert c.get("/api/v1/admin/test-coverage", headers=h).status_code == 403, role
        assert c.post("/api/v1/admin/run-tests", json={"verbose": False},
                      headers=h).status_code == 403, role


def test_unauthenticated_callers_are_refused():
    c = TestClient(app)
    assert c.post("/api/v1/admin/run-tests", json={"verbose": False}).status_code in (401, 403)
    assert c.get("/api/v1/admin/test-report").status_code in (401, 403)
