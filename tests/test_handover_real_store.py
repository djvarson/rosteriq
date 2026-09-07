"""
Shift Handover Notes against the REAL store — no fake service.

The feature shipped calling save_to_key/load_from_key/get_employee_shifts,
which existed on no store: every create/get/acknowledge 500'd on any real
deployment ("Handover feature unwired"). These tests pin the whole flow
through the API on MemoryStore, so the store contract can never silently
regress to fakes-only again. Also pins the venue-scope gate on acknowledge
(403 cross-tenant, 404 unknown — landed 2026-09-05).
"""

import uuid
from datetime import date, datetime, time

from fastapi.testclient import TestClient

from rosteriq.api import app
from rosteriq.database import get_db
from rosteriq.models import Shift, ShiftStatus

PW = "Passw0rd!234"


def _login(c, email):
    c.post("/api/auth/register", json={"email": email, "password": PW, "name": "U"})
    tok = c.post("/api/auth/login", json={"email": email, "password": PW}).json()["access_token"]
    return {"Authorization": f"Bearer {tok}"}


def _venue(c, h, vid):
    r = c.post("/venues", json={"id": vid, "name": vid, "state": "wa", "max_labour_pct": 30,
                                "tanda_org_id": "", "created_at": "2026-07-01T00:00:00"},
               headers=h)
    assert r.status_code in (200, 201), r.text


def _shift(vid, sid):
    s = Shift(id=sid, employee_id=f"{vid}-emp", date=date(2026, 9, 8),
              start_time=time(9), end_time=time(17),
              status=ShiftStatus.scheduled, role="bar")
    try:
        s.venue_id = vid
    except Exception:
        pass
    return s


def test_handover_full_flow_on_real_store_with_tenant_gates():
    c = TestClient(app)
    tag = uuid.uuid4().hex[:6]
    h_a = _login(c, f"ho_a_{tag}@x.com")
    _venue(c, h_a, f"ho-a-{tag}")
    h_b = _login(c, f"ho_b_{tag}@x.com")
    _venue(c, h_b, f"ho-b-{tag}")
    vid = f"ho-a-{tag}"

    db = get_db()
    sid = f"sh-{tag}"
    db.save_shift(_shift(vid, sid))

    # create — through the API, persisted via the real KV store methods
    r = c.post(f"/api/v1/shifts/{sid}/handover", json={
        "author_id": f"{vid}-emp", "author_name": "Outgoing Olly",
        "venue_id": vid, "priority": "important",
        "general_notes": {"notes": "Keg 3 is nearly dry — change before the rush."},
    }, headers=h_a)
    assert r.status_code == 201, r.text
    note_id = r.json()["id"]

    # read back by shift
    r = c.get(f"/api/v1/shifts/{sid}/handover", headers=h_a)
    assert r.status_code == 200, r.text

    # cross-tenant acknowledge is a 403 (the venue gate, not a swallowed 500)
    r = c.post(f"/api/v1/handover/{note_id}/acknowledge",
               json={"employee_id": "intruder"}, headers=h_b)
    assert r.status_code == 403, r.text

    # unknown note is a 404, not an existence oracle for other tenants
    r = c.post(f"/api/v1/handover/hn_nope_1/acknowledge",
               json={"employee_id": "x"}, headers=h_a)
    assert r.status_code == 404, r.text

    # the venue's own staff acknowledge fine
    r = c.post(f"/api/v1/handover/{note_id}/acknowledge",
               json={"employee_id": f"{vid}-incoming"}, headers=h_a)
    assert r.status_code == 200, r.text
    assert r.json()["acknowledged_by"] == f"{vid}-incoming"


def test_kv_store_round_trips_on_memory_store():
    db = get_db()
    db.save_to_key("kv-test-1", {"a": 1, "when": datetime(2026, 9, 7, 10)})
    got = db.load_from_key("kv-test-1")
    assert got and got["a"] == 1
    assert db.load_from_key("kv-missing") is None
