from datetime import date
from tests.test_tenant_scoping_round3 import world  # noqa: F401

def test_repro(world):
    c = world["c"]
    body = {"employee_id": world["emp_b"], "date": date.today().isoformat(),
            "options": [{"start_time": "09:00", "end_time": "17:00"}]}
    r = c.post("/api/v1/compare-shift-times", json=body, headers=world["mgr_a"])
    print("foreign ->", r.status_code, r.text[:200])
    assert r.status_code == 404
    body["employee_id"] = world["emp_a"]
    r = c.post("/api/v1/compare-shift-times", json=body, headers=world["mgr_a"])
    print("own ->", r.status_code, r.text[:120])
    assert r.status_code == 200
