from datetime import date
from tests.test_tenant_scoping_round3 import world  # noqa: F401  (fixture)


def test_cheapest_slot_foreign_employee(world):
    c = world["c"]
    body = {"employee_id": world["emp_b"], "date": date.today().isoformat(), "duration_hours": 1}
    r = c.post("/api/v1/cheapest-slot", json=body, headers=world["mgr_a"])
    print("foreign ->", r.status_code, r.text)
    assert r.status_code == 404
    body["employee_id"] = world["emp_a"]
    r = c.post("/api/v1/cheapest-slot", json=body, headers=world["mgr_a"])
    print("own ->", r.status_code, r.text)
    assert r.status_code == 200
    # unauthenticated
    r = c.post("/api/v1/cheapest-slot", json={"employee_id": world["emp_b"], "date": date.today().isoformat(), "duration_hours": 1})
    print("anon ->", r.status_code, r.text)
