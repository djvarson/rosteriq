"""
Wembley pilot: managing the team after the Deputy import.

* A staff record can be edited (name, contact, rate with the date it starts,
  type, level, areas, hours cap, work rights). An hourly rate over $150 is
  refused as a salary typed into the hourly box, and no record may take an
  email another record at the venue already carries.
* Someone who leaves is marked inactive: off generation, call-ins (SOS),
  cover, the kiosk and join codes, with their history kept. Postgres stores
  the flag and no longer hides inactive rows from history.
* The venue's own full manager can remove a leaver's personal details; the
  name goes from feed posts, comments and procedure sign-offs too, and their
  login is switched off (or loses just this venue if they work elsewhere).
* Join codes for everyone not linked yet, as JSON, CSV or a printable sheet.
* Notice and procedure audiences: the venue's areas, its staff's skills, and
  "All bar"-style groups, matched the same way on both.
"""

import asyncio
import inspect
import json
import os
import re
import shutil
import subprocess
import uuid
from contextlib import contextmanager
from datetime import date, datetime, time, timedelta
from decimal import Decimal

import pytest
from fastapi.testclient import TestClient

from rosteriq.api import app
from rosteriq.database import PostgresStore, get_db
from rosteriq.models import (
    AwardLevel, Employee, EmploymentType, Roster, Shift, ShiftStatus, State, VenueConfig,
)

PW = "Passw0rd!234"
NOW = datetime(2026, 7, 1)
WEM, APO = "wem-b4", "apo-b4"


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _login(c, email, role, venue_ids, section_grants=None):
    c.post("/api/auth/register", json={"email": email, "password": PW, "name": "U"})
    db = get_db()
    rec = db.get_user_by_email(email)
    rec["role"] = role
    rec["venue_ids"] = list(venue_ids)
    if section_grants:
        rec["section_grants"] = section_grants
    db.save_user(rec)
    tok = c.post("/api/auth/login", json={"email": email, "password": PW}).json()["access_token"]
    return {"Authorization": f"Bearer {tok}"}


def _email(tag):
    return f"{tag}_{uuid.uuid4().hex[:8]}@x.com"


def _venue(db, vid, name="The Wembley", areas=None):
    db.save_venue(VenueConfig(
        id=vid, name=name, tanda_org_id="", state=State.wa, timezone="Australia/Perth",
        min_staff={}, max_labour_pct=30.0, areas=areas or [], created_at=NOW,
    ))


def _emp(db, vid, eid, name, email=None, skills=("front bar",), active=True, phone=None,
         rate="30.00"):
    e = Employee(
        id=eid, venue_id=vid, name=name, employment_type=EmploymentType.casual,
        award_level=AwardLevel.level_2, state=State.wa, hourly_base_rate=Decimal(rate),
        email=email, phone=phone, skills=list(skills), active=active,
        created_at=NOW, updated_at=NOW,
    )
    db.save_employee(e)
    return e


def _published_shift(db, vid, emp_id, shift_id, day=None, start=time(17, 0), end=time(23, 0)):
    day = day or date.today()
    ws = day - timedelta(days=day.weekday())
    rid = f"{vid}-{shift_id}-r"
    db.save_roster(Roster(
        id=rid, venue_id=vid, week_start=ws, week_end=ws + timedelta(days=6),
        shifts=[Shift(id=shift_id, employee_id=emp_id, date=day, start_time=start,
                      end_time=end, break_minutes=30, status=ShiftStatus.scheduled,
                      role="front bar")],
        total_cost=None, created_at=NOW,
    ))
    db.update_roster_state(rid, "published", "test", "test")
    return day


@pytest.fixture
def wem():
    """The Wembley with a manager (Ben, who also runs Apothecary), a bar
    department manager, a platform owner and a few staff."""
    c = TestClient(app)
    db = get_db()
    _venue(db, WEM, areas=[
        {"name": "front bar", "label": "Front Bar"},
        {"name": "sports bar", "label": "Sports Bar"},
        {"name": "bottle shop", "label": "Bottle Shop"},
        {"name": "kitchen", "label": "Kitchen"},
    ])
    _venue(db, APO, name="Apothecary")
    ben_email = _email("ben")
    w = {
        "c": c, "db": db,
        "ben": _login(c, ben_email, "manager", [WEM, APO]),
        "ben_email": ben_email,
        "bar_lead": _login(c, _email("barlead"), "manager", [WEM], {WEM: ["front bar"]}),
        "apo_mgr": _login(c, _email("apo"), "manager", [APO]),
        "owner": _login(c, _email("owner"), "owner", []),
    }
    _emp(db, WEM, "w-liam", "Liam Hart", email="liam@x.com", skills=["front bar"], phone="0400 111 222")
    _emp(db, WEM, "w-ava", "Ava Moss", email="ava@x.com", skills=["sports bar", "bottle shop"])
    _emp(db, WEM, "w-kai", "Kai Long", email="kai@x.com", skills=["barista", "kitchen"])
    _emp(db, APO, "a-liam", "Liam Hart", email="liam@x.com", skills=["bar"])
    return w


def _detail(r):
    body = r.json()
    return body.get("detail") or (body.get("error") or {}).get("message") or ""


# ---------------------------------------------------------------------------
# Employee.active on the model and on Postgres
# ---------------------------------------------------------------------------

class _Cur:
    def __init__(self, log, rows):
        self.log, self.rows, self.rowcount = log, rows, 1

    def execute(self, sql, params=None):
        self.log.append((" ".join(sql.split()), params))

    def fetchall(self):
        return list(self.rows)

    def fetchone(self):
        return self.rows[0] if self.rows else None


class _PG:
    _TABLE_DDL = PostgresStore._TABLE_DDL
    _ensure_table = PostgresStore._ensure_table
    _row_to_employee = PostgresStore._row_to_employee

    def __init__(self, rows=()):
        self.log, self.rows = [], list(rows)

    @contextmanager
    def _cursor(self):
        yield _Cur(self.log, self.rows)

    def get_venue(self, venue_id):
        return None


def _pg_row(**over):
    row = {
        "id": "e1", "venue_id": "v", "tanda_id": None, "name": "Liam", "employment_type": "casual",
        "award_level": "level_2", "hourly_base_rate": Decimal("30"), "skills": ["bar"],
        "availability": {}, "max_hours_per_week": 38, "consecutive_days": 6, "phone": None,
        "email": None, "visa_status": None, "visa_expiry": None, "visa_work_limit_fortnight": None,
        "state": "wa", "created_at": NOW, "updated_at": NOW,
    }
    row.update(over)
    return row


def test_employee_is_active_by_default_and_postgres_keeps_the_flag():
    e = Employee(id="x", name="X", employment_type="casual", award_level="level_1", state="wa",
                 hourly_base_rate=Decimal("30"), created_at=NOW, updated_at=NOW)
    assert e.active is True and e.rate_history == []

    pg = _PG()
    e.active = False
    e.rate_history = [{"rate": "32.00", "effective_from": "2026-07-01"}]
    PostgresStore.save_employee(pg, e)
    stmt, params = [(s, p) for s, p in pg.log if s.startswith("INSERT INTO employees")][0]
    assert "active=EXCLUDED.active" in stmt and "rate_history=EXCLUDED.rate_history" in stmt
    assert False in params and any("32.00" in str(p) for p in params)

    # An inactive row reads back inactive; a row from before the column existed is active.
    assert PostgresStore._row_to_employee(_PG(), _pg_row(active=False)).active is False
    assert PostgresStore._row_to_employee(_PG(), _pg_row()).active is True
    assert PostgresStore._row_to_employee(
        _PG(), _pg_row(rate_history='[{"rate": "31.00"}]')).rate_history == [{"rate": "31.00"}]


def test_postgres_listings_keep_inactive_people_for_history():
    pg = _PG(rows=[_pg_row(active=False)])
    assert [e.active for e in PostgresStore.list_employees(pg)] == [False]
    assert [e.active for e in PostgresStore.get_employees(pg, "v")] == [False]
    assert not any("active = true" in s for s, _ in pg.log)
    ddl = inspect.getsource(PostgresStore._ensure_core_schema)
    assert "ADD COLUMN IF NOT EXISTS active BOOLEAN" in ddl
    assert "ADD COLUMN IF NOT EXISTS rate_history JSONB" in ddl


# ---------------------------------------------------------------------------
# Deactivate / reactivate
# ---------------------------------------------------------------------------

def test_manager_marks_a_leaver_inactive_and_history_stays(wem):
    c, db = wem["c"], wem["db"]
    _published_shift(db, WEM, "w-liam", "liam-past", day=date.today() - timedelta(days=7))
    _published_shift(db, WEM, "w-liam", "liam-next", day=date.today() + timedelta(days=1))

    r = c.post("/employees/w-liam/deactivate", headers=wem["ben"])
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["status"] == "inactive" and body["changed"] is True
    assert [s["shift_id"] for s in body["upcoming_shifts"]] == ["liam-next"]
    assert "give those to someone else" in body["message"]

    assert db.get_employee("w-liam").active is False
    assert db.get_employee("a-liam").active is True          # Apothecary record untouched
    listed = c.get(f"/employees?venue_id={WEM}", headers=wem["ben"]).json()["items"]
    assert {e["id"]: e["active"] for e in listed}["w-liam"] is False
    assert db.get_roster(f"{WEM}-liam-past-r").shifts[0].employee_id == "w-liam"
    events = db.list_events(venue_id=WEM, action_prefix="employee.deactivate")
    assert events and events[0]["resource_id"] == "w-liam"

    again = c.post("/employees/w-liam/deactivate", headers=wem["ben"]).json()
    assert again["changed"] is False

    r = c.post("/employees/w-liam/reactivate", headers=wem["ben"])
    assert r.status_code == 200 and db.get_employee("w-liam").active is True
    assert db.list_events(venue_id=WEM, action_prefix="employee.reactivate")


def test_deactivate_is_the_employees_own_venue_managers_call(wem):
    c = wem["c"]
    assert c.post("/employees/w-liam/deactivate", headers=wem["apo_mgr"]).status_code == 404
    assert c.post("/employees/w-liam/deactivate", headers=wem["bar_lead"]).status_code == 403
    staff = _login(c, "liam@x.com", "staff", [WEM])
    assert c.post("/employees/w-ava/deactivate", headers=staff).status_code == 403
    assert c.post("/employees/nobody/deactivate", headers=wem["ben"]).status_code == 404
    assert c.post("/employees/w-liam/deactivate", headers=wem["owner"]).status_code == 200
    demo = {"Authorization": f"Bearer {c.post('/api/auth/demo').json()['access_token']}"}
    assert c.post("/employees/w-ava/deactivate", headers=demo).status_code == 403
    assert get_db().get_employee("w-ava").active is True


def test_inactive_people_are_never_offered_work(wem):
    c, db = wem["c"], wem["db"]
    from rosteriq.api import _venue_employees
    from rosteriq.ai_agent import AgentContext
    c.post("/employees/w-liam/deactivate", headers=wem["ben"])

    # Generation pool
    assert "w-liam" not in {e.id for e in _venue_employees(WEM)}
    # SOS call-in list
    sos = json.loads(asyncio.run(AgentContext(WEM).execute_tool("find_available_staff", {})))
    named = {e["name"] for e in sos["available"] + sos["unavailable"]}
    assert "Liam Hart" not in named and "Ava Moss" in named
    # Kiosk board, and the kiosk refuses a clock-in
    board = c.get(f"/api/clock/board?venue_id={WEM}", headers=wem["ben"]).json()["staff"]
    assert "w-liam" not in {s["employee_id"] for s in board}
    r = c.post("/api/clock/in", json={"venue_id": WEM, "employee_id": "w-liam"}, headers=wem["ben"])
    assert r.status_code == 409 and "no longer active" in _detail(r)
    # Notices
    r = c.post("/api/announcements", json={"venue_id": WEM, "title": "Derby", "body": "In at 4",
                                           "audience": ["front bar"]}, headers=wem["ben"])
    assert r.status_code == 422          # Liam was the only front bar person


def test_inactive_staff_see_no_cover_and_cannot_claim(wem):
    c, db = wem["c"], wem["db"]
    _published_shift(db, WEM, "w-ava", "ava-shift", day=date.today() + timedelta(days=1))
    ava = _login(c, "ava@x.com", "staff", [WEM])
    liam = _login(c, "liam@x.com", "staff", [WEM])
    cover = c.post("/api/me/shifts/ava-shift/cover", json={"reason": "Sick"}, headers=ava).json()
    assert [x["id"] for x in c.get("/api/me/cover", headers=liam).json()["up_for_grabs"]] \
        == [cover["cover_id"]]

    c.post("/employees/w-liam/deactivate", headers=wem["ben"])
    prof = c.get("/api/me/profile", headers=liam).json()
    assert prof["linked"] is True and prof["venue_id"] == WEM    # his history is still his
    assert c.get("/api/me/cover", headers=liam).json()["up_for_grabs"] == []
    r = c.post(f"/api/me/cover/{cover['cover_id']}/claim", headers=liam)
    assert r.status_code == 403 and "no longer active" in _detail(r)

    # Once his login also holds Apothecary, it links to the record where he still works.
    db.save_user({**db.get_user_by_email("liam@x.com"), "venue_ids": [WEM, APO]})
    assert c.get("/api/me/profile", headers=liam).json()["venue_id"] == APO


def test_inactive_record_join_code_is_refused(wem):
    c, db = wem["c"], wem["db"]
    from rosteriq.services.linking import join_code
    _emp(db, WEM, "w-trial", "Trial Tess", skills=["front bar"], active=False)
    h = _login(c, _email("tess"), "staff", [])
    r = c.post("/api/me/link", json={"code": join_code("w-trial")}, headers=h)
    assert r.status_code == 400 and "isn't active" in _detail(r)


def test_a_whole_record_upsert_does_not_quietly_reactivate(wem):
    c, db = wem["c"], wem["db"]
    c.post("/employees/w-liam/deactivate", headers=wem["ben"])
    r = c.post("/employees", json={
        "id": "w-liam", "venue_id": WEM, "name": "Liam Hart", "employment_type": "casual",
        "award_level": "level_2", "state": "wa", "hourly_base_rate": "33.00",
        "email": "liam@x.com", "skills": ["front bar"],
        "created_at": "2026-07-01T00:00:00", "updated_at": "2026-07-01T00:00:00",
    }, headers=wem["ben"])
    assert r.status_code == 200, r.text
    after = db.get_employee("w-liam")
    assert after.active is False
    assert [h["rate"] for h in after.rate_history] == ["33.00"]
    assert after.rate_history[0]["previous_rate"] == "30.00"


# ---------------------------------------------------------------------------
# Edit a staff record
# ---------------------------------------------------------------------------

def test_manager_edits_a_staff_record(wem):
    c, db = wem["c"], wem["db"]
    today = date.today()
    r = c.put("/employees/w-ava", json={
        "name": "  Ava   Moss-Reid ", "email": "ava.reid@x.com", "phone": "0400 999 888",
        "hourly_base_rate": "34.55", "rate_effective_from": today.isoformat(),
        "employment_type": "part_time", "award_level": "level_3",
        "skills": ["Front Bar", "sports bar", "Functions"], "max_hours_per_week": 30,
        "visa_status": "student", "visa_expiry": (today + timedelta(days=200)).isoformat(),
        "visa_work_limit_fortnight": 48,
    }, headers=wem["ben"])
    assert r.status_code == 200, r.text
    assert r.json()["status"] == "updated" and r.json()["notes"] == []
    e = db.get_employee("w-ava")
    assert e.name == "Ava Moss-Reid" and e.email == "ava.reid@x.com" and e.phone == "0400 999 888"
    assert e.hourly_base_rate == Decimal("34.55")
    assert e.employment_type == EmploymentType.part_time and e.award_level == AwardLevel.level_3
    assert e.skills == ["front bar", "sports bar", "functions"]
    assert e.max_hours_per_week == 30 and e.visa_status == "student"
    assert e.visa_work_limit_fortnight == 48
    assert e.rate_history[-1]["rate"] == "34.55" and e.rate_history[-1]["previous_rate"] == "30.00"
    assert e.rate_history[-1]["effective_from"] == today.isoformat()
    upd = db.list_events(venue_id=WEM, action_prefix="employee.rate_change")
    assert upd and upd[0]["resource_id"] == "w-ava"

    # Only what is sent changes; null clears an optional field.
    r = c.put("/employees/w-ava", json={"visa_status": None, "visa_expiry": None,
                                        "visa_work_limit_fortnight": None}, headers=wem["ben"])
    e = db.get_employee("w-ava")
    assert r.status_code == 200 and e.visa_status is None and e.visa_work_limit_fortnight is None
    assert e.name == "Ava Moss-Reid"


def test_rate_change_dates(wem):
    c, db = wem["c"], wem["db"]
    today = date.today()
    r = c.put("/employees/w-kai", json={"hourly_base_rate": "36.00",
                                        "rate_effective_from": (today + timedelta(days=7)).isoformat()},
              headers=wem["ben"])
    assert r.status_code == 422 and "can't start in the future" in _detail(r)
    assert db.get_employee("w-kai").hourly_base_rate == Decimal("30.00")

    back = today - timedelta(days=10)
    r = c.put("/employees/w-kai", json={"hourly_base_rate": "36.00",
                                        "rate_effective_from": back.isoformat()}, headers=wem["ben"])
    assert r.status_code == 200, r.text
    assert "advisor to confirm" in r.json()["notes"][0] and "$30.00" in r.json()["notes"][0]
    assert db.get_employee("w-kai").rate_history[-1]["effective_from"] == back.isoformat()


def test_an_hourly_rate_over_150_is_refused_everywhere(wem):
    c, db = wem["c"], wem["db"]
    r = c.put("/employees/w-kai", json={"hourly_base_rate": "1250.00"}, headers=wem["ben"])
    assert r.status_code == 422 and "weekly or annual salary" in _detail(r)
    r = c.post("/employees", json={
        "id": "w-venue-mgr", "venue_id": WEM, "name": "Ben Ingram", "employment_type": "full_time",
        "award_level": "level_6", "state": "wa", "hourly_base_rate": "78000.00",
        "created_at": "2026-07-01T00:00:00", "updated_at": "2026-07-01T00:00:00",
    }, headers=wem["ben"])
    assert r.status_code == 422 and "$78,000.00 an hour" in _detail(r)
    assert db.get_employee("w-venue-mgr") is None
    assert c.put("/employees/w-kai", json={"hourly_base_rate": "150.00"},
                 headers=wem["ben"]).status_code == 200


def test_an_email_another_record_here_carries_is_refused(wem):
    c, db = wem["c"], wem["db"]
    r = c.put("/employees/w-kai", json={"email": "LIAM@x.com"}, headers=wem["ben"])
    assert r.status_code == 409 and "Liam Hart" in _detail(r)
    assert db.get_employee("w-kai").email == "kai@x.com"
    # Same person at the other venue is fine (Apothecary shares staff).
    _emp(db, APO, "a-kai", "Kai Long", email="kai.apo@x.com")
    assert c.put("/employees/a-kai", json={"email": "kai@x.com"},
                 headers=wem["ben"]).status_code == 200
    # Add Staff can't take a leaver's email either; the message says reactivate.
    c.post("/employees/w-liam/deactivate", headers=wem["ben"])
    r = c.post("/employees", json={
        "id": "w-liam-2", "venue_id": WEM, "name": "Liam Hart", "employment_type": "casual",
        "award_level": "level_2", "state": "wa", "hourly_base_rate": "30.00", "email": "liam@x.com",
        "created_at": "2026-07-01T00:00:00", "updated_at": "2026-07-01T00:00:00",
    }, headers=wem["ben"])
    assert r.status_code == 409 and "reactivate" in _detail(r)


def test_edit_is_gated_and_validated(wem):
    c = wem["c"]
    assert c.put("/employees/w-kai", json={"name": "X"}, headers=wem["apo_mgr"]).status_code == 404
    assert c.put("/employees/w-kai", json={"name": "X"}, headers=wem["bar_lead"]).status_code == 403
    staff = _login(c, "kai@x.com", "staff", [WEM])
    assert c.put("/employees/w-kai", json={"hourly_base_rate": "99"}, headers=staff).status_code == 403
    for body, words in (({"name": "  "}, "blank"), ({"email": "n/a"}, "isn't an email"),
                        ({"max_hours_per_week": 200}, "between 1 and 60"),
                        ({"visa_work_limit_fortnight": 500}, "between 1 and 152")):
        r = c.put("/employees/w-kai", json=body, headers=wem["ben"])
        assert r.status_code == 422 and words in _detail(r), (body, r.text)
    assert get_db().get_employee("w-kai").name == "Kai Long"


def test_import_update_records_the_rate_change(wem):
    c, db = wem["c"], wem["db"]
    r = c.post("/api/setup/import-staff", json={
        "venue_id": WEM, "update_existing": True,
        "content": "Name,Email,Pay rate\nKai Long,kai@x.com,32.40\n",
    }, headers=wem["ben"])
    assert r.status_code == 200, r.text
    e = db.get_employee("w-kai")
    assert e.hourly_base_rate == Decimal("32.40")
    assert e.rate_history[-1]["previous_rate"] == "30.00" and e.rate_history[-1]["changed_by"] == "import"


# ---------------------------------------------------------------------------
# Remove a leaver's personal details
# ---------------------------------------------------------------------------

def _seed_name_copies(db):
    liam_uid = db.get_user_by_email("liam@x.com")["id"]
    db.save_feed_post({"id": "p1", "venue_id": WEM, "author_user_id": liam_uid,
                       "author_name": "Liam Hart", "author_role": "staff", "body": "Swap Sat?",
                       "comments": [{"id": "c1", "author_user_id": liam_uid, "author_name": "Liam Hart",
                                     "body": "anyone"}], "reactions": {}, "created_at": NOW})
    db.save_feed_post({"id": "p-apo", "venue_id": APO, "author_user_id": liam_uid,
                       "author_name": "Liam Hart", "author_role": "staff", "body": "hi",
                       "comments": [], "reactions": {}, "created_at": NOW})
    db.save_sop_ack({"id": "k1", "venue_id": WEM, "doc_id": "d1", "doc_version": 1,
                     "employee_id": "w-liam", "employee_name": "Liam Hart", "acknowledged_at": NOW})
    return liam_uid


def test_venue_manager_removes_a_leavers_details(wem):
    c, db = wem["c"], wem["db"]
    _login(c, "liam@x.com", "staff", [WEM])
    liam_uid = _seed_name_copies(db)

    r = c.post("/api/privacy/anonymise/employee/w-liam", headers=wem["ben"])
    assert r.status_code == 200, r.text
    assert r.json()["logins_deactivated"] == 1
    e = db.get_employee("w-liam")
    assert "Liam" not in e.name and e.phone is None and e.active is False
    anon = e.name
    post = db.get_feed_post("p1")
    assert post["author_name"] == anon and post["comments"][0]["author_name"] == anon
    assert db.get_feed_post("p-apo")["author_name"] == "Liam Hart"   # another venue's feed
    assert db.list_sop_acks(WEM)[0]["employee_name"] == anon
    login = db.get_user_by_id(liam_uid)
    assert login["is_active"] is False and login["venue_ids"] == []
    assert db.get_employee("a-liam").name == "Liam Hart"               # Apothecary keeps him

    row = db.list_events(venue_id=WEM, action_prefix="privacy.anonymise")[0]
    assert "Liam" not in json.dumps(row["details"], default=str)
    # The removed record can't be edited or reactivated back into a person.
    assert c.put("/employees/w-liam", json={"name": "Liam Hart"}, headers=wem["ben"]).status_code == 409
    assert c.post("/employees/w-liam/reactivate", headers=wem["ben"]).status_code == 409


def test_a_login_used_at_another_venue_only_loses_this_one(wem):
    c, db = wem["c"], wem["db"]
    liam = _login(c, "liam@x.com", "staff", [WEM, APO])
    r = c.post("/api/privacy/anonymise/employee/w-liam", headers=wem["ben"])
    assert r.status_code == 200 and r.json()["logins_venue_removed"] == 1
    login = db.get_user_by_email("liam@x.com")
    assert login["is_active"] is True and login["venue_ids"] == [APO]
    assert c.get("/api/me/profile", headers=liam).json()["venue_id"] == APO


def test_remove_details_is_gated(wem):
    c, db = wem["c"], wem["db"]
    url = "/api/privacy/anonymise/employee/w-ava"
    assert c.post(url, headers=wem["apo_mgr"]).status_code == 404
    assert c.post(url, headers=wem["bar_lead"]).status_code == 403
    staff = _login(c, "kai@x.com", "staff", [WEM])
    assert c.post(url, headers=staff).status_code == 403
    assert db.get_employee("w-ava").name == "Ava Moss"
    # Never your own record from here.
    _emp(db, WEM, "w-ben", "Ben Ingram", email=wem["ben_email"], skills=["duty manager"])
    r = c.post("/api/privacy/anonymise/employee/w-ben", headers=wem["ben"])
    assert r.status_code == 400 and db.get_employee("w-ben").name == "Ben Ingram"
    # The platform owner keeps access.
    assert c.post(url, headers=wem["owner"]).status_code == 200


def test_remove_details_never_changes_a_managers_login_as_a_side_effect(wem):
    c, db = wem["c"], wem["db"]
    _emp(db, WEM, "w-dm", "Dana Duty", email="dana@x.com", skills=["duty manager"])
    _login(c, "dana@x.com", "manager", [WEM])
    r = c.post("/api/privacy/anonymise/employee/w-dm", headers=wem["ben"])
    assert r.status_code == 409 and "Set their access to staff" in _detail(r)
    assert db.get_employee("w-dm").name == "Dana Duty"
    assert db.get_user_by_email("dana@x.com")["is_active"] is True
    # The platform owner can.
    assert c.post("/api/privacy/anonymise/employee/w-dm", headers=wem["owner"]).status_code == 200
    assert db.get_user_by_email("dana@x.com")["is_active"] is False


def test_a_shared_email_proves_no_login(wem):
    c, db = wem["c"], wem["db"]
    _login(c, "kai@x.com", "staff", [WEM])
    _emp(db, WEM, "w-kai-dup", "Kai Long (old import)", email="kai@x.com", skills=["kitchen"])
    r = c.post("/api/privacy/anonymise/employee/w-kai-dup", headers=wem["ben"])
    assert r.status_code == 200 and r.json()["logins_deactivated"] == 0
    assert db.get_user_by_email("kai@x.com")["is_active"] is True


def test_postgres_scrub_rewrites_names_in_one_statement_each():
    pg = _PG()
    out = PostgresStore.scrub_person_name(pg, "v", "e1", ["u1"], "Anonymised Employee #e1")
    stmts = [s for s, _ in pg.log if s.startswith("UPDATE")]
    assert any("UPDATE sop_acknowledgements SET employee_name" in s for s in stmts)
    assert any("UPDATE feed_posts SET author_name" in s and "venue_id = %s" in s for s in stmts)
    assert any("jsonb_set(elem, '{author_name}'" in s for s in stmts)
    assert set(out) == {"feed_posts", "sop_acknowledgements"}


# ---------------------------------------------------------------------------
# Join codes in bulk
# ---------------------------------------------------------------------------

def test_join_codes_for_everyone_not_linked_yet(wem):
    c, db = wem["c"], wem["db"]
    from rosteriq.services.linking import join_code
    _login(c, "ava@x.com", "staff", [WEM])                         # Ava has linked
    _emp(db, WEM, "w-left", "Left Lou", email="lou@x.com", active=False)
    _emp(db, WEM, "w-new", "=HYPERLINK(\"x\")", phone="0411 000 000")

    r = c.get(f"/api/venues/{WEM}/join-codes", headers=wem["ben"])
    assert r.status_code == 200, r.text
    people = {p["employee_id"]: p for p in r.json()["people"]}
    assert set(people) == {"w-liam", "w-kai", "w-new"}
    assert people["w-liam"]["join_code"] == join_code("w-liam")
    assert people["w-liam"]["mobile"] == "0400 111 222"
    assert people["w-kai"]["link"].endswith("/register?join=1")

    csv_r = c.get(f"/api/venues/{WEM}/join-codes?format=csv", headers=wem["ben"])
    assert csv_r.status_code == 200 and csv_r.headers["content-type"].startswith("text/csv")
    lines = csv_r.text.strip().splitlines()
    assert lines[0] == "Name,Mobile,Email,Join code,Sign-up link"
    assert any(l.startswith("\"'=HYPERLINK") for l in lines)          # never a live formula
    assert any(join_code("w-kai") in l for l in lines)
    assert db.list_events(venue_id=WEM, action_prefix="staff.join_codes_export")


def test_join_codes_are_manager_only(wem):
    c = wem["c"]
    assert c.get(f"/api/venues/{WEM}/join-codes", headers=wem["apo_mgr"]).status_code == 403
    assert c.get(f"/api/venues/{WEM}/join-codes", headers=wem["bar_lead"]).status_code == 403
    staff = _login(c, "kai@x.com", "staff", [WEM])
    assert c.get(f"/api/venues/{WEM}/join-codes", headers=staff).status_code == 403


# ---------------------------------------------------------------------------
# Audiences: areas, skills and "All bar" groups
# ---------------------------------------------------------------------------

def test_group_audience_matches_any_area_with_that_word():
    from rosteriq.services.audience import matches
    front = Employee(id="1", name="F", employment_type="casual", award_level="level_1", state="wa",
                     hourly_base_rate=Decimal("30"), skills=["Front Bar"], created_at=NOW, updated_at=NOW)
    barista = front.model_copy(update={"skills": ["barista"]})
    assert matches(["all bar"], front) and not matches(["all bar"], barista)
    assert matches(["FRONT  bar"], front) and not matches(["bar"], front)
    assert matches([], barista)


def test_bar_notice_reaches_every_bar_and_says_what_to_pick(wem):
    c = wem["c"]
    r = c.post("/api/announcements", json={"venue_id": WEM, "title": "Derby Saturday",
                                           "body": "Everyone in at 4pm", "audience": ["bar"]},
               headers=wem["ben"])
    assert r.status_code == 422 and "'all bar'" in _detail(r)
    r = c.post("/api/announcements", json={"venue_id": WEM, "title": "Derby Saturday",
                                           "body": "Everyone in at 4pm", "audience": ["All Bar"]},
               headers=wem["ben"])
    assert r.status_code == 200, r.text
    seen = {}
    for who in ("liam@x.com", "ava@x.com", "kai@x.com"):
        h = _login(c, who, "staff", [WEM])
        seen[who] = [a["title"] for a in c.get("/api/me/announcements", headers=h).json()["announcements"]]
    assert seen["liam@x.com"] == ["Derby Saturday"] and seen["ava@x.com"] == ["Derby Saturday"]
    assert seen["kai@x.com"] == []                                   # barista isn't bar
    # A front bar department manager can't widen to every bar.
    r = c.post("/api/announcements", json={"venue_id": WEM, "title": "x", "body": "y",
                                           "audience": ["all bar"]}, headers=wem["bar_lead"])
    assert r.status_code == 403


def test_procedure_for_all_bar_applies_to_both_bars(wem):
    c = wem["c"]
    r = c.post("/api/sops/documents", json={"venue_id": WEM, "title": "Glass washer",
                                            "body": "Drain nightly", "applies_to": ["all bar"]},
               headers=wem["ben"])
    assert r.status_code in (200, 201), r.text
    docs = c.get(f"/api/sops/documents?venue_id={WEM}", headers=wem["ben"]).json()["documents"]
    doc = [d for d in docs if d["title"] == "Glass washer"][0]
    stats = doc.get("ack_stats") or doc
    assert sorted(stats["outstanding_names"]) == ["Ava Moss", "Liam Hart"]


def test_audience_options_come_from_areas_skills_and_groups(wem):
    c = wem["c"]
    r = c.get(f"/api/venues/{WEM}/audience-options", headers=wem["bar_lead"])
    assert r.status_code == 200, r.text
    opts = {o["value"]: o for o in r.json()["options"]}
    assert opts["front bar"]["kind"] == "area" and opts["front bar"]["label"] == "Front Bar"
    assert opts["barista"]["kind"] == "skill"
    assert opts["all bar"]["kind"] == "group" and opts["all bar"]["people"] == 2
    assert "bar" not in opts
    staff = _login(c, "kai@x.com", "staff", [WEM])
    assert c.get(f"/api/venues/{WEM}/audience-options", headers=staff).status_code == 403
    assert c.get(f"/api/venues/{WEM}/audience-options", headers=wem["apo_mgr"]).status_code == 403


# ---------------------------------------------------------------------------
# Dashboard: the Staff page controls (run under node with a stub DOM)
# ---------------------------------------------------------------------------

NODE = shutil.which("node")
DASHBOARD = os.path.join(os.path.dirname(__file__), "..", "static", "dashboard.html")


def _dash_js():
    html = open(DASHBOARD, encoding="utf-8").read()
    return re.findall(r"<script[^>]*>([\s\S]*?)</script>", html)[1], html


def _fn(src, name):
    m = re.search(r"^(?:async )?function " + re.escape(name) + r"\(", src, re.M)
    assert m, name
    i, depth = src.index("{", m.end()), 0
    for j in range(i, len(src)):
        depth += {"{": 1, "}": -1}.get(src[j], 0)
        if depth == 0:
            return src[m.start():j + 1]
    raise AssertionError(name)


_PRELUDE = r"""
const _els = {};
function el(id) {
  if (!_els[id]) _els[id] = { id, innerHTML: '', textContent: '', value: '', checked: false, style: {},
    disabled: false, addEventListener() {} };
  return _els[id];
}
global.document = { getElementById: el, querySelector: function () { return null; } };
global.esc = function (s) { return String(s == null ? '' : s).replace(/[&<>]/g, function (c) {
  return { '&': '&amp;', '<': '&lt;', '>': '&gt;' }[c]; }); };
global.localIso = function () { return '2026-10-10'; };
global.toasts = []; global.calls = []; global.opened = [];
global.showToast = function (m, k) { toasts.push([m, k]); };
global.openModal = function (id) { opened.push(id); };
global.closeModal = function () {};
global.loadVenueData = function () {};
global.venueSections = function () { return ['kitchen', 'bar', 'floor']; };
"""


@pytest.mark.skipif(NODE is None, reason="node runs the dashboard JS")
def test_staff_page_edit_inactive_filter_and_rate_guard():
    src, html = _dash_js()
    assert 'id="staff-status-filter"' in html and 'id="modal-edit-employee"' in html
    assert 'id="btn-join-codes"' in html and 'id="ee-remove-confirm"' in html
    js = "\n".join(_fn(src, n) for n in (
        "_weekHoursByEmployee", "_fmtAward", "renderStaffTable", "staffRateProblem", "_titleCase",
        "_editAreaPills", "openEditEmployee", "saveEditEmployee", "audiencePillOptions"))
    prog = _PRELUDE + r"""
var STAFF_MAX_HOURLY_RATE = 150;
var _staffTableWired = true, _staffRowIds = [], _editEmp = null, _editAreaChoices = [];
var _audienceCache = { venue: null, options: null, pending: null };
global._jwtRole = () => 'manager';
global.state = { isDemo: false, currentVenue: 'wem', rosters: [], employeesTotal: 2, employees: [
  { id: 'a', venue_id: 'wem', name: 'Ann <b>', skills: ['front bar'], hourly_base_rate: '30.00' },
  { id: 'b', venue_id: 'wem', name: 'Bob', active: false, skills: ['kitchen'], hourly_base_rate: '31.00' } ] };
global.fetchAPI = async function (url, opts) {
  calls.push([url, opts]);
  if (url.indexOf('/audience-options') >= 0) return { options: [
    { value: 'all bar', label: 'All bar', kind: 'group' }, { value: 'front bar', label: 'Front Bar', kind: 'area' },
    { value: 'sports bar', label: 'Sports Bar', kind: 'area' } ] };
  return { status: 'updated', notes: [] };
};
""" + js + r"""
(async () => {
  renderStaffTable();
  const current = el('staff-body').innerHTML;
  el('staff-status-filter').value = 'inactive';
  renderStaffTable();
  const left = el('staff-body').innerHTML;
  el('staff-status-filter').value = 'active';
  renderStaffTable();
  await openEditEmployee(0);
  const pills = el('ee-areas').innerHTML;
  el('ee-rate').value = '1250';
  await saveEditEmployee();
  const refused = el('ee-result').textContent;
  const putsBefore = calls.filter(c => c[1] && c[1].method === 'PUT').length;
  el('ee-rate').value = '32.5';
  el('ee-area-extra').value = 'Functions, front bar';
  await saveEditEmployee();
  const put = calls.filter(c => c[1] && c[1].method === 'PUT')[0];
  console.log(JSON.stringify({ current, left, pills, refused, putsBefore, put: put && [put[0], JSON.parse(put[1].body)],
                               opened }));
})().catch(e => console.log(JSON.stringify({ error: String(e && e.stack || e) })));
"""
    out = subprocess.run([NODE, "-e", prog], capture_output=True, text=True, timeout=60)
    data = json.loads([l for l in out.stdout.splitlines() if l.startswith("{")][-1])
    assert "error" not in data, data
    assert "Ann &lt;b&gt;" in data["current"] and "Bob" not in data["current"]
    assert "openEditEmployee(0)" in data["current"] and "Join code" in data["current"]
    assert "Bob" in data["left"] and "Inactive" in data["left"] and "Join code" not in data["left"]
    assert "Front Bar" in data["pills"] and "Sports Bar" in data["pills"] and "All bar" not in data["pills"]
    assert "weekly or annual salary" in data["refused"] and data["putsBefore"] == 0
    url, body = data["put"]
    assert url == "/employees/a"
    assert body["hourly_base_rate"] == "32.50" and body["rate_effective_from"] == "2026-10-10"
    assert body["skills"] == ["front bar", "functions"]
    assert data["opened"] == ["modal-edit-employee"]


@pytest.mark.skipif(NODE is None, reason="node runs the dashboard JS")
def test_notice_pills_come_from_the_venues_audience_options():
    src, _ = _dash_js()
    js = "\n".join(_fn(src, n) for n in ("audiencePillOptions", "_audienceOptionsNow", "annAudiencePills"))
    prog = _PRELUDE + r"""
var _audienceCache = { venue: null, options: null, pending: null };
var annAudience = [], annPillSections = [];
global.state = { isDemo: false, currentVenue: 'wem' };
global.fetchAPI = async function (url) {
  calls.push(url);
  return { options: [{ value: 'all bar', label: 'All bar', kind: 'group' },
                     { value: 'front bar', label: 'Front Bar', kind: 'area' }] };
};
""" + js + r"""
(async () => {
  annAudiencePills();
  const first = el('an-audience-pills').innerHTML;
  await new Promise(r => setTimeout(r, 10));
  console.log(JSON.stringify({ first, after: el('an-audience-pills').innerHTML, sections: annPillSections, calls }));
})().catch(e => console.log(JSON.stringify({ error: String(e && e.stack || e) })));
"""
    out = subprocess.run([NODE, "-e", prog], capture_output=True, text=True, timeout=60)
    data = json.loads([l for l in out.stdout.splitlines() if l.startswith("{")][-1])
    assert "error" not in data, data
    assert ">bar<" in data["first"]                     # fallback while the options load
    assert "All bar" in data["after"] and "Front Bar" in data["after"]
    assert data["sections"] == ["all bar", "front bar"]
    assert data["calls"] == ["/api/venues/wem/audience-options"]
