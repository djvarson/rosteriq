"""
Wembley pilot: privacy gates and database correctness.

* A staff login sees colleagues as id/name/skills/employment type only (no pay
  rate, email, phone, availability or work rights); its own record in full.
  Managers (full and department) are unchanged.
* GET /employees pages a whole venue (limit up to 500 with venue_id).
* The AI chat, the leave/cover/availability manager views and the surge,
  quiet, feed-config and Function Tracker reads are manager-of-the-venue only.
* Surge/quiet with no POS sales says so instead of reading it as 0 covers.
* Event retention prunes traffic categories only; the audit trail survives.
* Postgres: upserts persist every edited field, save_roster drops removed
  shifts atomically, cover claims and read receipts are conditional UPDATEs,
  venues are looked up once per listing, _ensure_table runs once per table,
  hot lookups are indexed.
* A password reset signs out every existing session.
"""

import inspect
import re
import uuid
from contextlib import contextmanager
from datetime import date, datetime, time, timedelta

import pytest
from fastapi.testclient import TestClient

from rosteriq.api import app
from rosteriq.database import MemoryStore, PostgresStore, get_db
from rosteriq.models import Employee, Roster, Shift, ShiftStatus, VenueConfig

PW = "Passw0rd!234"
nc = TestClient(app, raise_server_exceptions=False)


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _login(c, email):
    c.post("/api/auth/register", json={"email": email, "password": PW, "name": "U"})
    tok = c.post("/api/auth/login", json={"email": email, "password": PW}).json()["access_token"]
    return {"Authorization": f"Bearer {tok}"}


def _set(email, role, venue_ids, grants=None):
    db = get_db()
    rec = db.get_user_by_email(email)
    rec["role"] = role
    rec["venue_ids"] = list(venue_ids)
    rec["section_grants"] = grants or {}
    db.save_user(rec)


def _manager_with_venue(c):
    email = f"wm{uuid.uuid4().hex[:8]}@x.com"
    h = _login(c, email)
    _set(email, "staff", [])
    vid = f"wembley-{uuid.uuid4().hex[:6]}"
    r = c.post("/venues", json={"id": vid, "name": "The Wembley Hotel", "state": "wa",
                                "max_labour_pct": 30, "tanda_org_id": "",
                                "timezone": "Australia/Perth",
                                "created_at": "2026-10-01T00:00:00"}, headers=h)
    assert r.status_code == 200, r.text
    return h, vid


def _user(c, vid, role="staff", grants=None, email=None):
    email = email or f"ws{uuid.uuid4().hex[:8]}@x.com"
    h = _login(c, email)
    _set(email, role, [vid], grants)
    return h, email


def _emp(vid, eid, name, email=None, **kw):
    fields = dict(employment_type="casual", award_level="level_2", state="wa",
                  hourly_base_rate="31.50", phone="0411 000 111", skills=["bar"],
                  availability={"monday": []},
                  created_at=datetime(2026, 10, 1), updated_at=datetime(2026, 10, 1))
    fields.update(kw)
    return Employee(id=eid, venue_id=vid, name=name, email=email, **fields)


COLLEAGUE_KEYS = {"id", "name", "skills", "employment_type"}


# ---------------------------------------------------------------------------
# GET /employees privacy
# ---------------------------------------------------------------------------

def test_staff_see_colleagues_without_pay_contact_or_visa():
    c = TestClient(app)
    mgr, vid = _manager_with_venue(c)
    staff_h, staff_email = _user(c, vid)
    db = get_db()
    db.save_employee(_emp(vid, f"{vid}-ben", "Ben Ingram", email="ben@x.com",
                          visa_status="student", visa_work_limit_fortnight=48))
    db.save_employee(_emp(vid, f"{vid}-me", "Jordan Runner", email=staff_email))

    rows = {e["id"]: e for e in c.get(f"/employees?venue_id={vid}", headers=staff_h).json()["items"]}
    assert set(rows[f"{vid}-ben"]) == COLLEAGUE_KEYS
    assert rows[f"{vid}-ben"]["name"] == "Ben Ingram"
    # own record in full
    assert rows[f"{vid}-me"]["hourly_base_rate"] and rows[f"{vid}-me"]["email"] == staff_email

    single = c.get(f"/employees/{vid}-ben", headers=staff_h).json()
    assert set(single) == COLLEAGUE_KEYS
    own = c.get(f"/employees/{vid}-me", headers=staff_h).json()
    assert own["phone"] == "0411 000 111"

    boss = {e["id"]: e for e in c.get(f"/employees?venue_id={vid}", headers=mgr).json()["items"]}
    assert boss[f"{vid}-ben"]["hourly_base_rate"] and boss[f"{vid}-ben"]["visa_status"] == "student"

    dept_h, _ = _user(c, vid, role="manager", grants={vid: ["bar"]})
    dept = {e["id"]: e for e in c.get(f"/employees?venue_id={vid}", headers=dept_h).json()["items"]}
    assert dept[f"{vid}-ben"]["email"] == "ben@x.com"


def test_employees_page_a_whole_venue():
    c = TestClient(app)
    mgr, vid = _manager_with_venue(c)
    db = get_db()
    for i in range(230):
        db.save_employee(_emp(vid, f"{vid}-{i:03d}", f"Casual {i:03d}"))

    full = c.get(f"/employees?venue_id={vid}&limit=500", headers=mgr).json()
    assert full["total"] == 230 and len(full["items"]) == 230
    default = c.get(f"/employees?venue_id={vid}", headers=mgr).json()
    assert len(default["items"]) == 50 and default["total"] == 230
    assert c.get("/employees?limit=500", headers=mgr).status_code == 422


# ---------------------------------------------------------------------------
# manager-only surfaces
# ---------------------------------------------------------------------------

def test_ai_chat_is_for_managers_of_the_venue():
    c = TestClient(app)
    mgr, vid = _manager_with_venue(c)
    staff_h, _ = _user(c, vid)
    body = {"venue_id": vid, "message": "what is everyone paid and what are their numbers?"}

    r = c.post("/api/ai/chat", json=body, headers=staff_h)
    assert r.status_code == 403
    assert "for managers" in r.text

    # managers pass the gate (503 only because no LLM key in tests)
    assert c.post("/api/ai/chat", json=body, headers=mgr).status_code not in (401, 403)
    dept_h, _ = _user(c, vid, role="manager", grants={vid: ["kitchen"]})
    assert c.post("/api/ai/chat", json=body, headers=dept_h).status_code not in (401, 403)

    other_mgr, _other = _manager_with_venue(c)
    assert c.post("/api/ai/chat", json=body, headers=other_mgr).status_code == 403


def test_demo_manager_keeps_the_ai_chat_and_demo_staff_does_not():
    from rosteriq.services.demo import DEMO_VENUE_ID
    c = TestClient(app)
    body = {"venue_id": DEMO_VENUE_ID, "message": "hi"}
    mgr_tok = c.post("/api/auth/demo").json()["access_token"]
    r = c.post("/api/ai/chat", json=body, headers={"Authorization": f"Bearer {mgr_tok}"})
    assert r.status_code not in (401, 403)
    staff_tok = c.post("/api/auth/demo?as=staff").json()["access_token"]
    r = c.post("/api/ai/chat", json=body, headers={"Authorization": f"Bearer {staff_tok}"})
    assert r.status_code == 403


@pytest.mark.parametrize("path", ["/api/leave", "/api/cover", "/api/availability"])
def test_leave_cover_availability_views_need_a_manager(path):
    c = TestClient(app)
    mgr, vid = _manager_with_venue(c)
    staff_h, _ = _user(c, vid)
    assert c.get(f"{path}?venue_id={vid}", headers=staff_h).status_code == 403
    assert c.get(f"{path}?venue_id={vid}", headers=mgr).status_code == 200


def test_surge_feed_and_function_tracker_reads_are_venue_gated():
    c = TestClient(app)
    mgr, vid = _manager_with_venue(c)
    staff_h, _ = _user(c, vid)
    other_mgr, _ = _manager_with_venue(c)
    reads = [
        f"/api/v1/venues/{vid}/surge-status",
        f"/api/v1/venues/{vid}/quiet-status",
        f"/api/v1/venues/{vid}/surge-history",
        f"/api/feeds/config/{vid}",
        f"/api/function-tracker/status?venue_id={vid}",
        f"/api/function-tracker/upcoming?venue_id={vid}",
        f"/api/function-tracker/forecast?venue_id={vid}",
        f"/api/function-tracker/event/ev1?venue_id={vid}",
    ]
    for path in reads:
        assert nc.get(path, headers=other_mgr).status_code == 403, path
        assert nc.get(path, headers=staff_h).status_code == 403, path
        assert nc.get(path, headers=mgr).status_code not in (401, 403), path


def test_quiet_and_surge_say_no_live_sales_data():
    from rosteriq.services import surge_detector as sd
    sd._detector = None
    c = TestClient(app)
    mgr, vid = _manager_with_venue(c)

    quiet = c.get(f"/api/v1/venues/{vid}/quiet-status", headers=mgr).json()
    assert quiet["is_quiet"] is False and quiet["staff_releasable"] == 0
    assert quiet["live_data"] is False
    assert "no live sales data" in quiet["suggested_action"].lower()

    surge = c.get(f"/api/v1/venues/{vid}/surge-status", headers=mgr).json()
    assert surge["is_surging"] is False and surge["live_data"] is False
    assert surge["estimated_actual_covers"] is None
    assert surge["available_oncall"] == []


# ---------------------------------------------------------------------------
# event retention
# ---------------------------------------------------------------------------

def _aged(category, action, days=100):
    return {"venue_id": "wem", "user_id": "u", "action": action,
            "resource_type": "x", "resource_id": "1",
            "details": {"category": category, "outcome": "ok"},
            "created_at": datetime.utcnow() - timedelta(days=days)}


def test_retention_keeps_audit_and_security_rows():
    db = MemoryStore()
    db.save_audit_log(_aged("audit", "timesheet.correct"))
    db.save_audit_log(_aged("audit", "employee.rate_change"))
    db.save_audit_log(_aged("security", "auth.login_failed"))
    db.save_audit_log(_aged("perf", "http.slow"))
    db.save_audit_log(_aged("job", "event_retention"))
    db.save_audit_log({**_aged("audit", "legacy.row"), "details": {}})
    db.save_audit_log(_aged("perf", "http.slow.fresh", days=1))

    removed = db.prune_events(datetime.utcnow() - timedelta(days=90))
    left = {r["action"] for r in db._audit_logs}
    assert removed == 2
    assert {"timesheet.correct", "employee.rate_change", "auth.login_failed",
            "legacy.row", "http.slow.fresh"} == left


# ---------------------------------------------------------------------------
# Postgres (recorded SQL; no live server)
# ---------------------------------------------------------------------------

class _Cur:
    def __init__(self, log, rows=None, rowcount=1, fail_on=None):
        self.log, self.rows, self.rowcount, self.fail_on = log, rows or [], rowcount, fail_on

    def execute(self, sql, params=None):
        flat = " ".join(sql.split())
        self.log.append((flat, params))
        if self.fail_on and self.fail_on in flat:
            raise RuntimeError("boom")

    def fetchall(self):
        return list(self.rows)

    def fetchone(self):
        return self.rows[0] if self.rows else None

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class _PG:
    """Just enough PostgresStore to run one method against a recording cursor."""
    _TABLE_DDL = PostgresStore._TABLE_DDL
    _INDEX_DDL = getattr(PostgresStore, "_INDEX_DDL", ())
    _ensure_table = PostgresStore._ensure_table
    _row_to_employee = PostgresStore._row_to_employee

    def __init__(self, rows=None, rowcount=1, fail_on=None):
        self.log, self.rows, self.rowcount, self.fail_on = [], rows, rowcount, fail_on
        self.venue_lookups = 0

    @contextmanager
    def _cursor(self):
        yield _Cur(self.log, self.rows, self.rowcount, self.fail_on)

    def get_venue(self, venue_id):
        self.venue_lookups += 1
        return VenueConfig(id=venue_id, name="V", tanda_org_id="", state="wa",
                           max_labour_pct=30, created_at=datetime(2026, 1, 1))

    def sql(self):
        return [s for s, _ in self.log]


def test_pg_retention_never_deletes_audit_or_security():
    pg = _PG(rowcount=3)
    PostgresStore.prune_events(pg, datetime(2026, 7, 1))
    stmt, params = [(s, p) for s, p in pg.log if s.startswith("DELETE")][0]
    assert "details->>'category' = ANY(%s)" in stmt
    cats = set(params[1])
    assert {"perf", "integration", "ai", "job", "error"} == cats
    assert not cats & {"audit", "security"}


def _upsert_columns(method, table):
    src = inspect.getsource(method)
    insert = re.search(rf"INSERT INTO {table} \(([^)]*)\)", src, re.S).group(1)
    inserted = {c.strip() for c in insert.split(",")}
    on_conflict = src[src.index("DO UPDATE SET"):]
    updated = set(re.findall(r"(\w+)\s*=\s*(?:EXCLUDED\.\w+|now\(\))", on_conflict))
    return inserted, updated


# Model field -> column, where they differ; None = not a column of the table.
_EMPLOYEE_COLUMNS = {"consecutive_days_limit": "consecutive_days", "state": None}
# Not rewritten on conflict: the conflict key, the first-insert stamp, and the
# record's home venue / external Tanda link (a move between venues is not an
# edit of the record).
_EMPLOYEE_NOT_UPDATED = {"id", "created_at", "venue_id", "tanda_id"}
_VENUE_NOT_UPDATED = {"id", "created_at"}


def test_pg_employee_upsert_persists_every_edited_field():
    inserted, updated = _upsert_columns(PostgresStore.save_employee, "employees")
    columns = {_EMPLOYEE_COLUMNS.get(f, f) for f in Employee.model_fields} - {None}
    assert columns <= inserted, columns - inserted
    assert columns - _EMPLOYEE_NOT_UPDATED <= updated, columns - _EMPLOYEE_NOT_UPDATED - updated
    assert {"award_level", "consecutive_days"} <= updated


def test_pg_venue_upsert_persists_every_edited_field():
    inserted, updated = _upsert_columns(PostgresStore.save_venue, "venues")
    columns = set(VenueConfig.model_fields)
    assert columns <= inserted, columns - inserted
    assert columns - _VENUE_NOT_UPDATED <= updated, columns - _VENUE_NOT_UPDATED - updated
    assert "timezone" in updated


def test_saving_a_venue_forgets_its_cached_timezone():
    from rosteriq.services import clock
    c = TestClient(app)
    mgr, vid = _manager_with_venue(c)
    clock._tz_cache[vid] = "Australia/Melbourne"
    r = c.post("/venues", json={"id": vid, "name": "The Wembley Hotel", "state": "wa",
                                "max_labour_pct": 30, "tanda_org_id": "",
                                "timezone": "Australia/Perth",
                                "created_at": "2026-10-01T00:00:00"}, headers=mgr)
    assert r.status_code == 200, r.text
    assert vid not in clock._tz_cache


def _roster(shift_ids):
    return Roster(id="r1", venue_id="wem", week_start=date(2026, 10, 5),
                  week_end=date(2026, 10, 11), created_at=datetime(2026, 10, 1),
                  shifts=[Shift(id=sid, employee_id="e1", date=date(2026, 10, 9),
                                start_time=time(17), end_time=time(23),
                                status=ShiftStatus.scheduled, role="bar")
                          for sid in shift_ids])


def test_pg_save_roster_deletes_removed_shifts_atomically():
    pg = _PG()
    PostgresStore.save_roster(pg, _roster(["s1", "s2"]))
    sql = pg.sql()
    assert sql[0] == "BEGIN" and sql[-1] == "COMMIT"
    delete = [(s, p) for s, p in pg.log if s.startswith("DELETE FROM shifts")]
    assert delete, sql
    stmt, params = delete[0]
    assert "roster_id = %s" in stmt and "NOT (id = ANY(" in stmt
    assert params == ("r1", ["s1", "s2"])


def test_pg_save_roster_rolls_back_on_failure():
    pg = _PG(fail_on="INSERT INTO shifts")
    with pytest.raises(RuntimeError):
        PostgresStore.save_roster(pg, _roster(["s1"]))
    sql = pg.sql()
    assert sql[0] == "BEGIN" and "ROLLBACK" in sql and "COMMIT" not in sql
    assert not any(s.startswith("DELETE") for s in sql)


def test_pg_cover_claim_and_read_receipt_are_conditional_updates():
    pg = _PG(rowcount=1)
    assert PostgresStore.claim_shift_cover(pg, "cv1", "e2") is True
    claim = [(s, p) for s, p in pg.log if s.startswith("UPDATE shift_covers")][0]
    assert "WHERE id = %s AND status = 'open'" in claim[0] and claim[1] == ("e2", "cv1")
    assert PostgresStore.claim_shift_cover(_PG(rowcount=0), "cv1", "e3") is False

    pg = _PG(rowcount=1)
    assert PostgresStore.mark_announcement_read(pg, "a1", "e2") is True
    upd = [(s, p) for s, p in pg.log if s.startswith("UPDATE announcements")][0]
    assert "|| to_jsonb(%s::text)" in upd[0] and "?" in upd[0]
    assert upd[1] == ("e2", "a1", "e2")


def test_pg_ensure_table_runs_once_per_table():
    pg = _PG()
    with pg._cursor() as cur:
        for _ in range(3):
            PostgresStore._ensure_table(pg, cur, "timesheets")
            PostgresStore._ensure_table(pg, cur, "shift_covers")
    creates = [s for s in pg.sql() if s.startswith("CREATE TABLE")]
    assert len(creates) == 2


def _employee_row(i, venue_id):
    return {"id": f"e{i}", "venue_id": venue_id, "tanda_id": None, "name": f"E{i}",
            "employment_type": "casual", "award_level": "level_2",
            "hourly_base_rate": "31.50", "phone": None, "email": None, "skills": [],
            "availability": {}, "max_hours_per_week": 38, "consecutive_days": 6,
            "visa_status": None, "visa_expiry": None, "visa_work_limit_fortnight": None,
            "created_at": datetime(2026, 1, 1), "updated_at": datetime(2026, 1, 1)}


def test_pg_employee_listing_looks_up_each_venue_once():
    rows = [_employee_row(i, "wembley") for i in range(85)]
    rows += [_employee_row(100 + i, "apothecary") for i in range(28)]
    pg = _PG(rows=rows)
    assert len(PostgresStore.get_employees(pg, "wembley")) == 113
    assert pg.venue_lookups == 2
    pg = _PG(rows=rows)
    assert len(PostgresStore.list_employees(pg)) == 113
    assert pg.venue_lookups == 2


_HOT = {
    "timesheets": "work_date", "leave_requests": "venue_id", "shift_covers": "venue_id",
    "announcements": "venue_id", "sop_documents": "venue_id",
    "sop_acknowledgements": "venue_id", "feed_posts": "venue_id",
    "dish_sales": "sale_date", "ingredients": "venue_id", "stocktakes": "venue_id",
    "supplier_invoices": "venue_id", "payroll_batches": "venue_id",
    "employees": "venue_id", "rosters": "week_start", "shifts": "roster_id",
}


def test_pg_hot_lookups_are_indexed_best_effort():
    ddl = dict()
    for table, stmt in PostgresStore._INDEX_DDL:
        assert stmt.startswith("CREATE INDEX IF NOT EXISTS"), stmt
        ddl.setdefault(table, []).append(stmt)
    for table, col in _HOT.items():
        assert any(f"ON {table} (" in s and col in s for s in ddl.get(table, [])), table

    ok = _PG()
    PostgresStore._ensure_indexes(ok)
    sql = ok.sql()
    ts_table = sql.index(next(s for s in sql if s.startswith("CREATE TABLE IF NOT EXISTS timesheets")))
    ts_index = sql.index(next(s for s in sql if "idx_timesheets_venue_date" in s))
    assert ts_table < ts_index

    failing = _PG(fail_on="CREATE INDEX")
    PostgresStore._ensure_indexes(failing)          # never raises
    assert sum(s.startswith("CREATE INDEX") for s in failing.sql()) == len(PostgresStore._INDEX_DDL)


# ---------------------------------------------------------------------------
# cover claims and read receipts (MemoryStore parity, through the API)
# ---------------------------------------------------------------------------

def _linked_staff(c, vid, name):
    h, email = _user(c, vid)
    eid = f"{vid}-{name.lower()}"
    get_db().save_employee(_emp(vid, eid, name, email=email, availability={}))
    return h, eid


def test_second_cover_claimant_gets_409_even_from_a_stale_read(monkeypatch):
    from rosteriq.services.clock import venue_today
    c = TestClient(app)
    _mgr, vid = _manager_with_venue(c)
    a_h, a_id = _linked_staff(c, vid, "Amy")
    b_h, _b = _linked_staff(c, vid, "Bo")
    c_h, _c = _linked_staff(c, vid, "Cal")
    db = get_db()
    day = venue_today(vid, db) + timedelta(days=2)
    ws = day - timedelta(days=day.weekday())
    db.save_roster(Roster(id=f"{vid}-r", venue_id=vid, week_start=ws, week_end=ws + timedelta(days=6),
                          created_at=datetime(2026, 10, 1),
                          shifts=[Shift(id=f"{vid}-s", employee_id=a_id, date=day,
                                        start_time=time(17), end_time=time(23),
                                        status=ShiftStatus.scheduled, role="bar")]))
    r = c.post(f"/api/me/shifts/{vid}-s/cover", json={"reason": "sick"}, headers=a_h)
    assert r.status_code == 200, r.text
    cover_id = r.json()["cover_id"]
    open_snapshot = dict(db.get_shift_cover(cover_id))

    assert c.post(f"/api/me/cover/{cover_id}/claim", headers=b_h).status_code == 200
    # Cal's request read the cover before Bo's claim landed.
    monkeypatch.setattr(type(db), "get_shift_cover", lambda self, cid: dict(open_snapshot))
    late = c.post(f"/api/me/cover/{cover_id}/claim", headers=c_h)
    assert late.status_code == 409
    monkeypatch.undo()
    assert db.get_shift_cover(cover_id)["claimed_by"] == f"{vid}-bo"


def test_concurrent_read_receipts_are_all_kept(monkeypatch):
    c = TestClient(app)
    _mgr, vid = _manager_with_venue(c)
    s1_h, s1 = _linked_staff(c, vid, "Ivy")
    s2_h, s2 = _linked_staff(c, vid, "Max")
    db = get_db()
    db.save_announcement({"id": f"{vid}-a", "venue_id": vid, "title": "Grand final",
                          "body": "All hands Saturday", "pinned": True, "audience": [],
                          "read_by": [], "created_at": datetime(2026, 10, 1)})
    stale = dict(db.get_announcement(f"{vid}-a"))
    real_get = type(db).get_announcement
    monkeypatch.setattr(type(db), "get_announcement",
                        lambda self, aid: dict(stale, read_by=list(stale["read_by"])))
    assert c.post(f"/api/me/announcements/{vid}-a/read", headers=s1_h).status_code == 200
    assert c.post(f"/api/me/announcements/{vid}-a/read", headers=s2_h).status_code == 200
    assert c.post(f"/api/me/announcements/{vid}-a/read", headers=s2_h).status_code == 200
    monkeypatch.setattr(type(db), "get_announcement", real_get)
    assert db.get_announcement(f"{vid}-a")["read_by"] == [s1, s2]


# ---------------------------------------------------------------------------
# password reset
# ---------------------------------------------------------------------------

def test_password_reset_signs_out_existing_sessions():
    from rosteriq.services.auth import auth_service
    c = TestClient(app)
    email = f"reset{uuid.uuid4().hex[:6]}@x.com"
    c.post("/api/auth/register", json={"email": email, "password": PW, "name": "Jordan"})
    old = c.post("/api/auth/login", json={"email": email, "password": PW}).json()["refresh_token"]

    token = auth_service.create_password_reset_token(email)
    assert auth_service.reset_password(token, "N3wPassw0rd!x")

    assert c.post("/api/auth/refresh", json={"refresh_token": old}).status_code == 401
    fresh = c.post("/api/auth/login", json={"email": email, "password": "N3wPassw0rd!x"})
    assert fresh.status_code == 200
    assert c.post("/api/auth/refresh",
                  json={"refresh_token": fresh.json()["refresh_token"]}).status_code == 200
