"""
Wembley pilot, review wave A follow-ups: pay privacy, Postgres roster saves,
rate limits, payroll counting and the live roster.

* A staff login never reaches a colleague's pay: rosters, shift costing, the
  costing and penalty calculators, payroll batches, live labour and the event
  log are for managers of the venue.
* PostgresStore.save_roster is ONE statement on the shared autocommit
  connection (no BEGIN/COMMIT), and deletes only the shifts its caller
  dropped from the copy it held.
* The api_key rate tier needs a verified key; signed-in traffic from one IP
  also shares a generous ceiling; join-code guesses are capped per IP.
* Rostered payroll and the weekly digest count each week's live roster once.
* Publishing a RosterIQ roster retires the week's imported Deputy roster, so a
  recall never puts the old week back on staff phones.
"""

import inspect
import uuid
from contextlib import contextmanager
from datetime import date, datetime, time as dtime, timedelta
from decimal import Decimal

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from rosteriq.api import app
from rosteriq.database import PostgresStore, get_db
from rosteriq.middleware.rate_limiter import RateLimiterMiddleware
from rosteriq.models import Roster, Shift, ShiftStatus, normalise_skills
from rosteriq.services.clock import venue_today

PW = "Passw0rd!234"

# conftest neutralises the limiter's dispatch per test; keep the real one.
_REAL_LIMITER_DISPATCH = RateLimiterMiddleware.dispatch


def _login(c, email):
    c.post("/api/auth/register", json={"email": email, "password": PW, "name": "U"})
    tok = c.post("/api/auth/login", json={"email": email, "password": PW}).json()["access_token"]
    return {"Authorization": f"Bearer {tok}"}


def _monday(d):
    return d - timedelta(days=d.weekday())


def _shift(sid, emp, day, start=dtime(9), end=dtime(17), cost=None):
    return Shift(id=sid, employee_id=emp, date=day, start_time=start, end_time=end,
                 break_minutes=0, status=ShiftStatus.scheduled, role="bar", cost=cost)


def _seed(vid, rid, week_start, shifts, state=None, created_at=None):
    db = get_db()
    db.save_roster(Roster(id=rid, venue_id=vid, week_start=week_start,
                          week_end=week_start + timedelta(days=6), shifts=shifts,
                          total_cost=None, created_at=created_at or datetime.utcnow()))
    if state:
        db.update_roster_state(rid, state, "seed", "test")


def _world():
    """A non-owner manager, a WA venue with Alice ($30, linked staff login)
    and Bob ($41.25), and a venue-less stranger login."""
    c = TestClient(app)
    _login(c, f"f1_boot_{uuid.uuid4().hex[:6]}@x.com")
    email = f"f1m{uuid.uuid4().hex[:8]}@x.com"
    h = _login(c, email)
    vid = f"f1-{uuid.uuid4().hex[:6]}"
    assert c.post("/venues", json={"id": vid, "name": vid, "state": "wa", "max_labour_pct": 30,
                                   "tanda_org_id": "", "created_at": "2026-07-01T00:00:00"},
                  headers=h).status_code in (200, 201)
    mgr = _login(c, email)
    alice_email = f"f1a{uuid.uuid4().hex[:8]}@x.com"
    for eid, name, rate, mail in ((f"{vid}-alice", "Alice", "30.00", alice_email),
                                  (f"{vid}-bob", "Bob", "41.25", None)):
        body = {"id": eid, "venue_id": vid, "name": name, "employment_type": "casual",
                "award_level": "level_2", "state": "wa", "hourly_base_rate": rate,
                "skills": ["bar"], "created_at": "2026-07-01T00:00:00",
                "updated_at": "2026-07-01T00:00:00"}
        if mail:
            body["email"] = mail
        assert c.post("/employees", json=body, headers=mgr).status_code == 200, body
    staff = _login(c, alice_email)
    db = get_db()
    rec = db.get_user_by_email(alice_email)
    rec["venue_ids"], rec["role"] = [vid], "staff"
    db.save_user(rec)
    stranger = _login(c, f"f1x{uuid.uuid4().hex[:8]}@x.com")
    return c, mgr, staff, stranger, vid


def _published_week(vid):
    """A published roster for next week with one shift each for Alice and Bob."""
    monday = _monday(venue_today(vid, get_db())) + timedelta(days=7)
    rid = f"{vid}-pub"
    _seed(vid, rid, monday, [
        _shift(f"{vid}-s-alice", f"{vid}-alice", monday, cost=Decimal("300.00")),
        _shift(f"{vid}-s-bob", f"{vid}-bob", monday, cost=Decimal("462.00")),
    ], state="published")
    return rid, monday


# ---------------------------------------------------------------------------
# [0]/[20]/[27] Staff never reach a colleague's pay
# ---------------------------------------------------------------------------

def test_staff_cannot_read_rosters_or_their_costs():
    c, mgr, staff, _, vid = _world()
    rid, _ = _published_week(vid)
    for path in (f"/rosters?venue_id={vid}", "/rosters", f"/rosters?venue_id={vid}&current=true",
                 f"/rosters/{rid}", f"/rosters/{rid}/analyse", f"/rosters/{rid}/suggestions",
                 f"/rosters/{rid}/coverage", f"/costs/labour-percentage?roster_id={rid}&revenue=1000"):
        assert c.get(path, headers=staff).status_code == 403, path
        assert c.get(path, headers=mgr).status_code == 200, path
    listed = c.get(f"/rosters?venue_id={vid}", headers=mgr).json()["items"]
    assert [r["id"] for r in listed] == [rid]


def test_staff_cannot_cost_a_colleagues_shift():
    c, mgr, staff, _, vid = _world()
    path = (f"/costs/shift/{vid}-bob?shift_date=2026-10-12&start_hour=9&end_hour=17&state=wa")
    assert c.get(path, headers=staff).status_code == 403
    r = c.get(path, headers=mgr)
    assert r.status_code == 200 and Decimal(str(r.json()["base_cost"])) == Decimal("330.00")


def test_costing_router_is_venue_scoped_and_manager_only():
    c, mgr, staff, stranger, vid = _world()
    rid, monday = _published_week(vid)
    bob = f"{vid}-bob"
    annual = f"/api/v1/costing/employees/{bob}/annual-cost?avg_weekly_hours=1&weeks_per_year=1"
    projection = f"/api/v1/costing/employees/{bob}/cost-projection"
    assert c.get(annual, headers=staff).status_code == 403
    assert c.get(annual, headers=stranger).status_code == 404
    r = c.get(annual, headers=mgr)
    assert r.status_code == 200 and Decimal(r.json()["annual_base_pay"]) == Decimal("41.25")
    assert c.get(projection, headers=staff).status_code == 403
    assert c.get(projection, headers=stranger).status_code == 404

    compare = {"employee_ids": [bob], "shift_ids": [f"{vid}-s-bob"]}
    cheapest = {"eligible_employee_ids": [bob], "shift_id": f"{vid}-s-bob"}
    labour = {"shift_ids": [f"{vid}-s-bob"]}
    for path, body in (("/api/v1/costing/compare", compare),
                       ("/api/v1/costing/cheapest", cheapest),
                       (f"/api/v1/costing/rosters/{rid}/labour-cost", labour)):
        assert c.post(path, json=body, headers=staff).status_code == 403, path
        assert c.post(path, json=body, headers=stranger).status_code == 404, path
        assert c.post(path, json=body, headers=mgr).status_code == 200, path


def test_penalty_and_cut_calculators_are_manager_only():
    c, mgr, staff, _, vid = _world()
    bob = f"{vid}-bob"
    bodies = {
        "/api/v1/calculate-penalty": {"employee_id": bob, "date": "2026-10-12",
                                      "start_time": "09:00", "end_time": "17:00"},
        "/api/v1/compare-shift-times": {"employee_id": bob, "date": "2026-10-12",
                                        "options": [{"start_time": "09:00", "end_time": "17:00"}]},
        "/api/v1/compare-shift-days": {"employee_id": bob, "start_time": "09:00",
                                       "end_time": "17:00", "dates": ["2026-10-12"]},
        "/api/v1/cheapest-slot": {"employee_id": bob, "date": "2026-10-12", "duration_hours": 4},
    }
    for path, body in bodies.items():
        assert c.post(path, json=body, headers=staff).status_code == 403, path
        assert c.post(path, json=body, headers=mgr).status_code != 403, path
    # Cut recommendations price each person's remaining hours
    assert c.post("/decisions/recommend", json={"venue_id": vid, "variance": -0.5},
                  headers=staff).status_code == 403


def test_shift_split_and_optimiser_roster_reads_are_manager_only():
    c, mgr, staff, _, vid = _world()
    rid, _ = _published_week(vid)
    assert c.post(f"/api/v1/rosters/{rid}/auto-split", headers=staff).status_code == 403
    assert c.post(f"/api/v1/shifts/{vid}-s-bob/split", headers=staff,
                  json={"shift_id": f"{vid}-s-bob", "roster_id": rid}).status_code == 403
    assert c.get(f"/api/v1/rosters/{rid}", headers=staff).status_code == 403
    assert c.post(f"/api/v1/rosters/compare?original_roster_id={rid}&optimised_roster_id={rid}",
                  headers=staff).status_code == 403
    assert c.get(f"/api/v1/rosters/{rid}", headers=mgr).status_code == 200


def test_staff_cannot_read_payroll_batches_or_the_event_log():
    c, mgr, staff, _, vid = _world()
    _, monday = _published_week(vid)
    r = c.post("/api/payroll/prepare", json={
        "venue_id": vid, "period_start": monday.isoformat(),
        "period_end": (monday + timedelta(days=6)).isoformat(), "state": "wa"}, headers=mgr)
    assert r.status_code == 200, r.text
    bid = r.json()["batch_id"]
    end = (monday + timedelta(days=6)).isoformat()
    for path in (f"/api/payroll/batch/{bid}/csv?venue_id={vid}", f"/api/payroll/batch/{bid}",
                 f"/api/payroll/reconcile/{vid}?period_start={monday.isoformat()}&period_end={end}",
                 f"/api/payroll/history?venue_id={vid}",
                 f"/api/events?venue_id={vid}", f"/api/events/summary?venue_id={vid}",
                 f"/api/events/insights?venue_id={vid}"):
        assert c.get(path, headers=staff).status_code == 403, path
        assert c.get(path, headers=mgr).status_code == 200, path
    # The event log carries every rate change, so it is the same rule
    rows = c.get(f"/api/events?venue_id={vid}", headers=mgr).json()["events"]
    assert any(e["action"] == "employee.create" for e in rows)


def test_live_labour_reads_are_for_managers_of_the_venue():
    c, mgr, staff, stranger, vid = _world()
    for tail in ("labour-live", "labour-hourly", "labour-prediction", "labour-weekly",
                 "labour-thresholds", "labour-vs-forecast?forecast_revenue=1&forecast_labour_pct=30"):
        path = f"/api/v1/venues/{vid}/{tail}"
        assert c.get(path, headers=stranger).status_code == 403, path
        assert c.get(path, headers=staff).status_code == 403, path
        if "forecast" not in tail:        # that route's response model is broken for everyone
            assert c.get(path, headers=mgr).status_code != 403, path


def test_staff_own_views_still_work():
    c, mgr, staff, _, vid = _world()
    _published_week(vid)
    me = c.get("/api/me/shifts", headers=staff).json()
    assert me["linked"] and {s["id"] for s in me["shifts"]} <= {f"{vid}-s-alice"}
    own = c.get(f"/employees/{vid}-alice", headers=staff).json()
    assert Decimal(str(own["hourly_base_rate"])) == Decimal("30.00")


# ---------------------------------------------------------------------------
# [1]/[12]/[35]/[36] One statement per roster save on the shared connection
# ---------------------------------------------------------------------------

class _Cur:
    def __init__(self, log, rows=None):
        self.log, self.rows = log, rows or []

    def execute(self, sql, params=None):
        self.log.append((" ".join(sql.split()), params))

    def fetchone(self):
        return self.rows[0] if self.rows else None

    def fetchall(self):
        return list(self.rows)

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class _PG:
    """Just enough PostgresStore to run one method against a recording cursor."""
    _TABLE_DDL = PostgresStore._TABLE_DDL
    _INDEX_DDL = getattr(PostgresStore, "_INDEX_DDL", ())
    _ensure_table = PostgresStore._ensure_table
    _row_to_plain = staticmethod(PostgresStore._row_to_plain)

    def __init__(self, rows=None):
        self.log, self.rows = [], rows

    @contextmanager
    def _cursor(self):
        yield _Cur(self.log, self.rows)

    def sql(self):
        return [s for s, _ in self.log]


def _loaded(shift_ids, rid="r1"):
    """A roster as PostgresStore hands it out after a read."""
    row = {"id": rid, "venue_id": "wem", "week_start": date(2026, 10, 5),
           "week_end": date(2026, 10, 11), "total_cost": None, "created_at": datetime(2026, 10, 1)}
    shifts = [PostgresStore._row_to_shift(None, {
        "id": sid, "employee_id": "e1", "shift_date": date(2026, 10, 9), "start_time": dtime(17),
        "end_time": dtime(23), "break_minutes": 0, "status": "scheduled", "role": "bar",
        "cost": None, "penalty_multiplier": 1.0}) for sid in shift_ids]
    return PostgresStore._row_to_roster(None, row, shifts)


def _flat_params(params):
    if isinstance(params, dict):
        return list(params.values())
    return list(params or [])


def test_pg_save_roster_is_one_statement_with_no_explicit_transaction():
    pg = _PG()
    roster = _loaded(["s1", "s2"])
    roster.shifts = [s for s in roster.shifts if s.id != "s2"] + [_shift("s3", "e1", date(2026, 10, 10))]
    PostgresStore.save_roster(pg, roster)
    assert len(pg.log) == 1, pg.sql()
    sql, params = pg.log[0]
    for word in ("BEGIN", "COMMIT", "ROLLBACK", "START TRANSACTION"):
        assert word not in sql.upper().split(), sql
    assert "INSERT INTO rosters" in sql and "INSERT INTO shifts" in sql and "unnest(" in sql
    assert "DELETE FROM shifts" in sql and "NOT (id = ANY" not in sql
    flat = _flat_params(params)
    # Only the shift this caller dropped is deleted
    assert ["s2"] in flat
    assert ["s1", "s3"] in flat


def test_pg_stale_copy_never_deletes_a_shift_someone_else_added():
    pg = _PG()
    stale = _loaded(["s1"])                    # read before another request added s2
    stale.shifts.append(_shift("mine", "e1", date(2026, 10, 10)))
    PostgresStore.save_roster(pg, stale)
    assert len(pg.log) == 1
    sql, params = pg.log[0]
    assert "DELETE" not in sql                 # nothing was dropped from this copy
    assert not any("s2" in (p if isinstance(p, list) else [p]) for p in _flat_params(params))


def test_pg_new_roster_object_deletes_nothing_and_resaves_track_the_last_save():
    pg = _PG()
    fresh = Roster(id="r9", venue_id="wem", week_start=date(2026, 10, 5),
                   week_end=date(2026, 10, 11), created_at=datetime(2026, 10, 1),
                   shifts=[_shift("a", "e1", date(2026, 10, 6)), _shift("b", "e1", date(2026, 10, 7))])
    PostgresStore.save_roster(pg, fresh)
    assert "DELETE" not in pg.log[-1][0]
    fresh.shifts = [s for s in fresh.shifts if s.id != "b"]
    PostgresStore.save_roster(pg, fresh)
    sql, params = pg.log[-1]
    assert "DELETE FROM shifts" in sql and ["b"] in _flat_params(params)
    PostgresStore.save_roster(pg, fresh)       # saved already: nothing more to delete
    assert "DELETE" not in pg.log[-1][0]


def test_pg_duplicate_shift_ids_in_one_save_keep_the_last():
    pg = _PG()
    r = _loaded([])
    r.shifts = [_shift("x", "e1", date(2026, 10, 6), start=dtime(9)),
                _shift("x", "e1", date(2026, 10, 6), start=dtime(10))]
    PostgresStore.save_roster(pg, r)
    flat = _flat_params(pg.log[0][1])
    assert ["x"] in flat and [dtime(10)] in flat


def test_pg_feed_reaction_toggle_is_one_statement():
    row = {"id": "p1", "venue_id": "wem", "reactions": {"❤": ["u1"]}, "comments": [],
           "reaction_added": True}
    pg = _PG(rows=[row])
    post, state = PostgresStore.toggle_feed_reaction(pg, "p1", "❤", "u1")
    toggles = [(s, p) for s, p in pg.log if not s.startswith("CREATE")]
    assert len(toggles) == 1, pg.sql()
    sql = toggles[0][0]
    assert sql.startswith("UPDATE feed_posts") and "FOR UPDATE" not in sql
    assert not any(w in s.upper().split() for s in pg.sql() for w in ("BEGIN", "COMMIT", "ROLLBACK"))
    assert state == "added" and "reaction_added" not in post
    assert PostgresStore.toggle_feed_reaction(_PG(rows=[]), "ghost", "❤", "u1") == (None, None)


def test_postgres_store_never_opens_a_transaction_on_the_shared_connection():
    import rosteriq.database as database
    src = inspect.getsource(database.PostgresStore)
    for stmt in ('"BEGIN"', '"COMMIT"', '"ROLLBACK"'):
        assert stmt not in src, stmt
    assert not hasattr(database, "_explicit_transaction")


def test_memory_store_roster_save_still_replaces_shifts():
    c, mgr, staff, _, vid = _world()
    rid, monday = _published_week(vid)
    db = get_db()
    r = db.get_roster(rid)
    r.shifts = [s for s in r.shifts if s.employee_id != f"{vid}-bob"]
    db.save_roster(r)
    assert [s.id for s in db.get_roster(rid).shifts] == [f"{vid}-s-alice"]


# ---------------------------------------------------------------------------
# [2] Rate limiter: verified api keys only, a per-IP ceiling, join-code guesses
# ---------------------------------------------------------------------------

class _Limiter(RateLimiterMiddleware):
    dispatch = _REAL_LIMITER_DISPATCH
    TIER_LIMITS = {"unauthenticated": (3, 0.001), "authenticated": (50, 0.001),
                   "api_key": (10, 0.001)}
    IP_CEILING = (7, 0.001)


def _limited_app():
    a = FastAPI()
    a.add_middleware(_Limiter)

    @a.post("/api/auth/register")
    async def register():
        return {"ok": True}

    @a.get("/api/me/shifts")
    async def shifts():
        return {"ok": True}

    return a


def _token(user_id):
    from rosteriq.services.auth import AuthService
    tok, _ = AuthService.create_access_token(user_id, f"{user_id}@x.com", "staff")
    return {"Authorization": f"Bearer {tok}"}


def test_an_unverified_api_key_gets_the_anonymous_tier():
    c = TestClient(_limited_app(), client=("198.51.100.20", 1))
    codes = [c.post("/api/auth/register", headers={"X-API-Key": f"junk-{i}"}).status_code
             for i in range(5)]
    assert codes.count(429) == 2, codes
    r = c.post("/api/auth/register", headers={"X-API-Key": "riq_junk"})
    assert r.status_code == 429 and r.headers["X-RateLimit-Limit"] == "3"


def test_a_verified_api_key_gets_its_own_bucket_keyed_on_the_key():
    from rosteriq.services.auth import AuthService
    db = get_db()
    email = f"f1k{uuid.uuid4().hex[:8]}@x.com"
    TestClient(app).post("/api/auth/register", json={"email": email, "password": PW, "name": "K"})
    key = AuthService(db).generate_api_key(db.get_user_by_email(email)["id"])
    shared = _limited_app()
    a = TestClient(shared, client=("198.51.100.21", 1))
    b = TestClient(shared, client=("198.51.100.22", 1))
    ok = [a.get("/api/me/shifts", headers={"X-API-Key": key}).status_code for _ in range(6)]
    assert ok == [200] * 6
    r = b.get("/api/me/shifts", headers={"X-API-Key": key})
    assert r.status_code == 200 and r.headers["X-RateLimit-Limit"] == "10"
    assert int(r.headers["X-RateLimit-Remaining"]) == 3      # same bucket from another IP


def test_minting_accounts_on_one_ip_hits_the_ip_ceiling():
    shared = _limited_app()
    c = TestClient(shared, client=("198.51.100.30", 1))
    codes = []
    for user in ("u1", "u2", "u3"):
        codes += [c.get("/api/me/shifts", headers=_token(f"f1-{user}")).status_code for _ in range(3)]
    assert codes.count(200) == 7 and codes.count(429) == 2, codes
    other = TestClient(shared, client=("198.51.100.31", 1))
    assert other.get("/api/me/shifts", headers=_token("f1-u1")).status_code == 200


def test_production_ceiling_fits_a_pub_on_one_wifi():
    cap, refill = RateLimiterMiddleware.IP_CEILING
    assert cap >= 2000 and refill * 60 >= 2000
    assert "ip_ceiling" not in RateLimiterMiddleware.TIER_LIMITS


def test_join_code_guesses_are_capped_per_ip(monkeypatch):
    from rosteriq.services import linking
    monkeypatch.setattr(linking, "MAX_FAILED_CODES_PER_IP", 3)
    monkeypatch.setattr(linking, "_ip_failures", {})
    ip = f"10.66.{uuid.uuid4().int % 250}.7"
    c = TestClient(app, client=(ip, 1))
    first = _login(c, f"f1g{uuid.uuid4().hex[:8]}@x.com")
    for _ in range(3):
        assert c.post("/api/me/link", json={"code": "ZZZZ-ZZZZ"}, headers=first).status_code == 400
    # A fresh account on the same IP has its own per-user budget, but not the IP's
    second = _login(c, f"f1g{uuid.uuid4().hex[:8]}@x.com")
    assert c.post("/api/me/link", json={"code": "ZZZZ-ZZZZ"}, headers=second).status_code == 429
    elsewhere = TestClient(app, client=(f"10.67.{uuid.uuid4().int % 250}.7", 1))
    third = _login(elsewhere, f"f1g{uuid.uuid4().hex[:8]}@x.com")
    assert elsewhere.post("/api/me/link", json={"code": "ZZZZ-ZZZZ"},
                          headers=third).status_code == 400


# ---------------------------------------------------------------------------
# [3]/[21]/[24] Rostered payroll pays each shift once; the digest agrees
# ---------------------------------------------------------------------------

def _hours(batch_emp):
    return (Decimal(str(batch_emp["ordinary_hours"]))
            + sum(Decimal(str(p["hours"])) for p in batch_emp.get("penalty_entries", []))
            + Decimal(str(batch_emp.get("overtime_hours") or 0)))


def test_rostered_payroll_counts_the_live_roster_once():
    c, mgr, _, _, vid = _world()
    monday = date(2026, 10, 12)
    friday = monday + timedelta(days=4)
    alice = f"{vid}-alice"
    csv = f"Name,Date,Start,End,Role,Break\nAlice,{friday.strftime('%d/%m/%Y')},17:00,23:00,Bar,0\n"
    r = c.post("/api/setup/import-roster", json={"venue_id": vid, "content": csv}, headers=mgr)
    assert r.status_code == 200 and r.json()["shifts_imported"] == 1, r.text
    _seed(vid, f"{vid}-gen-a", monday, [_shift(f"{vid}-ga", alice, friday, dtime(17), dtime(23))],
          state="superseded")
    _seed(vid, f"{vid}-gen-b", monday, [_shift(f"{vid}-gb", alice, friday, dtime(17), dtime(23))],
          state="published")
    r = c.post("/api/payroll/prepare", json={
        "venue_id": vid, "period_start": monday.isoformat(),
        "period_end": (monday + timedelta(days=6)).isoformat(), "state": "wa"}, headers=mgr)
    assert r.status_code == 200, r.text
    batch = get_db().get_payroll_batch(r.json()["batch_id"])
    (emp,) = batch["employees"]
    assert _hours(emp) == Decimal("6")


def test_rostered_payroll_never_pays_an_unpublished_draft():
    c, mgr, _, _, vid = _world()
    monday = date(2026, 10, 12)
    _seed(vid, f"{vid}-draft", monday, [_shift(f"{vid}-d1", f"{vid}-alice", monday)])
    r = c.post("/api/payroll/prepare", json={
        "venue_id": vid, "period_start": monday.isoformat(),
        "period_end": (monday + timedelta(days=6)).isoformat(), "state": "wa"}, headers=mgr)
    assert r.status_code == 400, r.text


def test_weekly_digest_labour_counts_the_live_roster_once():
    from rosteriq.services.weekly_digest import WeeklyDigestGenerator
    _, _, _, _, vid = _world()
    monday = date(2026, 10, 12)
    alice = f"{vid}-alice"
    one = [_shift(f"{vid}-x", alice, monday + timedelta(days=4), dtime(17), dtime(23))]
    _seed(vid, f"{vid}-old", monday, [_shift(f"{vid}-o", alice, monday + timedelta(days=4),
                                             dtime(17), dtime(23))], state="superseded")
    _seed(vid, f"{vid}-live", monday, one, state="published")
    gen = WeeklyDigestGenerator()
    gen.db = get_db()
    section = gen._compile_labour_cost(vid, monday, monday + timedelta(days=6))
    staffing = gen._compile_staffing_metrics(vid, monday, monday + timedelta(days=6))
    assert staffing.total_hours == Decimal("6.00")
    single = gen._get_total_labour_cost(vid, monday, monday + timedelta(days=6))
    assert section.total_cost == single and single > 0


# ---------------------------------------------------------------------------
# [26] A batch saved without its super rate is labelled plainly
# ---------------------------------------------------------------------------

def test_legacy_batch_csv_does_not_claim_the_current_super_rate():
    c, mgr, _, _, vid = _world()
    db = get_db()
    bid = f"legacy-{uuid.uuid4().hex[:8]}"
    db.save_payroll_batch({
        "batch_id": bid, "venue_id": vid, "period_start": "2026-09-21", "period_end": "2026-09-27",
        "status": "draft", "created_at": "2026-09-28T00:00:00", "validation_errors": [],
        "employees": [{"name": "Amy", "ordinary_hours": "10.00", "ordinary_rate": "30.00",
                       "ordinary_gross": "300.00", "penalty_entries": [], "penalty_gross": "0.00",
                       "overtime_hours": "0.00", "overtime_amount": "0.00", "allowances": [],
                       "allowances_total": "0.00", "total_gross": "300.00",
                       "super_amount": "34.50", "total_with_super": "334.50"}],
        "total_gross": "300.00", "total_super": "34.50",
    })
    r = c.get(f"/api/payroll/batch/{bid}/csv?venue_id={vid}", headers=mgr)
    assert r.status_code == 200, r.text
    header = r.text.splitlines()[2].split(",")
    assert "Super" in header and not any(h.startswith("Super (") for h in header), header


# ---------------------------------------------------------------------------
# [22] Publishing retires the imported week; recall never brings it back
# ---------------------------------------------------------------------------

def test_recalling_a_published_roster_never_reinstates_the_deputy_week():
    c, mgr, staff, _, vid = _world()
    db = get_db()
    tomorrow = venue_today(vid, db) + timedelta(days=1)
    monday = _monday(tomorrow)
    alice = f"{vid}-alice"
    csv = f"Name,Date,Start,End,Role,Break\nAlice,{tomorrow.strftime('%d/%m/%Y')},11:00,17:00,Bar,0\n"
    assert c.post("/api/setup/import-roster", json={"venue_id": vid, "content": csv},
                  headers=mgr).status_code == 200
    imported = f"imported-{vid}-{monday.isoformat()}"

    def mine():
        body = c.get("/api/me/shifts", headers=staff).json()
        return [(s["date"], s["start"]) for s in body["shifts"]]

    assert mine() == [(tomorrow.isoformat(), "11:00")]
    _seed(vid, f"{vid}-B", monday, [_shift(f"{vid}-b1", alice, tomorrow, dtime(18), dtime(23))])
    r = c.post(f"/api/v1/rosters/{vid}-B/publish", json={"skip_approval": True}, headers=mgr)
    assert r.status_code == 200 and r.json()["success"], r.text
    assert db.get_roster_state(imported) == "superseded"
    assert mine() == [(tomorrow.isoformat(), "18:00")]

    r = c.post(f"/api/v1/rosters/{vid}-B/recall", json={"reason": "fix"}, headers=mgr)
    assert r.status_code == 200, r.text
    assert db.get_roster_state(imported) == "superseded"
    assert mine() == []


def test_generating_or_importing_still_leaves_the_imported_week_live():
    from rosteriq.services.current_roster import live_shifts
    c, mgr, staff, _, vid = _world()
    db = get_db()
    tomorrow = venue_today(vid, db) + timedelta(days=1)
    csv = f"Name,Date,Start,End,Role,Break\nAlice,{tomorrow.strftime('%d/%m/%Y')},11:00,17:00,Bar,0\n"
    c.post("/api/setup/import-roster", json={"venue_id": vid, "content": csv}, headers=mgr)
    _seed(vid, f"{vid}-draft", _monday(tomorrow),
          [_shift(f"{vid}-dr", f"{vid}-alice", tomorrow, dtime(18), dtime(23))])
    shifts = live_shifts(db, vid, tomorrow, tomorrow, staff=True)
    assert [s.start_time for s in shifts] == [dtime(11)]


# ---------------------------------------------------------------------------
# [23] A failed state read never supersedes a live roster
# ---------------------------------------------------------------------------

def test_supersede_skips_a_roster_whose_state_cannot_be_read(monkeypatch):
    from rosteriq.services.current_roster import supersede_week_drafts
    _, _, _, _, vid = _world()
    db = get_db()
    monday = date(2026, 10, 12)
    _seed(vid, f"{vid}-live", monday, [_shift(f"{vid}-l", f"{vid}-alice", monday)], state="published")
    _seed(vid, f"{vid}-old", monday, [_shift(f"{vid}-o", f"{vid}-alice", monday)])
    real = type(db).get_roster_state

    def flaky(self, rid):
        if rid == f"{vid}-live":
            raise RuntimeError("server closed the connection unexpectedly")
        return real(self, rid)

    monkeypatch.setattr(type(db), "get_roster_state", flaky)
    done = supersede_week_drafts(db, vid, monday, f"{vid}-new", reason="regenerated")
    monkeypatch.setattr(type(db), "get_roster_state", real)
    assert done == [f"{vid}-old"]
    assert db.get_roster_state(f"{vid}-live") == "published"


# ---------------------------------------------------------------------------
# [19] '/' belongs to the area name
# ---------------------------------------------------------------------------

def test_skills_keep_slashes_and_split_on_list_separators():
    assert normalise_skills("Bar/Bottle Shop") == ["bar/bottle shop"]
    assert normalise_skills("Front Bar, Sports Bar; Bottle Shop | Functions\nKitchen") == [
        "front bar", "sports bar", "bottle shop", "functions", "kitchen"]
    assert normalise_skills(["Bar/Bottle Shop", "bar/bottle shop"]) == ["bar/bottle shop"]


def test_announcement_reaches_a_section_named_with_a_slash():
    c, mgr, _, _, vid = _world()
    content = f"Name,Email,Role,Rate\nCass Lee,cass{uuid.uuid4().hex[:6]}@x.com,Bar/Bottle Shop,32\n"
    r = c.post("/api/setup/import-staff", json={"venue_id": vid, "content": content}, headers=mgr)
    assert r.status_code == 200, r.text
    cass = [e for e in get_db().get_employees(vid) if e.name == "Cass Lee"][0]
    assert "bar/bottle shop" in cass.skills
    r = c.post("/api/announcements", json={"venue_id": vid, "title": "Stocktake",
                                           "body": "Bottle shop stocktake Monday",
                                           "audience": ["bar/bottle shop"]}, headers=mgr)
    assert r.status_code == 200, r.text
