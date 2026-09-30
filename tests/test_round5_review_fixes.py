"""
Fixes from the round-5 adversarial review.

* The generator checked availability against the demand period, then rostered
  a template-adjusted shift (stretched to minimum engagement), so a part-timer
  free 17:00-19:00 was rostered 17:00-20:00.
* Demo sessions could write labour thresholds and onboarding templates, which
  live in per-worker memory a demo reset only clears on one worker (and
  onboarding reminders message staff).
* A demo still in use across the venue's midnight kept yesterday's roster:
  the seed only ran after a reset. Now a day-behind demo gets the day rolled
  forward without re-asserting the showcase over the visitor's work.
* Both workers seeded the demo at boot without the lock (duplicate
  checklists). Boot now seeds under the same lock, only when needed.
"""

from datetime import date, datetime, time, timedelta
from decimal import Decimal

from fastapi.testclient import TestClient

from rosteriq.api import app
from rosteriq.database import MemoryStore, get_db
from rosteriq.models import DemandForecast, Employee, State
from rosteriq.roster_optimiser import generate_daily_roster
from rosteriq.services import demo_reset
from rosteriq.services.demo import DEMO_VENUE_ID as D


def test_generator_checks_the_shift_it_will_actually_roster():
    mon = date(2026, 10, 12)
    pt = Employee(id="pt", venue_id="v", name="Part Timer", employment_type="part_time", award_level="level_2",
                  state="wa", hourly_base_rate=Decimal("30"), skills=["floor"],
                  availability={"monday": [{"start": "17:00", "end": "19:00"}]},
                  created_at=datetime(2026, 7, 1), updated_at=datetime(2026, 7, 1))
    fcs = [DemandForecast(id="f17", venue_id="v", date=mon, hour=17, predicted_covers=10.0,
                          confidence=0.8, model_version="t")]
    shifts = generate_daily_roster(mon, fcs, [pt], State.wa)
    # a 3-hour part-time minimum can't fit a 2-hour window: not rostered at all
    assert not [s for s in shifts if s.employee_id == "pt" and s.end_time > time(19, 0)], shifts


def test_demo_cant_write_per_worker_state_or_send_onboarding_reminders():
    c = TestClient(app)
    c.post("/api/auth/register", json={"email": "r5_boot@x.com", "password": "Passw0rd!234", "name": "B"})
    h = {"Authorization": "Bearer " + c.post("/api/auth/demo").json()["access_token"]}
    for method, path, body in [
        ("put", f"/api/v1/venues/{D}/labour-thresholds", {"green_max": 11, "amber_max": 12, "red_min": 13}),
        ("put", f"/api/v1/venues/{D}/onboarding-template", {"items": []}),
        ("post", "/api/v1/employees/demo-staff-001/onboarding", {}),
        ("post", "/api/v1/employees/demo-staff-001/onboarding/remind", {}),
    ]:
        r = getattr(c, method)(path, json=body, headers=h)
        assert r.status_code == 403, (path, r.status_code, r.text[:120])


def test_a_demo_in_use_across_midnight_rolls_the_day_forward(monkeypatch):
    from rosteriq.services import clock
    db = get_db()
    for k in ("demo:last_reset", "demo:last_session", "demo:activity"):
        db.save_to_key(k, {})
    t0 = datetime(2026, 9, 30, 15, 20)
    today = clock.venue_today(D, db)
    assert demo_reset.reset_and_seed(db, "203.0.113.7", now=t0)["status"] == "reset"
    post = db.get_feed_post("demo-feed-002")
    post["comments"] = [{"id": "c-pitch", "body": "pitch comment"}]
    db.save_feed_post(post)

    tomorrow = today + timedelta(days=1)
    monkeypatch.setattr(clock, "venue_today", lambda venue_id, db=None: tomorrow)
    out = demo_reset.reset_and_seed(db, "203.0.113.7", now=t0 + timedelta(minutes=50))
    assert out["status"] == "refreshed", out
    rosters = [r for r in db._rosters.values() if r.venue_id == D]
    assert len([s for r in rosters for s in r.shifts if s.date == tomorrow]) == 6
    assert db.get_feed_post("demo-feed-002")["comments"], "the refresh re-asserted the showcase"
    assert db.get_forecasts(D, tomorrow, tomorrow)


def test_boot_seeds_under_the_lock_only_when_needed():
    db = MemoryStore()
    assert demo_reset.seed_on_boot(db) == "seeded"
    assert demo_reset.seed_on_boot(db) == "kept"
    names = sorted(t["name"] for t in db._checklist_templates.values() if t.get("venue_id") == D)
    assert names == ["Closing", "Food Safety (daily)", "Opening"]
    demo_reset._lock.acquire()
    try:
        assert demo_reset.seed_on_boot(db, wait_seconds=0) == "busy"
    finally:
        demo_reset._lock.release()
