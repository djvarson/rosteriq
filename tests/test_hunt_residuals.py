"""
The eight residuals from the fix-verification pass — findings whose CITED
mechanism was fixed earlier while a sibling path survived. Each test pins the
sibling path.
"""

import asyncio
from datetime import date, datetime, time as dtime, timedelta
from decimal import Decimal

import pytest

from rosteriq.database import get_db


# ---------------------------------------------------------------------------
# labour tracker: no more phantom store methods
# ---------------------------------------------------------------------------

def test_labour_tracker_uses_real_store_methods_only():
    """get_active_shifts / get_shifts_for_date / get_staff_count_by_hour /
    get_total_staff_hours never existed on any store; every call raised
    AttributeError into a swallow, and live labour ops reported an empty
    venue all day."""
    import inspect
    from rosteriq.services import labour_tracker as lt
    src = inspect.getsource(lt)
    for phantom in ("db.get_active_shifts", "db.get_shifts_for_date",
                    "db.get_staff_count_by_hour", "db.get_total_staff_hours"):
        assert phantom not in src, f"{phantom} still referenced"


def test_labour_tracker_sees_rostered_staff():
    from rosteriq.models import Roster, Shift, ShiftStatus, Employee, EmploymentType, AwardLevel, State
    from rosteriq.services.labour_tracker import LabourTracker

    db = get_db()
    vid = "lt-venue"
    emp = Employee(id="lt-e1", venue_id=vid, name="Pat", employment_type=EmploymentType.casual,
                   award_level=AwardLevel.level_2, state=State.wa,
                   hourly_base_rate=Decimal("30.00"), skills=["bar"],
                   created_at=datetime(2026, 7, 1), updated_at=datetime(2026, 7, 1))
    db.save_employee(emp)
    today = date.today()
    db.save_roster(Roster(
        id="lt-r1", venue_id=vid, week_start=today - timedelta(days=today.weekday()),
        week_end=today - timedelta(days=today.weekday()) + timedelta(days=6),
        shifts=[Shift(id="lt-s1", employee_id="lt-e1", date=today,
                      start_time=dtime(0, 0), end_time=dtime(23, 59), break_minutes=0,
                      status=ShiftStatus.scheduled, role="bar")],
        created_at=datetime(2026, 7, 1)))

    tracker = LabourTracker()          # binds get_db() itself
    on_now = tracker._get_staff_on_shift(vid, datetime.combine(today, dtime(12, 0)))
    assert [e.id for e in on_now] == ["lt-e1"]
    hours = tracker._get_staff_hours_for_date(vid, today)
    assert hours > Decimal("23")
    by_hour = tracker._get_staff_count_by_hour(vid, today)
    assert by_hour.get(12) == 1


# ---------------------------------------------------------------------------
# test-coverage endpoint never runs the suite implicitly
# ---------------------------------------------------------------------------

def test_coverage_report_does_not_run_the_suite(monkeypatch):
    import inspect
    from rosteriq.routes import test_report as tr
    src = inspect.getsource(tr)
    # exactly one CALL, inside the gated run-tests path (a comment mentions it)
    import re
    calls = [l for l in src.splitlines()
             if "runner.run_all(" in l and not l.strip().startswith("#")]
    assert len(calls) == 1, calls


# ---------------------------------------------------------------------------
# scheduler: no boot storm, venue wall-clock run times
# ---------------------------------------------------------------------------

def test_jobs_do_not_fire_immediately_on_boot():
    from rosteriq.services.task_scheduler import JobConfig, JobType
    job = JobConfig(job_type=JobType.WEEKLY_DIGEST, enabled=True, interval_hours=168)
    assert not job.should_run(), "an interval job fired at construction time"
    daily = JobConfig(job_type=JobType.DAILY_DIGEST, enabled=True, run_time=dtime(6, 0))
    assert not daily.should_run() or daily.next_run > datetime.now() - timedelta(seconds=5)
    assert daily.next_run > datetime.now(), "a run_time job fired at construction time"


def test_run_time_is_interpreted_on_the_venue_wall_clock():
    """"6 AM digest" means 6 AM where the venues are, not 6 AM UTC (2 pm
    Perth)."""
    from zoneinfo import ZoneInfo
    from rosteriq.services.task_scheduler import JobConfig, JobType
    from rosteriq.services.clock import DEFAULT_TZ
    job = JobConfig(job_type=JobType.DAILY_DIGEST, enabled=True, run_time=dtime(6, 0))
    local = job.next_run.astimezone() if job.next_run.tzinfo else job.next_run
    # convert the scheduled host-clock time back to the venue zone: must be 6 AM
    scheduled_local = (job.next_run.astimezone(ZoneInfo(DEFAULT_TZ))
                       if job.next_run.tzinfo
                       else datetime.combine(job.next_run.date(), job.next_run.time()).astimezone(ZoneInfo(DEFAULT_TZ)))
    assert scheduled_local.hour == 6, f"digest scheduled for {scheduled_local.hour}:00 venue time"


# ---------------------------------------------------------------------------
# auth: background email sends are supervised
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_background_send_failures_are_logged_not_swallowed(caplog):
    from rosteriq.routes.auth import _send_in_background, _bg_sends

    async def boom():
        raise RuntimeError("smtp fell over")

    _send_in_background(boom(), "password_reset")
    assert _bg_sends, "no strong reference held — task can be GC-collected mid-send"
    await asyncio.sleep(0.05)
    assert any("password_reset send failed" in r.message for r in caplog.records), \
        [r.message for r in caplog.records][-5:]
    assert not _bg_sends, "done-callback did not release the reference"


# ---------------------------------------------------------------------------
# SMTP timeout present
# ---------------------------------------------------------------------------

def test_smtp_connect_has_a_timeout():
    import inspect
    from rosteriq.services import notifications
    src = inspect.getsource(notifications)
    assert "smtplib.SMTP(self.smtp_host, self.smtp_port, timeout=" in src


# ---------------------------------------------------------------------------
# per-venue AI budget
# ---------------------------------------------------------------------------

def test_ai_budget_throttles_the_venue_not_the_platform(monkeypatch):
    from rosteriq.routes import ai_agent as ra
    from fastapi import HTTPException
    monkeypatch.setattr(ra, "AI_DAILY_LIMIT", 3)
    ra._ai_usage.clear()
    for _ in range(3):
        ra._check_ai_budget("venue-A")          # within budget
    with pytest.raises(HTTPException) as ei:
        ra._check_ai_budget("venue-A")          # 4th blows it
    assert ei.value.status_code == 429
    ra._check_ai_budget("venue-B")              # another venue unaffected
    ra._ai_usage.clear()


# ---------------------------------------------------------------------------
# xero credential codec is single and legacy-tolerant
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_xero_module_codec_round_trips_and_reads_legacy_raw():
    """The module-level save/get (used by the OAuth callback and bill push)
    and the store's methods (used by payroll) must speak the same codec —
    and a legacy plaintext row must still open."""
    from rosteriq.xero_integration import (
        XeroCredentials, save_xero_credentials, get_xero_credentials)

    class _MemDB:                     # module functions' in-memory branch
        pass

    db = _MemDB()
    creds = XeroCredentials(
        venue_id="xc-v", client_id="cid", client_secret="sekrit",
        tenant_id="t", access_token="tok-a", refresh_token="tok-r",
        token_expires=datetime(2026, 9, 1), created_at=datetime(2026, 8, 1),
        updated_at=datetime(2026, 8, 1))
    await save_xero_credentials(db, creds)
    got = await get_xero_credentials(db, "xc-v")
    assert got.client_secret == "sekrit" and got.access_token == "tok-a"

    # the sealed/opened pair used on Postgres round-trips, and passes raw through
    from rosteriq.services.secret_box import encrypt_tokens, decrypt_tokens
    sealed = encrypt_tokens({"access_token": "abc", "client_secret": "def"})
    assert decrypt_tokens(sealed) == {"access_token": "abc", "client_secret": "def"}
    assert decrypt_tokens({"access_token": "raw-legacy"}) == {"access_token": "raw-legacy"}
