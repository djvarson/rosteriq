"""
"Today" is the VENUE's day. The host runs UTC; between local midnight and
~08:00 Perth the UTC date is still yesterday, and the briefing, snapshot,
staff portal, time clock and AI system prompt used to say so.
"""

from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pytest

from rosteriq.database import MemoryStore
from rosteriq.models import VenueConfig, State
from rosteriq.services import clock


def _venue(db, vid, tz):
    db.save_venue(VenueConfig(id=vid, name=vid, tanda_org_id="", state=State.wa,
                              timezone=tz, min_staff={}, max_labour_pct=30.0,
                              pos_system="none", created_at=datetime.utcnow()))


def test_venue_today_uses_the_venue_timezone(monkeypatch):
    db = MemoryStore()
    _venue(db, "perth", "Australia/Perth")
    _venue(db, "sydney", "Australia/Sydney")
    clock._tz_cache.clear()
    # 22:30 UTC on the 15th == 06:30 on the 16th in Perth, 08:30 on the 16th in Sydney (AEST)
    fixed = datetime(2026, 8, 15, 22, 30, tzinfo=timezone.utc)

    class _DT(datetime):
        @classmethod
        def now(cls, tz=None):
            return fixed.astimezone(tz) if tz else fixed.replace(tzinfo=None)
    monkeypatch.setattr(clock, "datetime", _DT)

    assert clock.venue_today("perth", db) == date(2026, 8, 16)
    assert clock.venue_today("sydney", db) == date(2026, 8, 16)
    assert clock.venue_now("perth", db).hour == 6
    assert clock.venue_timezone("perth", db) == "Australia/Perth"


def test_unknown_venue_and_bad_tz_fall_back_safely(monkeypatch):
    db = MemoryStore()
    clock._tz_cache.clear()
    assert clock.venue_timezone("nope", db) == clock.DEFAULT_TZ
    assert isinstance(clock.venue_today("nope", db), date)
    _venue(db, "weird", "Mars/Olympus")
    assert clock.venue_timezone("weird", db) == clock.DEFAULT_TZ  # invalid tz -> default, never raises
    assert isinstance(clock.venue_today(None), date)


def test_briefing_and_ai_prompt_use_venue_local_today(monkeypatch):
    """The morning screen at 06:30 Perth must be about the 16th, not the 15th."""
    from rosteriq.database import get_db
    from rosteriq import ai_agent
    db = get_db()
    _venue(db, "clock-venue", "Australia/Perth")
    clock._tz_cache.clear()
    fixed = datetime(2026, 8, 15, 22, 30, tzinfo=timezone.utc)

    class _DT(datetime):
        @classmethod
        def now(cls, tz=None):
            return fixed.astimezone(tz) if tz else fixed.replace(tzinfo=None)
    monkeypatch.setattr(clock, "datetime", _DT)

    prompt = ai_agent._system_prompt_now("clock-venue")
    assert "2026-08-16" in prompt and "2026-08-15" not in prompt
    ctx = ai_agent.AgentContext("clock-venue")
    assert ctx.today == date(2026, 8, 16)
    from rosteriq.services.clock import venue_today
    assert venue_today("clock-venue", db) == date(2026, 8, 16)
    assert date.today() != date(2026, 8, 16) or True  # host date is irrelevant to the assertion
