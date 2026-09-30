"""
No-show backups and the availability resolver read employee.availability
through the shared rule (services/availability_rules.py).

They used to treat a day that isn't listed as unavailable, so someone who had
only blocked out Monday vanished from Tuesday's backups, alternatives and
coverage. The three cases that used to go wrong:

* {}                  -> available
* {"monday": []}      -> unavailable Monday, AVAILABLE Tuesday
* a range             -> the shift must fit inside it, to the minute
"""

from datetime import date, datetime, time
from decimal import Decimal

import pytest

from rosteriq.database import get_db
from rosteriq.models import (
    AwardLevel, Employee, EmploymentType, Roster, Shift, ShiftStatus, State, VenueConfig,
)
from rosteriq.services.availability_resolver import AvailabilityResolver
from rosteriq.services.noshow_predictor import NoShowPredictor

MON = date(2026, 10, 5)
TUE = date(2026, 10, 6)
SAT = date(2026, 10, 10)
VENUE = "ven-adopt-resolver"

NO_CONSTRAINTS = {}
NOT_MONDAY = {"monday": []}
SAT_DAYTIME = {"saturday": [{"start": "09:00", "end": "17:30"}]}


def _emp(emp_id, availability, **kw) -> Employee:
    d = dict(
        id=emp_id, name=emp_id, venue_id=VENUE, employment_type=EmploymentType.casual,
        award_level=AwardLevel.level_1, state=State.vic, hourly_base_rate=Decimal("30.00"),
        skills=["bar"], availability=availability,
        created_at=datetime(2026, 1, 1), updated_at=datetime(2026, 1, 1),
    )
    d.update(kw)
    return Employee(**d)


def _shift(shift_id, day, start, end, employee_id="primary") -> Shift:
    return Shift(id=shift_id, employee_id=employee_id, date=day, start_time=start,
                 end_time=end, break_minutes=0, status=ShiftStatus.scheduled, role="bar")


def _roster(*shifts) -> Roster:
    return Roster(id=f"ros-{shifts[0].id}", venue_id=VENUE, week_start=MON,
                  week_end=date(2026, 10, 11), shifts=list(shifts),
                  created_at=datetime(2026, 10, 1))


def _venue() -> VenueConfig:
    return VenueConfig(id=VENUE, name="Adopt", tanda_org_id="t", state=State.vic,
                       min_staff={"bar": 1}, max_labour_pct=30, created_at=datetime(2026, 1, 1))


# ============================================================================
# no-show predictor: backups
# ============================================================================

class TestNoShowBackups:
    def _avail(self, availability, day, start, end):
        return NoShowPredictor()._is_employee_available(_emp("x", availability), day, start, end)

    def test_no_constraints_is_available(self):
        assert self._avail(NO_CONSTRAINTS, TUE, time(18, 0), time(23, 0))

    def test_blocked_monday_is_available_tuesday(self):
        assert not self._avail(NOT_MONDAY, MON, time(10, 0), time(14, 0))
        assert self._avail(NOT_MONDAY, TUE, time(10, 0), time(14, 0))   # used to be False

    def test_range_fits_to_the_minute(self):
        assert self._avail(SAT_DAYTIME, SAT, time(9, 0), time(17, 30))
        assert self._avail(SAT_DAYTIME, SAT, time(9, 0), time(17, 15))  # hour-floored before
        assert not self._avail(SAT_DAYTIME, SAT, time(9, 0), time(17, 31))
        assert not self._avail(SAT_DAYTIME, SAT, time(18, 0), time(23, 0))

    def test_suggest_backups_uses_the_rule(self):
        db = get_db()
        for e in (_emp("primary", {}), _emp("free", NO_CONSTRAINTS),
                  _emp("not_mon", NOT_MONDAY), _emp("sat_only", SAT_DAYTIME)):
            db.save_employee(e)
        db.save_roster(_roster(_shift("s-mon", MON, time(10, 0), time(14, 0)),
                               _shift("s-tue", TUE, time(10, 0), time(14, 0)),
                               _shift("s-sat-fit", SAT, time(9, 0), time(17, 30)),
                               _shift("s-sat-late", SAT, time(18, 0), time(23, 0))))
        p = NoShowPredictor()
        ids = lambda sid: {b.employee_id for b in p.suggest_backups(sid)}
        # sat_only only constrained Saturday, so Monday/Tuesday are open to them
        assert ids("s-mon") == {"free", "sat_only"}
        assert ids("s-tue") == {"free", "not_mon", "sat_only"}   # both used to be dropped
        assert ids("s-sat-fit") == {"free", "not_mon", "sat_only"}
        assert ids("s-sat-late") == {"free", "not_mon"}


# ============================================================================
# availability resolver
# ============================================================================

class TestResolverIsAvailableAndScore:
    def _r(self):
        return AvailabilityResolver()

    def test_no_constraints(self):
        r, s = self._r(), _shift("s", TUE, time(18, 0), time(23, 0))
        assert r._is_available(_emp("x", NO_CONSTRAINTS), s)
        assert r._score_availability_fit(_emp("x", NO_CONSTRAINTS), s) == 80.0

    def test_blocked_monday_available_tuesday(self):
        r, e = self._r(), _emp("x", NOT_MONDAY)
        mon, tue = _shift("m", MON, time(10, 0), time(14, 0)), _shift("t", TUE, time(10, 0), time(14, 0))
        assert not r._is_available(e, mon)
        assert r._score_availability_fit(e, mon) == 0.0
        assert r._is_available(e, tue)                           # used to be False
        assert r._score_availability_fit(e, tue) == 80.0          # used to be 0

    def test_range_fits_or_not(self):
        r, e = self._r(), _emp("x", SAT_DAYTIME)
        fits = _shift("f", SAT, time(9, 0), time(17, 30))
        over = _shift("o", SAT, time(9, 0), time(17, 31))
        assert r._is_available(e, fits) and r._score_availability_fit(e, fits) == 100.0
        assert not r._is_available(e, over) and r._score_availability_fit(e, over) == 0.0

    def test_find_alternatives_includes_unlisted_day(self, monkeypatch):
        db = get_db()
        for e in (_emp("primary", {}), _emp("free", NO_CONSTRAINTS),
                  _emp("not_mon", NOT_MONDAY), _emp("sat_only", SAT_DAYTIME)):
            db.save_employee(e)
        r = self._r()
        # _score_fairness calls a store method that doesn't exist yet; it is not
        # what this test is about, so give it a constant.
        monkeypatch.setattr(r, "_score_fairness", lambda *a, **k: 50.0)
        primary = db.get_employee("primary")

        def alt_ids(shift):
            return {a.employee.id for a in r.find_alternatives(shift, primary, VENUE)}

        assert alt_ids(_shift("m", MON, time(10, 0), time(14, 0))) == {"free", "sat_only"}
        assert alt_ids(_shift("t", TUE, time(10, 0), time(14, 0))) == {"free", "not_mon", "sat_only"}
        assert alt_ids(_shift("f", SAT, time(9, 0), time(17, 30))) == {"free", "not_mon", "sat_only"}
        assert alt_ids(_shift("l", SAT, time(18, 0), time(23, 0))) == {"free", "not_mon"}


class TestResolverTimeAdjustment:
    def test_no_constraint_or_unavailable_means_nothing_to_adjust(self):
        r = AvailabilityResolver()
        assert r.suggest_time_adjustment(_shift("t", TUE, time(10, 0), time(16, 0)), _emp("x", NO_CONSTRAINTS)) == []
        assert r.suggest_time_adjustment(_shift("m", MON, time(10, 0), time(16, 0)), _emp("x", NOT_MONDAY)) == []
        # Tuesday is unconstrained for NOT_MONDAY: the shift fits as-is
        assert r.suggest_time_adjustment(_shift("t", TUE, time(10, 0), time(16, 0)), _emp("x", NOT_MONDAY)) == []

    def test_range_trims_the_shift_to_the_window(self):
        r = AvailabilityResolver()
        adj = r.suggest_time_adjustment(_shift("s", SAT, time(9, 0), time(20, 0)), _emp("x", SAT_DAYTIME))
        assert len(adj) == 1
        assert (adj[0].adjusted_start, adj[0].adjusted_end) == (time(9, 0), time(17, 30))
        # a window that fits the whole shift needs no adjustment
        adj = r.suggest_time_adjustment(_shift("s", SAT, time(10, 0), time(16, 0)), _emp("x", SAT_DAYTIME))
        assert adj and adj[0].availability_conflict == "none" and adj[0].score == 100


class TestResolverCoverage:
    @pytest.fixture
    def seeded(self):
        db = get_db()
        db.save_venue(_venue())
        for e in (_emp("scheduled", {}), _emp("free", NO_CONSTRAINTS),
                  _emp("not_mon", NOT_MONDAY), _emp("sat_only", SAT_DAYTIME),
                  _emp("sat_late", {"saturday": [{"start": "16:00", "end": "23:59"}]})):
            db.save_employee(e)
        db.save_roster(_roster(_shift("cov-mon", MON, time(10, 0), time(14, 0), "scheduled"),
                               _shift("cov-tue", TUE, time(10, 0), time(14, 0), "scheduled"),
                               _shift("cov-sat", SAT, time(10, 0), time(14, 0), "scheduled")))
        return AvailabilityResolver()

    def test_monday(self, seeded):
        staff = seeded.get_availability_coverage(VENUE, MON).available_staff
        # not_mon is out; everyone unconstrained on Monday is in (used to be missing)
        assert staff == {"free": [(0, 24)], "sat_only": [(0, 24)], "sat_late": [(0, 24)]}

    def test_tuesday_unlisted_day_is_all_day(self, seeded):
        staff = seeded.get_availability_coverage(VENUE, TUE).available_staff
        assert staff == {"free": [(0, 24)], "not_mon": [(0, 24)],
                         "sat_only": [(0, 24)], "sat_late": [(0, 24)]}

    def test_saturday_ranges(self, seeded):
        staff = seeded.get_availability_coverage(VENUE, SAT).available_staff
        assert staff["sat_only"] == [(9, 17)]
        assert staff["sat_late"] == [(16, 24)]              # "23:59" = end of day
        assert staff["free"] == [(0, 24)] and staff["not_mon"] == [(0, 24)]
        assert "scheduled" not in staff
