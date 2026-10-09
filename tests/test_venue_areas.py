"""
services/venue_areas.py: the one reading of a venue's areas, per-weekday
minimums and trading hours.
"""

from datetime import date, datetime
from decimal import Decimal

import pytest

from rosteriq.models import Employee, VenueConfig
from rosteriq.services.venue_areas import (
    area_names, can_fill, min_staff_for, trading_hours_of_day, trading_window, venue_areas,
)

MON, WED, FRI = date(2026, 10, 12), date(2026, 10, 14), date(2026, 10, 16)


def _venue(**kw):
    base = dict(id="v", name="The Wembley Hotel", tanda_org_id="", state="wa", timezone="Australia/Perth",
                max_labour_pct=30, created_at=datetime(2026, 10, 1))
    base.update(kw)
    return VenueConfig(**base)


def _emp(skills):
    return Employee(id="e", venue_id="v", name="E", employment_type="casual", award_level="level_2", state="wa",
                    hourly_base_rate=Decimal("30"), skills=skills,
                    created_at=datetime(2026, 7, 1), updated_at=datetime(2026, 7, 1))


WEMBLEY_AREAS = [
    {"name": "Sports Bar", "min_by_day": {"mon": 1, "tue": 1, "wed": 2, "thu": 2, "fri": 3, "sat": 3, "sun": 2}},
    {"name": "Restaurant", "min_by_day": {d: 3 for d in ("mon", "tue", "wed", "thu", "fri", "sat", "sun")}},
    {"name": "Duty Manager", "min_by_day": {d: 1 for d in ("mon", "tue", "wed", "thu", "fri", "sat", "sun")}},
]


def test_per_weekday_minimums_and_supervisory_default():
    v = _venue(areas=WEMBLEY_AREAS)
    assert area_names(v) == ["sports bar", "restaurant", "duty manager"]
    assert min_staff_for(v, MON)["sports bar"] == 1 and min_staff_for(v, WED)["sports bar"] == 2
    dm = next(a for a in venue_areas(v) if a["name"] == "duty manager")
    assert dm["label"] == "Duty Manager"


def test_min_staff_fallback_when_no_areas():
    v = _venue(min_staff={"Kitchen": 2, "bar": 1})
    assert min_staff_for(v, FRI) == {"kitchen": 2, "bar": 1}


def test_who_can_fill_an_area():
    v = _venue(areas=WEMBLEY_AREAS + [{"name": "duty manager", "requires_skill": True}])
    assert can_fill(_emp(["Sports Bar"]), "sports bar", v)
    assert not can_fill(_emp(["floor"]), "sports bar", v)
    assert can_fill(_emp([]), "restaurant", v)                     # no skills recorded: general areas
    assert not can_fill(_emp([]), "duty manager", v)               # never a supervisory one
    assert can_fill(_emp(["duty manager"]), "Duty Manager", v)


def test_trading_hours_run_past_midnight_and_absent_means_unknown():
    v = _venue(trading_hours={"fri": {"open": "11:00", "close": "01:00"}, "mon": {"open": "10:00", "close": "24:00"}})
    start, end = trading_window(v, FRI)
    assert start == datetime(2026, 10, 16, 11) and end == datetime(2026, 10, 17, 1)
    assert trading_hours_of_day(v, FRI) == list(range(11, 24)) + [0]
    assert trading_hours_of_day(v, MON) == list(range(10, 24))
    assert trading_window(v, WED) is None


def test_validation_rejects_markup_and_bad_hours():
    with pytest.raises(ValueError):
        _venue(areas=[{"name": "<b>bar"}])
    with pytest.raises(ValueError):
        _venue(trading_hours={"mon": {"open": "25:00", "close": "02:00"}})
