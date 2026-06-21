"""
Public holidays must be correct for EVERY Australian state/territory — they drive
the public-holiday penalty rate, so a wrong date is wrong pay for any venue there.

Backed by the `holidays` library. These pin the state-specific quirks the old
hand-coded list got wrong (Easter Saturday only where it applies; King's Birthday
per state incl. WA's annually-proclaimed date; state-only days).

All dates are 2026 (verified against the holidays library).
"""

from datetime import date

import pytest

from rosteriq.models import State, DayType
from rosteriq.award_rules import get_public_holidays, get_day_type


KINGS_BIRTHDAY_2ND_MON_JUNE = date(2026, 6, 8)
EASTER_SATURDAY = date(2026, 4, 4)


@pytest.mark.parametrize("state,expected", [
    (State.nsw, True), (State.vic, True), (State.sa, True),
    (State.tas, True), (State.nt, True), (State.act, True),
    (State.wa, False),   # WA King's Birthday is in Sep/Oct, not June
    (State.qld, False),  # QLD King's Birthday is in October
])
def test_kings_birthday_june(state, expected):
    assert (KINGS_BIRTHDAY_2ND_MON_JUNE in get_public_holidays(state, 2026)) is expected


@pytest.mark.parametrize("state,expected", [
    (State.nsw, True), (State.vic, True), (State.qld, True), (State.sa, True),
    (State.nt, True), (State.act, True),
    (State.wa, False),   # Easter Saturday is NOT a public holiday in WA
    (State.tas, False),  # ...or TAS
])
def test_easter_saturday(state, expected):
    assert (EASTER_SATURDAY in get_public_holidays(State(state), 2026)) is expected


def test_wa_kings_birthday_is_the_proclaimed_date():
    """WA's King's Birthday is proclaimed annually — 28 Sep in 2026. A fixed rule
    can't compute this; the library can. This is the headline 'any state' win."""
    wa = get_public_holidays(State.wa, 2026)
    assert date(2026, 9, 28) in wa
    assert get_day_type(date(2026, 9, 28), State.wa) == DayType.public_holiday


def test_state_only_days_present():
    # WA Day (1st Mon June) in WA; Adelaide Cup (2nd Mon March) in SA.
    assert date(2026, 6, 1) in get_public_holidays(State.wa, 2026)
    assert date(2026, 3, 9) in get_public_holidays(State.sa, 2026)


@pytest.mark.parametrize("state", list(State))
def test_national_holidays_every_state(state):
    """The unambiguous national holidays (weekdays in 2026) appear in every state.
    (Anzac/Boxing Day fall on weekends in 2026 with state-specific substitution, so
    they're covered by the dedicated tests, not asserted bare here.)"""
    phs = get_public_holidays(state, 2026)
    for d in (date(2026, 1, 1),    # New Year's Day (Thu)
              date(2026, 1, 26),   # Australia Day (Mon)
              date(2026, 12, 25)):  # Christmas Day (Fri)
        assert d in phs, f"{d} missing for {state}"
    assert get_day_type(date(2026, 12, 25), state) == DayType.public_holiday
