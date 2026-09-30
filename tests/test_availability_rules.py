from datetime import date, time

from rosteriq.services.availability_rules import day_windows, is_available

TUE = date(2026, 10, 6)
MON = date(2026, 10, 5)


def test_no_constraints_means_available():
    assert is_available({}, TUE, "18:00", "23:00")
    assert is_available(None, TUE)


def test_unlisted_day_is_available_all_day():
    assert is_available({"monday": []}, TUE, "06:00", "23:59")


def test_empty_list_is_unavailable():
    assert not is_available({"monday": []}, MON)
    assert not is_available({"monday": []}, MON, "10:00", "12:00")


def test_ranges_are_checked_to_the_minute():
    a = {"saturday": [{"start": "11:00", "end": "17:30"}]}
    sat = date(2026, 10, 10)
    assert is_available(a, sat, "11:00", "17:30")
    assert not is_available(a, sat, "11:00", "17:31")
    assert not is_available(a, sat, "18:00", "23:00")
    assert is_available(a, sat)                              # partly available that day
    assert is_available(a, "Saturday", time(12, 0), time(16, 0))


def test_end_of_day_and_legacy_shapes():
    assert is_available({"friday": [{"start": "16:00", "end": "23:59"}]}, date(2026, 10, 9), "18:00", "00:00")
    assert day_windows({"friday": {"start": "06:00", "end": "23:00"}}, "friday") == [{"start": "06:00", "end": "23:00"}]
    assert is_available({"friday": True}, "friday") and not is_available({"friday": False}, "friday")


def test_hour_only_times_are_hours():
    a = {"monday": [{"start": "9", "end": "17"}]}
    assert is_available(a, MON, "09:00", "17:00")
    assert not is_available(a, MON, "08:00", "12:00")
