"""Tests for Fair Work Australia award rules engine."""

import pytest
from datetime import date, time, datetime, timedelta
from decimal import Decimal

from rosteriq.models import (
    Employee, Shift, EmploymentType, AwardLevel, State,
    DayType, ShiftStatus,
)
from rosteriq.award_rules import (
    get_penalty_multiplier, get_day_type, calculate_shift_cost,
    validate_shift_compliance, get_minimum_break_minutes,
    get_minimum_engagement_hours, check_consecutive_days,
    get_public_holidays, calculate_overtime_hours,
    PENALTY_MULTIPLIERS,
)


# ============================================================================
# Helpers
# ============================================================================

def make_employee(**overrides) -> Employee:
    defaults = dict(
        id="emp-1", name="Alice", employment_type=EmploymentType.full_time,
        award_level=AwardLevel.level_1, state=State.vic,
        hourly_base_rate=Decimal("28.50"),
        created_at=datetime(2025, 1, 1), updated_at=datetime(2025, 1, 1),
    )
    defaults.update(overrides)
    return Employee(**defaults)


def make_shift(**overrides) -> Shift:
    defaults = dict(
        id="shift-1", employee_id="emp-1", date=date(2026, 4, 7),  # Tuesday
        start_time=time(9, 0), end_time=time(17, 0), break_minutes=30,
        status=ShiftStatus.scheduled, role="bar",
    )
    defaults.update(overrides)
    return Shift(**defaults)


# ============================================================================
# Penalty multiplier tests — the most critical compliance tests
# ============================================================================

class TestPenaltyMultipliers:
    """Test all penalty rate combinations from the Hospitality Award."""

    # Full-time / Part-time rates
    def test_ft_weekday(self):
        assert get_penalty_multiplier(EmploymentType.full_time, DayType.weekday) == Decimal("1.0")

    def test_ft_saturday(self):
        assert get_penalty_multiplier(EmploymentType.full_time, DayType.saturday) == Decimal("1.25")

    def test_ft_sunday(self):
        assert get_penalty_multiplier(EmploymentType.full_time, DayType.sunday) == Decimal("1.5")

    def test_ft_public_holiday(self):
        # MA000009 full-time public holiday penalty is 225% (not 250%).
        assert get_penalty_multiplier(EmploymentType.full_time, DayType.public_holiday) == Decimal("2.25")

    def test_pt_weekday(self):
        assert get_penalty_multiplier(EmploymentType.part_time, DayType.weekday) == Decimal("1.0")

    def test_pt_saturday(self):
        assert get_penalty_multiplier(EmploymentType.part_time, DayType.saturday) == Decimal("1.25")

    def test_pt_sunday(self):
        assert get_penalty_multiplier(EmploymentType.part_time, DayType.sunday) == Decimal("1.5")

    def test_pt_public_holiday(self):
        # MA000009 part-time public holiday penalty is 225% (not 250%).
        assert get_penalty_multiplier(EmploymentType.part_time, DayType.public_holiday) == Decimal("2.25")

    # Casual rates (include 25% loading)
    def test_casual_weekday(self):
        assert get_penalty_multiplier(EmploymentType.casual, DayType.weekday) == Decimal("1.25")

    def test_casual_saturday(self):
        assert get_penalty_multiplier(EmploymentType.casual, DayType.saturday) == Decimal("1.5")

    def test_casual_sunday(self):
        assert get_penalty_multiplier(EmploymentType.casual, DayType.sunday) == Decimal("1.75")

    def test_casual_public_holiday(self):
        assert get_penalty_multiplier(EmploymentType.casual, DayType.public_holiday) == Decimal("2.5")

    # Overtime rates (full-time only)
    def test_ft_overtime_first_2_hours(self):
        result = get_penalty_multiplier(EmploymentType.full_time, DayType.weekday, overtime_hours=1.5)
        assert result == Decimal("1.5")

    def test_ft_overtime_after_2_hours(self):
        result = get_penalty_multiplier(EmploymentType.full_time, DayType.weekday, overtime_hours=3.0)
        assert result == Decimal("2.0")

    def test_ft_sunday_overtime(self):
        result = get_penalty_multiplier(EmploymentType.full_time, DayType.sunday, overtime_hours=1.0)
        assert result == Decimal("2.0")

    def test_ft_public_holiday_overtime(self):
        result = get_penalty_multiplier(EmploymentType.full_time, DayType.public_holiday, overtime_hours=1.0)
        assert result == Decimal("2.5")

    def test_casual_overtime_ignored(self):
        """Casual employees don't get separate overtime rates."""
        result = get_penalty_multiplier(EmploymentType.casual, DayType.weekday, overtime_hours=5.0)
        assert result == Decimal("1.25")  # Just casual base


# ============================================================================
# Day type detection tests
# ============================================================================

class TestGetDayType:
    def test_weekday(self):
        assert get_day_type(date(2026, 4, 7), State.vic) == DayType.weekday  # Tuesday

    def test_saturday(self):
        # 2026-04-04 is Easter Saturday — a VIC public holiday — so use a
        # plain Saturday instead. 2026-04-11 is a Saturday with no holiday.
        assert get_day_type(date(2026, 4, 11), State.vic) == DayType.saturday

    def test_sunday(self):
        # 2026-04-12 is a normal Sunday (2026-04-05 is Easter Sunday, a VIC PH).
        assert get_day_type(date(2026, 4, 12), State.vic) == DayType.sunday

    def test_easter_sunday_is_public_holiday_in_vic(self):
        # The library correctly flags Easter Sunday as a public holiday in VIC.
        assert get_day_type(date(2026, 4, 5), State.vic) == DayType.public_holiday

    def test_christmas_day(self):
        assert get_day_type(date(2026, 12, 25), State.vic) == DayType.public_holiday

    def test_anzac_day(self):
        assert get_day_type(date(2026, 4, 25), State.vic) == DayType.public_holiday

    def test_australia_day(self):
        assert get_day_type(date(2026, 1, 26), State.nsw) == DayType.public_holiday

    def test_good_friday_2026(self):
        # Good Friday 2026 is April 3
        assert get_day_type(date(2026, 4, 3), State.vic) == DayType.public_holiday

    def test_easter_monday_2026(self):
        # Easter Monday 2026 is April 6
        assert get_day_type(date(2026, 4, 6), State.vic) == DayType.public_holiday


# ============================================================================
# Public holidays tests
# ============================================================================

class TestPublicHolidays:
    def test_national_holidays_present(self):
        holidays = get_public_holidays(State.vic, 2026)
        assert date(2026, 1, 1) in holidays    # New Year's
        assert date(2026, 1, 26) in holidays   # Australia Day
        assert date(2026, 4, 25) in holidays   # Anzac Day
        assert date(2026, 12, 25) in holidays  # Christmas
        assert date(2026, 12, 26) in holidays  # Boxing Day

    def test_easter_dates_2026(self):
        holidays = get_public_holidays(State.vic, 2026)
        # Easter 2026: Sun Apr 5
        assert date(2026, 4, 3) in holidays   # Good Friday
        assert date(2026, 4, 4) in holidays   # Easter Saturday
        assert date(2026, 4, 6) in holidays   # Easter Monday

    def test_vic_melbourne_cup(self):
        holidays = get_public_holidays(State.vic, 2026)
        # First Tuesday in November 2026 = Nov 3
        melbourne_cup_dates = [d for d in holidays if d.month == 11]
        assert len(melbourne_cup_dates) >= 1

    def test_qld_kings_birthday(self):
        holidays = get_public_holidays(State.qld, 2026)
        # Last Monday in October
        october_holidays = [d for d in holidays if d.month == 10]
        assert len(october_holidays) >= 1

    def test_nsw_bank_holiday_is_not_a_general_public_holiday(self):
        # The NSW "Bank Holiday" (1st Mon August) is bank-only — it is NOT a general
        # public holiday and does NOT attract award public-holiday penalty rates, so
        # it must not appear here (the old hand-coded list wrongly included it).
        holidays = get_public_holidays(State.nsw, 2026)
        assert not [d for d in holidays if d.month == 8]

    def test_holidays_are_sorted(self):
        holidays = get_public_holidays(State.vic, 2026)
        assert holidays == sorted(holidays)

    def test_no_duplicates(self):
        holidays = get_public_holidays(State.vic, 2026)
        assert len(holidays) == len(set(holidays))


# ============================================================================
# Break requirements tests
# ============================================================================

class TestBreakRequirements:
    def test_short_shift_no_break(self):
        assert get_minimum_break_minutes(4.0) == 0

    def test_5_hour_boundary(self):
        assert get_minimum_break_minutes(5.0) == 0  # exactly 5 is not >5
        assert get_minimum_break_minutes(5.5) == 30

    def test_7_hour_boundary(self):
        assert get_minimum_break_minutes(7.0) == 30  # exactly 7 is >5 but not >7
        assert get_minimum_break_minutes(7.5) == 50

    def test_10_hour_boundary(self):
        assert get_minimum_break_minutes(10.0) == 50  # exactly 10 is >7 but not >10
        assert get_minimum_break_minutes(10.5) == 70

    def test_long_shift(self):
        assert get_minimum_break_minutes(11.0) == 70


# ============================================================================
# Minimum engagement tests
# ============================================================================

class TestMinimumEngagement:
    def test_casual_minimum(self):
        assert get_minimum_engagement_hours(EmploymentType.casual) == 2.0

    def test_part_time_minimum(self):
        assert get_minimum_engagement_hours(EmploymentType.part_time) == 3.0

    def test_full_time_no_minimum(self):
        assert get_minimum_engagement_hours(EmploymentType.full_time) == 0.0


# ============================================================================
# Shift compliance validation tests
# ============================================================================

class TestShiftCompliance:
    def test_valid_shift_no_violations(self):
        emp = make_employee()
        # 9:00-17:00 is an 8h shift, which requires a 50min break under the
        # Hospitality Award (>7h). Provide a compliant break.
        shift = make_shift(break_minutes=50)
        violations = validate_shift_compliance(emp, shift)
        assert violations == []

    def test_max_shift_length_violation(self):
        emp = make_employee()
        shift = make_shift(start_time=time(6, 0), end_time=time(18, 0), break_minutes=0)
        # 12 hours > 11.5 max
        violations = validate_shift_compliance(emp, shift)
        assert any("exceeds maximum" in v for v in violations)

    def test_insufficient_break_violation(self):
        emp = make_employee()
        shift = make_shift(
            start_time=time(9, 0), end_time=time(17, 0), break_minutes=10
        )
        # 8h shift needs 50min break, only has 10
        violations = validate_shift_compliance(emp, shift)
        assert any("break" in v.lower() for v in violations)

    def test_casual_minimum_engagement_violation(self):
        emp = make_employee(employment_type=EmploymentType.casual)
        shift = make_shift(start_time=time(9, 0), end_time=time(10, 0), break_minutes=0)
        # 1h shift < 2h casual minimum
        violations = validate_shift_compliance(emp, shift)
        assert any("minimum" in v.lower() for v in violations)

    def test_part_time_minimum_engagement_violation(self):
        emp = make_employee(employment_type=EmploymentType.part_time)
        shift = make_shift(start_time=time(9, 0), end_time=time(11, 0), break_minutes=0)
        # 2h shift < 3h part-time minimum
        violations = validate_shift_compliance(emp, shift)
        assert any("minimum" in v.lower() for v in violations)


# ============================================================================
# Consecutive days tests
# ============================================================================

class TestConsecutiveDays:
    def test_no_violation(self):
        shifts = [
            make_shift(id=f"s{i}", date=date(2026, 4, 6) + timedelta(days=i))
            for i in range(5)
        ]
        assert check_consecutive_days(shifts) is False

    def test_six_days_ok(self):
        shifts = [
            make_shift(id=f"s{i}", date=date(2026, 4, 6) + timedelta(days=i))
            for i in range(6)
        ]
        assert check_consecutive_days(shifts) is False

    def test_seven_days_violation(self):
        shifts = [
            make_shift(id=f"s{i}", date=date(2026, 4, 6) + timedelta(days=i))
            for i in range(7)
        ]
        assert check_consecutive_days(shifts) is True

    def test_empty_shifts(self):
        assert check_consecutive_days([]) is False

    def test_gap_resets_count(self):
        dates = [date(2026, 4, 6), date(2026, 4, 7), date(2026, 4, 8),
                 # gap on 9th
                 date(2026, 4, 10), date(2026, 4, 11), date(2026, 4, 12)]
        shifts = [make_shift(id=f"s{i}", date=d) for i, d in enumerate(dates)]
        assert check_consecutive_days(shifts) is False

    def test_custom_max_consecutive(self):
        shifts = [
            make_shift(id=f"s{i}", date=date(2026, 4, 6) + timedelta(days=i))
            for i in range(5)
        ]
        assert check_consecutive_days(shifts, max_consecutive=4) is True


# ============================================================================
# Overtime calculation tests
# ============================================================================

class TestOvertimeHours:
    def test_no_overtime_under_38(self):
        emp = make_employee()
        shifts = [
            make_shift(id=f"s{i}", start_time=time(9, 0), end_time=time(17, 0),
                       break_minutes=30, date=date(2026, 4, 6) + timedelta(days=i))
            for i in range(4)
        ]
        # 4 shifts * 7.5h = 30h — no overtime
        result = calculate_overtime_hours(emp, shifts)
        assert result == 0.0

    def test_overtime_over_38(self):
        emp = make_employee(max_hours_per_week=38.0)
        shifts = [
            make_shift(id=f"s{i}", start_time=time(8, 0), end_time=time(18, 0),
                       break_minutes=30, date=date(2026, 4, 6) + timedelta(days=i))
            for i in range(5)
        ]
        # 5 shifts * 9.5h = 47.5h — 9.5h overtime
        result = calculate_overtime_hours(emp, shifts)
        assert result == pytest.approx(9.5)

    def test_casual_no_overtime(self):
        emp = make_employee(employment_type=EmploymentType.casual)
        shifts = [
            make_shift(id=f"s{i}", start_time=time(8, 0), end_time=time(18, 0),
                       break_minutes=30, date=date(2026, 4, 6) + timedelta(days=i))
            for i in range(6)
        ]
        # Casuals don't accrue overtime
        assert calculate_overtime_hours(emp, shifts) == 0.0


# ============================================================================
# Shift cost calculation tests
# ============================================================================

class TestCalculateShiftCost:
    def test_weekday_full_time_basic(self):
        emp = make_employee(hourly_base_rate=Decimal("30.00"))
        shift = make_shift(
            start_time=time(9, 0), end_time=time(17, 0),
            break_minutes=30, date=date(2026, 4, 7),  # Tuesday
        )
        cost = calculate_shift_cost(emp, shift, State.vic)
        # 7.5h * $30 * 1.0 = $225.00
        assert cost == Decimal("225.00")

    def test_saturday_full_time(self):
        emp = make_employee(hourly_base_rate=Decimal("30.00"))
        shift = make_shift(
            start_time=time(9, 0), end_time=time(17, 0),
            break_minutes=30, date=date(2026, 4, 11),  # Saturday (not Easter Sat)
        )
        cost = calculate_shift_cost(emp, shift, State.vic)
        # 7.5h * $30 * 1.25 = $281.25
        assert cost == Decimal("281.25")

    def test_sunday_casual(self):
        emp = make_employee(
            employment_type=EmploymentType.casual,
            hourly_base_rate=Decimal("28.00"),
        )
        shift = make_shift(
            start_time=time(10, 0), end_time=time(16, 0),
            break_minutes=30, date=date(2026, 4, 12),  # normal Sunday (Apr 5 is Easter Sun, a PH)
        )
        cost = calculate_shift_cost(emp, shift, State.vic)
        # 5.5h * $28 * 1.75 = $269.50
        assert cost == Decimal("269.50")

    def test_public_holiday_cost(self):
        emp = make_employee(hourly_base_rate=Decimal("30.00"))
        shift = make_shift(
            start_time=time(9, 0), end_time=time(17, 0),
            break_minutes=30, date=date(2026, 12, 25),  # Christmas
        )
        cost = calculate_shift_cost(emp, shift, State.vic)
        # 7.5h * $30 * 2.25 (FT public holiday = 225%) = $506.25
        assert cost == Decimal("506.25")
