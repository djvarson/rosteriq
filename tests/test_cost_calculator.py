"""Tests for the cost calculator module."""

import pytest
from datetime import date, time, datetime, timedelta
from decimal import Decimal

from rosteriq.models import (
    Employee, Shift, Roster, State, EmploymentType,
    AwardLevel, ShiftStatus, CostBreakdown,
)
from rosteriq.cost_calculator import (
    calculate_shift_cost_breakdown, calculate_roster_cost,
    compare_rosters, estimate_daily_labour_cost,
    calculate_labour_percentage, find_cost_savings_opportunities,
    SUPER_RATE,
)


# ============================================================================
# Helpers
# ============================================================================

def make_employee(**overrides) -> Employee:
    defaults = dict(
        id="emp-1", name="Alice", employment_type=EmploymentType.full_time,
        award_level=AwardLevel.level_1, state=State.vic,
        hourly_base_rate=Decimal("30.00"),
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


def make_roster(**overrides) -> Roster:
    defaults = dict(
        id="roster-1", venue_id="venue-1",
        week_start=date(2026, 4, 6), week_end=date(2026, 4, 12),
        shifts=[], created_at=datetime(2026, 4, 1),
    )
    defaults.update(overrides)
    return Roster(**defaults)


# ============================================================================
# Shift cost breakdown tests
# ============================================================================

class TestShiftCostBreakdown:
    def test_weekday_full_time(self):
        emp = make_employee()
        shift = make_shift()
        breakdown = calculate_shift_cost_breakdown(emp, shift, State.vic)

        # 7.5h * $30 = $225 base
        assert breakdown.base_cost == Decimal("225.00")
        # Weekday FT = 1.0x, no penalty
        assert breakdown.penalty_cost == Decimal("0.00")
        # Not casual
        assert breakdown.casual_loading == Decimal("0.00")
        # Super: 11.5% of $225 = $25.88
        assert breakdown.super_contribution == Decimal("25.88")
        # Total: $225 + $25.88 = $250.88
        assert breakdown.total_cost == Decimal("250.88")

    def test_saturday_full_time(self):
        emp = make_employee()
        shift = make_shift(date=date(2026, 4, 11))  # Saturday (2026-04-04 is Easter Sat, a VIC public holiday)
        breakdown = calculate_shift_cost_breakdown(emp, shift, State.vic)

        assert breakdown.base_cost == Decimal("225.00")
        # Saturday FT = 1.25x, penalty = 0.25 * 225 = $56.25
        assert breakdown.penalty_cost == Decimal("56.25")
        assert breakdown.casual_loading == Decimal("0.00")

    def test_casual_weekday(self):
        emp = make_employee(employment_type=EmploymentType.casual)
        shift = make_shift()
        breakdown = calculate_shift_cost_breakdown(emp, shift, State.vic)

        assert breakdown.base_cost == Decimal("225.00")
        # Casual weekday = 1.25x. Loading = 25% of base = $56.25
        assert breakdown.casual_loading == Decimal("56.25")
        # No additional penalty beyond casual loading on weekday
        assert breakdown.penalty_cost == Decimal("0.00")

    def test_casual_sunday(self):
        emp = make_employee(employment_type=EmploymentType.casual)
        shift = make_shift(date=date(2026, 4, 12))  # Sunday (non-holiday)
        breakdown = calculate_shift_cost_breakdown(emp, shift, State.vic)

        assert breakdown.base_cost == Decimal("225.00")
        # Casual Sunday = 1.75x. Loading = 25% = $56.25
        assert breakdown.casual_loading == Decimal("56.25")
        # Penalty = (1.75 - 1.25) * $225 = $112.50
        assert breakdown.penalty_cost == Decimal("112.50")

    def test_super_rate_correct(self):
        assert SUPER_RATE == Decimal("0.115")

    def test_total_equals_components(self):
        emp = make_employee(employment_type=EmploymentType.casual)
        shift = make_shift(date=date(2026, 4, 12))  # Sunday (non-holiday)
        breakdown = calculate_shift_cost_breakdown(emp, shift, State.vic)

        expected_total = (
            breakdown.base_cost + breakdown.penalty_cost +
            breakdown.casual_loading + breakdown.super_contribution
        )
        assert breakdown.total_cost == expected_total

    def test_public_holiday_cost(self):
        emp = make_employee()
        shift = make_shift(date=date(2026, 12, 25))  # Christmas
        breakdown = calculate_shift_cost_breakdown(emp, shift, State.vic)

        # PH FT = 2.25x (225%), penalty = (2.25-1) * $225 = $281.25
        assert breakdown.penalty_cost == Decimal("281.25")


# ============================================================================
# Roster cost tests
# ============================================================================

class TestRosterCost:
    def test_empty_roster(self):
        roster = make_roster()
        employees = {"emp-1": make_employee()}
        cost = calculate_roster_cost(roster, employees, State.vic)
        assert cost == Decimal("0.00")

    def test_single_shift_roster(self):
        shift = make_shift()
        roster = make_roster(shifts=[shift])
        employees = {"emp-1": make_employee()}
        cost = calculate_roster_cost(roster, employees, State.vic)
        assert cost > Decimal("0")

    def test_missing_employee_raises(self):
        shift = make_shift(employee_id="unknown")
        roster = make_roster(shifts=[shift])
        with pytest.raises(ValueError, match="not found"):
            calculate_roster_cost(roster, {}, State.vic)

    def test_multi_shift_roster(self):
        shifts = [
            make_shift(id="s1", employee_id="emp-1"),
            make_shift(id="s2", employee_id="emp-2"),
        ]
        roster = make_roster(shifts=shifts)
        employees = {
            "emp-1": make_employee(id="emp-1"),
            "emp-2": make_employee(id="emp-2"),
        }
        cost = calculate_roster_cost(roster, employees, State.vic)
        single_cost = calculate_roster_cost(
            make_roster(shifts=[shifts[0]]), employees, State.vic
        )
        assert cost == single_cost * 2


# ============================================================================
# Roster comparison tests
# ============================================================================

class TestCompareRosters:
    def test_identical_rosters(self):
        shift = make_shift()
        original = make_roster(id="r1", shifts=[shift])
        optimised = make_roster(id="r2", shifts=[shift])
        employees = {"emp-1": make_employee()}

        comparison = compare_rosters(original, optimised, employees, State.vic)
        assert comparison.cost_savings == Decimal("0.00")
        assert comparison.hours_saved == 0.0

    def test_savings_when_fewer_shifts(self):
        shifts = [make_shift(id="s1"), make_shift(id="s2")]
        original = make_roster(id="r1", shifts=shifts)
        optimised = make_roster(id="r2", shifts=[shifts[0]])
        employees = {"emp-1": make_employee()}

        comparison = compare_rosters(original, optimised, employees, State.vic)
        assert comparison.cost_savings > Decimal("0")
        assert comparison.hours_saved > 0

    def test_alert_when_optimised_more_expensive(self):
        shift = make_shift(id="s1")
        original = make_roster(id="r1", shifts=[shift])
        optimised = make_roster(id="r2", shifts=[shift, make_shift(id="s2")])
        employees = {"emp-1": make_employee()}

        comparison = compare_rosters(original, optimised, employees, State.vic)
        assert any("more expensive" in a for a in comparison.alerts)


# ============================================================================
# Labour percentage tests
# ============================================================================

class TestLabourPercentage:
    def test_basic_percentage(self):
        result = calculate_labour_percentage(Decimal("3000"), Decimal("10000"))
        assert result == 30.0

    def test_zero_revenue_raises(self):
        with pytest.raises(ValueError):
            calculate_labour_percentage(Decimal("1000"), Decimal("0"))

    def test_negative_revenue_raises(self):
        with pytest.raises(ValueError):
            calculate_labour_percentage(Decimal("1000"), Decimal("-500"))

    def test_high_labour_percentage(self):
        result = calculate_labour_percentage(Decimal("8000"), Decimal("10000"))
        assert result == 80.0


# ============================================================================
# Daily cost estimate tests
# ============================================================================

class TestDailyLabourCost:
    def test_single_shift(self):
        shift = make_shift()
        employees = {"emp-1": make_employee()}
        cost = estimate_daily_labour_cost([shift], employees, State.vic)
        assert cost > Decimal("0")

    def test_empty_shifts(self):
        cost = estimate_daily_labour_cost([], {}, State.vic)
        assert cost == Decimal("0.00")


# ============================================================================
# Cost savings opportunities tests
# ============================================================================

class TestCostSavingsOpportunities:
    def test_long_shift_flagged(self):
        shift = make_shift(start_time=time(7, 0), end_time=time(18, 0), break_minutes=30)
        # 10.5h net > 8h threshold
        roster = make_roster(shifts=[shift])
        employees = {"emp-1": make_employee()}

        opportunities = find_cost_savings_opportunities(roster, employees, State.vic)
        assert any("splitting" in o.lower() for o in opportunities)

    def test_high_penalty_flagged(self):
        shift = make_shift(date=date(2026, 12, 25))  # Christmas = 2.5x
        roster = make_roster(shifts=[shift])
        employees = {"emp-1": make_employee()}

        opportunities = find_cost_savings_opportunities(roster, employees, State.vic)
        assert any("multiplier" in o.lower() for o in opportunities)
