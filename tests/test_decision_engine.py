"""Tests for the decision engine module."""

import pytest
from datetime import date, time, datetime, timedelta
from decimal import Decimal

from rosteriq.models import (
    Employee, Shift, StaffAction, EmploymentType, AwardLevel,
    State, ShiftStatus, StaffingRecommendation,
)
from rosteriq.decision_engine import (
    get_weekly_hours, estimate_cut_savings, rank_for_cut,
    rank_for_call_in, make_decision,
)


# ============================================================================
# Helpers
# ============================================================================

def make_employee(**overrides) -> Employee:
    defaults = dict(
        id="emp-1", name="Alice", employment_type=EmploymentType.casual,
        award_level=AwardLevel.level_1, state=State.vic,
        hourly_base_rate=Decimal("28.50"), skills=["bar", "floor"],
        created_at=datetime(2025, 1, 1), updated_at=datetime(2025, 1, 1),
    )
    defaults.update(overrides)
    return Employee(**defaults)


def make_shift(**overrides) -> Shift:
    defaults = dict(
        id="shift-1", employee_id="emp-1", date=date(2026, 4, 7),
        start_time=time(9, 0), end_time=time(17, 0), break_minutes=30,
        status=ShiftStatus.scheduled, role="bar",
    )
    defaults.update(overrides)
    return Shift(**defaults)


# ============================================================================
# Weekly hours tests
# ============================================================================

class TestGetWeeklyHours:
    def test_single_shift(self):
        shift = make_shift()
        hours = get_weekly_hours("emp-1", [shift])
        assert hours == 7.5

    def test_multiple_shifts(self):
        shifts = [
            make_shift(id="s1", start_time=time(9, 0), end_time=time(17, 0), break_minutes=30),
            make_shift(id="s2", start_time=time(9, 0), end_time=time(13, 0), break_minutes=0),
        ]
        hours = get_weekly_hours("emp-1", shifts)
        assert hours == 11.5  # 7.5 + 4.0

    def test_excludes_cancelled(self):
        shifts = [
            make_shift(id="s1", status=ShiftStatus.scheduled),
            make_shift(id="s2", status=ShiftStatus.cancelled),
        ]
        hours = get_weekly_hours("emp-1", shifts)
        assert hours == 7.5  # Only the scheduled one

    def test_excludes_no_show(self):
        shifts = [
            make_shift(id="s1", status=ShiftStatus.scheduled),
            make_shift(id="s2", status=ShiftStatus.no_show),
        ]
        hours = get_weekly_hours("emp-1", shifts)
        assert hours == 7.5

    def test_filters_by_employee_id(self):
        shifts = [
            make_shift(id="s1", employee_id="emp-1"),
            make_shift(id="s2", employee_id="emp-2"),
        ]
        assert get_weekly_hours("emp-1", shifts) == 7.5
        assert get_weekly_hours("emp-2", shifts) == 7.5

    def test_empty_shifts(self):
        assert get_weekly_hours("emp-1", []) == 0.0

    def test_no_matching_employee(self):
        shifts = [make_shift(employee_id="emp-2")]
        assert get_weekly_hours("emp-1", shifts) == 0.0


# ============================================================================
# Estimate cut savings tests
# ============================================================================

class TestEstimateCutSavings:
    def test_positive_remaining_hours(self):
        emp = make_employee(hourly_base_rate=Decimal("30.00"))
        shift = make_shift(date=date(2026, 4, 7))  # Tuesday weekday
        savings = estimate_cut_savings(emp, shift, 4.0, State.vic)
        # Casual weekday: 4h * $30 * 1.25 = $150
        assert savings == Decimal("150.00")

    def test_zero_remaining_hours(self):
        emp = make_employee()
        shift = make_shift()
        savings = estimate_cut_savings(emp, shift, 0.0, State.vic)
        assert savings == Decimal("0.00")

    def test_negative_remaining_hours(self):
        emp = make_employee()
        shift = make_shift()
        savings = estimate_cut_savings(emp, shift, -1.0, State.vic)
        assert savings == Decimal("0.00")

    def test_sunday_higher_savings(self):
        emp = make_employee(hourly_base_rate=Decimal("30.00"))
        shift_weekday = make_shift(date=date(2026, 4, 7))  # Tuesday
        shift_sunday = make_shift(date=date(2026, 4, 5))  # Sunday

        savings_weekday = estimate_cut_savings(emp, shift_weekday, 4.0, State.vic)
        savings_sunday = estimate_cut_savings(emp, shift_sunday, 4.0, State.vic)

        assert savings_sunday > savings_weekday


# ============================================================================
# Rank for cut tests
# ============================================================================

class TestRankForCut:
    def test_respects_min_staff(self):
        """Should not recommend cuts when at or below min_staff."""
        emp = make_employee()
        shift = make_shift()
        recs = rank_for_cut([(emp, shift)], State.vic, min_staff=1)
        assert len(recs) == 0

    def test_returns_recommendations(self):
        emp1 = make_employee(id="emp-1", employment_type=EmploymentType.casual)
        emp2 = make_employee(id="emp-2", employment_type=EmploymentType.full_time)
        s1 = make_shift(id="s1", employee_id="emp-1")
        s2 = make_shift(id="s2", employee_id="emp-2")

        recs = rank_for_cut([(emp1, s1), (emp2, s2)], State.vic, min_staff=1)
        assert len(recs) == 1  # Can only cut 1 (2 - min_staff=1)
        assert all(r.action == StaffAction.cut for r in recs)

    def test_casuals_prioritised_for_cut(self):
        casual = make_employee(id="emp-c", employment_type=EmploymentType.casual)
        fulltime = make_employee(id="emp-f", employment_type=EmploymentType.full_time)
        s1 = make_shift(id="s1", employee_id="emp-c")
        s2 = make_shift(id="s2", employee_id="emp-f")

        recs = rank_for_cut(
            [(casual, s1), (fulltime, s2)],
            State.vic, min_staff=0,
        )
        # Casual should be first (higher priority to cut)
        assert recs[0].employee_id == "emp-c"

    def test_each_recommendation_has_savings(self):
        emp = make_employee(id="emp-1")
        shift = make_shift(id="s1")
        emp2 = make_employee(id="emp-2")
        shift2 = make_shift(id="s2", employee_id="emp-2")

        recs = rank_for_cut(
            [(emp, shift), (emp2, shift2)],
            State.vic, min_staff=0, current_hour=12,
        )
        for r in recs:
            assert r.estimated_savings is not None
            assert r.estimated_savings >= Decimal("0")

    def test_empty_active_shifts(self):
        recs = rank_for_cut([], State.vic)
        assert recs == []


# ============================================================================
# Rank for call-in tests
# ============================================================================

class TestRankForCallIn:
    def test_returns_recommendations(self):
        emp = make_employee(id="emp-1")
        recs = rank_for_call_in(
            [emp], [], date(2026, 4, 7), 14, State.vic,
        )
        assert len(recs) == 1
        assert recs[0].action == StaffAction.call_in

    def test_full_time_under_hours_preferred(self):
        ft = make_employee(
            id="emp-ft", employment_type=EmploymentType.full_time,
            max_hours_per_week=38.0,
        )
        casual = make_employee(
            id="emp-c", employment_type=EmploymentType.casual,
        )

        recs = rank_for_call_in(
            [ft, casual], [], date(2026, 4, 7), 14, State.vic,
        )
        # Full-time with hours remaining should rank higher
        assert recs[0].employee_id == "emp-ft"

    def test_available_employee_ranked_higher(self):
        available = make_employee(
            id="emp-a",
            availability={"tuesday": [{"start": "8", "end": "18"}]},
        )
        no_avail = make_employee(id="emp-b", availability={})

        recs = rank_for_call_in(
            [no_avail, available], [],
            date(2026, 4, 7), 14, State.vic,  # Tuesday
        )
        assert recs[0].employee_id == "emp-a"

    def test_skill_match_considered(self):
        skilled = make_employee(id="emp-s", skills=["bar", "cocktails"])
        unskilled = make_employee(id="emp-u", skills=[])

        recs = rank_for_call_in(
            [unskilled, skilled], [],
            date(2026, 4, 7), 14, State.vic,
            needed_roles=["bar"],
        )
        assert recs[0].employee_id == "emp-s"

    def test_empty_available_employees(self):
        recs = rank_for_call_in([], [], date(2026, 4, 7), 14, State.vic)
        assert recs == []


# ============================================================================
# Make decision tests
# ============================================================================

class TestMakeDecision:
    def test_no_action_within_threshold(self):
        recs = make_decision(
            variance=0.1,
            active_shifts=[],
            available_employees=[],
            current_shifts_this_week={},
            state=State.vic,
            target_date=date(2026, 4, 7),
            target_hour=14,
        )
        assert recs == []

    def test_zero_variance_no_action(self):
        recs = make_decision(
            variance=0.0,
            active_shifts=[],
            available_employees=[],
            current_shifts_this_week={},
            state=State.vic,
            target_date=date(2026, 4, 7),
            target_hour=14,
        )
        assert recs == []

    def test_negative_variance_triggers_cuts(self):
        emp = make_employee(id="emp-1")
        emp2 = make_employee(id="emp-2")
        s1 = make_shift(id="s1", employee_id="emp-1")
        s2 = make_shift(id="s2", employee_id="emp-2")

        recs = make_decision(
            variance=-0.3,
            active_shifts=[(emp, s1), (emp2, s2)],
            available_employees=[],
            current_shifts_this_week={},
            state=State.vic,
            target_date=date(2026, 4, 7),
            target_hour=14,
        )
        assert len(recs) > 0
        assert all(r.action == StaffAction.cut for r in recs)

    def test_positive_variance_triggers_call_ins(self):
        emp = make_employee(id="emp-avail")

        recs = make_decision(
            variance=0.3,
            active_shifts=[],
            available_employees=[emp],
            current_shifts_this_week={},
            state=State.vic,
            target_date=date(2026, 4, 7),
            target_hour=14,
        )
        assert len(recs) > 0
        assert all(r.action == StaffAction.call_in for r in recs)

    def test_custom_threshold(self):
        emp = make_employee(id="emp-1")

        recs = make_decision(
            variance=0.1,  # Below default 0.15 but above 0.05
            active_shifts=[],
            available_employees=[emp],
            current_shifts_this_week={},
            state=State.vic,
            target_date=date(2026, 4, 7),
            target_hour=14,
            threshold=0.05,
        )
        assert len(recs) > 0

    def test_exact_negative_threshold_no_action(self):
        recs = make_decision(
            variance=-0.15,
            active_shifts=[],
            available_employees=[],
            current_shifts_this_week={},
            state=State.vic,
            target_date=date(2026, 4, 7),
            target_hour=14,
        )
        assert recs == []
