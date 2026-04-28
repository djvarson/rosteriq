"""Tests for RosterIQ data contracts (models and enums)."""

import pytest
from datetime import date, time, datetime, timedelta
from decimal import Decimal

from rosteriq.models import (
    # Enums
    EmploymentType, ShiftStatus, DayType, AlertType, SignalType,
    StaffAction, AwardLevel, State,
    # Models
    Employee, Shift, Roster, DemandForecast, VarianceSignal,
    StaffingRecommendation, CostBreakdown, RosterComparison,
    VenueConfig, TandaCredentials, APIError,
)


# ============================================================================
# Helper factories
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
        id="shift-1", employee_id="emp-1", date=date(2026, 4, 6),
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
# Enum tests
# ============================================================================

class TestEnums:
    def test_employment_types(self):
        assert EmploymentType.full_time.value == "full_time"
        assert EmploymentType.part_time.value == "part_time"
        assert EmploymentType.casual.value == "casual"

    def test_shift_statuses(self):
        statuses = [s.value for s in ShiftStatus]
        assert "scheduled" in statuses
        assert "cancelled" in statuses
        assert "no_show" in statuses
        assert len(statuses) == 6

    def test_day_types(self):
        assert DayType.public_holiday.value == "public_holiday"
        assert len(list(DayType)) == 4

    def test_signal_types(self):
        assert len(list(SignalType)) == 5
        assert SignalType.pos_trends.value == "pos_trends"

    def test_staff_actions(self):
        assert StaffAction.cut.value == "cut"
        assert StaffAction.call_in.value == "call_in"

    def test_award_levels(self):
        assert len(list(AwardLevel)) == 6

    def test_states(self):
        assert len(list(State)) == 8
        assert State.vic.value == "vic"

    def test_alert_types(self):
        assert AlertType.overstaffed.value == "overstaffed"
        assert AlertType.compliance_warning.value == "compliance_warning"


# ============================================================================
# Employee model tests
# ============================================================================

class TestEmployee:
    def test_create_valid(self):
        emp = make_employee()
        assert emp.id == "emp-1"
        assert emp.hourly_base_rate == Decimal("28.50")
        assert emp.max_hours_per_week == 38.0

    def test_hourly_rate_must_be_positive(self):
        with pytest.raises(ValueError, match="greater than 0"):
            make_employee(hourly_base_rate=Decimal("0"))

    def test_hourly_rate_negative_rejected(self):
        with pytest.raises(ValueError):
            make_employee(hourly_base_rate=Decimal("-5"))

    def test_max_hours_validation(self):
        with pytest.raises(ValueError, match="between 1 and 60"):
            make_employee(max_hours_per_week=0.5)

    def test_max_hours_upper_bound(self):
        with pytest.raises(ValueError):
            make_employee(max_hours_per_week=61)

    def test_defaults(self):
        emp = make_employee()
        assert emp.tanda_id is None
        assert emp.phone is None
        assert emp.consecutive_days_limit == 6

    def test_skills_list(self):
        emp = make_employee(skills=["bar", "kitchen", "management"])
        assert len(emp.skills) == 3

    def test_availability_dict(self):
        avail = {"monday": [{"start": "09:00", "end": "17:00"}]}
        emp = make_employee(availability=avail)
        assert "monday" in emp.availability


# ============================================================================
# Shift model tests
# ============================================================================

class TestShift:
    def test_create_valid(self):
        shift = make_shift()
        assert shift.id == "shift-1"
        assert shift.break_minutes == 30

    def test_duration_hours(self):
        shift = make_shift(start_time=time(9, 0), end_time=time(17, 0))
        assert shift.duration_hours == 8.0

    def test_net_hours(self):
        shift = make_shift(start_time=time(9, 0), end_time=time(17, 0), break_minutes=30)
        assert shift.net_hours == 7.5

    def test_overnight_shift_duration(self):
        shift = make_shift(start_time=time(22, 0), end_time=time(6, 0))
        assert shift.duration_hours == 8.0

    def test_break_minutes_non_negative(self):
        with pytest.raises(ValueError, match="break_minutes must be >= 0"):
            make_shift(break_minutes=-10)

    def test_zero_break(self):
        shift = make_shift(break_minutes=0)
        assert shift.net_hours == shift.duration_hours

    def test_penalty_multiplier_default(self):
        shift = make_shift()
        assert shift.penalty_multiplier == 1.0

    def test_short_shift(self):
        shift = make_shift(start_time=time(10, 0), end_time=time(12, 0), break_minutes=0)
        assert shift.duration_hours == 2.0
        assert shift.net_hours == 2.0


# ============================================================================
# Roster model tests
# ============================================================================

class TestRoster:
    def test_create_valid(self):
        roster = make_roster()
        assert roster.shift_count == 0
        assert roster.total_hours == 0.0

    def test_week_end_validation(self):
        with pytest.raises(ValueError, match="exactly 6 days"):
            make_roster(week_start=date(2026, 4, 6), week_end=date(2026, 4, 15))

    def test_total_hours_property(self):
        shifts = [
            make_shift(id="s1", start_time=time(9, 0), end_time=time(17, 0), break_minutes=30),
            make_shift(id="s2", start_time=time(10, 0), end_time=time(14, 0), break_minutes=0),
        ]
        roster = make_roster(shifts=shifts)
        assert roster.total_hours == 11.5  # 7.5 + 4.0

    def test_employees_used(self):
        shifts = [
            make_shift(id="s1", employee_id="emp-1"),
            make_shift(id="s2", employee_id="emp-2"),
            make_shift(id="s3", employee_id="emp-1"),
        ]
        roster = make_roster(shifts=shifts)
        assert roster.employees_used == {"emp-1", "emp-2"}

    def test_shift_count(self):
        shifts = [make_shift(id=f"s{i}") for i in range(5)]
        roster = make_roster(shifts=shifts)
        assert roster.shift_count == 5


# ============================================================================
# DemandForecast tests
# ============================================================================

class TestDemandForecast:
    def test_create_valid(self):
        fc = DemandForecast(
            id="fc-1", venue_id="v-1", date=date(2026, 4, 6),
            hour=12, predicted_covers=75.5, confidence=0.85,
            signals_used=[SignalType.historical], model_version="v1",
        )
        assert fc.predicted_covers == 75.5

    def test_hour_validation(self):
        with pytest.raises(ValueError):
            DemandForecast(
                id="fc-1", venue_id="v-1", date=date(2026, 4, 6),
                hour=24, predicted_covers=50, confidence=0.8,
                model_version="v1",
            )

    def test_confidence_range(self):
        with pytest.raises(ValueError):
            DemandForecast(
                id="fc-1", venue_id="v-1", date=date(2026, 4, 6),
                hour=12, predicted_covers=50, confidence=1.5,
                model_version="v1",
            )

    def test_negative_covers_rejected(self):
        with pytest.raises(ValueError):
            DemandForecast(
                id="fc-1", venue_id="v-1", date=date(2026, 4, 6),
                hour=12, predicted_covers=-10, confidence=0.8,
                model_version="v1",
            )


# ============================================================================
# VarianceSignal tests
# ============================================================================

class TestVarianceSignal:
    def test_create_valid(self):
        sig = VarianceSignal(
            signal_type=SignalType.weather, value=0.3,
            weight=0.15, confidence=0.9, source="bom",
            timestamp=datetime.now(),
        )
        assert sig.value == 0.3

    def test_weight_validation(self):
        with pytest.raises(ValueError):
            VarianceSignal(
                signal_type=SignalType.weather, value=0.3,
                weight=1.5, confidence=0.9, source="bom",
                timestamp=datetime.now(),
            )

    def test_confidence_validation(self):
        with pytest.raises(ValueError):
            VarianceSignal(
                signal_type=SignalType.weather, value=0.3,
                weight=0.5, confidence=-0.1, source="bom",
                timestamp=datetime.now(),
            )


# ============================================================================
# CostBreakdown tests
# ============================================================================

class TestCostBreakdown:
    def test_valid_breakdown(self):
        cb = CostBreakdown(
            base_cost=Decimal("200.00"), penalty_cost=Decimal("50.00"),
            casual_loading=Decimal("50.00"), super_contribution=Decimal("34.50"),
            total_cost=Decimal("334.50"),
        )
        assert cb.total_cost == Decimal("334.50")

    def test_total_must_match_components(self):
        with pytest.raises(ValueError, match="sum of components"):
            CostBreakdown(
                base_cost=Decimal("200.00"), penalty_cost=Decimal("50.00"),
                casual_loading=Decimal("50.00"), super_contribution=Decimal("34.50"),
                total_cost=Decimal("999.99"),
            )


# ============================================================================
# StaffingRecommendation tests
# ============================================================================

class TestStaffingRecommendation:
    def test_create_valid(self):
        rec = StaffingRecommendation(
            action=StaffAction.cut, employee_id="emp-1",
            reason="Casual with low hours", priority=0.85,
        )
        assert rec.action == StaffAction.cut

    def test_priority_range(self):
        with pytest.raises(ValueError):
            StaffingRecommendation(
                action=StaffAction.cut, employee_id="emp-1",
                reason="test", priority=1.5,
            )


# ============================================================================
# VenueConfig tests
# ============================================================================

class TestVenueConfig:
    def test_create_valid(self):
        vc = VenueConfig(
            id="v-1", name="The Local", tanda_org_id="org-123",
            state=State.vic, max_labour_pct=35.0,
            created_at=datetime(2025, 1, 1),
        )
        assert vc.timezone == "Australia/Melbourne"

    def test_max_labour_pct_validation(self):
        with pytest.raises(ValueError):
            VenueConfig(
                id="v-1", name="Test", tanda_org_id="org-1",
                state=State.vic, max_labour_pct=101.0,
                created_at=datetime(2025, 1, 1),
            )


# ============================================================================
# TandaCredentials and APIError tests
# ============================================================================

class TestTandaCredentials:
    def test_create(self):
        creds = TandaCredentials(
            client_id="cid", client_secret="secret", org_id="org-1",
        )
        assert creds.access_token is None

class TestAPIError:
    def test_create(self):
        err = APIError(status_code=404, message="Not found")
        assert err.detail is None
        assert err.retry_after is None
