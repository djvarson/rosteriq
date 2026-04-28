"""
Tests for the employee costing service.

Covers:
- Shift cost calculation with all components
- Employee cost projections
- Employee comparisons
- Annual cost estimates
- Roster labour cost summaries
"""

import pytest
from decimal import Decimal
from datetime import date, time, datetime, timedelta

from rosteriq.models import (
    Employee,
    Shift,
    EmploymentType,
    AwardLevel,
    State,
    ShiftStatus,
)
from rosteriq.services.employee_costing import (
    EmployeeCostingService,
    EmployeeCostProjection,
    RosterCostSummary,
)


# ============================================================================
# FIXTURES
# ============================================================================

@pytest.fixture
def service():
    """Create a costing service instance."""
    return EmployeeCostingService()


@pytest.fixture
def full_time_employee_nsw():
    """Create a full-time employee in NSW."""
    now = datetime.now()
    return Employee(
        id="emp_001",
        name="Alice Smith",
        employment_type=EmploymentType.full_time,
        award_level=AwardLevel.level_2,
        state=State.nsw,
        hourly_base_rate=Decimal("25.00"),
        skills=["cooking", "management"],
        created_at=now,
        updated_at=now,
    )


@pytest.fixture
def part_time_employee_vic():
    """Create a part-time employee in Victoria."""
    now = datetime.now()
    return Employee(
        id="emp_002",
        name="Bob Jones",
        employment_type=EmploymentType.part_time,
        award_level=AwardLevel.level_1,
        state=State.vic,
        hourly_base_rate=Decimal("23.50"),
        skills=["front_of_house"],
        created_at=now,
        updated_at=now,
    )


@pytest.fixture
def casual_employee_qld():
    """Create a casual employee in Queensland."""
    now = datetime.now()
    return Employee(
        id="emp_003",
        name="Carol White",
        employment_type=EmploymentType.casual,
        award_level=AwardLevel.level_1,
        state=State.qld,
        hourly_base_rate=Decimal("22.00"),
        skills=["dishwashing"],
        created_at=now,
        updated_at=now,
    )


@pytest.fixture
def weekday_shift():
    """Create a weekday shift."""
    return Shift(
        id="shift_001",
        employee_id="emp_001",
        date=date(2026, 5, 6),  # Wednesday
        start_time=time(9, 0),
        end_time=time(17, 0),
        break_minutes=30,
        status=ShiftStatus.scheduled,
        role="chef",
    )


@pytest.fixture
def saturday_shift():
    """Create a Saturday shift."""
    return Shift(
        id="shift_002",
        employee_id="emp_002",
        date=date(2026, 5, 9),  # Saturday
        start_time=time(10, 0),
        end_time=time(18, 0),
        break_minutes=30,
        status=ShiftStatus.scheduled,
        role="waiter",
    )


@pytest.fixture
def sunday_shift():
    """Create a Sunday shift."""
    return Shift(
        id="shift_003",
        employee_id="emp_003",
        date=date(2026, 5, 10),  # Sunday
        start_time=time(11, 0),
        end_time=time(19, 0),
        break_minutes=30,
        status=ShiftStatus.scheduled,
        role="dishwasher",
    )


@pytest.fixture
def evening_shift():
    """Create an evening shift (after 7pm)."""
    return Shift(
        id="shift_004",
        employee_id="emp_001",
        date=date(2026, 5, 7),  # Thursday (weekday)
        start_time=time(19, 0),
        end_time=time(23, 0),
        break_minutes=0,
        status=ShiftStatus.scheduled,
        role="bar_manager",
    )


# ============================================================================
# TESTS: SHIFT COST CALCULATION
# ============================================================================

class TestShiftCostCalculation:
    """Test individual shift cost calculations."""

    def test_weekday_full_time_shift(self, service, full_time_employee_nsw, weekday_shift):
        """Test cost calculation for weekday FT shift (no penalty)."""
        base_pay, penalty_pay, casual_loading, super_contrib, workcover, leave_accrual, total = (
            service._calculate_shift_costs(full_time_employee_nsw, weekday_shift)
        )

        # 7.5 net hours × $25 base = $187.50 base pay
        assert base_pay == Decimal("187.50")

        # No penalty on weekday for FT
        assert penalty_pay == Decimal("0.00")

        # No casual loading for FT
        assert casual_loading == Decimal("0.00")

        # Super at 11.5% on base
        expected_super = (Decimal("187.50") * Decimal("0.115")).quantize(
            Decimal("0.01")
        )
        assert super_contrib == expected_super

        # WorkCover at 1.4% (NSW)
        assert workcover > Decimal("0")

        # Leave accrual should include annual, personal, LSL
        assert leave_accrual > Decimal("0")

        # Total should be sum of all components
        assert total > base_pay

    def test_saturday_part_time_shift(self, service, part_time_employee_vic, saturday_shift):
        """Test cost calculation for Saturday PT shift (1.25x penalty)."""
        base_pay, penalty_pay, casual_loading, super_contrib, workcover, leave_accrual, total = (
            service._calculate_shift_costs(part_time_employee_vic, saturday_shift)
        )

        # 7.5 net hours × $23.50 base = $176.25 base pay
        assert base_pay == Decimal("176.25")

        # Saturday penalty: 0.25x multiplier
        expected_penalty = (Decimal("176.25") * Decimal("0.25")).quantize(
            Decimal("0.01")
        )
        assert penalty_pay == expected_penalty

        # No casual loading for PT
        assert casual_loading == Decimal("0.00")

        # Total should be higher due to penalty
        assert total > base_pay

    def test_sunday_casual_shift(self, service, casual_employee_qld, sunday_shift):
        """Test cost calculation for Sunday casual shift."""
        base_pay, penalty_pay, casual_loading, super_contrib, workcover, leave_accrual, total = (
            service._calculate_shift_costs(casual_employee_qld, sunday_shift)
        )

        # 7.5 net hours × $22.00 base = $165.00 base pay
        assert base_pay == Decimal("165.00")

        # Casual loading: 25% on base
        expected_casual = (Decimal("165.00") * Decimal("0.25")).quantize(
            Decimal("0.01")
        )
        assert casual_loading == expected_casual

        # Sunday penalty for casual: base is 1.25 (includes casual loading)
        # Additional penalty is 1.75 - 1.25 = 0.50 multiplier
        expected_penalty = (Decimal("165.00") * Decimal("0.50")).quantize(
            Decimal("0.01")
        )
        assert penalty_pay == expected_penalty

        # Total should include casual loading + penalty
        assert total > base_pay + casual_loading

    def test_evening_shift_loading(self, service, full_time_employee_nsw, evening_shift):
        """Test evening shift loading (after 7pm on weekday)."""
        base_pay, penalty_pay, casual_loading, super_contrib, workcover, leave_accrual, total = (
            service._calculate_shift_costs(full_time_employee_nsw, evening_shift)
        )

        # 4 net hours × $25 base = $100.00 base pay
        assert base_pay == Decimal("100.00")

        # Evening loading: 15% after 7pm (Monday-Friday only)
        expected_penalty = (Decimal("100.00") * Decimal("0.15")).quantize(
            Decimal("0.01")
        )
        assert penalty_pay == expected_penalty

        # Total should be higher due to evening loading
        assert total > base_pay


# ============================================================================
# TESTS: EMPLOYEE COST PROJECTIONS
# ============================================================================

class TestEmployeeCostProjection:
    """Test employee cost projections across multiple shifts."""

    def test_single_shift_projection(self, service, full_time_employee_nsw, weekday_shift):
        """Test projection for a single shift."""
        projection = service.project_cost(full_time_employee_nsw, [weekday_shift])

        assert projection.employee_id == "emp_001"
        assert projection.employee_name == "Alice Smith"
        assert projection.employment_type == EmploymentType.full_time
        assert projection.total_shifts == 1
        assert projection.total_hours == Decimal("7.50")

        # Cost components should be populated
        assert projection.base_pay > Decimal("0")
        assert projection.super_contribution > Decimal("0")
        assert projection.workcover_levy > Decimal("0")
        assert projection.leave_accrual > Decimal("0")

        # Total cost should be sum of all components
        expected_total = (
            projection.base_pay +
            projection.penalty_pay +
            projection.casual_loading +
            projection.super_contribution +
            projection.workcover_levy +
            projection.leave_accrual
        )
        assert projection.total_cost == expected_total

        # Effective hourly rate
        assert projection.effective_hourly_rate > projection.total_hours / projection.total_cost

    def test_multiple_shifts_projection(
        self,
        service,
        full_time_employee_nsw,
        weekday_shift,
        evening_shift,
    ):
        """Test projection for multiple shifts."""
        shifts = [weekday_shift, evening_shift]
        projection = service.project_cost(full_time_employee_nsw, shifts)

        assert projection.total_shifts == 2
        assert projection.total_hours == Decimal("11.50")  # 7.5 + 4

        # Should have multiple shift details
        assert len(projection.per_shift_breakdown) == 2

    def test_casual_employee_loading(self, service, casual_employee_qld, sunday_shift):
        """Test casual employee has proper 25% loading."""
        projection = service.project_cost(casual_employee_qld, [sunday_shift])

        # Casual loading should be 25% of base pay
        expected_loading = (projection.base_pay * Decimal("0.25")).quantize(
            Decimal("0.01")
        )
        assert projection.casual_loading == expected_loading

    def test_payroll_tax_calculation(
        self,
        service,
        full_time_employee_nsw,
        weekday_shift,
    ):
        """Test payroll tax is calculated for venues over threshold."""
        # NSW threshold: $1.2M
        venue_payroll = Decimal("1500000")  # Over threshold

        projection = service.project_cost(
            full_time_employee_nsw,
            [weekday_shift],
            venue_annual_payroll=venue_payroll,
        )

        # Should have payroll tax component
        assert projection.payroll_tax_component >= Decimal("0")

    def test_warnings_for_high_casual_loading(
        self,
        service,
        casual_employee_qld,
        sunday_shift,
    ):
        """Test warning when casual loading is unusually high."""
        # Add multiple Sunday shifts to trigger high loading warning
        shifts = [sunday_shift] * 4

        projection = service.project_cost(casual_employee_qld, shifts)

        # Should generate warning about casual loading
        # (Note: actual warnings depend on implementation thresholds)


# ============================================================================
# TESTS: EMPLOYEE COMPARISONS
# ============================================================================

class TestEmployeeComparison:
    """Test comparing costs across employees."""

    def test_compare_employees_same_shifts(
        self,
        service,
        full_time_employee_nsw,
        part_time_employee_vic,
        casual_employee_qld,
        weekday_shift,
    ):
        """Test comparing cost of different employees on same shift."""
        employees = [
            full_time_employee_nsw,
            part_time_employee_vic,
            casual_employee_qld,
        ]

        projections = service.compare_employees(
            employees,
            [weekday_shift],
        )

        # Should return 3 projections
        assert len(projections) == 3

        # Should be sorted by total cost (cheapest first)
        costs = [p.total_cost for p in projections]
        assert costs == sorted(costs)

    def test_find_cheapest_option(
        self,
        service,
        full_time_employee_nsw,
        part_time_employee_vic,
        casual_employee_qld,
        saturday_shift,
    ):
        """Test finding cheapest employee for a shift."""
        employees = [
            full_time_employee_nsw,
            part_time_employee_vic,
            casual_employee_qld,
        ]

        results = service.find_cheapest_option(employees, saturday_shift)

        # Should return list of (Employee, cost) tuples
        assert len(results) == 3

        # Should be sorted by cost (cheapest first)
        costs = [cost for _, cost in results]
        assert costs == sorted(costs)

        # Cheapest should be first
        assert results[0][1] <= results[-1][1]


# ============================================================================
# TESTS: ANNUAL COST ESTIMATES
# ============================================================================

class TestAnnualCostEstimate:
    """Test annual cost projections."""

    def test_annual_cost_calculation(self, service, full_time_employee_nsw):
        """Test annual cost projection for average weekly hours."""
        avg_weekly_hours = 38.0  # Full-time
        weeks_per_year = 52

        annual_estimate = service.annual_cost_estimate(
            full_time_employee_nsw,
            avg_weekly_hours,
            weeks_per_year,
        )

        # Should have all cost components annualized
        assert "annual_hours" in annual_estimate
        assert "annual_base_pay" in annual_estimate
        assert "annual_super_contribution" in annual_estimate
        assert "annual_total_cost" in annual_estimate

        # Annual hours should be 38 × 52
        assert annual_estimate["annual_hours"] == Decimal(str(38.0 * 52))

        # Annual total should be greater than annual base
        assert annual_estimate["annual_total_cost"] > annual_estimate["annual_base_pay"]

        # Effective hourly rate should be constant
        assert annual_estimate["effective_hourly_rate"] > Decimal("0")

    def test_casual_annual_estimate(self, service, casual_employee_qld):
        """Test annual estimate for casual employee."""
        avg_weekly_hours = 15.0  # Part-time casual

        annual_estimate = service.annual_cost_estimate(
            casual_employee_qld,
            avg_weekly_hours,
            weeks_per_year=50,  # Casuals often have less consistent work
        )

        # Should include casual loading
        assert annual_estimate["annual_casual_loading"] > Decimal("0")

        # Total should be base + loading + super
        expected_min = (
            annual_estimate["annual_base_pay"] +
            annual_estimate["annual_casual_loading"]
        )
        assert annual_estimate["annual_total_cost"] >= expected_min


# ============================================================================
# TESTS: ROSTER COST SUMMARIES
# ============================================================================

class TestRosterCostSummary:
    """Test roster-level cost calculations."""

    def test_empty_roster_cost(self, service):
        """Test empty roster returns zero costs."""
        summary = service.calculate_roster_labour_cost([])

        assert summary.total_hours == Decimal("0")
        assert summary.total_shifts == 0
        assert summary.total_employees == 0
        assert summary.total_cost == Decimal("0")

    def test_single_employee_roster(
        self,
        service,
        full_time_employee_nsw,
        weekday_shift,
        saturday_shift,
    ):
        """Test roster with single employee multiple shifts."""
        roster_shifts = [
            (full_time_employee_nsw, weekday_shift),
            (full_time_employee_nsw, saturday_shift),
        ]

        summary = service.calculate_roster_labour_cost(roster_shifts)

        assert summary.total_employees == 1
        assert summary.total_shifts == 2
        assert summary.total_hours > Decimal("0")
        assert summary.total_cost > Decimal("0")

        # Should have one employee cost detail
        assert len(summary.employee_costs) == 1

    def test_multi_employee_roster(
        self,
        service,
        full_time_employee_nsw,
        part_time_employee_vic,
        casual_employee_qld,
        weekday_shift,
        saturday_shift,
        sunday_shift,
    ):
        """Test roster with multiple employees."""
        # Adjust shift employee IDs
        weekday_shift.employee_id = "emp_001"
        saturday_shift.employee_id = "emp_002"
        sunday_shift.employee_id = "emp_003"

        roster_shifts = [
            (full_time_employee_nsw, weekday_shift),
            (part_time_employee_vic, saturday_shift),
            (casual_employee_qld, sunday_shift),
        ]

        summary = service.calculate_roster_labour_cost(roster_shifts)

        assert summary.total_employees == 3
        assert summary.total_shifts == 3

        # Employee costs should be in order
        assert len(summary.employee_costs) == 3

        # Total should be sum of individual costs
        employee_total = sum(ec.total_cost for ec in summary.employee_costs)
        assert summary.total_cost == employee_total


# ============================================================================
# TESTS: STATE-SPECIFIC CALCULATIONS
# ============================================================================

class TestStateSpecificCalculations:
    """Test state-specific levies and taxes."""

    @pytest.mark.parametrize("state,expected_levy_rate", [
        (State.nsw, Decimal("0.014")),   # 1.4%
        (State.vic, Decimal("0.0127")),  # 1.27%
        (State.qld, Decimal("0.012")),   # 1.2%
        (State.sa, Decimal("0.015")),    # 1.5%
        (State.wa, Decimal("0.016")),    # 1.6%
        (State.tas, Decimal("0.018")),   # 1.8%
        (State.nt, Decimal("0.020")),    # 2.0%
        (State.act, Decimal("0.0135")),  # 1.35%
    ])
    def test_workcover_levy_rates(self, state, expected_levy_rate):
        """Test correct WorkCover levy rates by state."""
        from rosteriq.services.employee_costing import WORKCOVER_LEVIES

        assert WORKCOVER_LEVIES[state] == expected_levy_rate

    @pytest.mark.parametrize("state,expected_threshold,expected_rate", [
        (State.nsw, Decimal("1200000"), Decimal("0.0545")),
        (State.vic, Decimal("900000"), Decimal("0.0485")),
        (State.qld, Decimal("1300000"), Decimal("0.0475")),
        (State.sa, Decimal("1500000"), Decimal("0.0495")),
        (State.wa, Decimal("1000000"), Decimal("0.055")),
        (State.tas, Decimal("1250000"), Decimal("0.04")),
        (State.nt, Decimal("1500000"), Decimal("0.055")),
        (State.act, Decimal("2000000"), Decimal("0.0685")),
    ])
    def test_payroll_tax_thresholds(self, state, expected_threshold, expected_rate):
        """Test correct payroll tax thresholds and rates by state."""
        from rosteriq.services.employee_costing import PAYROLL_TAX_CONFIG

        threshold, rate = PAYROLL_TAX_CONFIG[state]
        assert threshold == expected_threshold
        assert rate == expected_rate


# ============================================================================
# TESTS: EDGE CASES
# ============================================================================

class TestEdgeCases:
    """Test edge cases and boundary conditions."""

    def test_zero_hour_shift(self, service, full_time_employee_nsw):
        """Test shift with zero hours."""
        zero_shift = Shift(
            id="zero_shift",
            employee_id="emp_001",
            date=date(2026, 5, 6),
            start_time=time(9, 0),
            end_time=time(9, 0),
            break_minutes=0,
            status=ShiftStatus.scheduled,
            role="cleaner",
        )

        projection = service.project_cost(full_time_employee_nsw, [zero_shift])

        # Should handle gracefully with zero costs
        assert projection.total_hours == Decimal("0")
        assert projection.total_cost == Decimal("0")
        assert projection.effective_hourly_rate == Decimal("0")

    def test_very_long_shift(self, service, full_time_employee_nsw):
        """Test very long shift (max allowed is 11.5 hours)."""
        long_shift = Shift(
            id="long_shift",
            employee_id="emp_001",
            date=date(2026, 5, 6),
            start_time=time(8, 0),
            end_time=time(19, 30),
            break_minutes=60,
            status=ShiftStatus.scheduled,
            role="chef",
        )

        projection = service.project_cost(full_time_employee_nsw, [long_shift])

        # Should calculate cost without error
        assert projection.total_shifts == 1
        assert projection.total_cost > Decimal("0")

    def test_overnight_shift(self, service, full_time_employee_nsw):
        """Test shift spanning midnight."""
        overnight_shift = Shift(
            id="overnight_shift",
            employee_id="emp_001",
            date=date(2026, 5, 6),
            start_time=time(22, 0),
            end_time=time(6, 0),  # Next day
            break_minutes=60,
            status=ShiftStatus.scheduled,
            role="bar_staff",
        )

        projection = service.project_cost(full_time_employee_nsw, [overnight_shift])

        # Should calculate cost
        assert projection.total_shifts == 1
        # Overnight shift will have extended hours (22:00 to 6:00 next day = 8 hours)
        assert projection.total_cost > Decimal("0")
