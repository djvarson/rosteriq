"""
Integration tests for conflict_detector.py - Roster conflict detection engine.

Tests cover all 11 conflict types:
- DOUBLE_BOOKING: Overlapping shifts same day
- AVAILABILITY_VIOLATION: Shift outside employee availability
- OVERTIME_BREACH: Exceeds max_hours_per_week
- CONSECUTIVE_DAYS_VIOLATION: Exceeds max consecutive working days
- SKILL_MISMATCH: Employee lacks required shift skill
- MINIMUM_ENGAGEMENT: Shift too short for employment type
- MAX_SHIFT_LENGTH: Shift exceeds 11.5 hours
- BREAK_VIOLATION: Insufficient break duration
- UNDERSTAFFED_HOUR: Below minimum staff for hour
- OVERSTAFFED_HOUR: Exceeds 1.5x minimum staff
- FATIGUE_RISK: Insufficient rest between shifts (< 10 hours)
"""

from datetime import date, time, datetime, timedelta
from decimal import Decimal

from rosteriq.models import (
    Employee, Shift, Roster, VenueConfig,
    EmploymentType, ShiftStatus, AwardLevel, State,
)
from rosteriq.services.conflict_detector import (
    ConflictDetector, ConflictType, ConflictSeverity, RosterConflict,
)


# ============================================================================
# Test Fixtures
# ============================================================================

def create_test_employee(emp_id: str, **kwargs) -> Employee:
    """Create a test employee with defaults."""
    defaults = {
        "name": f"Employee {emp_id}",
        "employment_type": EmploymentType.part_time,
        "award_level": AwardLevel.level_2,
        "state": State.vic,
        "hourly_base_rate": Decimal("25.00"),
        "phone": "0412345678",
        "email": f"{emp_id}@test.com",
        "skills": ["general", "bar"],
        "availability": {
            "monday": [{"start": "08:00", "end": "22:00"}],
            "tuesday": [{"start": "08:00", "end": "22:00"}],
            "wednesday": [{"start": "08:00", "end": "22:00"}],
            "thursday": [{"start": "08:00", "end": "22:00"}],
            "friday": [{"start": "08:00", "end": "23:00"}],
            "saturday": [{"start": "10:00", "end": "23:00"}],
            "sunday": [{"start": "10:00", "end": "22:00"}],
        },
        "max_hours_per_week": 38.0,
        "consecutive_days_limit": 6,
        "created_at": datetime.now(),
        "updated_at": datetime.now(),
    }
    defaults.update(kwargs)
    return Employee(id=emp_id, **defaults)


def create_test_shift(
    shift_id: str,
    emp_id: str,
    test_date: date,
    start_hour: int,
    end_hour: int,
    break_minutes: int = 30,
) -> Shift:
    """Create a test shift."""
    return Shift(
        id=shift_id,
        employee_id=emp_id,
        date=test_date,
        start_time=time(start_hour, 0),
        end_time=time(end_hour, 0),
        break_minutes=break_minutes,
        status=ShiftStatus.scheduled,
        role="general",
        cost=Decimal("100.00"),
    )


def create_test_roster(week_start: date, venue_id: str, shifts: list) -> Roster:
    """Create a test roster."""
    return Roster(
        id="roster_test",
        venue_id=venue_id,
        week_start=week_start,
        week_end=week_start + timedelta(days=6),
        shifts=shifts,
        created_at=datetime.now(),
    )


def create_test_venue(**kwargs) -> VenueConfig:
    """Create a test venue."""
    defaults = {
        "id": "venue_test",
        "name": "Test Venue",
        "tanda_org_id": "tanda_org",
        "state": State.vic,
        "min_staff": {"general": 2},
        "max_labour_pct": 28.0,
        "created_at": datetime.now(),
    }
    defaults.update(kwargs)
    return VenueConfig(**defaults)


# ============================================================================
# Individual Conflict Type Tests
# ============================================================================

def test_double_booking_detection():
    """Test detection of overlapping shifts on same day."""
    print("Running test_double_booking_detection...", end=" ")
    week_start = date(2026, 4, 27)
    emp = create_test_employee("emp1")
    venue = create_test_venue()
    detector = ConflictDetector()

    # Two overlapping shifts same day
    shifts = [
        create_test_shift("s1", "emp1", week_start, 10, 14),
        create_test_shift("s2", "emp1", week_start, 13, 17),
    ]
    roster = create_test_roster(week_start, venue.id, shifts)

    conflicts = detector.detect_conflicts(roster, venue, [emp])

    double_booking = [c for c in conflicts if c.conflict_type == ConflictType.DOUBLE_BOOKING]
    assert len(double_booking) > 0, "Should detect double booking"
    assert any(c.severity == ConflictSeverity.CRITICAL for c in double_booking)

    print("PASS")


def test_availability_violation_detection():
    """Test detection of shifts outside availability."""
    print("Running test_availability_violation_detection...", end=" ")
    week_start = date(2026, 4, 27)  # Monday

    emp = create_test_employee("emp1")
    emp.availability["monday"] = [{"start": "09:00", "end": "17:00"}]

    venue = create_test_venue()
    detector = ConflictDetector()

    # Shift outside availability (before 9am)
    shifts = [create_test_shift("s1", "emp1", week_start, 7, 16)]
    roster = create_test_roster(week_start, venue.id, shifts)

    conflicts = detector.detect_conflicts(roster, venue, [emp])

    avail_violations = [c for c in conflicts if c.conflict_type == ConflictType.AVAILABILITY_VIOLATION]
    assert len(avail_violations) > 0, "Should detect availability violation"

    print("PASS")


def test_overtime_breach_detection():
    """Test detection of exceeding max_hours_per_week."""
    print("Running test_overtime_breach_detection...", end=" ")
    week_start = date(2026, 4, 27)

    emp = create_test_employee("emp1")
    emp.max_hours_per_week = 20.0  # Low limit for test

    venue = create_test_venue()
    detector = ConflictDetector()

    # Create shifts totaling > 20 hours in week
    shifts = [
        create_test_shift("s1", "emp1", week_start, 8, 16, break_minutes=30),      # 7.5h
        create_test_shift("s2", "emp1", week_start + timedelta(days=1), 8, 16, break_minutes=30),  # 7.5h
        create_test_shift("s3", "emp1", week_start + timedelta(days=2), 8, 16, break_minutes=30),  # 7.5h
    ]
    roster = create_test_roster(week_start, venue.id, shifts)

    conflicts = detector.detect_conflicts(roster, venue, [emp])

    overtime = [c for c in conflicts if c.conflict_type == ConflictType.OVERTIME_BREACH]
    assert len(overtime) > 0, "Should detect overtime breach"

    print("PASS")


def test_consecutive_days_violation_detection():
    """Test detection of exceeding consecutive days limit."""
    print("Running test_consecutive_days_violation_detection...", end=" ")
    week_start = date(2026, 4, 27)  # Monday

    emp = create_test_employee("emp1")
    emp.consecutive_days_limit = 4  # Max 4 consecutive days

    venue = create_test_venue()
    detector = ConflictDetector()

    # Create 5 consecutive days of shifts
    shifts = [
        create_test_shift(f"s{i}", "emp1", week_start + timedelta(days=i), 10, 14)
        for i in range(5)
    ]
    roster = create_test_roster(week_start, venue.id, shifts)

    conflicts = detector.detect_conflicts(roster, venue, [emp])

    consec_violations = [c for c in conflicts if c.conflict_type == ConflictType.CONSECUTIVE_DAYS_VIOLATION]
    assert len(consec_violations) > 0, "Should detect consecutive days violation"

    print("PASS")


def test_skill_mismatch_detection():
    """Test detection of skill mismatches."""
    print("Running test_skill_mismatch_detection...", end=" ")
    week_start = date(2026, 4, 27)

    emp = create_test_employee("emp1")
    emp.skills = ["bar", "general"]  # No kitchen skill

    venue = create_test_venue()
    detector = ConflictDetector()

    # Shift requiring kitchen skill
    shift = create_test_shift("s1", "emp1", week_start, 10, 14)
    shift.role = "kitchen"  # Employee doesn't have this skill

    roster = create_test_roster(week_start, venue.id, [shift])

    conflicts = detector.detect_conflicts(roster, venue, [emp])

    mismatches = [c for c in conflicts if c.conflict_type == ConflictType.SKILL_MISMATCH]
    assert len(mismatches) > 0, "Should detect skill mismatch"
    assert any(c.severity == ConflictSeverity.WARNING for c in mismatches)

    print("PASS")


def test_minimum_engagement_detection():
    """Test detection of shifts too short for employment type."""
    print("Running test_minimum_engagement_detection...", end=" ")
    week_start = date(2026, 4, 27)

    # Casual employee needs minimum 2 hour engagement
    emp = create_test_employee("emp1")
    emp.employment_type = EmploymentType.casual

    venue = create_test_venue()
    detector = ConflictDetector()

    # 1 hour shift (too short for casual)
    shift = create_test_shift("s1", "emp1", week_start, 10, 11, break_minutes=0)
    roster = create_test_roster(week_start, venue.id, [shift])

    conflicts = detector.detect_conflicts(roster, venue, [emp])

    min_engagement = [c for c in conflicts if c.conflict_type == ConflictType.MINIMUM_ENGAGEMENT]
    assert len(min_engagement) > 0, "Should detect minimum engagement violation"

    print("PASS")


def test_max_shift_length_detection():
    """Test detection of shifts exceeding 11.5 hours."""
    print("Running test_max_shift_length_detection...", end=" ")
    week_start = date(2026, 4, 27)

    emp = create_test_employee("emp1")
    venue = create_test_venue()
    detector = ConflictDetector()

    # 12 hour shift (exceeds 11.5 max)
    shift = create_test_shift("s1", "emp1", week_start, 8, 20, break_minutes=30)
    roster = create_test_roster(week_start, venue.id, [shift])

    conflicts = detector.detect_conflicts(roster, venue, [emp])

    max_length = [c for c in conflicts if c.conflict_type == ConflictType.MAX_SHIFT_LENGTH]
    assert len(max_length) > 0, "Should detect max shift length violation"
    assert any(c.severity == ConflictSeverity.CRITICAL for c in max_length)

    print("PASS")


def test_break_violation_detection():
    """Test detection of insufficient break duration."""
    print("Running test_break_violation_detection...", end=" ")
    week_start = date(2026, 4, 27)

    emp = create_test_employee("emp1")
    venue = create_test_venue()
    detector = ConflictDetector()

    # 8 hour shift but only 15 min break (needs 30)
    shift = create_test_shift("s1", "emp1", week_start, 10, 18, break_minutes=15)
    roster = create_test_roster(week_start, venue.id, [shift])

    conflicts = detector.detect_conflicts(roster, venue, [emp])

    break_violations = [c for c in conflicts if c.conflict_type == ConflictType.BREAK_VIOLATION]
    assert len(break_violations) > 0, "Should detect break violation"
    assert any(c.severity == ConflictSeverity.CRITICAL for c in break_violations)

    print("PASS")


def test_understaffed_hour_detection():
    """Test detection of understaffed hours."""
    print("Running test_understaffed_hour_detection...", end=" ")
    week_start = date(2026, 4, 27)

    emp1 = create_test_employee("emp1")
    emp2 = create_test_employee("emp2")

    venue = create_test_venue()
    venue.min_staff = {"general": 3}  # Need 3 staff

    detector = ConflictDetector()

    # Only 2 staff during hour 12
    shift1 = create_test_shift("s1", "emp1", week_start, 11, 13)
    shift2 = create_test_shift("s2", "emp2", week_start, 12, 14)

    roster = create_test_roster(week_start, venue.id, [shift1, shift2])

    conflicts = detector.detect_conflicts(roster, venue, [emp1, emp2])

    understaffed = [c for c in conflicts if c.conflict_type == ConflictType.UNDERSTAFFED_HOUR]
    assert len(understaffed) > 0, "Should detect understaffed hour"

    print("PASS")


def test_overstaffed_hour_detection():
    """Test detection of overstaffed hours."""
    print("Running test_overstaffed_hour_detection...", end=" ")
    week_start = date(2026, 4, 27)

    employees = [create_test_employee(f"emp{i}") for i in range(6)]

    venue = create_test_venue()
    venue.min_staff = {"general": 2}  # Target 2, overstaffed > 3

    detector = ConflictDetector()

    # 5 staff during hour 12-13 (overstaffed)
    shifts = [
        create_test_shift(f"s{i}", f"emp{i}", week_start, 12, 13)
        for i in range(5)
    ]

    roster = create_test_roster(week_start, venue.id, shifts)

    conflicts = detector.detect_conflicts(roster, venue, employees)

    overstaffed = [c for c in conflicts if c.conflict_type == ConflictType.OVERSTAFFED_HOUR]
    assert len(overstaffed) > 0, "Should detect overstaffed hour"
    assert any(c.severity == ConflictSeverity.INFO for c in overstaffed)

    print("PASS")


def test_fatigue_risk_detection():
    """Test detection of insufficient rest between shifts."""
    print("Running test_fatigue_risk_detection...", end=" ")
    week_start = date(2026, 4, 27)

    emp = create_test_employee("emp1")
    venue = create_test_venue()
    detector = ConflictDetector()

    # Shift ending at 22, next shift starting at 6 (only 8 hours rest, needs 10)
    shift1 = create_test_shift("s1", "emp1", week_start, 14, 22, break_minutes=30)
    shift2 = create_test_shift("s2", "emp1", week_start + timedelta(days=1), 6, 14, break_minutes=30)

    roster = create_test_roster(week_start, venue.id, [shift1, shift2])

    conflicts = detector.detect_conflicts(roster, venue, [emp])

    fatigue = [c for c in conflicts if c.conflict_type == ConflictType.FATIGUE_RISK]
    assert len(fatigue) > 0, "Should detect fatigue risk"

    print("PASS")


# ============================================================================
# Clean Roster Tests
# ============================================================================

def test_clean_roster_no_conflicts():
    """Test that a well-formed roster produces no conflicts."""
    print("Running test_clean_roster_no_conflicts...", end=" ")
    week_start = date(2026, 4, 27)

    emp = create_test_employee("emp1")
    venue = create_test_venue()
    detector = ConflictDetector()

    # Properly scheduled shifts
    shifts = [
        create_test_shift("s1", "emp1", week_start, 9, 17, break_minutes=30),
        create_test_shift("s2", "emp1", week_start + timedelta(days=2), 10, 18, break_minutes=30),
    ]

    roster = create_test_roster(week_start, venue.id, shifts)

    conflicts = detector.detect_conflicts(roster, venue, [emp])

    # Should have minimal or no conflicts
    critical_conflicts = [c for c in conflicts if c.severity == ConflictSeverity.CRITICAL]
    assert len(critical_conflicts) == 0, "Clean roster should have no critical conflicts"

    print("PASS")


# ============================================================================
# Multiple Concurrent Conflicts Tests
# ============================================================================

def test_multiple_concurrent_conflicts():
    """Test detection of multiple simultaneous conflicts."""
    print("Running test_multiple_concurrent_conflicts...", end=" ")
    week_start = date(2026, 4, 27)

    emp = create_test_employee("emp1")
    emp.availability["monday"] = [{"start": "09:00", "end": "17:00"}]
    emp.max_hours_per_week = 15.0
    emp.skills = ["bar"]

    venue = create_test_venue()
    detector = ConflictDetector()

    # Multiple conflicts:
    # 1. Outside availability (7am start)
    # 2. Too long shift (12 hours)
    # 3. Insufficient break (30 min in 12 hour shift)
    shifts = [
        create_test_shift("s1", "emp1", week_start, 7, 19, break_minutes=15),
        create_test_shift("s2", "emp1", week_start + timedelta(days=1), 10, 18, break_minutes=30),
    ]

    roster = create_test_roster(week_start, venue.id, shifts)

    conflicts = detector.detect_conflicts(roster, venue, [emp])

    assert len(conflicts) > 2, "Should detect multiple conflicts"

    conflict_types = {c.conflict_type for c in conflicts}
    assert ConflictType.AVAILABILITY_VIOLATION in conflict_types
    assert ConflictType.MAX_SHIFT_LENGTH in conflict_types
    assert ConflictType.BREAK_VIOLATION in conflict_types

    print("PASS")


# ============================================================================
# Severity Classification Tests
# ============================================================================

def test_severity_classification():
    """Test that conflicts are classified with appropriate severity."""
    print("Running test_severity_classification...", end=" ")
    week_start = date(2026, 4, 27)

    emp = create_test_employee("emp1")
    venue = create_test_venue()
    detector = ConflictDetector()

    # Mix of critical and warning issues
    shifts = [
        # Double booking = CRITICAL
        create_test_shift("s1", "emp1", week_start, 10, 14),
        create_test_shift("s2", "emp1", week_start, 13, 17),

        # Skill mismatch = WARNING
        create_test_shift("s3", "emp1", week_start + timedelta(days=1), 10, 14),
    ]
    shifts[2].role = "chef"  # Employee doesn't have this skill

    roster = create_test_roster(week_start, venue.id, shifts)

    conflicts = detector.detect_conflicts(roster, venue, [emp])

    critical = [c for c in conflicts if c.severity == ConflictSeverity.CRITICAL]
    warnings = [c for c in conflicts if c.severity == ConflictSeverity.WARNING]

    assert len(critical) > 0, "Should have critical conflicts"
    assert len(warnings) > 0, "Should have warning conflicts"

    print("PASS")


# ============================================================================
# Test Runner
# ============================================================================

if __name__ == "__main__":
    print("\n" + "="*70)
    print("CONFLICT DETECTOR INTEGRATION TESTS")
    print("="*70 + "\n")

    tests = [
        # Individual conflict type tests
        test_double_booking_detection,
        test_availability_violation_detection,
        test_overtime_breach_detection,
        test_consecutive_days_violation_detection,
        test_skill_mismatch_detection,
        test_minimum_engagement_detection,
        test_max_shift_length_detection,
        test_break_violation_detection,
        test_understaffed_hour_detection,
        test_overstaffed_hour_detection,
        test_fatigue_risk_detection,

        # Clean roster and multiple conflicts
        test_clean_roster_no_conflicts,
        test_multiple_concurrent_conflicts,

        # Severity classification
        test_severity_classification,
    ]

    passed = 0
    failed = 0

    for test in tests:
        try:
            test()
            passed += 1
        except AssertionError as e:
            print(f"FAIL - {str(e)}")
            failed += 1
        except Exception as e:
            print(f"FAIL - {type(e).__name__}: {str(e)}")
            failed += 1

    print("\n" + "="*70)
    print(f"Results: {passed} passed, {failed} failed")
    print("="*70)
