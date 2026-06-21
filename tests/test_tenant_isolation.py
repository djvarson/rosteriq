"""
Tests for multi-tenancy isolation enforcement.

Ensures that users from one venue cannot access data from another venue.
Tests TenantScopedDB filtering and access control.
"""

import pytest
from datetime import date, datetime
from decimal import Decimal

from rosteriq.database import MemoryStore
from rosteriq.models import (
    Employee, Shift, Roster, DemandForecast, VenueConfig,
    EmploymentType, ShiftStatus, AwardLevel, State,
)
from rosteriq.services.tenant_isolation import (
    TenantScopedDB, IsolationViolation, validate_venue_access,
)
from rosteriq.middleware.tenant import (
    TenantContext, _tenant_context, get_tenant_context_optional,
)


# ============================================================================
# Fixtures
# ============================================================================


@pytest.fixture
def memory_store():
    """Create a fresh in-memory store."""
    return MemoryStore()


@pytest.fixture
def scoped_db(memory_store):
    """Create a TenantScopedDB wrapper."""
    return TenantScopedDB(memory_store)


@pytest.fixture
def setup_test_data(memory_store):
    """
    Set up test data: 2 venues with employees, rosters, and forecasts.
    """
    # Venues
    venue1 = VenueConfig(
        id="venue-1",
        name="Restaurant A",
        tanda_org_id="tanda-1",
        state=State.vic,
        timezone="Australia/Melbourne",
        min_staff={"lunch": 3, "dinner": 5},
        max_labour_pct=Decimal("35"),
        pos_system="swiftpos",
        created_at=datetime(2026, 1, 1, 0, 0, 0),
    )

    venue2 = VenueConfig(
        id="venue-2",
        name="Restaurant B",
        tanda_org_id="tanda-2",
        state=State.nsw,
        timezone="Australia/Sydney",
        min_staff={"lunch": 2, "dinner": 4},
        max_labour_pct=Decimal("32"),
        pos_system="lightspeed",
        created_at=datetime(2026, 1, 1, 0, 0, 0),
    )

    memory_store.save_venue(venue1)
    memory_store.save_venue(venue2)

    # Employees - venue 1
    emp1 = Employee(
        id="emp-1",
        tanda_id="tanda-emp-1",
        name="Alice Smith",
        employment_type=EmploymentType.full_time,
        award_level=AwardLevel.level_3,
        state=State.vic,
        hourly_base_rate=Decimal("25.50"),
        phone="0401234567",
        email="alice@example.com",
        skills=["manager"],
        availability={},
        max_hours_per_week=40.0,
        consecutive_days_limit=6,
        created_at=datetime(2026, 1, 1),
        updated_at=datetime(2026, 1, 1),
    )
    emp1.venue_id = "venue-1"

    # Employees - venue 2
    emp2 = Employee(
        id="emp-2",
        tanda_id="tanda-emp-2",
        name="Bob Johnson",
        employment_type=EmploymentType.part_time,
        award_level=AwardLevel.level_2,
        state=State.nsw,
        hourly_base_rate=Decimal("22.00"),
        phone="0402345678",
        email="bob@example.com",
        skills=["waiter"],
        availability={},
        max_hours_per_week=20.0,
        consecutive_days_limit=5,
        created_at=datetime(2026, 1, 1),
        updated_at=datetime(2026, 1, 1),
    )
    emp2.venue_id = "venue-2"

    # Employees - shared between venues (testing cross-venue)
    emp3 = Employee(
        id="emp-3",
        tanda_id="tanda-emp-3",
        name="Charlie Brown",
        employment_type=EmploymentType.casual,
        award_level=AwardLevel.level_2,
        state=State.vic,
        hourly_base_rate=Decimal("20.00"),
        phone="0403456789",
        email="charlie@example.com",
        skills=["bar"],
        availability={},
        max_hours_per_week=15.0,
        consecutive_days_limit=4,
        created_at=datetime(2026, 1, 1),
        updated_at=datetime(2026, 1, 1),
    )
    emp3.venue_id = "venue-1"  # Assigned to venue 1 only

    memory_store.save_employee(emp1)
    memory_store.save_employee(emp2)
    memory_store.save_employee(emp3)

    # Rosters
    roster1 = Roster(
        id="roster-1",
        venue_id="venue-1",
        week_start=date(2026, 4, 20),
        week_end=date(2026, 4, 26),
        shifts=[
            Shift(
                id="shift-1",
                employee_id="emp-1",
                date=date(2026, 4, 20),
                start_time="09:00",
                end_time="17:00",
                break_minutes=30,
                status=ShiftStatus.scheduled,
                role="manager",
                cost=Decimal("204.00"),
            ),
        ],
        total_cost=Decimal("204.00"),
        created_at=datetime(2026, 1, 1),
    )

    roster2 = Roster(
        id="roster-2",
        venue_id="venue-2",
        week_start=date(2026, 4, 20),
        week_end=date(2026, 4, 26),
        shifts=[
            Shift(
                id="shift-2",
                employee_id="emp-2",
                date=date(2026, 4, 20),
                start_time="10:00",
                end_time="18:00",
                break_minutes=30,
                status=ShiftStatus.scheduled,
                role="waiter",
                cost=Decimal("176.00"),
            ),
        ],
        total_cost=Decimal("176.00"),
        created_at=datetime(2026, 1, 1),
    )

    memory_store.save_roster(roster1)
    memory_store.save_roster(roster2)

    # Forecasts
    fc1 = DemandForecast(
        id="fc-1",
        venue_id="venue-1",
        date=date(2026, 4, 20),
        hour=12,
        predicted_covers=45.0,
        confidence=0.92,
        signals_used=["historical", "weather"],
        model_version="v1.0",
    )

    fc2 = DemandForecast(
        id="fc-2",
        venue_id="venue-2",
        date=date(2026, 4, 20),
        hour=12,
        predicted_covers=30.0,
        confidence=0.88,
        signals_used=["historical", "events"],
        model_version="v1.0",
    )

    memory_store.add_forecasts([fc1, fc2])

    return {
        "venues": {"v1": venue1, "v2": venue2},
        "employees": {"emp1": emp1, "emp2": emp2, "emp3": emp3},
        "rosters": {"r1": roster1, "r2": roster2},
        "forecasts": {"fc1": fc1, "fc2": fc2},
    }


# ============================================================================
# Tests - Venue Access Validation
# ============================================================================


class TestVenueAccessValidation:
    """Test the validate_venue_access function."""

    def test_owner_always_has_access(self):
        """Owner role should have access to all venues."""
        assert validate_venue_access([], "venue-1", is_owner=True)
        assert validate_venue_access(["venue-1"], "venue-2", is_owner=True)
        assert validate_venue_access(["other"], "any-venue", is_owner=True)

    def test_user_with_venue_access(self):
        """User with venue in their list should have access."""
        assert validate_venue_access(["venue-1", "venue-2"], "venue-1")
        assert validate_venue_access(["venue-1", "venue-2"], "venue-2")

    def test_user_without_venue_access(self):
        """User without venue in their list should not have access."""
        assert not validate_venue_access(["venue-1"], "venue-2")
        assert not validate_venue_access(["venue-2"], "venue-1")
        assert not validate_venue_access([], "venue-1")


# ============================================================================
# Tests - TenantContext
# ============================================================================


class TestTenantContext:
    """Test the TenantContext class."""

    def test_owner_has_access_to_all(self):
        """Owner should have access to any venue."""
        tenant = TenantContext("user-1", ["venue-1"], is_owner=True)
        assert tenant.has_access_to("venue-1")
        assert tenant.has_access_to("venue-2")
        assert tenant.has_access_to("any-venue")

    def test_non_owner_has_limited_access(self):
        """Non-owner should only have access to their venues."""
        tenant = TenantContext("user-2", ["venue-1", "venue-2"], is_owner=False)
        assert tenant.has_access_to("venue-1")
        assert tenant.has_access_to("venue-2")
        assert not tenant.has_access_to("venue-3")

    def test_empty_venue_list(self):
        """User with no venues should have no access."""
        tenant = TenantContext("user-3", [], is_owner=False)
        assert not tenant.has_access_to("venue-1")
        assert not tenant.has_access_to("venue-2")


# ============================================================================
# Tests - TenantScopedDB: Venues
# ============================================================================


class TestTenantScopedDBVenues:
    """Test venue operations in TenantScopedDB."""

    def test_get_venue_without_tenant_context(self, scoped_db, setup_test_data):
        """
        Without tenant context, TenantScopedDB treats the call as auth-exempt
        (the path health/metrics/etc take) and does not crash.

        Previously this asserted AttributeError, which was an artifact of the
        thread-local implementation raising when `.value` was unset — not real
        access control. With the async-safe ContextVar, no context cleanly
        resolves to None. Real unauthenticated data requests never reach this
        layer: TenantMiddleware now rejects them with 401 first.
        """
        # No tenant context set — must not raise.
        assert get_tenant_context_optional() is None
        result = scoped_db.get_venue("venue-1")
        assert result is not None and result.id == "venue-1"

    def test_list_venues_for_owner(self, scoped_db, setup_test_data):
        """Owner should see all venues."""
        _tenant_context.value = TenantContext("owner-1", [], is_owner=True)
        try:
            venues = scoped_db.list_venues()
            assert len(venues) == 2
            assert any(v.id == "venue-1" for v in venues)
            assert any(v.id == "venue-2" for v in venues)
        finally:
            _tenant_context.value = None

    def test_list_venues_for_manager_single_venue(self, scoped_db, setup_test_data):
        """Manager with one venue should see only that venue."""
        _tenant_context.value = TenantContext("manager-1", ["venue-1"], is_owner=False)
        try:
            venues = scoped_db.list_venues()
            assert len(venues) == 1
            assert venues[0].id == "venue-1"
        finally:
            _tenant_context.value = None

    def test_list_venues_for_manager_multiple_venues(self, scoped_db, setup_test_data):
        """Manager with multiple venues should see only those venues."""
        _tenant_context.value = TenantContext(
            "manager-2", ["venue-1", "venue-2"], is_owner=False
        )
        try:
            venues = scoped_db.list_venues()
            assert len(venues) == 2
        finally:
            _tenant_context.value = None


# ============================================================================
# Tests - TenantScopedDB: Employees
# ============================================================================


class TestTenantScopedDBEmployees:
    """Test employee operations in TenantScopedDB."""

    def test_get_employee_own_venue(self, scoped_db, setup_test_data):
        """User should access employee from their own venue."""
        _tenant_context.value = TenantContext("manager-1", ["venue-1"], is_owner=False)
        try:
            emp = scoped_db.get_employee("emp-1")
            assert emp is not None
            assert emp.name == "Alice Smith"
        finally:
            _tenant_context.value = None

    def test_get_employee_other_venue_blocked(self, scoped_db, setup_test_data):
        """User should not access employee from other venue."""
        _tenant_context.value = TenantContext("manager-1", ["venue-1"], is_owner=False)
        try:
            with pytest.raises(IsolationViolation):
                scoped_db.get_employee("emp-2")  # emp-2 is in venue-2
        finally:
            _tenant_context.value = None

    def test_list_employees_filters_by_venue(self, scoped_db, setup_test_data):
        """list_employees should only return employees from user's venues."""
        _tenant_context.value = TenantContext("manager-1", ["venue-1"], is_owner=False)
        try:
            employees = scoped_db.list_employees()
            employee_ids = [e.id for e in employees]
            assert "emp-1" in employee_ids  # venue-1
            assert "emp-3" in employee_ids  # venue-1
            assert "emp-2" not in employee_ids  # venue-2
        finally:
            _tenant_context.value = None

    def test_save_employee_validates_venue(self, scoped_db, setup_test_data):
        """save_employee should validate venue access."""
        new_emp = Employee(
            id="emp-4",
            tanda_id="tanda-emp-4",
            name="Dave Wilson",
            employment_type=EmploymentType.casual,
            award_level=AwardLevel.level_1,
            state=State.vic,
            hourly_base_rate=Decimal("19.00"),
            phone="0404567890",
            email="dave@example.com",
            skills=[],
            availability={},
            max_hours_per_week=10.0,
            consecutive_days_limit=3,
            created_at=datetime(2026, 1, 1),
            updated_at=datetime(2026, 1, 1),
        )
        new_emp.venue_id = "venue-2"

        _tenant_context.value = TenantContext("manager-1", ["venue-1"], is_owner=False)
        try:
            with pytest.raises(IsolationViolation):
                scoped_db.save_employee(new_emp)
        finally:
            _tenant_context.value = None


# ============================================================================
# Tests - TenantScopedDB: Rosters
# ============================================================================


class TestTenantScopedDBRosters:
    """Test roster operations in TenantScopedDB."""

    def test_get_roster_own_venue(self, scoped_db, setup_test_data):
        """User should access roster from their own venue."""
        _tenant_context.value = TenantContext("manager-1", ["venue-1"], is_owner=False)
        try:
            roster = scoped_db.get_roster("roster-1")
            assert roster is not None
            assert roster.venue_id == "venue-1"
        finally:
            _tenant_context.value = None

    def test_get_roster_other_venue_blocked(self, scoped_db, setup_test_data):
        """User should not access roster from other venue."""
        _tenant_context.value = TenantContext("manager-1", ["venue-1"], is_owner=False)
        try:
            with pytest.raises(IsolationViolation):
                scoped_db.get_roster("roster-2")  # roster-2 is in venue-2
        finally:
            _tenant_context.value = None

    def test_list_rosters_filters_by_venue(self, scoped_db, setup_test_data):
        """list_rosters should only return rosters from user's venues."""
        _tenant_context.value = TenantContext("manager-1", ["venue-1"], is_owner=False)
        try:
            rosters = scoped_db.list_rosters()
            assert len(rosters) == 1
            assert rosters[0].id == "roster-1"
        finally:
            _tenant_context.value = None


# ============================================================================
# Tests - TenantScopedDB: Forecasts
# ============================================================================


class TestTenantScopedDBForecasts:
    """Test forecast operations in TenantScopedDB."""

    def test_get_forecasts_filters_by_venue(self, scoped_db, setup_test_data):
        """get_forecasts should filter by user's venues."""
        _tenant_context.value = TenantContext("manager-1", ["venue-1"], is_owner=False)
        try:
            forecasts = scoped_db.get_forecasts()
            assert len(forecasts) == 1
            assert forecasts[0].id == "fc-1"
            assert forecasts[0].venue_id == "venue-1"
        finally:
            _tenant_context.value = None

    def test_get_forecasts_with_specific_venue(self, scoped_db, setup_test_data):
        """get_forecasts with venue_id should validate access."""
        _tenant_context.value = TenantContext("manager-1", ["venue-1"], is_owner=False)
        try:
            forecasts = scoped_db.get_forecasts(venue_id="venue-1")
            assert len(forecasts) == 1
            assert forecasts[0].venue_id == "venue-1"
        finally:
            _tenant_context.value = None

    def test_get_forecasts_other_venue_blocked(self, scoped_db, setup_test_data):
        """get_forecasts with other venue_id should raise error."""
        _tenant_context.value = TenantContext("manager-1", ["venue-1"], is_owner=False)
        try:
            with pytest.raises(IsolationViolation):
                scoped_db.get_forecasts(venue_id="venue-2")
        finally:
            _tenant_context.value = None


# ============================================================================
# Tests - Cross-Tenant Data Access
# ============================================================================


class TestCrossTenantDataAccess:
    """Integration tests for cross-tenant isolation."""

    def test_user_a_cannot_see_user_b_data(self, scoped_db, setup_test_data):
        """User from venue-1 should not see data from venue-2."""
        # User from venue-1
        _tenant_context.value = TenantContext("user-a", ["venue-1"], is_owner=False)
        try:
            # Can access own data
            employees_a = scoped_db.list_employees()
            assert any(e.id == "emp-1" for e in employees_a)

            # Cannot access other venue data
            with pytest.raises(IsolationViolation):
                scoped_db.get_employee("emp-2")

            # Cannot access other venue rosters
            with pytest.raises(IsolationViolation):
                scoped_db.get_roster("roster-2")

        finally:
            _tenant_context.value = None

    def test_user_b_cannot_see_user_a_data(self, scoped_db, setup_test_data):
        """User from venue-2 should not see data from venue-1."""
        _tenant_context.value = TenantContext("user-b", ["venue-2"], is_owner=False)
        try:
            # Can access own data
            employees_b = scoped_db.list_employees()
            assert any(e.id == "emp-2" for e in employees_b)

            # Cannot access other venue data
            with pytest.raises(IsolationViolation):
                scoped_db.get_employee("emp-1")

            # Cannot access other venue rosters
            with pytest.raises(IsolationViolation):
                scoped_db.get_roster("roster-1")

        finally:
            _tenant_context.value = None

    def test_owner_can_access_all_data(self, scoped_db, setup_test_data):
        """Owner should access data from all venues."""
        _tenant_context.value = TenantContext("owner", [], is_owner=True)
        try:
            # Can see all employees
            employees = scoped_db.list_employees()
            assert len(employees) == 3

            # Can see all rosters
            rosters = scoped_db.list_rosters()
            assert len(rosters) == 2

            # Can see all forecasts
            forecasts = scoped_db.get_forecasts()
            assert len(forecasts) == 2

        finally:
            _tenant_context.value = None


# ============================================================================
# Tests - Audit Logging
# ============================================================================


class TestAuditLogging:
    """Test audit log operations."""

    def test_save_audit_log_validates_venue(self, scoped_db, setup_test_data):
        """Saving audit log should validate venue access."""
        entry = {
            "venue_id": "venue-2",
            "user_id": "manager-1",
            "action": "view",
            "resource_type": "employee",
            "resource_id": "emp-2",
            "details": {},
        }

        _tenant_context.value = TenantContext("manager-1", ["venue-1"], is_owner=False)
        try:
            with pytest.raises(IsolationViolation):
                scoped_db.save_audit_log(entry)
        finally:
            _tenant_context.value = None

    def test_list_audit_logs_validates_venue(self, scoped_db, setup_test_data):
        """Listing audit logs should validate venue access."""
        _tenant_context.value = TenantContext("manager-1", ["venue-1"], is_owner=False)
        try:
            with pytest.raises(IsolationViolation):
                scoped_db.list_audit_logs("venue-2")
        finally:
            _tenant_context.value = None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
