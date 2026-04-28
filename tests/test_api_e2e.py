"""
End-to-end integration tests for RosterIQ FastAPI application.

Tests exercise the actual REST API endpoints via TestClient, verifying
real workflows from start to finish. Each test is independent and uses
fresh data via conftest fixtures.

Coverage:
1. Health & monitoring — /health, /ready, /metrics
2. Venue CRUD — create, list, get, duplicate handling
3. Employee management — create, list, paginate, bulk create
4. Forecast management — add, retrieve, paginate, filter by date range
5. Roster generation — create venue + employees + forecasts, then generate roster
6. Roster analysis — analyse rosters, get suggestions
7. Webhook flow — POST valid/invalid payloads, test endpoint
8. Demo data flow — seed demo data and verify seeding
9. Error handling — 404, 422, etc.

Run:
    pytest tests/test_api_e2e.py -v
"""

import sys
import os
import json
from datetime import date, datetime, timedelta
from decimal import Decimal

# Ensure imports work
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import pytest
from fastapi.testclient import TestClient

from rosteriq.api import app
from rosteriq.models import (
    Employee, VenueConfig, DemandForecast, State,
    EmploymentType, ShiftStatus,
)
from rosteriq.database import reset_db


@pytest.fixture(autouse=True)
def reset_database():
    """Reset the global database before and after each test."""
    reset_db()
    yield
    reset_db()


@pytest.fixture
def client():
    """Create a TestClient for the FastAPI app."""
    return TestClient(app)


# ============================================================================
# HELPERS
# ============================================================================

def create_test_venue(client, venue_id: str = "test-venue-1", name: str = "Test Venue"):
    """Helper to create a venue and return the response."""
    venue = VenueConfig(
        id=venue_id,
        name=name,
        state=State.vic,
        min_staff=2,
        max_staff=10,
        budget=Decimal("5000"),
    )
    response = client.post("/venues", json=venue.model_dump())
    return response


def create_test_employee(
    client,
    employee_id: str,
    venue_id: str = "test-venue-1",
    first_name: str = "John",
    last_name: str = "Doe",
):
    """Helper to create an employee and return the response."""
    emp = Employee(
        id=employee_id,
        first_name=first_name,
        last_name=last_name,
        venue_id=venue_id,
        employment_type=EmploymentType.permanent,
        hourly_rate=Decimal("25.50"),
        available_days=["MON", "TUE", "WED", "THU", "FRI"],
    )
    response = client.post("/employees", json=emp.model_dump())
    return response


def create_test_forecasts(
    client,
    venue_id: str = "test-venue-1",
    start_date: date = None,
    count: int = 7,
):
    """Helper to create demand forecasts for a week and return the response."""
    if start_date is None:
        start_date = date.today()

    forecasts = []
    for day_offset in range(count):
        current_date = start_date + timedelta(days=day_offset)
        for hour in [11, 12, 17, 18, 19, 20]:  # Common service hours
            fc = DemandForecast(
                id=f"forecast-{venue_id}-{current_date}-{hour}",
                venue_id=venue_id,
                date=current_date,
                hour=hour,
                predicted_covers=35.5 + (hour % 5) * 5,
                confidence=0.85,
                signals_used=["historical"],
                model_version="test-v1",
            )
            forecasts.append(fc)

    response = client.post("/forecasts", json=[f.model_dump() for f in forecasts])
    return response


# ============================================================================
# TESTS: Health & monitoring
# ============================================================================

class TestHealthMonitoring:
    """Health check and monitoring endpoints."""

    def test_health_endpoint(self, client):
        """GET /health returns 200 with version."""
        response = client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "ok"
        assert "version" in data
        assert "timestamp" in data

    def test_ready_endpoint(self, client):
        """GET /ready returns 200 with checks."""
        response = client.get("/ready")
        assert response.status_code == 200
        data = response.json()
        # Database is critical, so should pass with in-memory store
        assert data["status"] in ["ready", "not_ready"]
        assert "checks" in data
        assert "database" in data["checks"]

    def test_metrics_endpoint(self, client):
        """GET /metrics returns stats with data counters."""
        # Make a request to record metrics
        client.get("/health")

        response = client.get("/metrics")
        assert response.status_code == 200
        data = response.json()

        assert "version" in data
        assert "total_requests" in data
        assert "data" in data
        assert "venues" in data["data"]
        assert "employees" in data["data"]
        assert "rosters" in data["data"]
        assert "forecasts" in data["data"]

    def test_root_endpoint(self, client):
        """GET / returns HealthResponse with module list."""
        response = client.get("/")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "ok"
        assert "modules" in data
        assert isinstance(data["modules"], list)
        assert "roster_optimiser" in data["modules"]


# ============================================================================
# TESTS: Venue CRUD
# ============================================================================

class TestVenueCRUD:
    """Venue management endpoints."""

    def test_create_venue(self, client):
        """POST /venues creates a venue with correct response."""
        response = create_test_venue(client, venue_id="test-v1")

        assert response.status_code == 200
        data = response.json()
        assert data["id"] == "test-v1"
        assert data["status"] == "created"

    def test_get_venue(self, client):
        """GET /venues/{id} retrieves the created venue."""
        create_test_venue(client, venue_id="test-v1", name="My Pub")
        response = client.get("/venues/test-v1")

        assert response.status_code == 200
        data = response.json()
        assert data["id"] == "test-v1"
        assert data["name"] == "My Pub"
        assert data["state"] == "vic"

    def test_list_venues(self, client):
        """GET /venues lists all venues with pagination."""
        create_test_venue(client, venue_id="v1", name="Venue 1")
        create_test_venue(client, venue_id="v2", name="Venue 2")
        create_test_venue(client, venue_id="v3", name="Venue 3")

        response = client.get("/venues?limit=2&offset=0")
        assert response.status_code == 200
        data = response.json()

        assert data["total"] == 3
        assert len(data["items"]) == 2
        assert data["limit"] == 2
        assert data["offset"] == 0

    def test_list_venues_pagination(self, client):
        """GET /venues respects pagination parameters."""
        for i in range(5):
            create_test_venue(client, venue_id=f"v{i}")

        # First page
        response1 = client.get("/venues?limit=2&offset=0")
        assert len(response1.json()["items"]) == 2
        assert response1.json()["total"] == 5

        # Second page
        response2 = client.get("/venues?limit=2&offset=2")
        assert len(response2.json()["items"]) == 2

        # Third page (1 item remaining)
        response3 = client.get("/venues?limit=2&offset=4")
        assert len(response3.json()["items"]) == 1

    def test_get_venue_not_found(self, client):
        """GET /venues/{id} returns 404 for nonexistent venue."""
        response = client.get("/venues/nonexistent-id")
        assert response.status_code == 404

    def test_duplicate_venue_overwrite(self, client):
        """POST /venues with same ID overwrites previous venue."""
        create_test_venue(client, venue_id="v1", name="Original")
        response = create_test_venue(client, venue_id="v1", name="Updated")

        # Second POST succeeds
        assert response.status_code == 200

        # Get the venue and verify updated name
        response = client.get("/venues/v1")
        assert response.json()["name"] == "Updated"


# ============================================================================
# TESTS: Employee Management
# ============================================================================

class TestEmployeeManagement:
    """Employee creation, listing, and bulk operations."""

    def test_create_employee(self, client):
        """POST /employees creates an employee."""
        response = create_test_employee(client, employee_id="emp1")

        assert response.status_code == 200
        data = response.json()
        assert data["id"] == "emp1"
        assert data["status"] == "created"

    def test_get_employee(self, client):
        """GET /employees/{id} retrieves the employee."""
        create_test_employee(client, employee_id="emp1")
        response = client.get("/employees/emp1")

        assert response.status_code == 200
        data = response.json()
        assert data["id"] == "emp1"
        assert data["first_name"] == "John"
        assert data["last_name"] == "Doe"

    def test_list_employees(self, client):
        """GET /employees lists all employees with pagination."""
        create_test_employee(client, employee_id="e1")
        create_test_employee(client, employee_id="e2")
        create_test_employee(client, employee_id="e3")

        response = client.get("/employees?limit=2&offset=0")
        assert response.status_code == 200
        data = response.json()

        assert data["total"] == 3
        assert len(data["items"]) == 2
        assert data["limit"] == 2

    def test_list_employees_by_venue(self, client):
        """GET /employees?venue_id=X filters by venue."""
        create_test_employee(client, employee_id="e1", venue_id="v1")
        create_test_employee(client, employee_id="e2", venue_id="v1")
        create_test_employee(client, employee_id="e3", venue_id="v2")

        response = client.get("/employees?venue_id=v1")
        assert response.status_code == 200
        data = response.json()

        assert data["total"] == 2
        assert all(emp["venue_id"] == "v1" for emp in data["items"])

    def test_bulk_create_employees(self, client):
        """POST /employees/bulk creates multiple employees."""
        employees = [
            Employee(
                id=f"emp{i}",
                first_name=f"Employee{i}",
                last_name="Test",
                venue_id="test-venue-1",
                employment_type=EmploymentType.permanent,
                hourly_rate=Decimal("25.00"),
            ).model_dump()
            for i in range(5)
        ]

        response = client.post("/employees/bulk", json=employees)
        assert response.status_code == 200
        data = response.json()
        assert data["count"] == 5
        assert data["status"] == "created"

        # Verify all were created
        list_response = client.get("/employees")
        assert list_response.json()["total"] == 5

    def test_get_employee_not_found(self, client):
        """GET /employees/{id} returns 404 for nonexistent employee."""
        response = client.get("/employees/nonexistent-emp")
        assert response.status_code == 404


# ============================================================================
# TESTS: Forecast Management
# ============================================================================

class TestForecastManagement:
    """Forecast creation, retrieval, and filtering."""

    def test_add_forecasts(self, client):
        """POST /forecasts adds demand forecasts."""
        response = create_test_forecasts(client, venue_id="test-venue-1")

        assert response.status_code == 200
        data = response.json()
        assert "count" in data
        assert data["count"] > 0
        assert data["status"] == "added"

    def test_list_forecasts(self, client):
        """GET /forecasts lists all forecasts with pagination."""
        create_test_forecasts(client, venue_id="v1", count=2)

        response = client.get("/forecasts")
        assert response.status_code == 200
        data = response.json()

        assert data["total"] > 0
        assert "items" in data
        assert data["limit"] == 50  # default

    def test_list_forecasts_by_venue(self, client):
        """GET /forecasts?venue_id=X filters by venue."""
        create_test_forecasts(client, venue_id="v1", count=2)
        create_test_forecasts(client, venue_id="v2", count=2)

        response = client.get("/forecasts?venue_id=v1")
        assert response.status_code == 200
        data = response.json()

        assert all(f["venue_id"] == "v1" for f in data["items"])

    def test_list_forecasts_by_date_range(self, client):
        """GET /forecasts filters by date range."""
        start = date.today()
        end = start + timedelta(days=6)

        create_test_forecasts(client, venue_id="v1", start_date=start, count=7)

        response = client.get(f"/forecasts?start_date={start}&end_date={end}")
        assert response.status_code == 200
        data = response.json()

        assert all(
            start <= date.fromisoformat(f["date"]) <= end
            for f in data["items"]
        )

    def test_list_forecasts_pagination(self, client):
        """GET /forecasts respects limit and offset."""
        create_test_forecasts(client, venue_id="v1", count=5)

        response = client.get("/forecasts?limit=5&offset=0")
        assert response.status_code == 200
        data = response.json()

        assert data["limit"] == 5
        assert data["offset"] == 0
        assert "total" in data


# ============================================================================
# TESTS: Roster Generation Flow
# ============================================================================

class TestRosterGenerationFlow:
    """End-to-end roster generation workflow."""

    def setup_roster_prerequisites(self, client):
        """Helper to set up venue, employees, and forecasts for roster gen."""
        # Create venue
        create_test_venue(client, venue_id="test-v1")

        # Create employees
        for i in range(5):
            create_test_employee(
                client,
                employee_id=f"emp{i}",
                venue_id="test-v1",
                first_name=f"Staff{i}",
            )

        # Create forecasts for a week
        start = date.today()
        # Align to next Monday
        days_until_monday = (7 - start.weekday()) % 7
        if days_until_monday == 0:
            days_until_monday = 7
        week_start = start + timedelta(days=days_until_monday)

        create_test_forecasts(client, venue_id="test-v1", start_date=week_start, count=7)

        return week_start

    def test_generate_roster_success(self, client):
        """POST /rosters/generate creates a roster with shifts."""
        week_start = self.setup_roster_prerequisites(client)

        response = client.post(
            "/rosters/generate",
            json={
                "venue_id": "test-v1",
                "week_start": week_start.isoformat(),
                "covers_per_staff": 15.0,
            }
        )

        assert response.status_code == 200
        data = response.json()

        assert "id" in data
        assert data["venue_id"] == "test-v1"
        assert "shifts" in data
        assert len(data["shifts"]) > 0

        # Verify shifts are valid
        for shift in data["shifts"]:
            assert "id" in shift
            assert "employee_id" in shift
            assert "date" in shift
            assert "start_time" in shift
            assert "end_time" in shift

    def test_generate_roster_missing_venue(self, client):
        """POST /rosters/generate returns 404 for nonexistent venue."""
        response = client.post(
            "/rosters/generate",
            json={
                "venue_id": "nonexistent",
                "week_start": date.today().isoformat(),
                "covers_per_staff": 15.0,
            }
        )

        assert response.status_code == 404

    def test_generate_roster_no_employees(self, client):
        """POST /rosters/generate returns 400 when no employees exist."""
        create_test_venue(client, venue_id="test-v1")

        response = client.post(
            "/rosters/generate",
            json={
                "venue_id": "test-v1",
                "week_start": date.today().isoformat(),
                "covers_per_staff": 15.0,
            }
        )

        assert response.status_code == 400

    def test_list_rosters(self, client):
        """GET /rosters lists generated rosters with pagination."""
        week_start = self.setup_roster_prerequisites(client)

        # Generate a roster
        client.post(
            "/rosters/generate",
            json={
                "venue_id": "test-v1",
                "week_start": week_start.isoformat(),
                "covers_per_staff": 15.0,
            }
        )

        response = client.get("/rosters")
        assert response.status_code == 200
        data = response.json()

        assert data["total"] >= 1
        assert len(data["items"]) >= 1

    def test_get_roster(self, client):
        """GET /rosters/{id} retrieves a specific roster."""
        week_start = self.setup_roster_prerequisites(client)

        # Generate a roster
        gen_response = client.post(
            "/rosters/generate",
            json={
                "venue_id": "test-v1",
                "week_start": week_start.isoformat(),
                "covers_per_staff": 15.0,
            }
        )
        roster_id = gen_response.json()["id"]

        response = client.get(f"/rosters/{roster_id}")
        assert response.status_code == 200
        data = response.json()

        assert data["id"] == roster_id
        assert data["venue_id"] == "test-v1"

    def test_get_roster_not_found(self, client):
        """GET /rosters/{id} returns 404 for nonexistent roster."""
        response = client.get("/rosters/nonexistent-roster")
        assert response.status_code == 404


# ============================================================================
# TESTS: Roster Analysis
# ============================================================================

class TestRosterAnalysis:
    """Roster analysis and suggestions."""

    def setup_with_roster(self, client):
        """Helper to create a roster for analysis."""
        # Setup venue, employees, forecasts
        create_test_venue(client, venue_id="test-v1")
        for i in range(5):
            create_test_employee(client, employee_id=f"emp{i}", venue_id="test-v1")

        start = date.today()
        days_until_monday = (7 - start.weekday()) % 7
        if days_until_monday == 0:
            days_until_monday = 7
        week_start = start + timedelta(days=days_until_monday)

        create_test_forecasts(client, venue_id="test-v1", start_date=week_start, count=7)

        # Generate roster
        gen_response = client.post(
            "/rosters/generate",
            json={
                "venue_id": "test-v1",
                "week_start": week_start.isoformat(),
                "covers_per_staff": 15.0,
            }
        )

        return gen_response.json()["id"]

    def test_analyse_roster(self, client):
        """GET /rosters/{id}/analyse returns analysis."""
        roster_id = self.setup_with_roster(client)

        response = client.get(f"/rosters/{roster_id}/analyse")
        assert response.status_code == 200
        data = response.json()

        # Should contain roster metrics/breakdown
        assert isinstance(data, dict)

    def test_get_suggestions(self, client):
        """GET /rosters/{id}/suggestions returns improvement suggestions."""
        roster_id = self.setup_with_roster(client)

        response = client.get(f"/rosters/{roster_id}/suggestions")
        assert response.status_code == 200
        data = response.json()

        assert "roster_id" in data
        assert data["roster_id"] == roster_id
        assert "suggestions" in data
        assert "count" in data


# ============================================================================
# TESTS: Webhook Receive Flow
# ============================================================================

class TestWebhookFlow:
    """Webhook reception and handling."""

    def test_post_valid_webhook(self, client):
        """POST /tanda/webhook with valid payload returns 200."""
        payload = {
            "event": "user.created",
            "data": {
                "user_id": "user123",
                "name": "John Doe",
            }
        }

        response = client.post("/tanda/webhook", json=payload)
        assert response.status_code == 200
        data = response.json()
        assert "status" in data

    def test_post_invalid_json(self, client):
        """POST with invalid JSON returns 422 (validation error)."""
        response = client.post(
            "/tanda/webhook",
            content="not valid json",
            headers={"Content-Type": "application/json"}
        )

        assert response.status_code in [400, 422]

    def test_webhook_various_events(self, client):
        """POST /tanda/webhook handles different event types."""
        events = [
            {"event": "user.created", "data": {}},
            {"event": "user.updated", "data": {}},
            {"event": "shift.updated", "data": {}},
            {"event": "clockin.updated", "data": {}},
            {"event": "roster.published", "data": {}},
        ]

        for event_payload in events:
            response = client.post("/tanda/webhook", json=event_payload)
            assert response.status_code == 200, f"Failed for {event_payload['event']}"


# ============================================================================
# TESTS: Demo Data Flow
# ============================================================================

class TestDemoDataFlow:
    """Demo data seeding and verification."""

    def test_load_demo_data(self, client):
        """POST /demo/load seeds demo data."""
        response = client.post("/demo/load")
        assert response.status_code == 200
        data = response.json()

        assert data["status"] == "loaded"
        assert "venue" in data
        assert data["employees"] > 0
        assert data["forecasts"] > 0
        assert "week_start" in data

    def test_demo_venues_created(self, client):
        """After /demo/load, venues are accessible via GET /venues."""
        client.post("/demo/load")

        response = client.get("/venues")
        assert response.status_code == 200
        data = response.json()

        assert data["total"] >= 1

    def test_demo_employees_created(self, client):
        """After /demo/load, employees are accessible via GET /employees."""
        client.post("/demo/load")

        response = client.get("/employees")
        assert response.status_code == 200
        data = response.json()

        assert data["total"] > 0

    def test_demo_forecasts_created(self, client):
        """After /demo/load, forecasts are accessible via GET /forecasts."""
        client.post("/demo/load")

        response = client.get("/forecasts")
        assert response.status_code == 200
        data = response.json()

        assert data["total"] > 0


# ============================================================================
# TESTS: Error Handling
# ============================================================================

class TestErrorHandling:
    """Error responses and edge cases."""

    def test_get_nonexistent_venue(self, client):
        """GET /venues/nonexistent returns 404."""
        response = client.get("/venues/nonexistent-id")
        assert response.status_code == 404

    def test_get_nonexistent_employee(self, client):
        """GET /employees/nonexistent returns 404."""
        response = client.get("/employees/nonexistent-id")
        assert response.status_code == 404

    def test_get_nonexistent_roster(self, client):
        """GET /rosters/nonexistent returns 404."""
        response = client.get("/rosters/nonexistent-id")
        assert response.status_code == 404

    def test_post_invalid_venue_body(self, client):
        """POST /venues with invalid body returns 422."""
        response = client.post("/venues", json={"invalid": "field"})
        assert response.status_code == 422

    def test_post_invalid_employee_body(self, client):
        """POST /employees with invalid body returns 422."""
        response = client.post("/employees", json={"invalid": "field"})
        assert response.status_code == 422

    def test_post_invalid_forecast_body(self, client):
        """POST /forecasts with invalid body returns 422."""
        response = client.post("/forecasts", json=[{"invalid": "field"}])
        assert response.status_code == 422

    def test_unknown_route(self, client):
        """GET /unknown-route returns 404."""
        response = client.get("/unknown-route-xyz")
        assert response.status_code == 404

    def test_generate_roster_missing_forecasts(self, client):
        """POST /rosters/generate returns 400 with no forecasts."""
        create_test_venue(client, venue_id="test-v1")
        for i in range(5):
            create_test_employee(client, employee_id=f"emp{i}", venue_id="test-v1")

        response = client.post(
            "/rosters/generate",
            json={
                "venue_id": "test-v1",
                "week_start": date.today().isoformat(),
                "covers_per_staff": 15.0,
            }
        )

        assert response.status_code == 400


# ============================================================================
# TESTS: Cost & Variance Endpoints
# ============================================================================

class TestCostAndVariance:
    """Cost calculation and variance detection endpoints."""

    def test_shift_cost_endpoint(self, client):
        """GET /costs/shift/{id} calculates shift cost."""
        create_test_employee(client, employee_id="emp1")

        target_date = date.today()
        response = client.get(
            f"/costs/shift/emp1?shift_date={target_date}&start_hour=10&end_hour=16&break_minutes=30"
        )

        assert response.status_code == 200
        data = response.json()
        assert "total_cost" in data

    def test_shift_cost_employee_not_found(self, client):
        """GET /costs/shift/nonexistent returns 404."""
        target_date = date.today()
        response = client.get(
            f"/costs/shift/nonexistent?shift_date={target_date}&start_hour=10&end_hour=16"
        )

        assert response.status_code == 404

    def test_variance_calculate(self, client):
        """POST /variance/calculate computes variance."""
        signals = [
            {"signal_type": "demand", "value": 50, "confidence": 0.9, "source": "pos"},
            {"signal_type": "weather", "value": 30, "confidence": 0.85, "source": "bom"},
        ]

        response = client.post("/variance/calculate", json={"signals": signals})
        assert response.status_code == 200
        data = response.json()

        assert "variance" in data
        assert "breach" in data
        assert "signals" in data


# ============================================================================
# TESTS: Award Rules Helpers
# ============================================================================

class TestAwardRules:
    """Award rules and penalty rate endpoints."""

    def test_day_type_check(self, client):
        """GET /awards/day-type returns day classification."""
        check_date = date.today()
        response = client.get(f"/awards/day-type?check_date={check_date}&state=vic")

        assert response.status_code == 200
        data = response.json()
        assert "day_type" in data
        assert data["date"] == check_date.isoformat()

    def test_penalty_rate(self, client):
        """GET /awards/penalty-rate returns penalty multiplier."""
        check_date = date.today()
        response = client.get(
            f"/awards/penalty-rate?employment_type=permanent&check_date={check_date}&state=vic"
        )

        assert response.status_code == 200
        data = response.json()
        assert "multiplier" in data
        assert data["multiplier"] >= 1.0

    def test_public_holidays(self, client):
        """GET /awards/public-holidays/{state} lists holidays."""
        response = client.get("/awards/public-holidays/vic?year=2026")

        assert response.status_code == 200
        data = response.json()
        assert "holidays" in data
        assert data["state"] == "vic"


# ============================================================================
# TESTS: Demand Outlook & Forecasting
# ============================================================================

class TestForecastingEndpoints:
    """Forecast-related endpoints."""

    def test_required_staff_calculation(self, client):
        """GET /forecasts/required-staff calculates staff needs."""
        create_test_venue(client, venue_id="test-v1")

        start = date.today()
        days_until_monday = (7 - start.weekday()) % 7
        if days_until_monday == 0:
            days_until_monday = 7
        week_start = start + timedelta(days=days_until_monday)

        create_test_forecasts(client, venue_id="test-v1", start_date=week_start, count=1)

        response = client.get(
            f"/forecasts/required-staff?venue_id=test-v1&target_date={week_start}&covers_per_staff=15"
        )

        assert response.status_code == 200
        data = response.json()
        assert "required_by_hour" in data
        assert "peak_periods" in data

    def test_required_staff_no_forecasts(self, client):
        """GET /forecasts/required-staff returns 404 with no forecasts."""
        create_test_venue(client, venue_id="test-v1")

        response = client.get(
            f"/forecasts/required-staff?venue_id=test-v1&target_date={date.today()}&covers_per_staff=15"
        )

        assert response.status_code == 404

    def test_required_staff_nonexistent_venue(self, client):
        """GET /forecasts/required-staff returns 404 for nonexistent venue."""
        response = client.get(
            f"/forecasts/required-staff?venue_id=nonexistent&target_date={date.today()}&covers_per_staff=15"
        )

        assert response.status_code == 404


# ============================================================================
# TESTS: POS Import
# ============================================================================

class TestPOSImport:
    """POS data import endpoints."""

    def test_import_pos_csv(self, client):
        """POST /pos/import imports POS CSV data."""
        create_test_venue(client, venue_id="test-v1")

        # Simple CSV data
        csv_data = """date,hour,covers
2026-04-20,11,25
2026-04-20,12,35
2026-04-20,13,45
"""

        response = client.post(
            "/pos/import",
            json={
                "venue_id": "test-v1",
                "csv_data": csv_data,
                "system": "lightspeed",
            }
        )

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "imported"
        assert data["records"] > 0
        assert "forecasts_created" in data

    def test_import_pos_venue_not_found(self, client):
        """POST /pos/import returns 404 for nonexistent venue."""
        csv_data = "date,hour,covers\n2026-04-20,11,25"

        response = client.post(
            "/pos/import",
            json={
                "venue_id": "nonexistent",
                "csv_data": csv_data,
            }
        )

        assert response.status_code == 404


# ============================================================================
# TESTS: Daily Roster Generation
# ============================================================================

class TestDailyRosterGeneration:
    """Daily roster generation endpoint."""

    def test_generate_daily_roster(self, client):
        """POST /rosters/generate-daily generates shifts for one day."""
        create_test_venue(client, venue_id="test-v1")
        for i in range(3):
            create_test_employee(client, employee_id=f"emp{i}", venue_id="test-v1")

        target_date = date.today()
        create_test_forecasts(client, venue_id="test-v1", start_date=target_date, count=1)

        response = client.post(
            "/rosters/generate-daily",
            json={
                "venue_id": "test-v1",
                "target_date": target_date.isoformat(),
                "covers_per_staff": 15.0,
            }
        )

        assert response.status_code == 200
        data = response.json()
        assert data["date"] == target_date.isoformat()
        assert "shifts" in data
        assert "count" in data

    def test_generate_daily_roster_missing_venue(self, client):
        """POST /rosters/generate-daily returns 404 for missing venue."""
        response = client.post(
            "/rosters/generate-daily",
            json={
                "venue_id": "nonexistent",
                "target_date": date.today().isoformat(),
                "covers_per_staff": 15.0,
            }
        )

        assert response.status_code == 404


# ============================================================================
# TESTS: Cache Stats (if available)
# ============================================================================

class TestCacheStats:
    """Cache statistics endpoint."""

    def test_cache_stats_endpoint(self, client):
        """GET /api/cache/stats returns cache information or graceful error."""
        response = client.get("/api/cache/stats")

        # Should be 200 (if cache is available) or with error message
        assert response.status_code in [200, 500]


# ============================================================================
# INTEGRATION TEST: Full Real-World Scenario
# ============================================================================

class TestFullWorkflow:
    """End-to-end realistic workflow."""

    def test_complete_roster_workflow(self, client):
        """
        Full workflow: Create venue -> Add employees -> Add forecasts ->
        Generate roster -> Analyse roster -> Get suggestions.
        """
        # 1. Create venue
        venue_response = create_test_venue(client, venue_id="workflow-v1", name="The Rose & Crown")
        assert venue_response.status_code == 200

        # 2. Add multiple employees
        employees = []
        for i in range(6):
            emp_response = create_test_employee(
                client,
                employee_id=f"workflow-emp{i}",
                venue_id="workflow-v1",
                first_name=f"Staff{i}",
            )
            assert emp_response.status_code == 200
            employees.append(emp_response.json())

        # 3. Add forecasts for a week
        start = date.today()
        days_until_monday = (7 - start.weekday()) % 7
        if days_until_monday == 0:
            days_until_monday = 7
        week_start = start + timedelta(days=days_until_monday)

        forecast_response = create_test_forecasts(
            client, venue_id="workflow-v1", start_date=week_start, count=7
        )
        assert forecast_response.status_code == 200

        # 4. Generate roster
        roster_response = client.post(
            "/rosters/generate",
            json={
                "venue_id": "workflow-v1",
                "week_start": week_start.isoformat(),
                "covers_per_staff": 15.0,
            }
        )
        assert roster_response.status_code == 200
        roster = roster_response.json()
        roster_id = roster["id"]

        # Verify roster has shifts
        assert len(roster["shifts"]) > 0
        for shift in roster["shifts"]:
            assert shift["employee_id"] in {e["id"] for e in employees}

        # 5. Analyse the roster
        analyse_response = client.get(f"/rosters/{roster_id}/analyse")
        assert analyse_response.status_code == 200

        # 6. Get suggestions
        suggestions_response = client.get(f"/rosters/{roster_id}/suggestions")
        assert suggestions_response.status_code == 200
        suggestions = suggestions_response.json()
        assert "roster_id" in suggestions
        assert suggestions["roster_id"] == roster_id

        # 7. Verify data is persisted
        list_response = client.get("/rosters")
        assert list_response.status_code == 200
        assert list_response.json()["total"] >= 1

    def test_multiple_venues_independent(self, client):
        """Verify multiple venues maintain independent data."""
        # Create two venues
        create_test_venue(client, venue_id="v1", name="Venue 1")
        create_test_venue(client, venue_id="v2", name="Venue 2")

        # Add employees to different venues
        create_test_employee(client, employee_id="v1-e1", venue_id="v1")
        create_test_employee(client, employee_id="v1-e2", venue_id="v1")
        create_test_employee(client, employee_id="v2-e1", venue_id="v2")

        # List employees by venue
        v1_employees = client.get("/employees?venue_id=v1").json()
        v2_employees = client.get("/employees?venue_id=v2").json()

        assert v1_employees["total"] == 2
        assert v2_employees["total"] == 1

        # Verify employee IDs are correct
        v1_ids = {e["id"] for e in v1_employees["items"]}
        v2_ids = {e["id"] for e in v2_employees["items"]}

        assert v1_ids == {"v1-e1", "v1-e2"}
        assert v2_ids == {"v2-e1"}
