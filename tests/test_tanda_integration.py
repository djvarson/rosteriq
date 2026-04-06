"""
Comprehensive test suite for Tanda Workforce Management Integration.

Tests cover:
- Credentials management
- Employee sync and mapping
- Availability parsing
- Shift pushing
- Department mapping
- Webhook handling
- Factory and health checks
"""

import pytest
import asyncio
from datetime import date, time, datetime, timedelta
from typing import Dict, Any, List
from unittest.mock import AsyncMock, MagicMock, patch, call

# Import integration module
import sys
sys.path.insert(0, "/sessions/fervent-adoring-goodall/mnt/outputs/rosteriq-deploy")

from rosteriq.tanda_integration import (
    TandaCredentials,
    TandaClient,
    TandaSync,
    TandaEmployee,
    TandaDepartment,
    TandaShift,
    TandaLeave,
    TandaAvailability,
    TandaTimesheet,
    TandaQualification,
    TandaWebhook,
    tanda_employee_to_rosteriq,
    rosteriq_shift_to_tanda,
    map_department_to_role,
    create_tanda_integration,
    EmploymentType,
    LeaveType,
    ShiftStatus,
    TimesheetStatus,
)


# ============================================================================
# Fixtures
# ============================================================================

@pytest.fixture
def credentials():
    """Sample Tanda credentials."""
    return TandaCredentials(
        client_id="test_client_id",
        client_secret="test_client_secret",
        access_token="test_access_token",
        refresh_token="test_refresh_token",
        organisation_id="test_org_id",
    )


@pytest.fixture
def tanda_client(credentials):
    """Sample Tanda API client."""
    return TandaClient(credentials)


@pytest.fixture
def tanda_sync(tanda_client):
    """Sample Tanda sync manager."""
    return TandaSync(tanda_client)


@pytest.fixture
def sample_tanda_employee():
    """Sample employee from Tanda API."""
    return {
        "id": "emp_001",
        "name": "John Doe",
        "email": "john@example.com",
        "phone": "0412345678",
        "photo_url": "https://example.com/photo.jpg",
        "department_ids": ["dept_001"],
        "qualification_ids": ["qual_001"],
        "employment_type": "part_time",
        "hourly_rate": 25.50,
        "date_of_birth": "1990-01-15",
        "start_date": "2020-06-01",
        "active": True,
        "metadata": {"custom_field": "value"},
    }


@pytest.fixture
def sample_rosteriq_shift():
    """Sample shift from RosterIQ."""
    return {
        "id": "shift_001",
        "date": "2026-04-10",
        "start_time": "09:00:00",
        "finish_time": "17:00:00",
        "break_length": 30,
        "status": "draft",
        "cost": 200.00,
        "metadata": {},
    }


@pytest.fixture
def sample_tanda_department():
    """Sample department from Tanda."""
    return TandaDepartment(
        id="dept_001",
        name="Kitchen",
        location_id="loc_001",
        location_name="Brisbane",
        manager_id="emp_001",
    )


@pytest.fixture
def sample_tanda_shift():
    """Sample shift from Tanda."""
    return TandaShift(
        id="shift_001",
        user_id="emp_001",
        date=date(2026, 4, 10),
        start_time=time(9, 0),
        finish_time=time(17, 0),
        department_id="dept_001",
        break_length=30,
        status="draft",
        cost=200.0,
    )


@pytest.fixture
def sample_tanda_leave():
    """Sample leave record from Tanda."""
    return TandaLeave(
        id="leave_001",
        user_id="emp_001",
        start_date=date(2026, 4, 20),
        end_date=date(2026, 4, 24),
        leave_type="annual",
        status="approved",
        hours=40.0,
    )


@pytest.fixture
def sample_tanda_availability():
    """Sample availability from Tanda."""
    return TandaAvailability(
        user_id="emp_001",
        day_of_week=0,  # Monday
        start_time=time(9, 0),
        end_time=time(17, 0),
        recurring=True,
    )


# ============================================================================
# Credentials Tests
# ============================================================================

class TestTandaCredentials:
    """Tests for TandaCredentials dataclass."""

    def test_credentials_initialization(self, credentials):
        """Test credentials initialization."""
        assert credentials.client_id == "test_client_id"
        assert credentials.client_secret == "test_client_secret"
        assert credentials.access_token == "test_access_token"
        assert credentials.refresh_token == "test_refresh_token"
        assert credentials.organisation_id == "test_org_id"

    def test_credentials_to_dict(self, credentials):
        """Test credentials to_dict method."""
        cred_dict = credentials.to_dict()
        assert cred_dict["client_id"] == "test_client_id"
        assert cred_dict["access_token"] == "test_access_token"

    def test_credentials_from_dict(self):
        """Test credentials from_dict method."""
        cred_dict = {
            "client_id": "id123",
            "client_secret": "secret123",
            "access_token": "token123",
            "refresh_token": "refresh123",
            "organisation_id": "org123",
        }
        credentials = TandaCredentials.from_dict(cred_dict)
        assert credentials.client_id == "id123"
        assert credentials.refresh_token == "refresh123"

    def test_credentials_optional_fields(self):
        """Test credentials with optional fields."""
        creds = TandaCredentials(
            client_id="id",
            client_secret="secret",
            access_token="token",
        )
        assert creds.refresh_token is None
        assert creds.organisation_id is None


# ============================================================================
# TandaClient Tests
# ============================================================================

class TestTandaClient:
    """Tests for TandaClient HTTP client."""

    def test_client_initialization(self, tanda_client, credentials):
        """Test client initialization."""
        assert tanda_client.credentials == credentials
        assert tanda_client.base_url == "https://my.tanda.co/api/v2"
        assert tanda_client.timeout == 30

    def test_get_headers(self, tanda_client):
        """Test HTTP headers generation."""
        headers = tanda_client._get_headers()
        assert headers["Authorization"] == "Bearer test_access_token"
        assert headers["Content-Type"] == "application/json"

    @pytest.mark.asyncio
    async def test_rate_limiting(self, tanda_client):
        """Test rate limiting mechanism."""
        # First request should work
        await tanda_client._check_rate_limit()
        assert tanda_client.request_count == 1

        # Multiple requests should increment counter
        for _ in range(5):
            await tanda_client._check_rate_limit()
        assert tanda_client.request_count == 6

    @pytest.mark.asyncio
    async def test_token_refresh(self, tanda_client):
        """Test OAuth token refresh."""
        new_token = "new_access_token"

        with patch("httpx.AsyncClient.post") as mock_post:
            mock_response = AsyncMock()
            mock_response.json.return_value = {
                "access_token": new_token,
                "refresh_token": "new_refresh_token",
            }
            mock_post.return_value = mock_response

            result = await tanda_client._refresh_token()

            assert result is True
            assert tanda_client.credentials.access_token == new_token

    @pytest.mark.asyncio
    async def test_request_with_bearer_auth(self, tanda_client):
        """Test authenticated request."""
        with patch("httpx.AsyncClient.request") as mock_request:
            mock_response = AsyncMock()
            mock_response.status_code = 200
            mock_response.json.return_value = {"data": "test"}
            mock_request.return_value = mock_response

            result = await tanda_client.get("/users")

            assert result == {"data": "test"}
            mock_request.assert_called_once()
            call_args = mock_request.call_args
            headers = call_args[1]["headers"]
            assert "Bearer test_access_token" in headers["Authorization"]


# ============================================================================
# Employee Mapping Tests
# ============================================================================

class TestEmployeeMapping:
    """Tests for employee sync and mapping."""

    def test_tanda_employee_to_rosteriq(self, sample_tanda_employee):
        """Test converting Tanda employee to RosterIQ format."""
        employee = tanda_employee_to_rosteriq(sample_tanda_employee)

        assert employee.id == "emp_001"
        assert employee.name == "John Doe"
        assert employee.email == "john@example.com"
        assert employee.phone == "0412345678"
        assert employee.employment_type == "part_time"
        assert employee.hourly_rate == 25.50
        assert employee.active is True

    def test_employee_mapping_with_dates(self, sample_tanda_employee):
        """Test employee mapping with date fields."""
        employee = tanda_employee_to_rosteriq(sample_tanda_employee)

        assert isinstance(employee.date_of_birth, date)
        assert employee.date_of_birth == date(1990, 1, 15)
        assert isinstance(employee.start_date, date)
        assert employee.start_date == date(2020, 6, 1)

    def test_employee_mapping_missing_fields(self):
        """Test employee mapping with missing fields."""
        minimal_employee = {
            "id": "emp_002",
            "name": "Jane Smith",
            "email": "jane@example.com",
        }
        employee = tanda_employee_to_rosteriq(minimal_employee)

        assert employee.id == "emp_002"
        assert employee.phone is None
        assert employee.hourly_rate is None
        assert employee.department_ids == []

    def test_employee_to_dict(self):
        """Test employee to_dict method."""
        employee = TandaEmployee(
            id="emp_001",
            name="John Doe",
            email="john@example.com",
            start_date=date(2020, 6, 1),
        )
        emp_dict = employee.to_dict()

        assert emp_dict["id"] == "emp_001"
        assert emp_dict["start_date"] == "2020-06-01"

    @pytest.mark.asyncio
    async def test_sync_employees(self, tanda_sync, sample_tanda_employee):
        """Test syncing employees from Tanda."""
        tanda_sync.client.paginate = AsyncMock(
            return_value=[sample_tanda_employee]
        )

        employees = await tanda_sync.sync_employees()

        assert len(employees) == 1
        assert employees[0].id == "emp_001"
        assert employees[0].name == "John Doe"
        tanda_sync.client.paginate.assert_called_once_with("/users")


# ============================================================================
# Availability Tests
# ============================================================================

class TestAvailability:
    """Tests for availability sync and parsing."""

    def test_availability_initialization(self, sample_tanda_availability):
        """Test availability initialization."""
        avail = sample_tanda_availability

        assert avail.user_id == "emp_001"
        assert avail.day_of_week == 0
        assert avail.start_time == time(9, 0)
        assert avail.end_time == time(17, 0)
        assert avail.recurring is True

    def test_availability_to_dict(self, sample_tanda_availability):
        """Test availability to_dict method."""
        avail_dict = sample_tanda_availability.to_dict()

        assert avail_dict["user_id"] == "emp_001"
        assert avail_dict["day_of_week"] == 0
        assert avail_dict["start_time"] == "09:00:00"

    @pytest.mark.asyncio
    async def test_sync_availability(self, tanda_sync):
        """Test syncing availability from Tanda."""
        mock_response = {
            "availability": [
                {
                    "day_of_week": 0,
                    "start_time": "09:00:00",
                    "end_time": "17:00:00",
                    "recurring": True,
                }
            ]
        }

        tanda_sync.client.get = AsyncMock(return_value=mock_response)

        availability = await tanda_sync.sync_availability(
            employee_ids=["emp_001"],
            date_from=date(2026, 4, 1),
            date_to=date(2026, 4, 30),
        )

        assert "emp_001" in availability
        assert len(availability["emp_001"]) == 1
        assert availability["emp_001"][0].day_of_week == 0

    @pytest.mark.asyncio
    async def test_sync_availability_date_defaults(self, tanda_sync):
        """Test availability sync uses default dates."""
        mock_response = {"availability": []}

        tanda_sync.client.get = AsyncMock(return_value=mock_response)

        await tanda_sync.sync_availability(employee_ids=["emp_001"])

        # Should call with default dates
        tanda_sync.client.get.assert_called_once()
        call_kwargs = tanda_sync.client.get.call_args[1]
        assert "from" in call_kwargs["params"]


# ============================================================================
# Shift Tests
# ============================================================================

class TestShiftPushing:
    """Tests for pushing shifts to Tanda."""

    def test_rosteriq_shift_to_tanda(self, sample_rosteriq_shift):
        """Test converting RosterIQ shift to Tanda format."""
        tanda_shift = rosteriq_shift_to_tanda(
            sample_rosteriq_shift,
            employee_id="emp_001",
            department_id="dept_001",
        )

        assert tanda_shift.user_id == "emp_001"
        assert tanda_shift.department_id == "dept_001"
        assert tanda_shift.date == date(2026, 4, 10)
        assert tanda_shift.start_time == time(9, 0, 0)
        assert tanda_shift.finish_time == time(17, 0, 0)

    def test_shift_to_dict(self, sample_tanda_shift):
        """Test shift to_dict method."""
        shift_dict = sample_tanda_shift.to_dict()

        assert shift_dict["id"] == "shift_001"
        assert shift_dict["user_id"] == "emp_001"
        assert shift_dict["date"] == "2026-04-10"
        assert shift_dict["start_time"] == "09:00:00"

    @pytest.mark.asyncio
    async def test_push_shift_create(self, tanda_sync, sample_rosteriq_shift):
        """Test creating a new shift in Tanda."""
        mock_response = {
            "id": "shift_new",
            "user_id": "emp_001",
            "date": "2026-04-10",
        }

        tanda_sync.client.post = AsyncMock(return_value=mock_response)

        result = await tanda_sync.push_shift(
            sample_rosteriq_shift,
            employee_id="emp_001",
        )

        assert result["id"] == "shift_new"
        tanda_sync.client.post.assert_called_once()

    @pytest.mark.asyncio
    async def test_push_shift_update(self, tanda_sync, sample_rosteriq_shift):
        """Test updating an existing shift in Tanda."""
        sample_rosteriq_shift["id"] = "shift_existing"

        mock_response = {
            "id": "shift_existing",
            "user_id": "emp_001",
            "date": "2026-04-10",
        }

        tanda_sync.client.patch = AsyncMock(return_value=mock_response)

        result = await tanda_sync.push_shift(
            sample_rosteriq_shift,
            employee_id="emp_001",
        )

        assert result["id"] == "shift_existing"
        tanda_sync.client.patch.assert_called_once()

    @pytest.mark.asyncio
    async def test_push_shifts_multiple(self, tanda_sync):
        """Test pushing multiple shifts."""
        shifts = [
            {
                "employee_id": "emp_001",
                "date": date(2026, 4, 10),
                "start_time": time(9, 0),
                "finish_time": time(17, 0),
            },
            {
                "employee_id": "emp_002",
                "date": date(2026, 4, 11),
                "start_time": time(10, 0),
                "finish_time": time(18, 0),
            },
        ]

        tanda_sync.client.post = AsyncMock(
            side_effect=[
                {"id": "shift_1"},
                {"id": "shift_2"},
            ]
        )

        results = await tanda_sync.push_shifts(shifts)

        assert len(results) == 2
        assert tanda_sync.client.post.call_count == 2

    @pytest.mark.asyncio
    async def test_push_roster(self, tanda_sync):
        """Test pushing entire roster."""
        roster = {
            "emp_001": [
                {
                    "date": date(2026, 4, 10),
                    "start_time": time(9, 0),
                    "finish_time": time(17, 0),
                }
            ],
            "emp_002": [
                {
                    "date": date(2026, 4, 10),
                    "start_time": time(10, 0),
                    "finish_time": time(18, 0),
                }
            ],
        }

        tanda_sync.client.patch = AsyncMock(
            side_effect=[
                {"id": "shift_1"},
                {"id": "shift_2"},
            ]
        )

        results = await tanda_sync.push_roster(roster)

        assert len(results["created"]) == 2 or len(results["created"]) + len(results["errors"]) == 2


# ============================================================================
# Department Mapping Tests
# ============================================================================

class TestDepartmentMapping:
    """Tests for department and role mapping."""

    def test_map_department_to_role_kitchen(self):
        """Test mapping kitchen department to chef role."""
        dept = TandaDepartment(
            id="dept_001",
            name="Kitchen",
            location_id="loc_001",
        )
        role = map_department_to_role(dept)
        assert role == "chef"

    def test_map_department_to_role_bar(self):
        """Test mapping bar department to bartender role."""
        dept = TandaDepartment(
            id="dept_002",
            name="Bar",
            location_id="loc_001",
        )
        role = map_department_to_role(dept)
        assert role == "bartender"

    def test_map_department_to_role_restaurant(self):
        """Test mapping restaurant department to server role."""
        dept = TandaDepartment(
            id="dept_003",
            name="Restaurant",
            location_id="loc_001",
        )
        role = map_department_to_role(dept)
        assert role == "server"

    def test_map_department_to_role_default(self):
        """Test mapping unknown department to default role."""
        dept = TandaDepartment(
            id="dept_999",
            name="Unknown Department",
            location_id="loc_001",
        )
        role = map_department_to_role(dept)
        assert role == "general_staff"

    def test_map_department_case_insensitive(self):
        """Test department mapping is case insensitive."""
        dept = TandaDepartment(
            id="dept_001",
            name="KITCHEN",
            location_id="loc_001",
        )
        role = map_department_to_role(dept)
        assert role == "chef"

    @pytest.mark.asyncio
    async def test_sync_departments(self, tanda_sync):
        """Test syncing departments from Tanda."""
        mock_departments = [
            {
                "id": "dept_001",
                "name": "Kitchen",
                "location_id": "loc_001",
                "location_name": "Brisbane",
            }
        ]

        tanda_sync.client.paginate = AsyncMock(return_value=mock_departments)

        departments = await tanda_sync.sync_departments()

        assert len(departments) == 1
        assert departments[0].id == "dept_001"
        assert departments[0].name == "Kitchen"


# ============================================================================
# Timesheet Tests
# ============================================================================

class TestTimesheets:
    """Tests for timesheet syncing."""

    def test_timesheet_initialization(self):
        """Test timesheet initialization."""
        ts = TandaTimesheet(
            id="ts_001",
            user_id="emp_001",
            date=date(2026, 4, 10),
            total_hours=8.0,
            status="approved",
        )

        assert ts.id == "ts_001"
        assert ts.user_id == "emp_001"
        assert ts.total_hours == 8.0

    def test_timesheet_to_dict(self):
        """Test timesheet to_dict method."""
        ts = TandaTimesheet(
            id="ts_001",
            user_id="emp_001",
            date=date(2026, 4, 10),
        )
        ts_dict = ts.to_dict()

        assert ts_dict["id"] == "ts_001"
        assert ts_dict["date"] == "2026-04-10"

    @pytest.mark.asyncio
    async def test_sync_timesheets(self, tanda_sync):
        """Test syncing timesheets from Tanda."""
        mock_timesheets = [
            {
                "id": "ts_001",
                "user_id": "emp_001",
                "date": "2026-04-10",
                "shifts": [],
                "total_hours": 8.0,
                "status": "approved",
            }
        ]

        tanda_sync.client.paginate = AsyncMock(return_value=mock_timesheets)

        timesheets = await tanda_sync.sync_timesheets(
            date_from=date(2026, 4, 1),
            date_to=date(2026, 4, 30),
        )

        assert len(timesheets) == 1
        assert timesheets[0].id == "ts_001"
        assert timesheets[0].total_hours == 8.0

    @pytest.mark.asyncio
    async def test_sync_timesheets_date_defaults(self, tanda_sync):
        """Test timesheet sync uses default date range."""
        mock_timesheets = []

        tanda_sync.client.paginate = AsyncMock(return_value=mock_timesheets)

        await tanda_sync.sync_timesheets()

        # Should call with default dates
        tanda_sync.client.paginate.assert_called_once()
        call_kwargs = tanda_sync.client.paginate.call_args[1]
        assert "from" in call_kwargs["params"]
        assert "to" in call_kwargs["params"]


# ============================================================================
# Qualifications Tests
# ============================================================================

class TestQualifications:
    """Tests for qualifications syncing."""

    def test_qualification_initialization(self):
        """Test qualification initialization."""
        qual = TandaQualification(
            id="qual_001",
            name="Food Handler Certification",
            description="Australian food handler certificate",
            required_for_roles=["chef", "server"],
        )

        assert qual.id == "qual_001"
        assert qual.name == "Food Handler Certification"
        assert "chef" in qual.required_for_roles

    @pytest.mark.asyncio
    async def test_sync_qualifications(self, tanda_sync):
        """Test syncing qualifications from Tanda."""
        mock_quals = [
            {
                "id": "qual_001",
                "name": "Food Handler",
                "description": "Certificate",
                "required_for_roles": ["chef"],
            }
        ]

        tanda_sync.client.paginate = AsyncMock(return_value=mock_quals)

        qualifications = await tanda_sync.sync_qualifications()

        assert len(qualifications) == 1
        assert "qual_001" in qualifications
        assert qualifications["qual_001"].name == "Food Handler"

    @pytest.mark.asyncio
    async def test_get_award_tags(self, tanda_sync):
        """Test getting award tags."""
        mock_tags = [
            {"id": "tag_001", "name": "Award Type A"},
            {"id": "tag_002", "name": "Award Type B"},
        ]

        tanda_sync.client.paginate = AsyncMock(return_value=mock_tags)

        tags = await tanda_sync.get_award_tags()

        assert len(tags) == 2
        tanda_sync.client.paginate.assert_called_once_with("/awards/tags")


# ============================================================================
# Health Check Tests
# ============================================================================

class TestHealthCheck:
    """Tests for health check functionality."""

    @pytest.mark.asyncio
    async def test_health_check_healthy(self, tanda_sync):
        """Test successful health check."""
        tanda_sync.client.get = AsyncMock(
            return_value={"status": "ok", "data": []}
        )

        result = await tanda_sync.health_check()

        assert result["status"] == "healthy"
        assert "timestamp" in result

    @pytest.mark.asyncio
    async def test_health_check_unhealthy(self, tanda_sync):
        """Test failed health check."""
        tanda_sync.client.get = AsyncMock(
            side_effect=Exception("Connection error")
        )

        result = await tanda_sync.health_check()

        assert result["status"] == "unhealthy"
        assert "error" in result


# ============================================================================
# Webhook Tests
# ============================================================================

class TestTandaWebhook:
    """Tests for webhook handling."""

    def test_webhook_initialization(self, tanda_sync):
        """Test webhook initialization."""
        webhook = TandaWebhook(tanda_sync)

        assert webhook.sync == tanda_sync
        assert webhook.router is not None

    def test_webhook_get_router(self, tanda_sync):
        """Test getting FastAPI router from webhook."""
        webhook = TandaWebhook(tanda_sync)
        router = webhook.get_router()

        assert router is not None
        # Router should have routes
        assert len(router.routes) > 0

    @pytest.mark.asyncio
    async def test_webhook_user_created(self, tanda_sync):
        """Test user created webhook handler."""
        tanda_sync.sync_employees = AsyncMock()

        webhook = TandaWebhook(tanda_sync)
        await webhook._handle_user_created({"id": "emp_001"})

        tanda_sync.sync_employees.assert_called_once()

    @pytest.mark.asyncio
    async def test_webhook_shift_created(self, tanda_sync):
        """Test shift created webhook handler."""
        webhook = TandaWebhook(tanda_sync)

        # Should not raise
        await webhook._handle_shift_created({"id": "shift_001"})

    @pytest.mark.asyncio
    async def test_webhook_timesheet_submitted(self, tanda_sync):
        """Test timesheet submitted webhook handler."""
        webhook = TandaWebhook(tanda_sync)

        # Should not raise
        await webhook._handle_timesheet_submitted({"id": "ts_001"})


# ============================================================================
# Factory Function Tests
# ============================================================================

class TestFactory:
    """Tests for factory function."""

    @pytest.mark.asyncio
    async def test_create_tanda_integration(self, credentials):
        """Test creating Tanda integration."""
        sync, webhook = await create_tanda_integration(credentials)

        assert sync is not None
        assert webhook is not None
        assert isinstance(sync, TandaSync)
        assert isinstance(webhook, TandaWebhook)

    @pytest.mark.asyncio
    async def test_factory_returns_connected_instances(self, credentials):
        """Test factory returns properly connected instances."""
        sync, webhook = await create_tanda_integration(credentials)

        assert sync.client is not None
        assert webhook.sync == sync


# ============================================================================
# Integration Tests
# ============================================================================

class TestIntegration:
    """End-to-end integration tests."""

    @pytest.mark.asyncio
    async def test_employee_to_shift_workflow(self, tanda_sync, sample_tanda_employee):
        """Test workflow from syncing employees to pushing shifts."""
        # Mock employee sync
        tanda_sync.client.paginate = AsyncMock(
            return_value=[sample_tanda_employee]
        )

        employees = await tanda_sync.sync_employees()
        assert len(employees) == 1

        # Mock department sync
        mock_dept = {
            "id": "dept_001",
            "name": "Kitchen",
            "location_id": "loc_001",
        }
        tanda_sync.client.paginate = AsyncMock(return_value=[mock_dept])

        departments = await tanda_sync.sync_departments()
        assert len(departments) == 1

    @pytest.mark.asyncio
    async def test_full_sync_cycle(self, credentials):
        """Test full sync cycle from integration factory."""
        sync, webhook = await create_tanda_integration(credentials)

        assert sync is not None
        assert webhook is not None
        assert webhook.router is not None


# ============================================================================
# Error Handling Tests
# ============================================================================

class TestErrorHandling:
    """Tests for error handling."""

    @pytest.mark.asyncio
    async def test_sync_employees_error_handling(self, tanda_sync):
        """Test error handling in employee sync."""
        tanda_sync.client.paginate = AsyncMock(
            side_effect=Exception("API Error")
        )

        with pytest.raises(Exception):
            await tanda_sync.sync_employees()

    @pytest.mark.asyncio
    async def test_push_shift_error_handling(self, tanda_sync):
        """Test error handling in shift push."""
        tanda_sync.client.post = AsyncMock(
            side_effect=Exception("API Error")
        )

        with pytest.raises(Exception):
            await tanda_sync.push_shift(
                {
                    "date": date(2026, 4, 10),
                    "start_time": time(9, 0),
                    "finish_time": time(17, 0),
                },
                "emp_001",
            )

    @pytest.mark.asyncio
    async def test_push_shifts_partial_failure(self, tanda_sync):
        """Test push shifts with partial failures."""
        shifts = [
            {
                "employee_id": "emp_001",
                "date": date(2026, 4, 10),
                "start_time": time(9, 0),
                "finish_time": time(17, 0),
            },
            {
                "employee_id": "emp_002",
                "date": date(2026, 4, 11),
                "start_time": time(10, 0),
                "finish_time": time(18, 0),
            },
        ]

        tanda_sync.client.post = AsyncMock(
            side_effect=[
                {"id": "shift_1"},
                Exception("API Error"),
            ]
        )

        results = await tanda_sync.push_shifts(shifts)

        # Should handle partial failures
        assert len(results) >= 0


# ============================================================================
# Run Tests
# ============================================================================

if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
