"""
Comprehensive test suite for HumanForce Workforce Management Adapter.

Tests cover:
- Demo adapter data generation
- HumanForceClient initialization and authentication modes
- Factory integration
- Error handling
"""

import asyncio
import os
import sys
from datetime import date, time, datetime, timedelta
from pathlib import Path
from typing import Dict, Any, Optional, AsyncIterator
from unittest.mock import AsyncMock, MagicMock, patch, Mock

# Add root to path
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT)) if str(ROOT) not in sys.path else None

# Mock httpx before importing modules that depend on it
sys.modules['httpx'] = MagicMock()

from rosteriq.humanforce_adapter import (
    HumanForceAdapter,
    DemoHumanForceAdapter,
)
from rosteriq.humanforce_integration import (
    HumanForceClient,
    HumanForceCredentials,
    HumanForceAPIError,
)
from rosteriq.scheduling_platform_factory import get_scheduling_adapter
from rosteriq.tanda_adapter import Employee, Shift, Leave, Availability


# ============================================================================
# Test: Demo HumanForce Adapter
# ============================================================================

def test_demo_humanforce_adapter_generates_employees():
    """Demo adapter should generate 45 employees for Sovereign Hotel Group."""
    adapter = DemoHumanForceAdapter()
    assert len(adapter._employees) == 45

    for emp in adapter._employees:
        assert emp.id.startswith("humanforce_")
        assert len(emp.name) > 0
        assert emp.employment_type in ("full_time", "part_time", "casual")
        assert emp.hourly_rate > 0
        assert "Sovereign" in emp.department_name


def test_demo_humanforce_adapter_venue_name():
    """Demo adapter should use Sovereign Hotel Group as venue persona."""
    adapter = DemoHumanForceAdapter()

    # Check that all employees have Sovereign in their department name
    sovereign_count = sum(
        1 for emp in adapter._employees
        if "Sovereign" in emp.department_name
    )
    assert sovereign_count == 45


def test_demo_humanforce_adapter_multi_location():
    """Demo adapter should distribute employees across 3 locations."""
    adapter = DemoHumanForceAdapter()

    locations = set(emp.department_id for emp in adapter._employees)
    assert len(locations) == 3

    # Each location should have ~15 employees
    for loc_id in locations:
        count = sum(1 for emp in adapter._employees if emp.department_id == loc_id)
        assert 14 <= count <= 16


def test_demo_humanforce_adapter_employment_mix():
    """Demo adapter should have realistic employment type distribution."""
    adapter = DemoHumanForceAdapter()

    full_time = sum(1 for e in adapter._employees if e.employment_type == "full_time")
    part_time = sum(1 for e in adapter._employees if e.employment_type == "part_time")
    casual = sum(1 for e in adapter._employees if e.employment_type == "casual")

    # Check distribution: ~33% full-time, ~27% part-time, ~40% casual
    assert full_time >= 12  # At least ~26%
    assert part_time >= 10  # At least ~22%
    assert casual >= 15  # At least ~33%
    assert full_time + part_time + casual == 45


def test_demo_humanforce_adapter_role_diversity():
    """Demo adapter should have diverse roles including management and kitchen."""
    adapter = DemoHumanForceAdapter()

    roles = set(emp.role for emp in adapter._employees)

    # Check for key roles
    assert any("Manager" in role for role in roles), "Should have Manager roles"
    assert any("Chef" in role for role in roles), "Should have Chef roles"
    assert any("Bartender" in role for role in roles), "Should have Bartender roles"
    assert any("Server" in role or "Floor" in role for role in roles), "Should have Floor/Server roles"


def test_demo_humanforce_adapter_generates_shifts():
    """Demo adapter should generate realistic shifts."""
    adapter = DemoHumanForceAdapter()
    assert len(adapter._shifts) > 0

    for shift in adapter._shifts:
        assert shift.id.startswith("shift_")
        assert shift.employee_id
        assert isinstance(shift.date, date)
        assert shift.start_time < shift.end_time


def test_demo_humanforce_adapter_generates_leave():
    """Demo adapter should generate leave records."""
    adapter = DemoHumanForceAdapter()
    # May be empty if random doesn't select employees
    for emp_id, leaves in adapter._leave.items():
        for leave in leaves:
            assert leave.id.startswith("leave_")
            assert leave.employee_id == emp_id
            assert leave.start_date <= leave.end_date


async def test_demo_humanforce_adapter_get_employees():
    """Demo adapter should return employees via async method."""
    adapter = DemoHumanForceAdapter()
    employees = await adapter.get_employees("org_123")
    assert len(employees) == 45
    assert all(isinstance(e, Employee) for e in employees)


async def test_demo_humanforce_adapter_get_availability():
    """Demo adapter should return availability."""
    adapter = DemoHumanForceAdapter()
    employees = await adapter.get_employees("org_123")
    emp_ids = [e.id for e in employees[:2]]

    availability = await adapter.get_availability(
        emp_ids,
        (date.today(), date.today() + timedelta(days=14))
    )

    assert len(availability) == 2
    for emp_id in emp_ids:
        assert emp_id in availability
        assert isinstance(availability[emp_id], list)


async def test_demo_humanforce_adapter_get_leave():
    """Demo adapter should return leave records."""
    adapter = DemoHumanForceAdapter()
    employees = await adapter.get_employees("org_123")
    emp_ids = [e.id for e in employees[:2]]

    leave = await adapter.get_leave(
        emp_ids,
        (date.today(), date.today() + timedelta(days=30))
    )

    assert len(leave) == 2
    for emp_id in emp_ids:
        assert emp_id in leave
        assert isinstance(leave[emp_id], list)


async def test_demo_humanforce_adapter_get_shifts():
    """Demo adapter should return shifts."""
    adapter = DemoHumanForceAdapter()
    shifts = await adapter.get_shifts(
        "org_123",
        (date.today(), date.today() + timedelta(days=14))
    )

    assert isinstance(shifts, list)
    if shifts:
        assert all(isinstance(s, Shift) for s in shifts)


async def test_demo_humanforce_adapter_get_timesheets():
    """Demo adapter should return timesheets."""
    adapter = DemoHumanForceAdapter()
    timesheets = await adapter.get_timesheets(
        "org_123",
        (date.today(), date.today() + timedelta(days=14))
    )

    assert isinstance(timesheets, list)
    if timesheets:
        for ts in timesheets:
            assert ts.employee_id
            assert ts.date
            assert ts.hours >= 0


async def test_demo_humanforce_adapter_push_draft_roster():
    """Demo adapter should simulate pushing draft roster."""
    adapter = DemoHumanForceAdapter()

    test_shifts = [
        Shift(
            id="test_shift_1",
            employee_id="humanforce_1",
            date=date.today(),
            start_time=time(9, 0),
            end_time=time(17, 0),
            role="Bartender",
            status="draft",
            break_minutes=30,
        )
    ]

    result = await adapter.push_draft_roster("org_123", test_shifts)
    assert "created" in result
    assert "errors" in result
    assert len(result["created"]) == 1
    assert len(result["errors"]) == 0


async def test_demo_humanforce_adapter_get_forecast_revenue():
    """Demo adapter should return empty list for revenue forecast."""
    adapter = DemoHumanForceAdapter()
    forecast = await adapter.get_forecast_revenue(
        "org_123",
        (date.today(), date.today() + timedelta(days=7))
    )

    assert isinstance(forecast, list)
    assert len(forecast) == 0


async def test_demo_humanforce_adapter_handle_webhook():
    """Demo adapter should handle webhooks."""
    adapter = DemoHumanForceAdapter()
    result = await adapter.handle_webhook("employee.created", {"id": "test"})

    assert result["status"] == "processed"
    assert result["event"] == "employee.created"


# ============================================================================
# Test: HumanForce Client - Initialization and Credentials
# ============================================================================

def test_humanforce_credentials():
    """HumanForceCredentials should handle token expiry."""
    creds = HumanForceCredentials(
        region="apac",
        api_key="test_key",
    )
    assert not creds.is_expired()

    creds_expired = HumanForceCredentials(
        region="apac",
        api_key="test_key",
        expires_at=datetime.now() - timedelta(hours=1),
    )
    assert creds_expired.is_expired()


def test_humanforce_client_initialization_api_key():
    """HumanForceClient should initialize with API key."""
    client = HumanForceClient(
        region="apac",
        api_key="test_api_key_12345",
    )

    assert client.region == "apac"
    assert client.api_key == "test_api_key_12345"
    assert client.base_url == "https://apac.humanforce.com/api/v1"


def test_humanforce_client_initialization_oauth():
    """HumanForceClient should initialize with OAuth credentials."""
    client = HumanForceClient(
        region="apac",
        client_id="test_client_id",
        client_secret="test_client_secret",
    )

    assert client.client_id == "test_client_id"
    assert client.client_secret == "test_client_secret"


def test_humanforce_client_initialization_access_token():
    """HumanForceClient should initialize with pre-set access token."""
    client = HumanForceClient(
        region="apac",
        access_token="preauth_token_xyz",
    )

    assert client.access_token == "preauth_token_xyz"


def test_humanforce_client_get_headers():
    """HumanForceClient should construct correct auth headers."""
    client = HumanForceClient(
        region="apac",
        api_key="mytoken",
    )

    headers = client._get_headers("mytoken")
    assert headers["Authorization"] == "Bearer mytoken"
    assert headers["Content-Type"] == "application/json"


def test_humanforce_client_base_url_regions():
    """HumanForceClient should support different regions."""
    client_apac = HumanForceClient(region="apac", api_key="test")
    assert "apac" in client_apac.base_url

    client_eu = HumanForceClient(region="eu", api_key="test")
    assert "eu" in client_eu.base_url

    client_na = HumanForceClient(region="na", api_key="test")
    assert "na" in client_na.base_url


# ============================================================================
# Test: Factory Integration
# ============================================================================

def test_factory_returns_demo_humanforce_by_default():
    """Factory should return DemoHumanForceAdapter by default (no credentials)."""
    # Clear env vars
    for var in ["HUMANFORCE_API_KEY", "HUMANFORCE_CLIENT_ID", "HUMANFORCE_CLIENT_SECRET"]:
        os.environ.pop(var, None)

    os.environ["ROSTERIQ_PLATFORM"] = "humanforce"

    try:
        adapter = get_scheduling_adapter()
        assert isinstance(adapter, DemoHumanForceAdapter)
    finally:
        os.environ.pop("ROSTERIQ_PLATFORM", None)


def test_factory_returns_demo_humanforce_explicit():
    """Factory should return DemoHumanForceAdapter when explicitly requested."""
    os.environ["ROSTERIQ_PLATFORM"] = "demo_humanforce"

    try:
        adapter = get_scheduling_adapter()
        assert isinstance(adapter, DemoHumanForceAdapter)
    finally:
        os.environ.pop("ROSTERIQ_PLATFORM", None)


def test_factory_returns_real_humanforce_with_api_key():
    """Factory should return HumanForceAdapter when API key is configured."""
    os.environ["ROSTERIQ_PLATFORM"] = "humanforce"
    os.environ["HUMANFORCE_API_KEY"] = "test_api_key"
    os.environ["ROSTERIQ_DATA_MODE"] = "live"

    try:
        adapter = get_scheduling_adapter()
        assert isinstance(adapter, HumanForceAdapter)
    finally:
        os.environ.pop("ROSTERIQ_PLATFORM", None)
        os.environ.pop("HUMANFORCE_API_KEY", None)
        os.environ.pop("ROSTERIQ_DATA_MODE", None)


def test_factory_returns_real_humanforce_with_oauth():
    """Factory should return HumanForceAdapter when OAuth credentials are configured."""
    os.environ["ROSTERIQ_PLATFORM"] = "humanforce"
    os.environ["HUMANFORCE_CLIENT_ID"] = "test_client_id"
    os.environ["HUMANFORCE_CLIENT_SECRET"] = "test_client_secret"
    os.environ["ROSTERIQ_DATA_MODE"] = "live"

    try:
        adapter = get_scheduling_adapter()
        assert isinstance(adapter, HumanForceAdapter)
    finally:
        os.environ.pop("ROSTERIQ_PLATFORM", None)
        os.environ.pop("HUMANFORCE_CLIENT_ID", None)
        os.environ.pop("HUMANFORCE_CLIENT_SECRET", None)
        os.environ.pop("ROSTERIQ_DATA_MODE", None)


# ============================================================================
# Test: Error Handling
# ============================================================================

def test_humanforce_api_error():
    """HumanForceAPIError should store error details."""
    error = HumanForceAPIError(
        "Test error",
        status_code=401,
        detail="Unauthorized access",
        response_json={"error": "auth_failed"},
    )

    assert error.status_code == 401
    assert error.detail == "Unauthorized access"
    assert error.response_json == {"error": "auth_failed"}
    assert "Test error" in str(error)


# ============================================================================
# Run Tests
# ============================================================================

def run_async_test(coro):
    """Helper to run async test functions."""
    loop = asyncio.get_event_loop()
    return loop.run_until_complete(coro)


if __name__ == "__main__":
    # Synchronous tests
    print("Running synchronous tests...")
    test_demo_humanforce_adapter_generates_employees()
    test_demo_humanforce_adapter_venue_name()
    test_demo_humanforce_adapter_multi_location()
    test_demo_humanforce_adapter_employment_mix()
    test_demo_humanforce_adapter_role_diversity()
    test_demo_humanforce_adapter_generates_shifts()
    test_demo_humanforce_adapter_generates_leave()
    test_humanforce_credentials()
    test_humanforce_client_initialization_api_key()
    test_humanforce_client_initialization_oauth()
    test_humanforce_client_initialization_access_token()
    test_humanforce_client_get_headers()
    test_humanforce_client_base_url_regions()
    test_humanforce_api_error()
    test_factory_returns_demo_humanforce_by_default()
    test_factory_returns_demo_humanforce_explicit()
    test_factory_returns_real_humanforce_with_api_key()
    test_factory_returns_real_humanforce_with_oauth()

    # Async tests
    print("Running async tests...")
    run_async_test(test_demo_humanforce_adapter_get_employees())
    run_async_test(test_demo_humanforce_adapter_get_availability())
    run_async_test(test_demo_humanforce_adapter_get_leave())
    run_async_test(test_demo_humanforce_adapter_get_shifts())
    run_async_test(test_demo_humanforce_adapter_get_timesheets())
    run_async_test(test_demo_humanforce_adapter_push_draft_roster())
    run_async_test(test_demo_humanforce_adapter_get_forecast_revenue())
    run_async_test(test_demo_humanforce_adapter_handle_webhook())

    print("\nAll tests passed!")
