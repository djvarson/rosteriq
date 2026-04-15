"""
Comprehensive test suite for Deputy Workforce Management Adapter.

Tests cover:
- Demo adapter data generation
- DeputyClient HTTP operations
- Pagination
- Rate limiting and backoff
- Factory integration
"""

import asyncio
import os
import sys
from datetime import date, time, datetime, timedelta
from pathlib import Path
from typing import Dict, Any, Optional
from unittest.mock import AsyncMock, MagicMock, patch, Mock

# Add root to path
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT)) if str(ROOT) not in sys.path else None

# Mock httpx before importing modules that depend on it
# (tanda_adapter imports httpx at module level)
sys.modules['httpx'] = MagicMock()

from rosteriq.deputy_adapter import (
    DeputyAdapter,
    DemoDeputyAdapter,
)
from rosteriq.deputy_integration import (
    DeputyClient,
    DeputyCredentials,
    DeputyAPIError,
)
from rosteriq.scheduling_platform_factory import get_scheduling_adapter
from rosteriq.tanda_adapter import Employee, Shift, Leave, Availability


# ============================================================================
# Test: Demo Deputy Adapter
# ============================================================================

def test_demo_deputy_adapter_generates_employees():
    """Demo adapter should generate non-empty employee list."""
    adapter = DemoDeputyAdapter()
    assert len(adapter._employees) == 10

    for emp in adapter._employees:
        assert emp.id.startswith("deputy_")
        assert len(emp.name) > 0
        assert emp.employment_type in ("full_time", "part_time", "casual")
        assert emp.hourly_rate > 0


def test_demo_deputy_adapter_generates_shifts():
    """Demo adapter should generate realistic shifts."""
    adapter = DemoDeputyAdapter()
    assert len(adapter._shifts) > 0

    for shift in adapter._shifts:
        assert shift.id.startswith("shift_")
        assert shift.employee_id
        assert isinstance(shift.date, date)
        assert shift.start_time < shift.end_time


def test_demo_deputy_adapter_generates_leave():
    """Demo adapter should generate leave records."""
    adapter = DemoDeputyAdapter()
    # May be empty if random doesn't select employees
    for emp_id, leaves in adapter._leave.items():
        for leave in leaves:
            assert leave.id.startswith("leave_")
            assert leave.employee_id == emp_id
            assert leave.start_date <= leave.end_date


async def test_demo_deputy_adapter_get_employees():
    """Demo adapter should return employees via async method."""
    adapter = DemoDeputyAdapter()
    employees = await adapter.get_employees("org_123")
    assert len(employees) == 10
    assert all(isinstance(e, Employee) for e in employees)


async def test_demo_deputy_adapter_get_availability():
    """Demo adapter should return availability."""
    adapter = DemoDeputyAdapter()
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


async def test_demo_deputy_adapter_get_leave():
    """Demo adapter should return leave records."""
    adapter = DemoDeputyAdapter()
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


async def test_demo_deputy_adapter_get_shifts():
    """Demo adapter should return shifts."""
    adapter = DemoDeputyAdapter()
    shifts = await adapter.get_shifts(
        "org_123",
        (date.today(), date.today() + timedelta(days=14))
    )

    assert isinstance(shifts, list)
    if shifts:
        assert all(isinstance(s, Shift) for s in shifts)


async def test_demo_deputy_adapter_get_timesheets():
    """Demo adapter should return timesheets."""
    adapter = DemoDeputyAdapter()
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


async def test_demo_deputy_adapter_push_draft_roster():
    """Demo adapter should simulate roster push."""
    adapter = DemoDeputyAdapter()
    shifts = [
        Shift(
            id="test_1",
            employee_id="deputy_1",
            date=date.today() + timedelta(days=1),
            start_time=time(9, 0),
            end_time=time(17, 0),
            role="bar",
            status="draft",
            break_minutes=30,
        )
    ]

    result = await adapter.push_draft_roster("org_123", shifts)
    assert "created" in result
    assert "errors" in result
    assert len(result["created"]) == 1


async def test_demo_deputy_adapter_get_forecast_revenue():
    """Demo adapter should return empty forecast (not supported)."""
    adapter = DemoDeputyAdapter()
    forecasts = await adapter.get_forecast_revenue(
        "org_123",
        (date.today(), date.today() + timedelta(days=7))
    )

    assert forecasts == []


async def test_demo_deputy_adapter_handle_webhook():
    """Demo adapter should handle webhooks."""
    adapter = DemoDeputyAdapter()
    result = await adapter.handle_webhook(
        "employee.created",
        {"id": "emp_123", "name": "Test"}
    )

    assert result["status"] == "processed"
    assert result["event"] == "employee.created"


# ============================================================================
# Test: Deputy Client
# ============================================================================

def test_deputy_credentials():
    """DeputyCredentials should handle token expiry."""
    creds = DeputyCredentials(
        subdomain="test",
        access_token="token123",
    )
    assert not creds.is_expired()

    creds_expired = DeputyCredentials(
        subdomain="test",
        access_token="token123",
        expires_at=datetime.now() - timedelta(hours=1),
    )
    assert creds_expired.is_expired()


def test_deputy_client_initialization():
    """DeputyClient should initialize with credentials."""
    client = DeputyClient(
        subdomain="mycompany",
        access_token="test_token",
    )

    assert client.subdomain == "mycompany"
    assert client.base_url == "https://mycompany.deputy.com/api/v1"
    assert client.access_token == "test_token"


def test_deputy_client_get_headers():
    """DeputyClient should construct correct auth headers."""
    client = DeputyClient(
        subdomain="test",
        access_token="mytoken",
    )

    headers = client._get_headers()
    assert headers["Authorization"] == "Bearer mytoken"
    assert headers["Content-Type"] == "application/json"


def test_deputy_client_get_headers_permanent_token():
    """DeputyClient should prefer permanent token over access token."""
    client = DeputyClient(
        subdomain="test",
        access_token="access_token",
        permanent_token="permanent_token",
    )

    headers = client._get_headers()
    assert headers["Authorization"] == "Bearer permanent_token"


async def test_deputy_client_get_handles_response():
    """DeputyClient.get should construct URL and call _retry_with_backoff."""
    client = DeputyClient(subdomain="test", access_token="token")

    mock_response = {"data": [{"id": "emp_1"}]}

    with patch.object(client, '_retry_with_backoff', new_callable=AsyncMock) as mock_retry:
        mock_retry.return_value = mock_response

        result = await client.get("/employee", params={"active": True})

        assert result == mock_response
        mock_retry.assert_called_once()
        call_args = mock_retry.call_args
        assert call_args[0][0] == "GET"
        assert "/employee" in call_args[0][1]


async def test_deputy_client_post_constructs_request():
    """DeputyClient.post should construct POST request."""
    client = DeputyClient(subdomain="test", access_token="token")

    mock_response = {"id": "shift_123", "created": True}

    with patch.object(client, '_retry_with_backoff', new_callable=AsyncMock) as mock_retry:
        mock_retry.return_value = mock_response

        result = await client.post("/roster", json={"date": "2026-04-15"})

        assert result == mock_response
        mock_retry.assert_called_once()
        call_args = mock_retry.call_args
        assert call_args[0][0] == "POST"


async def test_deputy_client_put_constructs_request():
    """DeputyClient.put should construct PUT request."""
    client = DeputyClient(subdomain="test", access_token="token")

    mock_response = {"id": "roster_123", "updated": True}

    with patch.object(client, '_retry_with_backoff', new_callable=AsyncMock) as mock_retry:
        mock_retry.return_value = mock_response

        result = await client.put("/roster/123", json={"status": "published"})

        assert result == mock_response
        mock_retry.assert_called_once()


async def test_deputy_client_paginate_single_page():
    """DeputyClient.paginate should handle single page response."""
    client = DeputyClient(subdomain="test", access_token="token")

    mock_response = {"data": [{"id": "emp_1"}, {"id": "emp_2"}]}

    with patch.object(client, 'get', new_callable=AsyncMock) as mock_get:
        mock_get.return_value = mock_response

        result = await client.paginate("/employee")

        assert len(result) == 2
        assert result[0]["id"] == "emp_1"


async def test_deputy_client_paginate_multiple_pages():
    """DeputyClient.paginate should loop until no more items."""
    client = DeputyClient(subdomain="test", access_token="token")

    responses = [
        {"data": [{"id": f"emp_{i}"} for i in range(100)]},
        {"data": [{"id": f"emp_{i}"} for i in range(100, 150)]},
        {"data": []},  # Last page empty
    ]

    with patch.object(client, 'get', new_callable=AsyncMock) as mock_get:
        mock_get.side_effect = responses

        result = await client.paginate("/employee", params={"active": True})

        assert len(result) == 150
        # mock_get.call_count should be 2 (stops after 150 items < page_size)
        # Actually when it gets 50 items (< 100), it stops
        assert mock_get.call_count == 2


async def test_deputy_client_paginate_handles_limit_response():
    """DeputyClient.paginate should stop when fewer items returned than limit."""
    client = DeputyClient(subdomain="test", access_token="token")

    responses = [
        {"data": [{"id": f"emp_{i}"} for i in range(100)]},
        {"data": [{"id": f"emp_{i}"} for i in range(100, 120)]},  # < page_size
    ]

    with patch.object(client, 'get', new_callable=AsyncMock) as mock_get:
        mock_get.side_effect = responses

        result = await client.paginate("/employee")

        assert len(result) == 120
        assert mock_get.call_count == 2


async def test_deputy_client_429_triggers_backoff():
    """DeputyClient should backoff on 429 rate limit."""
    client = DeputyClient(subdomain="test", access_token="token")

    # Create a mock response for 429
    mock_429_response = Mock()
    mock_429_response.status_code = 429

    mock_200_response = Mock()
    mock_200_response.status_code = 200
    mock_200_response.json.return_value = {"data": [{"id": "emp_1"}]}

    responses = [mock_429_response, mock_200_response]

    async def mock_request(*args, **kwargs):
        return responses.pop(0)

    with patch('httpx.AsyncClient') as mock_client_cls:
        mock_client = AsyncMock()
        mock_client.request = mock_request
        mock_client.__aenter__.return_value = mock_client
        mock_client.__aexit__.return_value = None
        mock_client_cls.return_value = mock_client

        result = await client.get("/employee")
        assert result == {"data": [{"id": "emp_1"}]}


async def test_deputy_client_non_2xx_raises_error():
    """DeputyClient should raise DeputyAPIError on non-2xx response."""
    client = DeputyClient(subdomain="test", access_token="token")

    mock_response = Mock()
    mock_response.status_code = 404
    mock_response.text = "Not found"
    mock_response.json.return_value = {"error": "Resource not found"}

    with patch('httpx.AsyncClient') as mock_client_cls:
        mock_client = AsyncMock()
        mock_client.request = AsyncMock(return_value=mock_response)
        mock_client.__aenter__.return_value = mock_client
        mock_client.__aexit__.return_value = None
        mock_client_cls.return_value = mock_client

        try:
            await client.get("/employee/999")
            assert False, "Should have raised DeputyAPIError"
        except DeputyAPIError as e:
            assert e.status_code == 404
            assert "Not found" in e.detail


# ============================================================================
# Test: Factory Integration
# ============================================================================

def test_factory_returns_demo_deputy_by_default():
    """Factory should return DemoDeputyAdapter when ROSTERIQ_PLATFORM=deputy and no credentials."""
    with patch.dict(os.environ, {
        "ROSTERIQ_PLATFORM": "deputy",
        "DEPUTY_SUBDOMAIN": "",
        "DEPUTY_ACCESS_TOKEN": "",
    }):
        adapter = get_scheduling_adapter()
        assert isinstance(adapter, DemoDeputyAdapter)


def test_factory_returns_demo_deputy_when_demo_mode():
    """Factory should return DemoDeputyAdapter when ROSTERIQ_DATA_MODE=demo."""
    with patch.dict(os.environ, {
        "ROSTERIQ_PLATFORM": "deputy",
        "ROSTERIQ_DATA_MODE": "demo",
        "DEPUTY_SUBDOMAIN": "mycompany",
        "DEPUTY_ACCESS_TOKEN": "token123",
    }):
        adapter = get_scheduling_adapter()
        assert isinstance(adapter, DemoDeputyAdapter)


def test_factory_returns_real_deputy_with_credentials():
    """Factory should return DeputyAdapter when credentials provided."""
    with patch.dict(os.environ, {
        "ROSTERIQ_PLATFORM": "deputy",
        "ROSTERIQ_DATA_MODE": "live",
        "DEPUTY_SUBDOMAIN": "mycompany",
        "DEPUTY_ACCESS_TOKEN": "token123",
    }):
        adapter = get_scheduling_adapter()
        assert isinstance(adapter, DeputyAdapter)


def test_factory_explicit_demo_deputy():
    """Factory should return DemoDeputyAdapter for explicit demo_deputy."""
    adapter = get_scheduling_adapter("demo_deputy")
    assert isinstance(adapter, DemoDeputyAdapter)


def test_factory_explicit_deputy_demo():
    """Factory should return DemoDeputyAdapter for explicit deputy_demo."""
    adapter = get_scheduling_adapter("deputy_demo")
    assert isinstance(adapter, DemoDeputyAdapter)


def test_factory_invalid_platform_raises_error():
    """Factory should raise ValueError for unknown platform."""
    try:
        get_scheduling_adapter("unknown_platform")
        assert False, "Should have raised ValueError"
    except ValueError as e:
        assert "unknown" in str(e).lower()


def test_factory_defaults_to_tanda():
    """Factory should default to Tanda when ROSTERIQ_PLATFORM not set."""
    with patch.dict(os.environ, {
        "ROSTERIQ_PLATFORM": "",
        "TANDA_CLIENT_ID": "",
        "TANDA_CLIENT_SECRET": "",
    }, clear=False):
        adapter = get_scheduling_adapter()
        # Should be DemoTandaAdapter (no credentials)
        from rosteriq.tanda_adapter import DemoTandaAdapter
        assert isinstance(adapter, DemoTandaAdapter)


# ============================================================================
# Run Tests
# ============================================================================

if __name__ == "__main__":
    # Run async tests
    async def run_async_tests():
        await test_demo_deputy_adapter_get_employees()
        await test_demo_deputy_adapter_get_availability()
        await test_demo_deputy_adapter_get_leave()
        await test_demo_deputy_adapter_get_shifts()
        await test_demo_deputy_adapter_get_timesheets()
        await test_demo_deputy_adapter_push_draft_roster()
        await test_demo_deputy_adapter_get_forecast_revenue()
        await test_demo_deputy_adapter_handle_webhook()
        await test_deputy_client_get_handles_response()
        await test_deputy_client_post_constructs_request()
        await test_deputy_client_put_constructs_request()
        await test_deputy_client_paginate_single_page()
        await test_deputy_client_paginate_multiple_pages()
        await test_deputy_client_paginate_handles_limit_response()
        await test_deputy_client_429_triggers_backoff()
        await test_deputy_client_non_2xx_raises_error()
        print("All async tests passed!")

    # Run sync tests
    test_demo_deputy_adapter_generates_employees()
    test_demo_deputy_adapter_generates_shifts()
    test_demo_deputy_adapter_generates_leave()
    test_deputy_credentials()
    test_deputy_client_initialization()
    test_deputy_client_get_headers()
    test_deputy_client_get_headers_permanent_token()
    test_factory_returns_demo_deputy_by_default()
    test_factory_returns_demo_deputy_when_demo_mode()
    test_factory_returns_real_deputy_with_credentials()
    test_factory_explicit_demo_deputy()
    test_factory_explicit_deputy_demo()
    test_factory_invalid_platform_raises_error()
    test_factory_defaults_to_tanda()
    print("All sync tests passed!")

    # Run async tests
    asyncio.run(run_async_tests())
    print("\nAll tests passed!")
