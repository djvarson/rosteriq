"""Tests for the Tanda API adapter module."""

import pytest
import asyncio
import time
from datetime import date, datetime, timedelta
from decimal import Decimal
from unittest.mock import AsyncMock, patch, MagicMock

from rosteriq.models import (
    TandaCredentials, State, EmploymentType, ShiftStatus,
)
from rosteriq.tanda_adapter import (
    TandaAdapter, RateLimiter, TandaAPIError,
    _TANDA_EMPLOYMENT_MAP, _TANDA_STATUS_MAP,
)


# ============================================================================
# Helpers
# ============================================================================

def make_credentials(**overrides) -> TandaCredentials:
    defaults = dict(
        client_id="test-client-id",
        client_secret="test-secret",
        access_token="test-token",
        refresh_token="test-refresh",
        org_id="org-123",
    )
    defaults.update(overrides)
    return TandaCredentials(**defaults)


def make_tanda_user(id=1, name="Alice", employment_type="casual", hourly_rate=28.5):
    return {
        "id": id,
        "name": name,
        "employment_type": employment_type,
        "hourly_rate": hourly_rate,
        "phone": "0412345678",
        "email": "alice@example.com",
        "qualifications": ["bar", "floor"],
    }


def make_tanda_shift(id=1, user_id=1, start_ts=None, end_ts=None):
    if start_ts is None:
        start_ts = datetime(2026, 4, 7, 9, 0).timestamp()
    if end_ts is None:
        end_ts = datetime(2026, 4, 7, 17, 0).timestamp()
    return {
        "id": id,
        "user_id": user_id,
        "start": start_ts,
        "finish": end_ts,
        "breaks": [{"start": start_ts + 14400, "finish": start_ts + 16200}],
        "status": "confirmed",
        "department": "bar",
    }


# ============================================================================
# Rate limiter tests
# ============================================================================

class TestRateLimiter:
    @pytest.mark.asyncio
    async def test_allows_requests_within_limit(self):
        limiter = RateLimiter(max_requests=5, window_seconds=60)
        for _ in range(5):
            await limiter.acquire()
        assert len(limiter.requests) == 5

    @pytest.mark.asyncio
    async def test_cleans_old_requests(self):
        limiter = RateLimiter(max_requests=5, window_seconds=1)
        limiter.requests = [time.time() - 2.0]  # Old request
        limiter._clean_old_requests()
        assert len(limiter.requests) == 0

    def test_initial_state(self):
        limiter = RateLimiter()
        assert limiter.max_requests == 100
        assert limiter.window_seconds == 60
        assert limiter.requests == []

    def test_custom_limits(self):
        limiter = RateLimiter(max_requests=50, window_seconds=30)
        assert limiter.max_requests == 50
        assert limiter.window_seconds == 30


# ============================================================================
# Employment type mapping tests
# ============================================================================

class TestMappings:
    def test_employment_type_mapping(self):
        assert _TANDA_EMPLOYMENT_MAP["full_time"] == EmploymentType.full_time
        assert _TANDA_EMPLOYMENT_MAP["part_time"] == EmploymentType.part_time
        assert _TANDA_EMPLOYMENT_MAP["casual"] == EmploymentType.casual
        assert _TANDA_EMPLOYMENT_MAP["full-time"] == EmploymentType.full_time
        assert _TANDA_EMPLOYMENT_MAP["part-time"] == EmploymentType.part_time

    def test_status_mapping(self):
        assert _TANDA_STATUS_MAP["pending"] == ShiftStatus.scheduled
        assert _TANDA_STATUS_MAP["confirmed"] == ShiftStatus.confirmed
        assert _TANDA_STATUS_MAP["completed"] == ShiftStatus.completed
        assert _TANDA_STATUS_MAP["cancelled"] == ShiftStatus.cancelled


# ============================================================================
# Adapter initialisation tests
# ============================================================================

class TestAdapterInit:
    def test_default_state(self):
        creds = make_credentials()
        adapter = TandaAdapter(creds)
        assert adapter.state == State.vic
        assert adapter._client is None

    def test_custom_state(self):
        creds = make_credentials()
        adapter = TandaAdapter(creds, state=State.nsw)
        assert adapter.state == State.nsw

    def test_base_url(self):
        assert TandaAdapter.BASE_URL == "https://my.tanda.co/api/v2"


# ============================================================================
# User mapping tests
# ============================================================================

class TestMapTandaUser:
    def test_basic_mapping(self):
        creds = make_credentials()
        adapter = TandaAdapter(creds)
        user_data = make_tanda_user()
        employee = adapter._map_tanda_user(user_data)

        assert employee.id == "1"
        assert employee.tanda_id == "1"
        assert employee.name == "Alice"
        assert employee.employment_type == EmploymentType.casual
        assert employee.hourly_base_rate == Decimal("28.5")
        assert employee.phone == "0412345678"
        assert employee.email == "alice@example.com"
        assert "bar" in employee.skills

    def test_full_time_mapping(self):
        creds = make_credentials()
        adapter = TandaAdapter(creds)
        user_data = make_tanda_user(employment_type="full_time")
        employee = adapter._map_tanda_user(user_data)
        assert employee.employment_type == EmploymentType.full_time

    def test_unknown_employment_defaults_casual(self):
        creds = make_credentials()
        adapter = TandaAdapter(creds)
        user_data = make_tanda_user(employment_type="contractor")
        employee = adapter._map_tanda_user(user_data)
        assert employee.employment_type == EmploymentType.casual

    def test_state_from_adapter(self):
        creds = make_credentials()
        adapter = TandaAdapter(creds, state=State.qld)
        employee = adapter._map_tanda_user(make_tanda_user())
        assert employee.state == State.qld


# ============================================================================
# Shift mapping tests
# ============================================================================

class TestMapTandaShift:
    def test_basic_mapping(self):
        creds = make_credentials()
        adapter = TandaAdapter(creds)
        shift_data = make_tanda_shift()
        shift = adapter._map_tanda_shift(shift_data)

        assert shift.id == "1"
        assert shift.employee_id == "1"
        assert shift.date == date(2026, 4, 7)
        assert shift.start_time.hour == 9
        assert shift.end_time.hour == 17
        assert shift.status == ShiftStatus.confirmed
        assert shift.role == "bar"

    def test_break_calculation(self):
        creds = make_credentials()
        adapter = TandaAdapter(creds)
        shift_data = make_tanda_shift()
        shift = adapter._map_tanda_shift(shift_data)
        assert shift.break_minutes == 30  # 1800 seconds / 60

    def test_iso_string_times(self):
        creds = make_credentials()
        adapter = TandaAdapter(creds)
        shift_data = {
            "id": 2,
            "user_id": 1,
            "start": "2026-04-07T09:00:00",
            "finish": "2026-04-07T17:00:00",
            "breaks": [],
            "status": "pending",
            "department": "floor",
        }
        shift = adapter._map_tanda_shift(shift_data)
        assert shift.date == date(2026, 4, 7)
        assert shift.start_time.hour == 9
        assert shift.break_minutes == 0

    def test_missing_status_defaults_scheduled(self):
        creds = make_credentials()
        adapter = TandaAdapter(creds)
        shift_data = {
            "id": 3,
            "user_id": 1,
            "start": datetime(2026, 4, 7, 9, 0).timestamp(),
            "finish": datetime(2026, 4, 7, 17, 0).timestamp(),
            "breaks": [],
        }
        shift = adapter._map_tanda_shift(shift_data)
        assert shift.status == ShiftStatus.scheduled


# ============================================================================
# Context manager tests
# ============================================================================

class TestContextManager:
    @pytest.mark.asyncio
    async def test_enter_creates_client(self):
        creds = make_credentials()
        adapter = TandaAdapter(creds)
        assert adapter._client is None

        async with adapter:
            assert adapter._client is not None

    @pytest.mark.asyncio
    async def test_exit_closes_client(self):
        creds = make_credentials()
        adapter = TandaAdapter(creds)

        async with adapter:
            pass

        assert adapter._client is None


# ============================================================================
# Error handling tests
# ============================================================================

class TestErrorHandling:
    def test_tanda_api_error_message(self):
        from rosteriq.models import APIError
        err = TandaAPIError(APIError(status_code=500, message="Server error"))
        assert "500" in str(err)
        assert "Server error" in str(err)

    def test_no_refresh_token_raises(self):
        creds = make_credentials(refresh_token=None)
        adapter = TandaAdapter(creds)

        async def test():
            with pytest.raises(TandaAPIError):
                await adapter.refresh_token()

        asyncio.get_event_loop().run_until_complete(test())


# ============================================================================
# Health check tests
# ============================================================================

class TestHealthCheck:
    @pytest.mark.asyncio
    async def test_health_check_returns_bool(self):
        """Health check should return a boolean."""
        creds = make_credentials()
        adapter = TandaAdapter(creds)
        # Without a real API, this will fail — that's expected
        result = await adapter.health_check()
        assert isinstance(result, bool)
        assert result is False  # Can't reach real API
        await adapter.close()
