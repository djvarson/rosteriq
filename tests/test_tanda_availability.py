"""
Tests for the tanda_availability module.

Tests availability window parsing, demo reader, overlap logic, and
handling of various Tanda response formats.
"""

import asyncio
from datetime import date, timedelta
from unittest.mock import AsyncMock, MagicMock, patch
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from rosteriq.tanda_availability import (
    AvailabilityWindow,
    TandaAvailabilityReader,
    DemoAvailabilityReader,
    overlap,
)
from rosteriq.tanda_adapter import DemoTandaAdapter, TandaAdapter


def _run(coro):
    """Small helper to run async tests without an asyncio plugin."""
    return asyncio.get_event_loop().run_until_complete(coro)


# ============================================================================
# Demo Reader Tests
# ============================================================================

def test_demo_reader_generates_windows():
    """Demo reader should generate windows for all employees."""
    adapter = DemoTandaAdapter()
    reader = DemoAvailabilityReader(adapter)

    windows = _run(reader.get_availability("test_org"))

    assert len(windows) > 0, "Demo reader should generate windows"
    assert all(isinstance(w, AvailabilityWindow) for w in windows)


def test_demo_reader_windows_have_valid_day_of_week():
    """All demo windows should have valid day_of_week (0-6)."""
    adapter = DemoTandaAdapter()
    reader = DemoAvailabilityReader(adapter)

    windows = _run(reader.get_availability("test_org"))

    for window in windows:
        assert 0 <= window.day_of_week <= 6, f"Invalid day_of_week: {window.day_of_week}"


def test_demo_reader_windows_have_valid_times():
    """All demo windows should have start < end in HH:MM format."""
    adapter = DemoTandaAdapter()
    reader = DemoAvailabilityReader(adapter)

    windows = _run(reader.get_availability("test_org"))

    for window in windows:
        assert isinstance(window.start_time, str), "start_time should be string"
        assert isinstance(window.end_time, str), "end_time should be string"

        # Parse times
        start_h, start_m = map(int, window.start_time.split(":"))
        end_h, end_m = map(int, window.end_time.split(":"))
        start_mins = start_h * 60 + start_m
        end_mins = end_h * 60 + end_m

        assert start_mins < end_mins, f"start >= end for {window}"


def test_demo_reader_has_weekend_coverage():
    """Demo windows should include at least one Saturday or Sunday."""
    adapter = DemoTandaAdapter()
    reader = DemoAvailabilityReader(adapter)

    windows = _run(reader.get_availability("test_org"))

    weekend_windows = [w for w in windows if w.day_of_week in (5, 6)]
    assert len(weekend_windows) > 0, "Should have at least one weekend window"


def test_demo_reader_filter_by_employee():
    """Demo reader should filter by employee_id if provided."""
    adapter = DemoTandaAdapter()
    reader = DemoAvailabilityReader(adapter)

    # Get first employee
    employees = adapter._generate_employees()
    emp_id = employees[0].id

    # Get windows for that employee
    windows = _run(reader.get_availability("test_org", employee_id=emp_id))

    # All windows should be for that employee
    assert all(w.employee_id == emp_id for w in windows)


# ============================================================================
# Overlap Tests
# ============================================================================

def test_overlap_shift_fully_inside_window():
    """Overlap should return True when shift is fully inside window."""
    window = AvailabilityWindow(
        employee_id="emp1",
        day_of_week=1,  # Tuesday
        start_time="09:00",
        end_time="17:00",
    )
    shift_date = date(2026, 4, 21)  # Tuesday, 2026-04-21
    assert overlap(window, "10:00", "12:00", shift_date) is True


def test_overlap_shift_fully_outside_window():
    """Overlap should return False when shift is fully outside window."""
    window = AvailabilityWindow(
        employee_id="emp1",
        day_of_week=1,  # Tuesday
        start_time="09:00",
        end_time="17:00",
    )
    shift_date = date(2026, 4, 21)  # Tuesday
    assert overlap(window, "18:00", "22:00", shift_date) is False


def test_overlap_shift_wrong_day():
    """Overlap should return False when shift is on wrong day of week."""
    window = AvailabilityWindow(
        employee_id="emp1",
        day_of_week=1,  # Tuesday
        start_time="09:00",
        end_time="17:00",
    )
    # Monday, 2026-04-20
    shift_date = date(2026, 4, 20)
    assert overlap(window, "10:00", "12:00", shift_date) is False


def test_overlap_shift_partial_overlap_start():
    """Overlap should return False for partial overlap (shift starts before window)."""
    window = AvailabilityWindow(
        employee_id="emp1",
        day_of_week=1,
        start_time="09:00",
        end_time="17:00",
    )
    shift_date = date(2026, 4, 21)  # Tuesday
    # Shift: 08:00-10:00 (starts before window)
    assert overlap(window, "08:00", "10:00", shift_date) is False


def test_overlap_shift_partial_overlap_end():
    """Overlap should return False for partial overlap (shift ends after window)."""
    window = AvailabilityWindow(
        employee_id="emp1",
        day_of_week=1,
        start_time="09:00",
        end_time="17:00",
    )
    shift_date = date(2026, 4, 21)  # Tuesday
    # Shift: 16:00-18:00 (ends after window)
    assert overlap(window, "16:00", "18:00", shift_date) is False


def test_overlap_edge_case_shift_start_equals_window_end():
    """Overlap should return False when shift start == window end (no gap)."""
    window = AvailabilityWindow(
        employee_id="emp1",
        day_of_week=1,
        start_time="09:00",
        end_time="17:00",
    )
    shift_date = date(2026, 4, 21)
    # Shift starts exactly when window ends
    assert overlap(window, "17:00", "18:00", shift_date) is False


def test_overlap_edge_case_shift_end_equals_window_start():
    """Overlap should return False when shift end == window start (no gap)."""
    window = AvailabilityWindow(
        employee_id="emp1",
        day_of_week=1,
        start_time="09:00",
        end_time="17:00",
    )
    shift_date = date(2026, 4, 21)
    # Shift ends exactly when window starts
    assert overlap(window, "08:00", "09:00", shift_date) is False


def test_overlap_respects_valid_from():
    """Overlap should return False if shift date is before valid_from."""
    window = AvailabilityWindow(
        employee_id="emp1",
        day_of_week=1,
        start_time="09:00",
        end_time="17:00",
        valid_from=date(2026, 5, 1),
    )
    # Shift before valid_from
    shift_date = date(2026, 4, 21)
    assert overlap(window, "10:00", "12:00", shift_date) is False


def test_overlap_respects_valid_until():
    """Overlap should return False if shift date is after valid_until."""
    window = AvailabilityWindow(
        employee_id="emp1",
        day_of_week=1,
        start_time="09:00",
        end_time="17:00",
        valid_until=date(2026, 4, 20),
    )
    # Shift after valid_until
    shift_date = date(2026, 4, 21)
    assert overlap(window, "10:00", "12:00", shift_date) is False


# ============================================================================
# Tanda Reader Tests
# ============================================================================

def test_tanda_reader_parses_list_response():
    """TandaAvailabilityReader should parse standard list response."""
    # Mock adapter and client
    mock_client = AsyncMock()
    mock_client.get = AsyncMock(
        return_value={
            "data": [
                {
                    "employee_id": "emp1",
                    "day_of_week": 1,
                    "start_time": "09:00",
                    "end_time": "17:00",
                },
                {
                    "employee_id": "emp2",
                    "day_of_week": 2,
                    "start_time": "10:00",
                    "end_time": "18:00",
                },
            ]
        }
    )

    mock_adapter = MagicMock(spec=TandaAdapter)
    mock_adapter.client = mock_client

    reader = TandaAvailabilityReader(mock_adapter)
    windows = _run(reader.get_availability("test_org"))

    assert len(windows) == 2
    assert windows[0].employee_id == "emp1"
    assert windows[1].employee_id == "emp2"


def test_tanda_reader_parses_single_dict_response():
    """TandaAvailabilityReader should parse single-dict response (some tenants)."""
    mock_client = AsyncMock()
    # Single dict response (some tenants)
    mock_client.get = AsyncMock(
        return_value={
            "employee_id": "emp1",
            "day_of_week": 1,
            "start_time": "09:00",
            "end_time": "17:00",
        }
    )

    mock_adapter = MagicMock(spec=TandaAdapter)
    mock_adapter.client = mock_client

    reader = TandaAvailabilityReader(mock_adapter)
    windows = _run(reader.get_availability("test_org"))

    assert len(windows) == 1
    assert windows[0].employee_id == "emp1"


def test_tanda_reader_handles_string_weekday():
    """TandaAvailabilityReader should handle weekday as string."""
    mock_client = AsyncMock()
    mock_client.get = AsyncMock(
        return_value={
            "data": [
                {
                    "employee_id": "emp1",
                    "day_of_week": "tuesday",
                    "start_time": "09:00",
                    "end_time": "17:00",
                },
            ]
        }
    )

    mock_adapter = MagicMock(spec=TandaAdapter)
    mock_adapter.client = mock_client

    reader = TandaAvailabilityReader(mock_adapter)
    windows = _run(reader.get_availability("test_org"))

    assert len(windows) == 1
    assert windows[0].day_of_week == 1  # Tuesday


def test_tanda_reader_handles_hhmmss_time_format():
    """TandaAvailabilityReader should convert HH:MM:SS to HH:MM."""
    mock_client = AsyncMock()
    mock_client.get = AsyncMock(
        return_value={
            "data": [
                {
                    "employee_id": "emp1",
                    "day_of_week": 1,
                    "start_time": "09:00:30",
                    "end_time": "17:00:45",
                },
            ]
        }
    )

    mock_adapter = MagicMock(spec=TandaAdapter)
    mock_adapter.client = mock_client

    reader = TandaAvailabilityReader(mock_adapter)
    windows = _run(reader.get_availability("test_org"))

    assert len(windows) == 1
    assert windows[0].start_time == "09:00"
    assert windows[0].end_time == "17:00"


def test_tanda_reader_skips_malformed_entries():
    """TandaAvailabilityReader should skip malformed entries without raising."""
    mock_client = AsyncMock()
    mock_client.get = AsyncMock(
        return_value={
            "data": [
                {
                    "employee_id": "emp1",
                    "day_of_week": 1,
                    "start_time": "09:00",
                    "end_time": "17:00",
                },
                {
                    # Missing employee_id
                    "day_of_week": 2,
                    "start_time": "10:00",
                    "end_time": "18:00",
                },
                {
                    "employee_id": "emp3",
                    # Missing day_of_week
                    "start_time": "11:00",
                    "end_time": "19:00",
                },
            ]
        }
    )

    mock_adapter = MagicMock(spec=TandaAdapter)
    mock_adapter.client = mock_client

    reader = TandaAvailabilityReader(mock_adapter)
    windows = _run(reader.get_availability("test_org"))

    # Should only parse the first valid entry
    assert len(windows) == 1
    assert windows[0].employee_id == "emp1"


def test_tanda_reader_filters_by_employee_id():
    """TandaAvailabilityReader should pass employee_id to the API."""
    mock_client = AsyncMock()
    mock_client.get = AsyncMock(
        return_value={
            "data": [
                {
                    "employee_id": "emp1",
                    "day_of_week": 1,
                    "start_time": "09:00",
                    "end_time": "17:00",
                },
            ]
        }
    )

    mock_adapter = MagicMock(spec=TandaAdapter)
    mock_adapter.client = mock_client

    reader = TandaAvailabilityReader(mock_adapter)
    _run(reader.get_availability("test_org", employee_id="emp1"))

    # Verify the API was called with employee_id param
    mock_client.get.assert_called_once()
    call_kwargs = mock_client.get.call_args[1]
    assert "params" in call_kwargs
    assert call_kwargs["params"].get("employee_id") == "emp1"
