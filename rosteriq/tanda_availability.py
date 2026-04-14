"""
Tanda Availability Adapter for RosterIQ.

Provides availability window reads from Tanda's /organisations/{org_id}/availability
endpoint. Allows the roster engine to validate whether proposed shifts fit within
an employee's stated availability windows.

Includes:
- AvailabilityWindow dataclass
- Abstract AvailabilityReader base class
- TandaAvailabilityReader (pulls from Tanda API)
- DemoAvailabilityReader (generates plausible demo data)
- overlap() helper to check if a shift fits in an availability window
"""

import asyncio
import logging
import os
import random
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import date, time
from typing import List, Optional, Dict, Any

from rosteriq.tanda_adapter import DemoTandaAdapter, TandaAdapter, SchedulingPlatformAdapter

logger = logging.getLogger(__name__)


# ============================================================================
# Data Models
# ============================================================================

@dataclass
class AvailabilityWindow:
    """
    Employee availability window.

    Represents a recurring or one-off block of time when an employee is available.
    """
    employee_id: str
    day_of_week: int  # 0=Monday, 6=Sunday
    start_time: str  # "HH:MM" format
    end_time: str  # "HH:MM" format
    recurring: bool = True
    valid_from: Optional[date] = None
    valid_until: Optional[date] = None
    notes: Optional[str] = None


# ============================================================================
# Availability Reader Base Class
# ============================================================================

class AvailabilityReader(ABC):
    """
    Abstract base class for availability readers.

    Defines interface for fetching employee availability from various sources.
    """

    @abstractmethod
    async def get_availability(
        self,
        org_id: str,
        employee_id: Optional[str] = None,
    ) -> List[AvailabilityWindow]:
        """
        Retrieve availability windows for employees.

        Args:
            org_id: Organization/venue ID
            employee_id: Optional employee ID to filter by (None = all employees)

        Returns:
            List of AvailabilityWindow objects
        """
        pass


# ============================================================================
# Tanda Availability Reader
# ============================================================================

class TandaAvailabilityReader(AvailabilityReader):
    """
    Reads employee availability from Tanda's /organisations/{org_id}/availability endpoint.

    Handles:
    - List response (standard) or single-dict response (some tenants)
    - Weekdays as integer (0..6) or string ("monday".."sunday")
    - Times as "HH:MM" or "HH:MM:SS"
    - Missing fields → skip entry and log at DEBUG level
    """

    def __init__(self, tanda_adapter: SchedulingPlatformAdapter):
        """
        Initialize with a TandaAdapter instance.

        Args:
            tanda_adapter: TandaAdapter or similar with a client that has
                          a paginate() method and get() method.
        """
        self.adapter = tanda_adapter
        # Access the client directly if it's a TandaAdapter
        if isinstance(tanda_adapter, TandaAdapter):
            self.client = tanda_adapter.client
        else:
            self.client = None

    async def get_availability(
        self,
        org_id: str,
        employee_id: Optional[str] = None,
    ) -> List[AvailabilityWindow]:
        """
        Retrieve availability from Tanda's availability endpoint.

        Args:
            org_id: Tanda organization ID
            employee_id: Optional employee ID filter

        Returns:
            List of AvailabilityWindow objects
        """
        if self.client is None:
            logger.warning("TandaAvailabilityReader has no client; returning empty list")
            return []

        try:
            # Fetch from Tanda's availability endpoint
            response = await self.client.get(
                f"/organisations/{org_id}/availability",
                params={"employee_id": employee_id} if employee_id else {},
            )

            # Handle both list and single-dict responses
            raw = response.get("data", response.get("availability", response))
            if isinstance(raw, dict) and "employee_id" in raw:
                # Single-dict response (some tenants)
                raw = [raw]
            elif not isinstance(raw, list):
                logger.debug(f"Unexpected response shape from Tanda availability: {type(raw)}")
                return []

            windows: List[AvailabilityWindow] = []
            for entry in raw:
                window = self._parse_availability_entry(entry)
                if window:
                    windows.append(window)

            logger.info(f"Retrieved {len(windows)} availability windows from Tanda for org {org_id}")
            return windows

        except Exception as e:
            logger.error(f"Failed to get availability from Tanda: {e}")
            raise

    def _parse_availability_entry(self, entry: Dict[str, Any]) -> Optional[AvailabilityWindow]:
        """
        Parse a single availability entry from Tanda.

        Skips malformed entries and logs at DEBUG level.

        Args:
            entry: Raw availability entry dict

        Returns:
            AvailabilityWindow or None if malformed
        """
        try:
            employee_id = entry.get("employee_id")
            if not employee_id:
                logger.debug("Availability entry missing employee_id; skipping")
                return None

            day_of_week = entry.get("day_of_week")
            if day_of_week is None:
                logger.debug(f"Availability entry for {employee_id} missing day_of_week; skipping")
                return None

            # Handle weekday as string ("monday".."sunday") or int (0..6)
            if isinstance(day_of_week, str):
                weekday_map = {
                    "monday": 0, "tuesday": 1, "wednesday": 2, "thursday": 3,
                    "friday": 4, "saturday": 5, "sunday": 6,
                }
                day_of_week = weekday_map.get(day_of_week.lower())
                if day_of_week is None:
                    logger.debug(f"Unknown weekday string for {employee_id}; skipping")
                    return None
            elif not isinstance(day_of_week, int) or day_of_week < 0 or day_of_week > 6:
                logger.debug(f"Invalid day_of_week {day_of_week} for {employee_id}; skipping")
                return None

            # Parse times (handle "HH:MM:SS" → "HH:MM")
            start_time = entry.get("start_time")
            end_time = entry.get("end_time")

            if not start_time or not end_time:
                logger.debug(f"Availability entry for {employee_id} missing times; skipping")
                return None

            # Normalize to "HH:MM"
            start_time = self._normalize_time(start_time)
            end_time = self._normalize_time(end_time)

            if not start_time or not end_time:
                logger.debug(f"Could not parse times for {employee_id}; skipping")
                return None

            return AvailabilityWindow(
                employee_id=employee_id,
                day_of_week=day_of_week,
                start_time=start_time,
                end_time=end_time,
                recurring=entry.get("recurring", True),
                valid_from=entry.get("valid_from"),
                valid_until=entry.get("valid_until"),
                notes=entry.get("notes"),
            )

        except Exception as e:
            logger.debug(f"Error parsing availability entry: {e}")
            return None

    @staticmethod
    def _normalize_time(time_str: str) -> Optional[str]:
        """
        Normalize time string to "HH:MM" format.

        Handles "HH:MM:SS" → "HH:MM".

        Args:
            time_str: Time string (any format)

        Returns:
            "HH:MM" string or None if invalid
        """
        if not isinstance(time_str, str):
            return None

        parts = time_str.split(":")
        if len(parts) < 2:
            return None

        try:
            hour = int(parts[0])
            minute = int(parts[1])
            if hour < 0 or hour > 23 or minute < 0 or minute > 59:
                return None
            return f"{hour:02d}:{minute:02d}"
        except (ValueError, IndexError):
            return None


# ============================================================================
# Demo Availability Reader
# ============================================================================

class DemoAvailabilityReader(AvailabilityReader):
    """
    Generates plausible availability windows for demo employees.

    Produces 3-5 windows per employee with evening/weekend bias typical
    for Australian hospitality (casual staff available evenings and weekends).
    """

    def __init__(self, demo_adapter: DemoTandaAdapter):
        """
        Initialize with a DemoTandaAdapter.

        Args:
            demo_adapter: DemoTandaAdapter instance
        """
        self.adapter = demo_adapter

    async def get_availability(
        self,
        org_id: str,
        employee_id: Optional[str] = None,
    ) -> List[AvailabilityWindow]:
        """
        Generate plausible availability windows for demo employees.

        Args:
            org_id: Organization/venue ID (unused in demo)
            employee_id: Optional employee ID filter

        Returns:
            List of AvailabilityWindow objects
        """
        employees = self.adapter._generate_employees()
        windows: List[AvailabilityWindow] = []

        for employee in employees:
            if employee_id and employee.id != employee_id:
                continue

            # Generate 3-5 availability windows per employee
            num_windows = random.randint(3, 5)
            generated = self._generate_windows_for_employee(employee.id, num_windows)
            windows.extend(generated)

        logger.info(f"Generated {len(windows)} demo availability windows")
        return windows

    @staticmethod
    def _generate_windows_for_employee(employee_id: str, num_windows: int) -> List[AvailabilityWindow]:
        """
        Generate realistic availability windows for a single employee.

        Uses evening/weekend bias typical for Australian hospitality.

        Args:
            employee_id: Employee ID
            num_windows: Number of windows to generate

        Returns:
            List of AvailabilityWindow objects
        """
        windows: List[AvailabilityWindow] = []

        # Evening hours (typical for hospitality casuals)
        evening_shifts = [
            ("17:00", "22:00"),  # Dinner rush
            ("17:30", "23:00"),  # Longer dinner
            ("18:00", "23:30"),  # Late dinner
        ]

        # Weekend/flexible
        flexible_shifts = [
            ("09:00", "17:00"),  # Day shift
            ("10:00", "18:00"),  # Later day
            ("11:00", "19:00"),  # Lunch-to-dinner
        ]

        # Ensure at least one window covers the weekend
        days_covered = set()

        for i in range(num_windows):
            if i == 0:
                # First window should be weekend
                day_of_week = random.choice([5, 6])  # Saturday or Sunday
                start_time, end_time = random.choice(flexible_shifts + evening_shifts)
            elif len(days_covered) < 3:
                # Cover at least 3 different days
                day_of_week = random.choice([d for d in range(7) if d not in days_covered])
                start_time, end_time = random.choice(evening_shifts + flexible_shifts)
            else:
                # Fill in remaining with random
                day_of_week = random.randint(0, 6)
                start_time, end_time = random.choice(evening_shifts + flexible_shifts)

            days_covered.add(day_of_week)

            windows.append(
                AvailabilityWindow(
                    employee_id=employee_id,
                    day_of_week=day_of_week,
                    start_time=start_time,
                    end_time=end_time,
                    recurring=True,
                    notes=None,
                )
            )

        return windows


# ============================================================================
# Availability Overlap Helper
# ============================================================================

def overlap(
    window: AvailabilityWindow,
    shift_start,
    shift_end,
    shift_date: date,
) -> bool:
    """
    Check if a shift fits within an availability window.

    A shift fits if:
    1. The shift date's day-of-week matches the window's day_of_week
    2. The shift time falls within the window's time range (window.start_time <= shift_start, shift_end <= window.end_time)
    3. The shift date is within the window's valid_from/valid_until range (if set)

    Edge case: shift_start == window.end_time → NOT overlapping (no gap at boundaries)

    Args:
        window: AvailabilityWindow to check against
        shift_start: Shift start — either a ``datetime`` or an ``"HH:MM"`` string.
        shift_end: Shift end — either a ``datetime`` or an ``"HH:MM"`` string.
        shift_date: Date of the shift (used for day-of-week + validity check)

    Returns:
        True if shift fits within window, False otherwise
    """
    # Check date range validity
    if window.valid_from and shift_date < window.valid_from:
        return False
    if window.valid_until and shift_date > window.valid_until:
        return False

    # Check day of week
    if shift_date.weekday() != window.day_of_week:
        return False

    # Normalise shift_start / shift_end to "HH:MM" strings so we can compare
    # lexicographically against window.start_time / window.end_time.
    def _as_hhmm(t) -> str:
        if isinstance(t, str):
            # Tolerate "HH:MM:SS" by trimming seconds.
            return t[:5]
        # datetime / time objects
        return t.strftime("%H:%M")

    ss = _as_hhmm(shift_start)
    se = _as_hhmm(shift_end)

    # Check time overlap (edge case: exact boundary → not overlapping)
    if ss >= window.end_time or se <= window.start_time:
        return False

    # Shift must fit fully within window
    if ss < window.start_time or se > window.end_time:
        return False

    return True
