"""
Australian Public Holiday Manager for RosterIQ

Provides core functionality for managing Australian public holidays:
- National and state-specific holidays
- Easter calculation (Anonymous Gregorian algorithm)
- Penalty multiplier lookup
- Custom venue-specific holidays
- SQLite persistence for holiday calendars and custom holidays

Key data structures:
- HolidayType enum: NATIONAL, STATE, LOCAL, CUSTOM
- PublicHoliday dataclass: holiday_id, name, date, state, holiday_type, is_gazetted, substitute_date, penalty_multiplier
- HolidayCalendar dataclass: year, state, holidays
- PublicHolidayStore: SQLite-persisted store for holidays
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date, timedelta
from enum import Enum
from typing import Dict, List, Optional, Tuple
from uuid import uuid4
import threading
import sqlite3

try:
    from rosteriq.persistence import write_txn, is_persistence_enabled, register_schema, json_dumps
except ImportError:
    write_txn = None
    is_persistence_enabled = None
    register_schema = None
    json_dumps = None

logger = logging.getLogger("rosteriq.public_holidays")


# ─────────────────────────────────────────────────────────────────────────────
# Enums and Data Structures
# ─────────────────────────────────────────────────────────────────────────────


class HolidayType(Enum):
    """Holiday classification."""
    NATIONAL = "national"
    STATE = "state"
    LOCAL = "local"
    CUSTOM = "custom"


@dataclass
class PublicHoliday:
    """Represents a single public holiday."""
    holiday_id: str
    name: str
    date: date
    state: str  # "ALL" for national, state abbreviation (QLD, NSW, VIC, etc.) for state-specific
    holiday_type: HolidayType
    is_gazetted: bool = True
    substitute_date: Optional[date] = None  # If falls on weekend, substitute Monday
    penalty_multiplier: float = 2.5  # Default: 250% for full/part-time; casual gets 25% loading + 125%

    def to_dict(self) -> Dict:
        """Serialize to dictionary."""
        return {
            "holiday_id": self.holiday_id,
            "name": self.name,
            "date": self.date.isoformat(),
            "state": self.state,
            "holiday_type": self.holiday_type.value,
            "is_gazetted": self.is_gazetted,
            "substitute_date": self.substitute_date.isoformat() if self.substitute_date else None,
            "penalty_multiplier": self.penalty_multiplier,
        }


@dataclass
class HolidayCalendar:
    """Represents a year's holidays for a state."""
    year: int
    state: str
    holidays: List[PublicHoliday] = field(default_factory=list)

    def to_dict(self) -> Dict:
        """Serialize to dictionary."""
        return {
            "year": self.year,
            "state": self.state,
            "holidays": [h.to_dict() for h in self.holidays],
        }


# ─────────────────────────────────────────────────────────────────────────────
# Easter Calculation (Anonymous Gregorian Algorithm)
# ─────────────────────────────────────────────────────────────────────────────


def calculate_easter(year: int) -> date:
    """
    Calculate Easter Sunday for a given year using the Anonymous Gregorian algorithm.

    Pure mathematical calculation, no external dependencies.
    Valid for years 1583–4099.
    """
    a = year % 19
    b = year // 100
    c = year % 100
    d = b // 4
    e = b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i = c // 4
    k = c % 4
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    month = (h + l - 7 * m + 114) // 31
    day = ((h + l - 7 * m + 114) % 31) + 1
    return date(year, month, day)


# ─────────────────────────────────────────────────────────────────────────────
# Generate National and State Holidays
# ─────────────────────────────────────────────────────────────────────────────


def generate_national_holidays(year: int) -> List[PublicHoliday]:
    """
    Generate all national public holidays for a given year.

    National holidays apply to all Australian states.
    """
    holidays = []

    # New Year's Day (Jan 1)
    holidays.append(
        PublicHoliday(
            holiday_id=f"NEW_YEAR_{year}",
            name="New Year's Day",
            date=date(year, 1, 1),
            state="ALL",
            holiday_type=HolidayType.NATIONAL,
        )
    )

    # Australia Day (Jan 26)
    holidays.append(
        PublicHoliday(
            holiday_id=f"AUSTRALIA_DAY_{year}",
            name="Australia Day",
            date=date(year, 1, 26),
            state="ALL",
            holiday_type=HolidayType.NATIONAL,
        )
    )

    # Easter holidays (Good Friday, Easter Saturday, Easter Monday)
    easter_sunday = calculate_easter(year)
    good_friday = easter_sunday - timedelta(days=2)
    easter_saturday = easter_sunday - timedelta(days=1)
    easter_monday = easter_sunday + timedelta(days=1)

    holidays.append(
        PublicHoliday(
            holiday_id=f"GOOD_FRIDAY_{year}",
            name="Good Friday",
            date=good_friday,
            state="ALL",
            holiday_type=HolidayType.NATIONAL,
        )
    )

    holidays.append(
        PublicHoliday(
            holiday_id=f"EASTER_SATURDAY_{year}",
            name="Easter Saturday",
            date=easter_saturday,
            state="ALL",
            holiday_type=HolidayType.NATIONAL,
        )
    )

    holidays.append(
        PublicHoliday(
            holiday_id=f"EASTER_MONDAY_{year}",
            name="Easter Monday",
            date=easter_monday,
            state="ALL",
            holiday_type=HolidayType.NATIONAL,
        )
    )

    # ANZAC Day (Apr 25)
    holidays.append(
        PublicHoliday(
            holiday_id=f"ANZAC_DAY_{year}",
            name="ANZAC Day",
            date=date(year, 4, 25),
            state="ALL",
            holiday_type=HolidayType.NATIONAL,
        )
    )

    # Queen's Birthday (varies by state; most states June, QLD October)
    # Default for non-QLD states: second Monday in June
    june_first = date(year, 6, 1)
    days_until_monday = (7 - june_first.weekday()) % 7
    if days_until_monday == 0:
        first_monday = june_first
    else:
        first_monday = june_first + timedelta(days=days_until_monday)
    queens_birthday_date = first_monday + timedelta(days=7)  # Second Monday

    holidays.append(
        PublicHoliday(
            holiday_id=f"QUEENS_BIRTHDAY_{year}",
            name="Queen's Birthday",
            date=queens_birthday_date,
            state="ALL",  # This is overridden in state-specific holidays
            holiday_type=HolidayType.NATIONAL,
        )
    )

    # Christmas Day (Dec 25)
    holidays.append(
        PublicHoliday(
            holiday_id=f"CHRISTMAS_DAY_{year}",
            name="Christmas Day",
            date=date(year, 12, 25),
            state="ALL",
            holiday_type=HolidayType.NATIONAL,
        )
    )

    # Boxing Day (Dec 26)
    holidays.append(
        PublicHoliday(
            holiday_id=f"BOXING_DAY_{year}",
            name="Boxing Day",
            date=date(year, 12, 26),
            state="ALL",
            holiday_type=HolidayType.NATIONAL,
        )
    )

    return holidays


def generate_state_holidays(year: int, state: str) -> List[PublicHoliday]:
    """
    Generate state-specific public holidays for a given year and state.

    Args:
        year: The year to generate holidays for
        state: State abbreviation (QLD, NSW, VIC, etc.)

    Returns:
        List of state-specific holidays (does not include national holidays)
    """
    holidays = []
    state = state.upper()

    if state == "QLD":
        # Royal Queensland Show (Ekka) - Brisbane only, usually second Wednesday in August
        august_first = date(year, 8, 1)
        days_until_wednesday = (2 - august_first.weekday()) % 7
        if days_until_wednesday == 0:
            first_wednesday = august_first
        else:
            first_wednesday = august_first + timedelta(days=days_until_wednesday)
        ekka_date = first_wednesday + timedelta(days=7)  # Second Wednesday

        holidays.append(
            PublicHoliday(
                holiday_id=f"EKKA_{year}",
                name="Royal Queensland Show (Ekka)",
                date=ekka_date,
                state="QLD",
                holiday_type=HolidayType.STATE,
            )
        )

        # Reconciliation Day - May 27 (from 2026 onwards)
        if year >= 2026:
            holidays.append(
                PublicHoliday(
                    holiday_id=f"RECONCILIATION_DAY_{year}",
                    name="Reconciliation Day",
                    date=date(year, 5, 27),
                    state="QLD",
                    holiday_type=HolidayType.STATE,
                )
            )

        # Queen's Birthday for QLD - first Monday in October
        october_first = date(year, 10, 1)
        days_until_monday = (0 - october_first.weekday()) % 7
        if days_until_monday == 0:
            queens_birthday_qld = october_first
        else:
            queens_birthday_qld = october_first + timedelta(days=days_until_monday)

        holidays.append(
            PublicHoliday(
                holiday_id=f"QUEENS_BIRTHDAY_QLD_{year}",
                name="Queen's Birthday",
                date=queens_birthday_qld,
                state="QLD",
                holiday_type=HolidayType.STATE,
            )
        )

    # Add more states as needed (NSW, VIC, etc.)

    return holidays


def get_holidays_for_year(year: int, state: str) -> HolidayCalendar:
    """
    Get combined national and state-specific holidays for a given year and state.

    Args:
        year: The year to get holidays for
        state: State abbreviation (QLD, NSW, etc.) or "ALL" for national-only

    Returns:
        HolidayCalendar with combined holidays
    """
    state = state.upper()
    holidays = []

    # Add national holidays
    national = generate_national_holidays(year)
    holidays.extend(national)

    # Add state-specific holidays if not "ALL"
    if state != "ALL":
        state_specific = generate_state_holidays(year, state)
        holidays.extend(state_specific)

    # Sort by date
    holidays.sort(key=lambda h: h.date)

    return HolidayCalendar(year=year, state=state, holidays=holidays)


def is_public_holiday(check_date: date, state: str) -> Tuple[bool, Optional[PublicHoliday]]:
    """
    Check if a given date is a public holiday in a state.

    Args:
        check_date: The date to check
        state: State abbreviation

    Returns:
        Tuple of (is_holiday: bool, holiday: Optional[PublicHoliday])
    """
    calendar = get_holidays_for_year(check_date.year, state)
    for holiday in calendar.holidays:
        if holiday.date == check_date:
            return (True, holiday)
    return (False, None)


def get_penalty_multiplier(check_date: date, state: str, employment_type: str = "casual") -> float:
    """
    Get the penalty multiplier for a date.

    Args:
        check_date: The date to check
        state: State abbreviation
        employment_type: "casual", "full_time", or "part_time"

    Returns:
        float: Penalty multiplier (1.0 if not a holiday, or holiday penalty rate)
    """
    is_holiday, holiday = is_public_holiday(check_date, state)
    if not is_holiday or holiday is None:
        return 1.0

    # Casual workers get the multiplier as stated (125% base + 25% loading)
    # Full-time/part-time get the full multiplier
    if employment_type.lower() == "casual":
        return holiday.penalty_multiplier
    else:
        return holiday.penalty_multiplier


def get_upcoming_holidays(state: str, days_ahead: int = 90) -> List[PublicHoliday]:
    """
    Get upcoming public holidays for a state.

    Args:
        state: State abbreviation
        days_ahead: Number of days to look ahead (default 90)

    Returns:
        List of upcoming PublicHoliday objects
    """
    today = date.today()
    cutoff = today + timedelta(days=days_ahead)

    upcoming = []
    current_year = today.year

    # Check current year and next year to ensure we cover the full range
    for year in [current_year, current_year + 1]:
        calendar = get_holidays_for_year(year, state)
        for holiday in calendar.holidays:
            if today <= holiday.date <= cutoff:
                upcoming.append(holiday)

    upcoming.sort(key=lambda h: h.date)
    return upcoming


def apply_substitute_day(holiday: PublicHoliday) -> PublicHoliday:
    """
    If a holiday falls on a weekend, compute the substitute Monday.

    Australian practice: if a public holiday falls on Saturday or Sunday,
    a Monday substitute is observed instead.

    Args:
        holiday: The PublicHoliday to check

    Returns:
        Updated PublicHoliday with substitute_date set if applicable
    """
    weekday = holiday.date.weekday()  # 0=Monday, 5=Saturday, 6=Sunday

    if weekday == 5:  # Saturday
        holiday.substitute_date = holiday.date + timedelta(days=2)  # Next Monday
    elif weekday == 6:  # Sunday
        holiday.substitute_date = holiday.date + timedelta(days=1)  # Next Monday

    return holiday


# ─────────────────────────────────────────────────────────────────────────────
# SQLite-Persisted Store
# ─────────────────────────────────────────────────────────────────────────────

# Register the schema for public holidays
_SCHEMA = """
CREATE TABLE IF NOT EXISTS public_holidays (
    holiday_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    date TEXT NOT NULL,
    state TEXT NOT NULL,
    holiday_type TEXT NOT NULL,
    is_gazetted INTEGER NOT NULL DEFAULT 1,
    substitute_date TEXT,
    penalty_multiplier REAL NOT NULL DEFAULT 2.5,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS custom_holidays (
    holiday_id TEXT PRIMARY KEY,
    venue_id TEXT NOT NULL,
    name TEXT NOT NULL,
    date TEXT NOT NULL,
    penalty_multiplier REAL NOT NULL DEFAULT 2.5,
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_holidays_state_date ON public_holidays(state, date);
CREATE INDEX IF NOT EXISTS idx_holidays_date ON public_holidays(date);
CREATE INDEX IF NOT EXISTS idx_custom_venue_date ON custom_holidays(venue_id, date);
"""

if register_schema:
    register_schema("public_holidays", _SCHEMA)


class PublicHolidayStore:
    """Thread-safe store for managing public holidays with SQLite persistence."""

    def __init__(self):
        self._lock = threading.Lock()
        self._cache: Dict[str, HolidayCalendar] = {}

    def add_custom_holiday(
        self, venue_id: str, name: str, holiday_date: date, penalty_multiplier: float = 2.5
    ) -> PublicHoliday:
        """
        Add a custom venue-specific holiday.

        Args:
            venue_id: The venue identifier
            name: Holiday name
            holiday_date: The date of the holiday
            penalty_multiplier: Penalty multiplier (default 2.5 = 250%)

        Returns:
            The created PublicHoliday
        """
        holiday_id = f"CUSTOM_{venue_id}_{holiday_date.isoformat()}_{uuid4().hex[:8]}"
        holiday = PublicHoliday(
            holiday_id=holiday_id,
            name=name,
            date=holiday_date,
            state=venue_id,  # Use venue_id as "state" for custom holidays
            holiday_type=HolidayType.CUSTOM,
            penalty_multiplier=penalty_multiplier,
        )

        if is_persistence_enabled and is_persistence_enabled():
            try:
                with write_txn() as conn:
                    from rosteriq.persistence import now_iso
                    conn.execute(
                        """
                        INSERT INTO custom_holidays
                        (holiday_id, venue_id, name, date, penalty_multiplier, created_at)
                        VALUES (?, ?, ?, ?, ?, ?)
                        """,
                        (
                            holiday_id,
                            venue_id,
                            name,
                            holiday_date.isoformat(),
                            penalty_multiplier,
                            now_iso(),
                        ),
                    )
            except Exception as e:
                logger.error(f"Failed to persist custom holiday: {e}")

        return holiday

    def get_custom_holidays(self, venue_id: str) -> List[PublicHoliday]:
        """
        Get all custom holidays for a venue.

        Args:
            venue_id: The venue identifier

        Returns:
            List of custom PublicHoliday objects
        """
        holidays = []

        if is_persistence_enabled and is_persistence_enabled():
            try:
                from rosteriq.persistence import connection
                conn = connection()
                rows = conn.execute(
                    """
                    SELECT holiday_id, name, date, penalty_multiplier
                    FROM custom_holidays
                    WHERE venue_id = ?
                    ORDER BY date
                    """,
                    (venue_id,),
                ).fetchall()

                for row in rows:
                    holidays.append(
                        PublicHoliday(
                            holiday_id=row[0],
                            name=row[1],
                            date=date.fromisoformat(row[2]),
                            state=venue_id,
                            holiday_type=HolidayType.CUSTOM,
                            penalty_multiplier=row[3],
                        )
                    )
            except Exception as e:
                logger.error(f"Failed to load custom holidays: {e}")

        return holidays

    def delete_custom_holiday(self, holiday_id: str) -> bool:
        """
        Delete a custom holiday by ID.

        Args:
            holiday_id: The holiday ID

        Returns:
            True if deleted, False if not found
        """
        if is_persistence_enabled and is_persistence_enabled():
            try:
                with write_txn() as conn:
                    cursor = conn.execute(
                        "DELETE FROM custom_holidays WHERE holiday_id = ?",
                        (holiday_id,),
                    )
                    return cursor.rowcount > 0
            except Exception as e:
                logger.error(f"Failed to delete custom holiday: {e}")
                return False

        return False


# Global store instance
_store = PublicHolidayStore()


def get_store() -> PublicHolidayStore:
    """Get the global PublicHolidayStore instance."""
    return _store
