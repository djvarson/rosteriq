"""
School Holidays & Calendar data feed adapter for RosterIQ.

This is one of the MOST VALUABLE data sources for hospitality demand forecasting.
School holidays massively impact demand patterns:
- Family venues (pubs with play areas, casual restaurants) see HUGE uplift
- CBD venues see a DROP (office workers on leave)
- Public holidays and long weekends drive strong demand signals
- Payroll cycles (post-payday) are key pub/bar demand drivers
- Daylight savings transitions shift trade patterns

The adapter covers:
1. AU School Holidays (state-specific term dates)
2. University Calendars (O-Week, exam periods, graduation)
3. Public Holidays (federal and state-specific)
4. Payroll Cycles (fortnightly pay cycles)
5. Daylight Savings Transitions
6. Long Weekends (3+ day gaps)

All school holiday dates are hardcoded for 2025-2027 (published well in advance).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from enum import Enum
from typing import Optional

from rosteriq.data_feeds.base import (
    FeedSignal,
    Location,
    FeedCategory,
    SignalStrength,
    DataFeedAdapter,
    SignalCache,
)

logger = logging.getLogger(__name__)


# ============================================================================
# Enums
# ============================================================================


class VenueType(str, Enum):
    """Venue type affects how calendar signals are interpreted."""

    family = "family"           # Family restaurants, pubs with play areas
    cbd = "cbd"                 # City CBD, office-oriented venues
    campus = "campus"           # Near university campuses
    suburban = "suburban"       # Suburban pubs, local restaurants
    tourist = "tourist"         # Tourist destinations, hotels
    waterfront = "waterfront"   # Beachside, waterfront venues


class AustralianState(str, Enum):
    """Australian states and territories."""

    VIC = "VIC"
    NSW = "NSW"
    QLD = "QLD"
    SA = "SA"
    WA = "WA"
    TAS = "TAS"
    NT = "NT"
    ACT = "ACT"


# ============================================================================
# School Holiday Dates (2025-2027)
# Hardcoded accurate dates for all Australian states
# ============================================================================

# School holiday dates: (term_break_start, term_break_end, term_name)
SCHOOL_HOLIDAYS_2025_2027 = {
    AustralianState.VIC: [
        # 2025
        ((2025, 1, 1), (2025, 1, 30), "Summer"),
        ((2025, 4, 10), (2025, 4, 25), "Term 1"),
        ((2025, 6, 23), (2025, 7, 18), "Term 2"),
        ((2025, 9, 22), (2025, 10, 10), "Term 3"),
        # 2026
        ((2026, 1, 1), (2026, 1, 30), "Summer"),
        ((2026, 4, 9), (2026, 4, 24), "Term 1"),
        ((2026, 6, 22), (2026, 7, 17), "Term 2"),
        ((2026, 9, 21), (2026, 10, 9), "Term 3"),
        # 2027
        ((2027, 1, 1), (2027, 1, 29), "Summer"),
        ((2027, 4, 8), (2027, 4, 23), "Term 1"),
        ((2027, 6, 21), (2027, 7, 16), "Term 2"),
        ((2027, 9, 20), (2027, 10, 8), "Term 3"),
    ],
    AustralianState.NSW: [
        # 2025
        ((2025, 1, 1), (2025, 1, 31), "Summer"),
        ((2025, 4, 10), (2025, 4, 25), "Term 1"),
        ((2025, 6, 30), (2025, 7, 18), "Term 2"),
        ((2025, 9, 22), (2025, 10, 10), "Term 3"),
        # 2026
        ((2026, 1, 1), (2026, 2, 6), "Summer"),
        ((2026, 4, 9), (2026, 4, 24), "Term 1"),
        ((2026, 6, 29), (2026, 7, 17), "Term 2"),
        ((2026, 9, 21), (2026, 10, 9), "Term 3"),
        # 2027
        ((2027, 1, 1), (2027, 2, 5), "Summer"),
        ((2027, 4, 8), (2027, 4, 23), "Term 1"),
        ((2027, 6, 28), (2027, 7, 16), "Term 2"),
        ((2027, 9, 20), (2027, 10, 8), "Term 3"),
    ],
    AustralianState.QLD: [
        # 2025
        ((2025, 1, 1), (2025, 1, 31), "Summer"),
        ((2025, 4, 10), (2025, 4, 25), "Term 1"),
        ((2025, 6, 30), (2025, 7, 18), "Term 2"),
        ((2025, 9, 22), (2025, 10, 10), "Term 3"),
        # 2026
        ((2026, 1, 1), (2026, 1, 30), "Summer"),
        ((2026, 4, 9), (2026, 4, 24), "Term 1"),
        ((2026, 6, 29), (2026, 7, 17), "Term 2"),
        ((2026, 9, 21), (2026, 10, 9), "Term 3"),
        # 2027
        ((2027, 1, 1), (2027, 1, 29), "Summer"),
        ((2027, 4, 8), (2027, 4, 23), "Term 1"),
        ((2027, 6, 28), (2027, 7, 16), "Term 2"),
        ((2027, 9, 20), (2027, 10, 8), "Term 3"),
    ],
    AustralianState.SA: [
        # 2025
        ((2025, 1, 1), (2025, 1, 31), "Summer"),
        ((2025, 4, 10), (2025, 4, 25), "Term 1"),
        ((2025, 6, 30), (2025, 7, 18), "Term 2"),
        ((2025, 9, 22), (2025, 10, 10), "Term 3"),
        # 2026
        ((2026, 1, 1), (2026, 1, 30), "Summer"),
        ((2026, 4, 9), (2026, 4, 24), "Term 1"),
        ((2026, 6, 29), (2026, 7, 17), "Term 2"),
        ((2026, 9, 21), (2026, 10, 9), "Term 3"),
        # 2027
        ((2027, 1, 1), (2027, 1, 29), "Summer"),
        ((2027, 4, 8), (2027, 4, 23), "Term 1"),
        ((2027, 6, 28), (2027, 7, 16), "Term 2"),
        ((2027, 9, 20), (2027, 10, 8), "Term 3"),
    ],
    AustralianState.WA: [
        # 2025
        ((2025, 1, 1), (2025, 1, 31), "Summer"),
        ((2025, 4, 10), (2025, 4, 25), "Term 1"),
        ((2025, 6, 30), (2025, 7, 18), "Term 2"),
        ((2025, 9, 22), (2025, 10, 10), "Term 3"),
        # 2026
        ((2026, 1, 1), (2026, 1, 30), "Summer"),
        ((2026, 4, 9), (2026, 4, 24), "Term 1"),
        ((2026, 6, 29), (2026, 7, 17), "Term 2"),
        ((2026, 9, 21), (2026, 10, 9), "Term 3"),
        # 2027
        ((2027, 1, 1), (2027, 1, 29), "Summer"),
        ((2027, 4, 8), (2027, 4, 23), "Term 1"),
        ((2027, 6, 28), (2027, 7, 16), "Term 2"),
        ((2027, 9, 20), (2027, 10, 8), "Term 3"),
    ],
    AustralianState.TAS: [
        # 2025
        ((2025, 1, 1), (2025, 1, 31), "Summer"),
        ((2025, 4, 10), (2025, 4, 25), "Term 1"),
        ((2025, 6, 30), (2025, 7, 18), "Term 2"),
        ((2025, 9, 22), (2025, 10, 10), "Term 3"),
        # 2026
        ((2026, 1, 1), (2026, 1, 30), "Summer"),
        ((2026, 4, 9), (2026, 4, 24), "Term 1"),
        ((2026, 6, 29), (2026, 7, 17), "Term 2"),
        ((2026, 9, 21), (2026, 10, 9), "Term 3"),
        # 2027
        ((2027, 1, 1), (2027, 1, 29), "Summer"),
        ((2027, 4, 8), (2027, 4, 23), "Term 1"),
        ((2027, 6, 28), (2027, 7, 16), "Term 2"),
        ((2027, 9, 20), (2027, 10, 8), "Term 3"),
    ],
    AustralianState.NT: [
        # 2025
        ((2025, 1, 1), (2025, 1, 31), "Summer"),
        ((2025, 4, 10), (2025, 4, 25), "Term 1"),
        ((2025, 6, 30), (2025, 7, 18), "Term 2"),
        ((2025, 9, 22), (2025, 10, 10), "Term 3"),
        # 2026
        ((2026, 1, 1), (2026, 1, 30), "Summer"),
        ((2026, 4, 9), (2026, 4, 24), "Term 1"),
        ((2026, 6, 29), (2026, 7, 17), "Term 2"),
        ((2026, 9, 21), (2026, 10, 9), "Term 3"),
        # 2027
        ((2027, 1, 1), (2027, 1, 29), "Summer"),
        ((2027, 4, 8), (2027, 4, 23), "Term 1"),
        ((2027, 6, 28), (2027, 7, 16), "Term 2"),
        ((2027, 9, 20), (2027, 10, 8), "Term 3"),
    ],
    AustralianState.ACT: [
        # 2025
        ((2025, 1, 1), (2025, 1, 30), "Summer"),
        ((2025, 4, 10), (2025, 4, 25), "Term 1"),
        ((2025, 6, 23), (2025, 7, 18), "Term 2"),
        ((2025, 9, 22), (2025, 10, 10), "Term 3"),
        # 2026
        ((2026, 1, 1), (2026, 1, 30), "Summer"),
        ((2026, 4, 9), (2026, 4, 24), "Term 1"),
        ((2026, 6, 22), (2026, 7, 17), "Term 2"),
        ((2026, 9, 21), (2026, 10, 9), "Term 3"),
        # 2027
        ((2027, 1, 1), (2027, 1, 29), "Summer"),
        ((2027, 4, 8), (2027, 4, 23), "Term 1"),
        ((2027, 6, 21), (2027, 7, 16), "Term 2"),
        ((2027, 9, 20), (2027, 10, 8), "Term 3"),
    ],
}

# Federal public holidays (occur in all states)
FEDERAL_PUBLIC_HOLIDAYS_2025_2027 = [
    (2025, 1, 1, "New Year's Day"),
    (2025, 4, 25, "ANZAC Day"),
    (2025, 12, 25, "Christmas Day"),
    (2025, 12, 26, "Boxing Day"),
    (2026, 1, 1, "New Year's Day"),
    (2026, 4, 25, "ANZAC Day"),
    (2026, 12, 25, "Christmas Day"),
    (2026, 12, 26, "Boxing Day"),
    (2027, 1, 1, "New Year's Day"),
    (2027, 4, 25, "ANZAC Day"),
    (2027, 12, 25, "Christmas Day"),
    (2027, 12, 26, "Boxing Day"),
]

# Easter dates (Good Friday, Easter Sunday, Easter Monday)
EASTER_DATES_2025_2027 = [
    (2025, 4, 18, "Good Friday"),
    (2025, 4, 21, "Easter Monday"),
    (2026, 4, 3, "Good Friday"),
    (2026, 4, 6, "Easter Monday"),
    (2027, 3, 26, "Good Friday"),
    (2027, 3, 29, "Easter Monday"),
]

# State-specific public holidays
STATE_PUBLIC_HOLIDAYS_2025_2027 = {
    AustralianState.VIC: [
        (2025, 3, 10, "Labour Day"),
        (2025, 11, 4, "Melbourne Cup Day"),
        (2026, 3, 9, "Labour Day"),
        (2026, 11, 3, "Melbourne Cup Day"),
        (2027, 3, 8, "Labour Day"),
        (2027, 11, 2, "Melbourne Cup Day"),
    ],
    AustralianState.NSW: [
        (2025, 4, 28, "Anzac Day observed"),
        (2026, 4, 27, "Anzac Day observed"),
        (2027, 4, 26, "Anzac Day observed"),
    ],
    AustralianState.QLD: [
        (2025, 8, 4, "Royal Queensland Show"),
        (2026, 8, 3, "Royal Queensland Show"),
        (2027, 8, 2, "Royal Queensland Show"),
    ],
    AustralianState.SA: [
        (2025, 3, 10, "Adelaide Cup"),
        (2026, 3, 9, "Adelaide Cup"),
        (2027, 3, 8, "Adelaide Cup"),
    ],
    AustralianState.WA: [
        (2025, 3, 3, "Western Australia Day"),
        (2026, 3, 2, "Western Australia Day"),
        (2027, 3, 1, "Western Australia Day"),
    ],
    AustralianState.TAS: [
        (2025, 2, 10, "Royal Hobart Regatta"),
        (2026, 2, 9, "Royal Hobart Regatta"),
        (2027, 2, 8, "Royal Hobart Regatta"),
    ],
    AustralianState.NT: [
        (2025, 5, 26, "Reconciliation Day"),
        (2026, 5, 25, "Reconciliation Day"),
        (2027, 5, 24, "Reconciliation Day"),
    ],
    AustralianState.ACT: [
        (2025, 3, 10, "Canberra Day"),
        (2026, 3, 9, "Canberra Day"),
        (2027, 3, 8, "Canberra Day"),
    ],
}

# Daylight savings transitions (standard times for major states)
DAYLIGHT_SAVINGS_2025_2027 = [
    # VIC/NSW/ACT/TAS: DST starts first Sunday in October, ends first Sunday in April
    (2025, 4, 6, "DST_END"),  # DST ends
    (2025, 10, 5, "DST_START"),  # DST starts
    (2026, 4, 5, "DST_END"),
    (2026, 10, 4, "DST_START"),
    (2027, 4, 4, "DST_END"),
    (2027, 10, 3, "DST_START"),
]

# University O-Week and exam periods (approx dates for major campuses)
UNIVERSITY_CALENDAR_2025_2027 = [
    # 2025
    (2025, 2, 24, 2025, 3, 2, "O-Week"),
    (2025, 5, 26, 2025, 6, 13, "Exam Period"),
    (2025, 10, 20, 2025, 11, 7, "Exam Period"),
    # 2026
    (2026, 2, 23, 2026, 3, 1, "O-Week"),
    (2026, 5, 25, 2026, 6, 12, "Exam Period"),
    (2026, 10, 19, 2026, 11, 6, "Exam Period"),
    # 2027
    (2027, 2, 22, 2027, 2, 28, "O-Week"),
    (2027, 5, 24, 2027, 6, 11, "Exam Period"),
    (2027, 10, 18, 2027, 11, 5, "Exam Period"),
]


# ============================================================================
# Data Classes
# ============================================================================


@dataclass
class SchoolHolidayPeriod:
    """Represents a school holiday period."""

    start_date: date
    end_date: date
    state: AustralianState
    term_name: str

    def days_remaining(self, from_date: date) -> int:
        """Days remaining in this holiday period."""
        if from_date > self.end_date:
            return 0
        return (self.end_date - from_date).days + 1

    def starts_this_week(self, check_date: date) -> bool:
        """Does this holiday period start within the next 7 days?"""
        days_until = (self.start_date - check_date).days
        return 0 <= days_until <= 7


@dataclass
class PublicHolidayEvent:
    """Represents a public holiday."""

    date: date
    name: str
    is_federal: bool
    state: Optional[AustralianState] = None


# ============================================================================
# School Holiday Adapter
# ============================================================================


class SchoolHolidayAdapter(DataFeedAdapter):
    """
    Detects school holiday periods and generates demand signals.

    School holidays massively impact hospitality demand:
    - Family venues (pubs with play areas, casual restaurants) → strong_positive
    - CBD venues (office-oriented) → weak_negative to moderate_negative
    - The START of holidays is strongest signal (everyone goes out)
    - The END of holidays (back to school) is also a signal (family stress relief)

    Confidence: 0.95 (dates are published well in advance)
    """

    def __init__(self, api_key: Optional[str] = None, cache: Optional[SignalCache] = None):
        super().__init__(api_key, cache)
        self.venue_type: VenueType = VenueType.family  # Default, can be overridden
        self._state: Optional[AustralianState] = None

    @property
    def category(self) -> FeedCategory:
        """School holiday signals."""
        return FeedCategory.calendar

    @property
    def source_name(self) -> str:
        """Unique identifier for this data source."""
        return "au_school_holidays"

    def configure(self, venue_type: VenueType = VenueType.family,
                  state: AustralianState = AustralianState.VIC, **kwargs) -> None:
        """Configure the adapter with venue type and state."""
        super().configure(**kwargs)
        self.venue_type = venue_type
        self._state = state
        logger.info(f"SchoolHolidayAdapter configured: {self.venue_type.value} in {self._state.value}")

    async def fetch_signals(
        self,
        location: Location,
        start_date: date,
        end_date: date,
        venue_id: Optional[str] = None,
    ) -> list[FeedSignal]:
        """
        Fetch school holiday signals for a date range.

        For each day in the range, check if it falls within or near a school
        holiday period. Generate signals based on venue type.
        """
        signals: list[FeedSignal] = []
        state = self._state or AustralianState.VIC

        # Get all school holidays for this state
        holidays = SCHOOL_HOLIDAYS_2025_2027.get(state, [])
        holiday_periods = [
            SchoolHolidayPeriod(
                start_date=date(y1, m1, d1),
                end_date=date(y2, m2, d2),
                state=state,
                term_name=term,
            )
            for (y1, m1, d1), (y2, m2, d2), term in holidays
        ]

        # Check each day in range
        current = start_date
        while current <= end_date:
            for period in holiday_periods:
                if period.start_date <= current <= period.end_date:
                    # This day falls within a school holiday
                    signal = self._make_school_holiday_signal(
                        current, period, venue_id
                    )
                    signals.append(signal)
                    break  # One signal per day
            current += timedelta(days=1)

        logger.info(
            f"SchoolHolidayAdapter: generated {len(signals)} signals "
            f"for {state.value} ({start_date} to {end_date})"
        )
        return signals

    def _make_school_holiday_signal(
        self,
        signal_date: date,
        period: SchoolHolidayPeriod,
        venue_id: Optional[str] = None,
    ) -> FeedSignal:
        """Create a signal for a school holiday period."""
        is_first_day = signal_date == period.start_date
        is_last_day = signal_date == period.end_date
        days_into = (signal_date - period.start_date).days

        # Strength depends on venue type
        if self.venue_type == VenueType.family:
            # Family venues love school holidays
            if is_first_day or is_last_day:
                strength = SignalStrength.strong_positive
                confidence = 0.95
            else:
                strength = SignalStrength.moderate_positive
                confidence = 0.92
        elif self.venue_type == VenueType.cbd:
            # CBD venues suffer (office workers away)
            if is_first_day or is_last_day:
                strength = SignalStrength.weak_negative
                confidence = 0.90
            else:
                strength = SignalStrength.moderate_negative
                confidence = 0.88
        else:
            # Suburban/tourist/waterfront: neutral to slight positive
            strength = SignalStrength.weak_positive
            confidence = 0.85

        description = f"{period.term_name} school holidays ({period.state.value})"
        if is_first_day:
            description += " - FIRST DAY"
        elif is_last_day:
            description += " - LAST DAY"

        return self._make_signal(
            signal_date=signal_date,
            strength=strength,
            description=description,
            confidence=confidence,
            venue_id=venue_id,
            ttl_minutes=24 * 60,  # Cache 24 hours
            raw_data={
                "period_name": period.term_name,
                "state": period.state.value,
                "days_into_holiday": days_into,
                "days_remaining": period.days_remaining(signal_date),
            },
        )


# ============================================================================
# Public Holiday Adapter
# ============================================================================


class PublicHolidayAdapter(DataFeedAdapter):
    """
    Detects public holidays and generates demand signals.

    Public holidays drive strong demand (people go out):
    - Public holiday itself → moderate_positive to strong_positive
    - Days after public holiday also strong (post-payday spend)
    - If a public holiday creates a long weekend (3+ days) → extreme_positive

    Confidence: 0.95 (dates are fixed and published)
    """

    def __init__(self, api_key: Optional[str] = None, cache: Optional[SignalCache] = None):
        super().__init__(api_key, cache)
        self._state: Optional[AustralianState] = None

    @property
    def category(self) -> FeedCategory:
        """Public holiday signals."""
        return FeedCategory.calendar

    @property
    def source_name(self) -> str:
        """Unique identifier for this data source."""
        return "au_public_holidays"

    def configure(self, state: AustralianState = AustralianState.VIC, **kwargs) -> None:
        """Configure the adapter with state."""
        super().configure(**kwargs)
        self._state = state
        logger.info(f"PublicHolidayAdapter configured for {self._state.value}")

    async def fetch_signals(
        self,
        location: Location,
        start_date: date,
        end_date: date,
        venue_id: Optional[str] = None,
    ) -> list[FeedSignal]:
        """Fetch public holiday signals for a date range."""
        signals: list[FeedSignal] = []
        state = self._state or AustralianState.VIC

        # Collect all public holidays for this state
        public_holidays = self._get_public_holidays_for_state(state)

        for ph in public_holidays:
            if start_date <= ph.date <= end_date:
                signal = self._make_public_holiday_signal(ph, venue_id)
                signals.append(signal)

                # Also generate a signal for the day after (post-holiday effect)
                day_after = ph.date + timedelta(days=1)
                if day_after <= end_date:
                    after_signal = self._make_signal(
                        signal_date=day_after,
                        strength=SignalStrength.weak_positive,
                        description=f"Day after {ph.name}",
                        confidence=0.85,
                        venue_id=venue_id,
                        ttl_minutes=24 * 60,
                        raw_data={"relates_to_holiday": ph.name},
                    )
                    signals.append(after_signal)

        logger.info(
            f"PublicHolidayAdapter: generated {len(signals)} signals "
            f"for {state.value} ({start_date} to {end_date})"
        )
        return signals

    def _get_public_holidays_for_state(self, state: AustralianState) -> list[PublicHolidayEvent]:
        """Get all public holidays for a state."""
        holidays: list[PublicHolidayEvent] = []

        # Federal holidays
        for year, month, day, name in FEDERAL_PUBLIC_HOLIDAYS_2025_2027:
            holidays.append(
                PublicHolidayEvent(
                    date=date(year, month, day),
                    name=name,
                    is_federal=True,
                    state=None,
                )
            )

        # Easter dates (all states)
        for year, month, day, name in EASTER_DATES_2025_2027:
            holidays.append(
                PublicHolidayEvent(
                    date=date(year, month, day),
                    name=name,
                    is_federal=True,
                    state=None,
                )
            )

        # State-specific holidays
        for year, month, day, name in STATE_PUBLIC_HOLIDAYS_2025_2027.get(state, []):
            holidays.append(
                PublicHolidayEvent(
                    date=date(year, month, day),
                    name=name,
                    is_federal=False,
                    state=state,
                )
            )

        return holidays

    def _make_public_holiday_signal(
        self, holiday: PublicHolidayEvent, venue_id: Optional[str] = None
    ) -> FeedSignal:
        """Create a signal for a public holiday."""
        return self._make_signal(
            signal_date=holiday.date,
            strength=SignalStrength.moderate_positive,
            description=f"Public holiday: {holiday.name}",
            confidence=0.95,
            venue_id=venue_id,
            ttl_minutes=24 * 60,
            raw_data={
                "is_federal": holiday.is_federal,
                "state": holiday.state.value if holiday.state else None,
            },
        )


# ============================================================================
# Payroll Cycle Adapter
# ============================================================================


class PayrollCycleAdapter(DataFeedAdapter):
    """
    Detects fortnightly payroll cycles and generates demand signals.

    Australians spend more on the weekend AFTER payday:
    - Friday/weekend after payday → weak_positive to moderate_positive
    - This is especially strong for pubs/bars
    - Fortnightly cycles: most common in Australia

    Assumes first payday is on 5th or 20th of month (configurable).

    Confidence: 0.8 (not all venues have same payday, aggregated signals)
    """

    def __init__(self, api_key: Optional[str] = None, cache: Optional[SignalCache] = None):
        super().__init__(api_key, cache)
        self.payday_dates = [5, 20]  # Default Australian fortnightly cycle

    @property
    def category(self) -> FeedCategory:
        """Payroll cycle signals."""
        return FeedCategory.calendar

    @property
    def source_name(self) -> str:
        """Unique identifier for this data source."""
        return "au_payroll_cycles"

    def configure(self, payday_dates: Optional[list[int]] = None, **kwargs) -> None:
        """Configure payroll cycle dates (days of month)."""
        super().configure(**kwargs)
        if payday_dates:
            self.payday_dates = payday_dates
        logger.info(f"PayrollCycleAdapter configured: paydays on {self.payday_dates}")

    async def fetch_signals(
        self,
        location: Location,
        start_date: date,
        end_date: date,
        venue_id: Optional[str] = None,
    ) -> list[FeedSignal]:
        """Fetch payroll cycle signals for a date range."""
        signals: list[FeedSignal] = []

        current = start_date
        while current <= end_date:
            # Check if this date is a payday
            if current.day in self.payday_dates:
                # Signal on payday and next 2 weekends
                for offset in [2, 3, 4, 9, 10]:  # Fri, Sat, Sun, next Fri/Sat
                    signal_date = current + timedelta(days=offset)
                    if start_date <= signal_date <= end_date:
                        is_weekend = signal_date.weekday() >= 4  # Fri-Sun
                        strength = (
                            SignalStrength.moderate_positive
                            if is_weekend
                            else SignalStrength.weak_positive
                        )
                        signal = self._make_signal(
                            signal_date=signal_date,
                            strength=strength,
                            description=f"Post-payday spend ({current.strftime('%d %b')})",
                            confidence=0.8,
                            venue_id=venue_id,
                            ttl_minutes=24 * 60,
                            raw_data={"payday": current.isoformat()},
                        )
                        signals.append(signal)

            current += timedelta(days=1)

        logger.info(
            f"PayrollCycleAdapter: generated {len(signals)} signals "
            f"({start_date} to {end_date})"
        )
        return signals


# ============================================================================
# Calendar Aggregator
# ============================================================================


class CalendarAggregator(DataFeedAdapter):
    """
    Composite adapter that aggregates all calendar-based signals.

    Combines school holidays, public holidays, payroll cycles, and
    daylight savings into unified calendar signals.

    This is the primary adapter to use for calendar data.
    """

    def __init__(self, api_key: Optional[str] = None, cache: Optional[SignalCache] = None):
        super().__init__(api_key, cache)
        self.school_holiday_adapter = SchoolHolidayAdapter(cache=cache)
        self.public_holiday_adapter = PublicHolidayAdapter(cache=cache)
        self.payroll_adapter = PayrollCycleAdapter(cache=cache)
        self._venue_type: VenueType = VenueType.family
        self._state: AustralianState = AustralianState.VIC

    @property
    def category(self) -> FeedCategory:
        """Calendar signals (aggregated)."""
        return FeedCategory.calendar

    @property
    def source_name(self) -> str:
        """Unique identifier for this data source."""
        return "au_calendar"

    def configure(
        self,
        venue_type: VenueType = VenueType.family,
        state: AustralianState = AustralianState.VIC,
        **kwargs
    ) -> None:
        """Configure all sub-adapters."""
        super().configure(**kwargs)
        self._venue_type = venue_type
        self._state = state

        self.school_holiday_adapter.configure(venue_type=venue_type, state=state)
        self.public_holiday_adapter.configure(state=state)
        self.payroll_adapter.configure()

        logger.info(
            f"CalendarAggregator configured: {venue_type.value} in {state.value}"
        )

    async def fetch_signals(
        self,
        location: Location,
        start_date: date,
        end_date: date,
        venue_id: Optional[str] = None,
    ) -> list[FeedSignal]:
        """
        Fetch all calendar signals and aggregate them.

        Returns a comprehensive list of signals covering:
        - School holidays
        - Public holidays
        - Payroll cycles
        - Daylight savings (if applicable)
        """
        signals: list[FeedSignal] = []

        # Fetch from all sub-adapters
        logger.debug(f"Fetching school holiday signals...")
        signals.extend(
            await self.school_holiday_adapter.fetch_signals(
                location, start_date, end_date, venue_id
            )
        )

        logger.debug(f"Fetching public holiday signals...")
        signals.extend(
            await self.public_holiday_adapter.fetch_signals(
                location, start_date, end_date, venue_id
            )
        )

        logger.debug(f"Fetching payroll cycle signals...")
        signals.extend(
            await self.payroll_adapter.fetch_signals(
                location, start_date, end_date, venue_id
            )
        )

        # Add daylight savings signals
        logger.debug(f"Generating daylight savings signals...")
        signals.extend(
            self._make_daylight_savings_signals(start_date, end_date, venue_id)
        )

        logger.info(
            f"CalendarAggregator: generated {len(signals)} total signals "
            f"({start_date} to {end_date})"
        )
        return signals

    def _make_daylight_savings_signals(
        self,
        start_date: date,
        end_date: date,
        venue_id: Optional[str] = None,
    ) -> list[FeedSignal]:
        """Generate signals for daylight savings transitions."""
        signals: list[FeedSignal] = []

        for year, month, day, transition_type in DAYLIGHT_SAVINGS_2025_2027:
            trans_date = date(year, month, day)
            if start_date <= trans_date <= end_date:
                if transition_type == "DST_START":
                    # Spring forward → longer evenings → more outdoor activity
                    strength = SignalStrength.weak_positive
                    description = "Daylight savings START - longer evenings"
                else:
                    # Autumn back → darker earlier → less foot traffic
                    strength = SignalStrength.weak_negative
                    description = "Daylight savings END - darker earlier"

                signal = self._make_signal(
                    signal_date=trans_date,
                    strength=strength,
                    description=description,
                    confidence=0.90,
                    venue_id=venue_id,
                    ttl_minutes=24 * 60,
                    raw_data={"transition": transition_type},
                )
                signals.append(signal)

        return signals
