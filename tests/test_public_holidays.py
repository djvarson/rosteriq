"""
Comprehensive test suite for Australian Public Holiday Manager.

Test coverage:
- Easter calculation (multiple years, verified against known dates)
- National holidays generation
- QLD state-specific holidays
- Holiday checking (is_public_holiday)
- Penalty multiplier calculation
- Substitute day logic (weekend holidays -> Monday)
- Custom venue holidays
- Upcoming holidays filtering
- Store persistence
"""

import unittest
from datetime import date, timedelta
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from rosteriq.public_holidays import (
    calculate_easter,
    generate_national_holidays,
    generate_state_holidays,
    get_holidays_for_year,
    is_public_holiday,
    get_penalty_multiplier,
    get_upcoming_holidays,
    apply_substitute_day,
    get_store,
    PublicHoliday,
    HolidayType,
    HolidayCalendar,
)

# Try to enable persistence for tests
try:
    from rosteriq.persistence import reset_for_tests, force_enable_for_tests
    PERSISTENCE_AVAILABLE = True
except ImportError:
    PERSISTENCE_AVAILABLE = False


class TestEasterCalculation(unittest.TestCase):
    """Test Easter Sunday calculation using Anonymous Gregorian algorithm."""

    def test_easter_2024(self):
        """Easter 2024 should be March 31."""
        easter = calculate_easter(2024)
        self.assertEqual(easter, date(2024, 3, 31))

    def test_easter_2025(self):
        """Easter 2025 should be April 20."""
        easter = calculate_easter(2025)
        self.assertEqual(easter, date(2025, 4, 20))

    def test_easter_2026(self):
        """Easter 2026 should be April 5."""
        easter = calculate_easter(2026)
        self.assertEqual(easter, date(2026, 4, 5))

    def test_easter_2027(self):
        """Easter 2027 should be March 28."""
        easter = calculate_easter(2027)
        self.assertEqual(easter, date(2027, 3, 28))

    def test_easter_2023(self):
        """Easter 2023 should be April 9."""
        easter = calculate_easter(2023)
        self.assertEqual(easter, date(2023, 4, 9))

    def test_easter_2020(self):
        """Easter 2020 should be April 12."""
        easter = calculate_easter(2020)
        self.assertEqual(easter, date(2020, 4, 12))

    def test_easter_returns_date(self):
        """Easter calculation should return a date object."""
        easter = calculate_easter(2026)
        self.assertIsInstance(easter, date)

    def test_easter_is_sunday(self):
        """Easter should always be a Sunday (weekday=6)."""
        for year in [2020, 2021, 2022, 2023, 2024, 2025, 2026]:
            easter = calculate_easter(year)
            self.assertEqual(easter.weekday(), 6, f"Easter {year} not on Sunday")


class TestNationalHolidaysGeneration(unittest.TestCase):
    """Test generation of national public holidays."""

    def test_national_holidays_2026_count(self):
        """Should generate exactly 9 national holidays for 2026."""
        holidays = generate_national_holidays(2026)
        self.assertEqual(len(holidays), 9)

    def test_national_holidays_2025_count(self):
        """Should generate exactly 9 national holidays for 2025."""
        holidays = generate_national_holidays(2025)
        self.assertEqual(len(holidays), 9)

    def test_new_years_day_2026(self):
        """New Year's Day should be Jan 1."""
        holidays = generate_national_holidays(2026)
        new_year = [h for h in holidays if h.name == "New Year's Day"]
        self.assertEqual(len(new_year), 1)
        self.assertEqual(new_year[0].date, date(2026, 1, 1))

    def test_australia_day_2026(self):
        """Australia Day should be Jan 26."""
        holidays = generate_national_holidays(2026)
        aus_day = [h for h in holidays if h.name == "Australia Day"]
        self.assertEqual(len(aus_day), 1)
        self.assertEqual(aus_day[0].date, date(2026, 1, 26))

    def test_good_friday_2026(self):
        """Good Friday 2026 should be April 3."""
        holidays = generate_national_holidays(2026)
        good_friday = [h for h in holidays if h.name == "Good Friday"]
        self.assertEqual(len(good_friday), 1)
        self.assertEqual(good_friday[0].date, date(2026, 4, 3))

    def test_easter_saturday_2026(self):
        """Easter Saturday 2026 should be April 4."""
        holidays = generate_national_holidays(2026)
        easter_sat = [h for h in holidays if h.name == "Easter Saturday"]
        self.assertEqual(len(easter_sat), 1)
        self.assertEqual(easter_sat[0].date, date(2026, 4, 4))

    def test_easter_monday_2026(self):
        """Easter Monday 2026 should be April 6."""
        holidays = generate_national_holidays(2026)
        easter_mon = [h for h in holidays if h.name == "Easter Monday"]
        self.assertEqual(len(easter_mon), 1)
        self.assertEqual(easter_mon[0].date, date(2026, 4, 6))

    def test_anzac_day_2026(self):
        """ANZAC Day should always be April 25."""
        holidays = generate_national_holidays(2026)
        anzac = [h for h in holidays if h.name == "ANZAC Day"]
        self.assertEqual(len(anzac), 1)
        self.assertEqual(anzac[0].date, date(2026, 4, 25))

    def test_christmas_day_2026(self):
        """Christmas Day should be Dec 25."""
        holidays = generate_national_holidays(2026)
        christmas = [h for h in holidays if h.name == "Christmas Day"]
        self.assertEqual(len(christmas), 1)
        self.assertEqual(christmas[0].date, date(2026, 12, 25))

    def test_boxing_day_2026(self):
        """Boxing Day should be Dec 26."""
        holidays = generate_national_holidays(2026)
        boxing = [h for h in holidays if h.name == "Boxing Day"]
        self.assertEqual(len(boxing), 1)
        self.assertEqual(boxing[0].date, date(2026, 12, 26))

    def test_all_national_holidays_marked_national(self):
        """All national holidays should have type NATIONAL."""
        holidays = generate_national_holidays(2026)
        for h in holidays:
            self.assertEqual(h.holiday_type, HolidayType.NATIONAL)

    def test_all_national_holidays_state_all(self):
        """All national holidays should have state='ALL'."""
        holidays = generate_national_holidays(2026)
        for h in holidays:
            self.assertEqual(h.state, "ALL")

    def test_national_holidays_have_ids(self):
        """All national holidays should have unique IDs."""
        holidays = generate_national_holidays(2026)
        ids = [h.holiday_id for h in holidays]
        self.assertEqual(len(ids), len(set(ids)))

    def test_national_holidays_sorted_by_date(self):
        """National holidays should be sorted by date when generated."""
        holidays = generate_national_holidays(2026)
        dates = [h.date for h in holidays]
        self.assertEqual(dates, sorted(dates))


class TestStateHolidaysQLD(unittest.TestCase):
    """Test generation of QLD-specific public holidays."""

    def test_qld_holidays_2026_count(self):
        """QLD should have 3 state-specific holidays in 2026: Ekka, Reconciliation Day, Queen's Birthday."""
        holidays = generate_state_holidays(2026, "QLD")
        self.assertEqual(len(holidays), 3)

    def test_qld_ekka_2026(self):
        """QLD Ekka should be second Wednesday in August."""
        holidays = generate_state_holidays(2026, "QLD")
        ekka = [h for h in holidays if h.name == "Royal Queensland Show (Ekka)"]
        self.assertEqual(len(ekka), 1)
        ekka_date = ekka[0].date
        # August 1, 2026 is a Saturday; second Wednesday is Aug 12
        self.assertEqual(ekka_date, date(2026, 8, 12))

    def test_qld_reconciliation_day_2026(self):
        """QLD Reconciliation Day should be May 27 (from 2026 onwards)."""
        holidays = generate_state_holidays(2026, "QLD")
        recon = [h for h in holidays if h.name == "Reconciliation Day"]
        self.assertEqual(len(recon), 1)
        self.assertEqual(recon[0].date, date(2026, 5, 27))

    def test_qld_reconciliation_day_2025_missing(self):
        """QLD Reconciliation Day should NOT exist in 2025."""
        holidays = generate_state_holidays(2025, "QLD")
        recon = [h for h in holidays if h.name == "Reconciliation Day"]
        self.assertEqual(len(recon), 0)

    def test_qld_queens_birthday_2026(self):
        """QLD Queen's Birthday should be first Monday in October."""
        holidays = generate_state_holidays(2026, "QLD")
        queens = [h for h in holidays if h.name == "Queen's Birthday"]
        self.assertEqual(len(queens), 1)
        # October 1, 2026 is a Thursday; first Monday is Oct 5
        self.assertEqual(queens[0].date, date(2026, 10, 5))

    def test_qld_holidays_marked_state(self):
        """QLD state holidays should have type STATE."""
        holidays = generate_state_holidays(2026, "QLD")
        for h in holidays:
            self.assertEqual(h.holiday_type, HolidayType.STATE)

    def test_qld_holidays_state_qld(self):
        """QLD state holidays should have state='QLD'."""
        holidays = generate_state_holidays(2026, "QLD")
        for h in holidays:
            self.assertEqual(h.state, "QLD")


class TestHolidayCalendar(unittest.TestCase):
    """Test HolidayCalendar generation and composition."""

    def test_get_holidays_all_2026_national_only(self):
        """'ALL' should return only national holidays."""
        calendar = get_holidays_for_year(2026, "ALL")
        self.assertEqual(calendar.year, 2026)
        self.assertEqual(calendar.state, "ALL")
        self.assertEqual(len(calendar.holidays), 9)
        for h in calendar.holidays:
            self.assertEqual(h.holiday_type, HolidayType.NATIONAL)

    def test_get_holidays_qld_2026_combined(self):
        """QLD 2026 should have national + state holidays."""
        calendar = get_holidays_for_year(2026, "QLD")
        self.assertEqual(calendar.year, 2026)
        self.assertEqual(calendar.state, "QLD")
        # 9 national + 3 QLD state = 12
        self.assertEqual(len(calendar.holidays), 12)

    def test_get_holidays_case_insensitive(self):
        """State abbreviation should be case-insensitive."""
        cal1 = get_holidays_for_year(2026, "qld")
        cal2 = get_holidays_for_year(2026, "QLD")
        self.assertEqual(len(cal1.holidays), len(cal2.holidays))

    def test_get_holidays_sorted_by_date(self):
        """Calendar holidays should be sorted by date."""
        calendar = get_holidays_for_year(2026, "QLD")
        dates = [h.date for h in calendar.holidays]
        self.assertEqual(dates, sorted(dates))

    def test_calendar_to_dict(self):
        """Calendar should serialize to dict correctly."""
        calendar = get_holidays_for_year(2026, "QLD")
        d = calendar.to_dict()
        self.assertEqual(d["year"], 2026)
        self.assertEqual(d["state"], "QLD")
        self.assertIn("holidays", d)
        self.assertEqual(len(d["holidays"]), 12)


class TestIsPublicHoliday(unittest.TestCase):
    """Test public holiday checking."""

    def test_new_years_day_2026_is_holiday(self):
        """Jan 1 2026 should be a public holiday."""
        is_hol, holiday = is_public_holiday(date(2026, 1, 1), "QLD")
        self.assertTrue(is_hol)
        self.assertIsNotNone(holiday)
        self.assertEqual(holiday.name, "New Year's Day")

    def test_random_date_not_holiday(self):
        """A random Tuesday should not be a holiday."""
        is_hol, holiday = is_public_holiday(date(2026, 3, 17), "QLD")
        self.assertFalse(is_hol)
        self.assertIsNone(holiday)

    def test_christmas_2026_is_holiday(self):
        """Dec 25 2026 should be a public holiday."""
        is_hol, holiday = is_public_holiday(date(2026, 12, 25), "QLD")
        self.assertTrue(is_hol)
        self.assertIsNotNone(holiday)
        self.assertEqual(holiday.name, "Christmas Day")

    def test_ekka_2026_is_holiday_qld(self):
        """Ekka (Aug 12 2026) should be a holiday in QLD."""
        is_hol, holiday = is_public_holiday(date(2026, 8, 12), "QLD")
        self.assertTrue(is_hol)
        self.assertEqual(holiday.name, "Royal Queensland Show (Ekka)")

    def test_ekka_2026_is_holiday_all(self):
        """Ekka should NOT be a holiday when checking 'ALL' (national only)."""
        is_hol, holiday = is_public_holiday(date(2026, 8, 12), "ALL")
        self.assertFalse(is_hol)

    def test_reconciliation_day_2026(self):
        """Reconciliation Day (May 27 2026) should be a holiday in QLD."""
        is_hol, holiday = is_public_holiday(date(2026, 5, 27), "QLD")
        self.assertTrue(is_hol)
        self.assertEqual(holiday.name, "Reconciliation Day")


class TestPenaltyMultiplier(unittest.TestCase):
    """Test penalty multiplier calculation."""

    def test_non_holiday_penalty_is_1_0(self):
        """Non-holiday should return 1.0 multiplier."""
        mult = get_penalty_multiplier(date(2026, 3, 17), "QLD", "casual")
        self.assertEqual(mult, 1.0)

    def test_christmas_casual_penalty(self):
        """Christmas casual penalty should be 2.5 (125% base + 25% loading)."""
        mult = get_penalty_multiplier(date(2026, 12, 25), "QLD", "casual")
        self.assertEqual(mult, 2.5)

    def test_christmas_full_time_penalty(self):
        """Christmas full-time penalty should be 2.5 (250%)."""
        mult = get_penalty_multiplier(date(2026, 12, 25), "QLD", "full_time")
        self.assertEqual(mult, 2.5)

    def test_christmas_part_time_penalty(self):
        """Christmas part-time penalty should be 2.5 (250%)."""
        mult = get_penalty_multiplier(date(2026, 12, 25), "QLD", "part_time")
        self.assertEqual(mult, 2.5)

    def test_penalty_case_insensitive(self):
        """Employment type should be case-insensitive."""
        mult1 = get_penalty_multiplier(date(2026, 12, 25), "QLD", "CASUAL")
        mult2 = get_penalty_multiplier(date(2026, 12, 25), "QLD", "casual")
        self.assertEqual(mult1, mult2)


class TestSubstituteDay(unittest.TestCase):
    """Test substitute day logic for weekend holidays."""

    def test_substitute_saturday_to_monday(self):
        """Holiday falling on Saturday should substitute to Monday."""
        # Find a Saturday holiday or create one
        holiday = PublicHoliday(
            holiday_id="test_sat",
            name="Test Saturday Holiday",
            date=date(2026, 4, 25),  # ANZAC Day 2026 is a Saturday
            state="ALL",
            holiday_type=HolidayType.NATIONAL,
        )
        updated = apply_substitute_day(holiday)
        self.assertEqual(updated.substitute_date, date(2026, 4, 27))  # Monday

    def test_substitute_sunday_to_monday(self):
        """Holiday falling on Sunday should substitute to Monday."""
        # Christmas 2022 was a Sunday
        holiday = PublicHoliday(
            holiday_id="test_sun",
            name="Test Sunday Holiday",
            date=date(2022, 12, 25),
            state="ALL",
            holiday_type=HolidayType.NATIONAL,
        )
        updated = apply_substitute_day(holiday)
        self.assertEqual(updated.substitute_date, date(2022, 12, 26))  # Monday

    def test_no_substitute_weekday(self):
        """Holiday on weekday should have no substitute date."""
        # Christmas 2026 is a Thursday
        holiday = PublicHoliday(
            holiday_id="test_weekday",
            name="Test Weekday Holiday",
            date=date(2026, 12, 25),
            state="ALL",
            holiday_type=HolidayType.NATIONAL,
        )
        updated = apply_substitute_day(holiday)
        self.assertIsNone(updated.substitute_date)


class TestUpcomingHolidays(unittest.TestCase):
    """Test upcoming holidays filtering."""

    def test_upcoming_holidays_within_range(self):
        """get_upcoming_holidays should return only holidays within days_ahead."""
        # Get upcoming from Jan 1 (or today)
        upcoming = get_upcoming_holidays("QLD", days_ahead=90)
        self.assertGreater(len(upcoming), 0)

        # Check all are in future and within 90 days
        today = date.today()
        cutoff = today + timedelta(days=90)
        for h in upcoming:
            self.assertGreaterEqual(h.date, today)
            self.assertLessEqual(h.date, cutoff)

    def test_upcoming_holidays_sorted_by_date(self):
        """Upcoming holidays should be sorted by date."""
        upcoming = get_upcoming_holidays("QLD", days_ahead=365)
        dates = [h.date for h in upcoming]
        self.assertEqual(dates, sorted(dates))

    def test_upcoming_holidays_max_days(self):
        """Should respect max days_ahead parameter."""
        upcoming_90 = get_upcoming_holidays("QLD", days_ahead=90)
        upcoming_365 = get_upcoming_holidays("QLD", days_ahead=365)
        # Fewer holidays in 90 days than 365 days
        self.assertLessEqual(len(upcoming_90), len(upcoming_365))


class TestPublicHolidayStore(unittest.TestCase):
    """Test SQLite-persisted custom holiday store."""

    def setUp(self):
        """Reset persistence before each test."""
        if PERSISTENCE_AVAILABLE:
            reset_for_tests()
            force_enable_for_tests(True)

    def tearDown(self):
        """Clean up after each test."""
        if PERSISTENCE_AVAILABLE:
            force_enable_for_tests(False)
            reset_for_tests()

    def test_store_instance(self):
        """Should return a valid store instance."""
        store = get_store()
        self.assertIsNotNone(store)

    def test_add_custom_holiday(self):
        """Should add a custom holiday to the store."""
        if not PERSISTENCE_AVAILABLE:
            self.skipTest("Persistence not available")

        store = get_store()
        holiday = store.add_custom_holiday(
            "venue_123", "Staff Day", date(2026, 6, 15), 2.5
        )

        self.assertEqual(holiday.name, "Staff Day")
        self.assertEqual(holiday.date, date(2026, 6, 15))
        self.assertEqual(holiday.penalty_multiplier, 2.5)
        self.assertEqual(holiday.holiday_type, HolidayType.CUSTOM)

    def test_get_custom_holidays(self):
        """Should retrieve custom holidays for a venue."""
        if not PERSISTENCE_AVAILABLE:
            self.skipTest("Persistence not available")

        store = get_store()
        store.add_custom_holiday("venue_123", "Staff Day", date(2026, 6, 15))
        store.add_custom_holiday("venue_123", "Family Day", date(2026, 7, 20))

        holidays = store.get_custom_holidays("venue_123")
        self.assertEqual(len(holidays), 2)
        names = [h.name for h in holidays]
        self.assertIn("Staff Day", names)
        self.assertIn("Family Day", names)

    def test_delete_custom_holiday(self):
        """Should delete a custom holiday."""
        if not PERSISTENCE_AVAILABLE:
            self.skipTest("Persistence not available")

        store = get_store()
        holiday = store.add_custom_holiday("venue_123", "Staff Day", date(2026, 6, 15))

        # Delete it
        success = store.delete_custom_holiday(holiday.holiday_id)
        self.assertTrue(success)

        # Should not be retrievable
        holidays = store.get_custom_holidays("venue_123")
        self.assertEqual(len(holidays), 0)

    def test_delete_nonexistent_holiday(self):
        """Should return False when deleting nonexistent holiday."""
        if not PERSISTENCE_AVAILABLE:
            self.skipTest("Persistence not available")

        store = get_store()
        success = store.delete_custom_holiday("nonexistent_id")
        self.assertFalse(success)

    def test_custom_holidays_sorted_by_date(self):
        """Custom holidays should be sorted by date."""
        if not PERSISTENCE_AVAILABLE:
            self.skipTest("Persistence not available")

        store = get_store()
        store.add_custom_holiday("venue_123", "Day 3", date(2026, 7, 20))
        store.add_custom_holiday("venue_123", "Day 1", date(2026, 6, 15))
        store.add_custom_holiday("venue_123", "Day 2", date(2026, 6, 20))

        holidays = store.get_custom_holidays("venue_123")
        dates = [h.date for h in holidays]
        self.assertEqual(dates, sorted(dates))


class TestPublicHolidayDataStructures(unittest.TestCase):
    """Test data structure serialization."""

    def test_holiday_to_dict(self):
        """PublicHoliday should serialize to dict."""
        h = PublicHoliday(
            holiday_id="test_123",
            name="Test Holiday",
            date=date(2026, 12, 25),
            state="QLD",
            holiday_type=HolidayType.NATIONAL,
            penalty_multiplier=2.5,
        )
        d = h.to_dict()
        self.assertEqual(d["holiday_id"], "test_123")
        self.assertEqual(d["name"], "Test Holiday")
        self.assertEqual(d["date"], "2026-12-25")
        self.assertEqual(d["state"], "QLD")
        self.assertEqual(d["holiday_type"], "national")
        self.assertEqual(d["penalty_multiplier"], 2.5)

    def test_holiday_to_dict_with_substitute(self):
        """Holiday with substitute_date should serialize correctly."""
        h = PublicHoliday(
            holiday_id="test_123",
            name="Test Holiday",
            date=date(2026, 4, 25),
            state="ALL",
            holiday_type=HolidayType.NATIONAL,
            substitute_date=date(2026, 4, 27),
        )
        d = h.to_dict()
        self.assertEqual(d["substitute_date"], "2026-04-27")

    def test_holiday_to_dict_without_substitute(self):
        """Holiday without substitute_date should have None in dict."""
        h = PublicHoliday(
            holiday_id="test_123",
            name="Test Holiday",
            date=date(2026, 12, 25),
            state="ALL",
            holiday_type=HolidayType.NATIONAL,
        )
        d = h.to_dict()
        self.assertIsNone(d["substitute_date"])


if __name__ == "__main__":
    unittest.main()
