"""Test suite for fatigue_manager.py module.

Tests the FatigueManager and related functions with 25+ test cases covering:
- Fatigue scoring (fresh employee, overworked, night shifts)
- Risk classification (LOW/MODERATE/HIGH/CRITICAL)
- Consecutive day counting
- Weekly hour calculation
- Pre-check before assigning shifts (would_exceed_limits)
- Roster-wide assessment
- Recommendations generation
- Store persistence
- Alert creation
"""

import sys
import os
import unittest
import tempfile
from datetime import datetime, timezone, date, timedelta

# Add parent to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from rosteriq.fatigue_manager import (
    calculate_fatigue_score,
    classify_risk,
    assess_fatigue,
    check_roster_fatigue,
    generate_recommendations,
    would_exceed_limits,
    get_fatigue_store,
    FatigueRiskLevel,
    FatigueAssessment,
    FatigueAlert,
    DEFAULT_RULES,
    _reset_for_tests,
    _count_consecutive_days,
    _find_last_day_off,
    _is_night_shift,
    _time_diff_hours,
)
from rosteriq import persistence as _p


class TestFatigueScoringFresh(unittest.TestCase):
    """Test fatigue scoring for fresh employees with minimal shifts."""

    def setUp(self):
        _reset_for_tests()
        _p.reset_for_tests()

    def test_score_no_shifts(self):
        """Fresh employee with no shifts should score 0."""
        score = calculate_fatigue_score(
            weekly_hours=0,
            consecutive_days=0,
            night_shifts=0,
            last_day_off_days_ago=0,
        )
        self.assertEqual(score, 0)

    def test_score_light_work_week(self):
        """Light work week (30 hours) should score LOW."""
        score = calculate_fatigue_score(
            weekly_hours=30,
            consecutive_days=3,
            night_shifts=0,
            last_day_off_days_ago=1,
        )
        self.assertLess(score, 30)

    def test_score_normal_work_week(self):
        """Normal 40-hour week should score LOW."""
        score = calculate_fatigue_score(
            weekly_hours=40,
            consecutive_days=5,
            night_shifts=0,
            last_day_off_days_ago=2,
        )
        self.assertLess(score, 30)


class TestFatigueScoringSevere(unittest.TestCase):
    """Test fatigue scoring for overworked employees."""

    def setUp(self):
        _reset_for_tests()
        _p.reset_for_tests()

    def test_score_exceeded_weekly_hours(self):
        """Exceeded weekly hours (55h) should increase score."""
        score = calculate_fatigue_score(
            weekly_hours=55,
            consecutive_days=5,
            night_shifts=0,
            last_day_off_days_ago=2,
        )
        self.assertGreater(score, 10)

    def test_score_consecutive_days_exceeded(self):
        """7 consecutive days should increase score."""
        score = calculate_fatigue_score(
            weekly_hours=40,
            consecutive_days=7,
            night_shifts=0,
            last_day_off_days_ago=7,
        )
        self.assertGreater(score, 20)

    def test_score_night_shifts_penalty(self):
        """Night shifts should increase score."""
        score_day = calculate_fatigue_score(
            weekly_hours=40,
            consecutive_days=5,
            night_shifts=0,
            last_day_off_days_ago=2,
        )
        score_night = calculate_fatigue_score(
            weekly_hours=40,
            consecutive_days=5,
            night_shifts=3,
            last_day_off_days_ago=2,
        )
        self.assertGreater(score_night, score_day)

    def test_score_no_rest_penalty(self):
        """No day off in 10 days should increase score."""
        score = calculate_fatigue_score(
            weekly_hours=40,
            consecutive_days=6,
            night_shifts=0,
            last_day_off_days_ago=10,
        )
        self.assertGreater(score, 20)

    def test_score_critical_overwork(self):
        """Critically overworked should score >75."""
        score = calculate_fatigue_score(
            weekly_hours=60,
            consecutive_days=7,
            night_shifts=4,
            last_day_off_days_ago=12,
        )
        self.assertGreater(score, 75)


class TestRiskClassification(unittest.TestCase):
    """Test risk level classification."""

    def setUp(self):
        _reset_for_tests()
        _p.reset_for_tests()

    def test_classify_low_risk(self):
        """Score <30 should be LOW."""
        risk = classify_risk(15)
        self.assertEqual(risk, FatigueRiskLevel.LOW)

    def test_classify_moderate_risk(self):
        """Score 30-49 should be MODERATE."""
        risk = classify_risk(40)
        self.assertEqual(risk, FatigueRiskLevel.MODERATE)

    def test_classify_high_risk(self):
        """Score 50-74 should be HIGH."""
        risk = classify_risk(60)
        self.assertEqual(risk, FatigueRiskLevel.HIGH)

    def test_classify_critical_risk(self):
        """Score >75 should be CRITICAL."""
        risk = classify_risk(80)
        self.assertEqual(risk, FatigueRiskLevel.CRITICAL)

    def test_classify_boundary_moderate_high(self):
        """Score 50 is boundary between MODERATE and HIGH."""
        risk = classify_risk(50)
        self.assertEqual(risk, FatigueRiskLevel.HIGH)

    def test_classify_boundary_high_critical(self):
        """Score 75 is boundary between HIGH and CRITICAL."""
        risk = classify_risk(75)
        self.assertEqual(risk, FatigueRiskLevel.CRITICAL)


class TestConsecutiveDaysCounting(unittest.TestCase):
    """Test consecutive days calculation."""

    def setUp(self):
        _reset_for_tests()
        _p.reset_for_tests()

    def test_count_zero_days(self):
        """No shifts should return 0."""
        count = _count_consecutive_days([])
        self.assertEqual(count, 0)

    def test_count_single_day(self):
        """Single shift should count as 1 day."""
        today = date.today()
        shifts = [{"date": today, "start": "09:00", "end": "17:00"}]
        count = _count_consecutive_days(shifts)
        self.assertEqual(count, 1)

    def test_count_consecutive_5_days(self):
        """5 consecutive days should count correctly."""
        today = date.today()
        shifts = [
            {"date": today - timedelta(days=i), "start": "09:00", "end": "17:00"}
            for i in range(5)
        ]
        count = _count_consecutive_days(shifts)
        self.assertEqual(count, 5)

    def test_count_with_gap_resets(self):
        """Gap in shifts should reset count."""
        today = date.today()
        shifts = [
            {"date": today, "start": "09:00", "end": "17:00"},
            {"date": today - timedelta(days=1), "start": "09:00", "end": "17:00"},
            {"date": today - timedelta(days=3), "start": "09:00", "end": "17:00"},
        ]
        count = _count_consecutive_days(shifts)
        # Should count from most recent consecutive (2 days: today and yesterday)
        self.assertEqual(count, 2)


class TestLastDayOff(unittest.TestCase):
    """Test finding last day off."""

    def setUp(self):
        _reset_for_tests()
        _p.reset_for_tests()

    def test_find_last_day_off_no_shifts(self):
        """No shifts should return None."""
        last_off = _find_last_day_off([])
        self.assertIsNone(last_off)

    def test_find_last_day_off_with_day_off(self):
        """Should find day with no shift."""
        today = date.today()
        shifts = [
            {"date": today, "start": "09:00", "end": "17:00"},
            {"date": today - timedelta(days=2), "start": "09:00", "end": "17:00"},
        ]
        last_off = _find_last_day_off(shifts)
        # Should find yesterday (no shift on that date)
        self.assertEqual(last_off, today - timedelta(days=1))

    def test_find_last_day_off_recent(self):
        """Should prioritize most recent day off."""
        today = date.today()
        shifts = [
            {"date": today, "start": "09:00", "end": "17:00"},
            {"date": today - timedelta(days=1), "start": "09:00", "end": "17:00"},
        ]
        last_off = _find_last_day_off(shifts)
        # Most recent day off would be 2 days ago (or earlier)
        self.assertIsNotNone(last_off)
        self.assertLessEqual(last_off, today - timedelta(days=2))


class TestNightShiftDetection(unittest.TestCase):
    """Test night shift detection."""

    def setUp(self):
        _reset_for_tests()
        _p.reset_for_tests()

    def test_night_shift_ends_midnight(self):
        """Shift ending at 00:00 should be night shift."""
        is_night = _is_night_shift({"end": "00:00"})
        self.assertTrue(is_night)

    def test_night_shift_ends_early_morning(self):
        """Shift ending at 04:00 should be night shift."""
        is_night = _is_night_shift({"end": "04:00"})
        self.assertTrue(is_night)

    def test_day_shift_ends_afternoon(self):
        """Shift ending at 17:00 should NOT be night shift."""
        is_night = _is_night_shift({"end": "17:00"})
        self.assertFalse(is_night)

    def test_night_shift_ends_late_evening(self):
        """Shift ending at 22:00 should be night shift."""
        is_night = _is_night_shift({"end": "22:00"})
        self.assertTrue(is_night)

    def test_day_shift_ends_morning(self):
        """Shift ending at 14:00 should NOT be night shift."""
        is_night = _is_night_shift({"end": "14:00"})
        self.assertFalse(is_night)


class TestTimeDiffHours(unittest.TestCase):
    """Test time difference calculation."""

    def setUp(self):
        _reset_for_tests()
        _p.reset_for_tests()

    def test_same_day_shift(self):
        """09:00 to 17:00 should be 8 hours."""
        hours = _time_diff_hours("09:00", "17:00")
        self.assertEqual(hours, 8.0)

    def test_overnight_shift(self):
        """22:00 to 06:00 should be 8 hours."""
        hours = _time_diff_hours("22:00", "06:00")
        self.assertEqual(hours, 8.0)

    def test_short_shift(self):
        """09:00 to 11:00 should be 2 hours."""
        hours = _time_diff_hours("09:00", "11:00")
        self.assertEqual(hours, 2.0)

    def test_exact_5_hours(self):
        """09:00 to 14:00 should be exactly 5 hours."""
        hours = _time_diff_hours("09:00", "14:00")
        self.assertEqual(hours, 5.0)


class TestAssessmentComprehensive(unittest.TestCase):
    """Test comprehensive fatigue assessment."""

    def setUp(self):
        _reset_for_tests()
        _p.reset_for_tests()

    def test_assess_fresh_employee(self):
        """Fresh employee should get LOW risk."""
        assessment = assess_fatigue(
            employee_id="emp_001",
            employee_name="Alice Smith",
            venue_id="venue_001",
            shifts_7_days=[],
            shifts_14_days=[],
        )
        self.assertEqual(assessment.risk_level, FatigueRiskLevel.LOW)
        self.assertEqual(assessment.score, 0)
        self.assertEqual(assessment.weekly_hours, 0)

    def test_assess_overworked_employee(self):
        """Overworked employee should get HIGH/CRITICAL risk."""
        today = date.today()
        shifts_7 = [
            {"date": today - timedelta(days=i), "start": "08:00", "end": "20:00"}
            for i in range(7)
        ]
        assessment = assess_fatigue(
            employee_id="emp_002",
            employee_name="Bob Jones",
            venue_id="venue_001",
            shifts_7_days=shifts_7,
            shifts_14_days=shifts_7,
        )
        self.assertIn(assessment.risk_level, (FatigueRiskLevel.HIGH, FatigueRiskLevel.CRITICAL))
        self.assertGreater(assessment.score, 50)

    def test_assess_creates_alert_high_risk(self):
        """Assessment should create alert for HIGH risk."""
        today = date.today()
        shifts_7 = [
            {"date": today - timedelta(days=i), "start": "08:00", "end": "20:00"}
            for i in range(6)
        ]
        assess_fatigue(
            employee_id="emp_003",
            employee_name="Charlie Brown",
            venue_id="venue_001",
            shifts_7_days=shifts_7,
            shifts_14_days=shifts_7,
        )
        store = get_fatigue_store()
        alerts = store.get_alerts("venue_001")
        self.assertGreater(len(alerts), 0)

    def test_assess_violations_recorded(self):
        """Assessment should record violations."""
        today = date.today()
        shifts_7 = [
            {"date": today - timedelta(days=i), "start": "08:00", "end": "20:00"}
            for i in range(7)
        ]
        assessment = assess_fatigue(
            employee_id="emp_004",
            employee_name="Diana Prince",
            venue_id="venue_001",
            shifts_7_days=shifts_7,
            shifts_14_days=shifts_7,
        )
        self.assertGreater(len(assessment.violations), 0)

    def test_assess_recommends_action(self):
        """High-risk assessment should recommend action."""
        today = date.today()
        shifts_7 = [
            {"date": today - timedelta(days=i), "start": "08:00", "end": "20:00"}
            for i in range(7)
        ]
        assessment = assess_fatigue(
            employee_id="emp_005",
            employee_name="Eve Adams",
            venue_id="venue_001",
            shifts_7_days=shifts_7,
            shifts_14_days=shifts_7,
        )
        self.assertGreater(len(assessment.recommendations), 0)


class TestWouldExceedLimits(unittest.TestCase):
    """Test pre-check before assigning shifts."""

    def setUp(self):
        _reset_for_tests()
        _p.reset_for_tests()

    def test_would_not_exceed_short_shift(self):
        """Short shift should not exceed limits."""
        proposed = {"date": date.today(), "start": "09:00", "end": "11:00"}
        existing = []
        would_exceed, reasons = would_exceed_limits(
            employee_id="emp_100",
            proposed_shift=proposed,
            existing_shifts=existing,
        )
        self.assertFalse(would_exceed)

    def test_would_exceed_weekly_hours(self):
        """Adding shift when at limit should flag."""
        today = date.today()
        existing = [
            {"date": today - timedelta(days=i), "start": "08:00", "end": "20:00"}
            for i in range(5)
        ]
        proposed = {"date": today, "start": "08:00", "end": "18:00"}
        would_exceed, reasons = would_exceed_limits(
            employee_id="emp_101",
            proposed_shift=proposed,
            existing_shifts=existing,
        )
        self.assertTrue(would_exceed)
        self.assertGreater(len(reasons), 0)

    def test_would_exceed_consecutive_days(self):
        """Adding shift at day 7 should flag."""
        today = date.today()
        existing = [
            {"date": today - timedelta(days=i), "start": "09:00", "end": "17:00"}
            for i in range(6)
        ]
        proposed = {"date": today, "start": "09:00", "end": "17:00"}
        would_exceed, reasons = would_exceed_limits(
            employee_id="emp_102",
            proposed_shift=proposed,
            existing_shifts=existing,
        )
        self.assertTrue(would_exceed)


class TestRosterWideCheck(unittest.TestCase):
    """Test roster-wide fatigue check."""

    def setUp(self):
        _reset_for_tests()
        _p.reset_for_tests()

    def test_roster_check_empty(self):
        """Empty roster should return empty list."""
        assessments = check_roster_fatigue(
            venue_id="venue_001",
            all_shifts_by_employee={},
        )
        self.assertEqual(len(assessments), 0)

    def test_roster_check_multiple_employees(self):
        """Should assess all employees in roster."""
        today = date.today()
        shifts_normal = [{"date": today, "start": "09:00", "end": "17:00"}]
        shifts_heavy = [
            {"date": today - timedelta(days=i), "start": "08:00", "end": "20:00"}
            for i in range(6)
        ]
        all_shifts = {
            "emp_200": shifts_normal,
            "emp_201": shifts_heavy,
        }
        assessments = check_roster_fatigue(
            venue_id="venue_001",
            all_shifts_by_employee=all_shifts,
        )
        self.assertEqual(len(assessments), 2)

    def test_roster_check_identifies_at_risk(self):
        """Should identify at-risk employees."""
        today = date.today()
        shifts_heavy = [
            {"date": today - timedelta(days=i), "start": "08:00", "end": "20:00"}
            for i in range(7)
        ]
        all_shifts = {
            "emp_300": shifts_heavy,
        }
        assessments = check_roster_fatigue(
            venue_id="venue_001",
            all_shifts_by_employee=all_shifts,
        )
        at_risk = [a for a in assessments if a.risk_level in (FatigueRiskLevel.HIGH, FatigueRiskLevel.CRITICAL)]
        self.assertGreater(len(at_risk), 0)


class TestRecommendations(unittest.TestCase):
    """Test recommendation generation."""

    def setUp(self):
        _reset_for_tests()
        _p.reset_for_tests()

    def test_recommend_critical_immediate_action(self):
        """CRITICAL risk should recommend immediate action."""
        assessment = FatigueAssessment(
            employee_id="emp_400",
            employee_name="Test",
            venue_id="venue_001",
            assessment_date=date.today(),
            risk_level=FatigueRiskLevel.CRITICAL,
            weekly_hours=60,
            fortnightly_hours=100,
            consecutive_days=7,
            last_day_off=date.today() - timedelta(days=10),
            night_shift_count=4,
            violations=[],
            recommendations=[],
            score=85,
        )
        recs = generate_recommendations(assessment)
        self.assertGreater(len(recs), 0)
        self.assertTrue(any("URGENT" in r or "critical" in r for r in recs))

    def test_recommend_consecutive_days(self):
        """High consecutive days should recommend day off."""
        assessment = FatigueAssessment(
            employee_id="emp_401",
            employee_name="Test",
            venue_id="venue_001",
            assessment_date=date.today(),
            risk_level=FatigueRiskLevel.HIGH,
            weekly_hours=45,
            fortnightly_hours=85,
            consecutive_days=6,
            last_day_off=date.today() - timedelta(days=6),
            night_shift_count=0,
            violations=[],
            recommendations=[],
            score=50,
        )
        recs = generate_recommendations(assessment)
        self.assertTrue(any("day off" in r.lower() for r in recs))

    def test_recommend_night_shifts(self):
        """High night shifts should recommend reduction."""
        assessment = FatigueAssessment(
            employee_id="emp_402",
            employee_name="Test",
            venue_id="venue_001",
            assessment_date=date.today(),
            risk_level=FatigueRiskLevel.MODERATE,
            weekly_hours=40,
            fortnightly_hours=80,
            consecutive_days=3,
            last_day_off=date.today() - timedelta(days=3),
            night_shift_count=4,
            violations=[],
            recommendations=[],
            score=35,
        )
        recs = generate_recommendations(assessment)
        self.assertTrue(any("night" in r.lower() for r in recs))

    def test_recommend_low_risk_normal(self):
        """LOW risk should have normal recommendation."""
        assessment = FatigueAssessment(
            employee_id="emp_403",
            employee_name="Test",
            venue_id="venue_001",
            assessment_date=date.today(),
            risk_level=FatigueRiskLevel.LOW,
            weekly_hours=35,
            fortnightly_hours=70,
            consecutive_days=3,
            last_day_off=date.today() - timedelta(days=1),
            night_shift_count=0,
            violations=[],
            recommendations=[],
            score=10,
        )
        recs = generate_recommendations(assessment)
        self.assertGreater(len(recs), 0)
        self.assertTrue(any("normal" in r.lower() or "continue" in r.lower() for r in recs))


class TestStorePersistence(unittest.TestCase):
    """Test store persistence."""

    def setUp(self):
        _reset_for_tests()
        _p.reset_for_tests()

    def test_store_saves_assessment(self):
        """Assessment should be stored and retrievable."""
        assessment = assess_fatigue(
            employee_id="emp_500",
            employee_name="Storage Test",
            venue_id="venue_001",
            shifts_7_days=[],
            shifts_14_days=[],
        )
        store = get_fatigue_store()
        retrieved = store.get_assessment("venue_001", "emp_500")
        self.assertIsNotNone(retrieved)
        self.assertEqual(retrieved.employee_name, "Storage Test")

    def test_store_saves_alert(self):
        """Alert should be stored and retrievable."""
        today = date.today()
        shifts = [
            {"date": today - timedelta(days=i), "start": "08:00", "end": "20:00"}
            for i in range(7)
        ]
        assess_fatigue(
            employee_id="emp_501",
            employee_name="Alert Test",
            venue_id="venue_001",
            shifts_7_days=shifts,
            shifts_14_days=shifts,
        )
        store = get_fatigue_store()
        alerts = store.get_alerts("venue_001")
        self.assertGreater(len(alerts), 0)

    def test_store_filters_alerts_by_risk(self):
        """Should filter alerts by risk level."""
        today = date.today()
        shifts = [
            {"date": today - timedelta(days=i), "start": "08:00", "end": "20:00"}
            for i in range(7)
        ]
        assess_fatigue(
            employee_id="emp_502",
            employee_name="Risk Filter Test",
            venue_id="venue_001",
            shifts_7_days=shifts,
            shifts_14_days=shifts,
        )
        store = get_fatigue_store()
        critical_alerts = store.get_alerts("venue_001", risk_level=FatigueRiskLevel.CRITICAL)
        high_alerts = store.get_alerts("venue_001", risk_level=FatigueRiskLevel.HIGH)
        # Should have some alerts at one of these levels
        self.assertGreater(len(critical_alerts) + len(high_alerts), 0)


class TestDefaultRules(unittest.TestCase):
    """Test default fatigue rules."""

    def setUp(self):
        _reset_for_tests()
        _p.reset_for_tests()

    def test_default_rules_exist(self):
        """Should have all default rules defined."""
        self.assertIn("max_weekly_hours", DEFAULT_RULES)
        self.assertIn("max_fortnightly_hours", DEFAULT_RULES)
        self.assertIn("max_consecutive_days", DEFAULT_RULES)
        self.assertIn("min_weekly_rest_hours", DEFAULT_RULES)

    def test_default_weekly_hours_50(self):
        """Default max weekly hours should be 50."""
        rule = DEFAULT_RULES["max_weekly_hours"]
        self.assertEqual(rule.max_value, 50.0)

    def test_default_fortnightly_hours_95(self):
        """Default max fortnightly hours should be 95."""
        rule = DEFAULT_RULES["max_fortnightly_hours"]
        self.assertEqual(rule.max_value, 95.0)

    def test_default_consecutive_days_6(self):
        """Default max consecutive days should be 6."""
        rule = DEFAULT_RULES["max_consecutive_days"]
        self.assertEqual(rule.max_value, 6.0)


class TestDataClassSerialization(unittest.TestCase):
    """Test data class serialization to dicts."""

    def setUp(self):
        _reset_for_tests()
        _p.reset_for_tests()

    def test_assessment_to_dict(self):
        """Assessment should serialize to dict."""
        assessment = FatigueAssessment(
            employee_id="emp_600",
            employee_name="Serialize Test",
            venue_id="venue_001",
            assessment_date=date.today(),
            risk_level=FatigueRiskLevel.LOW,
            weekly_hours=35,
            fortnightly_hours=70,
            consecutive_days=3,
            last_day_off=date.today() - timedelta(days=1),
            night_shift_count=0,
            violations=[],
            recommendations=["Continue current schedule"],
            score=15,
        )
        data = assessment.to_dict()
        self.assertEqual(data["employee_id"], "emp_600")
        self.assertEqual(data["risk_level"], "low")
        self.assertEqual(data["weekly_hours"], 35)

    def test_alert_to_dict(self):
        """Alert should serialize to dict."""
        alert = FatigueAlert(
            alert_id="alert_123",
            employee_id="emp_601",
            employee_name="Alert Serialize",
            venue_id="venue_001",
            risk_level=FatigueRiskLevel.HIGH,
            trigger="exceeded_weekly_hours",
            hours_worked=55,
        )
        data = alert.to_dict()
        self.assertEqual(data["alert_id"], "alert_123")
        self.assertEqual(data["risk_level"], "high")
        self.assertEqual(data["trigger"], "exceeded_weekly_hours")


if __name__ == "__main__":
    unittest.main()
