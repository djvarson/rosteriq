"""Tests for rosteriq.daily_digest — pure stdlib, no pytest."""
from __future__ import annotations

import sys
import unittest
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from rosteriq import daily_digest as dd  # noqa: E402
from rosteriq import tanda_history as _th  # noqa: E402
from rosteriq import headcount as _hc  # noqa: E402
from rosteriq import shift_swap as _ss  # noqa: E402


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _today() -> date:
    return datetime.now(timezone.utc).date()


def _tomorrow() -> date:
    return _today() + timedelta(days=1)


def _yesterday() -> date:
    return _today() - timedelta(days=1)


# ---------------------------------------------------------------------------
# Mock Store Classes
# ---------------------------------------------------------------------------

class MockTandaHistoryStore:
    """Mock tanda history store for testing."""

    def __init__(self):
        self._dailies = {}

    def put_daily_actuals(self, venue_id: str, day: date, actuals):
        """Store daily actuals."""
        key = (venue_id, day.isoformat())
        self._dailies[key] = actuals

    def get_daily_actuals(self, venue_id: str, day: date):
        """Retrieve daily actuals."""
        key = (venue_id, day.isoformat())
        return self._dailies.get(key)


class MockShiftNoteStore:
    """Mock shift note store for testing."""

    def __init__(self):
        self._notes = []

    def add_note(self, note):
        """Add a shift note."""
        self._notes.append(note)

    def list_recent(self, venue_id: str, limit: int = 10):
        """List recent notes for venue."""
        return [n for n in self._notes if n.venue_id == venue_id][:limit]


class MockSwapStore:
    """Mock swap store for testing."""

    def __init__(self):
        self._swaps = []

    def add_swap(self, swap):
        """Add a swap."""
        self._swaps.append(swap)

    def list_pending_review(self, venue_id: str):
        """List pending swaps for venue."""
        from rosteriq.shift_swap import SwapStatus
        return [
            s for s in self._swaps
            if s.venue_id == venue_id and s.status == SwapStatus.CLAIMED
        ]


# ---------------------------------------------------------------------------
# Test Cases
# ---------------------------------------------------------------------------

class TestBuildDigestReturnsAllSections(unittest.TestCase):
    """Test that build_digest returns all expected sections."""

    def test_all_sections_present(self):
        """Verify all expected keys present."""
        digest = dd.build_digest("venue_001")

        # Check top-level keys
        assert "venue_id" in digest
        assert "target_date" in digest
        assert "generated_at" in digest
        assert "sections" in digest

        # Check sections
        sections = digest["sections"]
        assert "forecast_summary" in sections
        assert "weather_alert" in sections
        assert "signals" in sections
        assert "yesterday_recap" in sections
        assert "shift_notes" in sections
        assert "pending_swaps" in sections
        assert "suggested_actions" in sections

    def test_venue_id_matches(self):
        """Verify venue_id is preserved."""
        digest = dd.build_digest("test_venue_123")
        assert digest["venue_id"] == "test_venue_123"

    def test_target_date_defaults_to_tomorrow(self):
        """Verify default target_date is tomorrow."""
        digest = dd.build_digest("venue_001")
        target_date_str = digest["target_date"]
        parsed = datetime.fromisoformat(target_date_str).date()
        assert parsed == _tomorrow()

    def test_target_date_can_be_specified(self):
        """Verify target_date can be overridden."""
        specific_date = _today() + timedelta(days=5)
        digest = dd.build_digest("venue_001", target_date=specific_date)
        assert digest["target_date"] == specific_date.isoformat()


class TestForecastSummaryWithHistory(unittest.TestCase):
    """Test forecast summary with historical data."""

    def test_uses_same_day_last_week(self):
        """Verify forecast pulls from same-day-last-week."""
        venue_id = "venue_001"
        target_date = _today() + timedelta(days=3)  # Friday maybe
        last_week = target_date - timedelta(days=7)

        # Create mock store with last week's data
        store = MockTandaHistoryStore()

        # Create DailyActuals with tanda_history structure
        from rosteriq.tanda_history import DailyActuals
        last_week_actuals = DailyActuals(
            venue_id=venue_id,
            day=last_week,
            actual_revenue=20000.0,
            employee_count=15,
        )
        store.put_daily_actuals(venue_id, last_week, last_week_actuals)

        forecast = dd._build_forecast_summary(venue_id, target_date, store)

        # Should have populated fields
        assert forecast["expected_revenue_low"] > 0
        assert forecast["expected_revenue_high"] > 0
        assert forecast["expected_covers"] == 15
        assert not forecast["limited_data"]

    def test_limited_data_flag_when_empty(self):
        """Verify limited_data flag when no history."""
        forecast = dd._build_forecast_summary("venue_002", _tomorrow(), None)
        assert forecast["limited_data"]
        assert forecast["expected_revenue_low"] > 0  # Sensible defaults
        assert forecast["expected_revenue_high"] > 0


class TestForecastSummaryNoData(unittest.TestCase):
    """Test forecast summary with no history."""

    def test_returns_sensible_defaults(self):
        """Verify sensible defaults when no data."""
        forecast = dd._build_forecast_summary("venue_999", _tomorrow())

        assert "expected_revenue_low" in forecast
        assert "expected_revenue_high" in forecast
        assert "expected_covers" in forecast
        assert "day_type" in forecast
        assert "limited_data" in forecast
        assert forecast["limited_data"]

    def test_day_type_classification(self):
        """Verify day_type is classified correctly."""
        # Test each day of the week
        mon = _today() + timedelta(days=(0 - _today().weekday()) % 7)  # Next Monday
        fri = mon + timedelta(days=4)
        sat = mon + timedelta(days=5)
        sun = mon + timedelta(days=6)

        forecast_fri = dd._build_forecast_summary("v", fri)
        forecast_sat = dd._build_forecast_summary("v", sat)
        forecast_sun = dd._build_forecast_summary("v", sun)

        assert forecast_fri["day_type"] == "friday"
        assert forecast_sat["day_type"] == "saturday"
        assert forecast_sun["day_type"] == "sunday"


class TestYesterdayRecap(unittest.TestCase):
    """Test yesterday recap."""

    def test_recap_with_history(self):
        """Verify recap pulls from yesterday."""
        venue_id = "venue_001"
        yesterday = _yesterday()

        store = MockTandaHistoryStore()
        from rosteriq.tanda_history import DailyActuals
        actuals = DailyActuals(
            venue_id=venue_id,
            day=yesterday,
            actual_revenue=18000.0,
            worked_cost=5400.0,  # 30%
            worked_hours=72,
            rostered_hours=70,
        )
        store.put_daily_actuals(venue_id, yesterday, actuals)

        recap = dd._build_yesterday_recap(venue_id, yesterday, store)

        assert recap["revenue"] == 18000.0
        assert recap["labour_cost"] == 5400.0
        assert recap["variance_hours"] == 2.0
        assert recap["performance"] == "over_rostered"

    def test_recap_no_store(self):
        """Verify recap handles missing store."""
        recap = dd._build_yesterday_recap("venue_001", _yesterday(), None)

        assert recap["revenue"] == 0
        assert recap["labour_cost"] == 0
        assert recap["performance"] == "on_target"


class TestSuggestActionsRain(unittest.TestCase):
    """Test suggested actions for rain."""

    def test_rain_over_70_percent(self):
        """Verify high rain triggers action."""
        forecast = {"day_type": "tuesday"}
        weather = {
            "condition": "rain",
            "temperature": 20,
            "rain_chance": 75,
            "impact": "Heavy rain",
        }
        yesterday = {
            "revenue": 18000,
            "labour_cost": 5400,
            "performance": "on_target",
            "variance_hours": 0,
        }
        signals = []
        swaps = []

        actions = dd._suggest_actions(forecast, weather, yesterday, signals, swaps)

        # Should have at least one high-priority action
        rain_actions = [a for a in actions if "Reduce outdoor" in a.get("action", "")]
        assert len(rain_actions) > 0
        assert rain_actions[0]["priority"] == "high"

    def test_no_alert_for_low_rain(self):
        """Verify low rain doesn't trigger alert."""
        forecast = {"day_type": "tuesday"}
        weather = {
            "condition": "partly cloudy",
            "temperature": 20,
            "rain_chance": 10,
            "impact": None,
        }
        yesterday = {
            "revenue": 18000,
            "labour_cost": 5400,
            "performance": "on_target",
            "variance_hours": 0,
        }
        signals = []
        swaps = []

        actions = dd._suggest_actions(forecast, weather, yesterday, signals, swaps)

        # No rain-specific action
        rain_actions = [a for a in actions if "Reduce outdoor" in a.get("action", "")]
        assert len(rain_actions) == 0


class TestSuggestActionsOverRostered(unittest.TestCase):
    """Test suggested actions for over-rostering."""

    def test_over_rostered_feedback(self):
        """Verify over-rostered yesterday triggers feedback."""
        forecast = {"day_type": "tuesday"}
        weather = None
        yesterday = {
            "revenue": 18000,
            "labour_cost": 5400,
            "performance": "over_rostered",
            "variance_hours": 3.5,
        }
        signals = []
        swaps = []

        actions = dd._suggest_actions(forecast, weather, yesterday, signals, swaps)

        # Should suggest tightening
        tighten_actions = [a for a in actions if "tighten" in a.get("action", "").lower()]
        assert len(tighten_actions) > 0
        assert tighten_actions[0]["priority"] in ("medium", "high")

    def test_under_rostered_feedback(self):
        """Verify under-rostered yesterday triggers feedback."""
        forecast = {"day_type": "tuesday"}
        weather = None
        yesterday = {
            "revenue": 18000,
            "labour_cost": 5400,
            "performance": "under_rostered",
            "variance_hours": -2.0,
        }
        signals = []
        swaps = []

        actions = dd._suggest_actions(forecast, weather, yesterday, signals, swaps)

        # Should suggest ensuring full coverage
        coverage_actions = [a for a in actions if "full coverage" in a.get("action", "").lower()]
        assert len(coverage_actions) > 0


class TestSuggestActionsPeakDays(unittest.TestCase):
    """Test suggested actions for peak days."""

    def test_friday_gets_peak_action(self):
        """Verify Friday triggers peak day action."""
        forecast = {"day_type": "friday"}
        weather = None
        yesterday = {"revenue": 18000, "labour_cost": 5400, "performance": "on_target", "variance_hours": 0}
        signals = []
        swaps = []

        actions = dd._suggest_actions(forecast, weather, yesterday, signals, swaps)

        peak_actions = [a for a in actions if "peak day" in a.get("action", "").lower()]
        assert len(peak_actions) > 0
        assert peak_actions[0]["priority"] == "high"

    def test_saturday_gets_peak_action(self):
        """Verify Saturday triggers peak day action."""
        forecast = {"day_type": "saturday"}
        weather = None
        yesterday = {"revenue": 18000, "labour_cost": 5400, "performance": "on_target", "variance_hours": 0}
        signals = []
        swaps = []

        actions = dd._suggest_actions(forecast, weather, yesterday, signals, swaps)

        peak_actions = [a for a in actions if "peak day" in a.get("action", "").lower()]
        assert len(peak_actions) > 0


class TestSuggestActionsPendingSwaps(unittest.TestCase):
    """Test suggested actions for pending swaps."""

    def test_pending_swaps_trigger_action(self):
        """Verify pending swaps generate action."""
        forecast = {"day_type": "tuesday"}
        weather = None
        yesterday = {"revenue": 18000, "labour_cost": 5400, "performance": "on_target", "variance_hours": 0}
        signals = []
        swaps = [{"status": "claimed"}]

        actions = dd._suggest_actions(forecast, weather, yesterday, signals, swaps)

        swap_actions = [a for a in actions if "swap" in a.get("action", "").lower()]
        assert len(swap_actions) > 0


class TestFormatTextOutput(unittest.TestCase):
    """Test text formatting."""

    def test_returns_non_empty_string(self):
        """Verify text output is non-empty."""
        digest = dd.build_digest("venue_001")
        text = dd.format_digest_text(digest)

        assert isinstance(text, str)
        assert len(text) > 100

    def test_contains_key_info(self):
        """Verify text contains key information."""
        digest = dd.build_digest("venue_001", target_date=_tomorrow())
        text = dd.format_digest_text(digest)

        assert "venue_001" in text
        assert "RosterIQ" in text
        assert "FORECAST SUMMARY" in text


class TestFormatHtmlOutput(unittest.TestCase):
    """Test HTML formatting."""

    def test_returns_html(self):
        """Verify HTML output is valid."""
        digest = dd.build_digest("venue_001")
        html = dd.format_digest_html(digest)

        assert isinstance(html, str)
        assert "<!DOCTYPE html>" in html or "<html>" in html
        assert "</html>" in html

    def test_contains_venue_and_date(self):
        """Verify HTML contains venue and date."""
        digest = dd.build_digest("venue_001", target_date=_tomorrow())
        html = dd.format_digest_html(digest)

        assert "venue_001" in html
        assert _tomorrow().isoformat() in html


class TestCollectPendingSwaps(unittest.TestCase):
    """Test pending swap collection."""

    def test_collect_pending_swaps(self):
        """Verify swaps are collected."""
        venue_id = "venue_001"
        store = MockSwapStore()

        # Add a claimed swap (pending review)
        swap = _ss.ShiftSwap(
            swap_id="swap_001",
            venue_id=venue_id,
            shift_id="shift_001",
            shift_date="2026-04-20",
            shift_start="09:00",
            shift_end="17:00",
            role="bartender",
            offered_by="emp_001",
            offered_by_name="John Doe",
            reason="Sick",
            status=_ss.SwapStatus.CLAIMED,
            claimed_by="emp_002",
            claimed_by_name="Jane Smith",
            claimed_at=datetime.now(timezone.utc),
        )
        store.add_swap(swap)

        swaps = dd._collect_pending_swaps(venue_id, swap_store=store)

        assert len(swaps) > 0
        assert swaps[0]["offered_by_name"] == "John Doe"
        assert swaps[0]["shift_date"] == "2026-04-20"

    def test_no_swaps_returns_empty(self):
        """Verify empty list when no swaps."""
        swaps = dd._collect_pending_swaps("venue_999", swap_store=None)
        assert swaps == []


class TestCollectSignals(unittest.TestCase):
    """Test signal collection."""

    def test_collects_day_of_week_signals(self):
        """Verify day-of-week signals are collected."""
        # Find a Friday
        today = _today()
        days_until_friday = (4 - today.weekday()) % 7
        friday = today + timedelta(days=days_until_friday)

        signals = dd._collect_signals("venue_001", friday)

        # Should have at least one signal mentioning Friday
        friday_signals = [s for s in signals if "friday" in s.get("summary", "").lower()]
        assert len(friday_signals) > 0

    def test_saturday_signals(self):
        """Verify Saturday is identified as peak."""
        today = _today()
        days_until_saturday = (5 - today.weekday()) % 7
        saturday = today + timedelta(days=days_until_saturday)

        signals = dd._collect_signals("venue_001", saturday)

        # Should have Saturday signal
        sat_signals = [s for s in signals if "saturday" in s.get("summary", "").lower()]
        assert len(sat_signals) > 0


class TestWeatherAlert(unittest.TestCase):
    """Test weather alert building."""

    def test_returns_none_when_unavailable(self):
        """Verify None when weather adapter unavailable."""
        alert = dd._build_weather_alert("venue_001", _tomorrow())
        # Should not raise, returns None
        assert alert is None or isinstance(alert, dict)


# ---------------------------------------------------------------------------
# Integration Tests
# ---------------------------------------------------------------------------

class TestDigestIntegration(unittest.TestCase):
    """Integration tests with multiple components."""

    def test_build_digest_with_all_stores(self):
        """Verify digest builds with all stores."""
        venue_id = "venue_001"
        target_date = _today() + timedelta(days=1)
        yesterday = target_date - timedelta(days=1)

        # Create mock stores
        history_store = MockTandaHistoryStore()
        note_store = MockShiftNoteStore()
        swap_store = MockSwapStore()

        # Populate history
        from rosteriq.tanda_history import DailyActuals
        actuals = DailyActuals(
            venue_id=venue_id,
            day=yesterday,
            actual_revenue=20000.0,
            worked_cost=5800.0,
            worked_hours=73,
            rostered_hours=70,
            employee_count=15,
        )
        history_store.put_daily_actuals(venue_id, yesterday, actuals)

        # Add a shift note
        note = _hc.ShiftNote(
            venue_id=venue_id,
            shift_id="shift_001",
            author_id="mgr_001",
            author_name="Manager Name",
            content="Busy Friday night, all systems go",
            tags=["busy", "positive"],
        )
        note_store.add_note(note)

        # Build digest
        digest = dd.build_digest(
            venue_id,
            target_date=target_date,
            history_store=history_store,
            note_store=note_store,
            swap_store=swap_store,
        )

        # Verify structure
        assert digest["venue_id"] == venue_id
        assert digest["target_date"] == target_date.isoformat()

        sections = digest["sections"]
        assert sections["yesterday_recap"]["revenue"] == 20000.0
        assert len(sections["shift_notes"]) > 0
        assert len(sections["suggested_actions"]) > 0


# ---------------------------------------------------------------------------
# Test Runner
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    unittest.main()
