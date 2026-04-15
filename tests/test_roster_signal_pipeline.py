"""
Tests for Roster Signal Pipeline Integration
==============================================

Tests verifying that all available demand signals flow through the roster
generation pipeline:
- POS sales history (via forecast engine baseline)
- Bookings (via signal aggregator)
- Weather (via BOM adapter / signal feeds)
- Events (via PerthIsOK adapter / signal feeds)
- Headcount actuals (via pattern learning)
- Pattern signals (via shift event store)

Uses stdlib unittest with mock stubs. No external dependencies needed.
All tests must pass with: PYTHONPATH=. python3 -m unittest tests.test_roster_signal_pipeline
"""

import os
import sys
import unittest
from unittest.mock import Mock, AsyncMock, patch, MagicMock
from datetime import date, datetime, timedelta, timezone
from dataclasses import dataclass

# Add parent directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

# Import classes under test
from rosteriq.roster_engine import (
    RosterEngine,
    Employee,
    EmploymentType,
    Role,
    DemandForecast,
    RosterConstraints,
)
from rosteriq.forecast_engine import ForecastEngine


# ============================================================================
# Mock Signal Objects (simulating signal_feeds.py Signal objects)
# ============================================================================

class MockSignal:
    """Mock Signal object for testing."""
    def __init__(self, source, signal_type, impact_score, confidence, description):
        self.source = Mock(value=source)
        self.signal_type = Mock(value=signal_type)
        self.impact_score = impact_score
        self.confidence = confidence
        self.description = description
        self.timestamp = datetime.now(timezone.utc)
        self.raw_data = {"date": date.today().isoformat()}


# ============================================================================
# Test Case 1: Signal Objects Flow Through Forecast Engine
# ============================================================================

class TestForecastEngineSignalCollection(unittest.TestCase):
    """
    Test that ForecastEngine._get_signal_objects collects and converts
    signal objects from signal aggregator.
    """

    def setUp(self):
        """Set up test fixtures."""
        self.mock_aggregator = AsyncMock()
        self.engine = ForecastEngine(
            pos_adapter=None,
            signal_aggregator=self.mock_aggregator,
            demo_mode=False,
        )

    def test_get_signal_objects_with_real_signals(self):
        """
        Test that _get_signal_objects collects real Signal objects from aggregator
        and converts them to dicts.
        """
        # Arrange: Mock aggregator returns signal objects
        test_signals = [
            MockSignal("weather", "positive", 0.3, 0.9, "Clear skies boost outdoor dining"),
            MockSignal("bookings", "positive", 0.4, 0.95, "7 confirmed tables for tonight"),
            MockSignal("events", "positive", 0.5, 0.85, "Stadium game spillback expected"),
        ]
        self.mock_aggregator.collect_all_signals = AsyncMock(return_value=test_signals)

        # Act
        async def run_test():
            signals = await self.engine._get_signal_objects(
                venue_id="test_venue",
                target_date=date.today(),
            )
            return signals

        # Use simple blocking execution for unittest
        import asyncio
        signals = asyncio.run(run_test())

        # Assert
        self.assertEqual(len(signals), 3)
        self.assertEqual(signals[0]['source'], 'weather')
        self.assertEqual(signals[1]['impact_score'], 0.4)
        self.assertEqual(signals[2]['confidence'], 0.85)

    def test_get_signal_objects_graceful_fallback(self):
        """
        Test that _get_signal_objects gracefully falls back to empty list
        when aggregator fails.
        """
        # Arrange: Mock aggregator throws exception
        self.mock_aggregator.collect_all_signals = AsyncMock(
            side_effect=Exception("API unavailable")
        )

        # Act
        async def run_test():
            signals = await self.engine._get_signal_objects(
                venue_id="test_venue",
                target_date=date.today(),
            )
            return signals

        import asyncio
        signals = asyncio.run(run_test())

        # Assert: Returns empty list, no crash
        self.assertEqual(signals, [])

    def test_forecast_carries_signal_objects(self):
        """
        Test that get_daily_forecast attaches signal_objects to returned
        DemandForecast.
        """
        # Arrange: Mock pos and signal aggregator
        mock_pos = Mock()
        test_signals = [
            MockSignal("weather", "positive", 0.3, 0.9, "Clear skies"),
        ]
        self.mock_aggregator.collect_all_signals = AsyncMock(return_value=test_signals)
        self.mock_aggregator.get_demand_multiplier = AsyncMock(return_value=1.1)
        self.mock_aggregator.get_signal_summary = AsyncMock(return_value="Weather: Clear")

        self.engine.pos_adapter = mock_pos
        self.engine.demo_mode = False

        # Act
        async def run_test():
            forecast = await self.engine.get_daily_forecast(
                venue_id="test_venue",
                target_date=date.today(),
            )
            return forecast

        import asyncio
        forecast = asyncio.run(run_test())

        # Assert
        self.assertIsNotNone(forecast)
        self.assertTrue(hasattr(forecast, 'signal_objects'))
        self.assertEqual(len(forecast.signal_objects), 1)
        self.assertEqual(forecast.signal_objects[0]['source'], 'weather')


# ============================================================================
# Test Case 2: Roster Engine Uses Signal Objects in Demand Slots
# ============================================================================

class TestRosterEngineSignalEnrichment(unittest.TestCase):
    """
    Test that RosterEngine._calculate_demand_slots incorporates signal
    information to boost/reduce demand units.
    """

    def setUp(self):
        """Set up test fixtures."""
        self.engine = RosterEngine(
            constraints=RosterConstraints(
                min_staff_per_hour=1,
                max_staff_per_hour=10,
                budget_limit_weekly=3000.0,
            )
        )

    def test_demand_slots_with_positive_signals(self):
        """
        Test that positive signals (high confidence) boost demand_units
        in resulting slots.
        """
        # Arrange: Create forecast with positive signals
        test_date = date.today().isoformat()
        forecast = DemandForecast(
            date=test_date,
            hourly_demand={
                11: {Role.FLOOR: 2.0, Role.BAR: 1.5, Role.KITCHEN: 1.0},
                12: {Role.FLOOR: 3.0, Role.BAR: 2.0, Role.KITCHEN: 2.0},
            },
            total_covers_expected=25.0,
            signals=["Good weather"],
            confidence=0.8,
        )

        # Attach positive signal objects
        forecast.signal_objects = [
            {
                'source': 'weather',
                'impact_type': 'positive',
                'impact_score': 0.4,
                'confidence': 0.9,
                'description': 'Clear skies boost outdoor dining',
            },
            {
                'source': 'bookings',
                'impact_type': 'positive',
                'impact_score': 0.3,
                'confidence': 0.85,
                'description': 'High booking demand',
            },
        ]

        # Act
        slots = self.engine._calculate_demand_slots([forecast], test_date[:10])

        # Assert: Demand units should be boosted
        # Signal boost = 1.0 + (0.4 * 0.2) + (0.3 * 0.2) = 1.14
        floor_slots = [s for s in slots if s['role'] == Role.FLOOR]
        self.assertTrue(len(floor_slots) > 0)

        # At least one slot should show signal_boost > 1.0
        has_boost = any(s['signal_boost'] > 1.0 for s in floor_slots)
        self.assertTrue(has_boost, "Positive signals should boost demand units")

    def test_demand_slots_with_negative_signals(self):
        """
        Test that negative signals (high confidence) reduce demand_units
        in resulting slots.
        """
        # Arrange: Create forecast with negative signals
        test_date = date.today().isoformat()
        forecast = DemandForecast(
            date=test_date,
            hourly_demand={
                11: {Role.FLOOR: 3.0, Role.BAR: 2.0, Role.KITCHEN: 1.5},
                18: {Role.FLOOR: 4.0, Role.BAR: 3.0, Role.KITCHEN: 2.5},
            },
            total_covers_expected=30.0,
            signals=["Bad weather"],
            confidence=0.75,
        )

        # Attach negative signal objects
        forecast.signal_objects = [
            {
                'source': 'weather',
                'impact_type': 'negative',
                'impact_score': 0.5,
                'confidence': 0.92,
                'description': 'Heavy rain reduces foot traffic',
            },
        ]

        # Act
        slots = self.engine._calculate_demand_slots([forecast], test_date[:10])

        # Assert: Demand units should be reduced
        # Signal boost = 1.0 * (1.0 - 0.5 * 0.1) = 0.95
        all_slots = [s for s in slots if s['role'] == Role.FLOOR]
        self.assertTrue(len(all_slots) > 0)

        # Check that signal_boost is reduced
        has_reduction = any(s['signal_boost'] < 1.0 for s in all_slots)
        self.assertTrue(has_reduction, "Negative signals should reduce demand units")

    def test_demand_slots_low_confidence_signals_ignored(self):
        """
        Test that low-confidence signals (< 0.5) do not affect demand.
        """
        # Arrange: Create forecast with low-confidence signals
        test_date = date.today().isoformat()
        forecast = DemandForecast(
            date=test_date,
            hourly_demand={
                11: {Role.FLOOR: 2.0, Role.BAR: 1.5},
            },
            total_covers_expected=15.0,
            signals=[],
            confidence=0.7,
        )

        # Attach low-confidence signal objects
        forecast.signal_objects = [
            {
                'source': 'events',
                'impact_type': 'positive',
                'impact_score': 0.6,
                'confidence': 0.3,  # Low confidence
                'description': 'Possible event (unconfirmed)',
            },
        ]

        # Act
        slots = self.engine._calculate_demand_slots([forecast], test_date[:10])

        # Assert: Signal boost should remain 1.0 (ignored due to low confidence)
        floor_slots = [s for s in slots if s['role'] == Role.FLOOR]
        self.assertTrue(len(floor_slots) > 0)
        # All slots should have boost == 1.0 (no boost applied)
        boosts = [s['signal_boost'] for s in floor_slots]
        self.assertTrue(all(b == 1.0 for b in boosts), "Low-confidence signals should be ignored")


# ============================================================================
# Test Case 3: Full Roster Generation Pipeline with Signals
# ============================================================================

class TestFullRosterSignalPipeline(unittest.TestCase):
    """
    Integration test: E2E roster generation with multiple signal types
    flowing through the pipeline.
    """

    def setUp(self):
        """Set up test fixtures."""
        self.engine = RosterEngine(
            constraints=RosterConstraints(
                min_staff_per_hour=1,
                max_staff_per_hour=10,
                budget_limit_weekly=3000.0,
            )
        )

        # Create test employees
        self.employees = [
            Employee(
                id="emp_001",
                name="Alice",
                role=Role.FLOOR,
                skills=[Role.FLOOR, Role.BAR],
                hourly_rate=25.0,
                max_hours_per_week=38,
                availability={i: [(11, 22)] for i in range(7)},
                employment_type=EmploymentType.FULL_TIME,
            ),
            Employee(
                id="emp_002",
                name="Bob",
                role=Role.KITCHEN,
                skills=[Role.KITCHEN],
                hourly_rate=26.0,
                max_hours_per_week=38,
                availability={i: [(11, 22)] for i in range(7)},
                employment_type=EmploymentType.PART_TIME,
            ),
            Employee(
                id="emp_003",
                name="Charlie",
                role=Role.BAR,
                skills=[Role.BAR],
                hourly_rate=24.0,
                max_hours_per_week=30,
                availability={i: [(11, 24)] for i in range(7)},
                employment_type=EmploymentType.CASUAL,
                is_manager=True,
            ),
        ]

    def test_roster_generation_with_signal_enriched_forecasts(self):
        """
        Test that full roster generation uses signal-enriched demand forecasts
        to influence staffing recommendations.
        """
        # Arrange: Create forecasts with signals (e.g., high booking demand)
        test_start = date.today().isoformat()
        forecasts = []

        for day_offset in range(7):
            forecast_date = (date.today() + timedelta(days=day_offset)).isoformat()
            base_demand = {
                11: {Role.FLOOR: 2.0, Role.BAR: 1.0, Role.KITCHEN: 1.0},
                12: {Role.FLOOR: 3.0, Role.BAR: 1.5, Role.KITCHEN: 2.0},
                18: {Role.FLOOR: 4.0, Role.BAR: 2.0, Role.KITCHEN: 2.5},
                19: {Role.FLOOR: 5.0, Role.BAR: 3.0, Role.KITCHEN: 3.0},
            }

            forecast = DemandForecast(
                date=forecast_date,
                hourly_demand=base_demand,
                total_covers_expected=40.0,
                signals=[],
                confidence=0.8,
            )

            # Attach signals: bookings boost on Fri/Sat
            is_weekend = (date.today() + timedelta(days=day_offset)).weekday() >= 4
            if is_weekend:
                forecast.signal_objects = [
                    {
                        'source': 'bookings',
                        'impact_type': 'positive',
                        'impact_score': 0.5,
                        'confidence': 0.95,
                        'description': 'High booking demand for weekend',
                    },
                ]
            else:
                forecast.signal_objects = []

            forecasts.append(forecast)

        # Act
        roster = self.engine.generate_roster(
            employees=self.employees,
            demand_forecasts=forecasts,
            week_start_date=test_start,
        )

        # Assert: Roster should be generated successfully
        self.assertIsNotNone(roster)
        # Multiple shifts per day per role (merging happens internally)
        self.assertGreater(len(roster.shifts), 0)
        self.assertGreater(roster.total_labour_cost, 0)
        self.assertGreater(roster.coverage_score, 0)

    def test_roster_respects_signal_priority_ordering(self):
        """
        Test that demand slots with higher signal boost get higher priority
        in assignment, resulting in better staff allocation for high-demand periods.
        """
        # Arrange: Two days with different signals
        test_start = date.today().isoformat()
        today_iso = date.today().isoformat()
        tomorrow_iso = (date.today() + timedelta(days=1)).isoformat()

        # Day 1: No signals (baseline)
        forecast_day1 = DemandForecast(
            date=today_iso,
            hourly_demand={
                11: {Role.FLOOR: 2.0, Role.BAR: 1.0, Role.KITCHEN: 1.0},
                18: {Role.FLOOR: 3.0, Role.BAR: 2.0, Role.KITCHEN: 2.0},
            },
            total_covers_expected=25.0,
            signals=[],
            confidence=0.7,
        )
        forecast_day1.signal_objects = []

        # Day 2: Strong positive signals (major event)
        forecast_day2 = DemandForecast(
            date=tomorrow_iso,
            hourly_demand={
                11: {Role.FLOOR: 2.0, Role.BAR: 1.0, Role.KITCHEN: 1.0},
                18: {Role.FLOOR: 3.0, Role.BAR: 2.0, Role.KITCHEN: 2.0},
            },
            total_covers_expected=25.0,
            signals=["Major sporting event spillback"],
            confidence=0.8,
        )
        forecast_day2.signal_objects = [
            {
                'source': 'events',
                'impact_type': 'positive',
                'impact_score': 0.6,
                'confidence': 0.95,
                'description': 'Stadium game nearby - expect 30% demand increase',
            },
        ]

        # Act: Generate slots for both days
        slots = self.engine._calculate_demand_slots(
            [forecast_day1, forecast_day2],
            test_start,
        )

        # Assert: Day 2 slots should have higher signal boost and priority
        day1_slots = [s for s in slots if s['date'] == today_iso]
        day2_slots = [s for s in slots if s['date'] == tomorrow_iso]

        self.assertTrue(len(day1_slots) > 0, "Should have slots for day 1")
        self.assertTrue(len(day2_slots) > 0, "Should have slots for day 2")

        # Compare signal boosts
        day1_boosts = [s['signal_boost'] for s in day1_slots]
        day2_boosts = [s['signal_boost'] for s in day2_slots]

        avg_day1_boost = sum(day1_boosts) / len(day1_boosts) if day1_boosts else 1.0
        avg_day2_boost = sum(day2_boosts) / len(day2_boosts) if day2_boosts else 1.0

        self.assertGreater(
            avg_day2_boost,
            avg_day1_boost,
            "Day with event signals should have higher average boost",
        )


# ============================================================================
# Test Case 4: Signal Metadata Preservation Through Pipeline
# ============================================================================

class TestSignalMetadataPreservation(unittest.TestCase):
    """
    Test that detailed signal metadata (source, impact_type, confidence)
    is preserved through the roster generation pipeline.
    """

    def test_signals_preserved_in_demand_slots(self):
        """
        Test that signal descriptions/sources are included in demand slots
        for audit trail and logging.
        """
        # Arrange
        engine = RosterEngine(
            constraints=RosterConstraints(budget_limit_weekly=3000.0)
        )

        test_date = date.today().isoformat()
        forecast = DemandForecast(
            date=test_date,
            hourly_demand={
                11: {Role.FLOOR: 2.0, Role.BAR: 1.0},
            },
            total_covers_expected=15.0,
            signals=["Bookings +40%", "Good weather"],
            confidence=0.85,
        )

        forecast.signal_objects = [
            {
                'source': 'bookings',
                'impact_type': 'positive',
                'impact_score': 0.4,
                'confidence': 0.95,
                'description': 'High booking volume',
            },
            {
                'source': 'weather',
                'impact_type': 'positive',
                'impact_score': 0.3,
                'confidence': 0.88,
                'description': 'Clear skies',
            },
        ]

        # Act
        slots = engine._calculate_demand_slots([forecast], test_date[:10])

        # Assert: Signals should be in slot metadata
        self.assertTrue(len(slots) > 0)
        for slot in slots:
            # Check that signals list is present
            self.assertIn('signals', slot)
            self.assertIn('signal_boost', slot)
            # Signals should be populated
            self.assertTrue(len(slot['signals']) > 0)


# ============================================================================
# Test Case 5: Graceful Degradation Without Signals
# ============================================================================

class TestGracefulDegradationWithoutSignals(unittest.TestCase):
    """
    Test that roster generation works correctly even when signals are
    unavailable (demo mode, API failures, etc.).
    """

    def test_roster_generation_without_signal_objects(self):
        """
        Test that roster generation works fine when forecast has no
        signal_objects attribute (backwards compatibility).
        """
        # Arrange
        engine = RosterEngine(
            constraints=RosterConstraints(budget_limit_weekly=3000.0)
        )

        test_start = date.today().isoformat()
        employees = [
            Employee(
                id="emp_001",
                name="Alice",
                role=Role.FLOOR,
                hourly_rate=25.0,
                availability={i: [(11, 22)] for i in range(7)},
            ),
        ]

        # Create forecast WITHOUT signal_objects (legacy format)
        forecast = DemandForecast(
            date=date.today().isoformat(),
            hourly_demand={
                11: {Role.FLOOR: 1.0},
                12: {Role.FLOOR: 1.0},
            },
            total_covers_expected=10.0,
            signals=[],
            confidence=0.7,
        )
        # Intentionally don't set signal_objects

        # Act: Should not crash
        slots = engine._calculate_demand_slots([forecast], test_start[:10])

        # Assert: Should handle gracefully with default signal_boost=1.0
        self.assertTrue(len(slots) > 0)
        for slot in slots:
            # Default: no signal boost applied
            self.assertEqual(slot['signal_boost'], 1.0)

    def test_roster_generation_with_empty_signals(self):
        """
        Test roster generation when signal_objects is empty list.
        """
        # Arrange
        engine = RosterEngine(
            constraints=RosterConstraints(budget_limit_weekly=3000.0)
        )

        test_start = date.today().isoformat()
        forecast = DemandForecast(
            date=date.today().isoformat(),
            hourly_demand={
                11: {Role.FLOOR: 1.0, Role.BAR: 1.0},
            },
            total_covers_expected=10.0,
            signals=[],
            confidence=0.7,
        )
        forecast.signal_objects = []  # Explicitly empty

        # Act
        slots = engine._calculate_demand_slots([forecast], test_start[:10])

        # Assert
        self.assertTrue(len(slots) > 0)
        # All slots should have baseline signal_boost
        for slot in slots:
            self.assertEqual(slot['signal_boost'], 1.0)


# ============================================================================
# Test Runner
# ============================================================================

if __name__ == "__main__":
    # Run all tests
    unittest.main(verbosity=2)
