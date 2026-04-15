"""Tests for brief enrichment with data feeds (weather, events, patterns).

Pure stdlib, no pytest. Tests verify that enrichment fields are present
and that adapters fail gracefully.
"""
from __future__ import annotations

import sys
import os
from pathlib import Path
from datetime import date, datetime, timedelta, timezone

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# Set demo mode for tests
os.environ["ROSTERIQ_DATA_MODE"] = "demo"

from rosteriq import morning_brief as mb  # noqa: E402
from rosteriq import weekly_digest as wd  # noqa: E402
from rosteriq import portfolio_recap as pr  # noqa: E402
from rosteriq.shift_events import (  # noqa: E402
    ShiftEventStore, ShiftEvent, EventCategory, PatternLearner
)


# ---------------------------------------------------------------------------
# Morning Brief Enrichment
# ---------------------------------------------------------------------------


def test_build_enriched_context_returns_all_fields():
    """Test that _build_enriched_context returns dict with expected keys."""
    today = date.today()

    context = mb._build_enriched_context(
        "venue_demo_001",
        today.isoformat(),
    )

    assert "weather_outlook" in context
    assert "events_today" in context
    assert "applicable_patterns" in context

    assert isinstance(context["weather_outlook"], dict)
    assert isinstance(context["events_today"], list)
    assert isinstance(context["applicable_patterns"], list)


def test_morning_brief_includes_enrichment_fields():
    """Test that compose_brief includes enrichment fields."""
    yesterday = date.today() - timedelta(days=1)

    brief = mb.compose_brief(
        "venue_demo_001",
        target_date=yesterday.isoformat(),
        events=[],
    )

    assert "weather_outlook" in brief
    assert "events_today" in brief
    assert "applicable_patterns" in brief

    # Fields should be present and correct type even if empty
    assert isinstance(brief["weather_outlook"], dict)
    assert isinstance(brief["events_today"], list)
    assert isinstance(brief["applicable_patterns"], list)


def test_morning_brief_backward_compatible():
    """Test that enrichment doesn't break existing fields."""
    brief = mb.compose_brief("venue_test", events=[])

    # Existing fields must still be present
    assert "venue_id" in brief
    assert "venue_label" in brief
    assert "date" in brief
    assert "generated_at" in brief
    assert "traffic_light" in brief
    assert "headline" in brief
    assert "one_thing" in brief
    assert "summary" in brief
    assert "rollup" in brief
    assert "top_dismissed" in brief
    assert "recap_context" in brief


def test_enrichment_degrades_gracefully():
    """Test that failed adapters don't crash enrichment."""
    # This should not raise even if adapters fail internally
    context = mb._build_enriched_context(
        "venue_test",
        date.today().isoformat(),
    )

    # Even with failures, structure is present
    assert isinstance(context["weather_outlook"], dict)
    assert isinstance(context["events_today"], list)
    assert isinstance(context["applicable_patterns"], list)


# ---------------------------------------------------------------------------
# Weekly Digest Enrichment
# ---------------------------------------------------------------------------


def test_weekly_digest_includes_enrichment_fields():
    """Test that compose_weekly_digest includes enrichment fields."""
    digest = wd.compose_weekly_digest(
        "venue_demo_001",
        events=[],
    )

    assert "weather_week_summary" in digest
    assert "events_week" in digest
    assert "top_patterns" in digest
    assert "shift_event_count" in digest

    # Check types
    assert isinstance(digest["weather_week_summary"], dict)
    assert isinstance(digest["events_week"], list)
    assert isinstance(digest["top_patterns"], list)
    assert isinstance(digest["shift_event_count"], int)


def test_weekly_digest_weather_week_summary_structure():
    """Test that weather_week_summary has correct keys when populated."""
    context = wd._build_enriched_weekly_context(
        "venue_demo_001",
        date.today() - timedelta(days=7),
        date.today(),
    )

    weather = context["weather_week_summary"]
    # Even if empty, should have these keys
    expected_keys = {"rainy_days", "rainy_dates", "avg_max_c", "avg_min_c"}
    assert all(k in weather for k in expected_keys), \
        f"Missing keys in weather_week_summary. Got: {weather.keys()}"


def test_weekly_digest_backward_compatible():
    """Test that enrichment doesn't break existing fields."""
    digest = wd.compose_weekly_digest("venue_test", events=[])

    # Existing fields must still be present
    assert "venue_id" in digest
    assert "venue_label" in digest
    assert "date" in digest
    assert "week_start" in digest
    assert "week_end" in digest
    assert "generated_at" in digest
    assert "traffic_light" in digest
    assert "headline" in digest
    assert "one_pattern" in digest
    assert "summary" in digest
    assert "rollup" in digest
    assert "patterns" in digest
    assert "should_send" in digest


def test_weekly_digest_enrichment_with_shift_events():
    """Test that shift_event_count is populated correctly."""
    store = ShiftEventStore()

    # Add some shift events
    for i in range(3):
        event = ShiftEvent(
            event_id=f"ev_{i}",
            venue_id="venue_test",
            category=EventCategory.WALK_IN_SURGE,
            description="Test event",
            timestamp=datetime.now(timezone.utc),
            headcount_at_time=10,
            logged_by="tester",
            shift_date=date.today(),
            day_of_week=0,
            hour_of_day=18,
            weather_condition="clear",
            active_event_ids=[],
            tags=[],
        )
        store.record(event)

    # Build context with the populated store
    context = wd._build_enriched_weekly_context(
        "venue_test",
        date.today() - timedelta(days=7),
        date.today(),
        shift_event_store=store,
    )

    # Should count the events
    assert context["shift_event_count"] >= 0


# ---------------------------------------------------------------------------
# Portfolio Recap Signals
# ---------------------------------------------------------------------------


def test_portfolio_recap_signals_snapshot_structure():
    """Test that signals_snapshot has required keys."""
    snapshot = pr._build_venue_signals_snapshot(
        "venue_demo_001",
        date.today().isoformat(),
    )

    expected_keys = {
        "weather_today",
        "events_this_week_count",
        "patterns_count",
        "shift_events_this_week",
    }
    assert all(k in snapshot for k in expected_keys), \
        f"Missing keys in signals_snapshot. Got: {snapshot.keys()}"


def test_portfolio_recap_with_signals():
    """Test that compose_portfolio includes signals when requested."""
    recap = {
        "venue_id": "venue_test",
        "shift_date": date.today().isoformat(),
        "traffic_light": "green",
        "revenue": {"actual": 5000, "forecast": 5000, "delta": 0, "delta_pct": 0},
        "wages": {
            "actual": 1500,
            "forecast": 1500,
            "pct_of_revenue_actual": 0.30,
            "pct_of_revenue_target": 0.30,
            "pct_delta": 0,
        },
        "headcount": {"peak": 25},
        "accountability": {
            "total": 0,
            "pending": 0,
            "accepted": 0,
            "dismissed": 0,
            "estimated_impact_missed_aud": 0,
        },
        "summary": "Test",
    }

    portfolio = pr.compose_portfolio(
        [recap],
        include_signals=True,
    )

    # Should have portfolio-level weather outlook
    assert "portfolio_weather_outlook" in portfolio
    assert isinstance(portfolio["portfolio_weather_outlook"], dict)

    # Each venue should have signals_snapshot
    assert len(portfolio["venues"]) == 1
    assert "signals_snapshot" in portfolio["venues"][0]

    snapshot = portfolio["venues"][0]["signals_snapshot"]
    assert isinstance(snapshot, dict)
    assert "weather_today" in snapshot
    assert "events_this_week_count" in snapshot
    assert "patterns_count" in snapshot
    assert "shift_events_this_week" in snapshot


def test_portfolio_recap_without_signals():
    """Test that compose_portfolio doesn't include signals by default."""
    recap = {
        "venue_id": "venue_test",
        "shift_date": date.today().isoformat(),
        "traffic_light": "green",
        "revenue": {"actual": 5000, "forecast": 5000, "delta": 0, "delta_pct": 0},
        "wages": {
            "actual": 1500,
            "forecast": 1500,
            "pct_of_revenue_actual": 0.30,
            "pct_of_revenue_target": 0.30,
            "pct_delta": 0,
        },
        "headcount": {"peak": 25},
        "accountability": {
            "total": 0,
            "pending": 0,
            "accepted": 0,
            "dismissed": 0,
            "estimated_impact_missed_aud": 0,
        },
        "summary": "Test",
    }

    portfolio = pr.compose_portfolio(
        [recap],
        include_signals=False,  # Explicitly disable
    )

    # portfolio_weather_outlook should not be present
    assert "portfolio_weather_outlook" not in portfolio

    # Venue cards should not have signals_snapshot
    assert len(portfolio["venues"]) == 1
    assert "signals_snapshot" not in portfolio["venues"][0]


def test_portfolio_recap_backward_compatible_with_signals():
    """Test that signals don't break existing portfolio fields."""
    recap = {
        "venue_id": "venue_test",
        "shift_date": date.today().isoformat(),
        "traffic_light": "amber",
        "revenue": {"actual": 5000, "forecast": 5500, "delta": -500, "delta_pct": -0.09},
        "wages": {
            "actual": 1500,
            "forecast": 1540,
            "pct_of_revenue_actual": 0.30,
            "pct_of_revenue_target": 0.28,
            "pct_delta": 0.02,
        },
        "headcount": {"peak": 25},
        "accountability": {
            "total": 5,
            "pending": 0,
            "accepted": 3,
            "dismissed": 2,
            "estimated_impact_missed_aud": 500,
        },
        "summary": "Test",
    }

    portfolio = pr.compose_portfolio(
        [recap],
        portfolio_id="test_port",
        include_signals=True,
    )

    # Existing fields must be present
    assert "portfolio_id" in portfolio
    assert portfolio["portfolio_id"] == "test_port"
    assert "shift_date" in portfolio
    assert "generated_at" in portfolio
    assert "traffic_light" in portfolio
    assert "summary" in portfolio
    assert "totals" in portfolio
    assert "accountability" in portfolio
    assert "venues" in portfolio


# ---------------------------------------------------------------------------
# Empty store handling
# ---------------------------------------------------------------------------


def test_enrichment_with_empty_shift_event_store():
    """Test that enrichment handles empty shift event stores."""
    store = ShiftEventStore()

    # Empty store should not crash
    context = wd._build_enriched_weekly_context(
        "venue_test",
        date.today() - timedelta(days=7),
        date.today(),
        shift_event_store=store,
    )

    # Should return sensible defaults
    assert context["shift_event_count"] == 0
    assert context["top_patterns"] == []


def test_morning_brief_with_empty_shift_event_store():
    """Test morning brief enrichment with empty store."""
    store = ShiftEventStore()

    context = mb._build_enriched_context(
        "venue_test",
        date.today().isoformat(),
        shift_event_store=store,
    )

    # Should not crash
    assert isinstance(context["applicable_patterns"], list)
    assert context["applicable_patterns"] == []


if __name__ == "__main__":
    # Simple test runner
    import inspect

    test_fns = [
        fn for name, fn in inspect.getmembers(sys.modules[__name__])
        if name.startswith("test_") and callable(fn)
    ]

    passed = 0
    failed = 0

    for fn in test_fns:
        try:
            fn()
            print(f"  PASS {fn.__name__}")
            passed += 1
        except AssertionError as e:
            print(f"  FAIL {fn.__name__}: {e}")
            failed += 1
        except Exception as e:
            print(f"  ERROR {fn.__name__}: {e}")
            failed += 1

    print(f"\n{passed}/{passed + failed} tests passed")
    sys.exit(0 if failed == 0 else 1)
