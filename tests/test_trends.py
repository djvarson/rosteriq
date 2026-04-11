"""Tests for rosteriq.trends — pure stdlib, no pytest."""
from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from rosteriq import trends as tr  # noqa: E402


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _ev(
    rid,
    *,
    day,
    status="pending",
    impact=0,
    text="rec",
):
    """Build an event anchored to a specific YYYY-MM-DD string."""
    return {
        "id": rid,
        "status": status,
        "impact_estimate_aud": impact,
        "text": text,
        "recorded_at": f"{day}T10:00:00Z",
        "responded_at": f"{day}T10:15:00Z" if status != "pending" else None,
    }


ANCHOR = date(2026, 4, 11)  # "today" — the window looks at 04-04 .. 04-10


# ---------------------------------------------------------------------------
# compute_daily_rollups
# ---------------------------------------------------------------------------

def test_daily_rollups_default_window_is_7_days_yesterday_last():
    rolls = tr.compute_daily_rollups([], window_days=7, today=ANCHOR)
    assert len(rolls) == 7
    # Oldest first, so index 0 is 7 days before anchor (04-04) and
    # index 6 is 1 day before anchor (04-10 = yesterday).
    assert rolls[0]["date"] == "2026-04-04"
    assert rolls[-1]["date"] == "2026-04-10"


def test_daily_rollups_zero_rows_fill_gaps():
    evs = [
        _ev("a", day="2026-04-10", status="dismissed", impact=100),
        _ev("b", day="2026-04-06", status="accepted"),
    ]
    rolls = tr.compute_daily_rollups(evs, window_days=7, today=ANCHOR)
    # Days with no events must still appear with zero counts
    non_empty = {r["date"]: r for r in rolls if r["total_events"] > 0}
    assert set(non_empty.keys()) == {"2026-04-06", "2026-04-10"}
    # Other days zeroed
    zeros = [r for r in rolls if r["date"] not in non_empty]
    for z in zeros:
        assert z["dismissed"] == 0 and z["accepted"] == 0 and z["missed_aud"] == 0.0


def test_daily_rollups_counts_and_sums_per_day():
    evs = [
        _ev("a", day="2026-04-10", status="dismissed", impact=300),
        _ev("b", day="2026-04-10", status="dismissed", impact=200),
        _ev("c", day="2026-04-10", status="accepted",  impact=150),
    ]
    rolls = tr.compute_daily_rollups(evs, window_days=7, today=ANCHOR)
    last = rolls[-1]
    assert last["date"] == "2026-04-10"
    assert last["dismissed"] == 2
    assert last["accepted"] == 1
    assert last["missed_aud"] == 500.0
    assert last["accepted_aud"] == 150.0
    # 1 accepted / (1 + 2) dismissed
    assert abs(last["acceptance_rate"] - 0.3333) < 0.001


def test_daily_rollups_ignores_events_outside_window():
    evs = [
        _ev("ancient", day="2025-12-01", status="dismissed", impact=999),
        _ev("future",  day="2026-05-01", status="dismissed", impact=999),
        _ev("keep",    day="2026-04-08", status="dismissed", impact=100),
    ]
    rolls = tr.compute_daily_rollups(evs, window_days=7, today=ANCHOR)
    total_missed = sum(r["missed_aud"] for r in rolls)
    assert total_missed == 100.0


def test_daily_rollups_clamps_bad_window_values():
    # Garbage window → sensible default. Must not crash.
    rolls = tr.compute_daily_rollups([], window_days=0, today=ANCHOR)
    assert 1 <= len(rolls) <= 90
    rolls_big = tr.compute_daily_rollups([], window_days=1000, today=ANCHOR)
    assert len(rolls_big) <= 90


# ---------------------------------------------------------------------------
# Slope math
# ---------------------------------------------------------------------------

def test_slope_detects_rising_series():
    out = tr._slope([1, 1, 5, 5])
    assert out["first_half"] == 1.0
    assert out["second_half"] == 5.0
    assert out["delta"] == 4.0


def test_slope_detects_falling_series():
    out = tr._slope([10, 10, 2, 2])
    assert out["delta"] == -8.0


def test_slope_empty_is_all_zero():
    out = tr._slope([])
    assert out["delta"] == 0.0
    assert out["first_half"] == 0.0


def test_slope_odd_length_puts_middle_in_second_half():
    # [1, 1, 1, 5, 5] → first=[1,1], second=[1,5,5]
    out = tr._slope([1, 1, 1, 5, 5])
    assert out["first_half"] == 1.0
    assert abs(out["second_half"] - 3.6666) < 0.001


# ---------------------------------------------------------------------------
# Classifier
# ---------------------------------------------------------------------------

def test_classifier_unknown_when_no_events():
    light = tr._classify_light(
        tr._slope([0, 0, 0]), tr._slope([0, 0, 0]), total_events=0
    )
    assert light == "unknown"


def test_classifier_red_when_acceptance_crashes():
    # Acceptance drops 20pt
    accept = {"delta": -0.20}
    missed = {"delta": 0.0}
    assert tr._classify_light(accept, missed, total_events=10) == "red"


def test_classifier_red_when_missed_dollars_jump():
    accept = {"delta": 0.0}
    missed = {"delta": 500.0}
    assert tr._classify_light(accept, missed, total_events=5) == "red"


def test_classifier_amber_on_mild_regression():
    accept = {"delta": -0.05}
    missed = {"delta": 0.0}
    assert tr._classify_light(accept, missed, total_events=5) == "amber"


def test_classifier_green_when_holding_steady():
    accept = {"delta": 0.01}
    missed = {"delta": 0.0}
    assert tr._classify_light(accept, missed, total_events=5) == "green"


# ---------------------------------------------------------------------------
# Headline
# ---------------------------------------------------------------------------

def test_headline_no_events_is_actionable():
    line = tr._compose_headline(
        window_days=7,
        acceptance_slope=tr._slope([0] * 7),
        missed_slope=tr._slope([0] * 7),
        total_events=0,
        total_missed=0,
    )
    assert "no accountability events" in line.lower() or "No accountability events" in line


def test_headline_leads_with_climbing_dollars():
    line = tr._compose_headline(
        window_days=7,
        acceptance_slope={"delta": 0.0},
        missed_slope={"delta": 150.0},
        total_events=10,
        total_missed=1000,
    )
    assert "climbing" in line.lower() or "more" in line.lower()
    assert "$" in line


def test_headline_warns_on_acceptance_drop():
    line = tr._compose_headline(
        window_days=7,
        acceptance_slope={"delta": -0.12},  # -12pt
        missed_slope={"delta": 0.0},
        total_events=15,
        total_missed=500,
    )
    assert "slipping" in line.lower() or "down" in line.lower()
    assert "12" in line  # absolute pt drop


def test_headline_celebrates_rising_acceptance():
    line = tr._compose_headline(
        window_days=7,
        acceptance_slope={"delta": 0.08},  # +8pt
        missed_slope={"delta": 0.0},
        total_events=20,
        total_missed=200,
    )
    assert "up" in line.lower() or "actioning more" in line.lower()


def test_headline_flat_when_nothing_moves():
    line = tr._compose_headline(
        window_days=7,
        acceptance_slope={"delta": 0.0},
        missed_slope={"delta": 0.0},
        total_events=5,
        total_missed=100,
    )
    assert "flat" in line.lower() or "hasn't changed" in line.lower()


# ---------------------------------------------------------------------------
# compose_trend (full pipeline)
# ---------------------------------------------------------------------------

def test_compose_trend_happy_path_falling_acceptance():
    """Early days: accepting everything. Later days: dismissing it all.
    Should light red with an acceptance-drop headline."""
    evs: list = []
    # Days 04-04 / 04-05 / 04-06: all accepted
    for i, day in enumerate(["2026-04-04", "2026-04-05", "2026-04-06"]):
        evs.append(_ev(f"a{i}", day=day, status="accepted", impact=50))
    # Days 04-07 .. 04-10: dismissing everything with escalating impact
    for i, day in enumerate(["2026-04-07", "2026-04-08", "2026-04-09", "2026-04-10"]):
        evs.append(_ev(f"d{i}", day=day, status="dismissed", impact=100 + i * 50))

    trend = tr.compose_trend("v1", evs, window_days=7, today=ANCHOR)
    assert trend["window_days"] == 7
    assert trend["traffic_light"] == "red"
    assert trend["totals"]["dismissed"] == 4
    assert trend["totals"]["accepted"] == 3
    # Acceptance slope must be negative
    assert trend["slopes"]["acceptance_rate"]["delta"] < 0
    # Series length matches window
    assert len(trend["series"]["acceptance_rate"]) == 7
    assert len(trend["series"]["missed_aud"]) == 7


def test_compose_trend_healthy_pattern_is_green():
    """Constant ~75% acceptance across the whole window → green."""
    evs: list = []
    for i, day in enumerate([
        "2026-04-04", "2026-04-05", "2026-04-06", "2026-04-07",
        "2026-04-08", "2026-04-09", "2026-04-10",
    ]):
        # 3 accepted + 1 dismissed each day
        evs.append(_ev(f"a{i}1", day=day, status="accepted", impact=50))
        evs.append(_ev(f"a{i}2", day=day, status="accepted", impact=50))
        evs.append(_ev(f"a{i}3", day=day, status="accepted", impact=50))
        evs.append(_ev(f"d{i}",  day=day, status="dismissed", impact=30))
    trend = tr.compose_trend("v1", evs, window_days=7, today=ANCHOR)
    assert trend["traffic_light"] == "green"
    assert trend["totals"]["accepted"] == 21
    assert trend["totals"]["dismissed"] == 7
    assert abs(trend["totals"]["acceptance_rate"] - 0.75) < 0.01


def test_compose_trend_empty_returns_zero_state_with_useful_headline():
    trend = tr.compose_trend("v1", [], window_days=7, today=ANCHOR)
    assert trend["traffic_light"] == "unknown"
    assert trend["totals"]["events"] == 0
    assert "no accountability events" in trend["headline"].lower()
    # Sparkline series still has window_days entries (of zero)
    assert all(v == 0.0 for v in trend["series"]["missed_aud"])


def test_compose_trend_window_14_days():
    evs = [
        _ev("old", day="2026-04-01", status="dismissed", impact=100),
        _ev("mid", day="2026-04-07", status="dismissed", impact=200),
        _ev("new", day="2026-04-10", status="dismissed", impact=300),
    ]
    trend = tr.compose_trend("v1", evs, window_days=14, today=ANCHOR)
    assert trend["window_days"] == 14
    assert trend["totals"]["missed_aud"] == 600.0


def test_compose_trend_window_28_days():
    evs = [_ev("only", day="2026-03-20", status="dismissed", impact=500)]
    trend = tr.compose_trend("v1", evs, window_days=28, today=ANCHOR)
    assert trend["window_days"] == 28
    assert trend["totals"]["missed_aud"] == 500.0


def test_compose_trend_includes_venue_id_and_timestamp():
    trend = tr.compose_trend("venue_xyz", [], window_days=7, today=ANCHOR)
    assert trend["venue_id"] == "venue_xyz"
    assert trend["generated_at"].endswith("Z")


# ---------------------------------------------------------------------------
# compose_trend_from_store
# ---------------------------------------------------------------------------

def test_compose_trend_from_store_uses_injected_stub():
    events = [_ev("a", day="2026-04-10", status="dismissed", impact=250)]

    class StubStore:
        def history(self, venue_id):
            assert venue_id == "venue_demo"
            return events

    trend = tr.compose_trend_from_store(
        "venue_demo",
        window_days=7,
        today=ANCHOR,
        store=StubStore(),
    )
    assert trend["totals"]["missed_aud"] == 250.0


def test_compose_trend_from_store_default_uses_real_store():
    from rosteriq import accountability_store as store
    store.clear()
    ev = store.record(
        "venue_trend_test",
        text="Cut 1 bar staff",
        source="wage_pulse",
        priority="high",
        impact_estimate_aud=175.0,
        rec_id="rec_trend_roundtrip_1",
    )
    store.respond("venue_trend_test", ev["id"], status="dismissed")
    from datetime import datetime, timezone
    today = datetime.now(timezone.utc).date()
    trend = tr.compose_trend_from_store(
        "venue_trend_test", window_days=7, today=today + __import__("datetime").timedelta(days=1)
    )
    store.clear()
    # The dismissal happened 'today' relative to wall clock, so anchor
    # = today+1 puts it within the window.
    assert trend["totals"]["missed_aud"] == 175.0
    assert trend["totals"]["dismissed"] == 1


if __name__ == "__main__":
    tests = [v for k, v in list(globals().items()) if k.startswith("test_") and callable(v)]
    passed = failed = 0
    for t in tests:
        try:
            t()
            passed += 1
            print(f"  PASS {t.__name__}")
        except AssertionError as e:
            failed += 1
            print(f"  FAIL {t.__name__}: {e}")
        except Exception as e:
            failed += 1
            print(f"  ERROR {t.__name__}: {type(e).__name__}: {e}")
    print(f"\n{passed}/{passed+failed} tests passed")
    sys.exit(0 if failed == 0 else 1)
