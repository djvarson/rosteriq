"""Tests for rosteriq.weekly_digest — pure stdlib, no pytest."""
from __future__ import annotations

import sys
from datetime import date, datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from rosteriq import weekly_digest as wd  # noqa: E402


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _ev(
    rid,
    *,
    day,
    status="dismissed",
    impact=0,
    text="rec",
    suffix="over_wage_high",
):
    """Build an event anchored to a specific YYYY-MM-DD string.

    `day` can be a date or a YYYY-MM-DD string. The rec_id is
    auto-generated from the suffix so the action-suffix detector finds
    it (unless `rid` is explicitly provided).
    """
    if isinstance(day, date):
        day = day.isoformat()
    rec_id = rid or f"rec_pulse_v_{day}_{suffix}"
    return {
        "id": rec_id,
        "rec_id": rec_id,
        "status": status,
        "impact_estimate_aud": impact,
        "text": text,
        "responded_at": f"{day}T10:00:00Z",
        "recorded_at": f"{day}T09:00:00Z",
    }


class StubStore:
    def __init__(self, events):
        self._events = list(events)

    def history(self, venue_id):
        return list(self._events)


# ---------------------------------------------------------------------------
# _parse_date
# ---------------------------------------------------------------------------

def test_parse_date_from_iso_string():
    assert wd._parse_date("2026-04-12") == date(2026, 4, 12)


def test_parse_date_from_iso_with_time():
    assert wd._parse_date("2026-04-12T14:00:00Z") == date(2026, 4, 12)


def test_parse_date_from_date_obj():
    d = date(2026, 4, 12)
    assert wd._parse_date(d) == d


def test_parse_date_from_datetime_obj():
    dt = datetime(2026, 4, 12, 15, 30, tzinfo=timezone.utc)
    assert wd._parse_date(dt) == date(2026, 4, 12)


def test_parse_date_returns_none_for_garbage():
    assert wd._parse_date(None) is None
    assert wd._parse_date("not a date") is None
    assert wd._parse_date("") is None
    assert wd._parse_date(42) is None


# ---------------------------------------------------------------------------
# _week_window
# ---------------------------------------------------------------------------

def test_week_window_defaults_7_days_ending_yesterday():
    today_utc = datetime.now(timezone.utc).date()
    start, end = wd._week_window(None)
    assert end == today_utc - __import__("datetime").timedelta(days=1)
    assert (end - start).days == 6  # inclusive window


def test_week_window_explicit_end_date():
    end_date = date(2026, 4, 12)  # Sunday
    start, end = wd._week_window(end_date)
    assert end == end_date
    assert start == date(2026, 4, 6)
    assert (end - start).days == 6


def test_week_window_custom_days():
    start, end = wd._week_window(date(2026, 4, 12), window_days=14)
    assert (end - start).days == 13


def test_week_window_clamps_zero_window_to_one():
    start, end = wd._week_window(date(2026, 4, 12), window_days=0)
    assert start == end


def test_week_window_clamps_absurdly_large_window():
    start, end = wd._week_window(date(2026, 4, 12), window_days=500)
    assert (end - start).days == 89  # clamped to 90


# ---------------------------------------------------------------------------
# _events_in_window
# ---------------------------------------------------------------------------

def test_events_in_window_filters_outside_dates():
    events = [
        _ev(None, day="2026-04-01"),
        _ev(None, day="2026-04-07"),
        _ev(None, day="2026-04-12"),
        _ev(None, day="2026-04-15"),
    ]
    in_win = wd._events_in_window(events, date(2026, 4, 6), date(2026, 4, 12))
    in_days = {e["rec_id"][-4:] for e in in_win}
    assert len(in_win) == 2


def test_events_in_window_inclusive_on_both_ends():
    events = [
        _ev(None, day="2026-04-06"),  # start
        _ev(None, day="2026-04-12"),  # end
    ]
    in_win = wd._events_in_window(events, date(2026, 4, 6), date(2026, 4, 12))
    assert len(in_win) == 2


def test_events_in_window_skips_events_without_dates():
    events = [{"id": "x", "status": "dismissed"}]
    assert wd._events_in_window(events, date(2026, 4, 6), date(2026, 4, 12)) == []


def test_events_in_window_prefers_responded_at_over_recorded_at():
    # recorded_at inside window, responded_at outside → excluded
    ev = {
        "id": "x",
        "rec_id": "rec_pulse_v_2026-04-20_over_wage_high",
        "status": "dismissed",
        "recorded_at": "2026-04-10T10:00:00Z",
        "responded_at": "2026-04-20T10:00:00Z",
    }
    assert wd._events_in_window([ev], date(2026, 4, 1), date(2026, 4, 12)) == []


# ---------------------------------------------------------------------------
# _roll_up_week
# ---------------------------------------------------------------------------

def test_roll_up_empty():
    r = wd._roll_up_week([])
    assert r["total_events"] == 0
    assert r["acceptance_rate"] == 0.0
    assert r["dismissed_aud"] == 0.0


def test_roll_up_counts_and_sums():
    events = [
        _ev(None, day="2026-04-10", status="accepted", impact=100),
        _ev(None, day="2026-04-11", status="dismissed", impact=250),
        _ev(None, day="2026-04-12", status="dismissed", impact=400),
        _ev(None, day="2026-04-12", status="pending"),
    ]
    r = wd._roll_up_week(events)
    assert r["total_events"] == 4
    assert r["accepted"] == 1
    assert r["dismissed"] == 2
    assert r["pending"] == 1
    assert r["dismissed_aud"] == 650.0
    assert r["accepted_aud"] == 100.0
    # 1 accepted / 3 responded
    assert abs(r["acceptance_rate"] - 1 / 3) < 0.001


def test_roll_up_tolerates_bad_impact_types():
    events = [
        _ev(None, day="2026-04-10", status="dismissed", impact="bad"),
        _ev(None, day="2026-04-11", status="dismissed", impact=None),
    ]
    r = wd._roll_up_week(events)
    assert r["dismissed"] == 2
    assert r["dismissed_aud"] == 0.0


# ---------------------------------------------------------------------------
# _detect_patterns
# ---------------------------------------------------------------------------

def test_detect_patterns_groups_by_suffix():
    events = [
        _ev(None, day="2026-04-10", status="dismissed", impact=200, suffix="over_wage_high"),
        _ev(None, day="2026-04-11", status="dismissed", impact=150, suffix="over_wage_high"),
        _ev(None, day="2026-04-12", status="dismissed", impact=80, suffix="over_wage_med"),
    ]
    patterns = wd._detect_patterns(events)
    assert len(patterns) == 2
    assert patterns[0]["pattern"] == "over_wage_high"
    assert patterns[0]["count"] == 2
    assert patterns[0]["dismissed_aud"] == 350.0


def test_detect_patterns_ignores_accepted():
    events = [
        _ev(None, day="2026-04-10", status="accepted", impact=200, suffix="over_wage_high"),
        _ev(None, day="2026-04-11", status="dismissed", impact=150, suffix="over_wage_med"),
    ]
    patterns = wd._detect_patterns(events)
    assert len(patterns) == 1
    assert patterns[0]["pattern"] == "over_wage_med"


def test_detect_patterns_caps_at_limit():
    events = []
    for i, suffix in enumerate(["over_wage_high", "over_wage_med", "under_wage", "burn_rate_high"]):
        events.append(_ev(None, day=f"2026-04-{10 + i:02d}", status="dismissed", impact=i * 100, suffix=suffix))
    patterns = wd._detect_patterns(events, limit=2)
    assert len(patterns) == 2


def test_detect_patterns_deterministic_tiebreak_on_count_and_impact():
    # Two suffixes with identical count — higher impact wins, then alphabetical
    events = [
        _ev(None, day="2026-04-10", status="dismissed", impact=100, suffix="under_wage"),
        _ev(None, day="2026-04-11", status="dismissed", impact=100, suffix="under_wage"),
        _ev(None, day="2026-04-12", status="dismissed", impact=300, suffix="over_wage_high"),
        _ev(None, day="2026-04-13", status="dismissed", impact=300, suffix="over_wage_high"),
    ]
    patterns = wd._detect_patterns(events)
    assert patterns[0]["pattern"] == "over_wage_high"  # higher impact


def test_detect_patterns_labels_known_suffixes():
    events = [
        _ev(None, day="2026-04-10", status="dismissed", impact=500, suffix="over_wage_high"),
    ]
    patterns = wd._detect_patterns(events)
    assert "Cut-staff" in patterns[0]["label"]


def test_detect_patterns_ignores_non_pulse_rec_ids():
    events = [
        {
            "id": "rec_manual_abc",
            "rec_id": "rec_manual_abc",
            "status": "dismissed",
            "impact_estimate_aud": 500,
            "text": "manual",
            "responded_at": "2026-04-10T10:00:00Z",
        }
    ]
    patterns = wd._detect_patterns(events)
    assert patterns == []


# ---------------------------------------------------------------------------
# _traffic_light
# ---------------------------------------------------------------------------

def test_traffic_light_green_for_empty_week():
    assert wd._traffic_light({"total_events": 0}) == "green"


def test_traffic_light_red_on_big_dismiss_dollar():
    assert (
        wd._traffic_light(
            {"total_events": 5, "dismissed_aud": 1500, "acceptance_rate": 0.8}
        )
        == "red"
    )


def test_traffic_light_red_on_low_acceptance_rate():
    assert (
        wd._traffic_light(
            {"total_events": 10, "dismissed_aud": 0, "acceptance_rate": 0.2}
        )
        == "red"
    )


def test_traffic_light_amber_on_medium_dismiss_dollar():
    assert (
        wd._traffic_light(
            {"total_events": 5, "dismissed_aud": 400, "acceptance_rate": 0.7}
        )
        == "amber"
    )


def test_traffic_light_green_on_healthy_week():
    assert (
        wd._traffic_light(
            {"total_events": 10, "dismissed_aud": 0, "acceptance_rate": 0.95}
        )
        == "green"
    )


# ---------------------------------------------------------------------------
# compose_weekly_digest — full pipeline
# ---------------------------------------------------------------------------

def test_compose_digest_empty_week_returns_all_quiet():
    d = wd.compose_weekly_digest(
        "venue_1",
        [],
        week_ending=date(2026, 4, 12),
        window_days=7,
    )
    assert d["venue_id"] == "venue_1"
    assert d["traffic_light"] == "green"
    assert d["rollup"]["total_events"] == 0
    assert "quiet" in d["headline"].lower()
    assert d["should_send"] is False
    assert d["patterns"] == []


def test_compose_digest_clean_week_all_actioned():
    events = [
        _ev(None, day="2026-04-10", status="accepted", impact=200),
        _ev(None, day="2026-04-11", status="accepted", impact=150),
    ]
    d = wd.compose_weekly_digest(
        "venue_1", events, week_ending=date(2026, 4, 12), window_days=7
    )
    assert d["rollup"]["dismissed"] == 0
    assert d["rollup"]["accepted"] == 2
    assert "clean" in d["headline"].lower()
    assert d["traffic_light"] == "green"
    assert d["should_send"] is True


def test_compose_digest_loud_week_flags_red_and_dollars():
    events = [
        _ev(None, day=f"2026-04-{d:02d}", status="dismissed", impact=300)
        for d in range(6, 13)
    ]
    d = wd.compose_weekly_digest(
        "venue_1", events, week_ending=date(2026, 4, 12), window_days=7
    )
    assert d["rollup"]["dismissed"] == 7
    assert d["rollup"]["dismissed_aud"] == 2100.0
    assert d["traffic_light"] == "red"
    assert "$2,100" in d["headline"] or "$2100" in d["headline"]


def test_compose_digest_top_pattern_drives_one_pattern_line():
    events = [
        _ev(None, day=f"2026-04-{d:02d}", status="dismissed", impact=200, suffix="over_wage_high")
        for d in range(7, 12)
    ]
    # Plus one lower-count pattern
    events.append(_ev(None, day="2026-04-12", status="dismissed", impact=100, suffix="burn_rate_high"))
    d = wd.compose_weekly_digest(
        "venue_1", events, week_ending=date(2026, 4, 12), window_days=7
    )
    assert len(d["patterns"]) == 2
    assert d["patterns"][0]["pattern"] == "over_wage_high"
    assert "cut-staff" in d["one_pattern"].lower()
    assert "5 times" in d["one_pattern"]


def test_compose_digest_window_boundaries_honoured():
    # Event outside the 7-day window should be excluded
    events = [
        _ev(None, day="2026-04-01", status="dismissed", impact=1000),  # out
        _ev(None, day="2026-04-10", status="dismissed", impact=200),   # in
    ]
    d = wd.compose_weekly_digest(
        "venue_1", events, week_ending=date(2026, 4, 12), window_days=7
    )
    assert d["rollup"]["total_events"] == 1
    assert d["rollup"]["dismissed_aud"] == 200.0


def test_compose_digest_passes_through_venue_label():
    d = wd.compose_weekly_digest(
        "venue_1", [], week_ending=date(2026, 4, 12), venue_label="Mojo's"
    )
    assert d["venue_label"] == "Mojo's"


def test_compose_digest_falls_back_venue_label_to_id():
    d = wd.compose_weekly_digest(
        "venue_xyz", [], week_ending=date(2026, 4, 12)
    )
    assert d["venue_label"] == "venue_xyz"


def test_compose_digest_date_fields_cover_full_window():
    d = wd.compose_weekly_digest(
        "v", [], week_ending=date(2026, 4, 12), window_days=7
    )
    assert d["week_start"] == "2026-04-06"
    assert d["week_end"] == "2026-04-12"
    assert d["window_days"] == 7


def test_compose_digest_deterministic_across_calls():
    events = [
        _ev(None, day="2026-04-10", status="dismissed", impact=200),
        _ev(None, day="2026-04-11", status="dismissed", impact=150),
    ]
    d1 = wd.compose_weekly_digest("v", events, week_ending=date(2026, 4, 12))
    d2 = wd.compose_weekly_digest("v", events, week_ending=date(2026, 4, 12))
    # Ignore generated_at which is clock-based
    d1.pop("generated_at")
    d2.pop("generated_at")
    assert d1 == d2


# ---------------------------------------------------------------------------
# render_text
# ---------------------------------------------------------------------------

def test_render_text_contains_headline_and_label():
    d = wd.compose_weekly_digest(
        "v", [], week_ending=date(2026, 4, 12), venue_label="Mojo's"
    )
    out = wd.render_text(d)
    assert "Mojo's" in out
    assert "2026-04-12" in out
    assert "quiet" in out.lower() or "clean" in out.lower()


def test_render_text_lists_top_patterns():
    events = [
        _ev(None, day="2026-04-10", status="dismissed", impact=300, suffix="over_wage_high"),
        _ev(None, day="2026-04-11", status="dismissed", impact=100, suffix="burn_rate_high"),
    ]
    d = wd.compose_weekly_digest("v", events, week_ending=date(2026, 4, 12))
    out = wd.render_text(d)
    assert "Cut-staff" in out
    assert "Burn-rate" in out


def test_render_text_is_pure_ascii_safe():
    events = [_ev(None, day="2026-04-10", status="dismissed", impact=500)]
    d = wd.compose_weekly_digest("v", events, week_ending=date(2026, 4, 12))
    out = wd.render_text(d)
    # Should render — no exceptions, no None
    assert isinstance(out, str) and len(out) > 0


# ---------------------------------------------------------------------------
# compose_weekly_digest_from_store
# ---------------------------------------------------------------------------

def test_from_store_uses_injected_stub():
    events = [
        _ev(None, day="2026-04-10", status="dismissed", impact=200),
    ]
    stub = StubStore(events)
    d = wd.compose_weekly_digest_from_store(
        "v", week_ending=date(2026, 4, 12), store=stub
    )
    assert d["rollup"]["dismissed"] == 1


def test_from_store_default_uses_real_store():
    from rosteriq import accountability_store as store
    store.clear()
    try:
        ev = store.record(
            "venue_weekly_test",
            text="Wage % projected at 33%. Cut 2 staff.",
            source="wage_pulse",
            priority="high",
            impact_estimate_aud=500.0,
            rec_id="rec_pulse_venue_weekly_test_2026-04-12_over_wage_high",
        )
        store.respond("venue_weekly_test", ev["id"], status="dismissed")
        # Window anchored to wall-clock today+some buffer so the event falls in
        today = datetime.now(timezone.utc).date()
        d = wd.compose_weekly_digest_from_store(
            "venue_weekly_test", week_ending=today, window_days=7
        )
        assert d["rollup"]["dismissed"] == 1
    finally:
        store.clear()


# ---------------------------------------------------------------------------
# Test runner
# ---------------------------------------------------------------------------

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
