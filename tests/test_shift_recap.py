"""Tests for the pure-stdlib shift-recap composer (rosteriq.shift_recap).

Runs without pytest / FastAPI / Pydantic — bottom-of-file runner executes
every `test_` function and reports pass/fail.
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from rosteriq import shift_recap as sr  # noqa: E402


# ---------------------------------------------------------------------------
# summarise_headcount
# ---------------------------------------------------------------------------

def test_summarise_empty_history_returns_zeros():
    out = sr.summarise_headcount([])
    assert out["peak"] == 0
    assert out["peak_time"] is None
    assert out["last_count"] == 0
    assert out["total_taps"] == 0
    assert out["reset_count"] == 0


def test_summarise_picks_peak_count_after_and_formats_time():
    history = [
        {"timestamp": "2026-04-11T18:00:00Z", "delta": 10, "count_after": 10, "source": "button"},
        {"timestamp": "2026-04-11T20:30:00Z", "delta": 40, "count_after": 50, "source": "group"},
        {"timestamp": "2026-04-11T21:00:00Z", "delta": 12, "count_after": 62, "source": "button"},
        {"timestamp": "2026-04-11T22:15:00Z", "delta": -15, "count_after": 47, "source": "button"},
    ]
    out = sr.summarise_headcount(history)
    assert out["peak"] == 62
    assert out["peak_time"] == "21:00"
    assert out["last_count"] == 47
    assert out["total_taps"] == 4          # 3 button + 1 group
    assert out["reset_count"] == 0


def test_summarise_counts_reset_separately_and_accepts_unsorted_input():
    history = [
        {"timestamp": "2026-04-11T22:00:00Z", "delta": 0, "count_after": 0, "source": "reset"},
        {"timestamp": "2026-04-11T18:00:00Z", "delta": 10, "count_after": 10, "source": "button"},
        {"timestamp": "2026-04-11T21:00:00Z", "delta": 5, "count_after": 15, "source": "button"},
    ]
    out = sr.summarise_headcount(history)
    assert out["peak"] == 15
    assert out["peak_time"] == "21:00"
    assert out["last_count"] == 0           # last in chronological order is the reset
    assert out["total_taps"] == 2           # reset is not a tap
    assert out["reset_count"] == 1


def test_summarise_tolerates_missing_fields():
    history = [
        {"timestamp": "2026-04-11T18:00:00Z", "count_after": 5},
        {"timestamp": "2026-04-11T19:00:00Z", "count_after": None, "source": "button"},
    ]
    out = sr.summarise_headcount(history)
    # 'None' count_after becomes 0 via _safe_int; 5 stays as peak.
    assert out["peak"] == 5


# ---------------------------------------------------------------------------
# compose_recap
# ---------------------------------------------------------------------------

def test_compose_recap_green_path_beats_forecast_and_undershoots_wage():
    recap = sr.compose_recap(
        venue_id="mojos",
        shift_date="2026-04-11",
        revenue_actual=22_000,
        revenue_forecast=18_000,
        wages_actual=5_000,     # 5000/22000 ≈ 22.7%
        wages_forecast=5_400,
        wage_target_pct=0.28,
        headcount_history=[
            {"timestamp": "2026-04-11T20:00:00Z", "delta": 30, "count_after": 30, "source": "group"},
        ],
    )
    assert recap["venue_id"] == "mojos"
    assert recap["shift_date"] == "2026-04-11"
    assert recap["revenue"]["actual"] == 22_000.0
    assert recap["revenue"]["forecast"] == 18_000.0
    assert recap["revenue"]["delta"] == 4_000.0
    # delta_pct ≈ 0.2222
    assert abs(recap["revenue"]["delta_pct"] - 0.2222) < 0.001
    assert recap["wages"]["pct_of_revenue_actual"] < 0.28
    assert recap["wages"]["pct_delta"] < 0
    assert recap["headcount"]["peak"] == 30
    assert recap["traffic_light"] == "green"
    assert "Clean shift" in recap["summary"]
    assert "peak 30" in recap["summary"]


def test_compose_recap_red_path_on_wage_overshoot():
    recap = sr.compose_recap(
        venue_id="earls",
        shift_date="2026-04-11",
        revenue_actual=10_000,
        revenue_forecast=10_000,   # neutral revenue
        wages_actual=3_500,        # 35% — way over 28% target
        wages_forecast=3_000,
        wage_target_pct=0.28,
        headcount_history=[],
    )
    assert recap["wages"]["pct_of_revenue_actual"] == 0.35
    assert recap["wages"]["pct_delta"] > 0.06
    assert recap["traffic_light"] == "red"
    assert "Tough shift" in recap["summary"]


def test_compose_recap_red_path_on_revenue_miss():
    recap = sr.compose_recap(
        venue_id="corner",
        shift_date="2026-04-11",
        revenue_actual=8_000,
        revenue_forecast=12_000,   # -33.3%
        wages_actual=2_200,        # wage % is fine
        wages_forecast=3_600,
        wage_target_pct=0.30,
        headcount_history=[],
    )
    assert recap["revenue"]["delta_pct"] < -0.10
    assert recap["traffic_light"] == "red"
    assert "Tough shift" in recap["summary"]


def test_compose_recap_amber_band():
    # -7% revenue miss (between AMBER_REVENUE_MISS=5% and RED_REVENUE_MISS=10%).
    # wages set to exactly 30% of actual revenue (9300 * 0.30 = 2790) so
    # wage-side is green and the worst-of collapse lands at amber.
    recap = sr.compose_recap(
        venue_id="francine",
        shift_date="2026-04-11",
        revenue_actual=9_300,
        revenue_forecast=10_000,
        wages_actual=2_790,
        wages_forecast=3_000,
        wage_target_pct=0.30,
        headcount_history=[],
    )
    assert -0.10 < recap["revenue"]["delta_pct"] <= -0.05
    assert abs(recap["wages"]["pct_delta"]) < 0.005
    assert recap["traffic_light"] == "amber"
    assert "Mixed shift" in recap["summary"]


def test_compose_recap_wage_fallback_from_revenue_when_no_wages_supplied():
    recap = sr.compose_recap(
        venue_id="clock",
        shift_date="2026-04-11",
        revenue_actual=10_000,
        revenue_forecast=10_000,
        wages_actual=None,
        wages_forecast=None,
        wage_target_pct=0.30,
    )
    # Fallback is WAGE_TO_REVENUE_FALLBACK=30% → exactly on target → green.
    assert recap["wages"]["actual"] == 3_000.0
    assert recap["wages"]["forecast"] == 3_000.0
    # 0.30 - 0.30 = 0 ± tiny float slop → still green
    assert abs(recap["wages"]["pct_delta"]) < 0.005
    assert recap["traffic_light"] == "green"


def test_compose_recap_zero_forecast_revenue_does_not_crash():
    recap = sr.compose_recap(
        venue_id="freo",
        shift_date="2026-04-11",
        revenue_actual=4_200,
        revenue_forecast=0,
        wages_actual=1_000,
        wages_forecast=0,
        wage_target_pct=0.30,
    )
    assert recap["revenue"]["delta_pct"] == 0.0   # safe when forecast is 0
    # wage % of revenue is computable because actual revenue > 0
    assert recap["wages"]["pct_of_revenue_actual"] > 0
    assert "no forecast" in recap["summary"]


def test_compose_recap_zero_actual_revenue_gives_sane_zeros():
    recap = sr.compose_recap(
        venue_id="courthouse",
        shift_date="2026-04-11",
        revenue_actual=0,
        revenue_forecast=10_000,
        wages_actual=0,
        wages_forecast=3_000,
        wage_target_pct=0.30,
    )
    assert recap["wages"]["pct_of_revenue_actual"] == 0.0
    assert recap["revenue"]["delta_pct"] < 0  # down 100%
    assert recap["traffic_light"] == "red"


def test_compose_recap_headcount_peak_flows_into_summary_text():
    recap = sr.compose_recap(
        venue_id="bar_francine",
        shift_date="2026-04-11",
        revenue_actual=15_000,
        revenue_forecast=14_500,
        wages_actual=4_050,        # 27% — under 28% target
        wages_forecast=4_060,
        wage_target_pct=0.28,
        headcount_history=[
            {"timestamp": "2026-04-11T19:30:00Z", "delta": 20, "count_after": 20, "source": "group"},
            {"timestamp": "2026-04-11T20:45:00Z", "delta": 35, "count_after": 55, "source": "group"},
            {"timestamp": "2026-04-11T21:15:00Z", "delta": -10, "count_after": 45, "source": "button"},
        ],
    )
    assert recap["headcount"]["peak"] == 55
    assert "peak 55" in recap["summary"]
    assert recap["traffic_light"] == "green"


def test_compose_recap_generated_at_is_utc_iso_and_z_suffix():
    recap = sr.compose_recap(
        venue_id="v",
        shift_date="2026-04-11",
        revenue_actual=1000,
        revenue_forecast=1000,
    )
    ts = recap["generated_at"]
    assert ts.endswith("Z")
    assert len(ts) == 20  # YYYY-MM-DDTHH:MM:SSZ


def test_compose_recap_rounds_numbers_to_two_dp():
    recap = sr.compose_recap(
        venue_id="v",
        shift_date="2026-04-11",
        revenue_actual=1234.5678,
        revenue_forecast=1000.1234,
        wages_actual=333.9999,
        wages_forecast=300.0,
    )
    # All money fields are rounded to 2dp
    assert recap["revenue"]["actual"] == 1234.57
    assert recap["revenue"]["forecast"] == 1000.12
    assert recap["wages"]["actual"] == 334.0    # 333.9999 rounds to 334
    assert recap["wages"]["forecast"] == 300.0


# ---------------------------------------------------------------------------
# summarise_accountability
# ---------------------------------------------------------------------------

def test_summarise_accountability_empty_returns_zero_block():
    out = sr.summarise_accountability([])
    assert out["total"] == 0
    assert out["pending"] == 0
    assert out["accepted"] == 0
    assert out["dismissed"] == 0
    assert out["estimated_impact_missed_aud"] == 0.0
    assert out["estimated_impact_pending_aud"] == 0.0
    assert out["acceptance_rate"] == 0.0
    assert out["top_missed"] == []


def test_summarise_accountability_counts_and_rates():
    recs = [
        {"id": "r1", "status": "accepted", "impact_estimate_aud": 100, "text": "cut bar",     "priority": "high"},
        {"id": "r2", "status": "accepted", "impact_estimate_aud": None, "text": "cut kitchen", "priority": "med"},
        {"id": "r3", "status": "dismissed", "impact_estimate_aud": 420, "text": "send 2 home", "priority": "high"},
        {"id": "r4", "status": "dismissed", "impact_estimate_aud": 180, "text": "send 1 home", "priority": "med"},
        {"id": "r5", "status": "pending",   "impact_estimate_aud": 50,  "text": "maybe cut",   "priority": "low"},
    ]
    out = sr.summarise_accountability(recs)
    assert out["total"] == 5
    assert out["pending"] == 1
    assert out["accepted"] == 2
    assert out["dismissed"] == 2
    assert out["estimated_impact_missed_aud"] == 600.0
    assert out["estimated_impact_pending_aud"] == 50.0
    # acceptance = 2 accepted / (2 accepted + 2 dismissed) = 0.5
    assert out["acceptance_rate"] == 0.5
    # top_missed must be sorted by impact descending
    assert len(out["top_missed"]) == 2
    assert out["top_missed"][0]["id"] == "r3"
    assert out["top_missed"][0]["impact_estimate_aud"] == 420.0
    assert out["top_missed"][1]["id"] == "r4"


def test_summarise_accountability_limits_top_missed():
    recs = [
        {"id": f"rec_{i}", "status": "dismissed", "impact_estimate_aud": 10 * (i + 1), "text": f"t{i}"}
        for i in range(10)
    ]
    out = sr.summarise_accountability(recs, top_missed_limit=3)
    assert len(out["top_missed"]) == 3
    # biggest impacts first (rec_9 = 100, rec_8 = 90, rec_7 = 80)
    assert out["top_missed"][0]["id"] == "rec_9"
    assert out["top_missed"][2]["id"] == "rec_7"


def test_summarise_accountability_tolerates_missing_fields():
    recs = [
        {"id": "a", "status": "dismissed"},  # no impact
        {"status": "accepted"},               # no id
        {},                                    # nothing — defaults to pending
    ]
    out = sr.summarise_accountability(recs)
    assert out["total"] == 3
    assert out["pending"] == 1
    assert out["accepted"] == 1
    assert out["dismissed"] == 1
    assert out["estimated_impact_missed_aud"] == 0.0


# ---------------------------------------------------------------------------
# compose_recap — recommendations parameter + accountability traffic light
# ---------------------------------------------------------------------------

def test_compose_recap_emits_accountability_block():
    recap = sr.compose_recap(
        venue_id="mojos",
        shift_date="2026-04-11",
        revenue_actual=20_000,
        revenue_forecast=20_000,
        wages_actual=5_600,
        wages_forecast=5_600,
        wage_target_pct=0.28,
        recommendations=[
            {"id": "r1", "status": "accepted", "impact_estimate_aud": 100, "text": "cut bar"},
        ],
    )
    assert "accountability" in recap
    assert recap["accountability"]["total"] == 1
    assert recap["accountability"]["accepted"] == 1


def test_compose_recap_red_on_big_missed_impact_even_when_financials_are_fine():
    # Revenue flat, wages on target — financials are all green.
    # But a $400 dismissed rec should drag the light to red via accountability.
    recap = sr.compose_recap(
        venue_id="francine",
        shift_date="2026-04-11",
        revenue_actual=20_000,
        revenue_forecast=20_000,
        wages_actual=5_600,
        wages_forecast=5_600,
        wage_target_pct=0.28,
        recommendations=[
            {"id": "r1", "status": "dismissed", "impact_estimate_aud": 400, "text": "cut 2 bartenders"},
        ],
    )
    assert recap["traffic_light"] == "red"
    assert "1 rec dismissed" in recap["summary"]
    assert "at stake" in recap["summary"]


def test_compose_recap_amber_on_medium_missed_impact():
    # $150 missed — above amber threshold (100) but below red (300).
    recap = sr.compose_recap(
        venue_id="francine",
        shift_date="2026-04-11",
        revenue_actual=20_000,
        revenue_forecast=20_000,
        wages_actual=5_600,
        wages_forecast=5_600,
        wage_target_pct=0.28,
        recommendations=[
            {"id": "r1", "status": "accepted", "impact_estimate_aud": 0, "text": "ok"},
            {"id": "r2", "status": "dismissed", "impact_estimate_aud": 150, "text": "cut 1"},
        ],
    )
    assert recap["traffic_light"] == "amber"


def test_compose_recap_amber_when_dismissed_with_zero_accepted():
    # Even at zero dollar missed, dismissing with no acceptances at all
    # should drag the light to at least amber.
    recap = sr.compose_recap(
        venue_id="earls",
        shift_date="2026-04-11",
        revenue_actual=20_000,
        revenue_forecast=20_000,
        wages_actual=5_600,
        wages_forecast=5_600,
        wage_target_pct=0.28,
        recommendations=[
            {"id": "r1", "status": "dismissed", "impact_estimate_aud": 0, "text": "whatever"},
        ],
    )
    assert recap["traffic_light"] == "amber"


def test_compose_recap_green_when_recs_all_accepted():
    recap = sr.compose_recap(
        venue_id="clock",
        shift_date="2026-04-11",
        revenue_actual=20_000,
        revenue_forecast=20_000,
        wages_actual=5_600,
        wages_forecast=5_600,
        wage_target_pct=0.28,
        recommendations=[
            {"id": "r1", "status": "accepted", "impact_estimate_aud": 400, "text": "cut 2"},
            {"id": "r2", "status": "accepted", "impact_estimate_aud": 100, "text": "cut 1"},
        ],
    )
    assert recap["traffic_light"] == "green"


def test_compose_recap_pending_recs_do_not_affect_light():
    # Pending recs should never drag the light — they haven't been
    # decided yet. Only accepted/dismissed actions count.
    recap = sr.compose_recap(
        venue_id="v",
        shift_date="2026-04-11",
        revenue_actual=20_000,
        revenue_forecast=20_000,
        wages_actual=5_600,
        wages_forecast=5_600,
        wage_target_pct=0.28,
        recommendations=[
            {"id": "r1", "status": "pending", "impact_estimate_aud": 9999, "text": "huge pending rec"},
        ],
    )
    assert recap["traffic_light"] == "green"


def test_compose_recap_no_recommendations_at_all_still_renders():
    recap = sr.compose_recap(
        venue_id="v",
        shift_date="2026-04-11",
        revenue_actual=20_000,
        revenue_forecast=20_000,
        wages_actual=5_600,
        wages_forecast=5_600,
        wage_target_pct=0.28,
        recommendations=None,
    )
    assert recap["accountability"]["total"] == 0
    assert recap["traffic_light"] == "green"


def test_compose_recap_summary_includes_dismissed_tail_with_dollars():
    recap = sr.compose_recap(
        venue_id="demo",
        shift_date="2026-04-11",
        revenue_actual=14_800,
        revenue_forecast=15_500,
        wages_actual=4_700,       # 4700/14800 ≈ 31.8%
        wages_forecast=4_340,
        wage_target_pct=0.28,
        headcount_history=[
            {"timestamp": "2026-04-11T20:45:00Z", "count_after": 55, "source": "group"},
        ],
        recommendations=[
            {"id": "r1", "status": "accepted",  "impact_estimate_aud": 200, "text": "cut bar at 8"},
            {"id": "r2", "status": "dismissed", "impact_estimate_aud": 420, "text": "cut 2 bartenders"},
            {"id": "r3", "status": "dismissed", "impact_estimate_aud": 180, "text": "send 1 floor"},
        ],
    )
    # Summary should include the dismissed tail with a rounded $ total.
    assert "2 recs dismissed" in recap["summary"]
    assert "at stake" in recap["summary"]
    # Traffic light should be red — wage overshoot is >2pt AND missed impact is $600.
    assert recap["traffic_light"] == "red"


if __name__ == "__main__":
    tests = [v for k, v in list(globals().items()) if k.startswith("test_") and callable(v)]
    passed = 0
    failed = 0
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
