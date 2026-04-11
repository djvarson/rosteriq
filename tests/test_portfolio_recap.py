"""Tests for rosteriq.portfolio_recap — pure stdlib, no pytest."""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from rosteriq import portfolio_recap as pr  # noqa: E402
from rosteriq import shift_recap as sr  # noqa: E402


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _mk_recap(
    venue_id="v",
    *,
    rev_actual=20_000,
    rev_forecast=20_000,
    wages_actual=5_600,
    wages_forecast=5_600,
    wage_target=0.28,
    headcount=[],
    recommendations=None,
):
    return sr.compose_recap(
        venue_id=venue_id,
        shift_date="2026-04-11",
        revenue_actual=rev_actual,
        revenue_forecast=rev_forecast,
        wages_actual=wages_actual,
        wages_forecast=wages_forecast,
        wage_target_pct=wage_target,
        headcount_history=headcount,
        recommendations=recommendations,
    )


# ---------------------------------------------------------------------------
# Worst-of traffic light
# ---------------------------------------------------------------------------

def test_worst_light_picks_red_over_amber_and_green():
    assert pr._worst_light(["green", "amber", "red"]) == "red"
    assert pr._worst_light(["green", "amber"]) == "amber"
    assert pr._worst_light(["green", "green"]) == "green"


def test_worst_light_ignores_unknown_when_real_lights_exist():
    assert pr._worst_light(["unknown", "green"]) == "green"
    assert pr._worst_light(["unknown", "red"]) == "red"


def test_worst_light_all_unknown_returns_unknown():
    assert pr._worst_light(["unknown", "unknown"]) == "unknown"
    assert pr._worst_light([]) == "unknown"


def test_worst_light_tolerates_missing_or_weird_values():
    assert pr._worst_light([None, "", "green"]) == "green"


# ---------------------------------------------------------------------------
# aggregate_totals
# ---------------------------------------------------------------------------

def test_aggregate_totals_sums_revenue_and_wages_across_venues():
    recaps = [
        _mk_recap("a", rev_actual=10_000, rev_forecast=10_000, wages_actual=2_800, wages_forecast=2_800),
        _mk_recap("b", rev_actual=15_000, rev_forecast=14_000, wages_actual=4_500, wages_forecast=3_920),
        _mk_recap("c", rev_actual=5_000,  rev_forecast=6_000,  wages_actual=1_800, wages_forecast=1_680),
    ]
    totals = pr.aggregate_totals(recaps)
    assert totals["venue_count"] == 3
    assert totals["revenue"]["actual"] == 30_000
    assert totals["revenue"]["forecast"] == 30_000
    assert totals["revenue"]["delta"] == 0
    assert totals["wages"]["actual"] == 9_100
    # group wage % = 9100 / 30000 = 30.33%
    assert abs(totals["wages"]["pct_of_revenue_actual"] - 0.3033) < 0.001


def test_aggregate_totals_handles_zero_forecast_gracefully():
    recaps = [_mk_recap("a", rev_actual=0, rev_forecast=0, wages_actual=0, wages_forecast=0)]
    totals = pr.aggregate_totals(recaps)
    assert totals["revenue"]["delta_pct"] == 0.0
    assert totals["wages"]["pct_of_revenue_actual"] == 0.0


def test_aggregate_totals_weighted_target_prioritises_bigger_venues():
    # Venue A: $40k forecast @ 28% target
    # Venue B: $10k forecast @ 25% target
    # Weighted target = (40000*0.28 + 10000*0.25) / 50000 = 0.274
    recaps = [
        _mk_recap("a", rev_actual=40_000, rev_forecast=40_000,
                  wages_actual=11_200, wages_forecast=11_200, wage_target=0.28),
        _mk_recap("b", rev_actual=10_000, rev_forecast=10_000,
                  wages_actual=2_500, wages_forecast=2_500, wage_target=0.25),
    ]
    totals = pr.aggregate_totals(recaps)
    assert abs(totals["wages"]["pct_of_revenue_target"] - 0.274) < 0.001


def test_aggregate_totals_peak_headcount_is_max_not_sum():
    recaps = [
        _mk_recap("a", headcount=[{"timestamp": "2026-04-11T19:00:00Z", "count_after": 30, "source": "group"}]),
        _mk_recap("b", headcount=[{"timestamp": "2026-04-11T19:00:00Z", "count_after": 55, "source": "group"}]),
        _mk_recap("c", headcount=[{"timestamp": "2026-04-11T19:00:00Z", "count_after": 42, "source": "group"}]),
    ]
    totals = pr.aggregate_totals(recaps)
    # Peak across the portfolio is the single highest peak (the busiest venue),
    # NOT the sum of all peaks — 3 venues don't magically make a 127-person shift.
    assert totals["headcount"]["peak_across_portfolio"] == 55


# ---------------------------------------------------------------------------
# aggregate_accountability
# ---------------------------------------------------------------------------

def test_aggregate_accountability_sums_counts_and_dollars():
    recs_a = [
        {"id": "a1", "status": "accepted", "impact_estimate_aud": 100, "text": "cut bar"},
        {"id": "a2", "status": "dismissed", "impact_estimate_aud": 300, "text": "send 1"},
    ]
    recs_b = [
        {"id": "b1", "status": "dismissed", "impact_estimate_aud": 200, "text": "send 2"},
        {"id": "b2", "status": "pending",   "impact_estimate_aud": 50,  "text": "pending"},
    ]
    recaps = [
        _mk_recap("a", recommendations=recs_a),
        _mk_recap("b", recommendations=recs_b),
    ]
    out = pr.aggregate_accountability(recaps)
    assert out["total"] == 4
    assert out["accepted"] == 1
    assert out["dismissed"] == 2
    assert out["pending"] == 1
    assert out["estimated_impact_missed_aud"] == 500.0
    assert out["estimated_impact_pending_aud"] == 50.0
    assert abs(out["acceptance_rate"] - 0.3333) < 0.001


def test_aggregate_accountability_top_missed_tags_venue_id_and_sorts_by_impact():
    recs_a = [{"id": "a1", "status": "dismissed", "impact_estimate_aud": 300, "text": "A rec"}]
    recs_b = [{"id": "b1", "status": "dismissed", "impact_estimate_aud": 500, "text": "B rec"}]
    recs_c = [{"id": "c1", "status": "dismissed", "impact_estimate_aud": 100, "text": "C rec"}]
    recaps = [
        _mk_recap("a", recommendations=recs_a),
        _mk_recap("b", recommendations=recs_b),
        _mk_recap("c", recommendations=recs_c),
    ]
    out = pr.aggregate_accountability(recaps)
    assert out["top_missed"][0]["venue_id"] == "b"
    assert out["top_missed"][0]["impact_estimate_aud"] == 500
    assert out["top_missed"][1]["venue_id"] == "a"
    assert out["top_missed"][2]["venue_id"] == "c"


def test_aggregate_accountability_empty_returns_zero_block():
    recaps = [_mk_recap("a")]
    out = pr.aggregate_accountability(recaps)
    assert out["total"] == 0
    assert out["estimated_impact_missed_aud"] == 0.0
    assert out["acceptance_rate"] == 0.0


# ---------------------------------------------------------------------------
# compose_portfolio
# ---------------------------------------------------------------------------

def test_compose_portfolio_empty_returns_sane_zero_state():
    out = pr.compose_portfolio([])
    assert out["venues"] == []
    assert out["traffic_light"] == "unknown"
    assert out["totals"]["venue_count"] == 0
    assert "No venues" in out["summary"]


def test_compose_portfolio_summary_counts_lights_correctly():
    # One red (big wage blow-out), one amber (mild rev miss),
    # one green (everything fine). Thresholds per shift_recap._classify:
    #   red_rev_miss = 5%, amber_rev_miss = 2%
    #   red_wage_overshoot = 2pt, amber_wage_overshoot = 0.5pt
    recaps = [
        _mk_recap("a",
                  rev_actual=20_000, rev_forecast=20_000,
                  wages_actual=7_000, wages_forecast=5_600),   # +7pt wage -> red
        _mk_recap("b",
                  rev_actual=19_400, rev_forecast=20_000,      # -3% rev -> amber
                  wages_actual=5_600, wages_forecast=5_600),   # wage neutral
        _mk_recap("c",
                  rev_actual=20_500, rev_forecast=20_000,
                  wages_actual=5_500, wages_forecast=5_600),   # healthy
    ]
    out = pr.compose_portfolio(recaps, portfolio_id="dale_group")
    assert "1 red" in out["summary"]
    assert "1 amber" in out["summary"]
    assert "1 green" in out["summary"]
    assert out["traffic_light"] == "red"
    assert out["portfolio_id"] == "dale_group"


def test_compose_portfolio_sorts_venues_red_first():
    recaps = [
        _mk_recap("a"),  # green
        _mk_recap("b", wages_actual=7_000),  # red
        _mk_recap("c", rev_actual=18_500, rev_forecast=20_000),  # amber (rev miss)
    ]
    out = pr.compose_portfolio(recaps)
    venue_ids_in_order = [v["venue_id"] for v in out["venues"]]
    assert venue_ids_in_order[0] == "b"   # red
    assert venue_ids_in_order[1] == "c"   # amber
    assert venue_ids_in_order[2] == "a"   # green


def test_compose_portfolio_honours_venue_labels():
    recaps = [_mk_recap("v_mojos"), _mk_recap("v_earls")]
    out = pr.compose_portfolio(
        recaps,
        venue_labels={"v_mojos": "Mojo's Bar", "v_earls": "Earl's Kitchen"},
    )
    labels = {v["venue_id"]: v["label"] for v in out["venues"]}
    assert labels["v_mojos"] == "Mojo's Bar"
    assert labels["v_earls"] == "Earl's Kitchen"


def test_compose_portfolio_shift_date_falls_back_to_first_recap():
    recaps = [_mk_recap("a")]
    out = pr.compose_portfolio(recaps)
    # shift_recap always sets shift_date on its output
    assert out["shift_date"] == "2026-04-11"


def test_compose_portfolio_shift_date_explicit_override():
    recaps = [_mk_recap("a")]
    out = pr.compose_portfolio(recaps, shift_date="2026-12-31")
    assert out["shift_date"] == "2026-12-31"


def test_compose_portfolio_dismissed_dollars_flow_into_summary_tail():
    recs = [
        {"id": "r1", "status": "dismissed", "impact_estimate_aud": 400, "text": "cut bar"},
        {"id": "r2", "status": "dismissed", "impact_estimate_aud": 200, "text": "send 1"},
    ]
    recaps = [_mk_recap("a", recommendations=recs)]
    out = pr.compose_portfolio(recaps)
    assert "2 recs dismissed across the group" in out["summary"]
    assert "at stake" in out["summary"]


def test_compose_portfolio_killer_line_3_venues():
    # The killer sales moment: group owner opens the portfolio and sees
    # something like: "3 venues: 1 red, 1 amber, 1 green. Portfolio
    # revenue $X vs $Y. Group wage % Z. N recs dismissed ($M at stake)."
    recs_a = [
        {"id": "ra1", "status": "dismissed", "impact_estimate_aud": 500, "text": "cut bar"},
    ]
    recs_c = [
        {"id": "rc1", "status": "dismissed", "impact_estimate_aud": 300, "text": "send 1"},
        {"id": "rc2", "status": "accepted",  "impact_estimate_aud": 200, "text": "good call"},
    ]
    recaps = [
        _mk_recap("mojos",   rev_actual=14_800, rev_forecast=15_500, wages_actual=4_700, wages_forecast=4_340, recommendations=recs_a),
        _mk_recap("earls",   rev_actual=18_500, rev_forecast=20_000, wages_actual=5_600, wages_forecast=5_600),
        _mk_recap("francine", rev_actual=12_000, rev_forecast=12_000, wages_actual=3_360, wages_forecast=3_360, recommendations=recs_c),
    ]
    out = pr.compose_portfolio(recaps, portfolio_id="dale_group",
                                venue_labels={"mojos": "Mojo's", "earls": "Earl's", "francine": "Francine's"})
    print("PORTFOLIO LIGHT:", out["traffic_light"])
    print("PORTFOLIO SUMMARY:", out["summary"])
    # Assertions: worst-of is red, 3 venues reported
    assert out["traffic_light"] == "red"
    assert out["totals"]["venue_count"] == 3
    assert "$45" in out["summary"] or "45.3" in out["summary"] or "$45.3" in out["summary"]
    assert "2 recs dismissed" in out["summary"]
    # Top missed should have venue_id tagged
    assert all("venue_id" in m for m in out["accountability"]["top_missed"])


# ---------------------------------------------------------------------------
# Moment 14d — trends overlay on mini cards
# ---------------------------------------------------------------------------

class _FakeTrends:
    """Stub rosteriq.trends that returns a scripted trend per venue."""

    def __init__(self, by_venue, raise_on=None):
        self.by_venue = dict(by_venue or {})
        self.raise_on = set(raise_on or [])
        self.calls = []

    def compose_trend_from_store(self, venue_id, *, window_days=7, store=None):
        self.calls.append((venue_id, window_days, store))
        if venue_id in self.raise_on:
            raise RuntimeError("boom")
        return self.by_venue.get(venue_id, {
            "venue_id": venue_id,
            "window_days": window_days,
            "traffic_light": "green",
            "headline": "Flat",
            "daily": [],
            "series": {
                "acceptance_rate": [0.0] * window_days,
                "missed_aud": [0.0] * window_days,
                "total_events": [0] * window_days,
            },
            "slopes": {
                "acceptance_rate": {"first_half": 0, "second_half": 0, "delta": 0.0},
                "missed_aud":      {"first_half": 0, "second_half": 0, "delta": 0.0},
                "total_events":    {"first_half": 0, "second_half": 0, "delta": 0.0},
            },
            "totals": {"events": 0, "accepted": 0, "dismissed": 0, "missed_aud": 0.0, "acceptance_rate": 0.0},
        })


def _trend_fixture(venue_id, *, light="amber", series_accept=None, series_missed=None, accept_delta=0.0, missed_delta=0.0):
    series_accept = series_accept if series_accept is not None else [0.5, 0.6, 0.7, 0.55, 0.6, 0.65, 0.7]
    series_missed = series_missed if series_missed is not None else [50, 40, 60, 80, 30, 20, 10]
    return {
        "venue_id": venue_id,
        "window_days": len(series_accept),
        "traffic_light": light,
        "headline": f"{venue_id} trend: {light}",
        "daily": [{"date": f"2026-04-{i+1:02d}"} for i in range(len(series_accept))],
        "series": {
            "acceptance_rate": list(series_accept),
            "missed_aud": list(series_missed),
            "total_events": [1] * len(series_accept),
        },
        "slopes": {
            "acceptance_rate": {"first_half": 0.5, "second_half": 0.5 + accept_delta, "delta": accept_delta},
            "missed_aud":      {"first_half": 50, "second_half": 50 + missed_delta, "delta": missed_delta},
            "total_events":    {"first_half": 1, "second_half": 1, "delta": 0.0},
        },
        "totals": {
            "events": len(series_accept),
            "accepted": 4,
            "dismissed": 3,
            "missed_aud": sum(series_missed),
            "acceptance_rate": 0.57,
        },
    }


def test_compact_trend_drops_daily_and_keeps_series():
    tr = _trend_fixture("v1")
    compact = pr._compact_trend(tr)
    assert "daily" not in compact
    assert compact["traffic_light"] == "amber"
    assert compact["headline"].startswith("v1 trend")
    assert compact["series"]["acceptance_rate"] == tr["series"]["acceptance_rate"]
    assert compact["series"]["missed_aud"] == tr["series"]["missed_aud"]
    assert compact["slopes"]["acceptance_rate_delta"] == 0.0
    assert compact["slopes"]["missed_aud_delta"] == 0.0
    assert compact["totals"]["events"] == 7
    assert compact["window_days"] == 7


def test_compact_trend_handles_missing_fields_gracefully():
    compact = pr._compact_trend({})
    assert compact["traffic_light"] == "unknown"
    assert compact["headline"] == ""
    assert compact["series"]["acceptance_rate"] == []
    assert compact["series"]["missed_aud"] == []
    assert compact["slopes"]["acceptance_rate_delta"] == 0.0
    assert compact["totals"]["events"] == 0


def test_compact_trend_coerces_non_numeric_series_values_to_zero():
    tr = {
        "series": {
            "acceptance_rate": [0.5, "nope", None, 0.8],
            "missed_aud": [10, "bad", 20, None],
        },
        "slopes": {
            "acceptance_rate": {"delta": "x"},
            "missed_aud": {"delta": None},
        },
    }
    compact = pr._compact_trend(tr)
    assert compact["series"]["acceptance_rate"] == [0.5, 0.0, 0.0, 0.8]
    assert compact["series"]["missed_aud"] == [10.0, 0.0, 20.0, 0.0]
    assert compact["slopes"]["acceptance_rate_delta"] == 0.0
    assert compact["slopes"]["missed_aud_delta"] == 0.0


def test_compose_portfolio_without_include_trends_skips_trend_field():
    recaps = [_mk_recap("a"), _mk_recap("b")]
    fake = _FakeTrends({})
    out = pr.compose_portfolio(recaps, trends_module=fake)
    assert fake.calls == []  # no calls when include_trends=False
    for v in out["venues"]:
        assert "trend" not in v


def test_compose_portfolio_with_include_trends_attaches_trend_per_venue():
    recaps = [_mk_recap("a"), _mk_recap("b")]
    fake = _FakeTrends({
        "a": _trend_fixture("a", light="red",   accept_delta=-0.15, missed_delta=250.0),
        "b": _trend_fixture("b", light="green", accept_delta=0.05,  missed_delta=-100.0),
    })
    out = pr.compose_portfolio(recaps, include_trends=True, trends_module=fake)
    # Trends should have been fetched for each venue with the default window
    assert [c[0] for c in fake.calls] == ["a", "b"]
    assert all(c[1] == 7 for c in fake.calls)
    # Each mini-card has a compact trend payload
    by_id = {v["venue_id"]: v for v in out["venues"]}
    assert by_id["a"]["trend"]["traffic_light"] == "red"
    assert by_id["a"]["trend"]["slopes"]["acceptance_rate_delta"] == -0.15
    assert by_id["a"]["trend"]["slopes"]["missed_aud_delta"] == 250.0
    assert by_id["b"]["trend"]["traffic_light"] == "green"
    assert by_id["b"]["trend"]["headline"].startswith("b trend")
    # daily should NOT be on the compact form
    assert "daily" not in by_id["a"]["trend"]


def test_compose_portfolio_trend_window_days_flows_to_trends():
    recaps = [_mk_recap("a")]
    fake = _FakeTrends({})
    pr.compose_portfolio(recaps, include_trends=True, trend_window_days=28, trends_module=fake)
    assert fake.calls[0][1] == 28


def test_compose_portfolio_trend_fetch_error_is_isolated_per_venue():
    recaps = [_mk_recap("a"), _mk_recap("b")]
    fake = _FakeTrends(
        {"b": _trend_fixture("b", light="amber")},
        raise_on={"a"},
    )
    out = pr.compose_portfolio(recaps, include_trends=True, trends_module=fake)
    by_id = {v["venue_id"]: v for v in out["venues"]}
    # Venue "a" had a trend error — mini rendered without the trend key
    assert "trend" not in by_id["a"]
    # Venue "b" still gets a trend
    assert by_id["b"]["trend"]["traffic_light"] == "amber"


def test_compose_portfolio_include_trends_on_empty_portfolio_is_noop():
    fake = _FakeTrends({})
    out = pr.compose_portfolio([], include_trends=True, trends_module=fake)
    assert fake.calls == []
    assert out["venues"] == []


def test_compose_portfolio_include_trends_uses_injected_store():
    recaps = [_mk_recap("a")]
    fake = _FakeTrends({})
    sentinel = object()
    pr.compose_portfolio(recaps, include_trends=True, trends_module=fake, trend_store=sentinel)
    assert fake.calls[0][2] is sentinel


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
