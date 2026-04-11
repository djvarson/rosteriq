"""Tests for rosteriq.morning_brief — pure stdlib, no pytest."""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from rosteriq import morning_brief as mb  # noqa: E402


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _ev(
    rid,
    *,
    status="pending",
    impact=0,
    text="rec",
    recorded="2026-04-10T19:00:00Z",
    responded="2026-04-10T19:15:00Z",
    source="wage_pulse",
    priority="med",
):
    return {
        "id": rid,
        "status": status,
        "impact_estimate_aud": impact,
        "text": text,
        "recorded_at": recorded,
        "responded_at": responded if status != "pending" else None,
        "source": source,
        "priority": priority,
    }


def _recap(
    *,
    light="amber",
    rev_actual=18_000,
    rev_forecast=20_000,
    wage_pct=0.30,
    wage_target=0.28,
    peak=50,
):
    return {
        "traffic_light": light,
        "revenue": {
            "actual": rev_actual,
            "forecast": rev_forecast,
            "delta_pct": (rev_actual - rev_forecast) / rev_forecast if rev_forecast else 0,
        },
        "wages": {
            "pct_of_revenue_actual": wage_pct,
            "pct_of_revenue_target": wage_target,
            "pct_delta": wage_pct - wage_target,
        },
        "headcount": {"peak": peak},
    }


# ---------------------------------------------------------------------------
# Date filtering
# ---------------------------------------------------------------------------

def test_events_for_date_filters_by_responded_first_then_recorded():
    evs = [
        _ev("a", recorded="2026-04-10T09:00:00Z", responded="2026-04-10T09:05:00Z", status="accepted"),
        _ev("b", recorded="2026-04-10T23:50:00Z", responded="2026-04-11T00:10:00Z", status="dismissed"),
        _ev("c", recorded="2026-04-09T19:00:00Z", responded="2026-04-09T19:10:00Z", status="dismissed"),
    ]
    out = mb._events_for_date(evs, "2026-04-10")
    assert len(out) == 1
    assert out[0]["id"] == "a"

    out11 = mb._events_for_date(evs, "2026-04-11")
    assert len(out11) == 1
    assert out11[0]["id"] == "b"


def test_events_for_date_tolerates_missing_timestamps():
    evs = [
        {"id": "x", "status": "dismissed"},  # no timestamps at all
        _ev("y", recorded="2026-04-10T19:00:00Z", responded=None, status="pending"),
    ]
    out = mb._events_for_date(evs, "2026-04-10")
    assert [e["id"] for e in out] == ["y"]


# ---------------------------------------------------------------------------
# Roll-up math
# ---------------------------------------------------------------------------

def test_roll_up_events_counts_and_sums():
    evs = [
        _ev("a", status="dismissed", impact=300),
        _ev("b", status="dismissed", impact=200),
        _ev("c", status="accepted",  impact=400),
        _ev("d", status="pending",   impact=50),
    ]
    out = mb._roll_up_events(evs)
    assert out["total"] == 4
    assert out["dismissed"] == 2
    assert out["accepted"] == 1
    assert out["pending"] == 1
    assert out["missed_aud"] == 500.0
    assert out["accepted_aud"] == 400.0
    # 1 accepted / (1 accepted + 2 dismissed) = 1/3
    assert abs(out["acceptance_rate"] - 0.3333) < 0.001


def test_roll_up_events_empty_returns_zero_block():
    out = mb._roll_up_events([])
    assert out["total"] == 0
    assert out["missed_aud"] == 0.0
    assert out["acceptance_rate"] == 0.0


def test_roll_up_events_tolerates_garbage_impact_values():
    evs = [
        _ev("a", status="dismissed", impact="not a number"),
        _ev("b", status="dismissed", impact=None),
        _ev("c", status="dismissed", impact=150),
    ]
    out = mb._roll_up_events(evs)
    assert out["dismissed"] == 3
    assert out["missed_aud"] == 150.0


# ---------------------------------------------------------------------------
# Top dismissed ranking
# ---------------------------------------------------------------------------

def test_top_dismissed_sorts_by_impact_desc_and_limits_to_three():
    evs = [
        _ev("a", status="dismissed", impact=100, text="small"),
        _ev("b", status="dismissed", impact=500, text="huge"),
        _ev("c", status="dismissed", impact=300, text="mid"),
        _ev("d", status="dismissed", impact=200, text="med"),
        _ev("e", status="accepted",  impact=999, text="not dismissed"),
    ]
    top = mb._top_dismissed(evs)
    assert len(top) == 3
    assert [t["text"] for t in top] == ["huge", "mid", "med"]


def test_top_dismissed_ignores_non_dismissed():
    evs = [
        _ev("a", status="pending",   impact=900, text="still pending"),
        _ev("b", status="accepted",  impact=500, text="won"),
        _ev("c", status="dismissed", impact=100, text="only dismissed"),
    ]
    top = mb._top_dismissed(evs)
    assert len(top) == 1
    assert top[0]["text"] == "only dismissed"


def test_top_dismissed_newest_first_on_impact_tie():
    evs = [
        _ev("old", status="dismissed", impact=200,
            responded="2026-04-10T12:00:00Z"),
        _ev("new", status="dismissed", impact=200,
            responded="2026-04-10T18:00:00Z"),
    ]
    top = mb._top_dismissed(evs)
    assert top[0]["id"] == "new"


# ---------------------------------------------------------------------------
# Headline
# ---------------------------------------------------------------------------

def test_headline_leads_with_dollars_when_missed_exists():
    rollup = {"dismissed": 2, "accepted": 1, "missed_aud": 800.0}
    line = mb._headline(rollup=rollup, yesterday_recap=_recap(light="red"))
    assert "$800" in line
    assert "2" in line
    assert "dismissed" in line.lower()


def test_headline_falls_back_to_counts_when_dollars_missing():
    rollup = {"dismissed": 3, "accepted": 1, "missed_aud": 0.0}
    line = mb._headline(rollup=rollup, yesterday_recap=None)
    assert "3" in line
    assert "dismissed" in line.lower()
    assert "$" not in line  # No dollars to show


def test_headline_clean_day_message_when_no_dismissals():
    rollup = {"dismissed": 0, "accepted": 4, "missed_aud": 0.0}
    line = mb._headline(rollup=rollup, yesterday_recap=_recap(light="green"))
    assert "Clean" in line or "clean" in line
    assert "4" in line


def test_headline_empty_day_uses_light_hint():
    rollup = {"dismissed": 0, "accepted": 0, "missed_aud": 0.0}
    red_line = mb._headline(rollup=rollup, yesterday_recap=_recap(light="red"))
    assert "red" in red_line.lower()
    amber_line = mb._headline(rollup=rollup, yesterday_recap=_recap(light="amber"))
    assert "amber" in amber_line.lower()


# ---------------------------------------------------------------------------
# "One thing" nudge
# ---------------------------------------------------------------------------

def test_one_thing_quotes_biggest_miss_when_dollars_exist():
    rollup = {"dismissed": 2, "accepted": 0, "missed_aud": 700.0}
    top = [
        {"text": "Cut 2 bar staff now — wage % trending +5pt.",
         "impact_estimate_aud": 500.0},
        {"text": "Send 1 home after peak.", "impact_estimate_aud": 200.0},
    ]
    line = mb._pick_one_thing(
        rollup=rollup, top=top, yesterday_recap=_recap()
    )
    assert "500" in line
    assert "Cut 2 bar staff now" in line
    assert "Don't repeat" in line or "don't repeat" in line.lower()


def test_one_thing_generic_nudge_when_dismissed_but_no_impact():
    rollup = {"dismissed": 1, "accepted": 0, "missed_aud": 0.0}
    top = [{"text": "", "impact_estimate_aud": 0.0}]
    line = mb._pick_one_thing(
        rollup=rollup, top=top, yesterday_recap=None
    )
    assert "1" in line
    assert "rec" in line.lower()


def test_one_thing_positive_message_when_clean_day_with_action():
    rollup = {"dismissed": 0, "accepted": 3, "missed_aud": 0.0}
    line = mb._pick_one_thing(
        rollup=rollup, top=[], yesterday_recap=_recap(light="green")
    )
    assert "Clean" in line or "clean" in line
    assert "3" in line


def test_one_thing_zero_state_when_nothing_fired():
    rollup = {"dismissed": 0, "accepted": 0, "missed_aud": 0.0}
    line = mb._pick_one_thing(
        rollup=rollup, top=[], yesterday_recap=_recap(light="unknown")
    )
    assert "clean" in line.lower() or "pulse" in line.lower()


# ---------------------------------------------------------------------------
# compose_brief
# ---------------------------------------------------------------------------

def test_compose_brief_happy_path_includes_dollars_and_nudge():
    evs = [
        _ev("a", status="dismissed", impact=500,
            text="Cut 2 bar staff now — wage % trending +5pt",
            responded="2026-04-10T19:30:00Z"),
        _ev("b", status="dismissed", impact=200,
            text="Send 1 home after peak",
            responded="2026-04-10T20:15:00Z"),
        _ev("c", status="accepted", impact=100,
            text="Stack FOH at 7pm",
            responded="2026-04-10T18:45:00Z"),
    ]
    brief = mb.compose_brief(
        "venue_001",
        target_date="2026-04-10",
        events=evs,
        yesterday_recap=_recap(light="red"),
        venue_label="Mojo's Bar",
    )
    assert brief["venue_id"] == "venue_001"
    assert brief["venue_label"] == "Mojo's Bar"
    assert brief["date"] == "2026-04-10"
    assert brief["traffic_light"] == "red"
    assert brief["rollup"]["dismissed"] == 2
    assert brief["rollup"]["accepted"] == 1
    assert brief["rollup"]["missed_aud"] == 700.0
    assert "700" in brief["headline"]
    assert len(brief["top_dismissed"]) == 2
    assert brief["top_dismissed"][0]["text"].startswith("Cut 2")
    assert "500" in brief["one_thing"]
    assert brief["recap_context"]["wage_pct_actual"] == 0.30
    assert "Revenue" in brief["summary"]


def test_compose_brief_ignores_events_from_other_days():
    evs = [
        _ev("wrong_day", status="dismissed", impact=999,
            recorded="2026-04-09T19:00:00Z", responded="2026-04-09T19:15:00Z"),
        _ev("right_day", status="dismissed", impact=100,
            recorded="2026-04-10T19:00:00Z", responded="2026-04-10T19:15:00Z"),
    ]
    brief = mb.compose_brief(
        "v",
        target_date="2026-04-10",
        events=evs,
        yesterday_recap=None,
    )
    assert brief["rollup"]["dismissed"] == 1
    assert brief["rollup"]["missed_aud"] == 100.0
    assert len(brief["top_dismissed"]) == 1
    assert brief["top_dismissed"][0]["id"] == "right_day"


def test_compose_brief_empty_events_produces_sane_zero_brief():
    brief = mb.compose_brief(
        "v", target_date="2026-04-10", events=[], yesterday_recap=None
    )
    assert brief["rollup"]["total"] == 0
    assert brief["top_dismissed"] == []
    assert brief["headline"]  # non-empty
    assert brief["one_thing"]  # non-empty nudge still fires


def test_compose_brief_without_recap_still_works():
    evs = [_ev("a", status="dismissed", impact=300)]
    brief = mb.compose_brief(
        "v", target_date="2026-04-10", events=evs
    )
    assert brief["recap_context"] == {}
    assert brief["traffic_light"] == "unknown"
    assert "300" in brief["headline"]


def test_compose_brief_default_target_date_is_yesterday():
    brief = mb.compose_brief("v", events=[])
    # shape check — exact date depends on wall clock
    assert len(brief["date"]) == 10
    assert brief["date"][4] == "-"
    assert brief["date"][7] == "-"


# ---------------------------------------------------------------------------
# render_text
# ---------------------------------------------------------------------------

def test_render_text_includes_headline_and_top_dismissed():
    brief = mb.compose_brief(
        "venue_001",
        target_date="2026-04-10",
        events=[
            _ev("a", status="dismissed", impact=500,
                text="Cut 2 bar staff now",
                responded="2026-04-10T19:00:00Z"),
        ],
        yesterday_recap=_recap(light="red"),
        venue_label="Mojo's Bar",
    )
    text = mb.render_text(brief)
    assert "Mojo's Bar" in text
    assert "2026-04-10" in text
    assert "Cut 2 bar staff now" in text
    assert "$500" in text
    assert "Do differently today" in text


def test_render_text_omits_top_section_when_no_dismissals():
    brief = mb.compose_brief(
        "v", target_date="2026-04-10", events=[], yesterday_recap=_recap(light="green")
    )
    text = mb.render_text(brief)
    assert "Top dismissed recs" not in text
    assert "Do differently today" in text  # nudge still fires


# ---------------------------------------------------------------------------
# compose_brief_from_store
# ---------------------------------------------------------------------------

def test_compose_brief_from_store_uses_injected_stub():
    events = [
        _ev("a", status="dismissed", impact=400,
            responded="2026-04-10T19:00:00Z"),
    ]

    class StubStore:
        def history(self, venue_id):
            assert venue_id == "venue_001"
            return events

    brief = mb.compose_brief_from_store(
        "venue_001",
        target_date="2026-04-10",
        yesterday_recap=_recap(),
        store=StubStore(),
    )
    assert brief["rollup"]["missed_aud"] == 400.0


def test_compose_brief_from_store_default_uses_real_store():
    # Round-trip against the real accountability_store. This is the
    # most important integration test — it proves the ledger → brief
    # flow works with no mocks.
    from rosteriq import accountability_store as store
    store.clear()
    ev = store.record(
        "venue_brief_test",
        text="Cut 1 bar staff — over wage target",
        source="wage_pulse",
        priority="high",
        impact_estimate_aud=350.0,
        rec_id="rec_brief_roundtrip_1",
    )
    store.respond("venue_brief_test", ev["id"], status="dismissed")
    # Because responded_at is 'now', the brief needs target_date=today
    from datetime import datetime, timezone
    today = datetime.now(timezone.utc).date().isoformat()
    brief = mb.compose_brief_from_store(
        "venue_brief_test", target_date=today
    )
    store.clear()
    assert brief["rollup"]["dismissed"] == 1
    assert brief["rollup"]["missed_aud"] == 350.0
    assert "350" in brief["headline"]


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
