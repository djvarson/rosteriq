"""Tests for the pure-stdlib head-count store (rosteriq.headcount_store)."""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from rosteriq import headcount_store as hc  # noqa: E402


def _reset():
    hc.clear()


def test_fresh_venue_seeds_with_start_of_shift_entry():
    _reset()
    state = hc.state("venue_x")
    assert state["current"] == 0
    assert state["venue_id"] == "venue_x"
    assert len(state["recent"]) == 1
    seed = state["recent"][0]
    assert seed["note"] == "Start of shift"
    assert seed["source"] == "reset"
    assert seed["delta"] == 0
    assert seed["count_after"] == 0


def test_apply_positive_delta_updates_count_and_appends_entry():
    _reset()
    hc.apply_delta("v1", delta=3, note=None, source="button")
    hc.apply_delta("v1", delta=5, note="bus group", source="group")
    state = hc.state("v1")
    assert state["current"] == 8
    # Recent is newest first: group, tap, seed
    assert state["recent"][0]["delta"] == 5
    assert state["recent"][0]["note"] == "bus group"
    assert state["recent"][0]["count_after"] == 8
    assert state["recent"][1]["delta"] == 3
    assert state["recent"][1]["count_after"] == 3
    assert state["recent"][2]["note"] == "Start of shift"


def test_negative_delta_is_clamped_at_zero():
    _reset()
    entry = hc.apply_delta("v2", delta=-5, note=None, source="button")
    # Clamped: went from 0 to 0
    assert entry["delta"] == 0
    assert entry["count_after"] == 0
    state = hc.state("v2")
    assert state["current"] == 0


def test_mixed_sequence_matches_expected_running_count():
    _reset()
    ops = [(+10, "bus group"), (+5, None), (-3, None), (+1, None), (-20, "function ends")]
    for d, note in ops:
        hc.apply_delta("v3", delta=d, note=note, source="group" if note else "button")
    # Running: 0 -> 10 -> 15 -> 12 -> 13 -> 0 (clamped from -7)
    state = hc.state("v3")
    assert state["current"] == 0
    first = state["recent"][0]
    assert first["note"] == "function ends"
    assert first["count_after"] == 0
    assert first["delta"] == -13  # actual, not requested


def test_reset_appends_entry_and_preserves_history():
    _reset()
    hc.apply_delta("v4", delta=20, note=None, source="button")
    hc.apply_delta("v4", delta=10, note=None, source="button")
    hc.reset("v4", count=0, note="Manual reset")
    state = hc.state("v4")
    assert state["current"] == 0
    history = hc.store()["v4"]
    assert len(history) == 4  # seed + 2 taps + reset
    assert history[-1]["source"] == "reset"
    assert history[-1]["note"] == "Manual reset"
    assert history[-1]["delta"] == -30


def test_reset_to_positive_absolute_value():
    _reset()
    hc.reset("v5", count=42, note=None)
    state = hc.state("v5")
    assert state["current"] == 42
    latest = state["recent"][0]
    assert latest["count_after"] == 42
    assert latest["source"] == "reset"
    assert latest["note"] == "Reset"  # default


def test_reset_to_negative_is_clamped_to_zero():
    _reset()
    hc.reset("v5b", count=-100, note=None)
    assert hc.state("v5b")["current"] == 0


def test_multiple_venues_are_isolated():
    _reset()
    hc.apply_delta("pub_A", delta=15, note=None, source="button")
    hc.apply_delta("pub_B", delta=3, note=None, source="button")
    assert hc.state("pub_A")["current"] == 15
    assert hc.state("pub_B")["current"] == 3
    assert hc.state("pub_C")["current"] == 0  # fresh venue


def test_history_is_bounded_at_max():
    _reset()
    over = hc.MAX_HISTORY + 50
    for _ in range(over):
        hc.apply_delta("v_big", delta=1, note=None, source="button")
    history = hc.store()["v_big"]
    assert len(history) == hc.MAX_HISTORY
    # Count tracks every applied delta regardless of truncation
    assert history[-1]["count_after"] == over


def test_recent_limit_respected_but_history_intact():
    _reset()
    for i in range(20):
        hc.apply_delta("v_r", delta=1, note=f"tap {i}", source="button")
    state = hc.state("v_r", recent_limit=5)
    assert len(state["recent"]) == 5
    # Full history remains on the store (seed + 20 taps)
    assert len(hc.store()["v_r"]) == 21


def test_state_venue_id_round_trips():
    _reset()
    state = hc.state("venue with spaces")
    assert state["venue_id"] == "venue with spaces"


def test_note_preserved_verbatim_including_unicode():
    _reset()
    hc.apply_delta("v_uni", delta=4, note="café regulars 🍺", source="group")
    state = hc.state("v_uni")
    assert state["recent"][0]["note"] == "café regulars 🍺"


def test_timestamps_are_iso_format():
    _reset()
    hc.apply_delta("v_ts", delta=1, note=None, source="button")
    state = hc.state("v_ts")
    from datetime import datetime
    # Should parse cleanly as ISO
    datetime.fromisoformat(state["updated_at"])
    for e in state["recent"]:
        datetime.fromisoformat(e["timestamp"])


def test_history_is_monotonic_in_timestamps():
    _reset()
    for _ in range(10):
        hc.apply_delta("v_mono", delta=1, note=None, source="button")
    history = hc.store()["v_mono"]
    timestamps = [e["timestamp"] for e in history]
    assert timestamps == sorted(timestamps)


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
