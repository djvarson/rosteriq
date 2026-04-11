"""Tests for rosteriq.accountability_store — pure-stdlib, no pytest.

Runs with `python tests/test_accountability.py`; every `test_` function
is executed and pass/fail is reported.
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from rosteriq import accountability_store as acct  # noqa: E402


def _reset():
    acct.clear()


# ---------------------------------------------------------------------------
# history / record
# ---------------------------------------------------------------------------

def test_history_starts_empty_for_unknown_venue():
    _reset()
    hist = acct.history("new_venue")
    assert hist == []
    # Accessing creates the bucket; the list is mutable and retained.
    hist.append({"fake": "event"})
    assert len(acct.history("new_venue")) == 1


def test_record_appends_a_pending_event_with_uuid_id():
    _reset()
    ev = acct.record("mojos", text="Cut 2 bartenders", source="wage_pulse",
                     impact_estimate_aud=420.0, priority="high")
    assert ev["id"].startswith("rec_")
    assert ev["venue_id"] == "mojos"
    assert ev["status"] == "pending"
    assert ev["text"] == "Cut 2 bartenders"
    assert ev["source"] == "wage_pulse"
    assert ev["impact_estimate_aud"] == 420.0
    assert ev["priority"] == "high"
    assert ev["responded_at"] is None
    assert ev["response_note"] is None
    assert ev["recorded_at"].endswith("Z")


def test_record_with_rec_id_is_idempotent():
    _reset()
    ev1 = acct.record("earls", text="Send 1 home", rec_id="rec_fixed_1",
                      impact_estimate_aud=120.0, priority="med")
    assert len(acct.history("earls")) == 1
    # Re-recording the same rec_id returns the existing event unchanged
    ev2 = acct.record("earls", text="Totally different text", rec_id="rec_fixed_1",
                      impact_estimate_aud=999.0, priority="low")
    assert ev1 is ev2
    assert ev2["text"] == "Send 1 home"
    assert ev2["impact_estimate_aud"] == 120.0
    assert len(acct.history("earls")) == 1


def test_record_normalises_priority_when_invalid():
    _reset()
    ev = acct.record("v", text="x", priority="urgent")
    assert ev["priority"] == "med"


def test_record_trims_text():
    _reset()
    ev = acct.record("v", text="   spaced  out   ")
    assert ev["text"] == "spaced  out"


def test_record_truncates_history_at_max():
    _reset()
    original_max = acct.MAX_HISTORY
    acct.MAX_HISTORY = 5
    try:
        for i in range(8):
            acct.record("v", text=f"rec {i}")
        hist = acct.history("v")
        assert len(hist) == 5
        # Oldest dropped — "rec 0" through "rec 2" should be gone
        texts = [e["text"] for e in hist]
        assert texts == ["rec 3", "rec 4", "rec 5", "rec 6", "rec 7"]
    finally:
        acct.MAX_HISTORY = original_max


# ---------------------------------------------------------------------------
# respond
# ---------------------------------------------------------------------------

def test_respond_accepts_and_timestamps_event():
    _reset()
    ev = acct.record("v", text="Cut 1", rec_id="rec_a")
    out = acct.respond("v", "rec_a", status="accepted", note="sent a bartender home")
    assert out["status"] == "accepted"
    assert out["responded_at"].endswith("Z")
    assert out["response_note"] == "sent a bartender home"
    assert ev["status"] == "accepted"  # mutated in place


def test_respond_dismiss_without_note_is_allowed():
    _reset()
    acct.record("v", text="Cut 1", rec_id="rec_b")
    out = acct.respond("v", "rec_b", status="dismissed")
    assert out["status"] == "dismissed"
    assert out["response_note"] is None


def test_respond_bad_status_raises_value_error():
    _reset()
    acct.record("v", text="Cut 1", rec_id="rec_c")
    try:
        acct.respond("v", "rec_c", status="pending")
    except ValueError:
        return
    raise AssertionError("respond() should have raised ValueError for bad status")


def test_respond_unknown_rec_id_raises_key_error():
    _reset()
    try:
        acct.respond("nonexistent", "rec_xyz", status="accepted")
    except KeyError:
        return
    raise AssertionError("respond() should have raised KeyError for unknown id")


# ---------------------------------------------------------------------------
# summary
# ---------------------------------------------------------------------------

def test_summary_empty_venue_returns_zero_block():
    _reset()
    out = acct.summary("empty_v")
    assert out["total"] == 0
    assert out["pending"] == 0
    assert out["accepted"] == 0
    assert out["dismissed"] == 0
    assert out["estimated_impact_missed_aud"] == 0.0
    assert out["estimated_impact_pending_aud"] == 0.0
    assert out["acceptance_rate"] == 0.0


def test_summary_mixed_statuses_and_acceptance_rate():
    _reset()
    acct.record("v", text="a", rec_id="r1", impact_estimate_aud=100)
    acct.record("v", text="b", rec_id="r2", impact_estimate_aud=200)
    acct.record("v", text="c", rec_id="r3", impact_estimate_aud=300)
    acct.record("v", text="d", rec_id="r4", impact_estimate_aud=50)
    acct.respond("v", "r1", status="accepted")
    acct.respond("v", "r2", status="dismissed")
    acct.respond("v", "r3", status="dismissed")

    out = acct.summary("v")
    assert out["total"] == 4
    assert out["pending"] == 1
    assert out["accepted"] == 1
    assert out["dismissed"] == 2
    assert out["estimated_impact_missed_aud"] == 500.0  # 200 + 300
    assert out["estimated_impact_pending_aud"] == 50.0
    # acceptance rate = 1 accepted / (1 accepted + 2 dismissed) = 0.3333
    assert abs(out["acceptance_rate"] - 0.3333) < 0.001


def test_summary_tolerates_none_and_string_impact():
    _reset()
    acct.record("v", text="a", rec_id="r1", impact_estimate_aud=None)
    acct.record("v", text="b", rec_id="r2", impact_estimate_aud=150.0)
    acct.respond("v", "r1", status="dismissed")
    acct.respond("v", "r2", status="dismissed")
    # Manually corrupt one event's impact to exercise the try/except path
    acct.history("v")[1]["impact_estimate_aud"] = "not a number"

    out = acct.summary("v")
    assert out["dismissed"] == 2
    # r1 had None → 0, r2 had corrupt string → 0
    assert out["estimated_impact_missed_aud"] == 0.0


# ---------------------------------------------------------------------------
# state
# ---------------------------------------------------------------------------

def test_state_returns_recent_newest_first_and_summary():
    _reset()
    for i in range(5):
        acct.record("v", text=f"rec {i}", rec_id=f"r{i}")
    state = acct.state("v", recent_limit=3)
    assert state["venue_id"] == "v"
    assert state["generated_at"].endswith("Z")
    assert "summary" in state
    assert state["summary"]["total"] == 5
    # Recent limited to 3, newest first
    ids = [e["id"] for e in state["recent"]]
    assert ids == ["r4", "r3", "r2"]


def test_state_empty_venue_still_has_sane_shape():
    _reset()
    state = acct.state("brand_new")
    assert state["venue_id"] == "brand_new"
    assert state["summary"]["total"] == 0
    assert state["recent"] == []


# ---------------------------------------------------------------------------
# cross-venue isolation
# ---------------------------------------------------------------------------

def test_records_are_isolated_per_venue():
    _reset()
    acct.record("v1", text="a", rec_id="r1")
    acct.record("v2", text="b", rec_id="r2")
    assert len(acct.history("v1")) == 1
    assert len(acct.history("v2")) == 1
    # respond on v1 doesn't find rec belonging to v2
    try:
        acct.respond("v1", "r2", status="accepted")
    except KeyError:
        pass
    else:
        raise AssertionError("should not find cross-venue rec")


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
