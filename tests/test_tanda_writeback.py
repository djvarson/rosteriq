"""Tests for rosteriq.tanda_writeback — pure stdlib, no pytest."""
from __future__ import annotations

import json
import os
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from rosteriq import tanda_writeback as tw  # noqa: E402


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _rec(rec_id, *, status="accepted", impact=0.0, text="", source="wage_pulse"):
    return {
        "id": rec_id,
        "rec_id": rec_id,
        "status": status,
        "impact_estimate_aud": impact,
        "text": text,
        "source": source,
    }


class StubStore:
    def __init__(self, events):
        self._events = list(events)

    def history(self, venue_id):
        return list(self._events)


# ---------------------------------------------------------------------------
# _action_suffix
# ---------------------------------------------------------------------------

def test_action_suffix_parses_standard_pulse_rec_id():
    assert tw._action_suffix("rec_pulse_venue_demo_001_2026-04-12_over_wage_high") == "over_wage_high"


def test_action_suffix_handles_venue_with_underscores():
    assert tw._action_suffix("rec_pulse_a_b_c_2026-04-12_under_wage") == "under_wage"


def test_action_suffix_returns_none_for_non_pulse_id():
    assert tw._action_suffix("rec_manual_abc123") is None
    assert tw._action_suffix("random") is None
    assert tw._action_suffix("") is None
    assert tw._action_suffix(None) is None


def test_action_suffix_returns_none_when_no_date_segment():
    assert tw._action_suffix("rec_pulse_venue_foo_bar") is None


# ---------------------------------------------------------------------------
# compose_delta_from_rec
# ---------------------------------------------------------------------------

def test_compose_delta_over_wage_high_maps_to_cut_staff():
    rec = _rec(
        "rec_pulse_venue_demo_001_2026-04-12_over_wage_high",
        impact=450.0,
        text="Wage % projected at 33% — 5pt over 28% target. Cut 2 staff now to avoid a $450 overrun.",
    )
    delta = tw.compose_delta_from_rec(rec)
    assert delta is not None
    assert delta.kind == tw.DELTA_CUT_STAFF
    assert delta.count == 2
    assert delta.timing_hint == "immediate"
    assert delta.priority == "high"
    assert delta.impact_estimate_aud == 450.0
    assert delta.source_rec_id.endswith("over_wage_high")
    assert delta.reason.startswith("Wage % projected at 33%")
    assert delta.metadata["action_suffix"] == "over_wage_high"


def test_compose_delta_over_wage_med_maps_to_send_home():
    rec = _rec(
        "rec_pulse_venue_demo_001_2026-04-12_over_wage_med",
        impact=220.0,
        text="Wage % trending 3.5pt over 28% target (now 31.5%). Send 1 staff home after the next peak — ~$220 at risk.",
    )
    delta = tw.compose_delta_from_rec(rec)
    assert delta is not None
    assert delta.kind == tw.DELTA_SEND_HOME
    assert delta.count == 1
    assert delta.timing_hint == "after_peak"
    assert delta.priority == "med"


def test_compose_delta_under_wage_maps_to_call_in():
    rec = _rec(
        "rec_pulse_venue_demo_001_2026-04-12_under_wage",
        impact=0.0,
        text="Wage % is 4.0pt under 28% target — possibly understaffed.",
    )
    delta = tw.compose_delta_from_rec(rec)
    assert delta is not None
    assert delta.kind == tw.DELTA_CALL_IN
    assert delta.timing_hint == "before_service"


def test_compose_delta_burn_rate_high_maps_to_trim_shift():
    rec = _rec(
        "rec_pulse_venue_demo_001_2026-04-12_burn_rate_high",
        impact=310.0,
        text="Burn rate $180/hr with 2h 30m left — projecting $1,200 vs $900 forecast.",
    )
    delta = tw.compose_delta_from_rec(rec)
    assert delta is not None
    assert delta.kind == tw.DELTA_TRIM_SHIFT
    assert delta.timing_hint == "next_break"
    assert delta.priority == "high"
    assert delta.impact_estimate_aud == 310.0


def test_compose_delta_returns_none_for_manual_rec():
    rec = _rec("rec_manual_abc", text="Free-text human rec")
    assert tw.compose_delta_from_rec(rec) is None


def test_compose_delta_returns_none_for_unknown_action_suffix():
    rec = _rec("rec_pulse_v_2026-04-12_mystery_action")
    assert tw.compose_delta_from_rec(rec) is None


def test_compose_delta_returns_none_for_non_dict():
    assert tw.compose_delta_from_rec(None) is None
    assert tw.compose_delta_from_rec("string") is None


def test_compose_delta_tolerates_missing_impact():
    rec = _rec("rec_pulse_v_2026-04-12_over_wage_med")
    del rec["impact_estimate_aud"]
    delta = tw.compose_delta_from_rec(rec)
    assert delta is not None
    assert delta.impact_estimate_aud == 0.0


def test_compose_delta_tolerates_bad_impact_type():
    rec = _rec("rec_pulse_v_2026-04-12_over_wage_med", impact="not a number")
    delta = tw.compose_delta_from_rec(rec)
    assert delta is not None
    assert delta.impact_estimate_aud == 0.0


def test_compose_delta_reason_falls_back_when_text_empty():
    rec = _rec("rec_pulse_v_2026-04-12_over_wage_high", text="")
    delta = tw.compose_delta_from_rec(rec)
    assert delta is not None
    assert "over_wage_high" in delta.reason


# ---------------------------------------------------------------------------
# Sinks
# ---------------------------------------------------------------------------

def test_null_sink_records_calls_and_returns_dry_run():
    sink = tw.NullSink()
    delta = tw.ShiftDelta(kind=tw.DELTA_CUT_STAFF, count=2, timing_hint="immediate", reason="test")
    result = sink.apply("venue_1", delta)
    assert result["ok"] is True
    assert result["dry_run"] is True
    assert len(sink.calls) == 1
    assert sink.calls[0]["venue_id"] == "venue_1"
    assert sink.calls[0]["delta"]["kind"] == tw.DELTA_CUT_STAFF


def test_journal_sink_writes_jsonl_entry():
    with tempfile.TemporaryDirectory() as d:
        path = os.path.join(d, "sub", "journal.jsonl")
        sink = tw.JournalSink(path)
        delta = tw.ShiftDelta(
            kind=tw.DELTA_SEND_HOME, count=1, timing_hint="after_peak", reason="test"
        )
        result = sink.apply("venue_9", delta)
        assert result["ok"] is True
        assert result["path"] == path
        with open(path) as f:
            lines = [json.loads(l) for l in f if l.strip()]
        assert len(lines) == 1
        assert lines[0]["venue_id"] == "venue_9"
        assert lines[0]["delta"]["kind"] == tw.DELTA_SEND_HOME
        assert "ts" in lines[0]


def test_journal_sink_appends_multiple_entries():
    with tempfile.TemporaryDirectory() as d:
        path = os.path.join(d, "j.jsonl")
        sink = tw.JournalSink(path)
        for i in range(3):
            sink.apply(
                f"venue_{i}",
                tw.ShiftDelta(kind=tw.DELTA_CUT_STAFF, count=1, timing_hint="immediate", reason="r"),
            )
        with open(path) as f:
            lines = f.readlines()
        assert len(lines) == 3


def test_journal_sink_returns_error_dict_on_bad_path():
    sink = tw.JournalSink("/proc/1/bad/nope/cannot-write.jsonl")
    result = sink.apply(
        "v", tw.ShiftDelta(kind=tw.DELTA_CUT_STAFF, count=1, timing_hint="immediate", reason="r")
    )
    assert result["ok"] is False
    assert "error" in result


def test_callable_sink_wraps_function():
    calls = []

    def fn(venue_id, delta_dict):
        calls.append((venue_id, delta_dict["kind"]))
        return {"custom": "yes"}

    sink = tw.CallableSink("custom", fn)
    delta = tw.ShiftDelta(kind=tw.DELTA_CUT_STAFF, count=1, timing_hint="immediate", reason="r")
    result = sink.apply("v1", delta)
    assert result["custom"] == "yes"
    assert result["ok"] is True
    assert result["sink"] == "custom"
    assert calls == [("v1", tw.DELTA_CUT_STAFF)]


def test_callable_sink_captures_exceptions():
    def boom(venue_id, delta_dict):
        raise RuntimeError("kaboom")

    sink = tw.CallableSink("broken", boom)
    delta = tw.ShiftDelta(kind=tw.DELTA_CUT_STAFF, count=1, timing_hint="immediate", reason="r")
    result = sink.apply("v1", delta)
    assert result["ok"] is False
    assert "kaboom" in result["error"]


# ---------------------------------------------------------------------------
# Sink registry
# ---------------------------------------------------------------------------

def test_registry_register_and_clear():
    tw.clear_sinks()
    assert tw.registered_sinks() == []
    tw.register_sink(tw.NullSink())
    tw.register_sink(tw.NullSink())
    assert len(tw.registered_sinks()) == 2
    tw.clear_sinks()
    assert tw.registered_sinks() == []


# ---------------------------------------------------------------------------
# writeback_accepted_rec — end-to-end
# ---------------------------------------------------------------------------

def test_writeback_skips_when_rec_not_found():
    store = StubStore([])
    tw.clear_sinks()
    result = tw.writeback_accepted_rec("venue_1", "rec_missing", store=store)
    assert result["status"] == "skipped"
    assert result["reason"] == "rec_not_found"
    assert result["delta"] is None
    assert result["results"] == []


def test_writeback_skips_when_rec_pending():
    rec = _rec("rec_pulse_v_2026-04-12_over_wage_high", status="pending")
    store = StubStore([rec])
    tw.clear_sinks()
    result = tw.writeback_accepted_rec(
        "venue_1", "rec_pulse_v_2026-04-12_over_wage_high", store=store
    )
    assert result["status"] == "skipped"
    assert "pending" in result["reason"]


def test_writeback_skips_when_rec_dismissed():
    rec = _rec("rec_pulse_v_2026-04-12_over_wage_high", status="dismissed")
    store = StubStore([rec])
    tw.clear_sinks()
    result = tw.writeback_accepted_rec(
        "venue_1", "rec_pulse_v_2026-04-12_over_wage_high", store=store
    )
    assert result["status"] == "skipped"
    assert "dismissed" in result["reason"]


def test_writeback_returns_no_delta_for_manual_rec():
    rec = _rec("rec_manual_abc", status="accepted", text="Human rec")
    store = StubStore([rec])
    tw.clear_sinks()
    result = tw.writeback_accepted_rec("venue_1", "rec_manual_abc", store=store)
    assert result["status"] == "no_delta"
    assert result["delta"] is None


def test_writeback_no_sinks_returns_ok_but_empty_results():
    rec = _rec(
        "rec_pulse_v_2026-04-12_over_wage_high",
        status="accepted",
        impact=500.0,
        text="Wage % projected at 33%. Cut 2 staff now.",
    )
    store = StubStore([rec])
    tw.clear_sinks()
    result = tw.writeback_accepted_rec(
        "venue_1", "rec_pulse_v_2026-04-12_over_wage_high", store=store
    )
    assert result["status"] == "ok"
    assert result["reason"] == "no_sinks_registered"
    assert result["delta"]["kind"] == tw.DELTA_CUT_STAFF
    assert result["results"] == []


def test_writeback_fans_out_to_multiple_sinks():
    rec = _rec(
        "rec_pulse_v_2026-04-12_over_wage_high",
        status="accepted",
        impact=500.0,
        text="Cut staff now",
    )
    store = StubStore([rec])

    null1 = tw.NullSink()
    null2 = tw.NullSink()
    result = tw.writeback_accepted_rec(
        "venue_1",
        "rec_pulse_v_2026-04-12_over_wage_high",
        store=store,
        sinks=[null1, null2],
    )
    assert result["status"] == "ok"
    assert len(result["results"]) == 2
    assert all(r["ok"] for r in result["results"])
    assert len(null1.calls) == 1
    assert len(null2.calls) == 1


def test_writeback_isolates_failing_sink():
    rec = _rec(
        "rec_pulse_v_2026-04-12_over_wage_med",
        status="accepted",
        text="Send 1 home",
    )
    store = StubStore([rec])

    def good_fn(venue_id, delta):
        return {"wrote": True}

    def bad_fn(venue_id, delta):
        raise ValueError("down")

    good = tw.CallableSink("good", good_fn)
    bad = tw.CallableSink("bad", bad_fn)
    result = tw.writeback_accepted_rec(
        "venue_1",
        "rec_pulse_v_2026-04-12_over_wage_med",
        store=store,
        sinks=[good, bad],
    )
    assert result["status"] == "partial"
    assert len(result["results"]) == 2
    by_name = {r["sink"]: r for r in result["results"]}
    assert by_name["good"]["ok"] is True
    assert by_name["bad"]["ok"] is False


def test_writeback_uses_module_registry_when_sinks_omitted():
    rec = _rec(
        "rec_pulse_v_2026-04-12_under_wage",
        status="accepted",
        text="Call in a casual",
    )
    store = StubStore([rec])

    tw.clear_sinks()
    reg_sink = tw.NullSink()
    tw.register_sink(reg_sink)
    try:
        result = tw.writeback_accepted_rec(
            "venue_1", "rec_pulse_v_2026-04-12_under_wage", store=store
        )
        assert result["status"] == "ok"
        assert len(result["results"]) == 1
        assert len(reg_sink.calls) == 1
    finally:
        tw.clear_sinks()


def test_writeback_delta_contains_source_rec_id():
    rec = _rec(
        "rec_pulse_v_2026-04-12_burn_rate_high",
        status="accepted",
        impact=200.0,
        text="Trim at next break",
    )
    store = StubStore([rec])
    tw.clear_sinks()
    result = tw.writeback_accepted_rec(
        "venue_1", "rec_pulse_v_2026-04-12_burn_rate_high", store=store
    )
    assert result["delta"]["source_rec_id"].endswith("burn_rate_high")
    assert result["delta"]["kind"] == tw.DELTA_TRIM_SHIFT


# ---------------------------------------------------------------------------
# read_journal
# ---------------------------------------------------------------------------

def test_read_journal_returns_empty_for_missing_file():
    assert tw.read_journal("/tmp/rq_definitely_does_not_exist_xyz.jsonl") == []


def test_read_journal_returns_most_recent_first():
    with tempfile.TemporaryDirectory() as d:
        path = os.path.join(d, "j.jsonl")
        sink = tw.JournalSink(path)
        for i in range(5):
            sink.apply(
                f"venue_{i}",
                tw.ShiftDelta(kind=tw.DELTA_CUT_STAFF, count=1, timing_hint="immediate", reason=f"r{i}"),
            )
        entries = tw.read_journal(path)
        assert len(entries) == 5
        # Newest first
        assert entries[0]["venue_id"] == "venue_4"
        assert entries[-1]["venue_id"] == "venue_0"


def test_read_journal_filters_by_venue_id():
    with tempfile.TemporaryDirectory() as d:
        path = os.path.join(d, "j.jsonl")
        sink = tw.JournalSink(path)
        for venue in ["v1", "v2", "v1", "v3", "v1"]:
            sink.apply(
                venue,
                tw.ShiftDelta(kind=tw.DELTA_CUT_STAFF, count=1, timing_hint="immediate", reason="r"),
            )
        entries = tw.read_journal(path, venue_id="v1")
        assert len(entries) == 3
        assert all(e["venue_id"] == "v1" for e in entries)


def test_read_journal_respects_limit():
    with tempfile.TemporaryDirectory() as d:
        path = os.path.join(d, "j.jsonl")
        sink = tw.JournalSink(path)
        for i in range(10):
            sink.apply(
                "v",
                tw.ShiftDelta(kind=tw.DELTA_CUT_STAFF, count=1, timing_hint="immediate", reason="r"),
            )
        entries = tw.read_journal(path, limit=4)
        assert len(entries) == 4


def test_read_journal_tolerates_malformed_lines():
    with tempfile.TemporaryDirectory() as d:
        path = os.path.join(d, "j.jsonl")
        with open(path, "w") as f:
            f.write(json.dumps({"ts": "x", "venue_id": "v", "delta": {}}) + "\n")
            f.write("garbage not json\n")
            f.write("\n")
            f.write(json.dumps({"ts": "y", "venue_id": "v", "delta": {}}) + "\n")
        entries = tw.read_journal(path)
        assert len(entries) == 2


# ---------------------------------------------------------------------------
# Store round-trip — the real accountability_store
# ---------------------------------------------------------------------------

def test_writeback_against_real_store_full_accepted_flow():
    from rosteriq import accountability_store as store
    store.clear()
    try:
        # Record a pulse-shaped rec
        ev = store.record(
            "venue_wb_test",
            text="Wage % projected at 33% — 5pt over target. Cut 2 staff now.",
            source="wage_pulse",
            priority="high",
            impact_estimate_aud=450.0,
            rec_id="rec_pulse_venue_wb_test_2026-04-12_over_wage_high",
        )
        # Accept it
        store.respond("venue_wb_test", ev["id"], status="accepted")

        # Run writeback with a capturing null sink
        sink = tw.NullSink()
        result = tw.writeback_accepted_rec(
            "venue_wb_test",
            "rec_pulse_venue_wb_test_2026-04-12_over_wage_high",
            sinks=[sink],
        )
        assert result["status"] == "ok"
        assert result["delta"]["kind"] == tw.DELTA_CUT_STAFF
        assert result["delta"]["impact_estimate_aud"] == 450.0
        assert len(sink.calls) == 1
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
