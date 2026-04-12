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

# ---------------------------------------------------------------------------
# TandaApiSink (Moment 14-follow-on 2 — real plugin surface)
# ---------------------------------------------------------------------------

class _RecordingSleep:
    """Captures sleep durations without actually blocking."""
    def __init__(self):
        self.calls = []
    def __call__(self, s):
        self.calls.append(float(s))


def _install_attempt_once(sink, sequence):
    """Replace sink._attempt_once with a scripted sequence of result dicts."""
    i = {"n": 0}
    def _fake(data, headers):
        n = i["n"]
        i["n"] = n + 1
        if n < len(sequence):
            return dict(sequence[n])
        return dict(sequence[-1])
    sink._attempt_once = _fake


def _sample_delta():
    rec = _rec(
        "rec_pulse_vA_2026-04-10_over_wage_high",
        status="accepted",
        impact=120.0,
        text="Cut two staff.",
    )
    return tw.compose_delta_from_rec(rec)


def test_tanda_api_sink_builds_default_payload_with_venue_and_delta():
    sink = tw.TandaApiSink("https://tanda.example/api/writeback")
    delta = _sample_delta()
    payload = sink._build_payload("vA", delta)
    assert payload["venue_id"] == "vA"
    assert payload["rec_id"] == "rec_pulse_vA_2026-04-10_over_wage_high"
    assert payload["delta"]["kind"] == tw.DELTA_CUT_STAFF
    assert payload["delta"]["count"] == 2


def test_tanda_api_sink_transform_overrides_payload_shape():
    def reshape(venue_id, delta):
        return {"shift_delta": delta, "v": venue_id}
    sink = tw.TandaApiSink("https://tanda.example/api", transform=reshape)
    delta = _sample_delta()
    payload = sink._build_payload("vA", delta)
    assert payload == {"shift_delta": delta.to_dict(), "v": "vA"}


def test_tanda_api_sink_non_dict_transform_result_is_wrapped():
    sink = tw.TandaApiSink("https://tanda.example/api", transform=lambda v, d: "nope")
    payload = sink._build_payload("vA", _sample_delta())
    assert payload == {"raw": "nope"}


def test_tanda_api_sink_headers_include_bearer_and_idempotency_key():
    sink = tw.TandaApiSink(
        "https://tanda.example/api",
        api_token="secret_abc",
        extra_headers={"X-Tanda-Org": "dale_group"},
    )
    headers = sink._build_headers(_sample_delta())
    assert headers["Authorization"] == "Bearer secret_abc"
    assert headers["Idempotency-Key"] == "rec_pulse_vA_2026-04-10_over_wage_high"
    assert headers["Content-Type"] == "application/json"
    assert headers["Accept"] == "application/json"
    assert headers["X-Tanda-Org"] == "dale_group"


def test_tanda_api_sink_headers_skip_auth_when_no_token():
    sink = tw.TandaApiSink("https://tanda.example/api")
    headers = sink._build_headers(_sample_delta())
    assert "Authorization" not in headers


def test_tanda_api_sink_compute_backoff_exponential_with_cap():
    sink = tw.TandaApiSink("https://tanda.example/api", backoff_base_s=1.0, backoff_cap_s=5.0)
    assert sink._compute_backoff(0) == 1.0
    assert sink._compute_backoff(1) == 2.0
    assert sink._compute_backoff(2) == 4.0
    assert sink._compute_backoff(3) == 5.0  # capped
    assert sink._compute_backoff(10) == 5.0


def test_tanda_api_sink_compute_backoff_zero_base_is_zero():
    sink = tw.TandaApiSink("https://tanda.example/api", backoff_base_s=0)
    assert sink._compute_backoff(0) == 0.0
    assert sink._compute_backoff(5) == 0.0


def test_tanda_api_sink_succeeds_first_attempt_no_sleep():
    sleeper = _RecordingSleep()
    sink = tw.TandaApiSink(
        "https://tanda.example/api",
        max_attempts=3,
        sleep=sleeper,
    )
    _install_attempt_once(sink, [
        {"ok": True, "status_code": 200, "retryable": False, "response": {"accepted": True}},
    ])
    result = sink.apply("vA", _sample_delta())
    assert result["ok"] is True
    assert result["status_code"] == 200
    assert result["response"] == {"accepted": True}
    assert len(result["attempts"]) == 1
    assert sleeper.calls == []  # no retries, no sleep
    assert result["idempotency_key"] == "rec_pulse_vA_2026-04-10_over_wage_high"


def test_tanda_api_sink_retries_then_succeeds():
    sleeper = _RecordingSleep()
    sink = tw.TandaApiSink(
        "https://tanda.example/api",
        max_attempts=3,
        backoff_base_s=0.1,
        sleep=sleeper,
    )
    _install_attempt_once(sink, [
        {"ok": False, "status_code": 503, "error": "HTTP 503", "retryable": True},
        {"ok": False, "status_code": 502, "error": "HTTP 502", "retryable": True},
        {"ok": True,  "status_code": 200, "retryable": False, "response": None},
    ])
    result = sink.apply("vA", _sample_delta())
    assert result["ok"] is True
    assert len(result["attempts"]) == 3
    # Two sleeps between three attempts
    assert len(sleeper.calls) == 2


def test_tanda_api_sink_bails_on_non_retryable_4xx():
    sleeper = _RecordingSleep()
    sink = tw.TandaApiSink(
        "https://tanda.example/api",
        max_attempts=5,
        sleep=sleeper,
        dead_letter_path=None,
    )
    _install_attempt_once(sink, [
        {"ok": False, "status_code": 400, "error": "HTTP 400", "retryable": False},
        {"ok": True,  "status_code": 200, "retryable": False},
    ])
    result = sink.apply("vA", _sample_delta())
    assert result["ok"] is False
    # Only one attempt — non-retryable bailed immediately
    assert len(result["attempts"]) == 1
    assert result["status_code"] == 400
    assert sleeper.calls == []


def test_tanda_api_sink_exhausts_retries_and_dead_letters():
    tmpdir = tempfile.mkdtemp(prefix="rq_tanda_dl_")
    try:
        dl = os.path.join(tmpdir, "tanda_dl.jsonl")
        sleeper = _RecordingSleep()
        sink = tw.TandaApiSink(
            "https://tanda.example/api",
            max_attempts=3,
            backoff_base_s=0.1,
            sleep=sleeper,
            dead_letter_path=dl,
        )
        _install_attempt_once(sink, [
            {"ok": False, "status_code": 503, "error": "HTTP 503", "retryable": True},
        ])
        result = sink.apply("vA", _sample_delta())
        assert result["ok"] is False
        assert len(result["attempts"]) == 3
        assert result["dead_lettered_to"] == dl
        # Verify the file was appended with a full entry
        with open(dl) as f:
            lines = [ln for ln in f.read().splitlines() if ln]
        assert len(lines) == 1
        entry = json.loads(lines[0])
        assert entry["venue_id"] == "vA"
        assert entry["payload"]["delta"]["kind"] == tw.DELTA_CUT_STAFF
        assert len(entry["attempts"]) == 3
    finally:
        import shutil
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_tanda_api_sink_never_raises_on_payload_build_failure():
    def bomb(v, d):
        raise RuntimeError("cannot serialize")
    sink = tw.TandaApiSink("https://tanda.example/api", transform=bomb)
    result = sink.apply("vA", _sample_delta())
    assert result["ok"] is False
    assert "payload_build_failed" in result["error"]


def test_tanda_api_sink_url_error_is_retryable():
    sleeper = _RecordingSleep()
    sink = tw.TandaApiSink(
        "https://tanda.example/api",
        max_attempts=3,
        backoff_base_s=0.1,
        sleep=sleeper,
    )
    _install_attempt_once(sink, [
        {"ok": False, "status_code": 0, "error": "URLError: conn refused", "retryable": True},
        {"ok": False, "status_code": 0, "error": "URLError: conn refused", "retryable": True},
        {"ok": True,  "status_code": 200, "retryable": False},
    ])
    result = sink.apply("vA", _sample_delta())
    assert result["ok"] is True
    assert len(result["attempts"]) == 3


def test_tanda_api_sink_single_attempt_mode():
    sleeper = _RecordingSleep()
    sink = tw.TandaApiSink(
        "https://tanda.example/api",
        max_attempts=1,
        sleep=sleeper,
    )
    _install_attempt_once(sink, [
        {"ok": False, "status_code": 500, "error": "HTTP 500", "retryable": True},
    ])
    result = sink.apply("vA", _sample_delta())
    assert result["ok"] is False
    assert len(result["attempts"]) == 1
    # max_attempts=1 → no sleeps ever
    assert sleeper.calls == []


def test_read_dead_letter_returns_empty_on_missing_file():
    assert tw.read_dead_letter("/tmp/nope_does_not_exist_rq.jsonl") == []


def test_read_dead_letter_filters_by_venue_and_reverses_order():
    tmpdir = tempfile.mkdtemp(prefix="rq_tanda_dl_read_")
    try:
        dl = os.path.join(tmpdir, "dl.jsonl")
        with open(dl, "w") as f:
            f.write(json.dumps({"ts": "2026-04-10T00:00:00Z", "venue_id": "a", "payload": {"i": 1}}) + "\n")
            f.write("not json\n")  # tolerated
            f.write(json.dumps({"ts": "2026-04-11T00:00:00Z", "venue_id": "b", "payload": {"i": 2}}) + "\n")
            f.write(json.dumps({"ts": "2026-04-12T00:00:00Z", "venue_id": "a", "payload": {"i": 3}}) + "\n")
        all_entries = tw.read_dead_letter(dl)
        assert [e["payload"]["i"] for e in all_entries] == [3, 2, 1]  # reversed
        only_a = tw.read_dead_letter(dl, venue_id="a")
        assert [e["payload"]["i"] for e in only_a] == [3, 1]
        limited = tw.read_dead_letter(dl, limit=1)
        assert len(limited) == 1
        assert limited[0]["payload"]["i"] == 3
    finally:
        import shutil
        shutil.rmtree(tmpdir, ignore_errors=True)


# ---------------------------------------------------------------------------
# Integration: real in-process HTTPServer
# ---------------------------------------------------------------------------

def _start_http_server(handler_cls):
    """Start an HTTPServer on 127.0.0.1:<ephemeral> in a daemon thread.

    Returns (server, thread, base_url). Caller must call server.shutdown().
    """
    from http.server import HTTPServer
    import threading
    server = HTTPServer(("127.0.0.1", 0), handler_cls)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, thread, f"http://127.0.0.1:{port}"


class _TandaRecordingHandler:
    """Factory for a BaseHTTPRequestHandler that records requests and
    replies with a scripted sequence of (status, body) tuples."""
    @staticmethod
    def make(script):
        from http.server import BaseHTTPRequestHandler
        received: List[Dict[str, Any]] = []
        # Closure-captured mutable list so a fresh factory per test
        # isolates state. Class-body names aren't visible to methods
        # in Python, which is why this uses a closure rather than a
        # class attribute.
        script_box = list(script)

        class _Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                length = int(self.headers.get("Content-Length") or 0)
                body = self.rfile.read(length) if length else b""
                received.append({
                    "path": self.path,
                    "headers": {k: v for k, v in self.headers.items()},
                    "body": body.decode("utf-8") if body else "",
                })
                if script_box:
                    status, resp_body = script_box.pop(0)
                else:
                    status, resp_body = 200, "{}"
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(resp_body.encode("utf-8"))))
                self.end_headers()
                self.wfile.write(resp_body.encode("utf-8"))

            def log_message(self, *args, **kwargs):
                pass  # silence default stderr logging

        return _Handler, received


def test_tanda_api_sink_integration_posts_to_real_server_and_sends_headers():
    handler, received = _TandaRecordingHandler.make([(200, '{"accepted": true}')])
    server, thread, base = _start_http_server(handler)
    try:
        sink = tw.TandaApiSink(
            base + "/writeback",
            api_token="live_token_xyz",
            timeout_s=3.0,
            max_attempts=1,
        )
        result = sink.apply("vA", _sample_delta())
        assert result["ok"] is True
        assert result["status_code"] == 200
        assert result["response"] == {"accepted": True}
        assert len(received) == 1
        req = received[0]
        assert req["path"] == "/writeback"
        assert req["headers"].get("Authorization") == "Bearer live_token_xyz"
        assert req["headers"].get("Idempotency-Key") == "rec_pulse_vA_2026-04-10_over_wage_high"
        assert req["headers"].get("Content-Type") == "application/json"
        body = json.loads(req["body"])
        assert body["venue_id"] == "vA"
        assert body["delta"]["kind"] == tw.DELTA_CUT_STAFF
    finally:
        server.shutdown()


def test_tanda_api_sink_integration_retries_503_then_succeeds():
    handler, received = _TandaRecordingHandler.make([
        (503, '{"error":"unavailable"}'),
        (502, '{"error":"bad gateway"}'),
        (200, '{"ok":true}'),
    ])
    server, thread, base = _start_http_server(handler)
    try:
        sink = tw.TandaApiSink(
            base + "/writeback",
            max_attempts=3,
            backoff_base_s=0.01,
            sleep=_RecordingSleep(),
        )
        result = sink.apply("vA", _sample_delta())
        assert result["ok"] is True
        assert len(result["attempts"]) == 3
        assert len(received) == 3
    finally:
        server.shutdown()


def test_tanda_api_sink_integration_bails_on_400_no_retry():
    handler, received = _TandaRecordingHandler.make([
        (400, '{"error":"bad payload"}'),
        (200, '{"ok":true}'),
    ])
    server, thread, base = _start_http_server(handler)
    try:
        sink = tw.TandaApiSink(
            base + "/writeback",
            max_attempts=5,
            sleep=_RecordingSleep(),
        )
        result = sink.apply("vA", _sample_delta())
        assert result["ok"] is False
        assert result["status_code"] == 400
        assert len(received) == 1  # no retry on 400
    finally:
        server.shutdown()


def test_tanda_api_sink_integration_dead_letters_persistent_5xx():
    tmpdir = tempfile.mkdtemp(prefix="rq_tanda_dl_int_")
    try:
        dl = os.path.join(tmpdir, "dl.jsonl")
        handler, received = _TandaRecordingHandler.make([
            (503, '{"error":"x"}'),
            (503, '{"error":"x"}'),
            (503, '{"error":"x"}'),
        ])
        server, thread, base = _start_http_server(handler)
        try:
            sink = tw.TandaApiSink(
                base + "/writeback",
                max_attempts=3,
                backoff_base_s=0.01,
                sleep=_RecordingSleep(),
                dead_letter_path=dl,
            )
            result = sink.apply("vA", _sample_delta())
            assert result["ok"] is False
            assert result["dead_lettered_to"] == dl
            entries = tw.read_dead_letter(dl, venue_id="vA")
            assert len(entries) == 1
            assert entries[0]["payload"]["delta"]["kind"] == tw.DELTA_CUT_STAFF
        finally:
            server.shutdown()
    finally:
        import shutil
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_tanda_api_sink_integration_writeback_accepted_rec_end_to_end():
    """Full pipeline: writeback_accepted_rec → TandaApiSink → real HTTP."""
    handler, received = _TandaRecordingHandler.make([(200, '{"applied":true}')])
    server, thread, base = _start_http_server(handler)
    try:
        rec = _rec(
            "rec_pulse_vA_2026-04-10_over_wage_high",
            status="accepted",
            impact=300.0,
            text="Cut two staff now.",
        )
        store = StubStore([rec])
        sink = tw.TandaApiSink(
            base + "/writeback",
            max_attempts=1,
        )
        result = tw.writeback_accepted_rec(
            "vA",
            "rec_pulse_vA_2026-04-10_over_wage_high",
            store=store,
            sinks=[sink],
        )
        assert result["status"] == "ok"
        assert result["delta"]["kind"] == tw.DELTA_CUT_STAFF
        assert len(result["results"]) == 1
        assert result["results"][0]["ok"] is True
        assert result["results"][0]["sink"] == "tanda_api"
        # And the real HTTP server got exactly one POST
        assert len(received) == 1
        body = json.loads(received[0]["body"])
        assert body["venue_id"] == "vA"
        assert body["delta"]["kind"] == tw.DELTA_CUT_STAFF
    finally:
        server.shutdown()


def test_tanda_api_sink_custom_name_overrides_default():
    sink = tw.TandaApiSink("https://tanda.example/api", name="tanda_prod")
    assert sink.name == "tanda_prod"


# ---------------------------------------------------------------------------
# NEW: Timeout, health check, and rate-limit enhancements
# ---------------------------------------------------------------------------

def test_tanda_api_sink_default_timeout_is_30s():
    """Default timeout should be 30 seconds (not 5s)."""
    sink = tw.TandaApiSink("https://tanda.example/api")
    assert sink.timeout_s == 30.0


def test_tanda_api_sink_default_backoff_base_is_1s():
    """Default backoff base should be 1 second."""
    sink = tw.TandaApiSink("https://tanda.example/api")
    assert sink.backoff_base_s == 1.0


def test_tanda_api_sink_default_backoff_cap_is_30s():
    """Default backoff cap should be 30 seconds."""
    sink = tw.TandaApiSink("https://tanda.example/api")
    assert sink.backoff_cap_s == 30.0


def test_tanda_api_sink_parse_retry_after_seconds():
    """_parse_retry_after_header should parse numeric seconds."""
    sink = tw.TandaApiSink("https://tanda.example/api")
    assert sink._parse_retry_after_header("60") == 60.0
    assert sink._parse_retry_after_header("0") == 0.0
    assert sink._parse_retry_after_header("3.5") == 3.5


def test_tanda_api_sink_parse_retry_after_none_on_invalid():
    """_parse_retry_after_header should return None for non-numeric."""
    sink = tw.TandaApiSink("https://tanda.example/api")
    # HTTP-date format is not parsed (would need email.utils)
    assert sink._parse_retry_after_header("Mon, 01 Jan 2026 12:00:00 GMT") is None
    assert sink._parse_retry_after_header("invalid") is None
    assert sink._parse_retry_after_header("") is None
    assert sink._parse_retry_after_header(None) is None


def test_tanda_api_sink_health_check_returns_dict_with_required_fields():
    """check_health() should return a dict with ok, status_code, response_time_ms, url."""
    from http.server import BaseHTTPRequestHandler

    class _HealthHandler(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", "2")
            self.end_headers()
            self.wfile.write(b"{}")

        def log_message(self, *args, **kwargs):
            pass

    server, thread, base = _start_http_server(_HealthHandler)
    try:
        sink = tw.TandaApiSink(base + "/health", timeout_s=1.0)
        health = sink.check_health()
        assert isinstance(health, dict)
        assert "ok" in health
        assert "status_code" in health
        assert "response_time_ms" in health
        assert "url" in health
        assert health["url"] == base + "/health"
    finally:
        server.shutdown()


def test_tanda_api_sink_health_check_success_returns_ok_true():
    """check_health() should return ok=True on 2xx response."""
    # Create a simple handler that supports GET for health checks
    from http.server import BaseHTTPRequestHandler

    class _HealthHandler(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", "2")
            self.end_headers()
            self.wfile.write(b"{}")

        def log_message(self, *args, **kwargs):
            pass

    server, thread, base = _start_http_server(_HealthHandler)
    try:
        sink = tw.TandaApiSink(base + "/health", timeout_s=1.0)
        health = sink.check_health()
        assert health["ok"] is True
        assert health["status_code"] == 200
        assert health["response_time_ms"] > 0
    finally:
        server.shutdown()


def test_tanda_api_sink_health_check_timeout_returns_ok_false():
    """check_health() should handle timeout gracefully."""
    # Use a URL that won't respond (127.0.0.1:1 is typically not listening)
    sink = tw.TandaApiSink("http://127.0.0.1:1/health", timeout_s=0.1)
    health = sink.check_health()
    assert health["ok"] is False
    assert "error" in health
    assert health["response_time_ms"] > 0


def test_tanda_api_sink_respects_retry_after_on_429():
    """When 429 is returned with Retry-After header, use that delay."""
    sleeper = _RecordingSleep()
    sink = tw.TandaApiSink(
        "https://tanda.example/api",
        max_attempts=3,
        backoff_base_s=1.0,
        sleep=sleeper,
    )
    # Simulate 429 with Retry-After: 5
    # We need to mock the HTTPError to include headers
    def _fake_attempt(data, headers):
        if not hasattr(_fake_attempt, 'call_count'):
            _fake_attempt.call_count = 0
        _fake_attempt.call_count += 1
        if _fake_attempt.call_count == 1:
            # First attempt: 429 with Retry-After
            return {
                "ok": False,
                "status_code": 429,
                "error": "HTTP 429",
                "retryable": True,
                "retry_after_s": 5.0,
            }
        else:
            # Retry succeeds
            return {
                "ok": True,
                "status_code": 200,
                "retryable": False,
                "response": None,
            }

    sink._attempt_once = _fake_attempt
    result = sink.apply("vA", _sample_delta())
    assert result["ok"] is True
    assert len(result["attempts"]) == 2
    # Should have slept with the Retry-After value (5.0), not backoff
    assert sleeper.calls == [5.0]


def test_tanda_api_sink_falls_back_to_backoff_without_retry_after():
    """When 429 without Retry-After, use exponential backoff."""
    sleeper = _RecordingSleep()
    sink = tw.TandaApiSink(
        "https://tanda.example/api",
        max_attempts=3,
        backoff_base_s=1.0,
        sleep=sleeper,
    )
    # Simulate 429 without Retry-After
    def _fake_attempt(data, headers):
        if not hasattr(_fake_attempt, 'call_count'):
            _fake_attempt.call_count = 0
        _fake_attempt.call_count += 1
        if _fake_attempt.call_count == 1:
            # First attempt: 429 without Retry-After
            return {
                "ok": False,
                "status_code": 429,
                "error": "HTTP 429",
                "retryable": True,
            }
        else:
            # Retry succeeds
            return {
                "ok": True,
                "status_code": 200,
                "retryable": False,
                "response": None,
            }

    sink._attempt_once = _fake_attempt
    result = sink.apply("vA", _sample_delta())
    assert result["ok"] is True
    assert len(result["attempts"]) == 2
    # Should have slept with exponential backoff (base=1.0, attempt 0 → 1.0 * 2^0 = 1.0)
    assert sleeper.calls == [1.0]


def test_tanda_api_sink_timeout_is_enforced_in_health_check():
    """check_health() should respect the timeout_s setting."""
    # A very short timeout should fail quickly
    sink = tw.TandaApiSink("http://127.0.0.1:1/health", timeout_s=0.05)
    health = sink.check_health()
    assert health["ok"] is False
    # Response time should be < 1 second (much less than a full retry)
    assert health["response_time_ms"] < 1000


def test_tanda_api_sink_custom_timeout_applied_to_posts():
    """Custom timeout_s should be applied to POST requests."""
    handler, received = _TandaRecordingHandler.make([(200, '{"ok":true}')])
    server, thread, base = _start_http_server(handler)
    try:
        sink = tw.TandaApiSink(base + "/writeback", timeout_s=2.0, max_attempts=1)
        result = sink.apply("vA", _sample_delta())
        assert result["ok"] is True
        # If we reach here, the timeout was sufficient for the request
    finally:
        server.shutdown()


def test_tanda_api_sink_integration_429_with_real_retry_after_header():
    """Integration: 429 with real Retry-After header from HTTP server."""
    class _RetryAfterHandler:
        @staticmethod
        def make():
            from http.server import BaseHTTPRequestHandler
            received = []
            script_box = [
                (429, '{"error":"rate_limited"}'),
                (200, '{"ok":true}'),
            ]

            class _Handler(BaseHTTPRequestHandler):
                def do_POST(self):
                    length = int(self.headers.get("Content-Length") or 0)
                    body = self.rfile.read(length) if length else b""
                    received.append({"path": self.path, "body": body.decode("utf-8")})
                    if script_box:
                        status, resp_body = script_box.pop(0)
                    else:
                        status, resp_body = 200, "{}"
                    self.send_response(status)
                    self.send_header("Content-Type", "application/json")
                    if status == 429:
                        # Send Retry-After header
                        self.send_header("Retry-After", "2")
                    self.send_header("Content-Length", str(len(resp_body.encode("utf-8"))))
                    self.end_headers()
                    self.wfile.write(resp_body.encode("utf-8"))

                def log_message(self, *args, **kwargs):
                    pass
            return _Handler, received

    handler, received = _RetryAfterHandler.make()
    server, thread, base = _start_http_server(handler)
    try:
        sleeper = _RecordingSleep()
        sink = tw.TandaApiSink(
            base + "/writeback",
            max_attempts=3,
            backoff_base_s=1.0,
            sleep=sleeper,
            timeout_s=3.0,
        )
        result = sink.apply("vA", _sample_delta())
        assert result["ok"] is True
        assert len(result["attempts"]) == 2
        # Should have slept with the Retry-After value (2.0)
        assert sleeper.calls == [2.0]
    finally:
        server.shutdown()


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
