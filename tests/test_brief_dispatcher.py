"""Tests for rosteriq.brief_dispatcher — pure stdlib, no pytest."""
from __future__ import annotations

import json
import os
import shutil
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from rosteriq import brief_dispatcher as bd  # noqa: E402


# ---------------------------------------------------------------------------
# Stubs
# ---------------------------------------------------------------------------

class StubStore:
    """Minimal accountability store stub — returns a fixed event list
    per venue. Matches the shape compose_brief_from_store expects."""

    def __init__(self, events_by_venue):
        self._events = dict(events_by_venue)

    def history(self, venue_id):
        return list(self._events.get(venue_id, []))


def _ev(rid, *, impact, text, status="dismissed", date="2026-04-10"):
    return {
        "id": rid,
        "status": status,
        "impact_estimate_aud": impact,
        "text": text,
        "recorded_at": f"{date}T19:00:00Z",
        "responded_at": f"{date}T19:30:00Z",
        "source": "wage_pulse",
        "priority": "high",
    }


class ExplodingSink:
    """Sink that raises inside send() — used to prove dispatch_brief
    catches uncaught exceptions so one broken sink never blacks out
    the others."""

    name = "exploding"

    def send(self, **kwargs):
        raise RuntimeError("kaboom")


class CountingSink:
    """A sink that both returns ok AND tracks call count — lets tests
    assert that dispatch_all fans out correctly across venues."""

    name = "counter"

    def __init__(self):
        self.calls = []

    def send(self, *, venue_id, brief, text_body):
        self.calls.append(venue_id)
        return {"status": "ok", "detail": f"count={len(self.calls)}"}


# ---------------------------------------------------------------------------
# MemorySink
# ---------------------------------------------------------------------------

def test_memory_sink_collects_brief_payloads():
    sink = bd.MemorySink()
    store = StubStore({
        "venue_a": [_ev("a1", impact=500, text="Cut bar staff")],
    })
    res = bd.dispatch_brief(
        "venue_a",
        target_date="2026-04-10",
        sinks=[sink],
        store=store,
    )
    assert len(sink.delivered) == 1
    assert sink.delivered[0]["venue_id"] == "venue_a"
    assert sink.delivered[0]["brief"]["rollup"]["dismissed"] == 1
    assert "Cut bar staff" in sink.delivered[0]["text_body"]
    assert res["delivered"][0]["status"] == "ok"
    assert res["delivered"][0]["sink"] == "memory"


# ---------------------------------------------------------------------------
# FileSink
# ---------------------------------------------------------------------------

def test_file_sink_writes_text_and_json_to_directory():
    tmp = tempfile.mkdtemp(prefix="rq_brief_test_")
    try:
        sink = bd.FileSink(tmp)
        store = StubStore({
            "venue_x": [_ev("x1", impact=400, text="Send 1 home")],
        })
        bd.dispatch_brief(
            "venue_x",
            target_date="2026-04-10",
            sinks=[sink],
            store=store,
        )
        txt_path = os.path.join(tmp, "morning_brief_venue_x_2026-04-10.txt")
        json_path = os.path.join(tmp, "morning_brief_venue_x_2026-04-10.json")
        assert os.path.exists(txt_path), f"missing {txt_path}"
        assert os.path.exists(json_path), f"missing {json_path}"
        text = open(txt_path).read()
        assert "Send 1 home" in text
        data = json.load(open(json_path))
        assert data["rollup"]["dismissed"] == 1
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def test_file_sink_slugs_venue_ids_with_unsafe_chars():
    tmp = tempfile.mkdtemp(prefix="rq_brief_test_")
    try:
        sink = bd.FileSink(tmp)
        store = StubStore({
            "Mojo's Bar!": [_ev("a", impact=100, text="rec")],
        })
        bd.dispatch_brief(
            "Mojo's Bar!",
            target_date="2026-04-10",
            sinks=[sink],
            store=store,
        )
        # Slug should replace apostrophe, space, exclamation.
        expected = os.path.join(tmp, "morning_brief_mojo_s_bar__2026-04-10.txt")
        assert os.path.exists(expected), f"expected {expected} to exist"
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def test_file_sink_returns_error_detail_on_bad_dir():
    # /dev/null is not a directory on Linux/macOS — file sink should
    # report the error without raising.
    sink = bd.FileSink("/dev/null/not-a-dir")
    store = StubStore({"v": [_ev("a", impact=100, text="rec")]})
    res = bd.dispatch_brief(
        "v", target_date="2026-04-10", sinks=[sink], store=store
    )
    assert res["delivered"][0]["status"] == "error"


# ---------------------------------------------------------------------------
# Registry + sink resolution
# ---------------------------------------------------------------------------

def test_dispatch_brief_uses_registry_sinks_when_sinks_arg_omitted():
    bd.clear_registry()
    bd.clear_sinks()

    mem = bd.MemorySink()
    bd.register_sink(mem)
    bd.register_venue("venue_reg", label="Mojo's", sinks=["memory"])

    store = StubStore({
        "venue_reg": [_ev("a", impact=200, text="rec for registry")],
    })
    res = bd.dispatch_brief(
        "venue_reg", target_date="2026-04-10", store=store
    )
    assert len(mem.delivered) == 1
    assert mem.delivered[0]["venue_id"] == "venue_reg"
    assert res["brief"]["venue_label"] == "Mojo's"

    bd.clear_registry()
    bd.clear_sinks()


def test_dispatch_brief_silently_skips_unknown_sink_names():
    bd.clear_registry()
    bd.clear_sinks()

    mem = bd.MemorySink()
    bd.register_sink(mem)
    bd.register_venue("v", sinks=["memory", "bogus_sink"])

    store = StubStore({"v": [_ev("a", impact=100, text="rec")]})
    res = bd.dispatch_brief("v", target_date="2026-04-10", store=store)

    # Only the registered sink should have delivered
    assert len(res["delivered"]) == 1
    assert res["delivered"][0]["sink"] == "memory"
    assert len(mem.delivered) == 1

    bd.clear_registry()
    bd.clear_sinks()


def test_register_venue_requires_non_empty_id():
    try:
        bd.register_venue("")
    except ValueError:
        return
    raise AssertionError("register_venue('') should raise ValueError")


def test_register_sink_requires_name():
    class NamelessSink:
        name = ""

        def send(self, **k):
            return {"status": "ok"}

    try:
        bd.register_sink(NamelessSink())
    except ValueError:
        return
    raise AssertionError("register_sink with empty name should raise")


# ---------------------------------------------------------------------------
# Robustness
# ---------------------------------------------------------------------------

def test_dispatch_brief_survives_exploding_sink():
    bd.clear_registry()
    bd.clear_sinks()

    boom = ExplodingSink()
    mem = bd.MemorySink()
    store = StubStore({"v": [_ev("a", impact=300, text="rec")]})

    res = bd.dispatch_brief(
        "v", target_date="2026-04-10", sinks=[boom, mem], store=store
    )
    # Exploding sink reported as error
    assert res["delivered"][0]["sink"] == "exploding"
    assert res["delivered"][0]["status"] == "error"
    assert "kaboom" in res["delivered"][0]["detail"]
    # Good sink still delivered
    assert res["delivered"][1]["status"] == "ok"
    assert len(mem.delivered) == 1


# ---------------------------------------------------------------------------
# dispatch_all
# ---------------------------------------------------------------------------

def test_dispatch_all_walks_registry_and_returns_summary():
    bd.clear_registry()
    bd.clear_sinks()

    counter = CountingSink()
    bd.register_sink(counter)
    bd.register_venue("venue_a", label="A", sinks=["counter"])
    bd.register_venue("venue_b", label="B", sinks=["counter"])
    bd.register_venue("venue_c", label="C", sinks=["counter"])

    store = StubStore({
        "venue_a": [_ev("a", impact=100, text="rec a")],
        "venue_b": [_ev("b", impact=200, text="rec b")],
        "venue_c": [_ev("c", impact=300, text="rec c")],
    })

    out = bd.dispatch_all(target_date="2026-04-10", store=store)
    assert out["summary"]["venues"] == 3
    assert out["summary"]["deliveries_ok"] == 3
    assert out["summary"]["deliveries_error"] == 0
    assert set(counter.calls) == {"venue_a", "venue_b", "venue_c"}

    bd.clear_registry()
    bd.clear_sinks()


def test_dispatch_all_uses_recap_fetcher_when_provided():
    bd.clear_registry()
    bd.clear_sinks()

    mem = bd.MemorySink()
    bd.register_sink(mem)
    bd.register_venue("venue_r", sinks=["memory"])

    recap = {
        "traffic_light": "red",
        "revenue": {"actual": 20_000, "forecast": 22_000, "delta_pct": -0.09},
        "wages": {
            "pct_of_revenue_actual": 0.32,
            "pct_of_revenue_target": 0.28,
            "pct_delta": 0.04,
        },
        "headcount": {"peak": 60},
    }
    fetched = {}

    def fetcher(vid):
        fetched[vid] = True
        return recap

    store = StubStore({
        "venue_r": [_ev("a", impact=500, text="cut staff")],
    })
    out = bd.dispatch_all(
        target_date="2026-04-10",
        recap_fetcher=fetcher,
        store=store,
    )
    assert "venue_r" in fetched
    delivered_brief = out["results"][0]["brief"]
    assert delivered_brief["traffic_light"] == "red"
    assert delivered_brief["recap_context"]["wage_pct_actual"] == 0.32

    bd.clear_registry()
    bd.clear_sinks()


def test_dispatch_all_tolerates_fetcher_raising():
    bd.clear_registry()
    bd.clear_sinks()

    mem = bd.MemorySink()
    bd.register_sink(mem)
    bd.register_venue("venue_f", sinks=["memory"])

    def fetcher(vid):
        raise RuntimeError("fetcher down")

    store = StubStore({"venue_f": [_ev("a", impact=100, text="rec")]})
    out = bd.dispatch_all(
        target_date="2026-04-10",
        recap_fetcher=fetcher,
        store=store,
    )
    # Should still deliver, just without recap context
    assert out["summary"]["deliveries_ok"] == 1
    assert out["results"][0]["brief"]["recap_context"] == {}

    bd.clear_registry()
    bd.clear_sinks()


def test_dispatch_all_empty_registry_is_a_noop():
    bd.clear_registry()
    bd.clear_sinks()
    out = bd.dispatch_all()
    assert out["summary"]["venues"] == 0
    assert out["summary"]["deliveries_ok"] == 0
    assert out["results"] == []


# ---------------------------------------------------------------------------
# StdoutSink (smoke test)
# ---------------------------------------------------------------------------

def test_stdout_sink_returns_ok():
    import io
    sink = bd.StdoutSink()
    # Swap stdout so the test doesn't pollute real output
    old = sys.stdout
    sys.stdout = io.StringIO()
    try:
        res = sink.send(
            venue_id="v",
            brief={"date": "2026-04-10"},
            text_body="hello brief",
        )
        out = sys.stdout.getvalue()
    finally:
        sys.stdout = old
    assert res["status"] == "ok"
    assert "hello brief" in out


# ---------------------------------------------------------------------------
# Weekly digest dispatch (Moment 14-follow-on 1)
# ---------------------------------------------------------------------------

def _weekly_ev(rid, *, impact, status="dismissed", suffix="over_wage_high", day=10):
    """Build an event whose rec_id embeds an action suffix so the
    weekly digest pattern detector picks it up."""
    return {
        "id": rid,
        "rec_id": f"rec_pulse_vwk_2026-04-{day:02d}_{suffix}",
        "status": status,
        "impact_estimate_aud": impact,
        "text": f"rec {rid}",
        "recorded_at": f"2026-04-{day:02d}T19:00:00Z",
        "responded_at": f"2026-04-{day:02d}T19:30:00Z",
        "source": "wage_pulse",
        "priority": "high",
    }


def test_dispatch_weekly_digest_delivers_digest_to_memory_sink():
    bd.clear_registry()
    bd.clear_sinks()

    mem = bd.MemorySink()
    store = StubStore({
        "vwk": [
            _weekly_ev("a", impact=400, status="dismissed", day=8),
            _weekly_ev("b", impact=200, status="dismissed", day=9),
            _weekly_ev("c", impact=300, status="accepted",  day=10),
        ],
    })
    res = bd.dispatch_weekly_digest(
        "vwk",
        week_ending="2026-04-11",
        window_days=7,
        sinks=[mem],
        store=store,
    )
    assert res["skipped"] is False
    assert len(res["delivered"]) == 1
    assert res["delivered"][0]["status"] == "ok"
    assert len(mem.delivered) == 1
    # The payload stamped onto the sink is a weekly digest, not a brief
    payload = mem.delivered[0]["brief"]
    assert payload["_kind"] == "weekly_digest"
    assert "week_start" in payload and "week_end" in payload
    assert payload["rollup"]["dismissed"] == 2
    assert "WEEKLY DIGEST" in mem.delivered[0]["text_body"]


def test_dispatch_weekly_digest_uses_registry_sinks_when_sinks_arg_omitted():
    bd.clear_registry()
    bd.clear_sinks()

    mem = bd.MemorySink()
    bd.register_sink(mem)
    bd.register_venue("vwk", label="Mojo's", sinks=["memory"])

    store = StubStore({
        "vwk": [_weekly_ev("a", impact=500, day=10)],
    })
    res = bd.dispatch_weekly_digest(
        "vwk", week_ending="2026-04-11", store=store
    )
    assert len(mem.delivered) == 1
    assert mem.delivered[0]["brief"]["venue_label"] == "Mojo's"
    assert res["delivered"][0]["status"] == "ok"

    bd.clear_registry()
    bd.clear_sinks()


def test_dispatch_weekly_digest_only_when_should_send_skips_quiet_weeks():
    bd.clear_registry()
    bd.clear_sinks()

    mem = bd.MemorySink()
    # Empty store → zero events → should_send = False
    store = StubStore({"quiet_venue": []})
    res = bd.dispatch_weekly_digest(
        "quiet_venue",
        week_ending="2026-04-11",
        sinks=[mem],
        store=store,
        only_when_should_send=True,
    )
    assert res["skipped"] is True
    assert res["delivered"] == []
    assert len(mem.delivered) == 0
    # The digest is still composed (so the caller can inspect it) —
    # we just didn't deliver to any sink.
    assert res["digest"]["should_send"] is False


def test_dispatch_weekly_digest_only_when_should_send_still_sends_loud_weeks():
    bd.clear_registry()
    bd.clear_sinks()

    mem = bd.MemorySink()
    store = StubStore({
        "loud_venue": [
            _weekly_ev("a", impact=1200, status="dismissed", day=8),
        ],
    })
    res = bd.dispatch_weekly_digest(
        "loud_venue",
        week_ending="2026-04-11",
        sinks=[mem],
        store=store,
        only_when_should_send=True,
    )
    assert res["skipped"] is False
    assert len(mem.delivered) == 1


def test_dispatch_weekly_digest_survives_exploding_sink():
    bd.clear_registry()
    bd.clear_sinks()

    boom = ExplodingSink()
    mem = bd.MemorySink()
    store = StubStore({"vwk": [_weekly_ev("a", impact=100, day=10)]})

    res = bd.dispatch_weekly_digest(
        "vwk",
        week_ending="2026-04-11",
        sinks=[boom, mem],
        store=store,
    )
    assert res["delivered"][0]["sink"] == "exploding"
    assert res["delivered"][0]["status"] == "error"
    assert "kaboom" in res["delivered"][0]["detail"]
    assert res["delivered"][1]["status"] == "ok"
    assert len(mem.delivered) == 1


def test_dispatch_weekly_digest_does_not_mutate_original_digest_dict():
    """Stamping ``_kind`` onto the sink payload must not leak back
    into the digest dict we return to the caller."""
    bd.clear_registry()
    bd.clear_sinks()

    mem = bd.MemorySink()
    store = StubStore({"vwk": [_weekly_ev("a", impact=200, day=9)]})
    res = bd.dispatch_weekly_digest(
        "vwk", week_ending="2026-04-11", sinks=[mem], store=store
    )
    assert "_kind" not in res["digest"]
    # Sink copy has the marker, caller's copy does not
    assert mem.delivered[0]["brief"]["_kind"] == "weekly_digest"


def test_file_sink_writes_weekly_digest_with_distinct_filename_prefix():
    bd.clear_registry()
    bd.clear_sinks()

    tmp = tempfile.mkdtemp(prefix="rq_wkly_file_")
    try:
        file_sink = bd.FileSink(tmp)
        store = StubStore({"vwk": [_weekly_ev("a", impact=300, day=10)]})
        bd.dispatch_weekly_digest(
            "vwk",
            week_ending="2026-04-11",
            sinks=[file_sink],
            store=store,
        )
        # The weekly file uses the kind prefix and the digest's date
        # (week_end), which for a 7-day window ending 2026-04-11 is
        # 2026-04-11 itself.
        txt_path = os.path.join(tmp, "weekly_digest_vwk_2026-04-11.txt")
        json_path = os.path.join(tmp, "weekly_digest_vwk_2026-04-11.json")
        assert os.path.exists(txt_path), f"missing {txt_path}"
        assert os.path.exists(json_path), f"missing {json_path}"
        text = open(txt_path).read()
        assert "WEEKLY DIGEST" in text
        data = json.load(open(json_path))
        assert data["_kind"] == "weekly_digest"
        assert data["rollup"]["dismissed"] == 1
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def test_file_sink_weekly_digest_does_not_clobber_morning_brief():
    bd.clear_registry()
    bd.clear_sinks()

    tmp = tempfile.mkdtemp(prefix="rq_coexist_")
    try:
        file_sink = bd.FileSink(tmp)
        store = StubStore({
            "vwk": [_weekly_ev("a", impact=400, day=10)],
        })
        # Daily brief
        bd.dispatch_brief(
            "vwk",
            target_date="2026-04-11",
            sinks=[file_sink],
            store=store,
        )
        # Weekly digest, same venue, same date stamp
        bd.dispatch_weekly_digest(
            "vwk",
            week_ending="2026-04-11",
            sinks=[file_sink],
            store=store,
        )
        daily = os.path.join(tmp, "morning_brief_vwk_2026-04-11.txt")
        weekly = os.path.join(tmp, "weekly_digest_vwk_2026-04-11.txt")
        assert os.path.exists(daily)
        assert os.path.exists(weekly)
        # Distinct content
        assert open(daily).read() != open(weekly).read()
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def test_dispatch_all_weekly_digests_walks_registry():
    bd.clear_registry()
    bd.clear_sinks()

    counter = CountingSink()
    bd.register_sink(counter)
    bd.register_venue("a", label="A", sinks=["counter"])
    bd.register_venue("b", label="B", sinks=["counter"])
    bd.register_venue("c", label="C", sinks=["counter"])

    store = StubStore({
        "a": [_weekly_ev("a1", impact=100, day=8)],
        "b": [_weekly_ev("b1", impact=200, day=9)],
        "c": [_weekly_ev("c1", impact=300, day=10)],
    })
    out = bd.dispatch_all_weekly_digests(
        week_ending="2026-04-11", store=store
    )
    assert out["summary"]["venues"] == 3
    assert out["summary"]["deliveries_ok"] == 3
    assert out["summary"]["deliveries_error"] == 0
    assert out["summary"]["kind"] == "weekly_digest"
    assert set(counter.calls) == {"a", "b", "c"}

    bd.clear_registry()
    bd.clear_sinks()


def test_dispatch_all_weekly_digests_counts_skipped_quiet_venues():
    bd.clear_registry()
    bd.clear_sinks()

    counter = CountingSink()
    bd.register_sink(counter)
    bd.register_venue("quiet", sinks=["counter"])
    bd.register_venue("loud", sinks=["counter"])

    store = StubStore({
        "quiet": [],
        "loud":  [_weekly_ev("l1", impact=500, day=9)],
    })
    out = bd.dispatch_all_weekly_digests(
        week_ending="2026-04-11",
        store=store,
        only_when_should_send=True,
    )
    assert out["summary"]["venues"] == 2
    assert out["summary"]["deliveries_ok"] == 1
    assert out["summary"]["skipped"] == 1
    assert counter.calls == ["loud"]

    bd.clear_registry()
    bd.clear_sinks()


def test_dispatch_all_weekly_digests_empty_registry_is_a_noop():
    bd.clear_registry()
    bd.clear_sinks()
    out = bd.dispatch_all_weekly_digests()
    assert out["summary"]["venues"] == 0
    assert out["summary"]["deliveries_ok"] == 0
    assert out["summary"]["skipped"] == 0
    assert out["results"] == []


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
