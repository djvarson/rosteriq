"""Tests for rosteriq.scheduled_jobs.

The scheduler is tested without threads or real sleep — ``tick`` takes
an injectable ``now_mono`` and the wall-clock ``now`` is driven through
a fake ``now_fn``. The sweep test writes to a temp file in the test
directory and inspects the rewritten contents.
"""
from __future__ import annotations

import json
import os
import sys
import tempfile
import threading
import time
import unittest
from datetime import datetime, timezone
from typing import Any, Dict, List

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from rosteriq import scheduled_jobs as sj
from rosteriq.tanda_writeback import ShiftDelta


# ---------------------------------------------------------------------------
# Fake clock + fake dispatcher + fake sink helpers
# ---------------------------------------------------------------------------

class FakeClock:
    """Drives both wall time and a fake monotonic cursor."""
    def __init__(self, start_wall: datetime, start_mono: float = 0.0):
        self._wall = start_wall
        self._mono = start_mono

    def now(self) -> datetime:
        return self._wall

    def mono(self) -> float:
        return self._mono

    def advance(self, seconds: float, *, wall_delta_minutes: float = 0.0) -> None:
        self._mono += seconds
        if wall_delta_minutes:
            from datetime import timedelta
            self._wall = self._wall + timedelta(minutes=wall_delta_minutes)

    def set_wall(self, wall: datetime) -> None:
        self._wall = wall


class RecordingSink:
    def __init__(self, name: str, *, fail_kinds=None, fail_count=0):
        self.name = name
        self.calls: List[Dict[str, Any]] = []
        self.fail_kinds = set(fail_kinds or [])
        # Number of times to fail before starting to succeed — lets us
        # test "fixed on retry" behaviour.
        self._remaining_failures = fail_count

    def apply(self, venue_id: str, delta) -> Dict[str, Any]:
        self.calls.append({"venue_id": venue_id, "delta": delta.to_dict()})
        if delta.kind in self.fail_kinds:
            return {"sink": self.name, "ok": False, "status_code": 500, "error": "kind_fail"}
        if self._remaining_failures > 0:
            self._remaining_failures -= 1
            return {"sink": self.name, "ok": False, "status_code": 500, "error": "transient"}
        return {"sink": self.name, "ok": True, "status_code": 200}


# ---------------------------------------------------------------------------
# Scheduler core
# ---------------------------------------------------------------------------

class SchedulerTickTest(unittest.TestCase):
    def setUp(self):
        self.clock = FakeClock(datetime(2026, 4, 13, 10, 0, tzinfo=timezone.utc))
        self.sched = sj.Scheduler(now_fn=self.clock.now)
        # Pin the scheduler's notion of "started" to mono=0.
        self.sched._started_mono = 0.0

    def test_job_does_not_fire_before_interval(self):
        fires = []
        def fn(now):
            fires.append(now)
            return {"ran": True}
        self.sched.add(sj.ScheduledJob(name="t", interval_s=60.0, fn=fn))

        # 30s in — too early.
        out = self.sched.tick(now_mono=30.0)
        self.assertEqual(len(fires), 0)
        self.assertEqual(out, [])

    def test_job_fires_when_interval_elapsed(self):
        fires = []
        def fn(now):
            fires.append(now)
            return {"ran": True}
        self.sched.add(sj.ScheduledJob(name="t", interval_s=60.0, fn=fn))

        out = self.sched.tick(now_mono=61.0)
        self.assertEqual(len(fires), 1)
        self.assertEqual(len(out), 1)
        self.assertTrue(out[0]["ok"])
        self.assertEqual(out[0]["job"], "t")

        # A second tick at the same wall time should NOT re-fire.
        out2 = self.sched.tick(now_mono=62.0)
        self.assertEqual(out2, [])
        self.assertEqual(len(fires), 1)

    def test_job_fires_again_after_another_interval(self):
        fires = []
        def fn(now):
            fires.append(now)
            return {"ran": True}
        self.sched.add(sj.ScheduledJob(name="t", interval_s=60.0, fn=fn))

        self.sched.tick(now_mono=61.0)
        # Only 30s after last run — not yet.
        self.sched.tick(now_mono=91.0)
        self.assertEqual(len(fires), 1)
        # 61s after last run — should fire.
        self.sched.tick(now_mono=122.0)
        self.assertEqual(len(fires), 2)

    def test_gate_blocks_firing_without_resetting_interval(self):
        """A gate that returns False should skip this tick but let the
        job fire on the next tick after another interval elapses —
        actually the way the gate works, as soon as it returns True
        and interval has elapsed we fire."""
        gate_return = [False]
        fires = []
        def fn(now):
            fires.append(now)
            return {"ran": True}
        job = sj.ScheduledJob(
            name="gated",
            interval_s=60.0,
            fn=fn,
            should_run=lambda now: gate_return[0],
        )
        self.sched.add(job)

        # Interval elapsed, but gate says no.
        self.sched.tick(now_mono=61.0)
        self.assertEqual(len(fires), 0)
        self.assertIsNone(job.last_run_ts)  # NOT stamped

        # Gate flips to True, next tick fires.
        gate_return[0] = True
        self.sched.tick(now_mono=62.0)
        self.assertEqual(len(fires), 1)
        self.assertIsNotNone(job.last_run_ts)

    def test_job_error_isolation(self):
        def good(now):
            return {"ok": True}
        def bad(now):
            raise RuntimeError("boom")
        self.sched.add(sj.ScheduledJob(name="good", interval_s=60.0, fn=good))
        self.sched.add(sj.ScheduledJob(name="bad", interval_s=60.0, fn=bad))

        out = self.sched.tick(now_mono=61.0)
        self.assertEqual(len(out), 2)
        statuses = {o["job"]: o for o in out}
        self.assertTrue(statuses["good"]["ok"])
        self.assertFalse(statuses["bad"]["ok"])
        self.assertIn("boom", statuses["bad"]["error"])

        # Both jobs' timers are reset — next fire waits another interval.
        out2 = self.sched.tick(now_mono=90.0)
        self.assertEqual(out2, [])

        # Bad job still retries on next interval (not taken out of rotation).
        out3 = self.sched.tick(now_mono=122.0)
        self.assertEqual(len(out3), 2)

    def test_gate_error_does_not_kill_scheduler(self):
        def fn(now):
            return {"ran": True}
        def bad_gate(now):
            raise ValueError("gate exploded")
        job = sj.ScheduledJob(
            name="t",
            interval_s=60.0,
            fn=fn,
            should_run=bad_gate,
        )
        self.sched.add(job)
        out = self.sched.tick(now_mono=61.0)
        self.assertEqual(len(out), 1)
        self.assertFalse(out[0]["ok"])
        self.assertIn("gate_error", out[0]["error"])
        # Gate errors DO stamp last_run_ts — otherwise a permanently
        # broken gate would lock the scheduler in a hot loop.
        self.assertIsNotNone(job.last_run_ts)

    def test_non_dict_result_wrapped(self):
        def fn(now):
            return "howdy"
        self.sched.add(sj.ScheduledJob(name="t", interval_s=60.0, fn=fn))
        out = self.sched.tick(now_mono=61.0)
        self.assertEqual(out[0]["result"], {"raw": "howdy"})

    def test_status_roundtrip(self):
        def fn(now):
            return {"ran": True}
        self.sched.add(sj.ScheduledJob(name="t", interval_s=60.0, fn=fn))
        self.sched.tick(now_mono=61.0)
        rows = self.sched.status()
        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row["name"], "t")
        self.assertEqual(row["runs"], 1)
        self.assertEqual(row["errors"], 0)
        self.assertEqual(row["last_result"], {"ran": True})
        self.assertIsNotNone(row["last_run_ts"])


# ---------------------------------------------------------------------------
# Monday morning gate
# ---------------------------------------------------------------------------

class MondayMorningGateTest(unittest.TestCase):
    def test_monday_morning_inside_window(self):
        gate = sj.monday_morning_gate(hour=8, window_hours=2)
        # 2026-04-13 is a Monday (weekday==0).
        monday_8am = datetime(2026, 4, 13, 8, 15, tzinfo=timezone.utc)
        self.assertTrue(gate(monday_8am))
        monday_9am = datetime(2026, 4, 13, 9, 59, tzinfo=timezone.utc)
        self.assertTrue(gate(monday_9am))

    def test_monday_morning_outside_window(self):
        gate = sj.monday_morning_gate(hour=8, window_hours=2)
        monday_7am = datetime(2026, 4, 13, 7, 59, tzinfo=timezone.utc)
        self.assertFalse(gate(monday_7am))
        monday_10am = datetime(2026, 4, 13, 10, 0, tzinfo=timezone.utc)
        self.assertFalse(gate(monday_10am))

    def test_not_monday(self):
        gate = sj.monday_morning_gate(hour=8, window_hours=2)
        # 2026-04-14 is a Tuesday.
        tuesday_8am = datetime(2026, 4, 14, 8, 15, tzinfo=timezone.utc)
        self.assertFalse(gate(tuesday_8am))
        sunday_8am = datetime(2026, 4, 12, 8, 15, tzinfo=timezone.utc)
        self.assertFalse(gate(sunday_8am))


# ---------------------------------------------------------------------------
# Weekly digest job factory
# ---------------------------------------------------------------------------

class WeeklyDigestJobTest(unittest.TestCase):
    def test_dispatcher_called_with_flags(self):
        calls = []
        def fake_dispatch(**kwargs):
            calls.append(kwargs)
            return {"venues": 3, "deliveries_ok": 3, "skipped": 0, "kind": "weekly_digest"}
        job = sj.make_weekly_digest_job(
            dispatcher=fake_dispatch,
            should_run=lambda now: True,  # always fire
            interval_s=10.0,
            window_days=14,
            only_when_should_send=True,
        )
        sched = sj.Scheduler(now_fn=lambda: datetime(2026, 4, 13, 8, 30, tzinfo=timezone.utc))
        sched._started_mono = 0.0
        sched.add(job)
        out = sched.tick(now_mono=11.0)
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0]["only_when_should_send"], True)
        self.assertEqual(calls[0]["window_days"], 14)
        self.assertEqual(len(out), 1)
        self.assertTrue(out[0]["ok"])
        self.assertIn("summary", out[0]["result"])

    def test_gated_to_monday_morning_only(self):
        calls = []
        def fake_dispatch(**kwargs):
            calls.append(kwargs)
            return {"venues": 0}

        clock = FakeClock(datetime(2026, 4, 14, 8, 30, tzinfo=timezone.utc))  # Tuesday
        sched = sj.Scheduler(now_fn=clock.now)
        sched._started_mono = 0.0
        # Use default monday_morning gate.
        job = sj.make_weekly_digest_job(
            dispatcher=fake_dispatch,
            interval_s=10.0,
        )
        sched.add(job)

        # Tuesday 08:30 — gate blocks.
        sched.tick(now_mono=11.0)
        self.assertEqual(len(calls), 0)

        # Jump to Monday 08:30.
        clock.set_wall(datetime(2026, 4, 20, 8, 30, tzinfo=timezone.utc))
        sched.tick(now_mono=22.0)
        self.assertEqual(len(calls), 1)


# ---------------------------------------------------------------------------
# Tanda dead-letter sweep
# ---------------------------------------------------------------------------

def _dl_entry(*, venue_id: str, kind: str = "cut_staff", source_rec_id: str = "rec_x"):
    """Build a dead-letter file line in the shape TandaApiSink writes."""
    return json.dumps({
        "ts": "2026-04-12T10:00:00Z",
        "url": "https://tanda.example/writeback",
        "venue_id": venue_id,
        "payload": {
            "venue_id": venue_id,
            "rec_id": source_rec_id,
            "delta": {
                "kind": kind,
                "count": 2,
                "timing_hint": "immediate",
                "reason": "cut two",
                "impact_estimate_aud": 120.0,
                "priority": "high",
                "source_rec_id": source_rec_id,
                "metadata": {},
            },
        },
        "attempts": [
            {"attempt": 1, "ok": False, "status_code": 503, "error": "HTTP 503"},
        ],
    })


class TandaDeadLetterSweepTest(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix="rq_sweep_")
        self.path = os.path.join(self.tmpdir, "dead_letter.jsonl")

    def tearDown(self):
        import shutil
        try:
            shutil.rmtree(self.tmpdir)
        except Exception:
            pass

    def _write(self, *entries):
        with open(self.path, "w", encoding="utf-8") as f:
            for e in entries:
                f.write(e + "\n")

    def test_noop_when_file_missing(self):
        sink = RecordingSink("tanda_api")
        summary = sj.sweep_tanda_dead_letter(self.path, sinks=[sink])
        self.assertEqual(summary["read"], 0)
        self.assertEqual(summary["resolved"], 0)
        self.assertEqual(summary["reason"], "no_file")
        self.assertEqual(sink.calls, [])

    def test_noop_when_no_sinks(self):
        self._write(_dl_entry(venue_id="v1"))
        summary = sj.sweep_tanda_dead_letter(self.path, sinks=[])
        self.assertEqual(summary["reason"], "no_sinks")
        # File should be untouched.
        with open(self.path) as f:
            self.assertEqual(len(f.readlines()), 1)

    def test_resolves_all_when_sinks_succeed(self):
        self._write(
            _dl_entry(venue_id="v1", source_rec_id="rec_a"),
            _dl_entry(venue_id="v2", source_rec_id="rec_b"),
        )
        sink = RecordingSink("tanda_api")
        summary = sj.sweep_tanda_dead_letter(self.path, sinks=[sink])
        self.assertEqual(summary["read"], 2)
        self.assertEqual(summary["retried"], 2)
        self.assertEqual(summary["resolved"], 2)
        self.assertEqual(summary["still_failing"], 0)
        self.assertEqual(len(sink.calls), 2)
        # File should be gone or empty.
        self.assertFalse(os.path.exists(self.path))

    def test_writes_back_still_failing_entries(self):
        self._write(
            _dl_entry(venue_id="v1", source_rec_id="rec_ok"),
            _dl_entry(venue_id="v2", kind="send_home", source_rec_id="rec_still_bad"),
        )
        # Sink fails any "send_home" delta.
        sink = RecordingSink("tanda_api", fail_kinds={"send_home"})
        summary = sj.sweep_tanda_dead_letter(self.path, sinks=[sink])
        self.assertEqual(summary["read"], 2)
        self.assertEqual(summary["resolved"], 1)
        self.assertEqual(summary["still_failing"], 1)
        # File should contain exactly one entry — the failing one.
        with open(self.path) as f:
            lines = [l for l in f.read().splitlines() if l.strip()]
        self.assertEqual(len(lines), 1)
        rehydrated = json.loads(lines[0])
        self.assertEqual(rehydrated["venue_id"], "v2")
        self.assertIn("swept_at", rehydrated)
        self.assertIn("sweep_results", rehydrated)
        self.assertEqual(len(rehydrated["sweep_results"]), 1)
        self.assertFalse(rehydrated["sweep_results"][0]["ok"])

    def test_malformed_entries_dropped(self):
        with open(self.path, "w", encoding="utf-8") as f:
            f.write("not valid json\n")
            f.write(_dl_entry(venue_id="v1") + "\n")
            f.write('{"not": "shaped right"}\n')
        sink = RecordingSink("tanda_api")
        summary = sj.sweep_tanda_dead_letter(self.path, sinks=[sink])
        self.assertEqual(summary["dropped_malformed"], 2)
        self.assertEqual(summary["resolved"], 1)
        self.assertEqual(summary["still_failing"], 0)
        self.assertEqual(len(sink.calls), 1)

    def test_cleans_up_stale_sweeping_file(self):
        stale = self.path + ".sweeping"
        with open(stale, "w", encoding="utf-8") as f:
            f.write("leftover\n")
        self._write(_dl_entry(venue_id="v1"))
        sink = RecordingSink("tanda_api")
        sj.sweep_tanda_dead_letter(self.path, sinks=[sink])
        self.assertFalse(os.path.exists(stale))

    def test_journal_sinks_filtered_out_of_default_sweep(self):
        """When ``sinks`` is None, we pull from tanda_writeback._SINKS
        but skip anything that looks like a journal — a journal retry
        would just re-append, not fix the upstream write."""
        from rosteriq import tanda_writeback as _tw
        # Save and restore the global registry.
        saved = list(_tw._SINKS)
        try:
            _tw._SINKS.clear()

            class DummyJournal:
                name = "journal_file"
                def apply(self, venue_id, delta):
                    return {"ok": True}

            dummy_journal = DummyJournal()
            _tw._SINKS.append(dummy_journal)

            self._write(_dl_entry(venue_id="v1"))
            summary = sj.sweep_tanda_dead_letter(self.path)  # sinks=None
            self.assertEqual(summary["reason"], "no_sinks")
        finally:
            _tw._SINKS.clear()
            _tw._SINKS.extend(saved)

    def test_tanda_api_sinks_used_when_registered(self):
        from rosteriq import tanda_writeback as _tw
        saved = list(_tw._SINKS)
        try:
            _tw._SINKS.clear()
            api_sink = RecordingSink("tanda_api")
            _tw._SINKS.append(api_sink)

            self._write(_dl_entry(venue_id="v1"))
            summary = sj.sweep_tanda_dead_letter(self.path)
            self.assertEqual(summary["resolved"], 1)
            self.assertEqual(len(api_sink.calls), 1)
        finally:
            _tw._SINKS.clear()
            _tw._SINKS.extend(saved)


# ---------------------------------------------------------------------------
# Sweep job factory
# ---------------------------------------------------------------------------

class TandaRetrySweepJobTest(unittest.TestCase):
    def test_factory_runs_sweep(self):
        tmpdir = tempfile.mkdtemp(prefix="rq_sweep_")
        try:
            path = os.path.join(tmpdir, "dl.jsonl")
            with open(path, "w") as f:
                f.write(_dl_entry(venue_id="v1") + "\n")
            sink = RecordingSink("tanda_api")
            job = sj.make_tanda_retry_sweep_job(
                dead_letter_path=path,
                sinks=[sink],
                interval_s=60.0,
            )
            sched = sj.Scheduler(now_fn=lambda: datetime(2026, 4, 13, tzinfo=timezone.utc))
            sched._started_mono = 0.0
            sched.add(job)
            out = sched.tick(now_mono=61.0)
            self.assertEqual(len(out), 1)
            self.assertTrue(out[0]["ok"])
            self.assertEqual(out[0]["result"]["summary"]["resolved"], 1)
            self.assertEqual(len(sink.calls), 1)
        finally:
            import shutil
            shutil.rmtree(tmpdir, ignore_errors=True)


# ---------------------------------------------------------------------------
# run_forever smoke
# ---------------------------------------------------------------------------

class RunForeverSmokeTest(unittest.TestCase):
    """Real threads but a deterministic fake sleep."""

    def test_thread_ticks_until_stopped(self):
        fires = []
        cond = threading.Event()
        def fn(now):
            fires.append(now)
            if len(fires) >= 2:
                cond.set()
            return {"n": len(fires)}
        sched = sj.Scheduler()
        sched._started_mono = time.monotonic() - 100  # fire immediately

        sched.add(sj.ScheduledJob(name="t", interval_s=0.01, fn=fn))
        t = sched.run_forever(poll_interval_s=0.01)
        try:
            self.assertTrue(cond.wait(timeout=2.0),
                            "run_forever did not tick within 2s")
        finally:
            sched.stop(join_timeout_s=1.0)
        self.assertGreaterEqual(len(fires), 2)
        self.assertFalse(t.is_alive())


# ---------------------------------------------------------------------------
# Global scheduler
# ---------------------------------------------------------------------------

class GlobalSchedulerTest(unittest.TestCase):
    def tearDown(self):
        sj.reset_global_scheduler_for_tests()

    def test_singleton_identity(self):
        a = sj.get_global_scheduler()
        b = sj.get_global_scheduler()
        self.assertIs(a, b)

    def test_reset_makes_new(self):
        a = sj.get_global_scheduler()
        sj.reset_global_scheduler_for_tests()
        b = sj.get_global_scheduler()
        self.assertIsNot(a, b)


if __name__ == "__main__":
    unittest.main(verbosity=2)
