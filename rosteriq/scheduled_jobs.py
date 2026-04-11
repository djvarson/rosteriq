"""Scheduled jobs — in-process cron for RosterIQ.

Moment 14 follow-on 4. Two things needed periodic execution:

1. **Weekly digest dispatch** — every Monday morning, walk the venue
   registry and fire the weekly digest for venues that had events in
   the last window. Reuses ``brief_dispatcher.dispatch_all_weekly_digests``
   with ``only_when_should_send=True`` so quiet weeks stay quiet.

2. **Tanda dead-letter sweep** — every few minutes, re-read any
   previously-dead-lettered Tanda writeback entries and retry them
   against the registered sinks. Entries that still fail get appended
   back to the dead-letter file; ones that succeed drop out. The
   writeback's ``Idempotency-Key`` header protects against double-apply
   if the upstream already processed an earlier attempt.

Design principles:

- **Pure stdlib.** Threading + time only. No APScheduler, no Celery,
  no Redis. This module should be testable with zero network and zero
  sleep.
- **Deterministic ticks.** The ``Scheduler`` doesn't sleep itself; it
  exposes a ``tick(now=...)`` method that callers drive. ``run_forever``
  wraps it in a background thread for production but stays out of the
  hot path for tests.
- **Injectable everything.** ``now_fn``, ``sleep_fn``, and the job
  callables are all overridable. Tests substitute a fake clock and a
  fake dispatcher and assert firing behaviour without touching the
  filesystem.
- **Error isolation.** One exploding job must never kill the scheduler
  loop or starve other jobs.

Tests live in ``tests/test_scheduled_jobs.py``.
"""
from __future__ import annotations

import json
import logging
import os
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional

logger = logging.getLogger("rosteriq.scheduled_jobs")


# ---------------------------------------------------------------------------
# Core scheduler
# ---------------------------------------------------------------------------

# A job callable takes the wall-clock "now" and returns an arbitrary result
# dict that the scheduler stores on the job record for inspection.
JobFn = Callable[[datetime], Dict[str, Any]]


@dataclass
class ScheduledJob:
    """A single recurring job.

    ``interval_s`` is the *minimum* gap between firings. The job may fire
    later than that if the scheduler was busy, but never earlier. A job
    with ``last_run_ts=None`` fires on the very first tick past the
    scheduler's start time — we don't fire the instant the scheduler is
    constructed so startup-time spikes don't blast every job at once.
    """
    name: str
    interval_s: float
    fn: JobFn
    last_run_ts: Optional[float] = None
    last_result: Optional[Dict[str, Any]] = None
    last_error: Optional[str] = None
    runs: int = 0
    errors: int = 0
    # Optional gate — if set, return False to skip this tick's firing
    # without resetting the interval timer. Used for "only on Monday
    # morning" style schedules where the main interval is e.g. 1 hour
    # but we only actually fire once per week.
    should_run: Optional[Callable[[datetime], bool]] = None

    def to_status(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "interval_s": self.interval_s,
            "last_run_ts": self.last_run_ts,
            "last_run_iso": (
                datetime.fromtimestamp(self.last_run_ts, tz=timezone.utc)
                .isoformat()
                .replace("+00:00", "Z")
                if self.last_run_ts is not None
                else None
            ),
            "runs": self.runs,
            "errors": self.errors,
            "last_error": self.last_error,
            "last_result": self.last_result,
        }


class Scheduler:
    """Deterministic tick-driven scheduler.

    Not a timer loop on its own — call :meth:`tick` from whatever driver
    you want. :meth:`run_forever` provides a background-thread driver
    that polls at a fixed cadence.
    """

    def __init__(
        self,
        *,
        now_fn: Optional[Callable[[], datetime]] = None,
        logger_: Optional[logging.Logger] = None,
    ) -> None:
        self._jobs: List[ScheduledJob] = []
        self._now_fn = now_fn or (lambda: datetime.now(timezone.utc))
        self._log = logger_ or logger
        self._started_mono: float = time.monotonic()
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None

    # ---- job management --------------------------------------------------

    def add(self, job: ScheduledJob) -> None:
        self._jobs.append(job)

    def jobs(self) -> List[ScheduledJob]:
        return list(self._jobs)

    def get(self, name: str) -> Optional[ScheduledJob]:
        for j in self._jobs:
            if j.name == name:
                return j
        return None

    def status(self) -> List[Dict[str, Any]]:
        return [j.to_status() for j in self._jobs]

    # ---- firing ----------------------------------------------------------

    def tick(self, *, now_mono: Optional[float] = None) -> List[Dict[str, Any]]:
        """Fire any jobs that are due. Returns a list of fire summaries.

        ``now_mono`` lets tests drive a fake monotonic clock. Production
        callers should pass ``None`` to use ``time.monotonic()``.
        """
        if now_mono is None:
            now_mono = time.monotonic()
        wall_now = self._now_fn()
        fired: List[Dict[str, Any]] = []

        for job in self._jobs:
            # First-run case: we consider the job as having "started"
            # at scheduler construction. So a job with interval_s=60
            # fires 60 seconds after the scheduler started — not the
            # instant we tick in.
            baseline = job.last_run_ts if job.last_run_ts is not None else self._started_mono
            if (now_mono - baseline) < job.interval_s:
                continue

            # Optional gate (e.g. only run on Monday morning).
            if job.should_run is not None:
                try:
                    if not bool(job.should_run(wall_now)):
                        # Don't reset the interval — try again on the
                        # next tick after `interval_s` elapses.
                        continue
                except Exception as exc:
                    self._log.exception(
                        "scheduled_jobs: should_run gate raised for %s", job.name
                    )
                    job.errors += 1
                    job.last_error = f"gate_error: {exc}"
                    job.last_run_ts = now_mono
                    fired.append({
                        "job": job.name,
                        "ok": False,
                        "error": job.last_error,
                        "gate": True,
                    })
                    continue

            try:
                result = job.fn(wall_now)
                if not isinstance(result, dict):
                    result = {"raw": result}
                job.last_result = result
                job.last_error = None
                job.runs += 1
                fired.append({"job": job.name, "ok": True, "result": result})
            except Exception as exc:
                self._log.exception(
                    "scheduled_jobs: job %s raised", job.name
                )
                job.errors += 1
                job.last_error = str(exc)
                fired.append({"job": job.name, "ok": False, "error": str(exc)})
            finally:
                job.last_run_ts = now_mono

        return fired

    # ---- background driver ----------------------------------------------

    def run_forever(
        self,
        *,
        poll_interval_s: float = 30.0,
        sleep_fn: Optional[Callable[[float], None]] = None,
    ) -> threading.Thread:
        """Start a daemon thread that ticks at ``poll_interval_s``.

        Returns the thread so callers can join or inspect it. The
        scheduler stops when :meth:`stop` is called. Safe to call twice;
        subsequent calls are a noop if a thread is already running.
        """
        if self._thread is not None and self._thread.is_alive():
            return self._thread

        sleep = sleep_fn or time.sleep
        self._stop_event.clear()

        def _loop() -> None:
            while not self._stop_event.is_set():
                try:
                    self.tick()
                except Exception:
                    self._log.exception("scheduled_jobs: tick loop crashed")
                # Break sleep into small chunks so stop() is prompt.
                slept = 0.0
                step = min(1.0, poll_interval_s)
                while slept < poll_interval_s and not self._stop_event.is_set():
                    sleep(step)
                    slept += step

        thread = threading.Thread(
            target=_loop,
            name="rosteriq-scheduler",
            daemon=True,
        )
        thread.start()
        self._thread = thread
        return thread

    def stop(self, *, join_timeout_s: float = 2.0) -> None:
        self._stop_event.set()
        if self._thread is not None and self._thread.is_alive():
            self._thread.join(timeout=join_timeout_s)


# ---------------------------------------------------------------------------
# Weekly digest gate
# ---------------------------------------------------------------------------

def monday_morning_gate(
    *,
    hour: int = 8,
    window_hours: int = 2,
) -> Callable[[datetime], bool]:
    """Return a ``should_run`` gate that fires on Monday morning only.

    ``hour`` and ``window_hours`` define a local-time firing window.
    Default is 08:00..09:59 — two hours wide so a ticker that runs
    hourly doesn't miss the window if the first tick lands at 08:30.

    We use the wall-clock ``now`` passed to the gate rather than
    ``datetime.now()`` so tests can drive the clock. Monday is
    ``weekday() == 0`` in Python's datetime.
    """
    def _gate(now: datetime) -> bool:
        try:
            if now.weekday() != 0:
                return False
            h = now.hour
            return hour <= h < (hour + window_hours)
        except Exception:
            return False
    return _gate


def make_weekly_digest_job(
    *,
    name: str = "weekly_digest",
    interval_s: float = 3600.0,
    dispatcher: Optional[Callable[..., Dict[str, Any]]] = None,
    should_run: Optional[Callable[[datetime], bool]] = None,
    only_when_should_send: bool = True,
    window_days: int = 7,
) -> ScheduledJob:
    """Build a scheduled job that fires the weekly digest dispatcher.

    The dispatcher is injectable so tests don't need a real venue
    registry or sinks. In production, callers should leave it as
    ``None`` and we'll lazy-import ``brief_dispatcher``.
    """
    resolved_gate = should_run or monday_morning_gate()

    def _fn(now: datetime) -> Dict[str, Any]:
        if dispatcher is None:
            from rosteriq import brief_dispatcher as _bd  # lazy
            dispatch = _bd.dispatch_all_weekly_digests
        else:
            dispatch = dispatcher
        summary = dispatch(
            only_when_should_send=only_when_should_send,
            window_days=window_days,
        )
        return {
            "dispatched_at": now.isoformat(),
            "summary": summary,
        }

    return ScheduledJob(
        name=name,
        interval_s=interval_s,
        fn=_fn,
        should_run=resolved_gate,
    )


# ---------------------------------------------------------------------------
# Tanda dead-letter sweep
# ---------------------------------------------------------------------------

def _reconstruct_delta(payload: Dict[str, Any]):
    """Rebuild a ``ShiftDelta`` from a dead-letter payload entry.

    Returns ``None`` if the payload isn't shaped like we expect — the
    sweep then drops the entry (leaves it in the file untouched) so it
    doesn't loop forever on a structurally broken record.
    """
    from rosteriq.tanda_writeback import ShiftDelta  # lazy
    if not isinstance(payload, dict):
        return None
    delta_dict = payload.get("delta")
    if not isinstance(delta_dict, dict):
        return None
    try:
        return ShiftDelta(
            kind=str(delta_dict.get("kind", "")),
            count=int(delta_dict.get("count", 0)),
            timing_hint=str(delta_dict.get("timing_hint", "")),
            reason=str(delta_dict.get("reason", "")),
            impact_estimate_aud=float(delta_dict.get("impact_estimate_aud", 0.0) or 0.0),
            priority=str(delta_dict.get("priority", "med")),
            source_rec_id=str(delta_dict.get("source_rec_id", "")),
            metadata=dict(delta_dict.get("metadata") or {}),
        )
    except Exception:
        return None


def sweep_tanda_dead_letter(
    dead_letter_path: str,
    *,
    sinks: Optional[List[Any]] = None,
    max_entries: int = 100,
) -> Dict[str, Any]:
    """Re-try every entry in the Tanda writeback dead-letter file.

    Process:

    1. Move the dead-letter file aside to ``{path}.sweeping`` so that
       new failures landing during the sweep don't collide with our
       rewrite. If the rename fails (file missing), return a noop
       summary.
    2. For each entry, reconstruct the ShiftDelta and re-apply via
       every sink in ``sinks``. If all sinks succeed for an entry,
       the entry is considered resolved and dropped from the file.
       Otherwise the entry is appended back to the *live* dead-letter
       file (which may have grown during the sweep — that's fine).
    3. Unparseable entries (bad JSON, missing payload) are dropped
       silently; they'd never retry successfully anyway, and keeping
       them would cause the sweep to reprocess them forever.

    Returns a summary dict::

        {
            "path": "...",
            "read": 12,
            "retried": 10,
            "resolved": 7,
            "still_failing": 3,
            "dropped_malformed": 2,
            "swept_at": "<iso>"
        }

    This function is idempotent at the transport level because every
    TandaApiSink request carries an ``Idempotency-Key`` header derived
    from the rec id — upstream dedupes rather than double-applies.
    """
    # Lazy imports keep this module import-light.
    from rosteriq import tanda_writeback as _tw

    if not dead_letter_path:
        return _empty_sweep_summary(dead_letter_path, reason="no_path")

    active_sinks: List[Any]
    if sinks is None:
        active_sinks = [s for s in list(_tw._SINKS) if _looks_like_tanda_api(s)]
    else:
        active_sinks = list(sinks)
    if not active_sinks:
        return _empty_sweep_summary(dead_letter_path, reason="no_sinks")

    if not os.path.exists(dead_letter_path):
        return _empty_sweep_summary(dead_letter_path, reason="no_file")

    swept_path = dead_letter_path + ".sweeping"
    try:
        # If a previous sweep crashed mid-run, pick up the leftover file.
        if os.path.exists(swept_path):
            os.remove(swept_path)
        os.rename(dead_letter_path, swept_path)
    except Exception as exc:
        logger.exception("sweep_tanda_dead_letter: rename failed")
        return _empty_sweep_summary(
            dead_letter_path, reason=f"rename_failed: {exc}"
        )

    read_count = 0
    retried = 0
    resolved = 0
    still_failing = 0
    dropped_malformed = 0

    try:
        with open(swept_path, "r", encoding="utf-8") as f:
            raw_lines = f.readlines()
    except Exception as exc:
        logger.exception("sweep_tanda_dead_letter: read failed")
        return _empty_sweep_summary(
            dead_letter_path, reason=f"read_failed: {exc}"
        )

    for raw in raw_lines:
        if read_count >= max_entries:
            # Preserve anything past the cap back to the live file
            # without retrying this cycle.
            _append_raw(dead_letter_path, raw)
            still_failing += 1
            continue
        raw = raw.strip()
        if not raw:
            continue
        read_count += 1
        try:
            entry = json.loads(raw)
        except Exception:
            dropped_malformed += 1
            continue
        if not isinstance(entry, dict):
            dropped_malformed += 1
            continue

        payload = entry.get("payload")
        venue_id = str(entry.get("venue_id") or (payload or {}).get("venue_id") or "")
        delta = _reconstruct_delta(payload) if isinstance(payload, dict) else None
        if delta is None or not venue_id:
            dropped_malformed += 1
            continue

        retried += 1
        all_ok = True
        sink_results: List[Dict[str, Any]] = []
        for sink in active_sinks:
            try:
                out = sink.apply(venue_id, delta)
                if not isinstance(out, dict):
                    out = {"ok": True, "raw": out}
                ok = bool(out.get("ok"))
                sink_results.append({
                    "sink": getattr(sink, "name", "sink"),
                    "ok": ok,
                    "status_code": out.get("status_code"),
                    "error": out.get("error"),
                })
                if not ok:
                    all_ok = False
            except Exception as exc:
                all_ok = False
                sink_results.append({
                    "sink": getattr(sink, "name", "sink"),
                    "ok": False,
                    "error": f"sink_raised: {exc}",
                })

        if all_ok:
            resolved += 1
        else:
            still_failing += 1
            # Stamp with sweep metadata and write back to the live file.
            entry["swept_at"] = (
                datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
            )
            entry["sweep_results"] = sink_results
            _append_raw(dead_letter_path, json.dumps(entry) + "\n")

    # Clean up the sweeping file — we've consumed everything in it.
    try:
        os.remove(swept_path)
    except Exception:
        pass

    return {
        "path": dead_letter_path,
        "read": read_count,
        "retried": retried,
        "resolved": resolved,
        "still_failing": still_failing,
        "dropped_malformed": dropped_malformed,
        "swept_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    }


def _empty_sweep_summary(path: Optional[str], *, reason: str) -> Dict[str, Any]:
    return {
        "path": path or "",
        "read": 0,
        "retried": 0,
        "resolved": 0,
        "still_failing": 0,
        "dropped_malformed": 0,
        "reason": reason,
        "swept_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    }


def _append_raw(path: str, line: str) -> None:
    try:
        directory = os.path.dirname(path)
        if directory:
            os.makedirs(directory, exist_ok=True)
        with open(path, "a", encoding="utf-8") as f:
            f.write(line if line.endswith("\n") else line + "\n")
    except Exception:
        logger.exception("sweep_tanda_dead_letter: append failed for %s", path)


def _looks_like_tanda_api(sink: Any) -> bool:
    """Heuristic: is this sink one that can retry to a real Tanda endpoint?

    We filter the journal sink out of default sweeps — a journal
    "retry" would just re-append a line, not fix the upstream write.
    """
    name = str(getattr(sink, "name", "") or "")
    if name.startswith("journal"):
        return False
    return hasattr(sink, "apply")


def make_tanda_retry_sweep_job(
    *,
    name: str = "tanda_retry_sweep",
    interval_s: float = 300.0,
    dead_letter_path: str,
    sinks: Optional[List[Any]] = None,
    max_entries: int = 100,
) -> ScheduledJob:
    """Build a scheduled job that sweeps the Tanda dead-letter file."""
    def _fn(now: datetime) -> Dict[str, Any]:
        summary = sweep_tanda_dead_letter(
            dead_letter_path,
            sinks=sinks,
            max_entries=max_entries,
        )
        return {
            "swept_at": now.isoformat(),
            "summary": summary,
        }
    return ScheduledJob(name=name, interval_s=interval_s, fn=_fn)


# ---------------------------------------------------------------------------
# Singleton scheduler for api_v2 wiring
# ---------------------------------------------------------------------------

_GLOBAL_SCHEDULER: Optional[Scheduler] = None


def get_global_scheduler() -> Scheduler:
    """Lazily construct a process-wide scheduler instance."""
    global _GLOBAL_SCHEDULER
    if _GLOBAL_SCHEDULER is None:
        _GLOBAL_SCHEDULER = Scheduler()
    return _GLOBAL_SCHEDULER


def reset_global_scheduler_for_tests() -> None:
    """Drop the singleton — tests only."""
    global _GLOBAL_SCHEDULER
    if _GLOBAL_SCHEDULER is not None:
        try:
            _GLOBAL_SCHEDULER.stop()
        except Exception:
            pass
    _GLOBAL_SCHEDULER = None
