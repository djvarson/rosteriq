"""Week-over-week accountability trends (Moment 13).

Takes a venue's raw accountability event history and rolls it up into
a time series the dashboard can show as sparklines plus a single
direction-of-travel headline:

    "Acceptance rate up 22pt over 7 days — you're actioning more recs."
    "Dismissed dollars climbing — last week cost you $X more than the
     week before."
    "Flat — no recs fired in either window."

The composer is deterministic, pure stdlib, and accepts the exact event
shape that ``accountability_store.history()`` returns, so the API layer
can just feed history into the composer and serve the output.

Why a separate module and not a shift_recap extension:

* ``shift_recap`` is single-day, ``morning_brief`` is single-day
  (yesterday), ``portfolio_recap`` is single-day across venues.
* ``trends`` is the multi-day axis. Keeping it separate lets each
  composer remain simple and independently testable, and avoids
  blowing up shift_recap's signature with a window parameter.

Pure stdlib. Tests live in ``tests/test_trends.py``.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

# ---------------------------------------------------------------------------
# Windows + date helpers
# ---------------------------------------------------------------------------

VALID_WINDOWS = (7, 14, 28)

# Thresholds for the headline classifier. Expressed in *percentage
# points* of acceptance-rate drift and *dollars* of missed-impact
# drift (second-half avg minus first-half avg).
ACCEPTANCE_RED_DROP_PT = 0.10   # -10pt → red
ACCEPTANCE_AMBER_DROP_PT = 0.03  # -3pt → amber
MISSED_RED_JUMP = 200.0          # +$200/day avg → red
MISSED_AMBER_JUMP = 75.0         # +$75/day avg → amber


def _today_utc() -> date:
    return datetime.now(timezone.utc).date()


def _parse_iso_date(s: str) -> Optional[date]:
    """Return the YYYY-MM-DD component of an ISO string as a date,
    or None on any malformed input."""
    if not s or not isinstance(s, str) or len(s) < 10:
        return None
    try:
        return date.fromisoformat(s[:10])
    except ValueError:
        return None


def _event_date(ev: Dict[str, Any]) -> Optional[date]:
    """Bucket by responded_at when actioned, recorded_at otherwise."""
    for key in ("responded_at", "recorded_at"):
        d = _parse_iso_date(str(ev.get(key) or ""))
        if d is not None:
            return d
    return None


# ---------------------------------------------------------------------------
# Daily roll-ups
# ---------------------------------------------------------------------------

def compute_daily_rollups(
    events: Iterable[Dict[str, Any]],
    *,
    window_days: int = 7,
    today: Optional[date] = None,
) -> List[Dict[str, Any]]:
    """Group events by day over the last ``window_days`` (inclusive of
    yesterday, exclusive of today — the trend is a look-back).

    Returns a list of ``window_days`` dicts, oldest first. Days with
    no events are present as zero rows so sparklines render without
    gaps.
    """
    if window_days not in VALID_WINDOWS:
        # Accept any positive int, but clamp to a sensible range so a
        # bogus ?window= param can't blow up the composer.
        window_days = max(1, min(int(window_days or 7), 90))

    anchor = today or _today_utc()
    # We look back over [anchor - window_days, anchor - 1] inclusive —
    # yesterday is the most recent day in the series.
    days: List[date] = [
        anchor - timedelta(days=window_days - i) for i in range(window_days)
    ]

    # Prime the buckets
    buckets: Dict[date, Dict[str, Any]] = {
        d: {
            "date": d.isoformat(),
            "dismissed": 0,
            "accepted": 0,
            "pending": 0,
            "missed_aud": 0.0,
            "accepted_aud": 0.0,
        }
        for d in days
    }

    for ev in events or []:
        if not isinstance(ev, dict):
            continue
        ev_date = _event_date(ev)
        if ev_date is None or ev_date not in buckets:
            continue
        status = (ev.get("status") or "pending").lower()
        try:
            impact = float(ev.get("impact_estimate_aud") or 0.0)
        except (TypeError, ValueError):
            impact = 0.0
        b = buckets[ev_date]
        if status == "dismissed":
            b["dismissed"] += 1
            b["missed_aud"] += impact
        elif status == "accepted":
            b["accepted"] += 1
            b["accepted_aud"] += impact
        else:
            b["pending"] += 1

    # Compute per-day acceptance rate and total_events
    out: List[Dict[str, Any]] = []
    for d in days:
        b = buckets[d]
        responded = b["accepted"] + b["dismissed"]
        rate = (b["accepted"] / responded) if responded > 0 else 0.0
        out.append({
            "date": b["date"],
            "dismissed": int(b["dismissed"]),
            "accepted": int(b["accepted"]),
            "pending": int(b["pending"]),
            "total_events": int(b["dismissed"] + b["accepted"] + b["pending"]),
            "missed_aud": round(b["missed_aud"], 2),
            "accepted_aud": round(b["accepted_aud"], 2),
            "acceptance_rate": round(rate, 4),
        })
    return out


# ---------------------------------------------------------------------------
# Slope computation
# ---------------------------------------------------------------------------

def _split_halves(values: List[float]) -> Tuple[List[float], List[float]]:
    """Split a series into first/second halves. Odd series put the
    middle element in the *second* half so the most-recent window is
    always at least as large as the older window."""
    n = len(values)
    mid = n // 2
    return values[:mid], values[mid:]


def _avg(values: List[float]) -> float:
    if not values:
        return 0.0
    return sum(values) / len(values)


def _slope(series: List[float]) -> Dict[str, Any]:
    """Very simple 'first-half avg vs second-half avg' slope.

    We deliberately don't run a least-squares fit: rostering signal is
    too noisy day-to-day for regression to mean much, and a two-bucket
    average is easier for a human to explain and audit.
    """
    if not series:
        return {"first_half": 0.0, "second_half": 0.0, "delta": 0.0}
    a, b = _split_halves(series)
    first = _avg(a)
    second = _avg(b)
    return {
        "first_half": round(first, 4),
        "second_half": round(second, 4),
        "delta": round(second - first, 4),
    }


# ---------------------------------------------------------------------------
# Classifier + headline
# ---------------------------------------------------------------------------

def _classify_light(
    acceptance_slope: Dict[str, Any],
    missed_slope: Dict[str, Any],
    *,
    total_events: int,
) -> str:
    """Red/amber/green verdict on "are you getting better or worse"."""
    if total_events == 0:
        return "unknown"

    accept_delta = float(acceptance_slope.get("delta") or 0.0)
    missed_delta = float(missed_slope.get("delta") or 0.0)

    # Red if either axis is badly regressing.
    if accept_delta <= -ACCEPTANCE_RED_DROP_PT or missed_delta >= MISSED_RED_JUMP:
        return "red"
    # Amber if either axis is mildly regressing.
    if accept_delta <= -ACCEPTANCE_AMBER_DROP_PT or missed_delta >= MISSED_AMBER_JUMP:
        return "amber"
    return "green"


def _compose_headline(
    *,
    window_days: int,
    acceptance_slope: Dict[str, Any],
    missed_slope: Dict[str, Any],
    total_events: int,
    total_missed: float,
) -> str:
    """Compose a single human headline summarising direction of travel.

    Prioritises the worst axis first so a manager sees the important
    number. Falls back on 'flat' / 'no data' when neither axis moved.
    """
    if total_events == 0:
        return f"No accountability events in the last {window_days} days — start actioning recs to light this up."

    accept_delta_pt = float(acceptance_slope.get("delta") or 0.0) * 100
    missed_delta = float(missed_slope.get("delta") or 0.0)

    # Worst-first: if dollars are climbing hard, lead with that
    if missed_delta >= MISSED_AMBER_JUMP:
        return (
            f"Dismissed dollars climbing — last half of the window averaged "
            f"~${abs(missed_delta):,.0f}/day more in missed impact than the first half. "
            f"Total missed over {window_days} days: ~${total_missed:,.0f}."
        )
    if accept_delta_pt <= -ACCEPTANCE_AMBER_DROP_PT * 100:
        return (
            f"Acceptance rate is slipping — down {abs(accept_delta_pt):.0f}pt "
            f"over the last {window_days} days. You're dismissing more recs than you used to."
        )
    if accept_delta_pt >= 3.0:
        return (
            f"Acceptance rate up {accept_delta_pt:.0f}pt over the last {window_days} days — "
            f"you're actioning more recs than you used to."
        )
    if missed_delta <= -MISSED_AMBER_JUMP:
        return (
            f"Dismissed dollars trending down ~${abs(missed_delta):,.0f}/day — "
            f"you're catching more of the wins."
        )
    return (
        f"Trend is flat over the last {window_days} days — "
        f"your actioning pattern hasn't changed."
    )


# ---------------------------------------------------------------------------
# Main composer
# ---------------------------------------------------------------------------

def compose_trend(
    venue_id: str,
    events: Iterable[Dict[str, Any]],
    *,
    window_days: int = 7,
    today: Optional[date] = None,
) -> Dict[str, Any]:
    """
    Roll a venue's accountability history into a trend dict for the
    last ``window_days`` days.

    Args:
        venue_id: The venue being trended.
        events: Full accountability event history (anything shaped like
            ``accountability_store.history()`` output).
        window_days: 7, 14, or 28 — anything else is clamped.
        today: Override the anchor date (mostly for tests).

    Returns:
        Dict with keys: ``venue_id``, ``window_days``, ``generated_at``,
        ``daily``, ``series``, ``slopes``, ``totals``, ``traffic_light``,
        ``headline``.
    """
    daily = compute_daily_rollups(events, window_days=window_days, today=today)

    acceptance_series = [float(d["acceptance_rate"]) for d in daily]
    missed_series = [float(d["missed_aud"]) for d in daily]
    total_events_series = [int(d["total_events"]) for d in daily]

    acceptance_slope = _slope(acceptance_series)
    missed_slope = _slope(missed_series)
    events_slope = _slope([float(v) for v in total_events_series])

    total_events = sum(total_events_series)
    total_missed = sum(missed_series)
    total_accepted = sum(int(d["accepted"]) for d in daily)
    total_dismissed = sum(int(d["dismissed"]) for d in daily)
    responded = total_accepted + total_dismissed
    overall_acceptance = (total_accepted / responded) if responded > 0 else 0.0

    light = _classify_light(
        acceptance_slope, missed_slope, total_events=total_events
    )
    headline = _compose_headline(
        window_days=len(daily),
        acceptance_slope=acceptance_slope,
        missed_slope=missed_slope,
        total_events=total_events,
        total_missed=total_missed,
    )

    return {
        "venue_id": str(venue_id or ""),
        "window_days": len(daily),
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "traffic_light": light,
        "headline": headline,
        "daily": daily,
        "series": {
            "acceptance_rate": [round(v, 4) for v in acceptance_series],
            "missed_aud": [round(v, 2) for v in missed_series],
            "total_events": total_events_series,
        },
        "slopes": {
            "acceptance_rate": acceptance_slope,
            "missed_aud": missed_slope,
            "total_events": events_slope,
        },
        "totals": {
            "events": int(total_events),
            "accepted": int(total_accepted),
            "dismissed": int(total_dismissed),
            "missed_aud": round(total_missed, 2),
            "acceptance_rate": round(overall_acceptance, 4),
        },
    }


# ---------------------------------------------------------------------------
# Convenience wrapper that pulls from the store
# ---------------------------------------------------------------------------

def compose_trend_from_store(
    venue_id: str,
    *,
    window_days: int = 7,
    today: Optional[date] = None,
    store: Any = None,
) -> Dict[str, Any]:
    """Pull events from ``accountability_store`` (or an injected stub)
    and call ``compose_trend``. Separate entry point so tests can drive
    ``compose_trend`` directly without touching the module-global store."""
    if store is None:
        from rosteriq import accountability_store as store  # lazy import
    events = store.history(venue_id)
    return compose_trend(
        venue_id,
        list(events or []),
        window_days=window_days,
        today=today,
    )
