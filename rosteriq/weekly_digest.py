"""Weekly digest — roll seven daily briefs into one Monday-morning summary.

Pure stdlib. Sits on top of ``trends`` and ``accountability_store`` to
answer the question: "looking back on the week that just ended, where
did accountability actually go — and what's the one pattern to fix
next week?"

Shape-compatible with the morning brief so the dispatcher plumbing
from Moment 12 can route it without any extra work. The difference:
the window is a 7-day block anchored to a week-ending date (defaults
to yesterday), and the pattern detection looks for repeating
dismissal categories rather than day-level specifics.

Design notes:

- Week windows are Mon–Sun by default, but ``week_ending`` lets the
  caller shift that for venues that close on a different day. The
  composer does not assume AU timezones — it takes a date string.
- Pattern detection groups dismissed recs by their rec-id action
  suffix (over_wage_high, burn_rate_high, etc.) so a venue that
  dismissed the same cut-staff rec five days in a row sees ONE
  pattern with a count of 5, not five separate lines.
- The digest headline and "one pattern to fix" are both deterministic
  — same events + same week always produce the same text. No clocks,
  no randomness.
- Empty weeks produce an "all quiet" headline with a positive frame
  rather than a blank card. The dispatcher can choose to skip sending
  these based on the ``should_send`` flag.
"""
from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple


DEFAULT_WINDOW_DAYS = 7

# Human-friendly labels for each rec-id action suffix. Used in pattern
# rollups so dismissals are reported in English rather than in
# code-shaped slugs.
_PATTERN_LABELS: Dict[str, str] = {
    "over_wage_high": "Cut-staff alerts",
    "over_wage_med": "Send-home-after-peak alerts",
    "under_wage": "Understaffing risk alerts",
    "burn_rate_high": "Burn-rate trim alerts",
}


# ---------------------------------------------------------------------------
# Date handling
# ---------------------------------------------------------------------------

def _parse_date(d: Any) -> Optional[date]:
    """Accept a ``date``, a ``datetime``, or a ``YYYY-MM-DD`` string."""
    if isinstance(d, date) and not isinstance(d, datetime):
        return d
    if isinstance(d, datetime):
        return d.date()
    if isinstance(d, str) and len(d) >= 10:
        try:
            return datetime.strptime(d[:10], "%Y-%m-%d").date()
        except ValueError:
            return None
    return None


def _week_window(
    week_ending: Optional[date],
    *,
    window_days: int = DEFAULT_WINDOW_DAYS,
) -> Tuple[date, date]:
    """Return ``(start_inclusive, end_inclusive)`` for a week window.

    Defaults end to yesterday (UTC) so running the digest first thing
    Monday morning reviews the week that just ended. Window is always
    N calendar days — 7 by default — and ``window_days`` is clamped to
    the same sane range as trends.
    """
    if window_days < 1:
        window_days = 1
    if window_days > 90:
        window_days = 90
    if week_ending is None:
        week_ending = datetime.now(timezone.utc).date() - timedelta(days=1)
    start = week_ending - timedelta(days=window_days - 1)
    return start, week_ending


def _event_date(ev: Dict[str, Any]) -> Optional[date]:
    """Pick the most relevant date for an event — responded_at wins
    over recorded_at so the digest reflects when a manager actually
    acted, not when the rec was first fired."""
    for key in ("responded_at", "recorded_at", "created_at"):
        d = _parse_date(ev.get(key))
        if d:
            return d
    return None


# ---------------------------------------------------------------------------
# Action-suffix parser (shared shape with tanda_writeback)
# ---------------------------------------------------------------------------

def _action_suffix(rec_id: str) -> Optional[str]:
    if not rec_id or not isinstance(rec_id, str):
        return None
    if not rec_id.startswith("rec_pulse_"):
        return None
    parts = rec_id.split("_")
    for i, p in enumerate(parts):
        if len(p) == 10 and p[4] == "-" and p[7] == "-":
            suffix = "_".join(parts[i + 1:])
            return suffix or None
    return None


# ---------------------------------------------------------------------------
# Core roll-up
# ---------------------------------------------------------------------------

def _events_in_window(
    events: Iterable[Dict[str, Any]],
    start: date,
    end: date,
) -> List[Dict[str, Any]]:
    """Filter events to the [start, end] date window (both inclusive)."""
    out: List[Dict[str, Any]] = []
    for ev in events or []:
        d = _event_date(ev)
        if d is None:
            continue
        if start <= d <= end:
            out.append(ev)
    return out


def _roll_up_week(events: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Aggregate totals for a week's worth of events."""
    total_events = len(events)
    accepted = 0
    dismissed = 0
    pending = 0
    dismissed_aud = 0.0
    accepted_aud = 0.0
    for ev in events:
        status = str(ev.get("status") or "").lower()
        try:
            impact = float(ev.get("impact_estimate_aud") or 0)
        except (TypeError, ValueError):
            impact = 0.0
        if status == "accepted":
            accepted += 1
            accepted_aud += impact
        elif status == "dismissed":
            dismissed += 1
            dismissed_aud += impact
        else:
            pending += 1
    responded = accepted + dismissed
    rate = (accepted / responded) if responded > 0 else 0.0
    return {
        "total_events": int(total_events),
        "accepted": int(accepted),
        "dismissed": int(dismissed),
        "pending": int(pending),
        "dismissed_aud": round(dismissed_aud, 2),
        "accepted_aud": round(accepted_aud, 2),
        "acceptance_rate": round(rate, 4),
    }


def _detect_patterns(
    events: List[Dict[str, Any]],
    *,
    limit: int = 3,
) -> List[Dict[str, Any]]:
    """Group dismissed events by action suffix and return the top N.

    Patterns are ranked by (count DESC, dismissed_aud DESC, suffix ASC)
    so two suffixes with identical counts resolve deterministically.
    """
    buckets: Dict[str, Dict[str, Any]] = defaultdict(
        lambda: {"count": 0, "dismissed_aud": 0.0, "sample_text": ""}
    )
    for ev in events:
        if str(ev.get("status") or "").lower() != "dismissed":
            continue
        suffix = _action_suffix(str(ev.get("rec_id") or ev.get("id") or ""))
        if not suffix:
            continue
        b = buckets[suffix]
        b["count"] += 1
        try:
            b["dismissed_aud"] += float(ev.get("impact_estimate_aud") or 0)
        except (TypeError, ValueError):
            pass
        if not b["sample_text"]:
            # Keep the first sentence of the first occurrence as a sample
            txt = str(ev.get("text") or "")
            b["sample_text"] = txt.split(".")[0].strip()

    ranked = sorted(
        buckets.items(),
        key=lambda kv: (-kv[1]["count"], -kv[1]["dismissed_aud"], kv[0]),
    )
    out: List[Dict[str, Any]] = []
    for suffix, b in ranked[:limit]:
        out.append({
            "pattern": suffix,
            "label": _PATTERN_LABELS.get(suffix, suffix.replace("_", " ").title()),
            "count": int(b["count"]),
            "dismissed_aud": round(b["dismissed_aud"], 2),
            "sample": b["sample_text"],
        })
    return out


# ---------------------------------------------------------------------------
# Headline + one-pattern-to-fix
# ---------------------------------------------------------------------------

def _traffic_light(rollup: Dict[str, Any]) -> str:
    """Green for high acceptance or quiet week, red for high dismiss $,
    amber otherwise."""
    total = int(rollup.get("total_events") or 0)
    if total == 0:
        return "green"
    dismissed_aud = float(rollup.get("dismissed_aud") or 0)
    rate = float(rollup.get("acceptance_rate") or 0)
    if dismissed_aud >= 1000.0 or rate <= 0.3:
        return "red"
    if dismissed_aud >= 300.0 or rate <= 0.6:
        return "amber"
    return "green"


def _headline(
    *,
    start: date,
    end: date,
    rollup: Dict[str, Any],
) -> str:
    total = int(rollup.get("total_events") or 0)
    dismissed = int(rollup.get("dismissed") or 0)
    dismissed_aud = float(rollup.get("dismissed_aud") or 0)
    rate = float(rollup.get("acceptance_rate") or 0)
    window_label = f"{start.isoformat()} → {end.isoformat()}"
    if total == 0:
        return f"All quiet for the week of {window_label} — no alerts fired."
    if dismissed == 0:
        return f"Clean week ({window_label}): you actioned every alert that fired."
    if dismissed_aud >= 500:
        return (
            f"Dismissed recs cost you ~${round(dismissed_aud):,} "
            f"the week of {window_label}."
        )
    return (
        f"Acceptance rate {round(rate * 100)}% for the week of {window_label} — "
        f"{dismissed} dismissed, {rollup.get('accepted')} actioned."
    )


def _one_pattern_to_fix(patterns: List[Dict[str, Any]]) -> str:
    if not patterns:
        return "Nothing to change — keep the streak going."
    top = patterns[0]
    count = int(top.get("count") or 0)
    if count == 0:
        return "Nothing to change — keep the streak going."
    label = str(top.get("label") or "alerts")
    impact = float(top.get("dismissed_aud") or 0)
    if impact > 0:
        return (
            f"{label}: dismissed {count} times this week, "
            f"~${round(impact):,} on the table. Action the next one."
        )
    return f"{label}: dismissed {count} times this week. Action the next one."


# ---------------------------------------------------------------------------
# Public composer
# ---------------------------------------------------------------------------

def compose_weekly_digest(
    venue_id: str,
    events: Iterable[Dict[str, Any]],
    *,
    week_ending: Any = None,
    window_days: int = DEFAULT_WINDOW_DAYS,
    venue_label: Optional[str] = None,
) -> Dict[str, Any]:
    """Build a deterministic weekly digest from an events list.

    Returns a dict with keys ``venue_id``, ``venue_label``, ``date``,
    ``week_start``, ``week_end``, ``generated_at``, ``traffic_light``,
    ``headline``, ``one_pattern``, ``summary``, ``rollup``, ``patterns``,
    ``should_send``. The shape is a superset of the morning brief so
    the dispatcher from Moment 12 can route it without modification.
    """
    end = _parse_date(week_ending)
    start, end = _week_window(end, window_days=window_days)

    in_window = _events_in_window(events, start, end)
    rollup = _roll_up_week(in_window)
    patterns = _detect_patterns(in_window)
    light = _traffic_light(rollup)
    headline = _headline(start=start, end=end, rollup=rollup)
    one_pattern = _one_pattern_to_fix(patterns)

    # A "should_send" hint — dispatchers may choose to skip quiet weeks
    # rather than always posting. Loud weeks always send.
    total = int(rollup.get("total_events") or 0)
    should_send = total > 0

    summary_bits: List[str] = []
    if total > 0:
        summary_bits.append(
            f"{total} alerts: {rollup.get('accepted')} actioned, "
            f"{rollup.get('dismissed')} dismissed, {rollup.get('pending')} still pending"
        )
        if rollup.get("dismissed_aud", 0) > 0:
            summary_bits.append(
                f"~${round(float(rollup.get('dismissed_aud') or 0)):,} on the table"
            )
    else:
        summary_bits.append("No accountability events this week")
    summary = " · ".join(summary_bits)

    return {
        "venue_id": str(venue_id or ""),
        "venue_label": str(venue_label or venue_id or ""),
        "date": end.isoformat(),
        "week_start": start.isoformat(),
        "week_end": end.isoformat(),
        "window_days": int(end.toordinal() - start.toordinal() + 1),
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "traffic_light": light,
        "headline": headline,
        "one_pattern": one_pattern,
        "summary": summary,
        "rollup": rollup,
        "patterns": patterns,
        "should_send": bool(should_send),
    }


def compose_weekly_digest_from_store(
    venue_id: str,
    *,
    week_ending: Any = None,
    window_days: int = DEFAULT_WINDOW_DAYS,
    venue_label: Optional[str] = None,
    store: Any = None,
) -> Dict[str, Any]:
    """Pull events from the accountability store and build a digest.

    ``store`` is injectable for tests — defaults to the real store.
    """
    if store is None:
        from rosteriq import accountability_store as store  # lazy import
    events = store.history(venue_id)
    return compose_weekly_digest(
        venue_id,
        list(events or []),
        week_ending=week_ending,
        window_days=window_days,
        venue_label=venue_label,
    )


# ---------------------------------------------------------------------------
# Plain-text renderer — shape-compatible with morning_brief.render_text
# ---------------------------------------------------------------------------

def render_text(digest: Dict[str, Any]) -> str:
    """Render a digest dict to plain text for email / Slack / cron curl.

    The output is intentionally boring and wide-width so it renders in
    monospace email clients. No emojis, no markdown — just text.
    """
    lines: List[str] = []
    label = str(digest.get("venue_label") or digest.get("venue_id") or "venue")
    week_start = str(digest.get("week_start") or "")
    week_end = str(digest.get("week_end") or "")
    lines.append(f"WEEKLY DIGEST — {label}")
    lines.append(f"Week of {week_start} → {week_end}")
    lines.append("")
    lines.append(str(digest.get("headline") or ""))
    lines.append("")

    rollup = digest.get("rollup") or {}
    lines.append(
        f"  Alerts: {rollup.get('total_events', 0)}  "
        f"Actioned: {rollup.get('accepted', 0)}  "
        f"Dismissed: {rollup.get('dismissed', 0)}  "
        f"Pending: {rollup.get('pending', 0)}"
    )
    dismissed_aud = float(rollup.get("dismissed_aud") or 0)
    if dismissed_aud > 0:
        lines.append(f"  Dismissed impact: ~${round(dismissed_aud):,}")
    rate = float(rollup.get("acceptance_rate") or 0)
    lines.append(f"  Acceptance rate: {round(rate * 100)}%")

    patterns = digest.get("patterns") or []
    if patterns:
        lines.append("")
        lines.append("Top patterns:")
        for p in patterns:
            impact = float(p.get("dismissed_aud") or 0)
            impact_str = f" (~${round(impact):,})" if impact > 0 else ""
            lines.append(
                f"  - {p.get('label')}: dismissed {p.get('count')}x{impact_str}"
            )

    lines.append("")
    lines.append("One pattern to fix next week:")
    lines.append(f"  {digest.get('one_pattern') or ''}")
    lines.append("")
    return "\n".join(lines)
