"""Morning Brief — the next-day accountability digest (Moment 11).

Answers the meeting question at 7am the day after:

    "You had all this data and kept people on — why?"

Looks at *yesterday's* shift recap and *yesterday's* accountability
events, and produces a deterministic brief dict the dashboard (or a
future email job) can render as: what was dismissed, what it cost,
and one thing to do differently today.

Pure stdlib. No FastAPI, no Pydantic, no IO. Tests live in
``tests/test_morning_brief.py``.

Why a separate module and not just a shift_recap extension:

* ``shift_recap`` is present-tense ("what is happening now")
* ``morning_brief`` is past-tense + forward-looking ("yesterday cost
  you $X — don't do it again today")

The two answer different questions for different moments in a
manager's day. Keeping them separate keeps each composer simple and
independently testable.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional

# ---------------------------------------------------------------------------
# Date helpers
# ---------------------------------------------------------------------------

def _today_iso() -> str:
    return datetime.now(timezone.utc).date().isoformat()


def _yesterday_iso() -> str:
    return (datetime.now(timezone.utc).date() - timedelta(days=1)).isoformat()


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _event_date(event: Dict[str, Any]) -> str:
    """Extract YYYY-MM-DD from an accountability event, preferring
    ``responded_at`` (when it was actioned) but falling back to
    ``recorded_at`` so freshly-dismissed recs still land on the right
    day even if the timestamps drift."""
    for key in ("responded_at", "recorded_at"):
        ts = event.get(key)
        if ts and isinstance(ts, str) and len(ts) >= 10 and ts[4] == "-":
            return ts[:10]
    return ""


# ---------------------------------------------------------------------------
# Formatting
# ---------------------------------------------------------------------------

def _fmt_money(n: float) -> str:
    n = float(n or 0.0)
    if abs(n) >= 1000:
        return f"${n / 1000:.1f}k"
    return f"${round(n):,.0f}"


def _fmt_pct(decimal: float, *, signed: bool = False) -> str:
    pts = (decimal or 0.0) * 100
    sign = "+" if (signed and pts >= 0) else ""
    return f"{sign}{pts:.1f}%"


def _safe(d: Optional[Dict[str, Any]], *path: str, default: float = 0.0) -> float:
    cur: Any = d
    for p in path:
        if not isinstance(cur, dict):
            return default
        cur = cur.get(p)
    if cur is None:
        return default
    try:
        return float(cur)
    except (TypeError, ValueError):
        return default


# ---------------------------------------------------------------------------
# Filter + classify
# ---------------------------------------------------------------------------

def _events_for_date(
    events: Iterable[Dict[str, Any]],
    target_date: str,
) -> List[Dict[str, Any]]:
    """Return only the events that land on ``target_date`` (YYYY-MM-DD)."""
    out: List[Dict[str, Any]] = []
    for ev in events or []:
        if not isinstance(ev, dict):
            continue
        if _event_date(ev) == target_date:
            out.append(ev)
    return out


def _roll_up_events(events: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Compute counts and $ for a day's events."""
    total = len(events)
    pending = accepted = dismissed = 0
    missed_aud = 0.0
    accepted_aud = 0.0

    for ev in events:
        status = (ev.get("status") or "pending").lower()
        try:
            impact = float(ev.get("impact_estimate_aud") or 0.0)
        except (TypeError, ValueError):
            impact = 0.0
        if status == "dismissed":
            dismissed += 1
            missed_aud += impact
        elif status == "accepted":
            accepted += 1
            accepted_aud += impact
        else:
            pending += 1

    responded = accepted + dismissed
    acceptance_rate = (accepted / responded) if responded > 0 else 0.0

    return {
        "total": total,
        "pending": pending,
        "accepted": accepted,
        "dismissed": dismissed,
        "missed_aud": round(missed_aud, 2),
        "accepted_aud": round(accepted_aud, 2),
        "acceptance_rate": round(acceptance_rate, 4),
    }


def _top_dismissed(events: List[Dict[str, Any]], limit: int = 3) -> List[Dict[str, Any]]:
    """Return the top-N dismissed events by impact, newest first on ties."""
    dismissed = [e for e in events if (e.get("status") or "").lower() == "dismissed"]
    dismissed.sort(
        key=lambda e: (
            -float(e.get("impact_estimate_aud") or 0),
            # newer timestamp wins ties
            -_ts_rank(e.get("responded_at") or e.get("recorded_at") or ""),
        )
    )
    out: List[Dict[str, Any]] = []
    for ev in dismissed[:limit]:
        out.append({
            "id": ev.get("id"),
            "text": ev.get("text") or "(no text)",
            "impact_estimate_aud": float(ev.get("impact_estimate_aud") or 0.0),
            "source": ev.get("source") or "",
            "priority": ev.get("priority") or "med",
            "responded_at": ev.get("responded_at") or ev.get("recorded_at"),
        })
    return out


def _ts_rank(ts: str) -> float:
    """Turn an ISO timestamp into a sortable int. Missing/malformed → 0."""
    if not ts or not isinstance(ts, str):
        return 0.0
    try:
        # Strip trailing Z for fromisoformat compatibility on older Pythons
        clean = ts.replace("Z", "+00:00") if ts.endswith("Z") else ts
        return datetime.fromisoformat(clean).timestamp()
    except Exception:
        return 0.0


# ---------------------------------------------------------------------------
# "One thing to do differently today" — the nudge
# ---------------------------------------------------------------------------

def _pick_one_thing(
    *,
    rollup: Dict[str, Any],
    top: List[Dict[str, Any]],
    yesterday_recap: Optional[Dict[str, Any]],
) -> str:
    """Choose the single most actionable reminder for today based on
    yesterday's post-mortem. Deterministic — same inputs, same output."""
    if rollup["dismissed"] == 0 and rollup["accepted"] == 0:
        return "No recs fired yesterday — you're starting clean. Keep an eye on the live pulse."

    if rollup["dismissed"] == 0 and rollup["accepted"] > 0:
        return f"Clean day — you actioned every rec that fired ({rollup['accepted']}). Keep doing that today."

    # At least one dismissed.
    if top:
        biggest = top[0]
        impact = float(biggest.get("impact_estimate_aud") or 0)
        text = str(biggest.get("text") or "").strip()
        if impact > 0 and text:
            # Keep it sharp: don't quote the whole rec, just the nudge.
            first_sentence = text.split(".")[0]
            if len(first_sentence) > 110:
                first_sentence = first_sentence[:107] + "..."
            return (
                f"Don't repeat yesterday's biggest miss — ~{_fmt_money(impact)} "
                f"on the table when you dismissed: \"{first_sentence}\". "
                f"If the same rec fires today, action it."
            )

    # Dismissed but no impact data — still call it out
    if rollup["dismissed"] > 0:
        word = "rec" if rollup["dismissed"] == 1 else "recs"
        return (
            f"You dismissed {rollup['dismissed']} {word} yesterday. "
            f"Treat every rec that fires today as load-bearing — action it or write down why."
        )

    # Shouldn't reach here, but fallback
    return "Review yesterday's recs before the pre-shift brief — patterns repeat."


# ---------------------------------------------------------------------------
# Headline
# ---------------------------------------------------------------------------

def _headline(
    *,
    rollup: Dict[str, Any],
    yesterday_recap: Optional[Dict[str, Any]],
) -> str:
    """One-line morning headline. Leads with dollars if dollars exist,
    otherwise with counts, otherwise with traffic-light state."""
    missed = float(rollup.get("missed_aud") or 0.0)
    dismissed = int(rollup.get("dismissed") or 0)
    accepted = int(rollup.get("accepted") or 0)
    light = str((yesterday_recap or {}).get("traffic_light") or "unknown").lower()

    if dismissed > 0 and missed > 0:
        return (
            f"Yesterday cost you ~{_fmt_money(missed)} in dismissed recs "
            f"({dismissed} ignored, {accepted} actioned)."
        )
    if dismissed > 0:
        return (
            f"You dismissed {dismissed} rec{'s' if dismissed != 1 else ''} yesterday "
            f"({accepted} actioned)."
        )
    if accepted > 0:
        return f"Clean day — {accepted} rec{'s' if accepted != 1 else ''} actioned, none dismissed."
    if light == "red":
        return "Yesterday was red. No recs fired — review the pulse settings."
    if light == "amber":
        return "Yesterday was amber. No recs fired — keep an eye on today's pulse."
    return "Starting fresh — no accountability events from yesterday."


# ---------------------------------------------------------------------------
# Main composer
# ---------------------------------------------------------------------------

def compose_brief(
    venue_id: str,
    *,
    target_date: Optional[str] = None,
    events: Optional[Iterable[Dict[str, Any]]] = None,
    yesterday_recap: Optional[Dict[str, Any]] = None,
    venue_label: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Compose a morning brief for ``venue_id`` covering ``target_date``
    (defaults to yesterday in UTC).

    Args:
        venue_id: The venue being briefed.
        target_date: YYYY-MM-DD string. Defaults to yesterday (UTC).
        events: Iterable of accountability events (full history OK —
            this function filters them itself). Accepts anything
            shaped like ``accountability_store.history()`` output.
        yesterday_recap: Optional shift_recap.compose_recap() output
            for that day. When present, the brief incorporates its
            traffic light and revenue/wage %s.
        venue_label: Optional human-friendly venue name.

    Returns:
        Dict with keys: ``venue_id``, ``venue_label``, ``date``,
        ``generated_at``, ``traffic_light``, ``headline``, ``one_thing``,
        ``summary``, ``rollup``, ``top_dismissed``, ``recap_context``.
    """
    vid = str(venue_id or "")
    date = target_date or _yesterday_iso()

    day_events = _events_for_date(list(events or []), date)
    rollup = _roll_up_events(day_events)
    top = _top_dismissed(day_events)

    light = str((yesterday_recap or {}).get("traffic_light") or "unknown").lower()
    headline = _headline(rollup=rollup, yesterday_recap=yesterday_recap)
    one_thing = _pick_one_thing(
        rollup=rollup, top=top, yesterday_recap=yesterday_recap
    )

    recap_context: Dict[str, Any] = {}
    if isinstance(yesterday_recap, dict):
        recap_context = {
            "revenue_actual": _safe(yesterday_recap, "revenue", "actual"),
            "revenue_forecast": _safe(yesterday_recap, "revenue", "forecast"),
            "revenue_delta_pct": _safe(yesterday_recap, "revenue", "delta_pct"),
            "wage_pct_actual": _safe(yesterday_recap, "wages", "pct_of_revenue_actual"),
            "wage_pct_target": _safe(yesterday_recap, "wages", "pct_of_revenue_target"),
            "wage_pct_delta": _safe(yesterday_recap, "wages", "pct_delta"),
            "peak_headcount": int(_safe(yesterday_recap, "headcount", "peak")),
        }

    # Build the detailed one-line summary — what an email subject line
    # would look like.
    parts: List[str] = [headline]
    if recap_context:
        parts.append(
            f"Revenue {_fmt_money(recap_context['revenue_actual'])} "
            f"({_fmt_pct(recap_context['revenue_delta_pct'], signed=True)} vs forecast), "
            f"wage % {_fmt_pct(recap_context['wage_pct_actual'])} "
            f"({_fmt_pct(recap_context['wage_pct_delta'], signed=True).replace('%', 'pt')} vs target)."
        )
    summary = " ".join(parts)

    return {
        "venue_id": vid,
        "venue_label": venue_label or vid,
        "date": date,
        "generated_at": _now_iso(),
        "traffic_light": light,
        "headline": headline,
        "one_thing": one_thing,
        "summary": summary,
        "rollup": rollup,
        "top_dismissed": top,
        "recap_context": recap_context,
    }


# ---------------------------------------------------------------------------
# Plain-text renderer (for future email / Slack jobs)
# ---------------------------------------------------------------------------

def render_text(brief: Dict[str, Any]) -> str:
    """Render a brief dict as plain text — suitable for an email body
    or a Slack message. Deterministic, no external deps."""
    if not isinstance(brief, dict):
        return ""
    lines: List[str] = []
    label = brief.get("venue_label") or brief.get("venue_id") or "Venue"
    date = brief.get("date") or ""
    lines.append(f"RosterIQ — Morning Brief for {label} ({date})")
    lines.append("=" * 60)
    lines.append("")
    lines.append(brief.get("headline") or "")
    lines.append("")

    rc = brief.get("recap_context") or {}
    if rc:
        rev_actual = _fmt_money(rc.get("revenue_actual", 0))
        rev_delta = _fmt_pct(float(rc.get("revenue_delta_pct") or 0), signed=True)
        wage_pct = _fmt_pct(float(rc.get("wage_pct_actual") or 0))
        wage_delta = _fmt_pct(
            float(rc.get("wage_pct_delta") or 0), signed=True
        ).replace("%", "pt")
        lines.append(f"Revenue: {rev_actual} ({rev_delta} vs forecast)")
        lines.append(f"Wage %:  {wage_pct} ({wage_delta} vs target)")
        lines.append(f"Peak head count: {rc.get('peak_headcount', 0)}")
        lines.append("")

    top = brief.get("top_dismissed") or []
    if top:
        lines.append("Top dismissed recs:")
        for i, t in enumerate(top, start=1):
            impact = _fmt_money(t.get("impact_estimate_aud") or 0)
            text = str(t.get("text") or "")
            first = text.split(".")[0]
            if len(first) > 110:
                first = first[:107] + "..."
            lines.append(f"  {i}. ~{impact}  {first}")
        lines.append("")

    one_thing = brief.get("one_thing") or ""
    if one_thing:
        lines.append("Do differently today:")
        lines.append(f"  {one_thing}")
        lines.append("")

    return "\n".join(lines).rstrip() + "\n"


# ---------------------------------------------------------------------------
# Convenience: pull events from accountability_store (injectable)
# ---------------------------------------------------------------------------

def compose_brief_from_store(
    venue_id: str,
    *,
    target_date: Optional[str] = None,
    yesterday_recap: Optional[Dict[str, Any]] = None,
    venue_label: Optional[str] = None,
    store: Any = None,
) -> Dict[str, Any]:
    """Pull events from ``accountability_store`` (or an injected stub)
    and call ``compose_brief``. Separate entry point so tests can drive
    ``compose_brief`` directly without touching the module-global store."""
    if store is None:
        from rosteriq import accountability_store as store  # lazy import
    events = store.history(venue_id)
    return compose_brief(
        venue_id,
        target_date=target_date,
        events=list(events or []),
        yesterday_recap=yesterday_recap,
        venue_label=venue_label,
    )
