"""Daily digest content builder — assembles pre-shift intelligence briefs.

Gathers tomorrow's forecast, signals, weather alerts, and suggested actions
into a formatted brief ready for dispatch via email, SMS, or webhook.

Pure stdlib. No FastAPI, no Pydantic, no hard external deps. Tests live in
tests/test_daily_digest.py.

This module powers the pre-shift intelligence system, giving managers a
consolidated view of what to expect, what might change, and what to watch
for in real-time during the shift.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

logger = logging.getLogger("rosteriq.daily_digest")


# ---------------------------------------------------------------------------
# Data helpers
# ---------------------------------------------------------------------------

def _today() -> date:
    """Return today's date (UTC)."""
    return datetime.now(timezone.utc).date()


def _tomorrow() -> date:
    """Return tomorrow's date (UTC)."""
    return _today() + timedelta(days=1)


def _yesterday() -> date:
    """Return yesterday's date (UTC)."""
    return _today() - timedelta(days=1)


def _now_iso() -> str:
    """Return current timestamp in ISO format."""
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _day_of_week(d: date) -> str:
    """Return day of week name (Monday, Tuesday, etc.)."""
    days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    return days[d.weekday()]


def _classify_day_type(d: date) -> str:
    """Classify day as weekday, friday, saturday, or sunday."""
    dow = d.weekday()
    if dow == 4:  # Friday
        return "friday"
    elif dow == 5:  # Saturday
        return "saturday"
    elif dow == 6:  # Sunday
        return "sunday"
    else:
        return "weekday"


def _fmt_money(n: float) -> str:
    """Format as currency."""
    n = float(n or 0.0)
    if abs(n) >= 1000:
        return f"${n / 1000:.1f}k"
    return f"${round(n):,.0f}"


def _fmt_pct(decimal: float, *, signed: bool = False) -> str:
    """Format as percentage."""
    pts = (decimal or 0.0) * 100
    sign = "+" if (signed and pts >= 0) else ""
    return f"{sign}{pts:.1f}%"


# ---------------------------------------------------------------------------
# Forecast summary builder
# ---------------------------------------------------------------------------

def _build_forecast_summary(
    venue_id: str,
    target_date: date,
    history_store=None,
) -> Dict[str, Any]:
    """Build a forecast summary for target_date.

    Pulls same-day-last-week from tanda history as a baseline.
    Returns expected revenue range, headcount, day-of-week pattern.

    Args:
        venue_id: The venue ID.
        target_date: Date to forecast for.
        history_store: Optional tanda history store.

    Returns:
        Dict with keys:
        - expected_revenue_low, expected_revenue_high
        - expected_covers (headcount estimate)
        - day_type (weekday|friday|saturday|sunday)
        - vs_last_week (percent change vs same day last week, or None)
        - limited_data (bool: True if no history available)
    """
    day_type = _classify_day_type(target_date)

    # If no history store, return sensible defaults
    if not history_store:
        return {
            "expected_revenue_low": 15000,
            "expected_revenue_high": 25000,
            "expected_covers": 120,
            "day_type": day_type,
            "vs_last_week": None,
            "limited_data": True,
        }

    try:
        # Try to get same-day-last-week
        last_week = target_date - timedelta(days=7)
        last_week_day = history_store.get_daily_actuals(venue_id, last_week)

        if last_week_day:
            # Use last week as baseline
            base_rev = float(last_week_day.actual_revenue or 0)
            base_headcount = int(last_week_day.employee_count or 0)

            # Apply day-of-week multiplier heuristic
            # (Peak days tend to be 15-20% busier)
            if day_type in ("friday", "saturday"):
                mid_point = base_rev * 1.12
            elif day_type == "sunday":
                mid_point = base_rev * 0.95
            else:
                mid_point = base_rev

            low = int(mid_point * 0.9)
            high = int(mid_point * 1.1)

            return {
                "expected_revenue_low": low,
                "expected_revenue_high": high,
                "expected_covers": base_headcount,
                "day_type": day_type,
                "vs_last_week": f"+0%",  # baseline is last week
                "limited_data": False,
            }
    except Exception as e:
        logger.warning(f"Failed to fetch forecast for {venue_id}/{target_date}: {e}")

    # Fallback to defaults with limited_data flag
    return {
        "expected_revenue_low": 15000,
        "expected_revenue_high": 25000,
        "expected_covers": 120,
        "day_type": day_type,
        "vs_last_week": None,
        "limited_data": True,
    }


# ---------------------------------------------------------------------------
# Weather alert builder
# ---------------------------------------------------------------------------

def _build_weather_alert(
    venue_id: str,
    target_date: date,
) -> Optional[Dict[str, Any]]:
    """Build a weather alert for target_date.

    Tries to import weather adapter. Checks for rain > 50%, temp > 35C or < 10C.
    Best effort — returns None on failure.

    Args:
        venue_id: The venue ID.
        target_date: Date to get weather for.

    Returns:
        Dict with keys: condition, temperature, rain_chance, impact
        Or None if weather unavailable or no alerts.
    """
    try:
        # Lazy import of weather adapter
        from rosteriq import weather_adapter as _weather_adapter
    except (ImportError, ModuleNotFoundError):
        return None

    try:
        forecast = _weather_adapter.get_forecast(venue_id, target_date)
        if not forecast:
            return None

        temp = float(forecast.get("temperature") or 20)
        rain = float(forecast.get("rain_chance") or 0)
        condition = str(forecast.get("condition") or "clear")

        # Determine if there's an alert
        has_alert = False
        impact_text = ""

        if rain > 50:
            has_alert = True
            impact_text = f"Rain {rain:.0f}% likely — expect 20-40% drop in outdoor covers"
        elif temp > 35:
            has_alert = True
            impact_text = f"Hot day ({temp:.0f}C) — monitor staff hydration, expect quieter early hours"
        elif temp < 10:
            has_alert = True
            impact_text = f"Cold ({temp:.0f}C) — boost indoor heating, expect reduced foot traffic"

        if not has_alert:
            return None

        return {
            "condition": condition,
            "temperature": round(temp, 1),
            "rain_chance": round(rain, 0),
            "impact": impact_text,
        }

    except Exception as e:
        logger.warning(f"Weather alert failed for {venue_id}: {e}")
        return None


# ---------------------------------------------------------------------------
# Yesterday recap builder
# ---------------------------------------------------------------------------

def _build_yesterday_recap(
    venue_id: str,
    yesterday: date,
    history_store=None,
) -> Dict[str, Any]:
    """Build a recap of yesterday's actuals.

    Pulls from tanda history. Returns revenue, labour cost, labour %, variance.

    Args:
        venue_id: The venue ID.
        yesterday: The date to recap (usually yesterday).
        history_store: Optional tanda history store.

    Returns:
        Dict with keys:
        - revenue, labour_cost, labour_pct (all as floats)
        - variance_hours
        - performance (on_target|over_rostered|under_rostered)
    """
    if not history_store:
        return {
            "revenue": 0,
            "labour_cost": 0,
            "labour_pct": 0,
            "variance_hours": 0,
            "performance": "on_target",
        }

    try:
        actuals = history_store.get_daily_actuals(venue_id, yesterday)
        if not actuals:
            return {
                "revenue": 0,
                "labour_cost": 0,
                "labour_pct": 0,
                "variance_hours": 0,
                "performance": "on_target",
            }

        revenue = float(actuals.actual_revenue or 0)
        labour_cost = float(actuals.worked_cost or 0)
        labour_pct = actuals.labour_pct or 0.0
        variance_hours = float(actuals.variance_hours or 0)

        # Classify performance
        if variance_hours < -1.0:
            performance = "under_rostered"
        elif variance_hours > 1.0:
            performance = "over_rostered"
        else:
            performance = "on_target"

        return {
            "revenue": round(revenue, 2),
            "labour_cost": round(labour_cost, 2),
            "labour_pct": round(labour_pct, 2),
            "variance_hours": round(variance_hours, 2),
            "performance": performance,
        }

    except Exception as e:
        logger.warning(f"Yesterday recap failed for {venue_id}: {e}")
        return {
            "revenue": 0,
            "labour_cost": 0,
            "labour_pct": 0,
            "variance_hours": 0,
            "performance": "on_target",
        }


# ---------------------------------------------------------------------------
# Signal collection
# ---------------------------------------------------------------------------

def _collect_signals(
    venue_id: str,
    target_date: date,
) -> List[Dict[str, Any]]:
    """Collect signals (events, sports, holidays) for target_date.

    Best effort. Returns empty list on failure.

    Returns:
        List of dicts with keys: source, type, summary, impact (positive|negative|neutral)
    """
    signals: List[Dict[str, Any]] = []

    # Day-of-week signals
    day_type = _classify_day_type(target_date)
    if day_type == "friday":
        signals.append({
            "source": "calendar",
            "type": "day_of_week",
            "summary": "Friday — typically busy evening",
            "impact": "positive",
        })
    elif day_type == "saturday":
        signals.append({
            "source": "calendar",
            "type": "day_of_week",
            "summary": "Saturday — peak day ahead",
            "impact": "positive",
        })
    elif day_type == "sunday":
        signals.append({
            "source": "calendar",
            "type": "day_of_week",
            "summary": "Sunday — reduced hours expected",
            "impact": "neutral",
        })

    # Try to load external signals (events, sports, holidays)
    try:
        from rosteriq import event_signals as _events
        venue_signals = _events.get_signals_for_date(venue_id, target_date)
        if venue_signals:
            signals.extend(venue_signals)
    except Exception:
        pass

    return signals


# ---------------------------------------------------------------------------
# Shift notes collection
# ---------------------------------------------------------------------------

def _collect_shift_notes(
    venue_id: str,
    days_back: int = 3,
    note_store=None,
) -> List[Dict[str, Any]]:
    """Collect recent shift notes for the venue.

    Args:
        venue_id: The venue ID.
        days_back: How many days of history to pull.
        note_store: Optional shift note store.

    Returns:
        List of note dicts with keys: author_name, content, tags, created_at
    """
    if not note_store:
        return []

    try:
        notes = note_store.list_recent(venue_id, limit=days_back * 5)
        return [
            {
                "author_name": n.author_name,
                "content": n.content,
                "tags": list(n.tags),
                "created_at": n.created_at.isoformat() if hasattr(n.created_at, "isoformat") else str(n.created_at),
            }
            for n in (notes or [])
        ]
    except Exception as e:
        logger.warning(f"Failed to collect shift notes for {venue_id}: {e}")
        return []


# ---------------------------------------------------------------------------
# Pending swaps collection
# ---------------------------------------------------------------------------

def _collect_pending_swaps(
    venue_id: str,
    swap_store=None,
) -> List[Dict[str, Any]]:
    """Collect pending shift swaps awaiting manager approval.

    Args:
        venue_id: The venue ID.
        swap_store: Optional shift swap store.

    Returns:
        List of swap dicts with keys: offered_by_name, shift_date, shift_start,
        shift_end, role, status
    """
    if not swap_store:
        return []

    try:
        swaps = swap_store.list_pending_review(venue_id)
        return [
            {
                "offered_by_name": s.offered_by_name,
                "shift_date": s.shift_date,
                "shift_start": s.shift_start,
                "shift_end": s.shift_end,
                "role": s.role,
                "status": s.status.value if hasattr(s.status, "value") else str(s.status),
            }
            for s in (swaps or [])
        ]
    except Exception as e:
        logger.warning(f"Failed to collect pending swaps for {venue_id}: {e}")
        return []


# ---------------------------------------------------------------------------
# Suggested actions
# ---------------------------------------------------------------------------

def _suggest_actions(
    forecast: Dict[str, Any],
    weather: Optional[Dict[str, Any]],
    yesterday: Dict[str, Any],
    signals: List[Dict[str, Any]],
    pending_swaps: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Generate rule-based suggestions.

    Args:
        forecast: From _build_forecast_summary
        weather: From _build_weather_alert or None
        yesterday: From _build_yesterday_recap
        signals: From _collect_signals
        pending_swaps: From _collect_pending_swaps

    Returns:
        List of action dicts with keys: priority (high|medium|low), action, reason
    """
    actions: List[Dict[str, Any]] = []

    # Weather-based suggestions
    if weather:
        rain = float(weather.get("rain_chance") or 0)
        if rain > 70:
            actions.append({
                "priority": "high",
                "action": "Reduce outdoor floor staff allocation",
                "reason": f"Heavy rain forecast ({rain:.0f}%)",
            })

    # Yesterday's performance feedback
    perf = yesterday.get("performance")
    if perf == "over_rostered":
        variance = float(yesterday.get("variance_hours") or 0)
        actions.append({
            "priority": "medium",
            "action": f"Yesterday was {abs(variance):.1f}h over-rostered — tighten rostering today",
            "reason": "Labour efficiency opportunity",
        })
    elif perf == "under_rostered":
        variance = float(yesterday.get("variance_hours") or 0)
        actions.append({
            "priority": "medium",
            "action": f"Yesterday was {abs(variance):.1f}h under-rostered — ensure full coverage today",
            "reason": "Staff burnout risk",
        })

    # Day-of-week signals
    day_type = forecast.get("day_type")
    if day_type in ("friday", "saturday"):
        actions.append({
            "priority": "high",
            "action": "Peak day — ensure full bar and floor coverage",
            "reason": f"{day_type.capitalize()} typically busy",
        })

    # Negative signal events
    for sig in signals:
        if sig.get("impact") == "negative":
            actions.append({
                "priority": "medium",
                "action": f"Monitor: {sig.get('summary')}",
                "reason": sig.get("source", "event"),
            })

    # Pending swaps
    if pending_swaps:
        actions.append({
            "priority": "medium",
            "action": f"{len(pending_swaps)} shift swap(s) pending your approval",
            "reason": "Manager action required",
        })

    # Sort by priority
    priority_order = {"high": 0, "medium": 1, "low": 2}
    actions.sort(key=lambda a: priority_order.get(a.get("priority", "low"), 2))

    return actions


# ---------------------------------------------------------------------------
# Main digest builder
# ---------------------------------------------------------------------------

def build_digest(
    venue_id: str,
    target_date: Optional[date] = None,
    history_store=None,
    headcount_store=None,
    note_store=None,
    swap_store=None,
) -> Dict[str, Any]:
    """Assemble a complete daily digest for a venue.

    Args:
        venue_id: The venue ID.
        target_date: Date to generate digest for (defaults to tomorrow).
        history_store: Optional tanda history store (from tanda_history.get_history_store).
        headcount_store: Optional headcount store (from headcount.get_headcount_store).
        note_store: Optional shift note store (from headcount.get_shift_note_store).
        swap_store: Optional swap store (from shift_swap.get_swap_store).

    Returns:
        Dict with keys:
        - venue_id, target_date, generated_at
        - sections: {
            forecast_summary, weather_alert, signals, yesterday_recap,
            shift_notes, pending_swaps, suggested_actions
          }
    """
    if target_date is None:
        target_date = _tomorrow()

    yesterday = target_date - timedelta(days=1)

    # Build each section
    forecast_summary = _build_forecast_summary(venue_id, target_date, history_store)
    weather_alert = _build_weather_alert(venue_id, target_date)
    signals = _collect_signals(venue_id, target_date)
    yesterday_recap = _build_yesterday_recap(venue_id, yesterday, history_store)
    shift_notes = _collect_shift_notes(venue_id, days_back=3, note_store=note_store)
    pending_swaps = _collect_pending_swaps(venue_id, swap_store=swap_store)

    # Generate suggested actions
    suggested_actions = _suggest_actions(
        forecast_summary,
        weather_alert,
        yesterday_recap,
        signals,
        pending_swaps,
    )

    return {
        "venue_id": venue_id,
        "target_date": target_date.isoformat(),
        "generated_at": _now_iso(),
        "sections": {
            "forecast_summary": forecast_summary,
            "weather_alert": weather_alert,
            "signals": signals,
            "yesterday_recap": yesterday_recap,
            "shift_notes": shift_notes,
            "pending_swaps": pending_swaps,
            "suggested_actions": suggested_actions,
        },
    }


# ---------------------------------------------------------------------------
# Text formatting
# ---------------------------------------------------------------------------

def format_digest_text(digest: Dict[str, Any]) -> str:
    """Render digest dict to plain text suitable for email or SMS.

    Args:
        digest: From build_digest.

    Returns:
        Plain text string, ~500 words max.
    """
    venue_id = digest.get("venue_id", "Venue")
    target_date = digest.get("target_date", "")
    sections = digest.get("sections", {})

    lines: List[str] = []
    lines.append("=" * 60)
    lines.append(f"RosterIQ Pre-Shift Brief")
    lines.append(f"Venue: {venue_id}")
    lines.append(f"Date: {target_date}")
    lines.append("=" * 60)
    lines.append("")

    # Forecast summary
    forecast = sections.get("forecast_summary", {})
    if forecast:
        lines.append("FORECAST SUMMARY")
        lines.append(f"  Day type: {forecast.get('day_type', 'unknown').upper()}")
        lines.append(f"  Expected revenue: {_fmt_money(forecast.get('expected_revenue_low', 0))}"
                     f" - {_fmt_money(forecast.get('expected_revenue_high', 0))}")
        lines.append(f"  Expected covers: {forecast.get('expected_covers', 0)}")
        if not forecast.get("limited_data"):
            lines.append(f"  vs. last week: {forecast.get('vs_last_week', 'N/A')}")
        lines.append("")

    # Weather alert
    weather = sections.get("weather_alert")
    if weather:
        lines.append("WEATHER ALERT")
        lines.append(f"  Condition: {weather.get('condition', 'unknown')}")
        lines.append(f"  Temperature: {weather.get('temperature', 'N/A')}C")
        lines.append(f"  Rain chance: {weather.get('rain_chance', 0):.0f}%")
        lines.append(f"  Impact: {weather.get('impact', 'Monitor')}")
        lines.append("")

    # Yesterday recap
    yesterday = sections.get("yesterday_recap", {})
    if yesterday and yesterday.get("revenue", 0) > 0:
        lines.append("YESTERDAY'S PERFORMANCE")
        lines.append(f"  Revenue: {_fmt_money(yesterday.get('revenue', 0))}")
        lines.append(f"  Labour cost: {_fmt_money(yesterday.get('labour_cost', 0))}"
                     f" ({_fmt_pct(yesterday.get('labour_pct', 0) / 100)})")
        lines.append(f"  Variance: {yesterday.get('variance_hours', 0):+.1f}h ({yesterday.get('performance', 'unknown')})")
        lines.append("")

    # Signals
    signals = sections.get("signals", [])
    if signals:
        lines.append("SIGNALS & EVENTS")
        for sig in signals:
            lines.append(f"  * {sig.get('summary', 'Event')} ({sig.get('impact', 'neutral')})")
        lines.append("")

    # Shift notes
    notes = sections.get("shift_notes", [])
    if notes:
        lines.append("RECENT SHIFT NOTES")
        for note in notes[:3]:  # Limit to 3 most recent
            lines.append(f"  * {note.get('author_name', 'Anonymous')}: {note.get('content', '')}")
        lines.append("")

    # Pending swaps
    swaps = sections.get("pending_swaps", [])
    if swaps:
        lines.append("PENDING SWAPS")
        lines.append(f"  {len(swaps)} swap(s) need your approval:")
        for swap in swaps[:3]:  # Limit to 3
            lines.append(f"    - {swap.get('offered_by_name', 'Staff')} on {swap.get('shift_date', 'TBD')}"
                         f" ({swap.get('role', 'role')})")
        lines.append("")

    # Suggested actions
    actions = sections.get("suggested_actions", [])
    if actions:
        lines.append("SUGGESTED ACTIONS")
        for action in actions[:5]:  # Limit to 5 actions
            priority = action.get("priority", "low").upper()
            lines.append(f"  [{priority}] {action.get('action', 'Action')}")
            lines.append(f"       → {action.get('reason', '')}")
        lines.append("")

    lines.append("=" * 60)
    lines.append("RosterIQ | Pre-Shift Intelligence")
    lines.append("=" * 60)

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# HTML formatting
# ---------------------------------------------------------------------------

def format_digest_html(digest: Dict[str, Any]) -> str:
    """Render digest dict to HTML email body.

    Args:
        digest: From build_digest.

    Returns:
        HTML string with inline styles, mobile-friendly.
    """
    venue_id = digest.get("venue_id", "Venue")
    target_date = digest.get("target_date", "")
    sections = digest.get("sections", {})

    forecast = sections.get("forecast_summary", {})
    weather = sections.get("weather_alert")
    yesterday = sections.get("yesterday_recap", {})
    signals = sections.get("signals", [])
    notes = sections.get("shift_notes", [])
    swaps = sections.get("pending_swaps", [])
    actions = sections.get("suggested_actions", [])

    html = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <style>
        body {{
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, Arial, sans-serif;
            color: #333;
            background: #f5f5f5;
            margin: 0;
            padding: 20px;
        }}
        .container {{
            max-width: 600px;
            margin: 0 auto;
            background: white;
            border-radius: 8px;
            overflow: hidden;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        .header {{
            background: linear-gradient(135deg, #1e3a8a 0%, #2563eb 100%);
            color: white;
            padding: 30px 20px;
            text-align: center;
        }}
        .header h1 {{
            margin: 0 0 8px 0;
            font-size: 24px;
            font-weight: bold;
        }}
        .header p {{
            margin: 0;
            font-size: 14px;
            opacity: 0.9;
        }}
        .section {{
            padding: 20px;
            border-bottom: 1px solid #f0f0f0;
        }}
        .section h2 {{
            margin: 0 0 12px 0;
            font-size: 16px;
            font-weight: 600;
            color: #1e3a8a;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }}
        .section p {{
            margin: 8px 0;
            font-size: 14px;
            line-height: 1.6;
        }}
        .forecast-item {{
            background: #f9fafb;
            padding: 12px;
            border-radius: 4px;
            margin: 8px 0;
            border-left: 3px solid #2563eb;
        }}
        .alert {{
            background: #fef3c7;
            border-left: 3px solid #f59e0b;
            padding: 12px;
            margin: 12px 0;
            border-radius: 4px;
        }}
        .action {{
            background: #eff6ff;
            border-left: 3px solid #3b82f6;
            padding: 12px;
            margin: 8px 0;
            border-radius: 4px;
        }}
        .action.high {{
            border-left-color: #ef4444;
            background: #fef2f2;
        }}
        .action-priority {{
            display: inline-block;
            font-weight: 600;
            font-size: 12px;
            padding: 2px 6px;
            border-radius: 3px;
            margin-right: 8px;
        }}
        .action-priority.high {{ background: #fee2e2; color: #dc2626; }}
        .action-priority.medium {{ background: #fef3c7; color: #d97706; }}
        .action-priority.low {{ background: #f0fdf4; color: #059669; }}
        .note {{
            background: #f9fafb;
            padding: 10px;
            margin: 8px 0;
            border-radius: 4px;
            font-size: 13px;
            border-left: 3px solid #6b7280;
        }}
        .footer {{
            background: #f9fafb;
            padding: 20px;
            text-align: center;
            border-top: 1px solid #f0f0f0;
            font-size: 12px;
            color: #6b7280;
        }}
        .divider {{
            height: 1px;
            background: #e5e7eb;
            margin: 12px 0;
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>RosterIQ Pre-Shift Brief</h1>
            <p>{venue_id} • {target_date}</p>
        </div>

        <div class="section">
            <h2>Forecast Summary</h2>
            <div class="forecast-item">
                <strong>Day Type:</strong> {forecast.get('day_type', 'Unknown').upper()}<br>
                <strong>Expected Revenue:</strong> {_fmt_money(forecast.get('expected_revenue_low', 0))} - {_fmt_money(forecast.get('expected_revenue_high', 0))}<br>
                <strong>Expected Covers:</strong> {forecast.get('expected_covers', 0)}
                {f"<br><strong>vs Last Week:</strong> {forecast.get('vs_last_week', 'N/A')}" if not forecast.get('limited_data') else ""}
            </div>
        </div>
"""

    if weather:
        html += f"""        <div class="section">
            <h2>Weather Alert</h2>
            <div class="alert">
                <strong>{weather.get('condition', 'Weather Update')}</strong><br>
                Temperature: {weather.get('temperature', 'N/A')}C | Rain: {weather.get('rain_chance', 0):.0f}%<br>
                <div style="margin-top: 8px; font-weight: 500;">{weather.get('impact', 'Monitor conditions')}</div>
            </div>
        </div>
"""

    if yesterday and yesterday.get("revenue", 0) > 0:
        html += f"""        <div class="section">
            <h2>Yesterday's Performance</h2>
            <div class="forecast-item">
                <strong>Revenue:</strong> {_fmt_money(yesterday.get('revenue', 0))}<br>
                <strong>Labour Cost:</strong> {_fmt_money(yesterday.get('labour_cost', 0))} ({_fmt_pct(yesterday.get('labour_pct', 0) / 100)})<br>
                <strong>Variance:</strong> {yesterday.get('variance_hours', 0):+.1f}h <em>({yesterday.get('performance', 'on_target')})</em>
            </div>
        </div>
"""

    if signals:
        html += f"""        <div class="section">
            <h2>Signals & Events</h2>
"""
        for sig in signals:
            impact_color = {"positive": "#10b981", "negative": "#ef4444", "neutral": "#6b7280"}
            color = impact_color.get(sig.get("impact"), "#6b7280")
            html += f'            <p style="margin: 8px 0; padding-left: 12px; border-left: 3px solid {color};">{sig.get("summary", "Event")} <em>({sig.get("impact", "neutral")})</em></p>\n'
        html += """        </div>
"""

    if notes:
        html += """        <div class="section">
            <h2>Recent Shift Notes</h2>
"""
        for note in notes[:3]:
            html += f'            <div class="note"><strong>{note.get("author_name", "Staff")}:</strong> {note.get("content", "")}</div>\n'
        html += """        </div>
"""

    if swaps:
        html += f"""        <div class="section">
            <h2>Pending Swaps ({len(swaps)})</h2>
            <p>Action required on the following shift swaps:</p>
"""
        for swap in swaps[:3]:
            html += f'            <div class="forecast-item">{swap.get("offered_by_name", "Staff")} • {swap.get("shift_date", "TBD")} ({swap.get("role", "role")})<br><small>{swap.get("shift_start", "")}-{swap.get("shift_end", "")}</small></div>\n'
        html += """        </div>
"""

    if actions:
        html += """        <div class="section">
            <h2>Suggested Actions</h2>
"""
        for action in actions[:5]:
            priority = action.get("priority", "low").lower()
            html += f'            <div class="action {priority}"><span class="action-priority {priority}">{priority.upper()}</span> {action.get("action", "Action")}<br><small>{action.get("reason", "")}</small></div>\n'
        html += """        </div>
"""

    html += """        <div class="footer">
            <p>This is an automated message from RosterIQ | Pre-Shift Intelligence System</p>
            <p style="margin-top: 8px; color: #9ca3af;">Generated for operational guidance. Review before relying on critical decisions.</p>
        </div>
    </div>
</body>
</html>
"""

    return html
