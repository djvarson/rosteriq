"""Shift recap composer — pure-stdlib, no FastAPI / Pydantic / pipeline imports.

Given a bundle of already-fetched numbers (revenue, wages, head-count history),
compose an end-of-shift recap that the dashboard can render as KPI tiles and
a one-line natural-language summary.

Separating this out from the API layer means the whole thing can be unit
tested with plain dicts — no async, no adapters, no environment.

Shape of a recap dict:

    {
        "venue_id": "...",
        "shift_date": "YYYY-MM-DD",
        "generated_at": "2026-04-11T12:34:56Z",
        "revenue": {
            "actual": float,
            "forecast": float,
            "delta": float,         # actual - forecast
            "delta_pct": float,     # (actual - forecast) / forecast, 0 if forecast == 0
        },
        "wages": {
            "actual": float,
            "forecast": float,
            "delta": float,
            "pct_of_revenue_actual": float,   # actual / revenue_actual, 0 if revenue_actual == 0
            "pct_of_revenue_target": float,   # supplied target, e.g. 0.30
            "pct_delta": float,               # pct_of_revenue_actual - target
        },
        "headcount": {
            "peak": int,
            "peak_time": "HH:MM" | None,
            "last_count": int,
            "total_taps": int,
            "reset_count": int,
        },
        "traffic_light": "green" | "amber" | "red",
        "summary": "...one-line english...",
    }

The traffic light logic is deterministic: green when both revenue beats
forecast and wage % is at-or-under target; red when either revenue misses
forecast by more than 5% OR wage % overshoots target by more than 2 points;
amber otherwise.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


DEFAULT_WAGE_TARGET_PCT = 0.30
WAGE_TO_REVENUE_FALLBACK = 0.30  # fallback forecast wage burn if none supplied
AMBER_REVENUE_MISS = 0.05        # -5% miss on revenue → at least amber
RED_REVENUE_MISS = 0.10          # -10% miss on revenue → red
RED_WAGE_OVERSHOOT = 0.02        # +2pt wage overshoot → red
AMBER_WAGE_OVERSHOOT = 0.005     # +0.5pt wage overshoot → amber


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        if value is None:
            return default
        return int(value)
    except (TypeError, ValueError):
        return default


def summarise_headcount(history: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Reduce a head-count history list (newest-last or newest-first OK) to
    peak / peak_time / last_count / tap & reset totals.

    Accepts the entry shape emitted by rosteriq.headcount_store:
        {timestamp, delta, count_after, note, source}
    """
    if not history:
        return {
            "peak": 0,
            "peak_time": None,
            "last_count": 0,
            "total_taps": 0,
            "reset_count": 0,
        }

    # Normalise: work on chronological order regardless of input direction.
    sorted_hist = sorted(history, key=lambda e: e.get("timestamp", ""))

    peak = 0
    peak_ts: Optional[str] = None
    total_taps = 0
    reset_count = 0

    for entry in sorted_hist:
        count = _safe_int(entry.get("count_after"))
        source = str(entry.get("source", ""))
        if count > peak:
            peak = count
            peak_ts = entry.get("timestamp")
        if source == "button" or source == "group":
            total_taps += 1
        if source == "reset":
            reset_count += 1

    # Last entry is the most recent count.
    last_count = _safe_int(sorted_hist[-1].get("count_after"))

    peak_time_str: Optional[str] = None
    if peak_ts:
        # timestamps are ISO e.g. 2026-04-11T22:04:15Z — pull the HH:MM slice
        # without depending on a parser so we keep stdlib-only and tolerant.
        t_part = peak_ts.split("T", 1)[1] if "T" in peak_ts else peak_ts
        peak_time_str = t_part[:5] if len(t_part) >= 5 else t_part

    return {
        "peak": peak,
        "peak_time": peak_time_str,
        "last_count": last_count,
        "total_taps": total_taps,
        "reset_count": reset_count,
    }


def _classify(
    revenue_delta_pct: float,
    wage_pct_delta: float,
) -> str:
    # Revenue side
    if revenue_delta_pct <= -RED_REVENUE_MISS:
        revenue_light = "red"
    elif revenue_delta_pct <= -AMBER_REVENUE_MISS:
        revenue_light = "amber"
    else:
        revenue_light = "green"

    # Wage side
    if wage_pct_delta >= RED_WAGE_OVERSHOOT:
        wage_light = "red"
    elif wage_pct_delta >= AMBER_WAGE_OVERSHOOT:
        wage_light = "amber"
    else:
        wage_light = "green"

    # Worst-of
    order = {"green": 0, "amber": 1, "red": 2}
    worst = max(revenue_light, wage_light, key=lambda lv: order[lv])
    return worst


def _compose_summary(
    *,
    revenue_actual: float,
    revenue_forecast: float,
    revenue_delta_pct: float,
    wage_pct_actual: float,
    wage_pct_target: float,
    headcount_peak: int,
    traffic_light: str,
) -> str:
    # Round-trip friendly human figures
    def _fmt_money(v: float) -> str:
        if v >= 1000:
            return f"${v/1000:.1f}k"
        return f"${v:,.0f}"

    rev_str = _fmt_money(revenue_actual)
    fcast_str = _fmt_money(revenue_forecast)
    delta_sign = "+" if revenue_delta_pct >= 0 else ""
    delta_str = f"{delta_sign}{revenue_delta_pct * 100:.1f}%"
    wage_actual_pts = wage_pct_actual * 100
    wage_target_pts = wage_pct_target * 100
    wage_diff = wage_actual_pts - wage_target_pts

    if traffic_light == "green":
        lead = "Clean shift"
    elif traffic_light == "amber":
        lead = "Mixed shift"
    else:
        lead = "Tough shift"

    if revenue_forecast > 0:
        rev_phrase = f"{rev_str} vs {fcast_str} forecast ({delta_str})"
    else:
        rev_phrase = f"{rev_str} booked (no forecast)"

    wage_phrase = (
        f"wages landed at {wage_actual_pts:.1f}% of sales "
        f"({'+' if wage_diff >= 0 else ''}{wage_diff:.1f}pt vs {wage_target_pts:.0f}% target)"
    )

    headcount_phrase = f"peak {headcount_peak} people on deck" if headcount_peak > 0 else "no head-count logged"

    return f"{lead}: {rev_phrase}, {wage_phrase}, {headcount_phrase}."


def compose_recap(
    *,
    venue_id: str,
    shift_date: str,
    revenue_actual: Any,
    revenue_forecast: Any,
    wages_actual: Any = None,
    wages_forecast: Any = None,
    wage_target_pct: float = DEFAULT_WAGE_TARGET_PCT,
    headcount_history: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """Pure composer — takes already-fetched numbers, returns a recap dict.

    `wages_actual` / `wages_forecast` can be None, in which case we fall back
    to WAGE_TO_REVENUE_FALLBACK * revenue for the forecast, and treat actual
    wages as the same fraction of actual revenue. This lets the recap still
    render something useful before SwiftPOS / Tanda wage feeds are wired.
    """
    rev_a = _safe_float(revenue_actual)
    rev_f = _safe_float(revenue_forecast)
    rev_delta = rev_a - rev_f
    rev_delta_pct = (rev_delta / rev_f) if rev_f > 0 else 0.0

    if wages_forecast is None or _safe_float(wages_forecast) <= 0:
        wg_f = rev_f * WAGE_TO_REVENUE_FALLBACK
    else:
        wg_f = _safe_float(wages_forecast)

    if wages_actual is None or _safe_float(wages_actual) <= 0:
        wg_a = rev_a * WAGE_TO_REVENUE_FALLBACK
    else:
        wg_a = _safe_float(wages_actual)

    wg_pct_actual = (wg_a / rev_a) if rev_a > 0 else 0.0
    wg_pct_delta = wg_pct_actual - wage_target_pct

    hc_summary = summarise_headcount(headcount_history or [])

    light = _classify(rev_delta_pct, wg_pct_delta)

    summary = _compose_summary(
        revenue_actual=rev_a,
        revenue_forecast=rev_f,
        revenue_delta_pct=rev_delta_pct,
        wage_pct_actual=wg_pct_actual,
        wage_pct_target=wage_target_pct,
        headcount_peak=hc_summary["peak"],
        traffic_light=light,
    )

    return {
        "venue_id": venue_id,
        "shift_date": shift_date,
        "generated_at": _now_iso(),
        "revenue": {
            "actual": round(rev_a, 2),
            "forecast": round(rev_f, 2),
            "delta": round(rev_delta, 2),
            "delta_pct": round(rev_delta_pct, 4),
        },
        "wages": {
            "actual": round(wg_a, 2),
            "forecast": round(wg_f, 2),
            "delta": round(wg_a - wg_f, 2),
            "pct_of_revenue_actual": round(wg_pct_actual, 4),
            "pct_of_revenue_target": round(wage_target_pct, 4),
            "pct_delta": round(wg_pct_delta, 4),
        },
        "headcount": hc_summary,
        "traffic_light": light,
        "summary": summary,
    }
