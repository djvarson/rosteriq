"""Multi-venue portfolio roll-up for group operators (Moment 9 / Tier 3).

Takes a list of per-venue shift recaps (each already produced by
``rosteriq.shift_recap.compose_recap``) and rolls them into a single
portfolio view with:

* Aggregated revenue, wages, wage %, peak head count, and accountability
* A worst-of traffic light across all venues
* A one-line English summary deterministic enough to use in a report
* A per-venue mini-recap array for the dashboard's sub-cards

Pure stdlib. No FastAPI, no Pydantic, no IO. Tests live in
``tests/test_portfolio_recap.py``.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

LIGHT_RANK = {"green": 0, "amber": 1, "red": 2, "unknown": -1}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _safe(d: Dict[str, Any], *path: str, default: float = 0.0) -> float:
    """Walk a nested dict path, returning ``default`` if any hop is missing
    or not castable to float."""
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


def _fmt_money(n: float) -> str:
    n = float(n or 0.0)
    if abs(n) >= 1000:
        return f"${n / 1000:.1f}k"
    return f"${round(n):,.0f}"


def _fmt_pct(decimal: float, *, signed: bool = False) -> str:
    pts = (decimal or 0.0) * 100
    sign = "+" if (signed and pts >= 0) else ""
    return f"{sign}{pts:.1f}%"


def _worst_light(lights: Iterable[str]) -> str:
    """Return the highest-severity light from a list (worst-of).
    Unknown lights do not count toward the worst-of — an all-unknown
    portfolio returns 'unknown'."""
    ranks = [LIGHT_RANK.get((l or "").lower(), -1) for l in lights]
    known = [r for r in ranks if r >= 0]
    if not known:
        return "unknown"
    top = max(known)
    for name, rank in LIGHT_RANK.items():
        if rank == top:
            return name
    return "unknown"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


# ---------------------------------------------------------------------------
# Totals + accountability aggregation
# ---------------------------------------------------------------------------

def aggregate_totals(venue_recaps: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Sum revenue, wages, headcount across venues and derive group-level %s.

    Returns a dict shaped like a single-venue recap's numeric block so
    the dashboard can reuse its KPI tile renderer unchanged.
    """
    revenue_actual = 0.0
    revenue_forecast = 0.0
    wages_actual = 0.0
    wages_forecast = 0.0
    # For wage target we compute a weighted avg by forecast revenue so a
    # $50k venue at 28% and a $5k venue at 25% roll up as a sensible
    # 27.7%-ish target rather than a naive mean.
    weighted_target_num = 0.0
    weighted_target_den = 0.0
    peak_headcount = 0
    venue_count = 0

    for r in venue_recaps:
        venue_count += 1
        revenue_actual += _safe(r, "revenue", "actual")
        revenue_forecast += _safe(r, "revenue", "forecast")
        wages_actual += _safe(r, "wages", "actual")
        wages_forecast += _safe(r, "wages", "forecast")
        tgt = _safe(r, "wages", "pct_of_revenue_target")
        fc = _safe(r, "revenue", "forecast")
        if tgt > 0 and fc > 0:
            weighted_target_num += tgt * fc
            weighted_target_den += fc
        hc = _safe(r, "headcount", "peak")
        if hc > peak_headcount:
            peak_headcount = int(hc)

    revenue_delta = revenue_actual - revenue_forecast
    revenue_delta_pct = (revenue_delta / revenue_forecast) if revenue_forecast > 0 else 0.0
    wage_pct_actual = (wages_actual / revenue_actual) if revenue_actual > 0 else 0.0
    wage_pct_target = (
        weighted_target_num / weighted_target_den
        if weighted_target_den > 0
        else 0.0
    )
    wage_pct_delta = wage_pct_actual - wage_pct_target

    return {
        "venue_count": venue_count,
        "revenue": {
            "actual": round(revenue_actual, 2),
            "forecast": round(revenue_forecast, 2),
            "delta": round(revenue_delta, 2),
            "delta_pct": round(revenue_delta_pct, 4),
        },
        "wages": {
            "actual": round(wages_actual, 2),
            "forecast": round(wages_forecast, 2),
            "pct_of_revenue_actual": round(wage_pct_actual, 4),
            "pct_of_revenue_target": round(wage_pct_target, 4),
            "pct_delta": round(wage_pct_delta, 4),
        },
        "headcount": {
            "peak_across_portfolio": int(peak_headcount),
        },
    }


def aggregate_accountability(venue_recaps: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Sum the per-venue accountability blocks into a portfolio total."""
    total = pending = accepted = dismissed = 0
    missed = 0.0
    pending_impact = 0.0
    top_missed: List[Dict[str, Any]] = []

    for r in venue_recaps:
        acct = r.get("accountability") or {}
        if not isinstance(acct, dict):
            continue
        total += int(_safe(acct, "total"))
        pending += int(_safe(acct, "pending"))
        accepted += int(_safe(acct, "accepted"))
        dismissed += int(_safe(acct, "dismissed"))
        missed += _safe(acct, "estimated_impact_missed_aud")
        pending_impact += _safe(acct, "estimated_impact_pending_aud")
        venue_id = r.get("venue_id") or "?"
        for m in (acct.get("top_missed") or [])[:5]:
            top_missed.append({**m, "venue_id": venue_id})

    acceptance_rate = (
        accepted / (accepted + dismissed)
        if (accepted + dismissed) > 0
        else 0.0
    )
    # Sort the portfolio's top_missed by impact and take the top 5
    top_missed.sort(key=lambda m: float(m.get("impact_estimate_aud") or 0), reverse=True)
    top_missed = top_missed[:5]

    return {
        "total": total,
        "pending": pending,
        "accepted": accepted,
        "dismissed": dismissed,
        "estimated_impact_missed_aud": round(missed, 2),
        "estimated_impact_pending_aud": round(pending_impact, 2),
        "acceptance_rate": round(acceptance_rate, 4),
        "top_missed": top_missed,
    }


# ---------------------------------------------------------------------------
# Per-venue mini-summary for dashboard sub-cards
# ---------------------------------------------------------------------------

def _venue_mini(
    r: Dict[str, Any],
    label: Optional[str],
    *,
    trend: Optional[Dict[str, Any]] = None,
    signals_snapshot: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    mini: Dict[str, Any] = {
        "venue_id": r.get("venue_id") or "",
        "label": label or (r.get("venue_id") or ""),
        "traffic_light": r.get("traffic_light") or "unknown",
        "revenue_actual": _safe(r, "revenue", "actual"),
        "revenue_delta_pct": _safe(r, "revenue", "delta_pct"),
        "wage_pct_actual": _safe(r, "wages", "pct_of_revenue_actual"),
        "wage_pct_delta": _safe(r, "wages", "pct_delta"),
        "peak_headcount": int(_safe(r, "headcount", "peak")),
        "accountability": {
            "dismissed": int(_safe(r.get("accountability") or {}, "dismissed")),
            "missed_aud": _safe(r.get("accountability") or {}, "estimated_impact_missed_aud"),
        },
        "summary": r.get("summary") or "",
    }
    if trend is not None:
        mini["trend"] = _compact_trend(trend)
    if signals_snapshot is not None:
        mini["signals_snapshot"] = signals_snapshot
    return mini


def _compact_trend(trend: Dict[str, Any]) -> Dict[str, Any]:
    """Reduce a full ``trends.compose_trend`` dict to just the fields a
    dashboard mini-card needs: traffic light, headline, two sparkline
    series, and the slope deltas. Large ``daily`` arrays are dropped —
    mini-cards render sparklines from the two series alone.
    """
    if not isinstance(trend, dict):
        return {}
    series = trend.get("series") or {}
    slopes = trend.get("slopes") or {}
    totals = trend.get("totals") or {}

    def _series_list(key: str) -> List[float]:
        vals = series.get(key) or []
        out: List[float] = []
        for v in vals:
            try:
                out.append(float(v))
            except (TypeError, ValueError):
                out.append(0.0)
        return out

    def _slope_delta(key: str) -> float:
        s = slopes.get(key) or {}
        try:
            return float(s.get("delta") or 0.0)
        except (TypeError, ValueError):
            return 0.0

    return {
        "window_days": int(trend.get("window_days") or 0),
        "traffic_light": str(trend.get("traffic_light") or "unknown"),
        "headline": str(trend.get("headline") or ""),
        "series": {
            "acceptance_rate": _series_list("acceptance_rate"),
            "missed_aud": _series_list("missed_aud"),
        },
        "slopes": {
            "acceptance_rate_delta": round(_slope_delta("acceptance_rate"), 4),
            "missed_aud_delta": round(_slope_delta("missed_aud"), 2),
        },
        "totals": {
            "events": int(totals.get("events") or 0),
            "missed_aud": float(totals.get("missed_aud") or 0.0),
            "acceptance_rate": float(totals.get("acceptance_rate") or 0.0),
        },
    }


# ---------------------------------------------------------------------------
# Venue signals snapshot
# ---------------------------------------------------------------------------


def _build_venue_signals_snapshot(
    venue_id: str,
    target_date: str,
    *,
    weather_adapter: Optional[Any] = None,
    events_adapter: Optional[Any] = None,
    shift_event_store: Optional[Any] = None,
) -> Dict[str, Any]:
    """Build a lightweight signals snapshot for a single venue.

    Returns: {weather_today, events_this_week_count, patterns_count, shift_events_this_week}
    """
    import asyncio
    import os
    from datetime import datetime as dt, timedelta, timezone, date as date_type

    snapshot: Dict[str, Any] = {
        "weather_today": {},
        "events_this_week_count": 0,
        "patterns_count": 0,
        "shift_events_this_week": 0,
    }

    data_mode = os.environ.get("ROSTERIQ_DATA_MODE", "demo").lower()

    try:
        target_day = dt.strptime(target_date, "%Y-%m-%d").date()
    except (ValueError, TypeError):
        return snapshot

    # Weather today
    try:
        if weather_adapter is None:
            if data_mode == "demo":
                from rosteriq.data_feeds.bom import DemoWeatherAdapter
                weather_adapter = DemoWeatherAdapter()
            else:
                from rosteriq.data_feeds.bom import DemoWeatherAdapter
                weather_adapter = DemoWeatherAdapter()

        async def _fetch_today_weather():
            forecast = await weather_adapter.get_forecast(venue_id, days=1)
            return forecast

        try:
            loop = asyncio.get_event_loop()
            if loop.is_closed():
                raise RuntimeError("Event loop closed")
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

        forecast = loop.run_until_complete(_fetch_today_weather())
        if forecast and forecast[0].date == target_day:
            snapshot["weather_today"] = {
                "max_c": forecast[0].max_c,
                "min_c": forecast[0].min_c,
                "rain_probability_pct": forecast[0].rain_probability_pct,
                "conditions": forecast[0].conditions,
            }
    except Exception:
        pass

    # Events this week
    try:
        if events_adapter is None:
            if data_mode == "demo":
                from rosteriq.data_feeds.events import DemoEventsAdapter
                events_adapter = DemoEventsAdapter()
            else:
                from rosteriq.data_feeds.events import DemoEventsAdapter
                events_adapter = DemoEventsAdapter()

        # Count events for this week (7 days from target_day)
        async def _fetch_week_events():
            week_start = dt.combine(target_day, dt.min.time()).replace(tzinfo=timezone.utc)
            week_end = dt.combine(target_day + timedelta(days=7), dt.max.time()).replace(tzinfo=timezone.utc)
            events_list = await events_adapter.get_events(venue_id, week_start, week_end)
            return events_list

        try:
            loop = asyncio.get_event_loop()
            if loop.is_closed():
                raise RuntimeError("Event loop closed")
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

        events_week = loop.run_until_complete(_fetch_week_events())
        snapshot["events_this_week_count"] = len(events_week)
    except Exception:
        pass

    # Patterns and shift events
    try:
        if shift_event_store is None:
            from rosteriq import shift_events as se_module
            shift_event_store = se_module.shift_event_store

        from rosteriq.shift_events import PatternLearner

        all_events = shift_event_store.for_venue(venue_id)
        patterns = PatternLearner.analyse(all_events)
        snapshot["patterns_count"] = len(patterns)

        # Count shift events this week
        week_start_dt = dt.combine(target_day, dt.min.time()).replace(tzinfo=timezone.utc)
        week_end_dt = dt.combine(target_day + timedelta(days=7), dt.max.time()).replace(tzinfo=timezone.utc)
        week_events = [
            e for e in all_events
            if week_start_dt <= e.timestamp <= week_end_dt
        ]
        snapshot["shift_events_this_week"] = len(week_events)
    except Exception:
        pass

    return snapshot


# ---------------------------------------------------------------------------
# Portfolio summary line
# ---------------------------------------------------------------------------

def _compose_summary(
    *,
    venue_recaps: List[Dict[str, Any]],
    totals: Dict[str, Any],
    accountability: Dict[str, Any],
) -> str:
    n = len(venue_recaps)
    if n == 0:
        return "No venues reporting yet."

    # Count lights
    lights = [(r.get("traffic_light") or "unknown").lower() for r in venue_recaps]
    red = sum(1 for l in lights if l == "red")
    amber = sum(1 for l in lights if l == "amber")
    green = sum(1 for l in lights if l == "green")

    parts: List[str] = []
    parts.append(f"{n} venues: {red} red, {amber} amber, {green} green.")

    rev_actual = _safe(totals, "revenue", "actual")
    rev_fc = _safe(totals, "revenue", "forecast")
    rev_delta_pct = _safe(totals, "revenue", "delta_pct")
    wage_pct_actual = _safe(totals, "wages", "pct_of_revenue_actual")
    wage_pct_target = _safe(totals, "wages", "pct_of_revenue_target")
    wage_pct_delta = _safe(totals, "wages", "pct_delta")

    if rev_fc > 0:
        parts.append(
            f"Portfolio revenue {_fmt_money(rev_actual)} vs "
            f"{_fmt_money(rev_fc)} ({_fmt_pct(rev_delta_pct, signed=True)})."
        )
    if wage_pct_actual > 0 and wage_pct_target > 0:
        parts.append(
            f"Group wage % {_fmt_pct(wage_pct_actual)} "
            f"({_fmt_pct(wage_pct_delta, signed=True).replace('%', 'pt')} vs "
            f"{_fmt_pct(wage_pct_target)} target)."
        )

    dismissed = int(_safe(accountability, "dismissed"))
    missed = _safe(accountability, "estimated_impact_missed_aud")
    if dismissed > 0:
        word = "rec" if dismissed == 1 else "recs"
        if missed > 0:
            parts.append(
                f"{dismissed} {word} dismissed across the group "
                f"(~{_fmt_money(missed)} at stake)."
            )
        else:
            parts.append(f"{dismissed} {word} dismissed across the group.")

    return " ".join(parts)


# ---------------------------------------------------------------------------
# Main composer
# ---------------------------------------------------------------------------

def compose_portfolio(
    venue_recaps: List[Dict[str, Any]],
    *,
    portfolio_id: Optional[str] = None,
    shift_date: Optional[str] = None,
    venue_labels: Optional[Dict[str, str]] = None,
    include_trends: bool = False,
    trend_window_days: int = 7,
    trends_module: Any = None,
    trend_store: Any = None,
    include_signals: bool = False,
    weather_adapter: Optional[Any] = None,
    events_adapter: Optional[Any] = None,
    shift_event_store: Optional[Any] = None,
) -> Dict[str, Any]:
    """
    Roll a list of per-venue recap dicts up into a portfolio view.

    Args:
        venue_recaps: The output of ``shift_recap.compose_recap`` for
            each venue in the portfolio. An empty list is valid — the
            composer returns a zero-state recap in that case.
        portfolio_id: Optional group identifier (free-form string).
        shift_date: Optional override; defaults to the first recap's
            ``shift_date`` when present.
        venue_labels: Optional ``{venue_id: human_name}`` mapping so the
            mini-card displays 'Mojo's Bar' rather than 'venue_001'.
        include_trends: When True, compute a compact trends overlay per
            venue (sparkline series + headline + slope deltas) by pulling
            from ``accountability_store``. Defaults False so existing
            callers pay no cost.
        trend_window_days: Window passed to ``compose_trend_from_store``
            when ``include_trends`` is True. 7, 14, or 28.
        trends_module: Injectable ``rosteriq.trends`` for tests.
        trend_store: Injectable store for the trends composer.
        include_signals: When True, compute signals snapshot per venue
            (weather, events, patterns). Defaults False.
        weather_adapter, events_adapter, shift_event_store: Injectables.

    Returns:
        Dict with keys: ``portfolio_id``, ``shift_date``, ``generated_at``,
        ``traffic_light`` (worst-of), ``summary``, ``totals``,
        ``accountability``, ``venues`` (list of mini summaries),
        ``portfolio_weather_outlook`` (when signals included).
    """
    recaps = list(venue_recaps or [])
    labels = dict(venue_labels or {})

    totals = aggregate_totals(recaps)
    accountability = aggregate_accountability(recaps)
    lights = [r.get("traffic_light") or "unknown" for r in recaps]
    traffic_light = _worst_light(lights)

    summary = _compose_summary(
        venue_recaps=recaps,
        totals=totals,
        accountability=accountability,
    )

    trends_by_venue: Dict[str, Dict[str, Any]] = {}
    if include_trends and recaps:
        if trends_module is None:
            from rosteriq import trends as trends_module  # lazy import
        for r in recaps:
            vid = str(r.get("venue_id") or "")
            if not vid:
                continue
            try:
                t = trends_module.compose_trend_from_store(
                    vid,
                    window_days=trend_window_days,
                    store=trend_store,
                )
                trends_by_venue[vid] = t
            except Exception:
                # A trend fetch failing for one venue must not blow up
                # the portfolio roll-up — mini-cards just render without
                # a sparkline in that case.
                continue

    # Build signals snapshots per venue if requested
    signals_by_venue: Dict[str, Dict[str, Any]] = {}
    if include_signals and recaps:
        resolved_shift_date = shift_date or (
            recaps[0].get("shift_date") if recaps and isinstance(recaps[0], dict) else None
        ) or _now_iso().split("T")[0]  # fallback to today
        for r in recaps:
            vid = str(r.get("venue_id") or "")
            if not vid:
                continue
            try:
                sig = _build_venue_signals_snapshot(
                    vid,
                    resolved_shift_date,
                    weather_adapter=weather_adapter,
                    events_adapter=events_adapter,
                    shift_event_store=shift_event_store,
                )
                signals_by_venue[vid] = sig
            except Exception:
                # A signal fetch failing for one venue must not blow up portfolio
                continue

    mini_venues = [
        _venue_mini(
            r,
            labels.get(r.get("venue_id") or ""),
            trend=trends_by_venue.get(str(r.get("venue_id") or "")) if include_trends else None,
            signals_snapshot=signals_by_venue.get(str(r.get("venue_id") or "")) if include_signals else None,
        )
        for r in recaps
    ]
    # Sort so red-light venues float to the top (most urgent first)
    mini_venues.sort(
        key=lambda v: -LIGHT_RANK.get((v.get("traffic_light") or "unknown").lower(), -1)
    )

    # Shift date: prefer the explicit arg; fall back to the first recap.
    resolved_date = shift_date or (
        recaps[0].get("shift_date") if recaps and isinstance(recaps[0], dict) else None
    ) or ""

    # Build portfolio-level weather outlook (venues with rain risk)
    portfolio_weather_outlook: Dict[str, Any] = {
        "rain_risk_venues": [],
        "clear_venues": [],
    }
    if include_signals:
        for sig_data in signals_by_venue.values():
            weather = sig_data.get("weather_today") or {}
            rain_prob = weather.get("rain_probability_pct", 0)
            if rain_prob >= 50:
                portfolio_weather_outlook["rain_risk_venues"].append(
                    {
                        "rain_probability_pct": rain_prob,
                        "conditions": weather.get("conditions", "unknown"),
                    }
                )
            else:
                portfolio_weather_outlook["clear_venues"].append(
                    {
                        "rain_probability_pct": rain_prob,
                        "conditions": weather.get("conditions", "clear"),
                    }
                )

    result = {
        "portfolio_id": portfolio_id or "",
        "shift_date": resolved_date,
        "generated_at": _now_iso(),
        "traffic_light": traffic_light,
        "summary": summary,
        "totals": totals,
        "accountability": accountability,
        "venues": mini_venues,
    }
    if include_signals:
        result["portfolio_weather_outlook"] = portfolio_weather_outlook

    return result
