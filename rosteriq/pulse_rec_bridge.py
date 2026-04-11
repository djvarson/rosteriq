"""Translate a live wage-pulse snapshot into recommendation events.

Pure stdlib. Reads a LiveWagePulseResponse-shaped dict and returns a
list of recommendation dicts ready to be passed to
``rosteriq.accountability_store.record()``.

Each recommendation carries a deterministic ``rec_id`` of the form
``rec_pulse_{venue}_{YYYY-MM-DD}_{bucket}`` so the accountability
store's idempotency handles dedupe automatically — the bridge can be
called on every pulse poll without spamming the ledger.

Buckets are tiered by severity so that a +3pt overrun and a +5pt
overrun are *different* events. When the shift escalates, the ledger
gets a new entry instead of silently overwriting the earlier one.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

DEFAULT_TARGET_WAGE_PCT = 0.28

# Thresholds, expressed as decimal points of wage-%-of-revenue.
OVER_WAGE_MED_PT = 0.03     # +3pt over target → amber
OVER_WAGE_HIGH_PT = 0.05    # +5pt over target → red
UNDER_WAGE_AMBER_PT = -0.03  # -3pt under target → understaffing risk


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _shift_date(pulse: Dict[str, Any]) -> str:
    """Extract YYYY-MM-DD from the pulse's ISO timestamp, or 'unknown'."""
    ts = str(pulse.get("timestamp") or "")
    return ts[:10] if len(ts) >= 10 and ts[4] == "-" else "unknown"


def _safe_float(pulse: Dict[str, Any], key: str, default: float = 0.0) -> float:
    v = pulse.get(key)
    if v is None:
        return default
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def _bucket_id(venue_id: str, shift_date: str, name: str) -> str:
    return f"rec_pulse_{venue_id}_{shift_date}_{name}"


def _fmt_pts(decimal_pts: float) -> str:
    """Format 0.045 → '4.5'."""
    return f"{round(decimal_pts * 100, 1)}"


def _fmt_money(n: float) -> str:
    return f"${round(n):,.0f}"


# ---------------------------------------------------------------------------
# Evaluation
# ---------------------------------------------------------------------------

def evaluate_pulse(
    pulse: Dict[str, Any],
    *,
    target_wage_pct: Optional[float] = None,
) -> List[Dict[str, Any]]:
    """
    Evaluate a pulse snapshot and return a list of recommendation dicts.

    Args:
        pulse: LiveWagePulseResponse-shaped dict (must carry venue_id
            and timestamp; other fields are tolerated-missing).
        target_wage_pct: The venue's target wage-%-of-revenue, as a
            decimal (e.g. ``0.28`` for 28%). Defaults to ``DEFAULT_TARGET_WAGE_PCT``.

    Returns:
        Zero or more recs. Each has keys ``venue_id``, ``text``,
        ``source``, ``priority``, ``impact_estimate_aud``, ``rec_id``
        — directly splat-able into ``accountability_store.record()``.
    """
    venue_id = str(pulse.get("venue_id") or "")
    if not venue_id:
        return []

    target = float(
        target_wage_pct if target_wage_pct is not None else DEFAULT_TARGET_WAGE_PCT
    )
    shift_date = _shift_date(pulse)

    projected = _safe_float(pulse, "projected_wage_pct_of_revenue")
    current = _safe_float(pulse, "current_wage_pct_of_revenue")
    revenue_forecast = _safe_float(pulse, "revenue_forecast_today")
    wages_forecast = _safe_float(pulse, "wages_forecast_today")
    wages_burned = _safe_float(pulse, "wages_burned_so_far")
    hourly_burn = _safe_float(pulse, "hourly_burn_rate")
    minutes_remaining = int(_safe_float(pulse, "minutes_remaining"))

    # Use projected wage % if we have one (forward-looking), else current.
    wage_pct = projected if projected > 0 else current
    # Round to 4dp BEFORE comparison to dodge floating-point drift on
    # boundary values — e.g. 0.31 - 0.28 comes out as 0.029999...
    # which would fail a naive `>= 0.03` check.
    pct_delta = round(wage_pct - target, 4)
    # Guard against missing data: if we have no wage-%-of-revenue at
    # all, don't emit either an over- or under-wage recommendation.
    # (Missing data would otherwise trip the under-wage tier with a
    # delta of -0.28 against a 0% wage_pct.)
    have_wage_pct = wage_pct > 0

    recs: List[Dict[str, Any]] = []

    # --- Over-wage, high severity (>= +5pt) ---
    if have_wage_pct and pct_delta >= OVER_WAGE_HIGH_PT:
        impact = round(pct_delta * revenue_forecast, 0) if revenue_forecast > 0 else 0.0
        recs.append({
            "venue_id": venue_id,
            "text": (
                f"Wage % projected at {_fmt_pts(wage_pct)}% — "
                f"{_fmt_pts(pct_delta)}pt over {_fmt_pts(target)}% target. "
                f"Cut 2 staff now to avoid a {_fmt_money(impact)} overrun."
            ),
            "source": "wage_pulse",
            "priority": "high",
            "impact_estimate_aud": impact,
            "rec_id": _bucket_id(venue_id, shift_date, "over_wage_high"),
        })
    # --- Over-wage, medium severity (+3pt .. +5pt) ---
    elif have_wage_pct and pct_delta >= OVER_WAGE_MED_PT:
        impact = round(pct_delta * revenue_forecast, 0) if revenue_forecast > 0 else 0.0
        recs.append({
            "venue_id": venue_id,
            "text": (
                f"Wage % trending {_fmt_pts(pct_delta)}pt over "
                f"{_fmt_pts(target)}% target (now {_fmt_pts(wage_pct)}%). "
                f"Send 1 staff home after the next peak — ~{_fmt_money(impact)} at risk."
            ),
            "source": "wage_pulse",
            "priority": "med",
            "impact_estimate_aud": impact,
            "rec_id": _bucket_id(venue_id, shift_date, "over_wage_med"),
        })
    # --- Under-wage, amber (<= -3pt) ---
    elif have_wage_pct and pct_delta <= UNDER_WAGE_AMBER_PT:
        impact = round(abs(pct_delta) * revenue_forecast, 0) if revenue_forecast > 0 else 0.0
        recs.append({
            "venue_id": venue_id,
            "text": (
                f"Wage % is {_fmt_pts(abs(pct_delta))}pt under "
                f"{_fmt_pts(target)}% target — possibly understaffed. "
                f"Consider calling in a casual to protect service."
            ),
            "source": "wage_pulse",
            "priority": "med",
            "impact_estimate_aud": impact,
            "rec_id": _bucket_id(venue_id, shift_date, "under_wage"),
        })

    # --- Burn-rate alarm: even if the wage-% is OK, a runaway hourly
    # burn with time left on the clock can still blow the day's wage
    # forecast. This fires independently of the wage-% tier above.
    if (
        hourly_burn > 0
        and wages_forecast > 0
        and minutes_remaining >= 60
    ):
        projected_remaining = hourly_burn * (minutes_remaining / 60)
        projected_total = wages_burned + projected_remaining
        overrun = projected_total - wages_forecast
        threshold = max(100.0, wages_forecast * 0.10)
        if overrun >= threshold:
            recs.append({
                "venue_id": venue_id,
                "text": (
                    f"Burn rate {_fmt_money(hourly_burn)}/hr with "
                    f"{minutes_remaining // 60}h {minutes_remaining % 60}m left — "
                    f"projecting {_fmt_money(projected_total)} vs "
                    f"{_fmt_money(wages_forecast)} forecast. Trim at next break."
                ),
                "source": "wage_pulse",
                "priority": "high",
                "impact_estimate_aud": round(overrun, 0),
                "rec_id": _bucket_id(venue_id, shift_date, "burn_rate_high"),
            })

    return recs


# ---------------------------------------------------------------------------
# Store round-trip
# ---------------------------------------------------------------------------

def record_pulse_recs(
    pulse: Dict[str, Any],
    *,
    target_wage_pct: Optional[float] = None,
    store: Any = None,
) -> List[Dict[str, Any]]:
    """
    Evaluate the pulse and push any emitted recs into the accountability
    store. Returns the list of store events (one per recorded rec — the
    store's idempotency means re-calls with the same pulse return the
    same events unchanged).

    ``store`` is injectable for tests; defaults to
    ``rosteriq.accountability_store``. The store must expose a
    ``record(venue_id, *, text, source, priority, impact_estimate_aud,
    rec_id) -> Dict`` function.
    """
    if store is None:
        from rosteriq import accountability_store as store  # lazy import

    evaluated = evaluate_pulse(pulse, target_wage_pct=target_wage_pct)
    out: List[Dict[str, Any]] = []
    for rec in evaluated:
        ev = store.record(
            rec["venue_id"],
            text=rec["text"],
            source=rec.get("source", "wage_pulse"),
            priority=rec.get("priority", "med"),
            impact_estimate_aud=rec.get("impact_estimate_aud"),
            rec_id=rec.get("rec_id"),
        )
        out.append(ev)
    return out
